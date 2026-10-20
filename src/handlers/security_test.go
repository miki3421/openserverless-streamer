// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func routed(req *http.Request) *http.Request {
	req.SetPathValue("ns", "synthetic")
	req.SetPathValue("pkg", "validation")
	req.SetPathValue("action", "events")
	return req
}

// This test is also run unmodified against the vulnerable source. A panic in
// the detached goroutine must be observed as a nonzero child-process exit.
func TestCancellationSurvivesUpstreamError(t *testing.T) {
	if os.Args[len(os.Args)-1] != "stream-child" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestCancellationSurvivesUpstreamError$", "--", "stream-child")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("streamer terminated: %v\n%s", err, output)
		}
		return
	}
	entered, release := make(chan struct{}), make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-release; w.WriteHeader(403) }))
	defer upstream.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		WebActionStreamHandler("127.0.0.1", upstream.URL)(w, routed(r))
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, "POST", server.URL, strings.NewReader("{}"))
	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, _ := server.Client().Do(req)
		if resp != nil {
			resp.Body.Close()
		}
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream did not start")
	}
	cancel()
	<-done
	time.Sleep(40 * time.Millisecond)
	close(release)
	time.Sleep(80 * time.Millisecond)
}

func TestBodyLimitRejectsBeforeUpstream(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(403) }))
	defer upstream.Close()
	for _, web := range []bool{true, false} {
		for _, method := range []string{"GET", "POST"} {
			t.Run(method+map[bool]string{true: "web", false: "private"}[web], func(t *testing.T) {
				req := routed(httptest.NewRequest(method, "/", strings.NewReader(`{"data":"`+strings.Repeat("x", (1<<20)+1)+`"}`)))
				req.Header.Set("Authorization", "Bearer synthetic:fixture")
				req.ContentLength = -1 // Also exercises unknown-length/chunked input.
				recorder := httptest.NewRecorder()
				handler := WebActionStreamHandler("127.0.0.1", upstream.URL)
				if !web {
					handler = ActionStreamHandler("127.0.0.1", upstream.URL)
				}
				handler(recorder, req)
				if recorder.Code != 413 {
					t.Fatalf("want 413, got %d", recorder.Code)
				}
			})
		}
	}
	if calls.Load() != 0 {
		t.Fatal("oversized input reached upstream")
	}
}

func TestMalformedObjectsRejectBeforeUpstream(t *testing.T) {
	for _, input := range []string{"null", "[]", "{} {}", "{invalid", `{"x":1} trailing`} {
		req := routed(httptest.NewRequest("POST", "/", strings.NewReader(input)))
		out := httptest.NewRecorder()
		WebActionStreamHandler("127.0.0.1", "http://127.0.0.1:1")(out, req)
		if out.Code != 400 {
			t.Fatalf("%s: want 400 got %d", input, out.Code)
		}
	}
}

func TestPrivateAuthorizationRequired(t *testing.T) {
	req := routed(httptest.NewRequest("POST", "/", strings.NewReader("{}")))
	out := httptest.NewRecorder()
	ActionStreamHandler("127.0.0.1", "http://127.0.0.1:1")(out, req)
	if out.Code != 401 {
		t.Fatalf("want 401 got %d", out.Code)
	}
}

type closeBody struct {
	io.Reader
	closed bool
}

func (b *closeBody) Close() error { b.closed = true; return nil }

type roundTrip func(*http.Request) (*http.Response, error)

func (rt roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return rt(r) }

func TestUpstreamResponseClosedOnSuccessAndFailure(t *testing.T) {
	for _, status := range []int{200, 202, 401, 403, 500} {
		body := &closeBody{Reader: strings.NewReader("synthetic")}
		client := &http.Client{Transport: roundTrip(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: status, Body: body}, nil
		})}
		req, _ := http.NewRequest("POST", "http://synthetic.test", nil)
		got := invoke(client, req)
		if got.err != nil || got.status != status || !body.closed {
			t.Fatalf("response %d not cleaned up", status)
		}
	}
}

func TestSuccessfulNonStreamingActionHasCallbackDeadline(t *testing.T) {
	entered := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); w.WriteHeader(200) }))
	defer upstream.Close()
	ctx, cancel := context.WithTimeout(context.Background(), streamStartTimeout+2*time.Second)
	defer cancel()
	req := routed(httptest.NewRequest("POST", "/", strings.NewReader("{}"))).WithContext(ctx)
	out := httptest.NewRecorder()
	started := time.Now()
	WebActionStreamHandler("127.0.0.1", upstream.URL)(out, req)
	if out.Code != 504 || time.Since(started) > streamStartTimeout+time.Second {
		t.Fatalf("missing callback deadline: %d", out.Code)
	}
}

func TestStreamConcurrencyLimitAndRecovery(t *testing.T) {
	for i := 0; i < cap(streamSlots); i++ {
		streamSlots <- struct{}{}
	}
	req := routed(httptest.NewRequest("POST", "/", strings.NewReader("{}")))
	out := httptest.NewRecorder()
	WebActionStreamHandler("127.0.0.1", "http://127.0.0.1:1")(out, req)
	for i := 0; i < cap(streamSlots); i++ {
		<-streamSlots
	}
	if out.Code != 429 {
		t.Fatalf("want 429 got %d", out.Code)
	}
}

func TestAuthenticatedCallbackPreservesExactBytesAndUpstreamHeaders(t *testing.T) {
	expected := bytes.Repeat([]byte("0123456789\n"), 1000)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Basic synthetic" || r.Header.Get("Content-Type") != "application/json" {
			t.Error("legitimate headers lost")
		}
		if r.Header.Get("X-Hop") != "" || r.ContentLength <= 0 {
			t.Error("unsafe framing forwarded")
		}
		var body map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			return
		}
		conn, err := net.DialTimeout("tcp", net.JoinHostPort(body["STREAM_HOST"].(string), body["STREAM_PORT"].(string)), time.Second)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		conn.Write([]byte("OPS-STREAM " + body["STREAM_TOKEN"].(string) + "\n"))
		conn.Write(expected)
	}))
	defer upstream.Close()
	req := routed(httptest.NewRequest("POST", "/", strings.NewReader(`{"STREAM_TOKEN":"caller-cannot-choose-this"}`)))
	req.Header.Set("Authorization", "Basic synthetic")
	req.Header.Set("Connection", "X-Hop")
	req.Header.Set("X-Hop", "must-not-forward")
	req.Header.Set("Content-Type", "text/plain")
	out := httptest.NewRecorder()
	WebActionStreamHandler("127.0.0.1", upstream.URL)(out, req)
	if out.Code != 200 || !bytes.Equal(out.Body.Bytes(), expected) {
		t.Fatalf("stream bytes changed (%d, %d)", out.Code, out.Body.Len())
	}
	if bytes.Contains(out.Body.Bytes(), []byte("OPS-STREAM")) {
		t.Fatal("handshake leaked")
	}
}

func TestUpstreamRedirectNeverFollowed(t *testing.T) {
	var leaked atomic.Bool
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked.Store(true) }))
	defer other.Close()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, other.URL, 307) }))
	defer upstream.Close()
	out := httptest.NewRecorder()
	WebActionStreamHandler("127.0.0.1", upstream.URL)(out, routed(httptest.NewRequest("POST", "/", strings.NewReader("{}"))))
	if out.Code != 502 || leaked.Load() {
		t.Fatal("redirect escaped fixed upstream")
	}
}

func TestURLSegmentsCannotEscapeFixedEndpoint(t *testing.T) {
	for _, value := range []string{"..", "../namespaces", "a/b", "a\\b", "a\nheader"} {
		req := routed(httptest.NewRequest("POST", "/", nil))
		req.SetPathValue("action", value)
		if _, err := actionURL("http://controller:3233", req, true); err == nil {
			t.Fatal("unsafe segment accepted")
		}
	}
}
