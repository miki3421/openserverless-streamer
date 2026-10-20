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
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func availabilityServer(t *testing.T) (*httptest.Server, <-chan struct{}) {
	t.Helper()
	entered := make(chan struct{}, 128)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, _, _ := r.BasicAuth()
		if user == "alpha-rotated" {
			user = "alpha"
		}
		if r.Method == "GET" && r.URL.Path == "/api/v1/namespaces" {
			if user != "alpha" && user != "beta" {
				w.WriteHeader(401)
				return
			}
			json.NewEncoder(w).Encode([]string{user})
			return
		}
		var body map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.WriteHeader(400)
			return
		}
		if body["hold"] == true {
			entered <- struct{}{}
			w.WriteHeader(202)
			return
		}
		conn, err := net.DialTimeout("tcp", net.JoinHostPort(body["STREAM_HOST"].(string), body["STREAM_PORT"].(string)), time.Second)
		if err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		defer conn.Close()
		fmt.Fprintf(conn, "OPS-STREAM %s\n\"ok\"\n", body["STREAM_TOKEN"].(string))
		w.WriteHeader(202)
	}))
	mux := http.NewServeMux()
	for _, method := range []string{"GET", "POST"} {
		mux.HandleFunc(method+" /web/{ns}/{pkg}/{action}", WebActionStreamHandler("127.0.0.1", upstream.URL))
		mux.HandleFunc(method+" /action/{ns}/{pkg}/{action}", ActionStreamHandler("127.0.0.1", upstream.URL))
	}
	proxy := httptest.NewServer(mux)
	t.Cleanup(func() { proxy.Close(); upstream.Close() })
	return proxy, entered
}

func holdStreams(t *testing.T, proxy *httptest.Server, entered <-chan struct{}, count int, target func(int) (string, string)) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		path, auth := target(i)
		req, _ := http.NewRequestWithContext(ctx, "POST", proxy.URL+path, strings.NewReader(`{"hold":true}`))
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, _ := proxy.Client().Do(req)
			if resp != nil {
				resp.Body.Close()
			}
		}()
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			cancel()
			wg.Wait()
			t.Fatal("expected pending action was not invoked")
		}
	}
	return func() { cancel(); wg.Wait() }
}

func availabilityRequest(t *testing.T, proxy *httptest.Server, path, auth string, extra http.Header) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", proxy.URL+path, strings.NewReader(`{}`))
	req.Header = extra.Clone()
	if req.Header == nil {
		req.Header = make(http.Header)
	}
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	resp, err := proxy.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(body)
}

func TestTenantQuotaPreservesOtherTenantAcrossRoutesAndAliases(t *testing.T) {
	proxy, entered := availabilityServer(t)
	stop := holdStreams(t, proxy, entered, 8, func(int) (string, string) {
		return "/action/alpha/validation/events", "Bearer alpha:key"
	})
	defer stop()
	for _, row := range []struct{ path, auth string }{
		{"/action/alpha/validation/events", "Bearer alpha:key"},
		{"/action/_/validation/events", "Basic " + base64.StdEncoding.EncodeToString([]byte("alpha:key"))},
		{"/web/other-target/validation/events", "Basic " + base64.StdEncoding.EncodeToString([]byte("alpha-rotated:key"))},
		{"/web/alpha/validation/events", "Basic " + base64.StdEncoding.EncodeToString([]byte("forged:key"))},
	} {
		status, _ := availabilityRequest(t, proxy, row.path, row.auth, nil)
		if status != 429 {
			t.Fatalf("quota alias bypass: got %d", status)
		}
	}
	for _, route := range []string{"web", "action"} {
		status, body := availabilityRequest(t, proxy, "/"+route+"/beta/validation/events", "Basic "+base64.StdEncoding.EncodeToString([]byte("beta:key")), nil)
		if status != 200 || body != "\"ok\"\n" {
			t.Fatalf("other tenant affected: %d %q", status, body)
		}
	}
	stop()
	status, _ := availabilityRequest(t, proxy, "/action/alpha/validation/events", "Bearer alpha:key", nil)
	if status != 200 {
		t.Fatal("tenant quota did not recover after cancellation")
	}
}

func TestForgedWebHeadersCannotConsumeVerifiedStreamCapacity(t *testing.T) {
	proxy, entered := availabilityServer(t)
	stop := holdStreams(t, proxy, entered, 16, func(i int) (string, string) {
		return fmt.Sprintf("/web/public-%d/validation/events", i), fmt.Sprintf("Bearer forged-%d:key", i)
	})
	defer stop()
	status, _ := availabilityRequest(t, proxy, "/web/next-target/validation/events", "Bearer different:key", http.Header{"X-Forwarded-For": {"203.0.113.99"}, "Forwarded": {"for=203.0.113.99"}})
	if status != 429 {
		t.Fatal("unverified caller bypassed peer quota")
	}
	status, body := availabilityRequest(t, proxy, "/web/beta/validation/events", "Bearer beta:key", nil)
	if status != 200 || body != "\"ok\"\n" {
		t.Fatal("anonymous traffic consumed verified capacity")
	}
}

func TestPrivateDefaultAliasDoesNotShareQuotaBetweenTenants(t *testing.T) {
	proxy, entered := availabilityServer(t)
	stop := holdStreams(t, proxy, entered, 8, func(int) (string, string) {
		return "/action/_/validation/events", "Bearer alpha:key"
	})
	defer stop()
	status, _ := availabilityRequest(t, proxy, "/action/alpha/validation/events", "Bearer alpha:key", nil)
	if status != 429 {
		t.Fatal("explicit target bypassed the default alias caller quota")
	}
	status, body := availabilityRequest(t, proxy, "/action/_/validation/events", "Bearer beta:key", nil)
	if status != 200 || body != "\"ok\"\n" {
		t.Fatal("one tenant consumed another tenant's default alias capacity")
	}
}

func TestWebCustomAuthorizationRemainsAnUpstreamDecision(t *testing.T) {
	// This is a web-action credential, not an OpenWhisk namespace key. It must
	// retain the bounded anonymous capacity and reach the web action unchanged.
	const credential = "Bearer custom-web-secret"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/namespaces" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.Header.Get("Authorization") != credential {
			t.Error("web authorization was changed")
			w.WriteHeader(http.StatusForbidden)
			return
		}
		var body map[string]interface{}
		if json.NewDecoder(r.Body).Decode(&body) != nil {
			w.WriteHeader(400)
			return
		}
		conn, err := net.DialTimeout("tcp", net.JoinHostPort(body["STREAM_HOST"].(string), body["STREAM_PORT"].(string)), time.Second)
		if err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		defer conn.Close()
		fmt.Fprintf(conn, "OPS-STREAM %s\n\"custom\"\n", body["STREAM_TOKEN"].(string))
		w.WriteHeader(202)
	}))
	defer upstream.Close()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /web/{ns}/{pkg}/{action}", WebActionStreamHandler("127.0.0.1", upstream.URL))
	proxy := httptest.NewServer(mux)
	defer proxy.Close()
	status, body := availabilityRequest(t, proxy, "/web/alpha/custom/events", credential, nil)
	if status != 200 || body != "\"custom\"\n" {
		t.Fatal("custom web authentication no longer works")
	}
}
