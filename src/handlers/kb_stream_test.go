// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package handlers

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/apache/openserverless-streaming-proxy/tcp"
)

func newKB(t *testing.T) (*kbStream, context.Context, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	s, e := setupKB(ctx, "127.0.0.1", "tenant/kb/ask-stream", "synthetic-cookie", map[string]interface{}{"question": "Perché?", "history": "[]"}, tcp.Request{V: 1, RID: strings.Repeat("01", 32), Key: strings.Repeat("ab", 32)})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(cancel)
	go s.run(ctx)
	return s, ctx, cancel
}
func dialKB(t *testing.T, s *kbStream) net.Conn {
	t.Helper()
	c, e := net.Dial("tcp", s.listener.Addr().String())
	if e != nil {
		t.Fatal(e)
	}
	c.SetDeadline(time.Now().Add(2 * time.Second))
	return c
}
func jsonLine(v interface{}) []byte { b, _ := json.Marshal(v); return append(b, '\n') }
func handshake(t *testing.T, s *kbStream, c net.Conn, fragment bool) {
	t.Helper()
	a := strings.Repeat("03", 32)
	h := tcp.Handshake{V: 1, RID: s.request.RID, ActionNonce: a, MAC: s.binding.HandshakeMAC(s.request.Key, "hello", a)}
	b := jsonLine(h)
	if fragment {
		for _, v := range b {
			if _, e := c.Write([]byte{v}); e != nil {
				t.Fatal(e)
			}
		}
	} else {
		if _, e := c.Write(b); e != nil {
			t.Fatal(e)
		}
	}
	line, e := bufio.NewReader(c).ReadBytes('\n')
	if e != nil {
		t.Fatal(e)
	}
	var ack tcp.Handshake
	if tcp.StrictJSON(line, &ack) != nil || ack.RID != h.RID || ack.ActionNonce != a || !tcp.EqualMAC(ack.MAC, s.binding.HandshakeMAC(s.request.Key, "ack", a)) {
		t.Fatal("invalid ACK")
	}
}
func frame(s *kbStream, seq int, kind string) tcp.Frame {
	f := tcp.Frame{V: 1, RID: s.request.RID, Seq: seq, Type: kind, Payload: base64.RawURLEncoding.EncodeToString([]byte(`{"answer":"è 漢字\\n<script>"}`))}
	f.MAC = f.ExpectedMAC(s.request.Key)
	return f
}
func TestKBFragmentationAndBytePreserving(t *testing.T) {
	s, _, _ := newKB(t)
	c := dialKB(t, s)
	defer c.Close()
	handshake(t, s, c, true)
	a, b := jsonLine(frame(s, 1, "progress")), jsonLine(frame(s, 2, "result"))
	both := append(append([]byte{}, a...), b...)
	for i := 0; i < len(both); i += 7 {
		end := i + 7
		if end > len(both) {
			end = len(both)
		}
		c.Write(both[i:end])
	}
	var got []byte
	for d := range s.data {
		got = append(got, d...)
	}
	if !bytes.Equal(got, both) {
		t.Fatal("record bytes changed")
	}
}
func TestKBInvalidPeerCannotAcquire(t *testing.T) {
	for _, which := range []string{"key", "binding", "rid", "nonce", "reflection", "session", "input", "port", "route", "expiry", "replay"} {
		t.Run(which, func(t *testing.T) {
			s, _, _ := newKB(t)
			c := dialKB(t, s)
			binding := s.binding
			key := s.request.Key
			a := strings.Repeat("03", 32)
			h := tcp.Handshake{V: 1, RID: s.request.RID, ActionNonce: a}
			switch which {
			case "key":
				key = strings.Repeat("ff", 32)
			case "binding":
				binding.RelayNonce = strings.Repeat("ff", 32)
			case "rid":
				h.RID = strings.Repeat("ff", 32)
			case "nonce":
				h.ActionNonce = "bad"
			case "session":
				binding.Session = strings.Repeat("ff", 32)
			case "input":
				binding.Input = strings.Repeat("ff", 32)
			case "port":
				binding.Port = "1"
			case "route":
				binding.Route = "tenant/other/action"
			case "expiry":
				binding.Expires--
			case "replay":
				binding.RelayNonce = strings.Repeat("00", 32)
			}
			domain := "hello"
			if which == "reflection" {
				domain = "ack"
			}
			h.MAC = binding.HandshakeMAC(key, domain, h.ActionNonce)
			c.Write(jsonLine(h))
			var b [1]byte
			if n, _ := c.Read(b[:]); n != 0 {
				t.Fatal("invalid peer received data")
			}
			c.Close()
			c = dialKB(t, s)
			defer c.Close()
			handshake(t, s, c, false)
			c.Write(jsonLine(frame(s, 1, "result")))
			if _, ok := <-s.data; !ok {
				t.Fatal("valid peer lost channel")
			}
		})
	}
}
func TestKBInvalidFramesClosed(t *testing.T) {
	for _, which := range []string{"mac", "seq", "rid", "type", "payload", "duplicate", "oversized", "eof", "maxevents", "maxtotal"} {
		t.Run(which, func(t *testing.T) {
			s, _, _ := newKB(t)
			c := dialKB(t, s)
			defer c.Close()
			handshake(t, s, c, false)
			f := frame(s, 1, "result")
			switch which {
			case "mac":
				f.MAC = strings.Repeat("00", 32)
			case "seq":
				f.Seq = 2
				f.MAC = f.ExpectedMAC(s.request.Key)
			case "rid":
				f.RID = strings.Repeat("02", 32)
				f.MAC = f.ExpectedMAC(s.request.Key)
			case "type":
				f.Type = "unknown"
				f.MAC = f.ExpectedMAC(s.request.Key)
			case "payload":
				f.Payload = "!"
				f.MAC = f.ExpectedMAC(s.request.Key)
			}
			switch which {
			case "duplicate":
				c.Write([]byte(`{"v":1,"v":1}` + "\n"))
			case "oversized":
				c.Write([]byte(strings.Repeat("a", tcp.MaxFrame+1) + "\n"))
			case "eof":
				c.Close()
			case "maxevents", "maxtotal":
				go func() {
					for seq := 1; seq <= tcp.MaxEvents+1; seq++ {
						g := frame(s, seq, "progress")
						if which == "maxtotal" {
							g.Payload = base64.RawURLEncoding.EncodeToString([]byte(`"` + strings.Repeat("a", 30000) + `"`))
							g.MAC = g.ExpectedMAC(s.request.Key)
						}
						if _, e := c.Write(jsonLine(g)); e != nil {
							return
						}
					}
					c.Close()
				}()
			default:
				c.Write(jsonLine(f))
			}
			count, total := 0, 0
			for d := range s.data {
				count++
				total += len(d)
			}
			if count > tcp.MaxEvents || total > tcp.MaxBytes {
				t.Fatal("limits exceeded")
			}
			if which != "maxevents" && which != "maxtotal" && count != 0 {
				t.Fatal("bad frame released")
			}
		})
	}
}
func TestKBCancelBackpressure(t *testing.T) {
	s, ctx, cancel := newKB(t)
	c := dialKB(t, s)
	defer c.Close()
	handshake(t, s, c, false)
	for i := 1; i <= 8; i++ {
		c.Write(jsonLine(frame(s, i, "progress")))
	}
	cancel()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("cancel timeout")
	}
	for range s.data {
	}
	var b [1]byte
	if n, _ := c.Read(b[:]); n != 0 {
		t.Fatal("socket not closed")
	}
}
func TestKBStrictDescriptor(t *testing.T) {
	for _, body := range []string{`{"stream_request":null}`, `{"stream_request":{}}`, `{"stream_request":"{}"}`, `{"stream_request":"{}","stream_request":"{}"}`, strings.Repeat(" ", maxRequestBytes+1)} {
		r := httptest.NewRequest("POST", "/", strings.NewReader(body))
		if _, _, e := parseBody(r); e == nil {
			t.Fatal("accepted malformed descriptor")
		}
	}
}
func TestKB202DoesNotTerminateAndHeaders(t *testing.T) {
	done := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		var body map[string]interface{}
		json.NewDecoder(r.Body).Decode(&body)
		if r.Header.Get("Cookie") != "synthetic-cookie" || r.Header.Get("Authorization") != "" || r.Header.Get("Origin") != "http://synthetic.invalid" {
			t.Error("headers")
		}
		var req tcp.Request
		tcp.StrictJSON([]byte(body["stream_request"].(string)), &req)
		var binding tcp.Binding
		tcp.StrictJSON([]byte(body["stream_binding"].(string)), &binding)
		s := &kbStream{request: req, binding: binding}
		go func() {
			time.Sleep(30 * time.Millisecond)
			c, e := net.Dial("tcp", net.JoinHostPort(binding.Host, binding.Port))
			if e != nil {
				return
			}
			defer c.Close()
			handshake(t, s, c, false)
			c.Write(jsonLine(frame(s, 1, "result")))
		}()
		w.WriteHeader(202)
	}))
	defer up.Close()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /web/{ns}/{pkg}/{action}", WebActionStreamHandler("127.0.0.1", up.URL))
	server := httptest.NewServer(mux)
	defer server.Close()
	sr, _ := json.Marshal(tcp.Request{V: 1, RID: strings.Repeat("01", 32), Key: strings.Repeat("ab", 32)})
	body, _ := json.Marshal(map[string]string{"question": "Perché?", "history": "[]", "stream_request": string(sr)})
	r, _ := http.NewRequest("POST", server.URL+"/web/tenant/kb/ask-stream", bytes.NewReader(body))
	r.Header.Set("Cookie", "synthetic-cookie")
	r.Header.Set("Origin", "http://synthetic.invalid")
	r.Header.Set("Authorization", "synthetic-not-forwarded")
	resp, e := http.DefaultClient.Do(r)
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	b, e := io.ReadAll(resp.Body)
	if e != nil || resp.StatusCode != 200 || !bytes.Contains(b, []byte(`"type":"result"`)) {
		t.Fatalf("202 truncated stream status %d len %d error %v", resp.StatusCode, len(b), e)
	}
	<-done
}

func TestKBPreFrameRejectionStatusIsSanitized(t *testing.T) {
	for _, code := range []int{400, 401, 403, 500} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(code)
				w.Write([]byte("synthetic-private-detail"))
			}))
			defer up.Close()
			mux := http.NewServeMux()
			mux.HandleFunc("POST /web/{ns}/{pkg}/{action}", WebActionStreamHandler("127.0.0.1", up.URL))
			relay := httptest.NewServer(mux)
			defer relay.Close()
			sr, _ := json.Marshal(tcp.Request{V: 1, RID: strings.Repeat("01", 32), Key: strings.Repeat("ab", 32)})
			body, _ := json.Marshal(map[string]string{"question": "Sintetica", "stream_request": string(sr)})
			resp, err := http.Post(relay.URL+"/web/tenant/kb/ask-stream", "application/json", bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			data, _ := io.ReadAll(resp.Body)
			want := code
			if code == 500 {
				want = 502
			}
			if resp.StatusCode != want || bytes.Contains(data, []byte("synthetic-private-detail")) {
				t.Fatalf("status %d, private body forwarded %v", resp.StatusCode, bytes.Contains(data, []byte("synthetic-private-detail")))
			}
		})
	}
}
func TestKBSwappedRequestsAndPortReuse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	create := func(rid, key string) *kbStream {
		s, e := setupKB(ctx, "127.0.0.1", "tenant/kb/ask-stream", "synthetic-"+rid, map[string]interface{}{"question": "Domanda " + rid, "history": "[]"}, tcp.Request{V: 1, RID: strings.Repeat(rid, 32), Key: strings.Repeat(key, 32)})
		if e != nil {
			t.Fatal(e)
		}
		return s
	}
	a, b := create("01", "ab"), create("02", "cd")
	oldAddr := a.listener.Addr().String()
	oldBinding := a.binding
	oldReq := a.request
	go a.run(ctx)
	go b.run(ctx)
	c := dialKB(t, b)
	nonce := strings.Repeat("03", 32)
	hello := tcp.Handshake{V: 1, RID: a.request.RID, ActionNonce: nonce, MAC: a.binding.HandshakeMAC(a.request.Key, "hello", nonce)}
	c.Write(jsonLine(hello))
	var scratch [1]byte
	if n, _ := c.Read(scratch[:]); n > 0 {
		t.Fatal("cross-request accepted")
	}
	c.Close()
	for _, s := range []*kbStream{a, b} {
		c = dialKB(t, s)
		handshake(t, s, c, false)
		c.Write(jsonLine(frame(s, 1, "result")))
		c.Close()
		for range s.data {
		}
	}
	listener, e := net.Listen("tcp", oldAddr)
	if e != nil {
		t.Fatal(e)
	}
	newer := &kbStream{listener: listener, request: tcp.Request{V: 1, RID: strings.Repeat("04", 32), Key: strings.Repeat("ef", 32)}, binding: oldBinding, data: make(chan []byte, 4), errors: make(chan error, 1)}
	newer.binding.RID = newer.request.RID
	newer.binding.RelayNonce = strings.Repeat("05", 32)
	go newer.run(ctx)
	c = dialKB(t, newer)
	hello = tcp.Handshake{V: 1, RID: oldReq.RID, ActionNonce: nonce, MAC: oldBinding.HandshakeMAC(oldReq.Key, "hello", nonce)}
	c.Write(jsonLine(hello))
	if n, _ := c.Read(scratch[:]); n > 0 {
		t.Fatal("port reuse replay accepted")
	}
	c.Close()
	c = dialKB(t, newer)
	defer c.Close()
	handshake(t, newer, c, false)
	c.Write(jsonLine(frame(newer, 1, "result")))
	for range newer.data {
	}
}
func TestKBExpiryAndHandshakeAttemptLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	body := map[string]interface{}{"question": "Sintetica", "history": "[]"}
	s, e := setupKB(ctx, "127.0.0.1", "tenant/kb/ask-stream", "synthetic-cookie", body, tcp.Request{V: 1, RID: strings.Repeat("01", 32), Key: strings.Repeat("ab", 32)})
	if e != nil {
		t.Fatal(e)
	}
	s.binding.Expires = time.Now().Unix() - 1
	go s.run(ctx)
	for i := 0; i < 8; i++ {
		c := dialKB(t, s)
		a := strings.Repeat("03", 32)
		c.Write(jsonLine(tcp.Handshake{V: 1, RID: s.request.RID, ActionNonce: a, MAC: s.binding.HandshakeMAC(s.request.Key, "hello", a)}))
		var b [1]byte
		if n, _ := c.Read(b[:]); n != 0 {
			t.Fatal("expired released ACK")
		}
		c.Close()
	}
	for range s.data {
	}
}
func TestKBGlobalSlotsAndHeaderAmbiguity(t *testing.T) {
	req := tcp.Request{V: 1, RID: strings.Repeat("01", 32), Key: strings.Repeat("ab", 32)}
	sr, _ := json.Marshal(req)
	body, _ := json.Marshal(map[string]string{"question": "Sintetica", "stream_request": string(sr)})
	for i := 0; i < cap(webStreamSlots); i++ {
		webStreamSlots <- struct{}{}
	}
	r := httptest.NewRequest("POST", "/", bytes.NewReader(body))
	r.SetPathValue("ns", "tenant")
	r.SetPathValue("pkg", "kb")
	r.SetPathValue("action", "ask-stream")
	w := httptest.NewRecorder()
	WebActionStreamHandler("127.0.0.1", "http://127.0.0.1:1")(w, r)
	for i := 0; i < cap(webStreamSlots); i++ {
		<-webStreamSlots
	}
	if w.Code != 503 {
		t.Fatal("global slots unbounded")
	}
	r = httptest.NewRequest("POST", "/", nil)
	r.Header.Add("Cookie", "a=1")
	r.Header.Add("Cookie", "b=2")
	if _, e := kbHeaders(r); e == nil {
		t.Fatal("ambiguous Cookie accepted")
	}
}
func TestHTTPClientStopClosesCallback(t *testing.T) {
	stopped := make(chan struct{})
	socketClosed := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]interface{}
		json.NewDecoder(r.Body).Decode(&body)
		var req tcp.Request
		tcp.StrictJSON([]byte(body["stream_request"].(string)), &req)
		var binding tcp.Binding
		tcp.StrictJSON([]byte(body["stream_binding"].(string)), &binding)
		s := &kbStream{request: req, binding: binding}
		c, e := net.Dial("tcp", net.JoinHostPort(binding.Host, binding.Port))
		if e != nil {
			t.Error(e)
			return
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(3 * time.Second))
		handshake(t, s, c, false)
		c.Write(jsonLine(frame(s, 1, "progress")))
		go func() { var b [1]byte; c.Read(b[:]); close(socketClosed) }()
		<-r.Context().Done()
		close(stopped)
	}))
	defer up.Close()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /web/{ns}/{pkg}/{action}", WebActionStreamHandler("127.0.0.1", up.URL))
	server := httptest.NewServer(mux)
	defer server.Close()
	sr, _ := json.Marshal(tcp.Request{V: 1, RID: strings.Repeat("01", 32), Key: strings.Repeat("ab", 32)})
	body, _ := json.Marshal(map[string]string{"question": "Sintetica", "stream_request": string(sr)})
	resp, e := http.Post(server.URL+"/web/tenant/kb/ask-stream", "application/json", bytes.NewReader(body))
	if e != nil {
		t.Fatal(e)
	}
	if _, e = bufio.NewReader(resp.Body).ReadBytes('\n'); e != nil {
		t.Fatal(e)
	}
	resp.Body.Close()
	for _, done := range []chan struct{}{stopped, socketClosed} {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("client Stop leaked active transport")
		}
	}
}
func TestKBTerminalOnceAndSilentPeerTimeout(t *testing.T) {
	s, _, _ := newKB(t)
	silent := dialKB(t, s)
	defer silent.Close()
	var b [1]byte
	silent.SetReadDeadline(time.Now().Add(2500 * time.Millisecond))
	if n, _ := silent.Read(b[:]); n != 0 {
		t.Fatal("silent peer authenticated")
	}
	c := dialKB(t, s)
	defer c.Close()
	handshake(t, s, c, false)
	both := append(jsonLine(frame(s, 1, "result")), jsonLine(frame(s, 2, "result"))...)
	c.Write(both)
	count := 0
	for range s.data {
		count++
	}
	if count != 1 {
		t.Fatal("multiple terminal records released")
	}
}

func TestKBSilentPeerDoesNotDelayAuthenticatedCallback(t *testing.T) {
	s, _, _ := newKB(t)
	silent := dialKB(t, s)
	defer silent.Close()
	start := time.Now()
	c := dialKB(t, s)
	defer c.Close()
	handshake(t, s, c, false)
	if time.Since(start) >= time.Second {
		t.Fatal("silent peer delayed authenticated callback")
	}
	c.Write(jsonLine(frame(s, 1, "result")))
	if _, open := <-s.data; !open {
		t.Fatal("authenticated frame lost")
	}
}
