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
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/apache/openserverless-streaming-proxy/tcp"
)

const maxRequestBytes = 65536
const streamTimeout = 130 * time.Second
const handshakeTimeout = 2 * time.Second

// Shared by KB, legacy and uploads whose protocol has not been parsed yet.
var webStreamSlots = make(chan struct{}, 32)

type kbStream struct {
	listener net.Listener
	binding  tcp.Binding
	request  tcp.Request
	data     chan []byte
	errors   chan error
	claimed  atomic.Bool
}

// A context by itself does not interrupt a request Body.Read. Set the actual
// HTTP read deadline, and close the body on cancellation. Request.Body's contract
// requires concurrent Close to unblock Read (also covers synthetic io.Pipe bodies).
func parseBodyWithContext(ctx context.Context, w http.ResponseWriter, r *http.Request) (map[string]interface{}, *tcp.Request, error) {
	controller := http.NewResponseController(w)
	if deadline, ok := ctx.Deadline(); ok {
		controller.SetReadDeadline(deadline)
	}
	closed := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		controller.SetReadDeadline(time.Now())
		r.Body.Close()
		close(closed)
	})
	defer func() {
		if !stop() {
			<-closed
		}
		// Do not leave a read deadline on a reusable HTTP connection.
		controller.SetReadDeadline(time.Time{})
	}()
	body, request, err := parseBody(r)
	if ctx.Err() != nil {
		return nil, nil, ctx.Err()
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return nil, nil, context.DeadlineExceeded
	}
	return body, request, err
}

func parseBody(r *http.Request) (map[string]interface{}, *tcp.Request, error) {
	defer r.Body.Close()
	raw, e := io.ReadAll(io.LimitReader(r.Body, maxRequestBytes+1))
	if e != nil {
		return nil, nil, tcp.ErrProtocol
	}
	if len(raw) > maxRequestBytes {
		return nil, nil, &http.MaxBytesError{Limit: maxRequestBytes}
	}
	return parseKBBodyBytes(raw)
}

// Detection precedes strict parsing so malformed KB never falls back.
func parseKBBodyBytes(raw []byte) (map[string]interface{}, *tcp.Request, error) {
	if len(raw) > maxRequestBytes {
		return nil, nil, tcp.ErrProtocol
	}
	body := map[string]interface{}{}
	var e error
	if len(raw) == 0 {
		return body, nil, nil
	}
	if e = json.Unmarshal(raw, &body); e != nil || body == nil {
		return nil, nil, tcp.ErrProtocol
	}
	sr, present := body["stream_request"]
	if !present {
		return body, nil, nil
	}
	if e = tcp.StrictJSON(raw, &body); e != nil {
		return nil, nil, e
	}
	text, ok := sr.(string)
	if !ok || len(text) > 256 {
		return nil, nil, tcp.ErrProtocol
	}
	var req tcp.Request
	if e = tcp.StrictJSON([]byte(text), &req); e != nil || req.V != 1 || !tcp.Hex32(req.Key) || !tcp.Hex32(req.RID) {
		return nil, nil, tcp.ErrProtocol
	}
	return body, &req, nil
}

func setupKB(ctx context.Context, addr, route, cookie string, body map[string]interface{}, req tcp.Request) (*kbStream, error) {
	question, ok := body["question"].(string)
	if !ok || !utf8.ValidString(question) || len([]rune(question)) < 1 || len([]rune(question)) > 2000 {
		return nil, tcp.ErrProtocol
	}
	history := "[]"
	if h, present := body["history"]; present {
		history, ok = h.(string)
		if !ok {
			return nil, tcp.ErrProtocol
		}
	}
	var messages []map[string]interface{}
	if tcp.StrictJSON([]byte(history), &messages) != nil || messages == nil || len(messages) > 12 {
		return nil, tcp.ErrProtocol
	}
	for _, m := range messages {
		role, rok := m["role"].(string)
		content, cok := m["content"].(string)
		if len(m) != 2 || !rok || !cok || (role != "user" && role != "assistant") || len([]rune(content)) > 1000 {
			return nil, tcp.ErrProtocol
		}
	}
	listener, e := net.Listen("tcp", net.JoinHostPort(addr, "0"))
	if e != nil {
		return nil, e
	}
	host, port, e := net.SplitHostPort(listener.Addr().String())
	if e != nil {
		listener.Close()
		return nil, e
	}
	nonce, e := tcp.Nonce()
	if e != nil {
		listener.Close()
		return nil, e
	}
	s := &kbStream{listener: listener, request: req, data: make(chan []byte, 4), errors: make(chan error, 1)}
	deadline, _ := ctx.Deadline()
	s.binding = tcp.Binding{V: 1, RID: req.RID, Route: route, Session: tcp.Digest([]byte(cookie)), Input: tcp.Digest(tcp.LP(question, history)), Host: host, Port: port, Expires: deadline.Unix(), RelayNonce: nonce}
	encoded, e := json.Marshal(s.binding)
	if e != nil {
		listener.Close()
		return nil, e
	}
	body["stream_binding"] = string(encoded)
	// These are existing upstream callback fields, never new configuration reads.
	body["STREAM_HOST"] = host
	body["STREAM_PORT"] = port
	return s, nil
}

func (s *kbStream) run(ctx context.Context) {
	defer close(s.data)
	defer s.listener.Close()
	stopped := make(chan struct{})
	defer close(stopped)
	go func() {
		select {
		case <-ctx.Done():
			s.listener.Close()
		case <-stopped:
		}
	}()
	deadline, _ := ctx.Deadline()
	if l, ok := s.listener.(*net.TCPListener); ok {
		l.SetDeadline(deadline)
	}
	var workers sync.WaitGroup
	results := make(chan error, 8)
	for attempt := 0; attempt < 8 && !s.claimed.Load(); attempt++ {
		conn, e := s.listener.Accept()
		if e != nil {
			break
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer conn.Close()
			results <- s.peer(ctx, conn, deadline)
		}()
	}
	workers.Wait()
	close(results)
	if ctx.Err() != nil {
		return
	}
	for e := range results {
		if e == nil {
			return
		}
	}
	s.fail(ctx, tcp.ErrProtocol)
}

var errAuthenticated = errors.New("authenticated stream failed")

func (s *kbStream) fail(ctx context.Context, e error) {
	select {
	case s.errors <- e:
	case <-ctx.Done():
	}
}
func readRecord(r *bufio.Reader, max int) ([]byte, error) {
	line, e := r.ReadSlice('\n')
	if e != nil || len(line) > max {
		return nil, tcp.ErrProtocol
	}
	// ASCII-only NDJSON avoids ambiguity across incremental browser decoders.
	for _, b := range line {
		if b > 127 || b == '\r' {
			return nil, tcp.ErrProtocol
		}
	}
	return line, nil
}
func (s *kbStream) peer(ctx context.Context, conn net.Conn, deadline time.Time) error {
	stopped := make(chan struct{})
	defer close(stopped)
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-stopped:
		}
	}()
	hd := time.Now().Add(handshakeTimeout)
	if deadline.Before(hd) {
		hd = deadline
	}
	conn.SetDeadline(hd)
	r := bufio.NewReaderSize(conn, tcp.MaxFrame+1)
	line, e := readRecord(r, 1024)
	if e != nil {
		return e
	}
	var hello tcp.Handshake
	if tcp.StrictJSON(line, &hello) != nil || hello.V != 1 || hello.RID != s.request.RID || !tcp.Hex32(hello.ActionNonce) || time.Now().Unix() >= s.binding.Expires || !tcp.EqualMAC(hello.MAC, s.binding.HandshakeMAC(s.request.Key, "hello", hello.ActionNonce)) {
		return tcp.ErrProtocol
	}
	if !s.claimed.CompareAndSwap(false, true) {
		return tcp.ErrProtocol
	}
	s.listener.Close()
	ack := tcp.Handshake{V: 1, RID: hello.RID, ActionNonce: hello.ActionNonce, MAC: s.binding.HandshakeMAC(s.request.Key, "ack", hello.ActionNonce)}
	b, _ := json.Marshal(ack)
	b = append(b, '\n')
	if e = writeAll(conn, b); e != nil {
		return errAuthenticated
	}
	conn.SetDeadline(deadline)
	total := 0
	for seq := 1; seq <= tcp.MaxEvents; seq++ {
		line, e = readRecord(r, tcp.MaxFrame)
		if e != nil {
			return errAuthenticated
		}
		total += len(line)
		if total > tcp.MaxBytes {
			return errAuthenticated
		}
		var f tcp.Frame
		if tcp.StrictJSON(line, &f) != nil || f.V != 1 || f.RID != s.request.RID || f.Seq != seq || !tcp.EqualMAC(f.MAC, f.ExpectedMAC(s.request.Key)) {
			return errAuthenticated
		}
		terminal := f.Type == "result" || f.Type == "error" || f.Type == "cancelled"
		if f.Type != "progress" && !terminal {
			return errAuthenticated
		}
		payload, e := base64.RawURLEncoding.Strict().DecodeString(f.Payload)
		if e != nil || base64.RawURLEncoding.EncodeToString(payload) != f.Payload {
			return errAuthenticated
		}
		var value interface{}
		if tcp.StrictJSON(payload, &value) != nil {
			return errAuthenticated
		}
		if ctx.Err() != nil || time.Now().Unix() >= s.binding.Expires {
			return errAuthenticated
		}
		out := append([]byte(nil), line...)
		select {
		case s.data <- out:
		case <-ctx.Done():
			return errAuthenticated
		}
		if terminal {
			return nil
		}
	}
	return errAuthenticated
}
func writeAll(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, e := w.Write(b)
		if e != nil {
			return e
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}

func kbHeaders(r *http.Request) (map[string]string, error) {
	out := map[string]string{}
	for _, name := range []string{"Cookie", "Origin", "X-Csrf-Token"} {
		v := r.Header.Values(name)
		if len(v) > 1 {
			return nil, tcp.ErrProtocol
		}
		if len(v) == 1 {
			out[name] = v[0]
		}
	}
	// No inherited forward, host, hop-by-hop or authorization headers.
	out["Content-Type"] = "application/json"
	return out, nil
}

// Validate route components before embedding them in a trusted apihost URL.
func validKBRoute(namespace, action string) bool {
	for _, s := range strings.Split(namespace+"/"+action, "/") {
		if s == "" || s == "." || s == ".." {
			return false
		}
		for _, c := range s {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("_-@.", c)) {
				return false
			}
		}
	}
	return true
}

func serveKB(ctx context.Context, w http.ResponseWriter, r *http.Request, addr, target string, body map[string]interface{}, req tcp.Request) {
	ns, pkg, action := r.PathValue("ns"), r.PathValue("pkg"), r.PathValue("action")
	if pkg == "" {
		pkg = "default"
	}
	if !validKBRoute(ns, pkg+"/"+action) {
		http.Error(w, "Invalid action target", http.StatusBadRequest)
		return
	}
	headers, err := kbHeaders(r)
	if err != nil {
		http.Error(w, "Invalid headers", http.StatusBadRequest)
		return
	}
	// Browser-controlled callback metadata must not reach the protected action.
	delete(body, "STREAM_TOKEN")
	delete(body, "stream_binding")
	sock, err := setupKB(ctx, addr, ns+"/"+pkg+"/"+action, headers["Cookie"], body, req)
	if err != nil {
		http.Error(w, "Invalid stream request", http.StatusBadRequest)
		return
	}
	defer sock.listener.Close()
	encoded, err := json.Marshal(body)
	if err != nil {
		http.Error(w, "Invalid stream request", http.StatusBadRequest)
		return
	}
	upstream, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(encoded))
	if err != nil {
		http.Error(w, "Action unavailable", http.StatusBadGateway)
		return
	}
	for name, value := range headers {
		upstream.Header.Set(name, value)
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}
	go sock.run(ctx)
	result := make(chan invocationResult, 1)
	go func() { result <- invoke(upstreamClient, upstream) }()
	written := false
	for {
		select {
		case <-ctx.Done():
			if !written && r.Context().Err() == nil {
				http.Error(w, "Stream timed out", http.StatusGatewayTimeout)
			}
			return
		case outcome := <-result:
			result = nil
			if outcome.err != nil || outcome.status < 200 || outcome.status >= 300 {
				if !written {
					status := http.StatusBadGateway
					if outcome.status == 400 || outcome.status == 401 || outcome.status == 403 {
						status = outcome.status
					}
					http.Error(w, "Action unavailable", status)
				}
				return
			}
		case <-sock.errors:
			if !written {
				http.Error(w, "Stream failed", http.StatusBadGateway)
			}
			return
		case data, open := <-sock.data:
			if !open {
				if !written {
					http.Error(w, "Stream failed", http.StatusBadGateway)
				}
				return
			}
			if !written {
				w.Header().Set("Content-Type", "application/x-ndjson")
				w.Header().Set("Cache-Control", "no-store")
				w.Header().Set("X-Content-Type-Options", "nosniff")
				w.Header().Set("X-Accel-Buffering", "no")
			}
			_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(5 * time.Second))
			if _, err := w.Write(data); err != nil {
				return
			}
			written = true
			flusher.Flush()
		}
	}
}
