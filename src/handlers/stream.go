// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package handlers

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/apache/openserverless-streaming-proxy/tcp"
)

const maxBodyBytes = 1 << 20
const streamStartTimeout = 20 * time.Second
const maxStreamLifetime = 5 * time.Minute

var upstreamClient = &http.Client{
	Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: maxStreamLifetime,
		IdleConnTimeout:       30 * time.Second,
		MaxIdleConns:          64,
		MaxIdleConnsPerHost:   16,
		MaxConnsPerHost:       64,
	},
	Timeout:       maxStreamLifetime,
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
}

type invocationResult struct {
	status int
	err    error
}

func streamHandler(addr, host string, web bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var credential string
		if !web {
			var err error
			credential, err = extractAuthToken(r)
			if err != nil {
				http.Error(w, "Authorization required", http.StatusUnauthorized)
				return
			}
		} else {
			credential, _ = extractAuthToken(r)
		}
		target, err := actionURL(host, r, web)
		if err != nil {
			http.Error(w, "Invalid action target", http.StatusBadRequest)
			return
		}
		lifetime := maxStreamLifetime
		if web {
			lifetime = streamTimeout
		}
		ctx, cancel := context.WithTimeout(r.Context(), lifetime)
		defer cancel()
		if web {
			select {
			case webStreamSlots <- struct{}{}:
				defer func() { cancel(); <-webStreamSlots }()
			default:
				r.Body.Close()
				http.Error(w, "Stream unavailable", http.StatusServiceUnavailable)
				return
			}
		}
		principal, identityStatus := streamPrincipal(ctx, host, credential)
		if !web && identityStatus != 0 {
			http.Error(w, http.StatusText(identityStatus), identityStatus)
			return
		}
		quotaNamespace := r.PathValue("ns")
		if !web && quotaNamespace == "_" && principal != "" {
			// OpenWhisk resolves this alias relative to the verified caller.
			// It must not create a shared target bucket across all tenants.
			quotaNamespace = "private-default:" + principal
		}
		release, admitted := streams.acquire(quotaNamespace, principal, requestPeer(r))
		if !admitted {
			http.Error(w, "Too many active streams", http.StatusTooManyRequests)
			return
		}
		defer release()
		// Quotas also cover slow/chunked input; no callback listener or action
		// invocation is allocated until the bounded JSON object is decoded.
		var body map[string]interface{}
		var kb *tcp.Request
		if web {
			body, kb, err = parseBodyWithContext(ctx, w, r)
		} else {
			r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
			body, err = injectHostPortInBody(r, "", "")
		}
		if err != nil {
			var tooLarge *http.MaxBytesError
			status := http.StatusBadRequest
			if errors.As(err, &tooLarge) {
				status = http.StatusRequestEntityTooLarge
			} else if web && ctx.Err() != nil {
				status = http.StatusRequestTimeout
			}
			http.Error(w, http.StatusText(status), status)
			return
		}
		if kb != nil {
			serveKB(ctx, w, r, addr, target, body, *kb)
			return
		}
		sock, err := tcp.SetupTcpServer(ctx, addr)
		if err != nil {
			http.Error(w, "Callback unavailable", http.StatusServiceUnavailable)
			return
		}
		body["STREAM_HOST"], body["STREAM_PORT"], body["STREAM_TOKEN"] = sock.Host, sock.Port, sock.Token
		encoded, err := json.Marshal(body)
		if err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		upstream, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(encoded))
		if err != nil {
			http.Error(w, "Invalid upstream configuration", http.StatusBadGateway)
			return
		}
		if web {
			copyActionHeaders(upstream.Header, r.Header)
		} else {
			upstream.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(credential)))
		}
		upstream.Header.Set("Content-Type", "application/json")
		result := make(chan invocationResult, 1)
		// The producer owns this buffered, single-result channel. It is never
		// closed by the consumer and cannot block after consumer cancellation.
		go func() { result <- invoke(upstreamClient, upstream) }()
		start := time.NewTimer(streamStartTimeout)
		defer start.Stop()
		connected := sock.Connected
		var startDeadline <-chan time.Time = start.C
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
			return
		}
		written := false
		for {
			select {
			case <-connected:
				connected = nil
				start.Stop()
				startDeadline = nil
			case <-startDeadline:
				if !written {
					http.Error(w, "Stream callback timed out", http.StatusGatewayTimeout)
				}
				return
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
						if outcome.status == 401 || outcome.status == 403 || outcome.status == 404 || outcome.status == 429 {
							status = outcome.status
						}
						http.Error(w, http.StatusText(status), status)
					}
					return
				}
				// A 202 response only acknowledges asynchronous invocation.
				// Callback establishment still has its independent deadline.
			case data, open := <-sock.StreamDataChan:
				if !open {
					return
				}
				if !written {
					w.Header().Set("Content-Type", "application/x-ndjson")
					w.Header().Set("Cache-Control", "no-store")
					w.Header().Set("X-Accel-Buffering", "no")
				}
				// Bound a stalled downstream write without imposing a total
				// server WriteTimeout that would truncate healthy streams.
				_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(10 * time.Second))
				if _, err := w.Write(data); err != nil {
					return
				}
				written = true
				flusher.Flush()
			}
		}
	}
}

func invoke(client *http.Client, req *http.Request) invocationResult {
	reply, err := client.Do(req)
	if err != nil {
		return invocationResult{err: err}
	}
	defer reply.Body.Close()
	// No need to retain action output: stream data travels on the callback.
	// Bound the drain rather than reading arbitrary response bodies forever.
	_, _ = io.Copy(io.Discard, io.LimitReader(reply.Body, 32<<10))
	return invocationResult{status: reply.StatusCode}
}

func actionURL(host string, r *http.Request, web bool) (string, error) {
	u, err := url.Parse(ensureProtocolScheme(host))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
		return "", errors.New("invalid upstream")
	}
	ns, pkg, action := r.PathValue("ns"), r.PathValue("pkg"), r.PathValue("action")
	if ns == "" || action == "" {
		return "", errors.New("missing target")
	}
	parts := []string{ns}
	prefix := "/api/v1/namespaces/"
	if web {
		prefix = "/api/v1/web/"
		if pkg == "" {
			pkg = "default"
		}
	} else {
		parts = append(parts, "actions")
	}
	if pkg != "" {
		parts = append(parts, pkg)
	}
	parts = append(parts, action)
	for i, value := range parts {
		if value == "." || value == ".." || strings.ContainsAny(value, "/\\\x00\r\n") {
			return "", errors.New("invalid path segment")
		}
		parts[i] = url.PathEscape(value)
	}
	raw := strings.TrimSuffix(u.EscapedPath(), "/") + prefix + strings.Join(parts, "/")
	u.Path, err = url.PathUnescape(raw)
	u.RawPath, u.RawQuery, u.Fragment = raw, "", ""
	if !web {
		u.RawQuery = "blocking=false&result=false"
	}
	return u.String(), err
}

func copyActionHeaders(dst, src http.Header) {
	blocked := map[string]bool{"Connection": true, "Proxy-Connection": true, "Keep-Alive": true, "Proxy-Authenticate": true, "Proxy-Authorization": true, "Te": true, "Trailer": true, "Transfer-Encoding": true, "Upgrade": true, "Content-Length": true, "Content-Type": true, "Host": true}
	for _, entry := range src.Values("Connection") {
		for _, name := range strings.Split(entry, ",") {
			blocked[http.CanonicalHeaderKey(strings.TrimSpace(name))] = true
		}
	}
	for name, values := range src {
		if !blocked[http.CanonicalHeaderKey(name)] {
			dst[name] = append([]string(nil), values...)
		}
	}
}

// Kept for the existing package tests/callers; the handler uses invoke directly
// with its own context. The producer never closes the caller-owned channel.
func asyncPostWebAction(errChan chan error, target string, body []byte, headers map[string]string) {
	ctx, cancel := context.WithTimeout(context.Background(), maxStreamLifetime)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ensureProtocolScheme(target), bytes.NewReader(body))
	if err == nil {
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		outcome := invoke(upstreamClient, req)
		err = outcome.err
		if err == nil && (outcome.status < 200 || outcome.status >= 300) {
			err = fmt.Errorf("not ok (%d %s)", outcome.status, http.StatusText(outcome.status))
		}
	}
	if err != nil {
		select {
		case errChan <- err:
		case <-ctx.Done():
		}
	}
}
