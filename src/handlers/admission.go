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
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

const maxActiveStreams = 64
const maxTenantStreams = 8
const maxAnonymousStreams = 32
const maxAnonymousPeerStreams = 16

// Identity lookup has a short-lived bound, separate from stream capacity.
// Request bodies are read only after applying the verified principal quota.
var identitySlots = make(chan struct{}, 16)
var streams = streamAdmission{buckets: make(map[string]int)}

type streamAdmission struct {
	mu        sync.Mutex
	active    int
	anonymous int
	buckets   map[string]int
}

func (a *streamAdmission) acquire(namespace, principal, peer string) (func(), bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	keys := []string{"target:" + namespace}
	limits := []int{maxTenantStreams}
	if principal != "" {
		keys = append(keys, "principal:"+principal)
		limits = append(limits, maxTenantStreams)
	} else {
		keys = append(keys, "peer:"+peer)
		limits = append(limits, maxAnonymousPeerStreams)
	}
	if a.active >= maxActiveStreams || (principal == "" && a.anonymous >= maxAnonymousStreams) {
		return nil, false
	}
	for i, key := range keys {
		if a.buckets[key] >= limits[i] {
			return nil, false
		}
	}
	a.active++
	if principal == "" {
		a.anonymous++
	}
	for _, key := range keys {
		a.buckets[key]++
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			a.mu.Lock()
			defer a.mu.Unlock()
			a.active--
			if principal == "" {
				a.anonymous--
			}
			for _, key := range keys {
				a.buckets[key]--
				if a.buckets[key] == 0 {
					delete(a.buckets, key)
				}
			}
		})
	}, true
}

func requestPeer(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return "unknown"
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.String()
	}
	return "unknown"
}

// OpenWhisk, not a claimed URL namespace or arbitrary Authorization text,
// establishes the principal. These are capacity checks only: the action's own
// upstream authorization still runs for every invocation. Web actions using
// custom credentials remain supported through the bounded anonymous pool.
func streamPrincipal(ctx context.Context, host, credential string) (string, int) {
	if !strings.Contains(credential, ":") {
		return "", http.StatusUnauthorized
	}
	select {
	case identitySlots <- struct{}{}:
		defer func() { <-identitySlots }()
	default:
		return "", http.StatusTooManyRequests
	}
	u, err := url.Parse(ensureProtocolScheme(host))
	if err != nil || u.Host == "" || u.User != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return "", http.StatusBadGateway
	}
	u.Path = strings.TrimSuffix(u.Path, "/") + "/api/v1/namespaces"
	u.RawPath, u.RawQuery, u.Fragment = "", "", ""
	lookup, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(lookup, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", http.StatusBadGateway
	}
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(credential)))
	reply, err := upstreamClient.Do(req)
	if err != nil {
		return "", http.StatusBadGateway
	}
	defer reply.Body.Close()
	if reply.StatusCode != http.StatusOK {
		if reply.StatusCode == 401 || reply.StatusCode == 403 || reply.StatusCode == 429 {
			return "", reply.StatusCode
		}
		return "", http.StatusBadGateway
	}
	data, err := io.ReadAll(io.LimitReader(reply.Body, (32<<10)+1))
	var namespaces []string
	if err != nil || len(data) > 32<<10 || json.Unmarshal(data, &namespaces) != nil || len(namespaces) == 0 {
		return "", http.StatusBadGateway
	}
	for _, namespace := range namespaces {
		if namespace == "" {
			return "", http.StatusBadGateway
		}
	}
	sort.Strings(namespaces)
	canonical, _ := json.Marshal(namespaces)
	return fmt.Sprintf("%x", sha256.Sum256(canonical)), 0
}
