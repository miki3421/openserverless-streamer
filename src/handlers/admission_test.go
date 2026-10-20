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
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAnonymousPoolLeavesHalfCapacityAndReleasesAllCounters(t *testing.T) {
	a := streamAdmission{buckets: make(map[string]int)}
	var releases []func()
	for i := 0; i < maxAnonymousStreams; i++ {
		release, ok := a.acquire(fmt.Sprint(i), "", fmt.Sprint(i))
		if !ok {
			t.Fatal("anonymous pool unexpectedly full")
		}
		releases = append(releases, release)
	}
	if _, ok := a.acquire("extra", "", "extra"); ok {
		t.Fatal("anonymous pool escaped its bound")
	}
	for i := maxAnonymousStreams; i < maxActiveStreams; i++ {
		release, ok := a.acquire(fmt.Sprint(i), fmt.Sprint(i), "same-proxy")
		if !ok {
			t.Fatal("verified capacity was not reserved")
		}
		releases = append(releases, release)
	}
	if _, ok := a.acquire("overflow", "overflow", "same-proxy"); ok {
		t.Fatal("global bound escaped")
	}
	for _, release := range releases {
		release()
		release()
	}
	if a.active != 0 || a.anonymous != 0 || len(a.buckets) != 0 {
		t.Fatal("admission counters leaked")
	}
}

func TestVerifiedPrincipalUsesCanonicalNamespacesNotCredentialText(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, _, _ := r.BasicAuth()
		if r.Method != "GET" || r.URL.Path != "/api/v1/namespaces" {
			t.Error("unexpected identity endpoint")
		}
		switch user {
		case "old-key":
			fmt.Fprint(w, `["beta","alpha"]`)
		case "rotated-key":
			fmt.Fprint(w, `["alpha","beta"]`)
		default:
			w.WriteHeader(http.StatusUnauthorized)
		}
	}))
	defer server.Close()
	first, status := streamPrincipal(context.Background(), server.URL, "old-key:secret")
	if status != 0 || first == "" {
		t.Fatal("valid identity not resolved")
	}
	second, status := streamPrincipal(context.Background(), server.URL, "rotated-key:other")
	if status != 0 || second != first {
		t.Fatal("credential rotation or response ordering bypassed identity quota")
	}
	for _, credential := range []string{"forged:key", "not-an-api-key"} {
		principal, status := streamPrincipal(context.Background(), server.URL, credential)
		if principal != "" || status != http.StatusUnauthorized {
			t.Fatal("unverified credential acquired a principal")
		}
	}
}
