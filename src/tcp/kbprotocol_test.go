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

package tcp

import (
	"strings"
	"testing"
)

func TestStrictKBJSON(t *testing.T) {
	for _, s := range []string{`{"a":1,"a":2}`, `{"a":{"x":1,"x":2}}`, `{} {}`, `{"a":"\ud800"}`, `{"a":"\udc00"}`, strings.Repeat("[", 34) + "0" + strings.Repeat("]", 34), string([]byte{'"', 255, '"'})} {
		var x interface{}
		if StrictJSON([]byte(s), &x) == nil {
			t.Fatalf("accepted invalid JSON case")
		}
	}
	for _, s := range []string{`{"a":"\ud83d\ude00"}`, `{"a":"è 漢字"}`, `{"a":"\\ud800"}`, `[]`} {
		var x interface{}
		if StrictJSON([]byte(s), &x) != nil {
			t.Fatalf("rejected valid JSON %s", s)
		}
	}
}
func TestCanonicalUnambiguous(t *testing.T) {
	if string(LP("ab", "c")) == string(LP("a", "bc")) {
		t.Fatal("ambiguous")
	}
	key := strings.Repeat("ab", 32)
	b := Binding{V: 1, RID: strings.Repeat("01", 32), Route: "tenant/kb/ask-stream", Session: Digest([]byte("synthetic-cookie")), Input: Digest(LP("Perché?", "[]")), Host: "127.0.0.1", Port: "32000", Expires: 1900000123, RelayNonce: strings.Repeat("02", 32)}
	a := strings.Repeat("03", 32)
	if EqualMAC(b.HandshakeMAC(key, "hello", a), b.HandshakeMAC(key, "ack", a)) {
		t.Fatal("reflection")
	}
	if Digest(b.Canonical()) != "93cc7977ad6a0cb8cc45f1ff5be57fdf849811b5161389192b4d64ba48956692" {
		t.Fatalf("binding vector %s", Digest(b.Canonical()))
	}
	if b.HandshakeMAC(key, "hello", a) != "e3cb9ce0a507e8175aae811125cf7a1b0e0a2839ae72cb251ef787981f26b165" {
		t.Fatal("hello vector")
	}
	if b.HandshakeMAC(key, "ack", a) != "f776203270ad4c775fc8bf0395cc3a0b6fe91a74a4a49b0f10068625e9098e6a" {
		t.Fatal("ack vector")
	}
	f := Frame{V: 1, RID: b.RID, Seq: 1, Type: "result", Payload: "eyJhbnN3ZXIiOiJzaW50ZXRpY28ifQ"}
	if f.ExpectedMAC(key) != "439338fa3dac0020411f45f1aa4083769a1cced4578410a90f803fcc644eba16" {
		t.Fatal("frame vector")
	}
}
