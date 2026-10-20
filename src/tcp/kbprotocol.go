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

// The KB protocol is intentionally separate from the legacy byte transport.
import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"unicode/utf8"
)

const MaxFrame = 65536
const MaxBytes = 262144
const MaxEvents = 128

var ErrProtocol = errors.New("invalid KB stream protocol")

type Request struct {
	V   int    `json:"v"`
	RID string `json:"rid"`
	Key string `json:"key"`
}
type Binding struct {
	V          int    `json:"v"`
	RID        string `json:"rid"`
	Route      string `json:"route"`
	Session    string `json:"session"`
	Input      string `json:"input"`
	Host       string `json:"host"`
	Port       string `json:"port"`
	Expires    int64  `json:"expires"`
	RelayNonce string `json:"relay_nonce"`
}
type Handshake struct {
	V           int    `json:"v"`
	RID         string `json:"rid"`
	ActionNonce string `json:"action_nonce"`
	MAC         string `json:"mac"`
}
type Frame struct {
	V       int    `json:"v"`
	RID     string `json:"rid"`
	Seq     int    `json:"seq"`
	Type    string `json:"type"`
	Payload string `json:"payload"`
	MAC     string `json:"mac"`
}

func LP(parts ...string) []byte {
	var b bytes.Buffer
	for _, p := range parts {
		binary.Write(&b, binary.BigEndian, uint32(len(p)))
		b.WriteString(p)
	}
	return b.Bytes()
}
func Digest(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }
func Nonce() (string, error) {
	var b [32]byte
	_, e := rand.Read(b[:])
	return hex.EncodeToString(b[:]), e
}
func Hex32(s string) bool {
	if len(s) != 64 {
		return false
	}
	b, e := hex.DecodeString(s)
	return e == nil && hex.EncodeToString(b) == s
}
func MAC(key string, data []byte) string {
	k, _ := hex.DecodeString(key)
	h := hmac.New(sha256.New, k)
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}
func EqualMAC(a, b string) bool { return Hex32(a) && Hex32(b) && hmac.Equal([]byte(a), []byte(b)) }
func (b Binding) Canonical() []byte {
	return LP("1", b.RID, b.Route, b.Session, b.Input, b.Host, b.Port, strconv.FormatInt(b.Expires, 10), b.RelayNonce)
}
func (b Binding) HandshakeMAC(key, domain, nonce string) string {
	return MAC(key, LP("kb-stream/v1/"+domain, string(b.Canonical()), nonce))
}
func (f Frame) ExpectedMAC(key string) string {
	return MAC(key, LP("kb-stream/v1/frame", f.RID, strconv.Itoa(f.Seq), f.Type, f.Payload))
}

// StrictJSON rejects duplicate keys at every depth, trailing values, invalid UTF8,
// and malformed surrogate escapes before the standard decoder can replace them.
func StrictJSON(data []byte, out interface{}) error {
	if !utf8.Valid(data) || !validSurrogates(data) {
		return ErrProtocol
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	if err := walkJSON(d, 0); err != nil {
		return ErrProtocol
	}
	if _, err := d.Token(); err != io.EOF {
		return ErrProtocol
	}
	var names []string
	switch out.(type) {
	case *Request:
		names = []string{"v", "rid", "key"}
	case *Binding:
		names = []string{"v", "rid", "route", "session", "input", "host", "port", "expires", "relay_nonce"}
	case *Handshake:
		names = []string{"v", "rid", "action_nonce", "mac"}
	case *Frame:
		names = []string{"v", "rid", "seq", "type", "payload", "mac"}
	}
	if names != nil {
		var m map[string]json.RawMessage
		if json.Unmarshal(data, &m) != nil || len(m) != len(names) {
			return ErrProtocol
		}
		for _, name := range names {
			if _, ok := m[name]; !ok {
				return ErrProtocol
			}
		}
	}
	d = json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return ErrProtocol
	}
	return nil
}
func walkJSON(d *json.Decoder, depth int) error {
	if depth > 32 {
		return ErrProtocol
	}
	t, e := d.Token()
	if e != nil {
		return e
	}
	delim, ok := t.(json.Delim)
	if !ok {
		return nil
	}
	if delim == '{' {
		seen := map[string]bool{}
		for d.More() {
			k, e := d.Token()
			if e != nil {
				return e
			}
			s, ok := k.(string)
			if !ok || seen[s] {
				return ErrProtocol
			}
			seen[s] = true
			if e = walkJSON(d, depth+1); e != nil {
				return e
			}
		}
	} else if delim == '[' {
		for d.More() {
			if e = walkJSON(d, depth+1); e != nil {
				return e
			}
		}
	} else {
		return ErrProtocol
	}
	_, e = d.Token()
	return e
}
func validSurrogates(b []byte) bool {
	for i := 0; i < len(b); i++ {
		if b[i] != '\\' {
			continue
		}
		i++
		if i >= len(b) {
			return false
		}
		if b[i] != 'u' {
			continue
		}
		if i+4 >= len(b) {
			return false
		}
		v, e := strconv.ParseUint(string(b[i+1:i+5]), 16, 16)
		if e != nil {
			return false
		}
		i += 4
		if v >= 0xDC00 && v <= 0xDFFF {
			return false
		}
		if v >= 0xD800 && v <= 0xDBFF {
			if i+6 >= len(b) || b[i+1] != '\\' || b[i+2] != 'u' {
				return false
			}
			w, e := strconv.ParseUint(string(b[i+3:i+7]), 16, 16)
			if e != nil || w < 0xDC00 || w > 0xDFFF {
				return false
			}
			i += 6
		}
	}
	return true
}
