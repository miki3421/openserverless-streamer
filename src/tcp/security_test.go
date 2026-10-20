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

package tcp

import (
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func dial(t *testing.T, s *SocketsServer) net.Conn {
	t.Helper()
	c, e := net.DialTimeout("tcp", net.JoinHostPort(s.Host, s.Port), time.Second)
	if e != nil {
		t.Fatal(e)
	}
	return c
}

func TestWrongAndPeerTokensDoNotConsumeVictimStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	victim, e := SetupTcpServer(ctx, "127.0.0.1")
	if e != nil {
		t.Fatal(e)
	}
	peer, e := SetupTcpServer(ctx, "127.0.0.1")
	if e != nil {
		t.Fatal(e)
	}
	for _, token := range []string{strings.Repeat("0", 64), peer.Token} {
		conn := dial(t, victim)
		conn.SetDeadline(time.Now().Add(2 * time.Second))
		conn.Write([]byte(CallbackPreamble(token)))
		io.ReadAll(conn)
		conn.Close()
		select {
		case <-victim.Connected:
			t.Fatal("unauthenticated callback selected")
		default:
		}
	}
	good := dial(t, victim)
	defer good.Close()
	good.Write([]byte(CallbackPreamble(victim.Token) + "synthetic\n"))
	select {
	case data := <-victim.StreamDataChan:
		if string(data) != "synthetic\n" {
			t.Fatal("wrong bytes")
		}
	case <-time.After(time.Second):
		t.Fatal("correct producer rejected")
	}
}

func TestSlowHandshakeExpiresAndCorrectProducerStillWorks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, e := SetupTcpServer(ctx, "127.0.0.1")
	if e != nil {
		t.Fatal(e)
	}
	conn := dial(t, s)
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	conn.Write([]byte("O"))
	io.ReadAll(conn)
	good := dial(t, s)
	defer good.Close()
	good.Write([]byte(CallbackPreamble(s.Token) + "ok"))
	select {
	case data := <-s.StreamDataChan:
		if string(data) != "ok" {
			t.Fatal("wrong bytes")
		}
	case <-time.After(time.Second):
		t.Fatal("listener consumed by slow peer")
	}
}

func TestBlockedProducerAndReaderExitOnCancel(t *testing.T) {
	for _, send := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		s, e := SetupTcpServer(ctx, "127.0.0.1")
		if e != nil {
			t.Fatal(e)
		}
		conn := dial(t, s)
		conn.Write([]byte(CallbackPreamble(s.Token)))
		<-s.Connected
		if send {
			conn.Write([]byte("blocked without consumer"))
			time.Sleep(10 * time.Millisecond)
		}
		cancel()
		deadline := time.After(time.Second)
		for {
			select {
			case _, ok := <-s.StreamDataChan:
				if !ok {
					goto closed
				}
			case <-deadline:
				t.Fatal("producer leaked after cancel")
			}
		}
	closed:
		conn.Close()
	}
}
