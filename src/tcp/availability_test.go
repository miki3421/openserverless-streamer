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
	"sync"
	"testing"
	"time"
)

func TestQueuedSlowPeersCannotDelayValidHandshake(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := SetupTcpServer(ctx, "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	var peers []net.Conn
	defer func() {
		for _, peer := range peers {
			peer.Close()
		}
	}()
	for i := 0; i < 64; i++ {
		peer := dial(t, s)
		peer.Write([]byte("O"))
		peers = append(peers, peer)
	}
	good := dial(t, s)
	defer good.Close()
	started := time.Now()
	good.Write([]byte(CallbackPreamble(s.Token) + "valid\n"))
	select {
	case data := <-s.StreamDataChan:
		if string(data) != "valid\n" {
			t.Fatal("wrong producer selected")
		}
		if time.Since(started) >= 750*time.Millisecond {
			t.Fatal("valid producer queued behind slow peers")
		}
	case <-time.After(750 * time.Millisecond):
		t.Fatal("valid producer queued behind slow peers")
	}
}

func TestOnlyOneConcurrentValidPeerWins(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := SetupTcpServer(ctx, "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{})
	var senders sync.WaitGroup
	for _, payload := range []string{"one\n", "two\n"} {
		conn := dial(t, s)
		senders.Add(1)
		go func(conn net.Conn, payload string) {
			defer senders.Done()
			defer conn.Close()
			<-gate
			conn.Write([]byte(CallbackPreamble(s.Token) + payload))
		}(conn, payload)
	}
	close(gate)
	var all []byte
	deadline := time.After(time.Second)
	for {
		select {
		case data, ok := <-s.StreamDataChan:
			if !ok {
				senders.Wait()
				if string(all) != "one\n" && string(all) != "two\n" {
					t.Fatalf("multiple winners: %q", all)
				}
				return
			}
			all = append(all, data...)
		case <-deadline:
			t.Fatal("winning peer did not finish")
		}
	}
}

func TestCancellationClosesAllPendingHandshakes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s, err := SetupTcpServer(ctx, "127.0.0.1")
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	var peers []net.Conn
	for i := 0; i < 32; i++ {
		peers = append(peers, dial(t, s))
	}
	cancel()
	for _, peer := range peers {
		peer.SetReadDeadline(time.Now().Add(time.Second))
		_, err := io.ReadAll(peer)
		peer.Close()
		if failure, ok := err.(net.Error); ok && failure.Timeout() {
			t.Fatal("pending peer leaked")
		}
	}
	select {
	case _, open := <-s.StreamDataChan:
		if open {
			t.Fatal("unexpected stream bytes")
		}
	case <-time.After(time.Second):
		t.Fatal("pending authentication workers leaked")
	}
}
