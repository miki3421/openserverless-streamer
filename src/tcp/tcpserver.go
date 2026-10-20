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
	"bufio"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

const handshakeTimeout = time.Second
const callbackIdleTimeout = 30 * time.Second
const handshakePrefix = "OPS-STREAM "
const maxPendingHandshakes = 32

type SocketsServer struct {
	ctx            context.Context
	listener       net.Listener
	Host           string
	Port           string
	Token          string
	StreamDataChan chan []byte
	Connected      chan struct{}
}

func SetupTcpServer(ctx context.Context, addr string) (*SocketsServer, error) {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", net.JoinHostPort(addr, "0"))
	if err != nil {
		return nil, errors.New("cannot create callback listener")
	}
	host, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		listener.Close()
		return nil, err
	}
	s := &SocketsServer{
		ctx: ctx, listener: listener, Host: host, Port: port,
		Token: hex.EncodeToString(secret), StreamDataChan: make(chan []byte),
		Connected: make(chan struct{}),
	}
	go s.WaitToCleanUp()
	go s.acceptConnections()
	return s, nil
}

func (s *SocketsServer) acceptConnections() {
	var mu sync.Mutex
	var workers sync.WaitGroup
	slots := make(chan struct{}, maxPendingHandshakes)
	pending := make(map[net.Conn]uint64)
	var sequence uint64
	claimed := false
	closePending := func() {
		for conn := range pending {
			delete(pending, conn)
			_ = conn.Close()
		}
	}
	defer func() {
		_ = s.listener.Close()
		mu.Lock()
		closePending()
		mu.Unlock()
		workers.Wait()
		close(s.StreamDataChan)
	}()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		// Keep accepting while authentication reads run independently. If the
		// bounded worker set is full, evict its oldest unauthenticated peer:
		// otherwise a queue of silent peers can lock out the valid producer.
		select {
		case slots <- struct{}{}:
		default:
			mu.Lock()
			var oldest net.Conn
			var order uint64
			for peer, started := range pending {
				if oldest == nil || started < order {
					oldest, order = peer, started
				}
			}
			if oldest != nil {
				delete(pending, oldest)
				_ = oldest.Close()
			}
			winner := claimed
			mu.Unlock()
			if winner {
				_ = conn.Close()
				return
			}
			select {
			case slots <- struct{}{}:
			case <-s.ctx.Done():
				_ = conn.Close()
				return
			}
		}
		mu.Lock()
		if claimed || s.ctx.Err() != nil {
			mu.Unlock()
			<-slots
			_ = conn.Close()
			return
		}
		sequence++
		pending[conn] = sequence
		mu.Unlock()
		workers.Add(1)
		go func(conn net.Conn) {
			defer workers.Done()
			defer func() { <-slots }()
			defer conn.Close()
			stopClose := context.AfterFunc(s.ctx, func() { _ = conn.Close() })
			defer stopClose()
			_ = conn.SetReadDeadline(time.Now().Add(handshakeTimeout))
			reader := bufio.NewReaderSize(conn, 256)
			preamble := make([]byte, len(handshakePrefix)+len(s.Token)+1)
			_, err := io.ReadFull(reader, preamble)
			expected := handshakePrefix + s.Token + "\n"
			mu.Lock()
			_, stillPending := pending[conn]
			delete(pending, conn)
			if err != nil || !stillPending || claimed || s.ctx.Err() != nil || subtle.ConstantTimeCompare(preamble, []byte(expected)) != 1 {
				mu.Unlock()
				return
			}
			claimed = true
			close(s.Connected)
			_ = s.listener.Close()
			closePending()
			mu.Unlock()
			// The same buffered reader owns any bytes read past the preamble.
			s.handleReader(conn, reader)
		}(conn)
	}
}

func (s *SocketsServer) handleConnection(conn net.Conn) {
	s.handleReader(conn, conn)
}

func (s *SocketsServer) handleReader(conn net.Conn, reader io.Reader) {
	defer conn.Close()
	stopClose := context.AfterFunc(s.ctx, func() { _ = conn.Close() })
	defer stopClose()
	for {
		if s.ctx.Err() != nil {
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(callbackIdleTimeout))
		// A fresh owned buffer avoids mutation after handing the slice to the HTTP
		// writer. Every send can be interrupted by cancellation.
		buf := make([]byte, 2048)
		n, err := reader.Read(buf)
		if n > 0 {
			select {
			case s.StreamDataChan <- buf[:n]:
			case <-s.ctx.Done():
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (s *SocketsServer) WaitToCleanUp() {
	<-s.ctx.Done()
	_ = s.listener.Close()
}

// CallbackPreamble documents the action-side wire format without logging it.
func CallbackPreamble(token string) string {
	return handshakePrefix + strings.TrimSpace(token) + "\n"
}
