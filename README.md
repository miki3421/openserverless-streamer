<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Apache OpenServerless Streamer (incubating)

The OpenServerless streamer is a tool to relay a stream from OpenWhisk actions 
to an outside HTTP client.

The streamer is a simple HTTP server that exposes an endpoint 
/stream/{namespace}/{action} to invoke the relative OpenWhisk action, open a 
socket for the action to write to, and relay the output to the client.

It expects 2 environment variables to be set:
- `OW_APIHOST`: the OpenWhisk API host
- `STREAMER_ADDR`: the address of the streamer server for the OpenWhisk actions to connect to

Other environment variables can be set to configure the streamer:

- `HTTP_SERVER_PORT`: the port the streamer server listens on (default: 80)
  
Cors handling is handled through these variables:

- `CORS_ENABLED`: set to 1 or true to enable the CORS handler
- `CORS_ALLOW_ORIGIN`: this defaults to `*`
- `CORS_ALLOW_METHODS`: this defaults to `GET,POST,OPTIONS`
- `CORS_ALLOW_HEADERS`: this defaults to `Authorization,Content-Type`


## Endpoints

The streamer exposes the following endpoints (use POST in case you need to send 
arguments to the action):

- `GET/POST /action/{namespace}/{action}`: to invoke the OpenWhisk action on the 
given namespace, default package, and action name. It requires an Authorization
header with Bearer token with the OpenWhisk AUTH token.

- `GET/POST /action/{namespace}/{package}/{action}`: to invoke the OpenWhisk 
action on the given namespace, custom package, and action name. It requires an 
Authorization header with Bearer token with the OpenWhisk AUTH token

- `GET/POST /web/{namespace}/{action}`: to invoke an OpenWhisk web action on the 
given namespace, default package, and action name.

- `GET/POST /web/{namespace}/{package}/{action}`: to invoke an OpenWhisk web 
action on the given namespace, custom package, and action name.

## Authenticated callback contract

The patched proxy injects `STREAM_HOST`, `STREAM_PORT`, and a random
`STREAM_TOKEN` into every invoked action. Before sending output on the TCP
connection, the action must send the ASCII bytes `OPS-STREAM `, followed by
the 64-character token and a newline. The proxy consumes this preamble and
forwards only subsequent bytes to the HTTP client. Keep the token out of logs
and responses. An incorrect token does not claim the listener. There is no
unauthenticated callback fallback on the patched server.

For Python, after opening the callback socket:

```python
connection.sendall(("OPS-STREAM " + args["STREAM_TOKEN"] + "\n").encode())
connection.sendall(b'{"event":"synthetic"}\n')
```

Existing streaming actions must add this handshake before switching to the
patched image. An action supporting both server generations may send the
preamble only when the server supplies `STREAM_TOKEN`; the patched server
always supplies it. The bundled example and Alpha/Beta validation action
implement this transition. The TCP payload is forwarded byte-for-byte, so
actions using NDJSON must supply their own newline delimiters.

Requests are bounded to 1 MiB of JSON, 64 active streams per proxy process,
20 seconds to authenticate the callback, 30 seconds of callback inactivity,
and five minutes total lifetime. A saturated process returns 429, oversized
input returns 413, and a missing callback returns 504. Upstream 401/403/404/429
are retained; upstream failures use a sanitized 502 response. Request
cancellation closes both the upstream request and callback resources.

## Building

Check the [DEVEL.md](https://github.com/apache/openserverless/blob/main/DEVEL.md)
in the [apache/openserverless](https://github.com/apache/openserverless) repository.
