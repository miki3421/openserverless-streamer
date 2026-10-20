<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Personal Streamer compatibility image

Published on 20 October 2026 from commit
`a6344fe715a5605f0601da6a982abcc920b5cc8e`, branch `advanced`.
This is a personal candidate, not an Apache release.

```text
ghcr.io/miki3421/openserverless-streamer@sha256:f8375da5f04d53878cfab9eeaf068addc3d2673cc897d06bebfc01c629dc71f1
```

- Platform: `linux/amd64` only.
- Source-specific tag: `advanced-a6344fe715a5605f0601da6a982abcc920b5cc8e`.
- [Successful publication and Go race/vet checks](https://github.com/miki3421/openserverless-streamer/actions/runs/39391090851).
- The registry manifest was fetched anonymously; its SHA-256 matches the
  registry digest header. No pull secret was required for that check.
- The accompanying client is [miki3421/truchat, advanced](https://github.com/miki3421/truchat/tree/advanced).
- [Remote installation, acceptance and rollback](https://github.com/miki3421/truchat/blob/advanced/docs/REMOTE_CODEX.md).

Upgrade every streaming action before replacing a shared Streamer. The server
requires the per-request capability preamble and forwards raw payload bytes;
NDJSON producers supply their own delimiters. Updated clients may omit the
preamble only when the old server does not supply `STREAM_TOKEN`.

Known limits remain: 1 MiB request JSON, 20 seconds to start/authenticate a
callback, one second to send its preamble, 30 seconds idle, five minutes total,
and 64 global stream slots. Two availability findings remain open: global-slot
starvation by one caller, and serial handling of partial TCP handshakes.
Do not interpret this publication as a complete multi-tenant security clearance.

Local verification includes six Python protocol tests, six browser-parser
tests (including all split positions and UTF-8), frontend build/type checks,
Go race/vet, and three real-proxy synthetic transport scenarios. The smoke
controller and AI are simulated; a full TruChat/OpenWhisk/model deployment on
the destination server still requires the acceptance procedure above.

## Unpublished follow-up

The local 20 October availability changes are **not** included in the digest
above. They add capacity isolation and concurrent callback authentication;
see `README.md` and `ADVANCED.md`. No replacement image has been published.
Do not use this historical artifact record as evidence that those two
availability findings are fixed on another installation.
