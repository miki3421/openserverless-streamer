<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# OPS Advanced — openserverless-streamer

Personal development branch maintained by miki3421, based on Apache OpenServerless 0.9.0. This is not an Apache release.

The `advanced` branch belongs to https://github.com/miki3421/openserverless-streamer.
Inherited GitHub Actions workflows remain archived under `.github/upstream-workflows/`.
The personal `advanced-image.yml` workflow publishes only from this repository's
`advanced` branch to `ghcr.io/miki3421/openserverless-streamer`, using a full-commit
image tag. It runs Go race tests and vet before publishing a Linux amd64 image.
It has no Git write permission and creates no tags, releases, or Apache images.

The callback authentication change requires a client-first rollout: update
streaming actions to send `OPS-STREAM <STREAM_TOKEN>\n`, and delimit every JSON
record with a newline before updating the shared Streamer. Never restore an
unauthenticated callback fallback. The browser must buffer incomplete records
across HTTP chunks.

This candidate includes the four original Streamer fixes reviewed on 19 October
2026. Two availability findings remain open: one caller can consume all 64
global stream slots, and partial callback handshakes are processed serially.
The image is a personal compatibility/security candidate, not a claim of complete
tenant-isolation hardening or an Apache release. See the TruChat fork's
`docs/REMOTE_CODEX.md` for installation, acceptance, limits and rollback.

See https://github.com/miki3421/openserverless/blob/advanced/ADVANCED.md for the component plan and validation status.

An authenticated KB-specific mode has been evaluated but is not implemented in
this image. See [KB_STREAMING_PROPOSAL.md](KB_STREAMING_PROPOSAL.md) for the
compatibility evidence, proposed mode boundary and acceptance gates.
