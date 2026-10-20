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

The published image in `COMPATIBILITY_IMAGE.md` includes the four original
Streamer fixes reviewed on 19 October 2026. Its two availability findings remain
part of that immutable image: global-slot starvation by one caller and serial
processing of partial callback handshakes.

The 20 October availability patch (SHA-256
`f9dda540474080c08f52fe90fc89eecd39a8ec79368af961ee1d5cdfc0587ae8`)
is integrated locally on `kb-streaming`. It addresses those findings with
controller-verified per-caller quotas, target/anonymous quotas and bounded
concurrent callback authentication. See `README.md` for the changed admission
limits and deployment tradeoffs. The `OPS-STREAM <STREAM_TOKEN>\n` preamble and
the private action time/body bounds remain unchanged. The web path receives
the shared 32-request, 64 KiB and 130-second KB contract in this branch.

The supplied patch reports original-trigger negative controls against commit
`663a7fd3f31580f2f1c40820135d3c3338edab55`, Go race/vet checks, parallel/slow
callback peers, credential/route aliases, custom web authorization, quota
recovery, TruChat protocol/parser/proxy smoke tests and Alpha/Beta on the
existing K3s lab. The TruChat smoke uses the actual client and proxy with
synthetic AI/controller responses; it is not a full deployed-model test.
The source owner reported a local image
`localhost/ops-streamer-availability:20261020-local2`; this checkout has not
built that image. This integration passed local Go tests, race tests and vet.
It has not been published or independently reviewed by Daybreak.
This candidate is not a claim of complete tenant-isolation hardening or an
Apache release. The TruChat fork's `docs/REMOTE_CODEX.md` still describes the
previously published image, its limits and rollback.

See https://github.com/miki3421/openserverless/blob/advanced/ADVANCED.md for the component plan and validation status.

An authenticated KB mode is implemented locally on `kb-streaming`; it is absent
from the published image. See [KB_STREAMING_PROPOSAL.md](KB_STREAMING_PROPOSAL.md)
for compatibility evidence, local test results and remaining release gates.
