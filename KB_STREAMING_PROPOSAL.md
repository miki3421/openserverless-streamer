# Proposal: authenticated KB stream mode on `advanced`

Status: proposed, 20 October 2026. This document is a design and verification
record. It does not change the proxy protocol, publish an image, or authorize a
shared Streamer rollout.

## Baseline and observed compatibility

- Streamer source reviewed: `a6344fe715a5605f0601da6a982abcc920b5cc8e`.
  The proposal's `advanced` base, `663a7fd3f31580f2f1c40820135d3c3338edab55`,
  adds documentation only after that source commit. The published image is pinned in
  [COMPATIBILITY_IMAGE.md](COMPATIBILITY_IMAGE.md); a future KB-capable image
  needs its own source commit and digest.
- TruChat source reviewed: `miki3421/truchat` commit
  `5cea341e39d7632edda4ea8966d577d51a37db47`. Its action sends
  `OPS-STREAM <STREAM_TOKEN>\n` before newline-delimited JSON. The proxy
  authenticates that preamble and forwards subsequent bytes unchanged.
- The KB consumer uses a different contract: the browser supplies a fresh
  `stream_request` (`v`, `rid`, `key`); the relay must create a request-specific
  `stream_binding`; the action and relay mutually authenticate with HELLO/ACK
  HMAC; the relay validates each sequenced, MAC-protected NDJSON frame and its
  terminal before forwarding it. The browser checks framing and correlation
  but relies on the trusted relay for cryptographic MAC verification.

These contracts are not wire compatible. The published proxy injects
`STREAM_TOKEN`, but no `stream_binding`, and its TCP listener expects the
`OPS-STREAM` preamble instead of the KB HELLO. The existing KB action rejects
the invocation before loading content or calling the model. In a synthetic
loopback run using the actual KB action and the proxy built from `a6344fe`,
the positive KB scenario returned action HTTP 400, proxy HTTP 502, zero events,
zero content loads and zero model calls; cleanup completed. This is a local
compatibility result, not a live or security acceptance test.

The KB relay patch prepared against Apache source `27cf4f4` does not apply
cleanly to this fork. `git apply --check` fails in the web action handler and
TCP server because both implementations change those files. Port the contract
deliberately; do not stack the patch or replace the TruChat flow wholesale.

## Proposed mode boundary

1. Preserve the existing TruChat/generic mode, including `STREAM_TOKEN`, its
   authenticated preamble, byte-preserving forwarding, current error mapping
   and its documented bounds. Never add an unauthenticated callback fallback.
2. Select KB mode only for the explicit KB action route and a valid versioned
   `stream_request`. A missing, malformed or conflicting descriptor on that
   route must fail closed before action invocation or callback allocation;
   never retry the request in generic mode. Do not let the HTTP caller provide
   trusted `STREAM_HOST`, `STREAM_PORT`, `STREAM_TOKEN` or `stream_binding`.
3. In KB mode inject the relay's listener host/port and a fresh binding over
   route, exact Cookie, raw question/history strings, request ID, expiry and
   relay nonce. Do not inject `STREAM_TOKEN` into the KB action: its current
   input contract rejects unknown fields. The action still verifies session,
   `kb.read`, Origin, CSRF, binding and approved callback host before content
   or provider access.
4. In KB mode require the KB HELLO/ACK HMAC exchange before either peer sends
   application data. Authenticate the complete frame stream at the relay:
   request ID, strict sequence, MAC, canonical payload, type, size, one
   terminal and no bytes after it. Forward original complete records with
   their newline; EOF without a terminal must remain a browser-visible error.
   A bad peer, bad frame or late callback must not downgrade to generic mode.
5. Preserve KB's pre-frame 400/401/403 semantics with sanitized bodies. Once
   streaming starts, terminate on errors without appending plain-text HTTP
   errors to NDJSON. Keep cancel, listener, upstream and socket cleanup bounded.

This mode should use the KB consumer's already approved per-request metadata
and protected callback-host setting. It proposes no new environment-variable
names, secret store, listener service or model configuration. Neither callback
protocol encrypts TCP; the deployment network and browser HTTP/TLS boundary
remain separate release decisions.

The existing KB contract uses a 64 KiB request cap, 32 admitted KB requests
before body parsing, 130-second relay lifetime, 2-second callback handshake,
64 KiB frames, 256 KiB total output and 128 events. The generic fork currently
allows 1 MiB requests, 64 global streams, 20 seconds to authenticate the
callback, 30 seconds idle and five minutes total. The implementation must
enforce the KB bounds for the KB route without silently reducing TruChat's
documented limits. Keep a common global cap, and apply the KB route cap before
reading its body. Review contention and the two existing availability findings
(global slot starvation and serial partial handshakes) before any shared
multi-tenant rollout.

## Verification required for implementation

- Pin the resulting fork commit and new immutable image digest. Review the
  actual diff against `advanced`; do not treat the published TruChat digest as
  KB capable.
- Retain TruChat compatibility tests and add positive/negative KB tests for
  descriptor and binding validation, wrong/replayed peers, HELLO/ACK,
  frame MAC/sequence/terminal, malformed input, limits, cancellation and
  concurrent streams. Run Go tests, race tests and vet on the exact candidate.
- Run the KB synthetic browser → relay → real action loopback suite, including
  a response delayed beyond 60 seconds, Stop, auth/CSRF rejection, expiry,
  provider error and cleanup. Keep all fixtures synthetic. Confirm the
  generic TruChat path and its browser parser still pass.
- Obtain independent security review of the merged trust boundary. The
  source author or this compatibility analysis cannot supply that review.
- Before a shared upgrade, inventory every active client, update clients
  first, qualify the tenant route and ingress, preserve the previous image and
  workload source for rollback, then run readiness and real browser E2E on the
  target under a separately authorized rollout. No real browser, model or
  cluster acceptance is claimed here.

## Verification already completed

At the identities above: TruChat's six Python compatibility tests and six Bun
parser tests passed; `npm ci`, Vite production build and TypeScript check
passed. The Streamer fork's Go tests and race tests passed from the pinned
source. The published registry manifest was readable anonymously, matched
the declared digest and identified a Linux amd64 image. Docker-based smoke,
live ingress, browser acceptance and installation were not run.

The KB's approved transport contract is the reference for this proposal.
Changing it to accept only the generic preamble would remove its request
binding and relay-side frame authentication; that requires a separate
architecture and security decision rather than an implicit client adaptation.
