# Authenticated KB stream mode on `kb-streaming`

Status: locally implemented and tested, 20 October 2026. This document records
the source boundary, local evidence and release gates. No Dockerfile/CI image
was built or published, and no shared Streamer rollout has occurred.

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
cleanly to this fork. Its protocol and handler tests were ported into the fork;
the web handler was integrated with the fork's existing private and generic
paths. The supplied availability patch was integrated first (SHA-256
`f9dda540474080c08f52fe90fc89eecd39a8ec79368af961ee1d5cdfc0587ae8`).

## Implemented mode boundary

1. Preserve the existing generic `STREAM_TOKEN` preamble, byte-preserving
   forwarding and private-action bounds. Never add an unauthenticated callback
   fallback. The web admission, body and lifetime bounds change as described
   below.
2. Presence of `stream_request` selects KB mode, independent of the action
   route. An invalid descriptor fails closed before action invocation or
   callback allocation; never retry in generic mode. The KB action itself
   rejects a missing descriptor or binding before protected content access.
   Do not let the HTTP caller provide
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

The web handler acquires one of 32 shared slots before parsing any body,
including generic uploads. All web requests have a 64 KiB body limit and
130-second lifetime. This applies the canonical KB admission contract before
the descriptor can be read. The private action handler retains its 1 MiB body,
64 process-wide admission and five-minute lifetime. The narrower web bounds
are a compatibility change for generic web clients and require client inventory
before a shared multi-tenant rollout. KB additionally uses a two-second
callback handshake, 64 KiB frames, 256 KiB total output and 128 events.

## Local verification and remaining release gates

- Pin the resulting fork commit. A new immutable image digest is still needed
  before any deployment; the published TruChat digest is not KB capable.
- Retain TruChat compatibility tests and add positive/negative KB tests for
  descriptor and binding validation, wrong/replayed peers, HELLO/ACK,
  frame MAC/sequence/terminal, malformed input, limits, cancellation and
  concurrent streams. Run Go tests, race tests and vet on the exact candidate.
- Re-run the KB synthetic browser → relay → real action loopback suite on the
  final commit when packaging it. Keep all fixtures synthetic. Confirm generic
  client compatibility after inventory; the existing legacy probe targets an
  older callback preamble and needs the fork adapter.
- Obtain independent security review of the merged trust boundary. The
  source author or this compatibility analysis cannot supply that review.
- Before a shared upgrade, inventory every active client, update clients
  first, qualify the tenant route and ingress, preserve the previous image and
  workload source for rollback, then run readiness and real browser E2E on the
  target under a separately authorized rollout. No real browser, model or
  cluster acceptance is claimed here.

The local Go suite, race suite and vet pass on the integrated source. The
synthetic browser → relay → real KB action loopback suite passed all eight
short cases: positive, anonymous, CSRF, session binding, input binding,
provider error, expiry and Stop. The delayed case passed with 20 events over
65.005 seconds. A loopback generic-web probe adapted to this fork's required
`OPS-STREAM` preamble passed four cases: HTTP 202 callback, 64 KiB boundary,
oversized body rejection and redirect refusal. The original older probe cannot
serve as a compatibility result because it sends an unauthenticated callback;
it timed out against this fork. The local wrapper binary used for the final
loopback runs has SHA-256
`39058a9d13d62f4411c32dc926abf9e37c03cf53e7cbf80832d02706f64f63e7`.
This is a local binary digest, not an installed image digest. A separate local
OCI layout for Linux amd64 was assembled from the static Go 1.27.2 build and
verified by blob hashes; its provisional manifest digest is
`sha256:cfdab288a97586acdaca9f66be0c066eae442750c1c8820259008b0e3e223abd`.
It uses the workstation certificate bundle and does not replace the fork's
Dockerfile/CI build or establish a registry or installed digest.

Independent security review, generic-client inventory, image build/digest,
deployment readiness and real-browser acceptance remain open. The source author
cannot claim independent security acceptance. No remote state was changed.

## Verification already completed

At the identities above: TruChat's six Python compatibility tests and six Bun
parser tests passed; `npm ci`, Vite production build and TypeScript check
passed. The Streamer fork's Go tests and race tests passed from the pinned
source. The published registry manifest was readable anonymously, matched
the declared digest and identified a Linux amd64 image. Docker-based smoke,
live ingress, browser acceptance and installation were not run.

The KB's approved transport contract is the reference for this source change.
Changing it to accept only the generic preamble would remove its request
binding and relay-side frame authentication; that requires a separate
architecture and security decision rather than an implicit client adaptation.
