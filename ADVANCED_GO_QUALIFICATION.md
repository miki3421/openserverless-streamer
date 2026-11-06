# ADV Streamer Go toolchain qualification, 2026-11-06

The previous qualified Streamer contained Go 1.25.14. Its scanner findings were
in the compiled standard library, so changing a container OS layer would not
correct them. This candidate rebuilds the same application source and module
graph with Go 1.27.3. The final image remains scratch, with a static binary,
public CA bundle and project license files. No application handler, callback
authentication, tenant admission, timeout or request-size policy is changed.

The Alpine build toolchain is pinned to its AMD64 manifest:
`golang:1.27.3-alpine@sha256:0fd028db9bdd4f01657bc8047c209b04d8ce19b853d776bd6f1e76a41729b936`.
Builds use `GOTOOLCHAIN=local` and `-mod=readonly`. The Go 1.25 module language
directive and go.sum remain unchanged. Race tests use the same Go version in
the pinned Bookworm builder, which supplies the native race-test compiler.
The CI publication trigger remains restricted to the personal `advanced`
branch; pushing this qualification branch does not publish an image.

Primary release reference: <https://go.dev/doc/devel/release#go1.27.3>.

## Source, image and verification

Application base: `e06143b` (tenant admission and callback availability fixes).
The only functional build change is the toolchain pin and read-only module
selection. Compiled build-info comparisons confirm identical application module
dependencies, CGO disabled and AMD64. License files match the previous image.
The binary and public CA bundle come from the pinned new build.

- Lab image: `localhost/ops-advanced-streamer:20261106-go1273-r1`
- Manifest: `sha256:5aa7a68a4be7ea75f20de996656623a46d639d2d933856ae393d979a1f374911`
- Configuration: `sha256:35b6ba1f1fe5cc078147d8d141b463ba33b55b1b5ede123ea2b908e7d25600c9`

All 32 top-level Go tests passed with `-race -count=1` (54 named pass events
including subtests); `go vet` passed. Existing tests cover invalid callback
tokens, peer separation, slow-peer admission, cancellation/resource release,
response closure, fixed-upstream redirects and callback establishment deadlines.
No tests were removed or skipped.

On ADV, 14 actual HTTP checks passed across Alpha and Beta: GET/POST streams
returned the expected two events; anonymous/invalid access returned 401; peer
credentials returned 403; null JSON returned 400; oversized JSON returned 413.
The Streamer pod did not restart during those checks. Twelve Alpha/Beta
functional/access checks also passed. The StatefulSet retained its identity;
its only spec change was the image. Services, ingress, environment, RBAC and
application definitions were not modified.

Trivy with identical databases reports 4 High / 9 Medium / 1 Low for the old
image and no vulnerability matches for the candidate's compiled binary. The
new scan still discovers the binary and its dependencies; empty findings do
not mean the scanner skipped it. This is not proof of absence of undiscovered
vulnerabilities and does not cover other OPS images or application packages.

## Limits and rollback

The image is locally imported into AMD64 ADV, not registry-published. ARM64,
fresh full installation, production traffic and sustained-load performance
remain separate qualifications. Baseline is unchanged. Private rollback
snapshots and evidence remain outside Git; rollback selects the old Streamer
image and matching catalog entry. Existing source and earlier failure evidence
are preserved.
