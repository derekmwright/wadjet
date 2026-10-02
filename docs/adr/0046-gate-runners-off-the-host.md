# ADR-0046: The heavy gates run on ephemeral AWS runners, not on the development host

- Status: Accepted (2026-10-02)
- Deciders: Derek Wright
- Context: the engine-correctness arcs (ADR-0012's gates, `docs/testing/GATES.md`)

## Context

Every arc round runs the same heavy gate set two or three times (the author, the reviewer, the landing battery): `./internal/coordinator/` without `-short` (≈50 min), `./internal/server/...` without `-short`, `./wadjet/`, TPC-H SF0.01, the two PostgreSQL oracle arms, and the pre-push hook's `go test -race -short ./internal/...`. On one development host these serialize on a lock (`tooling/heavy.lock`); with three arcs in flight the queue reached eight runs and the timing pins (#789, #1391) tripped under the load. Measured on 2026-10-02: 2–3 hours of lock wait per round per arc, and a practical width of two concurrent arcs.

The same host was earlier used for SF100 benchmark runs that belong on a sized machine.

## Decision

The heavy gate set runs on an ephemeral AWS runner per request: one branch sha in, one summary (`summary.json` + per-lane logs) to S3 out. The host keeps the human-speed loop: editing, `go build`, per-package tests, PostgreSQL oracle cell measurement, docscheck / licensecheck / gofmt / vet, and the landing itself.

- **Image**: Amazon Linux 2023 on x86_64 (the gates do not depend on the architecture), Go toolchain pinned to `go.mod`, Docker with `postgres:17-alpine` pulled, the repo cloned at boot via a read-only deploy key or a GitHub token in SSM Parameter Store.
- **Shape**: one `c7a.8xlarge` (32 vCPU, 64 GiB) spot instance per request; lanes in parallel; the coordinator package sharded by `-run` into four shards. Target: the longest lane under 15 minutes.
- **Lifecycle**: the instance terminates itself on completion; the reaper (`deploy/benchmark/reaper`) terminates anything tagged `wadjet-gate-runner` older than 40 minutes; at most four runners at once (a tag count checked by the dispatcher before launch).
- **Interface**: `tooling/gaterun.sh <sha> [lanes]` pushes the sha to a `gates/<sha>` ref, launches, waits on `s3://<bucket>/gates/<sha>/summary.json`, prints the lane table, exits with the worst lane's status. Authors, reviewers and the landing script call it instead of the local runs; the local lock stays for anything still run locally.
- **Approval**: a standing approval for runner launches under these caps (spot only, 40-minute hard terminate, four concurrent, the gate bucket only) — Derek, 2026-10-02. Anything outside the caps (a bigger instance, a longer runtime, a different bucket) is a deploy and needs its own approval.
- **Cost bound**: a `c7a.8xlarge` spot for 20 minutes is well under one dollar; an arc of three rounds plus a battery is about ten requests.

## Consequences

- A round's gate wait falls from hours of queue to about 15 minutes, independent across arcs; the host's timing pins stop tripping from load; three to five arcs can run in parallel bounded by review attention and usage, not the host.
- The sha-match reviewer rule (`tooling/REVIEWER_COMMON.md`) keeps its meaning: the summary names the sha it ran.
- The landing battery becomes a gate request on the landing sha; `tooling/land.sh` keeps the pre-push hook as the last local check.
- Logs leave the host: the evidence rule (every gate row names its log with the sha echoed) is met by the S3 path, mirrored into the arc directory by `gaterun.sh`.
- Alternatives considered: GitHub-hosted runners (slow and metered for a 50-minute package); a second permanent host (idle cost, the same lock problem at 2× width); sharding only locally (CPU-bound on the same box).
