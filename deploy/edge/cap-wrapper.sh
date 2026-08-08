#!/usr/bin/env bash
#
# cap-wrapper: run a wadjet process inside a hard memory-capped docker
# container — the edge-box simulator. Used as the tpch-harness
# -spawn-wrapper so every spawned coordinator/worker gets real OOM-kill
# semantics (GOMEMLIMIT alone is a soft Go-heap target; edge reality is
# the kernel OOM killer at the cgroup boundary).
#
# Usage (via harness):
#   EDGE_CAP_MB=4096 tpch-harness --mode=local \
#     --wadjet-bin=/tmp/wadjet-edge/wadjet \
#     --spawn-wrapper=deploy/edge/cap-wrapper.sh ...
#
# Requirements:
#   - The wadjet binary AND the harness run/data dirs must live under /tmp
#     (the only host mount), so paths resolve identically inside the
#     container.
#   - GOMEMLIMIT is deliberately NOT forwarded: the binary's own
#     cgroup-detection (memory.DetectMemoryLimit) must size itself from
#     the container limit — that detection path is part of what edge
#     validation tests.
#
# Env knobs:
#   EDGE_CAP_MB  hard memory cap per process in MiB (default 4096)
#   EDGE_CPUS    CPU count (default 4 — edge boxes are 2-4 cores; also
#                exported as GOMAXPROCS because Go ≤1.24 ignores cgroup
#                CPU quotas and would otherwise see all host cores,
#                running e.g. 24-way scan decode against a 384 MiB
#                envelope — a shape no real edge box has)
#   EDGE_IMAGE   container image (default debian:bookworm-slim — glibc
#                for the dynamically-linked wadjet binary)

set -euo pipefail

CAP_MB="${EDGE_CAP_MB:-4096}"
CPUS="${EDGE_CPUS:-4}"
IMAGE="${EDGE_IMAGE:-debian:bookworm-slim}"
# Hard TTL: the harness shutdown SIGKILLs the docker CLIENT after a 5s
# grace; the container survives that (sig-proxy can't forward SIGKILL).
# Three orphaned containers from an ENOSPC-slowed shutdown ran for 50
# minutes on 2026-06-11 — one kept compacting and physically deleted a
# regenerated dataset out from under the next run. timeout(1) inside the
# container guarantees orphans die on their own. Also sweep before runs:
#   docker ps -q --filter label=wadjet-edge | xargs -r docker rm -f
TTL="${EDGE_TTL_SECONDS:-7200}"

# --memory-swap == --memory: no swap escape hatch — exceed the cap, die.
# --init: PID-1 reaping + signal handling; the harness's SIGTERM reaches
#   wadjet via docker's default sig-proxy, so graceful shutdown works and
#   --rm cleans the container up.
# --network=host: the harness allocates ports on the host and passes them
#   as flags; host networking makes them line up (verified on WSL2 native
#   dockerd).
# -u host-uid: files written into the mounted /tmp stay owned by the user.
exec docker run --rm -i --init --network=host \
  --memory="${CAP_MB}m" --memory-swap="${CAP_MB}m" \
  --cpus="${CPUS}" \
  --label=wadjet-edge=1 \
  -v /tmp:/tmp \
  -u "$(id -u):$(id -g)" \
  -e HOME=/tmp/wadjet-edge-home \
  -e GOMAXPROCS="${CPUS}" \
  -e GODEBUG=gctrace=1 \
  -e WADJET_HEAP_DUMP_INTERVAL="${EDGE_HEAP_DUMP_INTERVAL:-5s}" \
  -e WADJET_HEAP_DUMP_DIR \
  -e WADJET_DROP_BEHIND \
  -e WADJET_REFAULT_STREAM_DISCOUNT \
  -e WADJET_REFAULT_PRESSURE_RATE \
  -e WADJET_REFAULT_EPISODE_CAP \
  -e WADJET_SCAN_AFFINITY \
  -e WADJET_BASE_PEER_TIER \
  -e WADJET_BASE_PEER_READTHROUGH \
  -e WADJET_ROWGROUP_READAHEAD \
  ${EDGE_GOGC:+-e WADJET_GOGC="${EDGE_GOGC}"} \
  "$IMAGE" timeout --signal=KILL "$TTL" "$@"
