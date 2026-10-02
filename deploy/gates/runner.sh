#!/bin/bash
# SPDX-License-Identifier: MIT
#
# The gate runner (ADR-0046): the launch template's user_data, unchanged per
# request. One sha in, s3://<bucket>/gates/<sha>/<run>/summary.json out.
#
# Inputs come ONLY from the instance's own tags, read through IMDS
# (instance_metadata_tags=enabled in the template):
#   sha     full 40-hex commit; cloned from origin's refs/gates/<sha>
#   run     the run id (UTC launch stamp) — the S3 prefix is gates/<sha>/<run>/
#   bucket  the gate bucket
#   lanes   "all" or a '+'-joined lane list (tag values carry no commas)
#   rerun   optional '+'-joined test names to rerun ALONE when they fail
#
# The lanes are tooling/battery_run.sh's, run as separate parallel processes
# with one log each, plus: lane F (./internal/coordinator/ no -short) split
# into four round-robin -run shards F1..F4, and HOOK, the pre-push hook's
# command. The two PostgreSQL oracle arms (PG1, PG2) run after the lanes, as
# the battery runs them, in parallel with each other (each starts its own
# uniquely named container on an ephemeral port).
#
# Lifecycle: `shutdown -h +38` at boot is the hard watchdog (the template's
# shutdown behaviour is terminate; the gate reaper's 40-minute bound is the
# backstop behind it). A soft watchdog at 35 minutes kills the lanes still
# running, records them as WATCHDOG (exit 124), uploads, and shuts down.
# summary.json is uploaded LAST: its presence is the completion signal.

set -u
export HOME=/root
G=/gate
OUT=$G/out
mkdir -p "$OUT/logs" "$G/tmp"
exec > "$OUT/logs/runner.log" 2>&1
shutdown -h +38 "gate runner hard watchdog"
T0=$(date +%s)
STARTED=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "runner start $STARTED"

# --- inputs from IMDS ---
TOK=$(curl -sf -X PUT -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" http://169.254.169.254/latest/api/token)
md() { curl -sf -H "X-aws-ec2-metadata-token: $TOK" "http://169.254.169.254/latest/meta-data/$1"; }
tag() { md "tags/instance/$1" || true; }
SHA=$(tag sha); RUN=$(tag run); BUCKET=$(tag bucket); LANES=$(tag lanes); RERUN=$(tag rerun)
REGION=$(md placement/region); IID=$(md instance-id); ITYPE=$(md instance-type)
LANES=${LANES:-all}
export AWS_DEFAULT_REGION=$REGION
echo "sha=$SHA run=$RUN bucket=$BUCKET lanes=$LANES rerun=$RERUN instance=$IID $ITYPE $REGION"
if [ -z "$SHA" ] || [ -z "$RUN" ] || [ -z "$BUCKET" ]; then
  echo "missing sha/run/bucket tag; nothing to report to"; shutdown -h now; exit 1
fi
DEST="s3://$BUCKET/gates/$SHA/$RUN"
CACHE="s3://$BUCKET/gates/cache"
REPO_URL=https://github.com/derekmwright/wadjet.git
SETUP_ERR=""

status() { echo "$(date -u +%H:%M:%S) $*" | tee -a "$OUT/status.txt"; aws s3 cp --quiet "$OUT/status.txt" "$DEST/status.txt" || true; }

# --- the summary (python3 ships with AL2023) ---
finalize() {
  local finished; finished=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  SHA="$SHA" RUN="$RUN" IID="$IID" ITYPE="$ITYPE" STARTED="$STARTED" FINISHED="$finished" \
  SETUP_ERR="$SETUP_ERR" DEST="$DEST" OUT="$OUT" WATCHDOG="${WATCHDOG:-0}" LANES="$LANES" \
  python3 - <<'PY'
import json, os, re, glob
out = os.environ["OUT"]
lanes = {}
for meta in sorted(glob.glob(f"{out}/meta/*.json")):
    m = json.load(open(meta))
    name = m["name"]
    log = f"{out}/logs/{name}.log"
    text = open(log, errors="replace").read() if os.path.exists(log) else ""
    m["fail"] = len(re.findall(r"^\s*--- FAIL", text, re.M))
    m["failed_tests"] = sorted(set(re.findall(r"^\s*--- FAIL: (\S+)", text, re.M)))[:200]
    m["log"] = f"{os.environ['DEST']}/logs/{name}.log"
    lanes[name] = m
reruns = []
for p in sorted(glob.glob(f"{out}/rerun/*.json")):
    reruns.append(json.load(open(p)))
passed_alone = {r["test"] for r in reruns if r["exit"] == 0}
worst = 0
for m in lanes.values():
    eff = m.get("exit", 1)
    top = {t for t in m["failed_tests"] if "/" not in t}
    if eff != 0 and top and top <= passed_alone and not m.get("note"):
        eff = 0
        m["note"] = "every failed test passed when rerun alone (--rerun-flakes)"
    m["effective_exit"] = eff
    worst = max(worst, eff)
setup = os.environ["SETUP_ERR"]
if setup:
    worst = max(worst, 1)
summary = {
    "sha": os.environ["SHA"], "run": os.environ["RUN"],
    "instance": os.environ["IID"], "instance_type": os.environ["ITYPE"],
    "started": os.environ["STARTED"], "finished": os.environ["FINISHED"],
    "lanes_requested": os.environ["LANES"],
    "setup_error": setup or None, "watchdog": os.environ["WATCHDOG"] == "1",
    "exit": worst, "lanes": lanes, "reruns": reruns,
    "phases": json.load(open(f"{out}/phases.json")) if os.path.exists(f"{out}/phases.json") else {},
}
json.dump(summary, open(f"{out}/summary.json", "w"), indent=1, sort_keys=True)
PY
  aws s3 cp --quiet --recursive "$OUT/logs" "$DEST/logs" || true
  aws s3 cp --quiet "$OUT/summary.json" "$DEST/summary.json" || true
  echo "summary uploaded to $DEST/summary.json"
}

phase() { # phase <name> <seconds>
  python3 -c "import json,os,sys; p='$OUT/phases.json'; d=json.load(open(p)) if os.path.exists(p) else {}; d[sys.argv[1]]=int(sys.argv[2]); json.dump(d,open(p,'w'))" "$1" "$2"
}

die_setup() { SETUP_ERR="$*"; status "SETUP FAILED: $*"; finalize; shutdown -h now; exit 1; }

# --- soft watchdog ---
(
  sleep $((35 * 60))
  echo "SOFT WATCHDOG at 35 min: killing running lanes"
  touch "$OUT/WATCHDOG"
  for pg in "$OUT"/pgid/*; do [ -f "$pg" ] && kill -TERM -- "-$(cat "$pg")" 2>/dev/null; done
) &
WD_PID=$!

status "setup: packages"
dnf install -y -q git docker tar gzip >/dev/null 2>&1 || dnf install -y git docker tar gzip || die_setup "dnf install"
# Docker serves only the oracle arms; when it does not start they SKIP, and a skipped arm is recorded as a failure below.
systemctl start docker || echo "docker did not start"
( docker pull -q postgres:17-alpine > "$OUT/logs/docker-pull.log" 2>&1; echo "EXIT=$?" >> "$OUT/logs/docker-pull.log" ) &
PULL_PID=$!

status "setup: clone $SHA"
GH_TOKEN=$(aws ssm get-parameter --name /wadjet/gates/github-token --with-decryption --query Parameter.Value --output text 2>/dev/null || true)
SRC=$G/src
mkdir -p "$SRC" && cd "$SRC" || die_setup "mkdir src"
git init -q .
if [ -n "$GH_TOKEN" ]; then
  git remote add origin "https://x-access-token:$GH_TOKEN@github.com/derekmwright/wadjet.git"
else
  git remote add origin "$REPO_URL"
fi
git fetch -q --depth 1 origin "refs/gates/$SHA" || git fetch -q --depth 1 origin "$SHA" || die_setup "fetch refs/gates/$SHA"
git checkout -q FETCH_HEAD || die_setup "checkout"
[ "$(git rev-parse HEAD)" = "$SHA" ] || die_setup "checked out $(git rev-parse HEAD), not $SHA"
git remote set-url origin "$REPO_URL"   # drop the token from .git/config

GOVER=$(awk '/^toolchain go/{sub("toolchain go","");print;exit} /^go [0-9]/{v=$2} END{if(v)print v}' go.mod | head -1)
status "setup: go $GOVER"
GOTGZ=go$GOVER.linux-amd64.tar.gz
if ! aws s3 cp --quiet "$CACHE/$GOTGZ" "$G/tmp/$GOTGZ"; then
  curl -sfL -o "$G/tmp/$GOTGZ" "https://go.dev/dl/$GOTGZ" || die_setup "download $GOTGZ"
  aws s3 cp --quiet "$G/tmp/$GOTGZ" "$CACHE/$GOTGZ" || true
fi
tar -C /usr/local -xzf "$G/tmp/$GOTGZ" || die_setup "untar go"
TASKVER=3.48.0
TASKTGZ=task_v${TASKVER}_linux_amd64.tar.gz
if ! aws s3 cp --quiet "$CACHE/$TASKTGZ" "$G/tmp/$TASKTGZ"; then
  curl -sfL -o "$G/tmp/$TASKTGZ" "https://github.com/go-task/task/releases/download/v$TASKVER/task_linux_amd64.tar.gz" || die_setup "download task"
  aws s3 cp --quiet "$G/tmp/$TASKTGZ" "$CACHE/$TASKTGZ" || true
fi
tar -C /usr/local/bin -xzf "$G/tmp/$TASKTGZ" task || die_setup "untar task"

export PATH=/usr/local/go/bin:/usr/local/bin:$PATH
export GOPATH=$G/gopath GOCACHE=$G/gocache GOMODCACHE=$G/gopath/pkg/mod TMPDIR=$G/tmp GOTOOLCHAIN=local
go version || die_setup "go version"

MODKEY=gomod-$(sha256sum go.sum | cut -c1-16).tar.gz
if aws s3 cp --quiet "$CACHE/$MODKEY" "$G/tmp/$MODKEY"; then
  mkdir -p "$GOMODCACHE" && tar -C "$GOMODCACHE" -xzf "$G/tmp/$MODKEY"
fi
go mod download || die_setup "go mod download"
if [ ! -f "$G/tmp/$MODKEY" ]; then
  tar -C "$GOMODCACHE" -czf "$G/tmp/$MODKEY" . && aws s3 cp --quiet "$G/tmp/$MODKEY" "$CACHE/$MODKEY" || true
fi
T1=$(date +%s); phase setup_s $((T1 - T0))

# --- warm the build cache once, so the lanes do not compile the same packages 15 times over ---
status "warm: compiling test binaries"
go test -count=1 -run '^$' ./internal/... ./wadjet/ ./test/ ./tools/... ./benchmarks/tpch/ > "$OUT/logs/warm.log" 2>&1
echo "EXIT=$?" >> "$OUT/logs/warm.log"
T2=$(date +%s); phase warm_s $((T2 - T1))

# --- the lanes ---
mkdir -p "$OUT/meta" "$OUT/pgid" "$OUT/rerun"
want() { # want <lane> <group>: is the lane requested?
  [ "$LANES" = all ] && return 0
  local l; for l in ${LANES//+/ }; do [ "$l" = "$1" ] || [ "$l" = "$2" ] && return 0; done; return 1
}
lane() { # lane <name> <cmd...>: runs in its own session (process group) so the watchdog can kill it
  local n=$1; shift
  (
    s=$(date +%s); st=$(date -u +%H:%M:%S)
    setsid -w bash -c 'echo $$ > "$0"; exec "$@"' "$OUT/pgid/$n" "$@" > "$OUT/logs/$n.log" 2>&1
    rc=$?
    [ -f "$OUT/WATCHDOG" ] && rc=124 && echo "WATCHDOG: killed at 35 min" >> "$OUT/logs/$n.log"
    echo "EXIT=$rc" >> "$OUT/logs/$n.log"
    printf '{"name":"%s","exit":%d,"wall_s":%d,"start":"%s"}\n' "$n" "$rc" $(( $(date +%s) - s )) "$st" > "$OUT/meta/$n.json"
    rm -f "$OUT/pgid/$n"
    echo "$(date -u +%H:%M:%S) lane $n EXIT=$rc wall=$(( $(date +%s) - s ))s" >> "$OUT/status.txt"
  ) &
  LANE_PIDS+=($!)
}
LANE_PIDS=()
wait_lanes() { [ ${#LANE_PIDS[@]} -gt 0 ] && wait "${LANE_PIDS[@]}"; LANE_PIDS=(); }

status "lanes: $LANES"
want gofmt gofmt && lane gofmt bash -c "f=\$(gofmt -l \$(git ls-files '*.go')); [ -z \"\$f\" ] || { echo \"\$f\"; exit 1; }; go vet ./... && go run ./tools/docscheck . && task build"
want A A && lane A go test -count=1 -short -p 2 ./internal/... ./wadjet/ ./test/ ./tools/... -timeout 25m
want B B && lane B go test -count=1 -p 2 ./internal/engine/... ./internal/planner/... ./internal/storage/... ./internal/worker/ -timeout 30m
want C C && lane C bash -c "go test -count=1 -run 'TestTPCHQueries$|TestTPCHOptimizationInvariance$|TestTPCHStageDumpGolden|TestTPCH_EnsureDistribution_Snapshot|TestTPCHDistributionConsistency|TestDuckDBCompare' ./benchmarks/tpch/ -timeout 20m && go test -count=1 -run 'TestTPCHQueriesDecimal|TestTPCHDecimalDeclaredTypes|TestTPCHOptimizationInvarianceDecimal|TestTwoPathInvarianceDecimal' ./benchmarks/tpch/ -timeout 20m"
want D D && lane D bash -c "go test -count=1 -run TestStandaloneVsDistributedDifferential ./internal/coordinator/ -timeout 20m && go test -count=1 -run 'TestTypeMatrix|TestNumericArc2' ./internal/coordinator/ -timeout 25m"
want E E && lane E go test -count=1 -run 'TestTypeMatrix|TestQueryPanicBoundary' ./wadjet/ -timeout 25m
if want F1 F || want F2 F || want F3 F || want F4 F; then
  # Lane F sharded: every top-level test/fuzz/example name, round-robin into four -run lists.
  go test -list . ./internal/coordinator/ 2>"$OUT/logs/F-list.log" | grep -E '^(Test|Fuzz|Example)' > "$OUT/F.names"
  echo "$(wc -l < "$OUT/F.names") coordinator test names" >> "$OUT/logs/F-list.log"
  for k in 1 2 3 4; do
    want "F$k" F || continue
    re=$(awk -v k="$k" 'NR % 4 == k % 4' "$OUT/F.names" | paste -sd'|')
    [ -n "$re" ] && lane "F$k" go test -count=1 -run "^($re)\$" ./internal/coordinator/ -timeout 60m
  done
fi
want G G && lane G go test -count=1 ./internal/server/ ./internal/server/pgwire/ ./internal/auth/... -timeout 25m
want HOOK HOOK && lane HOOK go test -race -short ./internal/... -timeout 10m
wait_lanes
T3=$(date +%s); phase lanes_s $((T3 - T2))

if { want PG1 PG || want PG2 PG; } && [ ! -f "$OUT/WATCHDOG" ]; then
  wait "$PULL_PID"
  status "oracle arms (image pull: $(tail -1 "$OUT/logs/docker-pull.log"))"
  want PG1 PG && lane PG1 task pg-oracle:test
  want PG2 PG && lane PG2 task pg-oracle:test-decimal
  wait_lanes
  # An oracle arm that skipped (no docker, no image) passed vacuously: that is a failure here.
  for n in PG1 PG2; do
    [ -f "$OUT/meta/$n.json" ] || continue
    if grep -q -- '--- SKIP: TestPostgresOracle' "$OUT/logs/$n.log"; then
      python3 -c "import json;p='$OUT/meta/$n.json';m=json.load(open(p));m['exit']=1;m['note']='the oracle SKIPPED (no PostgreSQL): a vacuous pass is a failure';json.dump(m,open(p,'w'))"
    fi
  done
fi
T4=$(date +%s); phase oracle_s $((T4 - T3))

# --- rerun-flakes: a named test that failed is rerun ALONE (no other lane running) ---
if [ -n "$RERUN" ] && [ ! -f "$OUT/WATCHDOG" ]; then
  for t in ${RERUN//+/ }; do
    for log in "$OUT"/logs/*.log; do
      grep -qE "^\s*--- FAIL: $t( |$)" "$log" || continue
      # the package is the first "FAIL\t<pkg>" / "--- FAIL" owner line after the test's FAIL line
      pkg=$(awk -v t="$t" '$0 ~ "--- FAIL: "t"( |$)" {f=1} f && /^(FAIL|ok)[ \t]+[a-z]/ {print $2; exit}' "$log")
      [ -n "$pkg" ] || continue
      n=$(basename "$log" .log)
      status "rerun alone: $t in $pkg (failed in lane $n)"
      s=$(date +%s)
      go test -count=1 -run "^$t\$" "$pkg" -timeout 10m > "$OUT/logs/rerun-$t.log" 2>&1
      rc=$?
      echo "EXIT=$rc" >> "$OUT/logs/rerun-$t.log"
      printf '{"test":"%s","package":"%s","lane":"%s","exit":%d,"wall_s":%d,"log":"%s"}\n' "$t" "$pkg" "$n" "$rc" $(( $(date +%s) - s )) "$DEST/logs/rerun-$t.log" > "$OUT/rerun/$t.json"
      break
    done
  done
fi

[ -f "$OUT/WATCHDOG" ] && WATCHDOG=1
kill "$WD_PID" 2>/dev/null
status "done; uploading"
finalize
shutdown -h now
