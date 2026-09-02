# Wadjet Runbook — run scenarios and the flag surface

How to run the engine in each deployment shape, which knobs matter for
which scenario, and what is (and is not) changeable at runtime. Grounded
against `cmd/wadjet/main.go` as of 2026-07-04 (post morsel-v2 merge).
Companion docs: `operations.md` (metrics, troubleshooting),
`configuration.md` (YAML config file), `distributed.md` (cluster
concepts), `security.md` (authn/z), `tuning.md`.

> **Freshness note:** `configuration.md`/`tuning.md`/`operations.md`
> predate the March→July flag additions. This file is the authoritative
> flag list until they are refreshed; where they conflict, trust this one.

## 1. Run scenarios

### 1.1 Embedded (library, no server)

```go
db, err := wadjet.Open(...)   // public API in wadjet/
```

No flags, no NATS, no server processes. The embedded pipeline runs
parallel by default (`exec.Pipeline` with Workers=NumCPU). Use
`objstore.NewMemStore()`/`FileStore` for tests and local tools.

### 1.2 Standalone dev server (local files)

```bash
wadjet serve --mode=standalone --storage-type=file --data-dir=./data --pg-addr=:5432
psql -h localhost -p 5432 -U wadjet -d wadjet
```

Everything in one process: embedded NATS (`--nats-port`, default 4222),
pgwire, HTTP (`:8080`), gRPC (`:9090`). Prometheus `/metrics` is served on the
HTTP API port here — `--metrics-addr` (default `:9100`) binds a listener only
in **worker** mode.

### 1.3 Standalone against S3-compatible storage

```bash
wadjet serve --mode=standalone \
  --storage-type=s3 --endpoint=minio:9000 --bucket=wadjet \
  --access-key=... --secret-key=... [--ssl --region=us-east-2]
```

Credentials empty = auto-detect from env/IAM. Works with MinIO, AWS S3,
R2.

### 1.4 Production distributed cluster

```bash
# Coordinator
wadjet serve --mode=coordinator \
  --pg-addr=:5432 --nats-url=nats://nats:4222 \
  --storage-type=s3 --endpoint=s3.us-east-2.amazonaws.com --ssl \
  --bucket=<data-bucket> --region=us-east-2 \
  --data-plane=grpc --data-plane-addr=:9091 \
  --catalog-snapshot-s3-prefix=s3://<bucket>/catalog/

# Each worker
wadjet serve --mode=worker \
  --nats-url=nats://nats:4222 \
  --storage-type=s3 --endpoint=... --bucket=... --region=... --ssl \
  --data-plane=grpc --coord-data-plane=<coord-host>:9091 \
  --spill-dir=/mnt/nvme/spill \
  --max-concurrent=4
```

Key facts for sizing and reliability:

- **NATS is the control plane** (heartbeats, cancellation, catalog/UDF
  KV, DLQ); **gRPC is the data plane** (`--data-plane=grpc`: task
  dispatch, results, gather, TaskProgress over one multiplexed
  bidi stream per worker, plaintext intra-cluster). `nats` remains the
  data-plane default for small/legacy setups; use `grpc` in production.
- **Memory envelope:** `--memory-budget` (per-task) and
  `--shared-pool-budget` (worker-wide pool) auto-detect from the cgroup
  when 0. The validated SF100 shape on 32 GB workers:
  `--memory-budget=5015936171 --shared-pool-budget=20063744686
  --cache-bytes=2229304965 --max-concurrent=4`.
- **Spill wants real disk.** Put `--spill-dir` on NVMe. tmpfs-backed
  `/tmp` turns spills into RAM and ENOSPCs under load (SF10 Q18,
  2026-05-03).
- **`--max-concurrent` is a memory knob, not a CPU knob.** It bounds
  memory-owning task pipelines per worker; 4 is validated at SF100 on
  32 GB. Intra-task CPU parallelism is `--morsel-workers` (§2).
- **Catalog snapshots** (`--catalog-snapshot-s3-prefix`, 5m interval
  default) make a rebooted cluster discover its catalog in seconds
  instead of re-scanning the bucket. `--force-restore-catalog=latest`
  recovers from a lost NATS KV.
- **Fast boot + result delivery:** `--result-store` (default 512 MB)
  keeps small results in memory instead of S3 round trips.

### 1.5 Memory-tight / edge envelopes

Defaults are already the validated memory-tight profile:
`--mmap-relief=true` (RSS ceiling auto = 85% of the detected limit) and
`--bounded-dirty-writes=true`. Two standing rules from the SF100 record:

- **Never enable `--spill-floating-budget`.** Convicted of a 2× Q18
  regression (bisect 2026-06-10); the static 40%/90% thresholds stay.
- Don't re-tune `--mmap-relief-threshold-mb` reflexively — thresholds
  proved insensitive; 0 (auto) is right unless the cgroup limit is
  invisible to the process.

### 1.6 Query-routing and execution-shape knobs

- `--local-fastpath-bytes` (default 64 MiB): queries whose post-pruning
  scan bytes fit under it run in-process on the coordinator, skipping
  the stage DAG entirely (measured 2-12× on small/top-N queries). 0
  disables. Routing and planning failures fall back to the DAG; so do
  execution failures unrelated to the query's meaning (result budget,
  memory budget, S3 unreachable). A deterministic execution failure is
  reported to the client — `WADJET_FASTPATH_STRICT=0` restores the
  unconditional fallback. `LocalFastPathStrictFailures()` counts them.
- `--streaming-exchange` (default **true**): consumers fetch stage
  outputs from producer workers' local disk over gRPC with async S3
  upload; any failure falls back to the durable S3 path. Validated
  SF10 −10% / SF100 −23% suite wall. `--peer-exchange-addr` (default
  `:0`) / `--peer-exchange-advertise` control the FetchShuffle listener
  when firewalls need a pinned port.
- `--late-materialization` (default **true**): inner/left hash-join
  output rides view (dictionary) columns — the column gather is
  deferred to the first consumer that needs owned storage, and join
  chains compose the indirection so a passed-through column is copied
  once, at its final consumer or the shuffle encode. Validated
  2026-07-09: SF10 −6.2% / SF100 −4.9% suite wall, Q08 −36%/−44%,
  row-identical both scales. `=false` restores eager join-output
  gather (the A/B kill switch; benchmark arms prove engagement via
  `late_mat=true` dispatch logs and worker `late_mat_batches`
  counters). See docs/design/late-materialization.md.
- `--morsel-workers` (default **0 = auto** since PR #198, validated by
  the 2026-07-08 SF100 4-arm campaign: −3.4% suite, Q08 −40%): width
  adapts to fragment size and idle CPU tokens. `1` = serial (the kill
  switch). `N>1` = fixed width (benchmark/testing knob, bypasses the
  size gate).

### 1.7 Worker lifecycle on Kubernetes (graceful drain)

Workers support graceful drain: stop taking new tasks, finish in-flight
work, flush pending stage-output uploads to the object store (so
consumers keep their durable fallback once the peer-exchange server goes
away), then exit.

Signal contract (worker mode):

| Trigger | Behavior |
|---|---|
| `SIGTERM`, `SIGQUIT` | graceful drain, then exit |
| `SIGINT` | hard stop (in-flight tasks abort; the coordinator's retry machinery re-dispatches them) |
| `POST /drain` on the metrics port | same as SIGTERM (returns 202 immediately; drain proceeds) |
| NATS `wadjet.worker.<id>.drain` | same (the coordinator sends this when it reaps a stale worker) |

During drain the worker keeps heartbeating with `Draining=true`, which
excludes it from dispatch targeting without tripping the reaper, and it
keeps serving peer-exchange fetches until uploads are flushed.
`--drain-timeout` bounds the whole sequence (0 = unbounded); on timeout
the worker escalates to a hard stop. In-flight SF100-class tasks can run
for minutes — size the bound (and the pod's grace period) accordingly.

Probes on the metrics port (`--metrics-addr`, default `:9100`):
`GET /healthz` (liveness), `GET /readyz` (readiness; 503 once draining).

```yaml
# Pod spec sketch
terminationGracePeriodSeconds: 900   # >= --drain-timeout + margin
containers:
- name: wadjet-worker
  args: ["serve", "--mode=worker", "--drain-timeout=10m", ...]
  livenessProbe:  { httpGet: { path: /healthz, port: 9100 } }
  readinessProbe: { httpGet: { path: /readyz,  port: 9100 } }
```

### 1.8 Edge federation

Edge clusters connect to central via NATS leaf nodes:
`--leaf-remote=nats://central:7422` (repeatable), `--cluster-id=<name>`,
mTLS via `--nats-tls-cert/-key/-ca`. See `distributed.md`.

### 1.9 Benchmarks

Use `deploy/benchmark/` (OpenTofu + `run-benchmark.sh`); see its
README. Local gate first: `cmd/tpch-harness --mode=local` (~11s)
catches distributed regressions before any EC2 spend. Disable
`--background-compaction=false` for comparable timings.

## 2. Flag reference (serve-relevant, as of 2026-07-04)

"Runtime?" = can this become a live switch without a restart, given how
the value is consumed today. **Today every flag is process-start only**
— there is no dynamic-config subsystem (see §3). The column records
what a SET-style facility could expose cheaply vs never.

| Flag | Default | What it does | Runtime? |
|---|---|---|---|
| `--mode` | standalone | standalone / coordinator / worker | boot-only (structural) |
| `--storage-type`, `--endpoint`, `--bucket`, `--region`, `--ssl`, `--access-key`, `--secret-key` | s3 / localhost:9000 / wadjet | object-store binding | boot-only (clients built at start) |
| `--data-dir` | — | FileStore root (`--storage-type=file`) | boot-only |
| `--pg-addr`, `--http-addr`, `--grpc-addr`, `--metrics-addr`, `--data-plane-addr`, `--peer-exchange-addr` | :5433 / :8080 / :9090 / :9100 / :9091 / :0 | listeners | boot-only (bound at start) |
| `--pg-tls-*`, `--nats-tls-*` | — | TLS material | boot-only (cert reload would be its own feature) |
| `--nats-url`, `--nats-port`, `--nats-store-dir` | — / 4222 | control plane | boot-only |
| `--data-plane`, `--coord-data-plane` | nats | task/result transport | boot-only (streams established at start) |
| `--cluster-id`, `--leaf-remote` | local | federation topology | boot-only |
| `--memory-budget` | 0 = auto | per-task budget; also drives GOMEMLIMIT derivation | boot-only today (tracker budgets + GOMEMLIMIT cached at start) |
| `--shared-pool-budget` | 0 = auto | worker-wide reservation pool | boot-only today (same) |
| `--cache-bytes` | 0 = auto (20% mem) | LRU file cache size | boot-only (sized at start) |
| `--spill-dir` | OS temp | spill volume | boot-only |
| `--max-concurrent` | 4 | task slots per worker (memory-owner count) | feasible-with-work (semaphore is sized at Start; needs a resizable gate) |
| `--max-concurrent-queries` | 0 | coordinator query gate | **feasible** (admission check) |
| `--query-timeout` | 0 | default per-query timeout | **feasible** (read per query) |
| `--morsel-workers` | 0 = auto | intra-task parallel width policy | **feasible** (policy read per fragment via `Executor.SetMorselWorkers`; needs atomic field + propagation) |
| `--late-materialization` | true | view-column join output vs eager gather | **feasible** (read per query on the coordinator; rides the join-probe op spec) |
| `--local-fastpath-bytes` | 64 MiB | in-process routing threshold | **feasible** (read per query on the coordinator) |
| `--streaming-exchange` | true | peer-fetch shuffle vs S3-only | **feasible** (per-stage decision; falls back safely by design) |
| `--mmap-relief`, `--mmap-relief-threshold-mb` | true / 0 = auto | RSS-ceiling MADV relief | **feasible** (periodic sweep reads config) |
| `--bounded-dirty-writes` | true | windowed sync_file_range on spill/stage writes | **feasible** (applies to newly opened writers) |
| `--spill-floating-budget` | false | floating spill threshold | feasible but **do not enable** (see §1.5) |
| `--background-compaction` | true | 5m small-file compaction sweep | **feasible** (ticker gate) |
| `--enable-alerts` | false | CREATE ALERT DDL + scheduler | **feasible** (scheduler gate) |
| `--catalog-snapshot-s3-prefix`, `--catalog-snapshot-interval`, `--force-restore-catalog` | — / 5m / — | catalog snapshot/restore | interval feasible; prefix/restore boot-only |
| `--result-store` | 512 MB | in-memory result store cap | feasible-with-work (store sized at start) |
| `--log-level` | info | slog level | **feasible** (slog LevelVar pattern) |
| `--otel-endpoint`, `--otel-insecure` | — | tracing exporter | boot-only |
| `--geoip-city`, `--geoip-asn` | — | GeoIP databases | feasible-with-work (reload) |
| `--config` | — | YAML file; **only the `auth:` and `geoip:` sections are applied at runtime**. The `storage:`/`nats:`/`http:`/`grpc:`/`worker:`/`parquet:`/`query_limits:` sections parse but never reach the running process — use the flags above. See `configuration.md`. | n/a |

## 3. Runtime switches: current state and the path there

**Today: none.** Every knob above is a process-start flag; changing any
of them means restarting the process (workers restart cheaply — tasks
are re-dispatched by the retry machinery, and catalog snapshots make
coordinator restarts fast — but it is still a restart).

The engine already has both halves of a natural dynamic-config design:

1. **A SQL surface on the coordinator** (pgwire): `SET wadjet.<knob> =
   <value>` session-scoped, `ALTER SYSTEM SET wadjet.<knob>` (or
   similar) cluster-scoped — the Postgres-idiomatic shape, usable from
   psql/JDBC/Superset.
2. **NATS KV with watchers** for propagation: the catalog and UDF
   registries already use exactly this pattern (KV bucket + revision
   CAS + worker-side watch). A `config` KV bucket that workers watch
   and apply to the **feasible** subset in §2 is a small, well-trodden
   extension.

The feasible subset is exactly the flags whose values are read
per-query, per-fragment, or per-sweep from a field (marked **feasible**
above): morsel width, fastpath threshold, streaming exchange, relief
ceiling, dirty-write bounding, compaction/alert/ snapshot cadence,
query gates, log level. The structural set (addresses, transports,
storage bindings, memory envelope) should stay boot-only — pretending
those are live switches invites half-reconfigured processes.

Not built yet; scoped as a small standalone workstream if wanted.
