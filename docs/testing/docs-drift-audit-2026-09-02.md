# User-facing documentation drift audit — 2026-09-02

**Commit audited: `9786ed72`.** Every claim below was checked against the code
at that commit. Where a doc contradicts `CLAUDE.md`, the CODE decides and both
are noted.

**Scope.** `README.md` and every file directly under `docs/`. Not in scope
(maintained per arc): `docs/adr/`, `docs/design/`, `docs/internals/`,
`docs/benchmarks/`, `docs/testing/`.

**Status vocabulary.**

| Status | Meaning |
|---|---|
| CORRECT | the code does what the doc says |
| DRIFTED | the code does something different now |
| STALE-NUMBER | a count/version/threshold that is simply out of date |
| UNSUPPORTED-CLAIM | asserted but nothing in the tree backs it |
| MISSING-FEATURE | the doc promises something that does not exist |
| DECISION-NEEDED | a product call, not a documentation call — left unedited |

Rows that only restate prose with nothing checkable in it are omitted. Runs of
obviously-correct claims of the same kind are grouped into one row.

---

## `README.md`

| line | claim | status | truth at 9786ed72 |
|---|---|---|---|
| 9 | "the coordinator plans queries and schedules tasks but **never touches data bytes**" | DRIFTED | the small-query local fast path executes the whole query **in-process on the coordinator** (`internal/coordinator/local_fastpath.go:72-78`, dispatched at `internal/coordinator/coordinator.go:951`), and the coordinator reads stage outputs directly (`internal/coordinator/stage_read.go:53`) |
| 10 | "viable at 512 MB RAM with spill-to-disk. Scale to zero, start in under 2 seconds." | UNSUPPORTED-CLAIM | no banked measurement of process start time anywhere in the tree |
| 13 | "**80+** network functions" | STALE-NUMBER | ~109 network-family names in `expr.DefaultRegistry`; line 167 of this same file already says "100+" |
| 15 | "`read_json()`, `read_csv()`, `read_parquet()` … glob patterns and named parameters" | CORRECT | `internal/planner/physical/table_func.go:34-46` |
| 16 | "**11 functions** for IP geolocation … and ASN lookup" | CORRECT | exactly 11 `geoip_*` names registered |
| 167 | "Network & protocol \| 100+" | CORRECT | ~109 |
| 168 | "GeoIP / ASN \| 11" | CORRECT | 11 |
| 169 | "Vector & embeddings \| 5 + `embed()`" | CORRECT | 5 vector functions in `expr.DefaultRegistry` plus `embed`/`embed_model`/`embed_dim` (`internal/embedding/func.go:33-35`) |
| 170 | "Date/time \| 30+" | CORRECT | ~35 |
| 171 | "String \| 45+" | CORRECT | ~50 |
| 172 | "Aggregate \| **23**" | STALE-NUMBER | **28** (`internal/planner/sql/ast.go:565-600`) |
| 182 | "SELECT, EXPLAIN, DESCRIBE, CREATE TABLE, DROP TABLE" | DRIFTED | the parser also accepts INSERT, UPDATE, DELETE, MERGE, ALTER, SHOW, ANALYZE, CREATE VIEW, CREATE FUNCTION and CREATE ALERT (`internal/planner/sql/lexer.go:175-246`, `internal/planner/sql/parser.go:34, 55, 117, 205`) |
| 183-193 | CTEs, set ops, all join kinds, subqueries, window frames, GROUPING SETS/CUBE/ROLLUP, DECIMAL, nested types, table functions, VECTOR(N), `embed()` | CORRECT | `internal/planner/sql/select_parser.go:269-292`; `internal/engine/batch/decimal.go`; `internal/engine/expr/vector_funcs.go` |
| 194 | "**280+** built-in scalar functions" | STALE-NUMBER | **358** in `expr.DefaultRegistry` (361 once `internal/embedding/func.go:33-35` registers `embed`/`embed_model`/`embed_dim`) |
| 195 | "**23** aggregate functions" | STALE-NUMBER | 28 |
| 196 | "User-defined functions (CREATE FUNCTION)" | CORRECT | `internal/planner/sql/parser.go:117-118` |
| 234-239 | 2048-row batches; push-based; typed kernels; 3-level pushdown; CBO; spill everywhere (join/aggregate/sort/window) | CORRECT | `internal/engine/batch/batch.go:11`; `internal/engine/exec/join_spill.go`, `partitioned_agg.go`, `sort_external.go`, `window_external.go` |
| 240 | "Morsel-driven parallelism (`--morsel-workers=0`) … **opt-in**" | DRIFTED | `0 = auto` is the **default** since the 2026-07-08 flip pair; `1` is the serial kill switch (`cmd/wadjet/main.go:183`) |
| 254 | "custom direct-to-columnar byte scanner (**8x faster** than `encoding/json`)" | UNSUPPORTED-CLAIM | `BenchmarkReader_RowOriented` / `BenchmarkReader_Columnar` exist (`internal/storage/json/columnar_test.go:350, 371`) but no ratio is banked anywhere |
| 263 | "**Apache Iceberg** metadata reading — register external Iceberg tables and query them via the catalog" | DRIFTED | `internal/iceberg/catalog.go:38` `RegisterTable` and `:61` `DiscoverAndRegister` are a **Go API with no caller outside `internal/iceberg`** — no SQL statement, CLI subcommand or server path registers an Iceberg table |
| 270-275, 277-279 | stage-DAG, streaming exchange, 64 MiB fast path, broadcast + probe-split, split control/data plane, memory-aware scheduling, catalog snapshots, federation, embedded NATS | CORRECT | `cmd/wadjet/main.go:194-198`; `internal/coordinator/local_fastpath.go:78`; `cmd/wadjet/main.go:1840` |
| 276 | "Graceful worker drain … Kubernetes-ready with `/healthz`, `/readyz`, and `POST /drain`" | CORRECT | `cmd/wadjet/main.go:1747-1767`, inside `runWorker`, on `--metrics-addr` |
| 300 | "Kubernetes-compatible probes on **every process** (`/healthz`, `/readyz`)" | DRIFTED | those routes are registered **only in `runWorker`** (`cmd/wadjet/main.go:1631, 1746-1767`) on `--metrics-addr` (`:9100`). Coordinator and standalone expose `/v1/health` and `/v1/ready` on the HTTP API instead (`internal/server/server.go:110-111`) |
| 306-311, 313-439 | benchmark prose and both result tables | CORRECT | the audited commit *is* the SF100 baseline commit; `docs/benchmarks/sf100-baseline-v0.18.12-2026-09-02.md` exists, and the ClickBench block already carries its own "not re-run since v0.17.0-clawback" caveat |
| 441-455 | the four benchmark commands and every path they name | CORRECT | `cmd/tpch-harness`, `deploy/benchmark/terraform`, `deploy/benchmark/terraform-clickbench`, `benchmarks/clickbench/rank.py`, `benchmarks/tpch/fingerprint-sf100.json` all exist |
| 469 | "The MCP server communicates over **stdio only** — there is no network listener." | CORRECT | `internal/server/mcp/server.go` |
| 500 | "The MCP server exposes **5 tools**" | STALE-NUMBER | **7** (`internal/server/mcp/server.go:355, 360, 375, 398, 417, 422, 427`) |
| 502-508 | the 5-row tool table | DRIFTED | omits `list_alerts` (`:422`) and `describe_alert` (`:427`), both returned unconditionally by `toolDefinitions()` |
| 517 | `import "github.com/derekmwright/wadjet/wadjet"` | CORRECT | `go.mod:1` plus the `wadjet/` package directory |
| 519-522 | `wadjet.Open(ctx, wadjet.Config{StorageEndpoint: "localhost:9000", Bucket: "analytics"})` | DRIFTED | **does not compile.** `wadjet.Config` (`wadjet/wadjet.go:44-68`) has no `StorageEndpoint` field; it takes `Store objstore.Store`. `docs/getting-started.md:155` shows the correct form |
| 530-547 | the documentation index | DRIFTED | omits `docs/performance-bottlenecks.md` and `docs/competitive-gaps.md`, both of which exist |
| 554-555 | the two TPC-H test commands | CORRECT | match `CLAUDE.md` and `.github/workflows/ci.yml:57` |

## `docs/README.md`

| line | claim | status | truth at 9786ed72 |
|---|---|---|---|
| 9-23 | the 14-row documentation index | DRIFTED | four docs in the same directory are unlisted: `runbook.md`, `disaster-recovery.md`, `performance-bottlenecks.md`, `competitive-gaps.md` |
| 37 | "Go Package: `github.com/derekmwright/wadjet/wadjet`" | CORRECT | `go.mod:1` |

## `docs/configuration.md`

| line | claim | status | truth at 9786ed72 |
|---|---|---|---|
| 11-14, 16, 19-20, 22-23, 25, 27 | `--mode` standalone, `--http-addr` `:8080`, `--grpc-addr` `:9090`, `--storage-type` s3, `--endpoint` localhost:9000, `--bucket` wadjet, `--nats-port` 4222, `--cluster-id` local, `--leaf-remote`, `--spill-dir` OS temp, `--config` | CORRECT | `cmd/wadjet/main.go:145-159, 172` |
| 21 | "`--nats-url` … Default `nats://localhost:4222`" | DRIFTED | the flag default is the empty string (`cmd/wadjet/main.go:157`) |
| 24 | "`--memory-budget` … (0 = unlimited, no spill) \| `0`" | DRIFTED | `0 = auto-detect from cgroup, or unlimited` (`cmd/wadjet/main.go:166`) — 0 does not mean "no spill" on a cgroup-limited host |
| 26 | "`--result-store` … (0 = disabled) \| `0`" | DRIFTED | the default is **512 MiB** (`cmd/wadjet/main.go:175`, `512*1024*1024`) |
| 9-27 | the `serve` flag table as a whole | DRIFTED | omits `--pg-addr` (default `:5433`, `cmd/wadjet/main.go:176`) — the PostgreSQL wire port — along with `--metrics-addr`, `--max-concurrent`, `--cache-bytes`, `--query-timeout`, `--morsel-workers`, `--local-fastpath-bytes`, `--shuffle-durability`, `--skew-split`, `--drain-timeout` |
| 33-37 | `query` command flags; `--format` default `json` | CORRECT | `cmd/wadjet/main.go:570` |
| 51 | `shell` `--format` default `table` | CORRECT | `cmd/wadjet/main.go:874` |
| 60-95, 97-180 | every YAML key shown (`mode`, `storage.*`, `nats.*`, `http.addr`, `grpc.addr`, `worker.*`, `parquet.*`, `auth.*`, `abac_policies.*`) | CORRECT | `internal/config/config.go:16-182` — every key matches a `yaml:` tag |
| 87, 93-95 | `cache_bytes: 268435456`; `compression: snappy`; `row_group_size: 131072`; `page_buffer_size: 262144` | CORRECT | `internal/config/config.go:205-213` `DefaultConfig()` |
| 207-209, 211-212 | ingest flush 128 MB / 1,000,000 rows / 60 s; page buffer 256 KB; Snappy | CORRECT | `internal/storage/ingest/ingest.go:28-40` |
| 210 | "Parquet row group size \| **128,000 rows**" | DRIFTED | `128 * 1024` = **131,072** (`internal/storage/ingest/ingest.go:38`, `internal/config/config.go:211`) |
| 218-220, 222, 225 | batch size 2,048; worker concurrency 4; worker cache 256 MB; spill dir OS temp; heartbeat 10 s | CORRECT | `internal/engine/batch/batch.go:11`; `cmd/wadjet/main.go:182`; `internal/config/config.go:207`; `internal/worker/worker.go:2466` |
| 221 | "Memory budget \| **0 (unlimited)**" | DRIFTED | 0 auto-detects a cgroup limit when one is present (`cmd/wadjet/main.go:166`) |
| 223 | "Result store \| **0 (disabled)**" | DRIFTED | 512 MiB (`cmd/wadjet/main.go:175`) |
| 224 | "Inline result threshold \| **64 KB**" | DRIFTED | **512 KB** (`internal/worker/executor.go:39`, `const inlineResultThreshold = 512 * 1024`) |
| 226 | "Batch pool max per class \| **16**" | DRIFTED | `runtime.NumCPU() * 4`, clamped to [32, 256] (`internal/engine/batch/pool.go:29-40`) |
| 237-242, 249-270 | `query_limits` global and per-role YAML | CORRECT | `internal/config/config.go:44-49, 94` |
| 276 | "**All** configuration can be overridden via `WADJET_*` environment variables" | UNSUPPORTED-CLAIM | only the variables enumerated in `applyEnvOverrides` are read (`internal/config/config.go:280-378`); there is no generic mapping |
| 282-283 | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | CORRECT | honoured through the MinIO credential chain when `--access-key`/`--secret-key` are empty (`internal/storage/objstore/minio.go:92-105`) |
| 284 | "`AWS_ENDPOINT_URL_S3` \| `--endpoint`" | MISSING-FEATURE | never read; the endpoint comes from `--endpoint` or `WADJET_STORAGE_ENDPOINT` (`internal/config/config.go:293`) |
| 285 | "`WADJET_BUCKET`" | DRIFTED | the variable is `WADJET_STORAGE_BUCKET` (`internal/config/config.go:302`) |
| 291-293 | `WADJET_HTTP_ADDR`, `WADJET_GRPC_ADDR`, `WADJET_MODE` | CORRECT | `internal/config/config.go:281-289` |
| 294 | "`WADJET_MAX_CONNECTIONS`" | MISSING-FEATURE | not read anywhere in the tree |
| 295 | "`WADJET_SLOW_QUERY_THRESHOLD`" | MISSING-FEATURE | not read anywhere in the tree |
| 296 | "`WADJET_SHUTDOWN_TIMEOUT`" | MISSING-FEATURE | not read; the drain bound is the `--drain-timeout` flag (`cmd/wadjet/main.go:184`) |
| 302-303 | `WADJET_NATS_PORT`, `WADJET_NATS_URL` | CORRECT | `internal/config/config.go:310-316` |
| 304 | "`WADJET_CLUSTER_ID`" | DRIFTED | the variable is `WADJET_NATS_CLUSTER_ID` (`internal/config/config.go:319`) |
| 310 | `WADJET_WORKER_MAX_CONCURRENT` | CORRECT | `internal/config/config.go:334` |
| 311 | "`WADJET_WORKER_CACHE_BYTES`" | MISSING-FEATURE | not read; set the cache with `--cache-bytes` (`cmd/wadjet/main.go:173`) or `worker.cache_bytes` |
| 312 | "`WADJET_MEMORY_BUDGET`" | DRIFTED | the variable is `WADJET_WORKER_MEMORY_BUDGET` (`internal/config/config.go:339`) |
| 313 | "`WADJET_RESULT_STORE_BYTES`" | MISSING-FEATURE | not read; use `--result-store` or `worker.result_store_bytes` |
| 319-321 | `WADJET_QUERY_MAX_SCAN_BYTES` / `_ROWS` / `_FILES` | CORRECT | `internal/config/config.go:353-367` |
| 327-328 | "`WADJET_RATE_LIMIT_RPS`", "`WADJET_RATE_LIMIT_BURST`" | MISSING-FEATURE | neither is read; no rate limiter is wired into any server path |

## `docs/distributed.md`

| line | claim | status | truth at 9786ed72 |
|---|---|---|---|
| 33 | "Routes to probe-split pipeline (preferred) or single-worker pipeline (fallback)" | DRIFTED | the coordinator routes to the **in-process local fast path** when post-pruning scan bytes stay under `--local-fastpath-bytes` (64 MiB default), otherwise to the **multi-stage DAG executor** (`internal/coordinator/coordinator.go:942-951, 1058`). Probe-split is one join strategy inside the DAG, not the routing decision |
| 39, 45-52 | coordinator embeds NATS; worker duties; heartbeat every 10 s | CORRECT | `internal/worker/worker.go:2466` |
| 50 | "results < **64 KB** are inlined" | DRIFTED | **512 KB** (`internal/worker/executor.go:39`) |
| 57-59 | `tasks` stream; result subjects; heartbeat subjects | CORRECT | `internal/distributed/subjects.go:7-20` |
| 66-95, 408-434 | every `wadjet serve` flag used in the coordinator / worker / federation examples | CORRECT | all exist in `cmd/wadjet/main.go:144-215` |
| 101-171 | the Docker Compose example (`build: .`) | MISSING-FEATURE | there is **no Dockerfile in the repository**, so `build: .` cannot succeed |
| 193, 246 | `image: ghcr.io/derekmwright/wadjet:latest` | MISSING-FEATURE | no Dockerfile and no workflow publishes a container image (`.github/workflows/` holds only `ci.yml`, `issue-worker.yml`, `pr-review.yml`) |
| 324-331 | task-type table (`scan`, `aggregate`, `join`, `sort`, `window`) | DRIFTED | `shuffle` is a sixth task subject (`internal/distributed/subjects.go:12`) and carries the exchange-repartition stages |
| 332 | "**Sort, aggregate, and window** tasks spill to disk" | DRIFTED | hash **join** spills too — grace partition-on-arrival (`internal/engine/exec/join_spill.go`) |
| 342 | "When operators (Sort, HashAggregate, Window) exceed the budget, they spill" | DRIFTED | same: every pipeline breaker spills, HashJoin included (ADR-0027) |
| 350 | "Results below **64 KB** are always passed inline via NATS" | DRIFTED | 512 KB (`internal/worker/executor.go:39`) |
| 354 | "LRU cache (**256 MB default**)" | DRIFTED | that is the YAML default (`internal/config/config.go:207`); the `--cache-bytes` flag default is `0` = auto-detect 20% of memory (`cmd/wadjet/main.go:173`) |
| 372-375 | fault-tolerance behaviours | CORRECT | consistent with the DAG's S3-materialized stage outputs |
| 399-403, 442, 448 | `--cluster-id` routing; `wadjet.tasks.<cluster-id>.<type>.<query-id>.<stage-id>`; `wadjet.tasks.<cluster-id>.>` | CORRECT | `internal/distributed/subjects.go:147-154` — exact match |

## `docs/tuning.md`

| line | claim | status | truth at 9786ed72 |
|---|---|---|---|
| 9 | "Worker concurrency \| CLI Flag **—**" | DRIFTED | `--max-concurrent` exists, default 4 (`cmd/wadjet/main.go:182`) |
| 10 | "LRU cache size \| CLI Flag **—** \| 256 MB" | DRIFTED | `--cache-bytes` exists (`cmd/wadjet/main.go:173`) and its default is `0` = auto-detect 20% of memory; 256 MB is the YAML default only |
| 11 | "Memory budget \| … \| **0 (unlimited)**" | DRIFTED | 0 auto-detects a cgroup limit when one is present (`cmd/wadjet/main.go:166`) |
| 12 | spill directory default OS temp dir | CORRECT | `cmd/wadjet/main.go:172` |
| 13 | "Result store \| … \| **0 (disabled)**" | DRIFTED | 512 MiB (`cmd/wadjet/main.go:175`) |
| 14, 16-19 | compression snappy; page buffer 256 KB; ingest 128 MB / 1,000,000 / 60 s | CORRECT | `internal/config/config.go:210-212`; `internal/storage/ingest/ingest.go:28-40` |
| 15 | "Row group size \| … \| **128,000 rows**" | DRIFTED | 131,072 (`internal/config/config.go:211`) |
| 27-52, 76-101, 124-149, 174-195 | every YAML key and CLI flag in the three environment profiles and the federation example | CORRECT | all keys exist in `internal/config/config.go:171-182`; all flags in `cmd/wadjet/main.go:144-215` |
| 217-218, 221, 224, 227, 280, 283, 286, 307, 310, 313 | every PromQL metric name (`wadjet_query_duration_seconds`, `wadjet_worker_task_duration_seconds`, `wadjet_scan_files_pruned_total`, `wadjet_scan_files_scanned_total`, `wadjet_cache_hits_total`, `wadjet_cache_misses_total`, `wadjet_worker_spill_events_total`, `wadjet_worker_spill_bytes_written_total`, `wadjet_worker_memory_used_bytes`, `wadjet_worker_memory_budget_bytes`) | CORRECT | `internal/metrics/metrics.go:61-231` — every name matches namespace + subsystem + name exactly |
| 291 | "operators (Sort, HashAggregate, Window) spill intermediate state to disk" | DRIFTED | hash join spills too (`internal/engine/exec/join_spill.go`) |
| 294 | "`memory_budget = 0` → unlimited memory, **no spill**" | DRIFTED | 0 auto-detects a cgroup limit; only an uncapped host gets unlimited (`cmd/wadjet/main.go:166`) |
| 322-347, 349-363, 365-375, 377-390 | result-store, cache and Parquet sizing guidance | CORRECT | advisory prose over real knobs |

## `docs/api-reference.md`

| line | claim | status | truth at 9786ed72 |
|---|---|---|---|
| 11, 19 | HTTP default `:8080`; `Authorization: Bearer <key>` | CORRECT | `cmd/wadjet/main.go:154`; `internal/auth/auth.go:193-199` |
| 39, 111, 135, 174, 199, 227, 250, 264, 283, 301 | the ten documented routes | CORRECT | all registered at `internal/server/server.go:100-117` |
| 56 | "`Content-Type` \| Required: **Yes**" | UNSUPPORTED-CLAIM | the body is decoded unconditionally; no route checks Content-Type (`internal/server/server.go:219-222`) |
| 63-87 | `query_id` / `columns` / `rows` / `stats.*` field names | CORRECT | `internal/server/server.go:203-215` |
| 86 | "`stats.rows_scanned` \| Total rows read from storage" | DRIFTED | true only on the embedded no-coordinator path. On the coordinator path — every `wadjet serve` mode — it carries `result.TotalRows`, the **output** row count (`internal/server/server.go:425`; `internal/coordinator/coordinator.go:1118-1128`) |
| 142-159 | the `GET /v1/tables/{name}` response body | DRIFTED | the handler serializes `catalog.TableMeta` verbatim (`internal/server/server.go:915`), which also carries `created_at`, `updated_at`, `version` and per-column `nullable`; `parquet.TypeID` has no `MarshalJSON`, so `type` is a **number**, not `"Timestamp"`. The example is also uncreatable: every partition key must be a schema column (`internal/storage/catalog/catalog.go:294-298`) and there is no `date` column |
| 163-167 | the 404 body | DRIFTED | the message is `table "nonexistent_table" not found` (`internal/storage/catalog/catalog.go:368, 375`) |
| 179-192 | the `GET /v1/queries` response | DRIFTED | the handler also returns `count`, and each entry is a full `QueryStatusResponse` with `sql`, `elapsed`, `total_rows` (`internal/server/async.go:19-27, 194-208`) |
| 204 | "Async query endpoints … require distributed mode … In standalone mode, these endpoints return `503`" | DRIFTED | `runStandalone` builds a coordinator and wires it in (`cmd/wadjet/main.go:1131`, `:1223`). The 503 comes from `s.coord == nil` (`internal/server/async.go:40-44`), which only happens on the embedded-library path. The note also omits `GET /v1/queries`, which has the same guard (`async.go:186-190`) |
| 214-220 | the async-submit 202 body | DRIFTED | `AsyncQueryResponse` also carries `state` (always `"running"`) and `plan` (`internal/server/async.go:12-16, 62-66`) |
| 232-241 | the query-status body | DRIFTED | `QueryStatusResponse` also carries `sql` and `stages` (`internal/server/async.go:19-27`) |
| 243 | states `pending`, `running`, `completed`, `failed`, `cancelled` | CORRECT | `internal/coordinator/query_tracker.go:22-36` |
| 288-294, 116-122 | `/v1/health` → `{"status":"ok"}`; `/v1/tables` → `{"tables":[...]}` | CORRECT | `internal/server/server.go:918-920, 895` |
| 308-321 | the Prometheus scrape example | DRIFTED | `wadjet_rows_scanned_total` does not exist — the metric is `wadjet_query_rows_scanned_total{table=…}`; `wadjet_queries_total` carries a `status` label and `wadjet_query_duration_seconds` a `type` label with buckets `{0.01,0.05,0.1,0.5,1,5,10,30,60,120}` (`internal/metrics/metrics.go:61-78`) |
| 39-304 | the endpoint list as a whole | MISSING-FEATURE | ten registered routes are undocumented: `POST /v1/tables`, `DELETE /v1/tables/{name}`, `GET /v1/ready`, `GET /v1/dlq`, `GET /v1/dlq/{entryID}`, `DELETE /v1/dlq`, `/debug/pprof/*` (`internal/server/server.go:107-127`), plus the admin API (`internal/server/admin.go:36-44`) |
| 464 | "no built-in rate limiting … no request timeouts" | DRIFTED | rate limiting is right (`internal/auth/ratelimit.go` is unwired), but the server does set `ReadTimeout` 30 s / `WriteTimeout` 5 m / `IdleTimeout` 120 s and honours `server.Config.MaxConnections` (`internal/server/server.go:151-163`) |

## `docs/grpc-api.md`

| line | claim | status | truth at 9786ed72 |
|---|---|---|---|
| 7, 9 | proto at `proto/wadjet/v1/wadjet.proto`; generated Go in `gen/wadjet/v1/`; default `:9090` | CORRECT | both paths exist; `cmd/wadjet/main.go:155`; `internal/config/config.go:165-167` |
| 13, 37, 39 | bearer token in `authorization` metadata; health RPCs bypass auth; `UNAUTHENTICATED` otherwise | CORRECT | `internal/server/grpc.go` `grpcAuthenticateContext` |
| 48, 79, 91, 103, 117, 127, 137, 147, 159 | all nine RPC signatures | CORRECT | `proto/wadjet/v1/wadjet.proto:11-38` — exact match |
| 58-69, 76, 82, 106, 108, 156, 166-174 | response fields; 1000-row stream batches; state list; `if_exists`; health service names | CORRECT | `wadjet.proto:44-64, 84-90, 152-155`; `internal/server/grpc.go:31` `streamBatchSize = 1000` |
| 15-20, 168-170 | bare `grpcurl` examples | DRIFTED | the server never registers `grpc.reflection` (absent from `internal/`, `cmd/` and `go.sum`), so every reflection-based `grpcurl` fails with "server does not support the reflection API". Line 17 also omits `-plaintext` |
| 86, 94 | "async, distributed mode only … Returns `UNAVAILABLE` in standalone mode" | DRIFTED | standalone wires a coordinator into the gRPC server (`cmd/wadjet/main.go:1131`, `:1320`). `UNAVAILABLE` only fires on the embedded path where `Coord` is nil |
| 150 | the column-type list for `CreateTable` | MISSING-FEATURE | `parquet.DeclaredColumn` also accepts `DECIMAL(p,s)`/`NUMERIC(p,s)`, `VECTOR(N)`, `DATE`, `UUID`, `ARRAY(T)`, `ROW(...)`, `MAP(K,V)` and many aliases (`internal/storage/parquet/schema.go:110-148, 241-334`) |
| 195-201, 243-244 | the Python and Java `protoc` commands | DRIFTED | with `-Iproto` the file argument must be relative to the include root (`wadjet/v1/wadjet.proto`), not `proto/wadjet/v1/wadjet.proto`; as written protoc errors "File does not reside within any path specified using --proto_path". Compare the working `Taskfile.yml` `proto:` task |
| 259 | "`task proto`" | CORRECT | `Taskfile.yml:66` |
| 261-264 | the "manual" regeneration command | DRIFTED | with no `-I`, `paths=source_relative` writes `proto/wadjet/v1/wadjet.pb.go`, not `gen/wadjet/v1/` where the committed code and the `go_package` option live |
| 275 | "`NOT_FOUND` \| … query ID not found" | DRIFTED | `CancelQuery` maps an unknown query ID to `INTERNAL`, not `NOT_FOUND` (`internal/server/grpc.go`) |
| 183-189, 249-253, 271-277 | Go client snippet; Rust `tonic_build`; the rest of the error-code table | CORRECT | `gen/wadjet/v1/wadjet_grpc.pb.go:63`; `internal/server/grpc.go` |

## `docs/getting-started.md`

| line | claim | status | truth at 9786ed72 |
|---|---|---|---|
| 7, 21-23, 29 | Go 1.26+; the `-o wadjet-bin` warning; `go get …/wadjet` | CORRECT | `go.mod:3` `go 1.26.1` |
| 34, 43-49 | table functions need no store; `query --format table`; `~/`, glob and HTTP paths | CORRECT | `cmd/wadjet/localquery.go:14-20, 35-49`; `internal/planner/physical/table_func.go:84-87, 261-262, 358-369, 394` |
| 78-84, 92-100, 105-112, 117-122 | `serve`, `query`, `shell`, `tables` flags and format defaults | CORRECT | `cmd/wadjet/main.go:145-154, 570, 873, 574-596` |
| 145 | `objstore.NewMinIOStore(ctx, objstore.MinIOConfig{…})` | DRIFTED | **does not compile.** The signature is `NewMinIOStore(cfg MinIOConfig)` — one argument (`internal/storage/objstore/minio.go:94`) |
| 164-178 | the `CreateTable` example | DRIFTED | fails at run time: `partition key "date" not found in schema` — the schema shown has no `date` column and `catalog.CreateTable` refuses that (`internal/storage/catalog/catalog.go:294-298`) |
| 191-202 | the `ingester.Ingest` example | DRIFTED | fails at run time: `missing partition key "date" in row` — every row must carry each partition key (`internal/storage/ingest/ingest.go:267-274`) |
| 155-158, 183-187, 191, 208-212, 217 | `wadjet.Open(ctx, wadjet.Config{Store, Bucket})`; `NewIngester` returns no error; `ingest.Config`; `Start`/`FlushAll`/`Stop`; `db.Query` → `result.Rows` | CORRECT | `wadjet/wadjet.go:44-46, 72, 219, 300`; `internal/storage/ingest/ingest.go:19-25, 95, 116, 254, 303` |
| 228-232, 253, 277-281 | `curl POST /v1/queries`; gRPC `:9090`; the four "next steps" links | CORRECT | `internal/server/server.go:100`; all four docs exist |
| 236-249 | the HTTP response example | DRIFTED | inherits the `stats.rows_scanned` drift above — on the coordinator path it is the result row count (`internal/server/server.go:425`) |
| 257-264, 269-271 | the two `grpcurl` examples and the Python `protoc` command | DRIFTED | no gRPC reflection is registered; and the `protoc` file argument must be relative to `-Iproto` |

---

## Five cross-cutting defects

These recur across `network-analytics.md`, `embedding.md`, `ingestion.md` and
`getting-started.md`, and are stated once here.

**X1 — `internal/…` packages cannot be imported by an out-of-tree module.**
`internal/` sits directly under the module root (`go.mod:1`), so only code
inside `github.com/derekmwright/wadjet` may import it. The public `wadjet`
package re-exports nothing, yet `Config.Store` is `objstore.Store`
(`wadjet/wadjet.go:46`), `CreateTable` takes `parquet.Schema`
(`wadjet/wadjet.go:185`) and `NewIngester` takes `ingest.Config`
(`wadjet/wadjet.go:219`). **An external Go program cannot construct any of
them** — it fails to build with `use of internal package … not allowed`.

**X2 — `NewMinIOStore` takes ONE argument.**
`internal/storage/objstore/minio.go:94`: `func NewMinIOStore(cfg MinIOConfig)
(*MinIOStore, error)`. Every doc call site passes `ctx` first — a compile error.

**X3 — the embedded catalog is in-memory and process-local.** `wadjet.Open`
with no `MetaKV` uses `catalog.NewWithStore` → `NewMemKV`, a plain Go map
(`wadjet/wadjet.go:78`, `internal/storage/catalog/kv_mem.go:18`). Tables
registered by a helper process vanish at exit and are invisible to
`wadjet serve`, which builds a NATS-backed catalog (`cmd/wadjet/main.go:1069-1073`).

**X4 — table data must live under `tables/<name>/` and files must be
REGISTERED.** `partition.TablePrefix` = `"tables/" + tableName`
(`internal/storage/partition/partition.go:46`). The scan path resolves files
from the manifest only (`internal/engine/scan/scanner.go:154-166`) — there is
no prefix discovery anywhere in the query path. `CreateTable` writes an *empty*
manifest; externally written Parquet must be registered with `catalog.AddFiles`
(`internal/storage/catalog/catalog.go:625`).

**X5 — partition pruning fires ONLY for the key names `year`, `month`, `day`,
`hour`.** `internal/planner/logical/optimizer.go:17-19` defines exactly that
set, gated at `optimizer.go:2766`. A table partitioned by `date`, `device`,
`region` or `tenant_id` gets **no** pruning. Separately, a partition key is
referenceable in SQL only if it is also a declared schema column
(`internal/planner/physical/validate.go:579`); otherwise a `WHERE` on it is
SQLSTATE 42703 (`validate.go:294`).

---

## `docs/architecture.md`

| line | claim | status | truth at 9786ed72 |
|---|---|---|---|
| 9-19, 60-66, 69-81, 85, 103-105, 111-147, 165-184, 193-205, 213-224, 230-241, 271, 278-281, 303, 305-307, 319-324 | layer diagram; Parquet rationale and codec set; catalog bucket `wadjet_catalog` and key scheme; lock TTL 30 s / refresh 10 s; 2048-row batches; GlobalPool; pipeline stages; vectorized execution; the `objstore.Store` interface **method-for-method**; Iceberg v1/v2 and the two catalog calls; ingest thresholds; writer config; `worker.result_store_bytes`; spill metric names; heartbeat 10 s; subject format; concurrency 4; 256 MB LRU; every named auth mechanism | CORRECT | `internal/storage/parquet/writer.go:15-21, 66-70`; `internal/storage/catalog/kv_nats.go:11`, `locks.go:12, 126`; `internal/engine/batch/batch.go:11`, `pool.go:130-147`; `internal/storage/objstore/store.go:40-66`; `internal/iceberg/metadata.go:124-125`, `reader.go:144-147`, `catalog.go:19, 61`; `internal/storage/ingest/ingest.go:30-33`; `internal/metrics/metrics.go:184-211`; `internal/worker/worker.go:189, 191, 2466`; `internal/distributed/subjects.go:148-154`; `internal/auth/` |
| 29-53 | the `internal/` package-layout tree | MISSING-FEATURE | every listed path exists, but 19 packages are absent — including three the rest of the doc set gets wrong: `internal/alerts` (CREATE ALERT runtime), `internal/telemetry` (OTel), `internal/optswitch` (the 38-toggle kill-switch registry). Also missing: `benchnotify`, `dataplane`, `embedding`, `format`, `geoip`, `harness`, `logio`, `oracle`, `sqlerr`, `wshf`, `engine/diskio`, `storage/{compaction,csv,dbscan,json,partition}` |
| 88-95 | `RecordBatch` with `Vectors[]`, `RowCount`, `SelectionVector: []uint16` | DRIFTED | `internal/engine/batch/batch.go:14-18`: the fields are `Columns []*Vector`, `Schema []parquet.Column`, `Len int`, `Sel []uint32`. No `Vectors`, no `RowCount`, and the selection vector is **uint32** |
| 101 | "with up to **16** batches cached per size class" | DRIFTED | `runtime.NumCPU() * 4`, clamped to [32, 256] (`internal/engine/batch/pool.go:27-40`) — never 16 |
| 163 | "**Pipeline breakers** (aggregates, sorts)" | DRIFTED | Window is a breaker too (`internal/engine/exec/window.go`, `window_external.go`), and a hash join's build side blocks (`internal/engine/exec/join.go:85-87`) |
| 207-209 | "Two implementations: MemStore … MinIOStore" | DRIFTED | four in-package: `memstore.go:23`, `minio.go:45`, `filestore.go:24` (`FileStore` — the local-dev store CLAUDE.md names), `http_store.go` (read-only HTTP), plus the `CachedStore` and base-table-cache decorators |
| 249 | "When an operator (Sort, HashAggregate, Window) exceeds the budget, it spills" | DRIFTED | HashJoin spills too — grace partition-on-arrival (`internal/engine/exec/join.go:85-87`, `join_spill.go`, `join_partition_arrival.go`). ADR-0027 is the settled position: **every** pipeline breaker spills |
| 259 | "This makes workers viable at 512 MB - 2 GB RAM." | UNSUPPORTED-CLAIM | no banked measurement; the banked SF100 baseline runs 3× `c7gd.4xlarge` at 32 GB |
| 273 | "Results smaller than **64 KB** bypass both the result store and S3" | DRIFTED | 512 KB (`internal/worker/executor.go:39`). A stale comment at `internal/distributed/messages.go:1065` says "< 256 KB"; the constant is the authority |
| 302, 304 | workers "execute locally (scan, aggregate, join, sort, window)"; "**Task types**: `scan`, `aggregate`, `join`, `sort`, `window`" | DRIFTED | `internal/distributed/messages.go:14-17`: `pipeline`, `shuffle`, `gather`, `stage`. No task carries those five type strings; a `stage` task's operator kind is in `StageType` (`messages.go:341-344`) |

## `docs/performance-bottlenecks.md`

The most drifted doc in the set. Six of sixteen numbered bottlenecks have been
closed since it was written — including all three legs of its own "Recurring
Anti-Pattern" section and the top item of its own priority list. Almost every
`file:line` anchor is stale, and §1 names a file that no longer exists.

| line | claim | status | truth at 9786ed72 |
|---|---|---|---|
| 18-34 | "§1 Distinct Operator: Per-Row String Allocation" at `internal/engine/exec/distinct.go:61-71` | DRIFTED | **the file does not exist** and there is no `Distinct` operator. DISTINCT is planned as a keys-only hash aggregate (`internal/engine/exec/aggregate.go:153-157`), which inherits the spill machinery. COUNT(DISTINCT) state already uses the open-addressing sets this section asks for (`internal/engine/exec/distinct_set.go:7-30`) |
| 36-56 | "§2 Spill Sort Falls Back to Row-Oriented Format" | DRIFTED | closed. Each spill sorts with the typed kernels and writes a sorted **columnar** run; finalize streams a k-way merge (`internal/engine/exec/sort.go:181-199, 255-266`; `sort_external.go:14-36`) |
| 58-73 | "§3 Aggregate Spill Uses ToRows()" | DRIFTED | closed for the grouped path: partial group state spills in a columnar format (magic `"WAGS\x02"`, `internal/engine/exec/aggregate_partial_spill.go:19-48`) and an ungrouped aggregate never buffers input (`aggregate.go:1431-1452`). `ToRows` survives only for extra-state aggregates and GROUPING SETS (`aggregate.go:1455-1458`) |
| 77-87 | "§4 … `reader.go:86-120` … reads `ci.MinValue(p)` per page" | DRIFTED | the anchor and the evidence are both wrong: `RowGroupStats` is `internal/storage/parquet/file_reader.go:278-360` and reads **row-group-level** statistics; there is no page-index read to repurpose. The page-pruning gap itself is real, and two finer prunes landed meanwhile (`internal/engine/scan/dict_prune.go:10-30`, `row_filter.go:66`) |
| 91-106 | "§5 Full-File S3 Download Strategy" at `util.go:454-461` | DRIFTED | anchor stale (`util.go:464-471`) and half the gap is closed, half is a settled decision: local-fd stores pread each projected chunk (`util.go:404-433`) and the worker range-reads whenever columns are pruned (`internal/worker/executor.go:1867-1881`). The whole-file GET is kept **deliberately** for S3 (`util.go:414-415`) |
| 112-130 | "§6 Per-Row Type Dispatch in ColumnCompare Filter" | DRIFTED | anchors stale and the hot path is gone: `ColumnCompare` (`internal/engine/exec/filter.go:164-166`) has zero callers; literal conversion is hoisted out of the row loop (`filter.go:186-200`) |
| 134-150 | "§7 LIKE Pattern Matching: Exponential Backtracking" | DRIFTED | largely closed: `compileLikePattern` specializes prefix/suffix/contains/equality (`internal/engine/exec/kernel/compare.go:2024-2070`) and LIKE is pushed into the scan, evaluated once per dictionary entry (`internal/engine/scan/like_filter.go:5-50`). Residual: the unmemoized `matchLikeRecur` on the row-predicate fallback (`filter.go:471-490`) |
| 154-168 | "§8 Selection Vector Allocation in OFFSET/LIMIT" | CORRECT | still true; only the anchors are stale — `internal/engine/exec/limit.go:60, 90, 163` |
| 172-189 | "§9 No Column-Level I/O Parallelism" | DRIFTED | the anchor points at a row-group pruning loop and the quoted code is absent; decode-side parallelism has since landed (`internal/engine/scan/decode_ahead.go`). The claim needs re-derivation before it can be restated |
| 193-206 | "§10 Worker Shuffle Uses ToRows()" | DRIFTED | closed. Shuffle writes columnar WSHF with no row boxing (`internal/worker/partitioned_shuffle_sink.go:518-531`, `internal/wshf/wshf.go:1-33`) |
| 212-220 | "§11 Batch Pool Single-Mutex Contention" | DRIFTED | the mutex is real but at `internal/engine/batch/pool.go:47`; the "16 per class" premise is wrong (see architecture line 101) |
| 224-238 | "§12 Source.Next() Mutex in Parallel Pipelines" | DRIFTED | **closed** — there is no `sourceMu` in `internal/engine/exec/pipeline.go`; workers call `Source.Next` unserialized (`pipeline.go:677`). The surviving `sourceMu` is the hash-join parallel build (`join.go:1395`), a different site |
| 242-257 | "§13 Filter Schema Recomputed Per File" | CORRECT | still true; anchors stale — definition at `internal/engine/scan/scanner.go:542-553`, per-file call at `scanner.go:400`, Init call at `scanner.go:187` |
| 261-269 | "§14 Row-Group Statistics Re-Aggregated Per Query" | DRIFTED | anchor stale (`internal/storage/parquet/file_reader.go:278-360`); a process-wide footer decode cache now removes part of the cost (`internal/storage/parquet/footer_cache.go`) |
| 273-289 | "§15 Aggregate Hash Table Pre-Sizing Without Cardinality Hints" | DRIFTED | **closed** — `HashAggregate.GroupNDVHint` carries the planner's HLL estimate (`internal/engine/exec/aggregate.go:189-194`), consumed at `aggregate.go:1830-1831`, set at `internal/planner/physical/plan.go:9607` |
| 293-301 | "§16 … Serial execution only checks every 64 batches" | DRIFTED | no 64-batch sampling exists on either path. Serial checks once per **output** batch by deliberate choice (`internal/engine/exec/pipeline.go:258-263`, #368); parallel checks once per source pull (`pipeline.go:653-658`) |
| 309-320 | "What's Already Well-Optimized" — every `file:line` | DRIFTED | every anchor has moved (inlined probes are `join.go:2801-2814`; SoA accumulators `agg_scatter.go:9-15`; adaptive bloom `bloom_filter_op.go:60-61, 90-103`; Top-K heap `sort.go:204-208, 1106-1117`; chunk allocator `aggregate.go:418, 635-651`; semi/anti key-only `join.go:150-154`; null kernels `kernel/agg.go:71-74`). The techniques and magnitudes still hold |
| 322-332 | "Recurring Anti-Pattern: Row-Oriented Fallbacks" (3 paths) | DRIFTED | all three converted to columnar; the section's own recommendation shipped |
| 336-347 | "Recommended Priority Order" | DRIFTED | rows 1, 4, 6, 7, 9 are done; row 2 is done for local-fd and worker paths and deliberately declined for S3; row 8 is largely done. Only rows 3, 5, 10 survive |
| 1-5 | (no date, no commit anchor) | DRIFTED | the doc carries no staleness marker despite six closed findings |

## `docs/competitive-gaps.md`

Four claimed gaps are closed and two are half closed — the highest-value drift
class in the set, because the doc tells a reader Wadjet cannot do things it does.

| line | claim | status | truth at 9786ed72 |
|---|---|---|---|
| 16 | "**21** native types" | STALE-NUMBER | 22 — `internal/storage/parquet/schema.go:16-37` (`TypeVector` is the one missed) |
| 23 | "Merge-on-read DML (INSERT, UPDATE, DELETE)" | DRIFTED | MERGE ships too (`internal/planner/sql/parser.go:251`, dispatched at `internal/server/server.go:296`) |
| 46-64 | **Gap 2: No Built-in Alerting or Detection Rules Engine** | MISSING-FEATURE (gap closed) | shipped: `CREATE ALERT` / `DROP ALERT` / `ALTER ALERT … ENABLE\|DISABLE` (`internal/planner/sql/parser.go:205-222`), scheduler (`internal/alerts/scheduler.go`, 10 s floor at `parser.go:14-16`), webhook + table sinks, Prometheus metrics, and a day-partitioned `alert_history` table (`internal/alerts/history.go:15-41`). Residual: Sigma import, MITRE mapping, email/Slack/PagerDuty sinks, triage workflow |
| 137-143 | Gap 6 connector list | CORRECT (incomplete) | `read_json`, `read_parquet`, `read_csv`, `postgres_scan`, `mysql_scan` (`internal/planner/physical/table_func.go:34-61`) and Iceberg read-only confirmed. Missing from the doc: the HTTP object store (`internal/storage/objstore/http_store.go`) |
| 165-181 | **Gap 8: No Full-Text Search** and its recommendation | DRIFTED (half closed) | the doc's own recommendation shipped: LIKE is pushed into the scan and evaluated once per dictionary entry (`internal/engine/scan/like_filter.go:5-24`), dictionary-probe row-group pruning answers point filters (`dict_prune.go:10-30`), `contains()` has a vectorized kernel and `regexp_like` a prepared-regexp fast path (`internal/engine/expr/regexp_prepared.go:26`). The inverted-index half stands |
| 185-196 | **Gap 9: No Backup/Restore** — "No explicit backup/restore commands" | DRIFTED (half closed) | automatic 5-minute catalog snapshots with 48-snapshot retention and a documented restore (`docs/disaster-recovery.md:5-13`); `CREATE SNAPSHOT` is a statement (`internal/planner/sql/parser.go:227-229, 625-630`) and `wadjet catalog list-snapshots` a CLI command. Genuinely missing: `BACKUP`/`RESTORE … AS OF` and any snapshot of the **data** |
| 200-211 | Gap 10 "the worker LRU cache helps with repeated file reads" | DRIFTED (incomplete) | query-result caching is indeed absent, but two more caches exist and are unnamed: `--decoded-cache-bytes` (`cmd/wadjet/main.go:207`) and `--base-table-cache-bytes` (`main.go:208`), both default 0 |
| 217-219 | Gap 11 "No multi-statement transactions, no isolation levels" | CORRECT (needs a nuance) | true, but pgwire **accepts and no-ops** `BEGIN`/`COMMIT`/`END` so BI clients keep working (`internal/server/pgwire/server.go:860-872`) — worth saying, or a reader expects a syntax error |
| 221-223 | **Gap 12: No Lateral Joins or Recursive CTEs** | MISSING-FEATURE (gap closed) | both ship: LATERAL is parsed (`internal/planner/sql/parser.go:471`) and decorrelated (`internal/planner/logical/builder.go:1413-1418`); `WITH RECURSIVE` is parsed (`parser.go:1147-1153`) and executed by fixed-point iteration (`internal/planner/physical/plan.go:2003-2006`) |
| 225-227 | **Gap 13: No Schema Evolution / ALTER TABLE** | MISSING-FEATURE (gap closed) | `ALTER TABLE … ADD \| DROP \| RENAME COLUMN` ships (`internal/planner/sql/parser.go:1268-1310`, `AlterTableInfo` at `parser.go:87-88`) |
| 229-231 | **Gap 14: No OpenTelemetry / Distributed Tracing** | MISSING-FEATURE (gap closed) | `internal/telemetry/telemetry.go:1-30` — OTLP gRPC exporter bridging the existing W3C TraceID/SpanID propagation, with configurable endpoint, TLS and sample rate |
| 241-247, 251-258 | the engine and Warden priority tables | DRIFTED | P1 "Full-text search optimization" is half done; P2 "Schema evolution (ALTER TABLE)" is done; Warden P0 "Alerting / detection rules engine" is shipped at the engine layer |
| 1-40, 66-135, 145-163 | Gap 1 (managed cloud), Gap 3 (materialized views), Gap 4 (retention/TTL), Gap 5 (streaming ingestion), Gap 7 (web UI) | CORRECT | still open: `CREATE` accepts only TABLE/VIEW/FUNCTION/ALERT/SNAPSHOT (`internal/planner/sql/parser.go:613-634`), no `MATERIALIZED` token, no `RETENTION`, no syslog/Kafka receiver, no UI |

## `docs/network-analytics.md`

| line | claim | status | truth at 9786ed72 |
|---|---|---|---|
| 64-72, 425, 430-453, 460-464, 482, 588-599, 603-633, 693 | the `serve` and `shell` flags; shell SQL shapes; `POST /v1/queries`; `resp.json()["rows"]`; `db.Query` → `result.Rows`; `COUNT(DISTINCT …)` in SELECT and HAVING; `COUNT(*)` boxed as `int64`; Prometheus `/metrics` | CORRECT | `cmd/wadjet/main.go:144-154, 840`; `internal/server/server.go:100, 116, 206`; `internal/planner/logical/builder.go:372`; `internal/planner/physical/plan.go:13682-13685` |
| 127, 205, 291 | Bento writes to `data/firewall_logs/…`, `data/netflow/…`, `data/device_inventory/…` | DRIFTED | X4 — the engine resolves table data only under `tables/<name>/` (`internal/storage/partition/partition.go:46`) and only through the manifest |
| 336-338, 556-558 | `import ".../internal/storage/objstore"` (and `/parquet`) | MISSING-FEATURE | X1 — verified compile error |
| 344, 569 | `objstore.NewMinIOStore(ctx, …)` | DRIFTED | X2 |
| 353-359 | `wadjet.Open(ctx, wadjet.Config{Store, Bucket})` | DRIFTED (effect) | signature is right, but X3 — this Config yields a MemKV catalog, so the registration step writes to a throwaway map |
| 362-414 | `db.CreateTable` and the `parquet.Type*` constants used | CORRECT | `wadjet/wadjet.go:185`; `internal/storage/parquet/schema.go:16-37, 400, 434` |
| 376, 398 | partition keys `{"date","device"}` / `{"date","exporter"}` with `date` absent from `Columns` | UNSUPPORTED-CLAIM | X5 — `date` is unbindable (42703) and, even declared, unprunable |
| 432, 441, 451, 498, 530, 596, 613, 630 | `WHERE date = '2026-03-15'` | UNSUPPORTED-CLAIM | X5 |
| 463, 665 | `CAST(timestamp / 3600000 * 3600000 AS Timestamp)` as an hour bucket | UNSUPPORTED-CLAIM | the division yields float64; `parseDateValue`'s `case float64` is `AddDate(0,0,int(tv))` — **days** since epoch (`internal/engine/expr/expr.go:4979-4980`). An epoch-millis number is read as ~1.77e12 days. Silently garbage; `date_trunc('hour', timestamp)` is the correct form |

## `docs/embedding.md`

| line | claim | status | truth at 9786ed72 |
|---|---|---|---|
| 3 | "embedded directly in Go applications via the `wadjet` package" | UNSUPPORTED-CLAIM | X1 — the package's own API surface is typed in `internal/…`; no external module can call `Open`, `CreateTable` or `NewIngester` |
| 17-18, 64, 137-139 | imports of `internal/storage/{objstore,parquet,ingest}` | MISSING-FEATURE | X1 |
| 22-27, 147-151 | `objstore.NewMinIOStore(ctx, …)` | DRIFTED | X2 (the four config fields themselves are right — `minio.go:18-21`) |
| 32-35, 47, 53-58, 66-73, 90-93, 97-99, 105-112, 116, 272-278, 332, 334-335 | `Open`; `CreateTable`; `ListTables`; `Catalog()`/`Store()`; `NewIngester` returning no error; `Start`/`FlushAll`/`Stop`; the flush description; `Query` → `Columns`/`Rows`; `row["src_ip"].(string)`; `COUNT(DISTINCT …)` with HAVING; `Query` concurrency-safe; `Ingest` mutex-guarded | CORRECT | `wadjet/wadjet.go:72, 185, 209, 219, 257, 264, 300, 1116, 1121`; `internal/storage/ingest/ingest.go:95, 116, 258, 275-295, 303` |
| 38-41 | "The `Config` struct accepts: Store / Bucket / Logger" | DRIFTED | `Config` has 12 fields (`wadjet/wadjet.go:44-68`); `MetaKV`, `MemoryBudget`, `SpillDir`, `AuthProvider`, `SortMergeJoinBytes`, `LateMaterialization`, `BushyJoinReorder`, `EnableAlerts` are omitted. `MetaKV`'s omission is what makes X3 invisible |
| 50 | `DropTable` "removes metadata only — Parquet files remain on S3" | CORRECT | reclaim is opt-in and only the background compactor enables it (`internal/storage/compaction/background.go:94`), which embedded `Open` never starts |
| 76-87 | `ingester.Ingest` with a row map carrying no `"date"` key against partition keys `{"date"}` | UNSUPPORTED-CLAIM | `internal/storage/ingest/ingest.go:265-272` requires every partition key in **each row**; this example errors on the first call |
| 100, 333 | "Updates the catalog manifest atomically via **NATS KV** revisions" | DRIFTED | X3 — with no `MetaKV` this is `MemKV`; the CAS is real but against an in-process map |
| 117-118 | `total := row["total"].(int64)` for `SUM(bytes_in)` over an INT64 column | UNSUPPORTED-CLAIM | **this panics.** `SUM(INT64)` declares `TypeDecimal` (`internal/planner/physical/plan.go:13855-13856`), and `GetValue` boxes a DECIMAL as its formatted **string** (`internal/engine/batch/vector.go:727`). This is the ADR-0012 exact-numeric change reaching the Go API |
| 188-199, 221, 250, 276 | `TypeIPv4` for `src_ip` (correct); partition key `{"date"}` and `WHERE date = …` | UNSUPPORTED-CLAIM | X5 |

## `docs/ingestion.md`

| line | claim | status | truth at 9786ed72 |
|---|---|---|---|
| 7-12, 43-51, 53-60, 76-79, 103, 374-378, 384-388 | the accumulator description; `parquet.Schema`/`Column` and type constants; `NewIngester` returning no error; `Start`/`FlushAll`/`Stop`; the 64-256 MB file-size target; the codec list | CORRECT | `internal/storage/ingest/ingest.go:95, 116, 275-295, 303, 342`; `internal/storage/parquet/schema.go:16-24, 400, 434`; `internal/storage/parquet/decompress.go:17-31` |
| 29-33 | flush defaults 128 MB / 1,000,000 / 60 s | CORRECT (values) | `internal/storage/ingest/ingest.go:28-34` |
| 31-32 | "Total buffer size **across all partitions**" / "**Total** buffered row count" | DRIFTED | both thresholds are **per-partition** — `ingest.go:291-292` tests them inside `for partPath, buf := range ing.buffers` |
| 33 | Time trigger "60 seconds — Wall-clock time since last flush" | DRIFTED | it is a fixed ticker (`ingest.go:99`), and it skips any partition holding fewer than `MinFlushRows` rows (default 100) — `ingest.go:33, 330-333` |
| 29-33 | the defaults table | DRIFTED (incomplete) | omits `RowGroupSize` (128 K, `ingest.go:33`) and `MinFlushRows` (100, `ingest.go:34`) |
| 38-40, 327-336 | imports of `internal/…`; `objstore.NewMinIOStore(ctx, …)` | MISSING-FEATURE / DRIFTED | X1, X2 |
| 54, 349, 365 | partition keys `{"date","device"}` / `{"date"}` with `date` absent from `Columns` | UNSUPPORTED-CLAIM | X5 |
| 63-73 | `ingester.Ingest(ctx, batch)` with rows carrying no `"date"` | UNSUPPORTED-CLAIM | `ingest.go:271` — fails on the first call |
| 87-89 | `s3://wadjet/data/syslog/date=…/part-0001.parquet` | DRIFTED | prefix is `tables/` (`internal/storage/partition/partition.go:46`) and the file is `chunk_<uuidv7>.parquet` (`ingest.go:356`) |
| 92, 96-101, 392-394 | "Good partitioning enables partition pruning"; the recommendation table (`date`; `tenant_id,date`; `date,device`; `region,date`); "Always include a time dimension (`date`, `hour`)" | UNSUPPORTED-CLAIM | X5 — of every key name recommended, only `hour` prunes. As written each recommendation yields a full scan |
| 152, 219, 284 | Bento paths under `data/` | DRIFTED | X4 |
| 324, 338-368 | "register the table schema in Wadjet so it can be queried" via `db.CreateTable` | UNSUPPORTED-CLAIM | X4 — `CreateTable` writes an **empty** manifest and the scan reads only the manifest, so Bento's files are never seen. `catalog.AddFiles` is the missing step; compounded by X3 |
| 398 | "catalog uses **NATS KV** with revision-based optimistic concurrency" | DRIFTED | X3 — true for `wadjet serve`, false for the embedded `Open` this doc's code uses |
| 398 | "a Parquet file may exist on S3 without a catalog entry … Periodic cleanup of orphaned files is recommended" | CORRECT | `flushBuffer` PUTs then updates the manifest (`ingest.go:377-380`), so the window is real, and no orphan reaper ships (`internal/coordinator/cleanup.go:48-92` sweeps only `queries/`) |

## `docs/operations.md`

| line | claim | status | truth at 9786ed72 |
|---|---|---|---|
| 7, 15, 19, 36-43, 64, 83-118, 128-151, 160-165, 177, 182, 200, 215-218, 228-230, 308, 329-331, 347 | `/metrics`; `wadjet_queries_total{status}`; `wadjet_active_queries`; the eight `wadjet_worker_*` metrics and their buckets; `wadjet_cache_bytes`; **all eight PromQL expressions**; `/v1/health` and the K8s probes; the `curl` table calls; `stats.rows_scanned`; the spill remediation; the 60 s ingest buffer; NATS 4222; the sizing-table flags; "logs to stderr" | CORRECT | `internal/metrics/metrics.go:61-230`; `internal/server/server.go:110, 116, 214, 425, 918-920`; `cmd/wadjet/main.go:156, 166, 175, 182, 259-265`; `internal/storage/ingest/ingest.go:32, 303` |
| 16 | `wadjet_query_duration_seconds` \| Labels **—** | DRIFTED | it carries a `type` label (`internal/metrics/metrics.go:67-72`); the buckets are right |
| 17-18 | `wadjet_query_rows_scanned`, `wadjet_query_bytes_read` | DRIFTED | both need the `_total` suffix (`internal/metrics/metrics.go:74-84`); `rows_scanned` also has a `table` label |
| 25-30 | `wadjet_files_scanned`, `wadjet_files_pruned`, `wadjet_row_groups_scanned`, `wadjet_row_groups_pruned`, `wadjet_partitions_scanned`, `wadjet_partitions_pruned` | DRIFTED | **none of the six exists.** Every scanner metric carries `Subsystem: "scan"` and a `_total` suffix (`internal/metrics/metrics.go:92-131`). The doc's own PromQL at lines 98 and 118 already uses the real names — it contradicts its own table |
| 49 | `wadjet_registered_workers` | DRIFTED | `wadjet_coordinator_registered_workers` (`internal/metrics/metrics.go:163-167`) |
| 55-56 | `wadjet_batches_processed`, `wadjet_rows_output` | DRIFTED | `wadjet_pipeline_batches_processed_total`, `wadjet_pipeline_rows_output_total` (`internal/metrics/metrics.go:170-181`) |
| 62-63 | `wadjet_cache_hits`, `wadjet_cache_misses` | DRIFTED | both need `_total` (`internal/metrics/metrics.go:212-223`) |
| 73 | `static_targets:` | DRIFTED | not a Prometheus key — the scrape config as written will not load; it is `static_configs:` |
| 68-77 | the scrape config as a whole | DRIFTED (incomplete) | only `/v1/health` bypasses the auth middleware (`internal/auth/middleware.go:62, 105`); `/metrics` needs a credential when auth is on. Worker metrics are on `--metrics-addr` (`:9100`), not `:8080` |
| 170, 207, 246, 271, 291 | `data/flow_logs/…`, `"Prefix": "data/"`, `s3://wadjet/data/` | DRIFTED | table data lives under `tables/<name>/` (`internal/storage/partition/partition.go:46-48`). **The S3 lifecycle rules match zero objects**, so an operator following this doc gets no expiry at all |
| 184 | "enable the in-memory result store with `--result-store`" | STALE-NUMBER | it is already on by default at 512 MiB (`cmd/wadjet/main.go:175`) |
| 188 | "Increase `worker.cache_bytes`" | UNSUPPORTED-CLAIM | the key parses (`internal/config/config.go:172`) but `cmd/wadjet` never applies `cfg.Worker` — the live knob is `--cache-bytes` (`cmd/wadjet/main.go:173`) |
| 193 | "Check worker count … `curl /v1/health \| jq .`" | DRIFTED | `/v1/health` returns only `{"status":"ok"}` (`internal/server/server.go:918-920`); worker count is on `/v1/ready` (`:938-940`) or `/v1/workers` (`internal/server/ops.go:25`) |
| 236 | "Wadjet is append-only — there's no built-in DELETE or UPDATE" | DRIFTED | both parse (`internal/planner/sql/dml_parser.go:123, 188`) and execute (`wadjet/dml.go:122, 179`) |
| 290-306 | "back up the NATS store directory" | CORRECT (incomplete) | never mentions the engine's own catalog-snapshot-to-S3 mechanism (`internal/storage/catalog/snapshot.go:44`, `cmd/wadjet/main.go:246-248`) |
| 13-64 | the metrics tables as a whole | MISSING-FEATURE | five registered alert metrics are undocumented: `wadjet_alert_evaluations_total`, `wadjet_alert_evaluation_duration_seconds`, `wadjet_alert_rows_matched`, `wadjet_alert_scheduler_list_errors_total`, `wadjet_alert_webhook_retries_total` (`internal/alerts/metrics.go:6-33`) |

## `docs/runbook.md`

The most accurate doc in the set.

| line | claim | status | truth at 9786ed72 |
|---|---|---|---|
| §1.1, §1.3-§1.6, §1.7, §1.8, §1.9, §2 | the embedded API and parallel-by-default scan; every storage flag; `--data-plane`/`--data-plane-addr`/`--coord-data-plane`/`--catalog-snapshot-*`/`--shared-pool-budget`/`--spill-dir`/`--max-concurrent`; `--mmap-relief` true with 85% auto ceiling, `--bounded-dirty-writes` true, `--spill-floating-budget` false; `WADJET_FASTPATH_STRICT`, `DefaultLocalFastPathBytes = 64<<20`, `--streaming-exchange`, `--peer-exchange-addr`, `--late-materialization`, `--morsel-workers`; **the entire signal contract** (SIGTERM/SIGQUIT → drain, SIGINT → hard stop, `POST /drain` → 202, `/healthz`, `/readyz` 503 while draining, drain-timeout escalation); the federation flags; `cmd/tpch-harness --mode=local`; and every default in the §2 flag table including `--pg-addr :5433` | CORRECT | `cmd/wadjet/main.go:146-215, 246-248, 1747-1767, 1804-1818`; `internal/coordinator/local_fastpath.go:31, 78, 119-121`; `internal/worker/worker.go:694, 1248`; `internal/planner/physical/plan.go:2507-2518, 14670-14677` |
| 33-35 | standalone runs "Prometheus (`--metrics-addr`, default `:9100`)" | DRIFTED | `metricsAddr` is bound only inside `runWorker` (`cmd/wadjet/main.go:1778`). Standalone and coordinator bind no `:9100`; `/metrics` is on `--http-addr` (`internal/server/server.go:116`) |
| 47-66 | the production example uses `--pg-addr=:5432` | STALE-NUMBER (cosmetic) | fine as an explicit override, but it disagrees with the `:5433` default the same doc records at line 198 |
| 224 | "`--config` \| YAML file mirroring these flags" | DRIFTED | only `auth:` and `geoip:` are applied (`cmd/wadjet/main.go:1084, 1233-1289`). The `storage:`/`nats:`/`http:`/`grpc:`/`worker:`/`parquet:`/`query_limits:` sections parse but never reach the running process |
| 196-224 | the §2 flag table | MISSING-FEATURE | omits fourteen shipped, defaulted flags: `--eager-dispatch` false, `--skew-split` true, `--shuffle-durability` eager, `--locality-placement` true, `--agg-partial-split` true, `--streaming-shuffle-read` true, `--async-scratch-purge` true, `--peer-wire-compression` true, `--scan-decode-ahead` true, `--shuffle-decode-ahead` true, `--reclaim-dropped-tables` false, `--bushy-join-reorder` false, `--sort-merge-join-bytes` 0, `--broadcast-bytes` 0 (`cmd/wadjet/main.go:187-215`) |

## `docs/disaster-recovery.md`

Describes a snapshot system that does not match the implementation in layout,
naming, retention, configuration or CLI. **Nearly every operator-runnable
command in it fails.**

| line | claim | status | truth at 9786ed72 |
|---|---|---|---|
| 7 | "Wadjet **automatically** snapshots all catalog metadata … every 5 minutes" | DRIFTED | snapshots are **off** unless `--catalog-snapshot-s3-prefix` is set (`internal/coordinator/catalog_snapshot.go:78-80`, wired at `cmd/wadjet/main.go:1195-1208`) |
| 7, 11, 27 | "JSON files stored under `_catalog/snapshots/`", named `2026-03-27T06:30:00Z.json` | DRIFTED | there is no default prefix, and a snapshot is a **directory**: `<prefix>snapshots/<ts>/manifest.json` plus one JSON per KV key, plus a `<prefix>latest` pointer. The timestamp format is `20060102T150405Z` (`internal/storage/catalog/snapshot.go:52-53, 86, 95-102`) |
| 9 | "configurable via `catalog_snapshot.interval` or `WADJET_CATALOG_SNAPSHOT_INTERVAL`" | MISSING-FEATURE (half) | the env var exists (`cmd/wadjet/main.go:495`); there is **no** `catalog_snapshot:` config section (`internal/config/config.go:14-27`) |
| 10 | "**Retention**: 48 snapshots (~4 hours), configurable" | DRIFTED | keep the 10 newest **plus** anything under 24 h, not configurable (`internal/coordinator/catalog_snapshot.go:118`) |
| 12 | "**Format**: JSON containing all NATS KV entries" | DRIFTED | one JSON object **per KV key**, indexed by `manifest.json` with a SHA256 each (`internal/storage/catalog/snapshot.go:25-42, 70-88`) |
| 13 | "**Leader-only**" | CORRECT | `internal/coordinator/coordinator.go:566` |
| 21, 66 | `wadjet catalog list-snapshots …` | MISSING-FEATURE | `catalogCmd()` registers exactly one subcommand, `snapshot` (`cmd/wadjet/main.go:1836`) |
| 35-36 | `jq '.entries \| length'`, `jq '.entries[].key'` | DRIFTED | the manifest has `key_count` and `keys[]` with `kv_key`/`s3_path`/`sha256` (`internal/storage/catalog/snapshot.go:26-42`) |
| 40-42 | KV keys `<cluster>.tables.<name>.schema`, `<cluster>.tables.<name>.partitions.<p>` | DRIFTED | real keys are `<cid>.meta`, `<cid>.table.<name>`, `<cid>.manifest.<name>`, `<cid>.alert.<name>` (`internal/storage/catalog/snapshot.go:107-113`) |
| 59, 69, 78 | `wadjet catalog restore …`, `--snapshot-key …` | MISSING-FEATURE | no `restore` subcommand and no `--snapshot-key` flag exist. Restore is a serve-time flag: `--force-restore-catalog=latest\|<timestamp>` (`cmd/wadjet/main.go:248`, consumed at `:1204`) |
| 53, 82 | `rm -rf ~/.wadjet/nats`; the `wadjet tables` verification | CORRECT | `cmd/wadjet/main.go:159, 576` |
| 99 | "Wadjet exposes snapshot metrics at `/metrics`" | UNSUPPORTED-CLAIM | no snapshot metric is registered anywhere. The two bullets beneath correctly say "check logs / check S3" — the heading is the error |
| 116-124 | the `catalog_snapshot:` YAML block | MISSING-FEATURE | no such config section. `retention`, `debounce`, `leader_only`, `enabled`, `prefix` have no config-file path, and **`debounce` describes mutation-triggered snapshots that do not exist** — the only triggers are the interval ticker and explicit `CREATE SNAPSHOT` (`internal/coordinator/catalog_snapshot.go:107-126`) |
| 126 | six `WADJET_CATALOG_SNAPSHOT_*` env vars | DRIFTED | only two exist: `_PREFIX` and `_INTERVAL` (`cmd/wadjet/main.go:492, 495`) |
| 132-133 | "RPO ≤5 min" | CORRECT (conditional) | true only once `--catalog-snapshot-s3-prefix` is set |

## `docs/security.md`

| line | claim | status | truth at 9786ed72 |
|---|---|---|---|
| 21-30 (keys), 35-60, 62-105, 108-140, 150-230, 246-300, 350-395, 420-460 | JWT config keys and both HMAC/RSA shapes; mTLS keys, CN→role with SAN-DNS/SAN-email fallback and `default_role`; `RequireAndVerifyClientCert` at TLS 1.2 minimum; identity-enrichment attribute names; the pgwire cleartext-password flow and gRPC bearer extraction; RBAC role/tables/allow and read/write/admin; 403 on denial; **all eleven ABAC condition operators**; deny-overrides, priority ordering, obligation merging, row-filter AND-ing, closed-world default deny; RBAC→ABAC auto-migration; `PolicyDecisionJSON` on the wire; `FilterExprs` pushdown; all six audit event names, levels and fields; `psql -p 5433` | CORRECT | `internal/config/config.go:71-95`; `internal/auth/mtls.go:27-105, 154-159`; `internal/auth/jwt.go:172-183`; `internal/auth/abac_eval.go:36-101, 115-149, 234-262`; `internal/auth/abac_config.go:8-90`; `internal/auth/audit.go:18-113`; `internal/server/pgwire/server.go:498-540`; `internal/server/grpc.go:476-494`; `cmd/wadjet/main.go:176` |
| 7 | "Configure one in the YAML config file." | **MISSING-FEATURE (fatal)** | **passing `--config` panics the server at startup.** `server.New` registers routes (`internal/server/server.go:100-126`) and then `Server.Start` calls `s.mux.Use(...)` at `internal/server/server.go:150`; chi v5.2.5 (`go.mod:10`) panics there. Reproduced against a build of this commit — `panic … server.go:150 … main.runStandalone.func2()`; the same binary without `--config` starts cleanly. The trigger is any loadable `--config`, since `provider` is assigned unconditionally at `cmd/wadjet/main.go:1244` |
| 3 | "**All** security configuration is hot-reloadable." | DRIFTED | authenticator/authorizer/policy set swap atomically (`internal/auth/provider.go:94-111`), but the mTLS `tls.Config` is built once at startup and never reloaded (`cmd/wadjet/main.go:1271-1277`) |
| 26-28 | api_keys `attributes:` (`clearance`, `department`) | MISSING-FEATURE | `config.AuthAPIKey` has only `key`/`name`/`role` (`internal/config/config.go:63-68`); the YAML key is silently dropped |
| 114 | "**API keys**: Custom `attributes` from config" | UNSUPPORTED-CLAIM | same cause — an API-key identity's `Attributes` is always empty on the config path |
| 107 | `https://wadjet.internal:8443/v1/queries` | DRIFTED | there is no `8443` default; TLS is served on `--http-addr` (`internal/server/server.go:180`) |
| 243 | ABAC `mask_column` with `value: "***REDACTED***"` | DRIFTED | the value becomes `MaskExpr` and is injected as a **SQL expression** (`internal/auth/abac_eval.go:144`, `internal/planner/logical/plan.go:632`). A bare `***REDACTED***` is not a SQL literal; it must be quoted |
| 284 | obligation `query_limit` — "Maximum rows returned" | UNSUPPORTED-CLAIM | `EvaluateTableAccess` explicitly no-ops it (`internal/auth/abac_eval.go:146-147`) |
| 311, 322, 329, 337-341, 406 | "`columns`: A map of column name to **replacement value**", and every example written that way (`src_ip: "***REDACTED***"`, `message: "[MASKED]"`, `src_ip: "EXTERNAL"`) | **DRIFTED (silent security failure)** | `columns` values are **actions**, not replacement strings. `ParsePolicies` switches on `allow`/`mask`/`deny` and its `default:` branch maps anything else to **`ColumnAllow`** (`internal/auth/policy.go:145-157`). **Every masking example in this doc configures no masking at all.** The replacement is not configurable: `defaultMask` returns `"***"`/`0`/`false` by type (`internal/auth/policy.go:106-121`) |
| 450 | example log `denied_columns=[]` | STALE-NUMBER (cosmetic) | empty lists are omitted (`internal/auth/audit.go:82-88`) |
| 465-530 | the whole "Query Cost Estimation and Guards" section | **UNSUPPORTED-CLAIM** | the guards reach no query. `physical.Planner.QueryLimits` is set only at `internal/server/server.go:474` from `s.config.QueryLimits`/`RoleLimits`, and **neither is assigned outside tests** — the `server.Config{…}` literals at `cmd/wadjet/main.go:1220` and `:1519` omit both. pgwire and the coordinator have no limit check at all |
| 490, 512 | per-role `query_limits` overrides | MISSING-FEATURE | `config.AuthRole.QueryLimits` parses (`internal/config/config.go:94`) but `auth.RoleConfig` has no such field and `buildAuth` drops it (`cmd/wadjet/main.go:2016-2017`) |
| 518-522 | `WADJET_QUERY_MAX_SCAN_BYTES` / `_ROWS` / `_FILES` | DRIFTED | read into a struct nothing consumes (`internal/config/config.go:353-365`) — setting them has no effect |
| 543 | "use Prometheus metrics … to track who queries what" | UNSUPPORTED-CLAIM | no metric carries identity; the `component=audit` log is the only per-identity record |

## `docs/sql-reference.md`

Every scalar-function *name* in this doc resolves in `DefaultRegistry` (358
names at this commit), all 24 documented aggregates are in `knownAggregates`
and all 16 window functions map 1:1 onto `exec.ParseWindowFunc`. 78 of the
documented example *results* were evaluated against the live registry and
matched exactly. The drift is in descriptions, signatures, counts and feature
claims — not in names.

| line | claim | status | truth at 9786ed72 |
|---|---|---|---|
| 10, 55, 75-79, 127-141, 175-213, 248, 305-339, 397-422, 468-491, 593-602, 654-674, 758-1290 (~330 rows), 1047-1058, 1207-1230, 1442-1452, 1470-1478 | `EXPLAIN [VERBOSE]`; read_json inference; read_csv params; the 11-row DB-connector type map; set-operation semantics including `INTERSECT ALL`/`EXCEPT ALL`; `PARTITION BY`; every predicate form; the aggregate table and `PERCENTILE_CONT` argument order; GROUPING SETS/ROLLUP/CUBE expansions; the operator set; the 16 window functions; ~330 function-table rows **and 78 evaluated example results**; GeoIP config; session functions; the precedence ladder; merge-on-read DML semantics | CORRECT | `internal/planner/sql/select_parser.go:113-166, 261-299, 1098-1105, 1183-1330, 2821-2848`; `internal/planner/sql/ast.go:565-599`; `internal/engine/exec/window.go:50-86`; `internal/storage/dbscan/scanner.go:105-157`; `internal/engine/expr/expr.go:4763-4802` |
| 7-18 | the statement table (10 statements) | DRIFTED | `parseDispatch` also dispatches INSERT/UPDATE/DELETE/MERGE/ALTER/ANALYZE (`internal/planner/sql/parser.go:301-323`), and `CREATE SNAPSHOT` / `CREATE\|DROP\|ALTER ALERT` are wired at `internal/coordinator/coordinator.go:882-894`. (`CREATE VIEW`/`DROP VIEW`/`ALTER TABLE` parse but have **no executor** — they must not be added) |
| 82 | read_parquet "with column-at-a-time page reading and **row-group stats pruning**" | UNSUPPORTED-CLAIM | the table-function path calls `readBatchDirect(reader, schema, nil)` with no required columns and no predicates (`internal/planner/physical/table_func.go:174`, `util.go:149`). Neither projection nor row-group pruning runs for `read_parquet()` — both exist only on the catalog scan path |
| 89 | "…with connection pooling and **configurable auth headers**" | UNSUPPORTED-CLAIM | `openHTTP` builds an **empty** `objstore.HTTPConfig{}` (`internal/planner/physical/table_func.go:341`); `HTTPConfig.Headers` exists but no SQL or CLI surface sets it. Pooling is real |
| 93 | "**Local** CSV files are read in streaming mode" | DRIFTED | every CSV/JSON source shape streams now — local, glob (lazy multi-file) and HTTP off the response body (`internal/planner/physical/table_func.go:222-225`). The 100-row inference sample is correct (`internal/storage/csv/reader.go:146`) |
| 95 | "HTTP sources and glob patterns still require a full download" | DRIFTED | true only for `read_parquet`, which needs `io.ReaderAt` (`internal/planner/physical/table_func.go:157-167, 350-353`) |
| 105 | "Run an arbitrary query with **pushdown**" | UNSUPPORTED-CLAIM | `dbScanSource` runs exactly the SQL handed to it (`internal/planner/physical/table_func.go:56-57`); `internal/storage/dbscan/` has no pushdown machinery. The outer `WHERE` is applied locally |
| 360 | correlated subqueries "**Re-executed per outer row**" | DRIFTED | the optimizer decorrelates all three shapes — `decorrelateScalarSubqueries` (`internal/planner/logical/optimizer.go:1066`), `decorrelateInSubqueries` (`:1422`), `decorrelateExists` (`:2861`); EXISTS/IN lower to semi/anti joins |
| 401-422 | the aggregate table documents `COUNT(DISTINCT …)` and no other DISTINCT form | DRIFTED | DISTINCT works for **every** aggregate PostgreSQL accepts it for (#703) — `internal/engine/exec/aggregate.go:95-110`, set at `internal/planner/physical/plan.go:9398-9402`. `SUM(DISTINCT x)`, `AVG(DISTINCT x)`, `string_agg(DISTINCT x, ',')` all parse |
| 579 | "**Place the smaller table on the right side** of the JOIN for best performance" | DRIFTED | the optimizer picks the build side itself from cardinality estimates (`internal/planner/logical/optimizer.go:3419-3426`) and cost-reorders three or more relations (`:3429`). Only OUTER joins keep the written order (`:3404-3407`) |
| 742, 744, 752 | `N PRECEDING`/`N FOLLOWING` = "N rows/**values**"; `RANGE` = "**logical value-based** boundaries" | DRIFTED | a RANGE frame with a value offset is **refused**: "RANGE frame with a value offset is not supported; use ROWS for a row-count frame" (`internal/planner/sql/select_parser.go:2164-2176`). RANGE accepts only UNBOUNDED PRECEDING / CURRENT ROW / UNBOUNDED FOLLOWING; `GROUPS` mode does not parse |
| 756 | "Wadjet includes **273** built-in scalar functions" | STALE-NUMBER | **358** (`expr.DefaultRegistry`, `internal/engine/expr/expr.go:3685`) |
| 765 | `LENGTH(s)` / `LEN(s)` — "String length" | DRIFTED | it counts **bytes**: `int32(len(toString(args[0])))` (`internal/engine/expr/expr.go:4216-4221`), so `LENGTH('日本語')` is 9 here and 3 in PostgreSQL. `CHAR_LENGTH` counts characters |
| 800-801 | `TO_UTF8(s)` "String to UTF-8 byte representation **(hex)**"; `FROM_UTF8(bytes)` "UTF-8 bytes **(hex)** to string" | DRIFTED | `to_utf8` returns raw bytes, not hex (`internal/engine/expr/expr.go:7417-7421`); `from_utf8` decodes bytes and returns a string argument unchanged — it does not decode hex (`expr.go:7424-7439`) |
| 1123 | `DATE_DIFF(unit, a, b)` with `DATE_DIFF('second', …)` | DRIFTED | `fnDateDiff` takes exactly **two** arguments and returns whole **days** (`internal/engine/expr/expr.go:4803-4828`). A 3-arg call parses `'second'` as a date, fails, and returns NULL |
| 1124 | `DATE_ADD(ts, interval)` with `DATE_ADD(timestamp, 3600000)` | DRIFTED | a numeric second argument counts **DAYS** (`internal/engine/expr/expr.go:4844-4846, 4869`). The documented example adds 3.6 million days |
| 1268 | embedding functions "Requires `WADJET_OPENAI_API_KEY`" | DRIFTED | three providers via `WADJET_EMBED_PROVIDER` — openai (default), voyage (`WADJET_VOYAGE_API_KEY`), ollama (keyless, `WADJET_OLLAMA_URL`) — `cmd/wadjet/main.go:1902-1969`. With no provider configured `embed()` is not registered at all |
| 1483-1490 | the INSERT/UPDATE coercion table | DRIFTED (incomplete) | `convertValue` also handles PORT, PROTOCOL, DURATION, DECIMAL, BYTES and IPv4/IPv6/MAC/CIDR/UUID (`wadjet/dml.go:2010-2140`); ARRAY/ROW/MAP/VECTOR are explicitly **refused** by the INSERT VALUES parser (`dml.go:2054-2059`) |
| 1494-1495 | "No lateral joins"; "No recursive CTEs" | DRIFTED | both ship — LATERAL parses in the comma and JOIN forms (`internal/planner/sql/select_parser.go:613-614, 650-655`) and decorrelates (`internal/planner/logical/builder.go:1413-1418`); `WITH RECURSIVE` parses (`parser.go:1147-1150`) and executes by fixed-point iteration (`internal/planner/physical/plan.go:2006-2035`). The real refusals are NATURAL JOIN, `JOIN … USING`, `RETURNING`, `MERGE … WHEN NOT MATCHED BY SOURCE/TARGET` (0A000), RANGE value offsets, `GROUPS` frames, and `SELECT DISTINCT ON` |

## `docs/data-types.md`

**Neither this doc nor `sql-reference.md` contains a wrong numeric claim.**
Line 29 ("1–38, default 38") and line 30 ("scale default 0") are correct
against `internal/storage/parquet/schema.go:177` and
`internal/engine/batch/decimal.go:262`. The docs are simply *silent* on
ADR-0024's exact-type results.

| line | claim | status | truth at 9786ed72 |
|---|---|---|---|
| 9-16, 20, 29-32, 36-41, 45-52, 58-62, 68, 74-78, 99-103, 111-113, 143, 147-153, 199, 218-228 | the numeric table and DECIMAL section (Int128 carrier, 16 bytes, precision 1-38 default 38, scale default 0, exact SUM/AVG/MIN/MAX, Parquet DECIMAL logical type); the offset/data string layout; the network-type storage table; Timestamp millis / Date int32 days / Duration int64 nanos; UUID 16 bytes; ARRAY/ROW/MAP layouts and the container function lists; `VECTOR(N)` as FIXED_LEN_BYTE_ARRAY at N×4 bytes; the OpenAI model dimensions and 50 K LRU; null bitmap and `COUNT(*)`; "network types require explicit schema registration"; the five compression codecs | CORRECT | `internal/engine/batch/decimal.go:259-263, 282`; `internal/storage/parquet/schema.go:24-36, 177, 206-215`; `internal/engine/batch/vector.go:93-96, 660, 1600`; `internal/storage/parquet/file_writer.go:295-300, 361, 365`; `internal/embedding/openai.go:31, 38-45`; `internal/storage/parquet/writer.go:16-20, 70` |
| 54 | "Network types are stored in their **compact binary representations (not as strings)**" | DRIFTED | CIDR is stored as a **string** (`internal/storage/parquet/schema.go:26`, written as `PhysicalByteArray` at `internal/storage/parquet/file_writer.go:361`). The doc's own table at line 49 says so, contradicting this sentence |
| 105 | "Nested types are fully supported in Parquet **read** … and display output" | DRIFTED | they are also **written** — `internal/storage/parquet/file_writer.go:155, 173, 210` with validation at `:268-292`; the compaction gate asserts containers-in-containers round-trip |
| 123, 137-141 | "requires `WADJET_OPENAI_API_KEY`"; the two-variable env block | DRIFTED | seven variables are read: `WADJET_EMBED_PROVIDER`, `_MODEL`, `_DIM`, `WADJET_OPENAI_API_KEY`, `WADJET_VOYAGE_API_KEY`, `WADJET_VOYAGE_INPUT_TYPE`, `WADJET_OLLAMA_URL` (`cmd/wadjet/main.go:1909-1963`) |
| 160-181 | `import ".../internal/storage/parquet"` under "**In Go (Embedded API)**" | UNSUPPORTED-CLAIM | X1 — `parquet` is an internal package; an external embedder cannot construct the `parquet.Schema` that `CreateTable` requires, and no public alias exists |
| 196 | `\| BYTE_ARRAY \| none \| **Bytes** \|` | DRIFTED | an unannotated `BYTE_ARRAY` maps to **String** — `TypeIDFromSchemaNode`'s physical fallback is `case PhysicalByteArray, PhysicalFixedLenByteArray: return TypeString` (`internal/storage/parquet/file_reader.go:676-677`). Nothing in that function returns `TypeBytes` |
| 187-197 | the Parquet type-inference table (9 rows) | DRIFTED (incomplete) | the other 8 rows are right, but the mapper also handles DATE, UUID, JSON, ENUM, the INTEGER logical type (bit-width split), TIME_MILLIS/TIME_MICROS and TIMESTAMP_MICROS/NANOS (all three precisions rescaled to one engine unit) — `internal/storage/parquet/file_reader.go:606-682` |
| 205-210 | the type-coercion table (4 rows) | DRIFTED (incomplete) | all four are right, but it omits the two rules that matter most now: integer-only arithmetic stays exact integer with 22003 on overflow (`internal/engine/expr/binop_numeric.go:22-35`), and DECIMAL participates via `DecimalCommon`/`DecimalResultType` (`internal/engine/batch/decimal_result_type.go:56-115`) |

### One engine defect found in passing

`GROUPING(a)` **cannot be parsed in a SELECT list** — `SELECT GROUPING(a),
SUM(b) FROM t GROUP BY ROLLUP(a)` fails with `unexpected token "GROUPING"`.
`GROUPING` is lexed as a keyword for `GROUPING SETS` and the expression parser
has no branch for it, though `grouping` is in `knownAggregates`
(`internal/planner/sql/ast.go:566`). Consequence for this audit: a
`GROUPING(...)` row must **not** be added to the aggregate table.

---

## Summary

### Counts per status per doc

| Doc | CORRECT | DRIFTED | STALE-NUMBER | UNSUPPORTED-CLAIM | MISSING-FEATURE | rows |
|---|---:|---:|---:|---:|---:|---:|
| `README.md` | 16 | 7 | 4 | 2 | 0 | 29 |
| `docs/README.md` | 1 | 1 | 0 | 0 | 0 | 2 |
| `docs/configuration.md` | 12 | 8 | 0 | 1 | 6 | 27 |
| `docs/distributed.md` | 6 | 6 | 0 | 0 | 2 | 14 |
| `docs/tuning.md` | 5 | 7 | 0 | 0 | 0 | 12 |
| `docs/api-reference.md` | 6 | 8 | 0 | 1 | 1 | 16 |
| `docs/grpc-api.md` | 6 | 6 | 0 | 0 | 1 | 13 |
| `docs/getting-started.md` | 6 | 5 | 0 | 0 | 0 | 11 |
| `docs/architecture.md` | 1 | 6 | 0 | 1 | 1 | 9 |
| `docs/performance-bottlenecks.md` | 2 | 17 | 0 | 0 | 0 | 19 |
| `docs/competitive-gaps.md` | 3 | 6 | 1 | 0 | 4 | 14 |
| `docs/network-analytics.md` | 2 | 3 | 0 | 4 | 2 | 11 |
| `docs/embedding.md` | 4 | 3 | 0 | 4 | 2 | 13 |
| `docs/ingestion.md` | 4 | 6 | 0 | 5 | 1 | 16 |
| `docs/operations.md` | 3 | 9 | 1 | 2 | 1 | 16 |
| `docs/runbook.md` | 2 | 2 | 1 | 0 | 1 | 6 |
| `docs/disaster-recovery.md` | 3 | 7 | 0 | 1 | 3 | 14 |
| `docs/security.md` | 2 | 5 | 1 | 4 | 3 | 15 |
| `docs/sql-reference.md` | 1 | 12 | 1 | 3 | 0 | 17 |
| `docs/data-types.md` | 1 | 6 | 0 | 1 | 0 | 8 |
| **Total** | **86** | **130** | **9** | **29** | **28** | **282** |

Grouped CORRECT rows cover many individual claims each; the DRIFTED /
STALE-NUMBER / UNSUPPORTED-CLAIM / MISSING-FEATURE rows are one finding apiece.

### The ten worst items

Ranked by what a reader would *do* with the claim and how wrong they would be.

1. **`docs/security.md:311-341, 406` — every cell-masking example silently disables masking.** `columns: {src_ip: "***REDACTED***"}` parses to `ColumnAllow` (`internal/auth/policy.go:145-157`, `default:` arm). An operator who follows this doc believes PII is masked and it is returned in full. Same defect in `docs/configuration.md:145`.
2. **`docs/security.md:7` — every YAML example in the doc is unusable: `--config` panics the server.** `Server.Start` installs middleware after routes (`internal/server/server.go:150`); chi v5.2.5 panics. Reproduced against a build of this commit.
3. **`docs/security.md:465-530` — query cost guards are documented as an enforced control and reach no query.** `server.Config.QueryLimits`/`RoleLimits` are never assigned outside tests. An operator relying on `max_scan_bytes` to bound spend has no control at all.
4. **`docs/disaster-recovery.md` — the entire recovery procedure fails.** Snapshots are off by default, `wadjet catalog list-snapshots` and `wadjet catalog restore` do not exist, the prefix and timestamp format are wrong, and the `catalog_snapshot:` config section does not exist. This is the doc read during an outage.
5. **`docs/operations.md:246, 271` — the S3 retention lifecycle rules match zero objects.** `"Prefix": "data/"` versus the real `tables/` prefix (`internal/storage/partition/partition.go:47`): an operator who applies them gets no expiry and an unbounded bill.
6. **`docs/ingestion.md:92-101, 392-394` (and `network-analytics.md`, `embedding.md`) — every recommended partition key yields a full scan.** Only `year`/`month`/`day`/`hour` prune (`internal/planner/logical/optimizer.go:17-19`); the docs recommend `date`, `tenant_id`, `device`, `region`. A partition key that is not also a declared column is additionally SQLSTATE 42703.
7. **`docs/ingestion.md:324-368` + `network-analytics.md:127, 205, 291` — the flagship Bento workflow cannot work.** Files are written under `data/` instead of `tables/`, and `CreateTable` leaves an empty manifest that no prefix scan ever fills, so queries return nothing. `catalog.AddFiles` is the missing step.
8. **`docs/embedding.md:117-118` — the documented type assertion panics.** `SUM` over an INT64 column now declares DECIMAL (`internal/planner/physical/plan.go:13855`), boxed as a **string** (`internal/engine/batch/vector.go:727`). `row["total"].(int64)` is a runtime panic — the numeric-typing arc reaching the Go API.
9. **`docs/sql-reference.md:1123-1124` — the two date-arithmetic signatures are wrong, and both fail silently.** `DATE_ADD(ts, 3600000)` is documented as "add an hour" but the numeric argument counts **days** (`internal/engine/expr/expr.go:4844-4846`), so it adds 3.6 million of them. `DATE_DIFF('second', a, b)` takes no unit argument — the 3-arg call parses `'second'` as a date, fails, and returns NULL. A wrong date and a NULL, with no error either way. (`docs/operations.md:25-30` is a close runner-up: six of the scanner metric names do not exist, so every dashboard built from that table is silently empty.)
10. **`README.md:519-522` and `docs/getting-started.md:145` — the two headline Go examples do not compile.** `wadjet.Config` has no `StorageEndpoint` field, and `NewMinIOStore` takes one argument, not `(ctx, cfg)`. `getting-started.md`'s example then `log.Fatal`s twice more on partition keys.

### DECISION-NEEDED (left unedited)

These need a product call, not a documentation correction.

| Item | The decision |
|---|---|
| `README.md:10` — "Scale to zero, start in under 2 seconds" | no banked measurement exists. Keep, re-measure, or drop? |
| `docs/architecture.md:259`, `README.md:10` — "viable at 512 MB - 2 GB RAM" | the spill architecture makes it plausible and `tuning.md` ships a 512 MB profile, but nothing measures it. Bank a number or soften? |
| `docs/embedding.md` premise — the embedded Go API is unreachable from an out-of-tree module (X1) | re-export `objstore.Store`, `parquet.Schema` and `ingest.Config` from `wadjet/` (a code change), or narrow the doc to in-repo use. The doc is annotated with the current constraint; the API decision stands |
| `docs/distributed.md:118, 134, 193, 246` — Docker Compose `build: .` and `ghcr.io/derekmwright/wadjet:latest` | ship a Dockerfile and an image-publish workflow, or drop the container-deployment story. The doc is annotated that neither exists today |
| `docs/competitive-gaps.md:241-258` — the engine and Warden roadmap priority tables | re-ranking a roadmap is the maintainer's call. The factual gap sections are corrected; the priorities are not |
| `docs/performance-bottlenecks.md` §4, §9 | the page-index prune (§4) and column-level I/O parallelism (§9) are real remaining gaps, but their impact estimates have no banked measurement and §9's evidence no longer exists in the code. Re-profile before restating |

### Three code defects this audit surfaced

Not documentation bugs — recorded here because the docs cannot be made both
truthful and useful until they are fixed.

1. `internal/server/server.go:150` — `mux.Use` after route registration panics under chi v5. No test calls `Server.Start()` with a provider.
2. `internal/auth/policy.go:156` — `default: ColumnAllow` turns a mistyped column action into a silent grant. It should reject an unrecognised action.
3. `cmd/wadjet/main.go:1220, 1519` — `server.Config.QueryLimits` and `RoleLimits` are never populated, so the cost guard is dead in every shipped path.
4. `internal/planner/sql/` — `GROUPING(a)` cannot be parsed in a SELECT list (`unexpected token "GROUPING"`), even though `grouping` is in `knownAggregates` (`internal/planner/sql/ast.go:566`). `GROUPING SETS`/`ROLLUP`/`CUBE` work, but the function that tells their rows apart cannot be called.
