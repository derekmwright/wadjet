# Wadjet

Columnar analytics engine in Go with network-native operations, distributed execution, and PostgreSQL wire protocol compatibility.

## Build & Test

```bash
# Build — every binary goes to dist/ (gitignored):
task build            # dist/wadjet (MIT) and dist/wadjetd (AGPL-3.0)
task build-all         # every ./cmd/... into dist/
task clean             # remove dist/
task licensecheck      # hold the license boundary (LICENSING.md)
# Never `go build -o wadjet`: wadjet/ is the API package dir and go writes the binary INTO it rather than refusing (cost two lost test files once — always build to dist/).

go test ./internal/...                                  # unit tests
task test-quiet PKGS="./internal/... ./wadjet/"          # quiet: per-package ok/FAIL, failing tests' names + last ~40 lines only [RUN=pattern]
task test-quiet-log LOG=path.txt                         # filters an existing text log the same way
task affected [BASE=sha]                                 # packages a diff could affect (reverse deps) — feed to `go test -p 2 $(task affected)`

go test -v -run TestTPCHQueries ./benchmarks/tpch/                                  # TPC-H correctness (SF0.01, ~5s)
TPCH_SCALE=1 go test -v -run TestTPCHQueriesLarge -timeout 30m ./benchmarks/tpch/    # TPC-H performance (SF1, ~66s baseline)
go test -bench=. -benchmem ./internal/engine/exec        # micro-benchmarks
go test -bench=. -benchmem ./internal/engine/scan
go test -bench=. -benchmem ./internal/engine/batch

wadjet serve --storage-type=file --data-dir=./wadjet-data --pg-addr=:5432   # embedded server (pgwire over the in-process engine, no S3)
wadjetd serve --mode=standalone --pg-addr=:5432                              # distributed server
```

## Licensing

ONE repo, ONE module, TWO licenses (`LICENSING.md`): the embedded engine and `cmd/wadjet` are MIT; `internal/coordinator`, `internal/worker`, `internal/coordinator/dagplan` (the stage-DAG planner), `internal/distributed`, `internal/dataplane`, `internal/wshf`, `internal/server` (not its `pgwire/` and `mcp/` subdirectories), `internal/clid`, `internal/harness`, `cmd/wadjetd` and the cluster-standing benchmark commands are AGPL-3.0 with a commercial option. Every .go file carries an SPDX header naming its region, every AGPL directory carries a verbatim `LICENSE` copy, and `go run ./tools/licensecheck .` (CI, `task housekeeping`) fails if an MIT package reaches AGPL code through a non-test import at ANY depth — or DECLARES the distributed planner's vocabulary (the stage, the exchange, the distribution property; see `tools/licensecheck/dagvocabulary.go`). A new package inherits MIT unless it is declared in `tools/licensecheck/regions.go`.

## Architecture

### Query Pipeline

SQL text → Parser (`internal/planner/sql/`, recursive descent, custom AST) → Logical Plan (`internal/planner/logical/`, tree of typed nodes + rule-based optimizer) → Physical Plan (`internal/planner/physical/`, executable local pipeline, ADR-0037 §6) → Execution (`internal/engine/exec/`, push-based: Source → [UnaryOps] → Sink) → Results.

### Key Packages

| Package | Purpose |
|---|---|
| `wadjet/` | Public embeddable API (`wadjet.DB`, `wadjet.Open()`) |
| `cmd/wadjet/` | MIT binary: the CLI and the embedded server |
| `cmd/wadjetd/` | AGPL binary: `serve --mode=standalone\|coordinator\|worker` |
| `internal/cli/` | The command tree, the persistent flags and the config precedence both binaries share |
| `internal/clid/` | The distributed serve modes (runStandalone / runCoordinator / runWorker) |
| `internal/queryroute/` | The interface pgwire routes SELECT through (the coordinator satisfies it; the embedded server installs none) |
| `internal/natsconn/` | Opening NATS: the embedded server, the connections, the JetStream context |
| `internal/catalogdir/` | The catalog DIRECTORY (`<data-dir>/_catalog`): the flock, the embedded JetStream store, the published holder — one mechanism under `wadjet.Config.DataDir`, the CLI commands and `wadjet serve` (ADR-0041) |
| `internal/engine/batch/` | Record batches, vectors, selection vectors, batch pooling |
| `internal/engine/exec/` | Pipeline executor, operators (filter, project, join, sort, aggregate, window); aggregate seams in `agg_consume.go`, `agg_accumulators.go`, `agg_partial_merge.go`, `agg_spill.go` |
| `internal/engine/expr/` | Expression compiler, 417 scalar functions; sections in `expr_arith.go`, `expr_compare.go`, `expr_scalar_fns.go`, `expr_string_fns.go` |
| `internal/engine/scan/` | 3-level predicate pushdown scanner |
| `internal/engine/memory/` | Per-task memory budget, spill-to-disk |
| `internal/planner/sql/` | SQL parser + AST types |
| `internal/planner/logical/` | Logical plan builder + optimizer |
| `internal/planner/physical/` | LOCAL pipeline planner (MIT): `planner_entry.go`, `pipeline_plan.go`, `declared_output.go`, `join_plan.go`, the query-limit cost walk. The stage DAG is `internal/coordinator/dagplan` |
| `internal/storage/objstore/` | S3-compatible object store (MemStore, MinIOStore, FileStore) |
| `internal/storage/catalog/` | Metadata in NATS KV |
| `internal/storage/parquet/` | Parquet reader/writer — see its own `CLAUDE.md` for package-safety rules |
| `internal/storage/ingest/` | Micro-batch accumulator + partitioner |
| `internal/coordinator/` | Query coordinator (plan, dispatch, merge); `dag_dispatch.go`, `dag_compute.go`, `dag_fragments.go`, `dag_merge.go` — see its own `CLAUDE.md` for the native-DAG map |
| `internal/coordinator/dagplan/` | The distributed PLANNER (AGPL): stage emission, distribution/exchange assignment, shuffle fusion, set-op stage planning, DAG validation and refusals |
| `internal/worker/` | Distributed task executor |
| `internal/server/pgwire/` | PostgreSQL wire protocol |
| `internal/auth/` | API keys, JWT, mTLS, RBAC, ABAC policy engine, identity enrichment |
| `internal/embedding/` | embed() SQL function — OpenAI / Voyage AI / Ollama providers, SQL-level batching (one API call per record batch), LRU cache, pluggable interface. Anthropic has no native embeddings endpoint; Voyage AI is its recommended path. |
| `internal/iceberg/` | Apache Iceberg metadata reader |
| `benchmarks/tpch/` | TPC-H benchmark suite (22 queries) |

### Execution Model

- **Vectorized**: Batches of 2048 rows, columnar layout, typed kernels
- **Selection vectors**: Filtering marks indices instead of copying rows
- **Push-based pipelines**: Source → UnaryOperator chain → Sink
- **Pipeline breakers**: Aggregate, Sort, Window act as SinkSource (consume all, then produce)
- **Spill-to-disk**: All pipeline breakers degrade gracefully past memory — HashJoin (grace partition-on-arrival), HashAggregate (partial-state k-way merge), Sort and Window (sorted-run external merge, streaming k-way; empty-PARTITION-BY windows stream via a two-pass evaluator over the runs; nested Array/Map/Row schemas ride the columnar run format). Remaining bounds: cume_dist holds back the open ORDER-BY peer group; window-partition peak memory is the largest single partition; a CROSS join — which is how an INNER join on an EXPRESSION rather than on columns is executed — does not spill at all, because its probe reads every build row and so cannot use a partitioned build (ADR-0006's 2026-09-03 routed-probe amendment, #832): its build must fit the budget and refuses loudly when it does not. An OUTER join whose ON has no column equality is a KEYLESS hash join, not a cross join, and spills like any other (ADR-0006's 2026-09-18 amendment).
- **Batch pooling**: `BatchPool` for zero-alloc batch reuse

### Core Interfaces

```go
type Source interface { Init(ctx context.Context) error; Next(ctx context.Context) (*batch.RecordBatch, error); Close() error }                                                  // produces batches
type UnaryOperator interface { Init(ctx context.Context) error; Execute(ctx context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error); Close() error }                  // transforms in-place, non-blocking
type Sink interface { Init(ctx context.Context) error; Consume(ctx context.Context, b *batch.RecordBatch) error; Finalize(ctx context.Context) error; Close() error }             // consumes all input, pipeline breaker
```

### Type System

22 types: Bool, Int32, Int64, Float32, Float64, String, Bytes, Timestamp, IPv4, IPv6, CIDR, MAC, Port, Protocol, Duration, UUID, Date, Decimal, Array, Row, Map, Vector.

Network-native types (IPv4, IPv6, CIDR, MAC, Port, Protocol) are first-class with dedicated vector storage and 80+ network functions. VECTOR(N) stores fixed-dimension float32 embeddings with cosine_similarity, l2_distance, dot_product.

### Storage

- **Object store**: S3-compatible (MinIO, AWS S3, R2). `MemStore` for tests, `FileStore` for local dev.
- **Catalog**: NATS KV for metadata. Optimistic concurrency via revision-based CAS.
- **Parquet**: Column projection, row-group predicate pushdown, nested type support.
- **Ingestion**: Micro-batch with configurable flush (128 MB / 1M rows / 60s).

### Distribution

- **Modes**: `standalone` (all-in-one), `coordinator` (plan+dispatch), `worker` (execute)
- **Small-query fast path**: the coordinator routes queries whose post-pruning catalog scan bytes stay under `--local-fastpath-bytes` (default 64 MiB; 0 disables) onto an in-process single-process pipeline, skipping the DAG's per-stage dispatch + S3 materialization. Both paths consume the identical optimized logical plan. A local failure falls back to the DAG only when it says nothing about the query's meaning (result-budget bail-out, local memory budget, S3 unreachable); a deterministic execution failure is reported to the client instead, since retrying it on a path that may disagree turns a loud failure into a silently different answer (#308, `WADJET_FASTPATH_STRICT=0` restores the old behavior). See `docs/internals/native-dag-execution.md` §Small-query local fast path.
- **Stage-DAG execution**: Distributed queries run as a multi-stage DAG. Every stage's output **materializes to S3** (`queries/<id>/...`) and is read back by the next stage — structurally like Trino's fault-tolerant execution with exchange spooling, not like its streaming mode. `exchange-repartition` stages hash-partition `.wshf` shuffle files; large-build joins (> `shuffleBuildThreshold`) take this path by default. `--shuffle-durability=eager|lazy|off` (default eager) controls whether those S3 uploads start immediately, queue until demanded (released on consumer retry / coordinator read / worker drain, elided at query end), or never run — see `docs/design/shuffle-durability.md`.
- **Broadcast + probe-split**: Builds under `BroadcastBytesThreshold` replicate to all workers; the probe side's files are split across workers, each runs the full join, coordinator merges partial results (re-aggregation, sort, dedup).
- **Skew-aware shuffle** (`--skew-split`, default on; `=false` = kill switch): at join dispatch, a partition group whose probe bytes exceed 256 MiB AND ≥2× the mean group splits into k sub-tasks dividing its probe files and replicating its build files. Uniform-heavy stages never split (ratio gate). See `docs/design/skew-aware-shuffle.md`; A/B fixture: `benchmarks/skew`.
- **NATS JetStream**: Task queues with request/reply result delivery, metadata KV
- **Federation**: NATS leaf nodes connect edge clusters to central
- **Internals map**: see `internal/coordinator/CLAUDE.md` for the native-DAG file map and navigation notes.
- **Decision records**: `docs/adr/` — the settled architectural positions (execution model, exchange design, durability/placement/scratch policies, measurement methodology) with the alternatives they beat. Read the relevant ADR before proposing changes in its territory; reopening one requires new evidence.

## Commit Convention

Use [Conventional Commits](https://www.conventionalcommits.org/): `<type>(<scope>): <description>`, optionally followed by a blank line + body and/or a blank line + footer(s).

**Sign-off required:** commit with `git commit -s` so every commit carries a `Signed-off-by:` trailer (the CLA consent mechanism — see CLA.md §6). Do NOT add `Co-Authored-By` trailers naming AI tools: the human committer is the sole author (see CONTRIBUTING.md, "AI-Assisted Development").

**Types:** `feat`, `fix`, `perf`, `refactor`, `test`, `docs`, `build`, `ci`, `chore` — **Scopes:** `planner`, `engine`, `exec`, `expr`, `batch`, `scan`, `storage`, `parquet`, `catalog`, `pgwire`, `auth`, `worker`, `coordinator`, `ingest`, `iceberg`, `embedding`, `tpch`

Examples:
```
feat(expr): add format_bytes and parse_bytes scalar functions
fix(planner): resolve ORDER BY aggregate to correct output column
perf(exec): use typed sort kernels instead of interface comparison
test(tpch): add SF1 regression benchmarks for Q5 and Q17
refactor(scan): extract predicate pushdown into separate module
```

## Code Guidelines

### Testing Requirements

See `docs/testing/GATES.md` for the full gate catalogue — regression/unit test requirements, the kill-switch optimization-invariance oracle, the PostgreSQL/DuckDB differential oracles, and the DECIMAL/spill/numeric-typing gates with their exact commands and rationale. Read it before any change to `internal/optswitch`, `internal/server/pgwire/`, numeric/decimal typing, or a pipeline breaker's spill/drain/merge/clone path.

### Code Style

- **Errors**: Wrap with context: `fmt.Errorf("building aggregate: %w", err)`
- **Imports**: Group as stdlib, third-party, internal. No blank lines within groups.
- **No over-engineering**: Don't add abstractions for single-use cases. Three similar lines beats a premature helper.
- **Selection vectors over copying**: Filter operations should set `batch.Sel`, not create new batches.
- **Typed kernels**: Resolve type once per batch/column, dispatch to typed function. No per-row type switches in hot paths.
- **Batch size**: 2048 rows (`batch.DefaultBatchSize`). Do not change without benchmarking.

### Documentation & Release Housekeeping

See `docs/testing/RELEASE.md` for the documentation-moves-with-the-code rule and the full pre-tag housekeeping pass (docs, doc comments, ADRs, issue closes, release notes, `task housekeeping`).

### What NOT to Do

- Don't add SIMD intrinsics — the Go compiler handles vectorization.
- Don't mock the object store in tests — use `objstore.NewMemStore()`.
- Don't add features to the SQL parser using a parser generator — it's recursive descent by design.
- Don't skip pgwire compatibility — tools like Superset, psql, and JDBC depend on it.

## Run Modes

```bash
wadjet serve --storage-type=file --data-dir=./wadjet-data --pg-addr=:5432    # embedded (one process, no S3)
wadjetd serve --mode=standalone --pg-addr=:5432                               # development (all-in-one)
wadjetd serve --mode=coordinator --pg-addr=:5432 --nats-url=nats://nats:4222  # production: coordinator
wadjetd serve --mode=worker --nats-url=nats://nats:4222                       # production: worker
psql -h localhost -p 5432 -U wadjet -d wadjet                                 # query interface
```

## CI/CD Automation

### Workflows

| Workflow | Trigger | Purpose |
|---|---|---|
| `ci.yml` | Push/PR to main | Build, unit tests, TPC-H SF0.01 correctness |
| `issue-worker.yml` | Issue opened or labeled `auto-fix` | Triage issues, auto-fix with PR |
| `pr-review.yml` | PR opened/updated | Automated code review |

### Issue-to-PR Flow

All Claude workflows are **label-gated** (only maintainers add labels, so no external user can trigger API costs): `needs-triage` → Claude triages, commenting analysis/root-cause/complexity → maintainer adds `auto-fix` → Claude branches, fixes, writes tests, opens a PR → maintainer adds `needs-review` on the PR → Claude reviews the diff → CI runs tests + the SF1 benchmark on every PR → a human approves and merges. Concurrency limits keep only one Claude workflow running at a time. Requires `ANTHROPIC_API_KEY` (repo or org Settings → Secrets → Actions).
