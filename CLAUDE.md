# Wadjet

Columnar analytics engine in Go with network-native operations, distributed execution, and PostgreSQL wire protocol compatibility.

## Build & Test

```bash
# Build — every binary goes to dist/ (gitignored). Use the Taskfile:
task build          # dist/wadjet
task build-all      # every ./cmd/... into dist/
task clean          # remove dist/
#
# Never `go build -o wadjet`: wadjet/ is the API package directory and go
# writes the binary INTO it rather than refusing. That is why /wadjet was
# once gitignored — which hid new source files in that package from
# `git status` and lost two test files.

# Unit tests
go test ./internal/...

# TPC-H correctness (SF0.01, ~5s)
go test -v -run TestTPCHQueries ./benchmarks/tpch/

# TPC-H performance (SF1, ~66s baseline)
TPCH_SCALE=1 go test -v -run TestTPCHQueriesLarge -timeout 30m ./benchmarks/tpch/

# Micro-benchmarks
go test -bench=. -benchmem ./internal/engine/exec
go test -bench=. -benchmem ./internal/engine/scan
go test -bench=. -benchmem ./internal/engine/batch

# Run standalone server
wadjet serve --mode=standalone --pg-addr=:5432
```

## Architecture

### Query Pipeline

```
SQL text
  → Parser (internal/planner/sql/)        — recursive descent, custom AST
  → Logical Plan (internal/planner/logical/) — tree of typed nodes + rule-based optimizer
  → Physical Plan (internal/planner/physical/) — executable pipeline(s) + distributed stages
  → Execution (internal/engine/exec/)      — push-based: Source → [UnaryOps] → Sink
  → Results
```

### Key Packages

| Package | Purpose |
|---|---|
| `wadjet/` | Public embeddable API (`wadjet.DB`, `wadjet.Open()`) |
| `cmd/wadjet/` | CLI entry point (Cobra: serve, query, shell, mcp) |
| `internal/engine/batch/` | Record batches, vectors, selection vectors, batch pooling |
| `internal/engine/exec/` | Pipeline executor, operators (filter, project, join, sort, aggregate, window) |
| `internal/engine/expr/` | Expression compiler, 273+ scalar functions |
| `internal/engine/scan/` | 3-level predicate pushdown scanner |
| `internal/engine/memory/` | Per-task memory budget, spill-to-disk |
| `internal/planner/sql/` | SQL parser + AST types |
| `internal/planner/logical/` | Logical plan builder + optimizer |
| `internal/planner/physical/` | Physical planner + distributed task stages |
| `internal/storage/objstore/` | S3-compatible object store (MemStore, MinIOStore, FileStore) |
| `internal/storage/catalog/` | Metadata in NATS KV |
| `internal/storage/parquet/` | Parquet reader/writer |
| `internal/storage/ingest/` | Micro-batch accumulator + partitioner |
| `internal/coordinator/` | Query coordinator (plan, dispatch, merge) |
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
- **Spill-to-disk**: All pipeline breakers degrade gracefully past memory — HashJoin (grace partition-on-arrival), HashAggregate (partial-state k-way merge), Sort and Window (sorted-run external merge, streaming k-way; empty-PARTITION-BY windows stream via a two-pass evaluator over the runs; nested Array/Map/Row schemas ride the columnar run format). Remaining bound: cume_dist holds back the open ORDER-BY peer group; window-partition peak memory is the largest single partition.
- **Batch pooling**: `BatchPool` for zero-alloc batch reuse

### Core Interfaces

```go
// Source produces batches
type Source interface {
    Init(ctx context.Context) error
    Next(ctx context.Context) (*batch.RecordBatch, error)
    Close() error
}

// UnaryOperator transforms batches in-place (non-blocking)
type UnaryOperator interface {
    Init(ctx context.Context) error
    Execute(ctx context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error)
    Close() error
}

// Sink consumes all input (pipeline breaker)
type Sink interface {
    Init(ctx context.Context) error
    Consume(ctx context.Context, b *batch.RecordBatch) error
    Finalize(ctx context.Context) error
    Close() error
}
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
- **Internals map**: `docs/internals/native-dag-execution.md` — file-anchored map of the native-DAG path (two coordinator entry paths, `walkStages` per-node stage emission, Stage→fragment conversion, the distribution-property/shuffle system, and inspection recipes). Start here before navigating coordinator/planner/worker distribution code.
- **Decision records**: `docs/adr/` — the settled architectural positions (execution model, exchange design, durability/placement/scratch policies, measurement methodology) with the alternatives they beat. Read the relevant ADR before proposing changes in its territory; reopening one requires new evidence.

## Commit Convention

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <description>

[optional body]

[optional footer(s)]
```

**Sign-off required:** commit with `git commit -s` so every commit carries a
`Signed-off-by:` trailer (the CLA consent mechanism — see CLA.md §6).
Do NOT add `Co-Authored-By` trailers naming AI tools: the human committer
is the sole author (see CONTRIBUTING.md, "AI-Assisted Development").

**Types:** `feat`, `fix`, `perf`, `refactor`, `test`, `docs`, `build`, `ci`, `chore`

**Scopes:** `planner`, `engine`, `exec`, `expr`, `batch`, `scan`, `storage`, `parquet`, `catalog`, `pgwire`, `auth`, `worker`, `coordinator`, `ingest`, `iceberg`, `embedding`, `tpch`

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

- **All bug fixes must include a regression test.** The test should fail before the fix and pass after.
- **New features must include unit tests** covering expected behavior and edge cases.
- **Performance-sensitive changes must include benchmark comparison.** Run TPC-H SF1 before and after to measure impact:
  ```bash
  # Before (on main or parent commit)
  TPCH_SCALE=1 go test -v -run TestTPCHQueriesLarge -timeout 30m ./benchmarks/tpch/ 2>&1 | tee /tmp/bench-before.txt

  # After (on feature branch)
  TPCH_SCALE=1 go test -v -run TestTPCHQueriesLarge -timeout 30m ./benchmarks/tpch/ 2>&1 | tee /tmp/bench-after.txt
  ```
- **Optimizations that can change the row set must register a kill switch** in `internal/optswitch` (env convention: `WADJET_<NAME>=0` disables) and pass the optimization-invariance oracle, which runs every corpus query with each switch individually disabled and asserts identical results:
  ```bash
  # TPC-H arm (SF0.01, ~30s, runs in CI)
  go test -run TestTPCHOptimizationInvariance ./benchmarks/tpch/
  # ClickBench arm (needs a local hits part)
  WADJET_HITS_PART=/path/to/hits_0.parquet go test -run TestHitsOptimizationInvariance ./benchmarks/clickbench/
  ```
  A divergence names the disabled toggle — that is the defect localization. Registering the switch extends the oracle for free; this is part of the definition of done for optimization work (#287).
- **PostgreSQL is the authority on SEMANTICS; DuckDB is the performance goal and a second correctness oracle.** Wadjet ships the PostgreSQL wire protocol, so "is this what a PostgreSQL client expects" is a question only PostgreSQL can answer. Changes to expression semantics, NULL handling, type resolution, error reporting or anything in `internal/server/pgwire/` run the PostgreSQL differential oracle:
  ```bash
  task pg-oracle:test                      # SF0.01, ~15s; starts and tears down its own postgres:17-alpine
  task pg-oracle:test-large SCALE=0.1      # generated tier (~60s), one source feeds both engines
  ```
  Two arms: `EngineSemantics` compares values through the embedded API, and `WireProtocol` compares what the WIRE carries (type OIDs, result format codes, RowDescription, NULL representation, SQLSTATE, command tag, CancelRequest) through pgx against both servers. The wire arm is the one DuckDB cannot provide — a value oracle cannot see a right value under a wrong OID. It **skips** when no server is reachable, so CI is unaffected. Divergences are pinned per entry (`knownBug`) or per wire PROPERTY (`pins`), and a pin that starts agreeing FAILS — deleting it is the fix's proof. Never exempt a divergence by narrowing the corpus: configure the oracle instead (the fixture is loaded into a `--locale=C` database with `COLLATE "C"` text columns, because wadjet compares strings by bytes; `TestPostgresOracleIsConfiguredForByteCollation` guards that and runs without a server).
- **DECIMAL gate**: the TPC-H fixture has a spec-conformant `DECIMAL(15,2)` variant (ADR-0024) — the same dbgen rows with the specification's eight monetary columns as exact fixed-point. It compares DIGIT FOR DIGIT where the answer is decimal, which the FLOAT64 gate's six-significant-digit quantum cannot. Run it after any change to decimal typing, arithmetic, aggregation or the wire declaration:
  ```bash
  go test -run 'TestTPCHQueriesDecimal|TestTPCHDecimalDeclaredTypes' ./benchmarks/tpch/
  go test -run 'TestTPCHOptimizationInvarianceDecimal|TestTwoPathInvarianceDecimal' ./benchmarks/tpch/
  task pg-oracle:test-decimal        # both oracle arms over the decimal fixture
  ```
  The FLOAT64 schema stays the default and the published-number benchmark; the variant is opt-in (`TPCH_DECIMAL=1`, or an explicit `Fixture`). Baseline numbers: `docs/benchmarks/tpch-decimal-baseline-2026-08-29.md`.
- **Spill gate**: the spilled path is a fifth execution arm no shape corpus reaches on purpose — a spill is a condition, not a query shape (ADR-0027). After any change to a pipeline breaker's spill, drain, merge or clone path (`exec/aggregate*.go`, `pipeline.go`, `partitioned_agg.go`, `sort_external.go`, `window_external.go`, `join_spill.go`, `memory/spill.go`) run the type-matrix spill sweep, which asserts per family that the operator actually spilled and replicates every budgeted cell five times:
  ```bash
  go test -run 'TestTypeMatrixAnswersTheSameUnderEveryMemoryBudget' ./wadjet/
  go test -run 'TestSpillArcShapesAgreeOnBothDistributionArms' ./internal/coordinator/   # DAG arms, forced drain
  go test -run 'TestEveryGroupKeyProducerWritesTheSameBytes' ./internal/engine/exec/     # the seam, per type per producer
  ```
  The third is the SEAM gate and it is the one to run first after touching a group key's encoding: a HashAggregate writes the merge key from four producers holding three different Go boxes, and it asserts that all of them write the SAME bytes for every flat type (ADR-0023 item 8, #788). It needs no budget and no plan, which is the point — a gate whose trigger is a CONDITION cannot be relied on to fire, and #788 survived four investigation rounds behind one.
  Condition-triggered defects are gated with the test-only knobs `exec.ForceAggDrainEvery(N)` / `WADJET_TEST_FORCE_AGG_DRAIN_EVERY=N` and `exec.ForceSmallSpillRuns` — take the reference arm DISARMED (arming both sides cancels the defect, #790). A single passing spilled run proves nothing: replicate.
- **Numeric typing gate**: a number means the same thing in every spelling and on every path, and the WIRE declares what PostgreSQL declares (ADR-0024 item 2, ADR-0012). After any change to numeric typing, DECIMAL arithmetic, aggregate result types, integer accumulators or the comparison kernels, run the five-arm census and BOTH oracle arms — the wire arm is the only one that sees a right value under a wrong OID:
  ```bash
  go test -run 'TestNumericArc2' ./internal/coordinator/     # single / spilled / DAG / DAG-shuffled / DAG-morsel
  task pg-oracle:test && task pg-oracle:test-decimal
  ```
  Integer SUM/AVG are EXACT types (bigint / numeric), not float64, and a sum that would wrap is an error, never a wrapped number. A DECIMAL result that does not fit the 128-bit carrier at PostgreSQL's scale is 22003, never a silently narrower scale. Deliberate divergences live in ADR-0012's list; a pin that starts agreeing FAILS.

- **Test patterns**: Table-driven tests preferred. Use `tb.Helper()` in test helpers. Use `objstore.NewMemStore()` for storage in tests (no real S3).

### Code Style

- **Errors**: Wrap with context: `fmt.Errorf("building aggregate: %w", err)`
- **Imports**: Group as stdlib, third-party, internal. No blank lines within groups.
- **No over-engineering**: Don't add abstractions for single-use cases. Three similar lines beats a premature helper.
- **Selection vectors over copying**: Filter operations should set `batch.Sel`, not create new batches.
- **Typed kernels**: Resolve type once per batch/column, dispatch to typed function. No per-row type switches in hot paths.
- **Batch size**: 2048 rows (`batch.DefaultBatchSize`). Do not change without benchmarking.

### Parquet Package Safety

The `internal/storage/parquet/` package is **critical infrastructure** — any data corruption here is catastrophic and could silently affect every query. Changes to this package require:

- **Exhaustive unit tests**: Every encoding/decoding path must be round-trip tested with edge cases (empty data, single value, max values, NaN, zero-length strings, etc.)
- **Fuzz testing**: Decoders that parse untrusted data (Thrift metadata, page data) should have fuzz tests
- **Bit-exact verification**: Verify output against files produced by Apache Parquet reference implementations (parquet-go, PyArrow)
- **No unsafe shortcuts**: Validate lengths and offsets before `unsafe.Slice` casts — an off-by-one corrupts memory silently
- **TPC-H correctness gate**: All 22 queries must pass at SF0.01 after any parquet change, before merging
- **Compaction gate**: `TestCompactionIsIdempotentOverTheTypeMatrix` (`internal/storage/compaction`) — all 22 types plus DECIMAL(9,2)/(18,4)/(38,10) and containers nested in containers, ingest → compact ×3, asserted on both read paths (row reader and native scan) with a PyArrow cross-check. Compaction REPLACES its inputs, so every read→write asymmetry there is silent data loss; run it after any change to the reader, the writer, or the compactor. It asserts VALUES and the declared SCHEMA, deliberately NOT the footer's statistics — a statistics defect is a wrong answer through the row-group prune, which `wadjet.TestTypeMatrixPruningNeverChangesTheAnswer` gates instead (ADR-0018).
- **ANALYZE gate**: `TestAnalyzeCoversEveryTypeMatrixColumn` (same package) — every type the sampler supports must produce a sketch, and the ones it does not are an explicit list asserted in both directions, so coverage cannot be lost by accident.

### Documentation Is Part of Done

- **User-facing docs move with the code.** An arc that changes a flag, a default, a config key, a type rule, a refusal, a supported-SQL statement or a benchmark number updates `docs/*.md` and the README in the same branch — the way ADRs already ship with the code. The 2026-09-02 drift audit (`docs/testing/docs-drift-audit-2026-09-02.md`) found 130 drifted and 29 unsupported claims across 20 docs after twelve releases that kept only CLAUDE.md and the ADRs current; it is the baseline the next audit diffs against.

### What NOT to Do

- Don't add SIMD intrinsics — the Go compiler handles vectorization.
- Don't mock the object store in tests — use `objstore.NewMemStore()`.
- Don't add features to the SQL parser using a parser generator — it's recursive descent by design.
- Don't skip pgwire compatibility — tools like Superset, psql, and JDBC depend on it.

## Run Modes

```bash
# Development (all-in-one)
wadjet serve --mode=standalone --pg-addr=:5432

# Production distributed
wadjet serve --mode=coordinator --pg-addr=:5432 --nats-url=nats://nats:4222
wadjet serve --mode=worker --nats-url=nats://nats:4222

# Query interface
psql -h localhost -p 5432 -U wadjet -d wadjet
```

## CI/CD Automation

### Workflows

| Workflow | Trigger | Purpose |
|---|---|---|
| `ci.yml` | Push/PR to main | Build, unit tests, TPC-H SF0.01 correctness |
| `issue-worker.yml` | Issue opened or labeled `auto-fix` | Triage issues, auto-fix with PR |
| `pr-review.yml` | PR opened/updated | Automated code review |

### Issue-to-PR Flow

All Claude workflows are **label-gated** — only maintainers can add labels, so no external user can trigger API costs.

1. Issue opened → maintainer reviews, adds `needs-triage` label
2. Claude triages: comments with analysis, root cause, complexity estimate
3. Maintainer adds `auto-fix` label → Claude creates branch, fixes, writes tests, opens PR
4. Maintainer adds `needs-review` label on PR → Claude reviews the diff
5. CI runs tests + SF1 benchmark automatically on all PRs
6. Human approves and merges

Concurrency limits ensure only one Claude workflow runs at a time.

### Required Secrets

- `ANTHROPIC_API_KEY` — Claude API key (set at repo or org level in Settings → Secrets → Actions)
