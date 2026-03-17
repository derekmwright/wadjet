# Wadjet

Columnar analytics engine in Go with network-native operations, distributed execution, and PostgreSQL wire protocol compatibility.

## Build & Test

```bash
# Build
go build -o wadjet ./cmd/wadjet

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
| `internal/iceberg/` | Apache Iceberg metadata reader |
| `benchmarks/tpch/` | TPC-H benchmark suite (22 queries) |

### Execution Model

- **Vectorized**: Batches of 2048 rows, columnar layout, typed kernels
- **Selection vectors**: Filtering marks indices instead of copying rows
- **Push-based pipelines**: Source → UnaryOperator chain → Sink
- **Pipeline breakers**: Aggregate, Sort, Window act as SinkSource (consume all, then produce)
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

21 types: Bool, Int32, Int64, Float32, Float64, String, Bytes, Timestamp, IPv4, IPv6, CIDR, MAC, Port, Protocol, Duration, UUID, Date, Decimal, Array, Row, Map.

Network-native types (IPv4, IPv6, CIDR, MAC, Port, Protocol) are first-class with dedicated vector storage and 80+ network functions.

### Storage

- **Object store**: S3-compatible (MinIO, AWS S3, R2). `MemStore` for tests, `FileStore` for local dev.
- **Catalog**: NATS KV for metadata. Optimistic concurrency via revision-based CAS.
- **Parquet**: Column projection, row-group predicate pushdown, nested type support.
- **Ingestion**: Micro-batch with configurable flush (128 MB / 1M rows / 60s).

### Distribution

- **Modes**: `standalone` (all-in-one), `coordinator` (plan+dispatch), `worker` (execute)
- **NATS JetStream**: Task queues, result subscriptions, metadata KV
- **Federation**: NATS leaf nodes connect edge clusters to central

## Commit Convention

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <description>

[optional body]

[optional footer(s)]
```

**Types:** `feat`, `fix`, `perf`, `refactor`, `test`, `docs`, `build`, `ci`, `chore`

**Scopes:** `planner`, `engine`, `exec`, `expr`, `batch`, `scan`, `storage`, `parquet`, `catalog`, `pgwire`, `auth`, `worker`, `coordinator`, `ingest`, `iceberg`, `tpch`

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
- **Test patterns**: Table-driven tests preferred. Use `tb.Helper()` in test helpers. Use `objstore.NewMemStore()` for storage in tests (no real S3).

### Code Style

- **Errors**: Wrap with context: `fmt.Errorf("building aggregate: %w", err)`
- **Imports**: Group as stdlib, third-party, internal. No blank lines within groups.
- **No over-engineering**: Don't add abstractions for single-use cases. Three similar lines beats a premature helper.
- **Selection vectors over copying**: Filter operations should set `batch.Sel`, not create new batches.
- **Typed kernels**: Resolve type once per batch/column, dispatch to typed function. No per-row type switches in hot paths.
- **Batch size**: 2048 rows (`batch.DefaultBatchSize`). Do not change without benchmarking.

### What NOT to Do

- Don't write custom Parquet encoding — use `parquet-go`.
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
