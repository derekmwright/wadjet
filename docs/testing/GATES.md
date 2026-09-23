# Testing Requirements — Gate Catalogue

Moved out of the root `CLAUDE.md` (arc TK, token-savings reorg, 2026-09-23) to
keep that file's per-turn load small. The root's Testing Requirements section
now just points here; this is the full catalogue, verbatim, unchanged.

## Testing Requirements

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
  A third, generated arm is the SQLancer harness (`tools/sqlancer/README.md`): `task sqlancer:run-mit ARGS=...` runs NoREC/TLP against `dist/wadjet serve` and `task sqlancer:triage` classifies the findings, with `docs/postgres-differences.md` as its known-difference list.
  Two arms: `EngineSemantics` compares values through the embedded API, and `WireProtocol` compares what the WIRE carries (type OIDs, result format codes, RowDescription, NULL representation, SQLSTATE, command tag, CancelRequest) through pgx against both servers. The wire arm is the one DuckDB cannot provide — a value oracle cannot see a right value under a wrong OID. It **skips** under `-short` or when no server is reachable, so CI is unaffected. Divergences are pinned per entry (`knownBug`) or per wire PROPERTY (`pins`), and a pin that starts agreeing FAILS — deleting it is the fix's proof. Never exempt a divergence by narrowing the corpus: configure the oracle instead (the fixture is loaded into a `--locale=C` database with `COLLATE "C"` text columns, because wadjet compares strings by bytes; `TestPostgresOracleIsConfiguredForByteCollation` guards that and runs without a server).
- **DECIMAL gate**: the TPC-H fixture has a spec-conformant `DECIMAL(15,2)` variant (ADR-0024) — the same dbgen rows with the specification's eight monetary columns as exact fixed-point. It compares DIGIT FOR DIGIT where the answer is decimal, which the FLOAT64 gate's six-significant-digit quantum cannot. Run it after any change to decimal typing, arithmetic, aggregation or the wire declaration:
  ```bash
  go test -run 'TestTPCHQueriesDecimal|TestTPCHDecimalDeclaredTypes' ./benchmarks/tpch/
  go test -run 'TestTPCHOptimizationInvarianceDecimal|TestTwoPathInvarianceDecimal' ./benchmarks/tpch/
  task pg-oracle:test-decimal        # both oracle arms over the decimal fixture
  ```
  The FLOAT64 schema stays the default and the published-number benchmark; the variant is opt-in (`TPCH_DECIMAL=1`, or an explicit `Fixture`). Baseline numbers: `docs/benchmarks/tpch-decimal-baseline-2026-08-29.md`.
- **Spill gate**: the spilled path is a fifth execution arm no shape corpus reaches on purpose — a spill is a condition, not a query shape (ADR-0027). After any change to a pipeline breaker's spill, drain, merge or clone path (`exec/aggregate*.go`, `exec/agg_*.go`, `pipeline.go`, `partitioned_agg.go`, `sort_external.go`, `window_external.go`, `join_spill.go`, `memory/spill.go`) run the type-matrix spill sweep, which asserts per family that the operator actually spilled and replicates every budgeted cell five times:
  ```bash
  go test -run 'TestTypeMatrixAnswersTheSameUnderEveryMemoryBudget' ./wadjet/
  go test -run 'TestSpillArcShapesAgreeOnBothDistributionArms' ./internal/coordinator/   # DAG arms, forced drain
  go test -run 'TestEveryGroupKeyProducerWritesTheSameBytes' ./internal/engine/exec/     # the seam, per type per producer
  go test -run 'TestContainerWindowKeysAnswerTheSameAcrossAWindowSpill' ./wadjet/        # containers through a window spill
  ```
  The container window gate runs at a 256 KiB budget, not the sweep's 512 KiB, and that is load-bearing: at 512 KiB the ARRAY and MAP columns never reach the spill threshold, so those cells would compare two in-memory runs. It asserts engagement PER CELL for that reason.
  The third is the SEAM gate and it is the one to run first after touching a group key's encoding: a HashAggregate writes the merge key from four producers holding three different Go boxes, and it asserts that all of them write the SAME bytes for every flat type (ADR-0023 item 8, #788). It needs no budget and no plan, which is the point — a gate whose trigger is a CONDITION cannot be relied on to fire, and #788 survived four investigation rounds behind one.
  Condition-triggered defects are gated with the test-only knobs `exec.ForceAggDrainEvery(N)` / `WADJET_TEST_FORCE_AGG_DRAIN_EVERY=N` and `exec.ForceSmallSpillRuns` — take the reference arm DISARMED (arming both sides cancels the defect, #790). A single passing spilled run proves nothing: replicate.
- **Numeric typing gate**: a number means the same thing in every spelling and on every path, and the WIRE declares what PostgreSQL declares (ADR-0024 item 2, ADR-0012). After any change to numeric typing, DECIMAL arithmetic, aggregate result types, integer accumulators or the comparison kernels, run the five-arm census and BOTH oracle arms — the wire arm is the only one that sees a right value under a wrong OID:
  ```bash
  go test -run 'TestNumericArc2' ./internal/coordinator/     # single / spilled / DAG / DAG-shuffled / DAG-morsel
  task pg-oracle:test && task pg-oracle:test-decimal
  ```
  Integer SUM/AVG are EXACT types (bigint / numeric), not float64, and a sum that would wrap is an error, never a wrapped number. A DECIMAL result that does not fit the 128-bit carrier at PostgreSQL's scale is 22003, never a silently narrower scale. Deliberate divergences live in ADR-0012's list; a pin that starts agreeing FAILS.

- **Test patterns**: Table-driven tests preferred. Use `tb.Helper()` in test helpers. Use `objstore.NewMemStore()` for storage in tests (no real S3).
- **A gate that walks the filesystem must skip directory names starting with `.` or `_`** (what the Go toolchain itself skips) or enumerate packages with `go list` / `golang.org/x/tools/go/packages` instead. A git worktree lives at `.claude/worktrees/<name>/`, NESTED inside the module root, and is a full second copy of the source — a walking gate sees every file in it a second time, so its verdict depends on whether anybody happens to have a worktree open. This has now been the defect twice (`61eba248`, then the #798 breaker-scope pin).
