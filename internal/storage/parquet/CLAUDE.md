# internal/storage/parquet

Moved out of the root `CLAUDE.md` (arc TK, token-savings reorg, 2026-09-23) —
this content used to be the root's "Parquet Package Safety" section, verbatim,
unchanged. It applies to everything under this directory.

### Parquet Package Safety

The `internal/storage/parquet/` package is **critical infrastructure** — any data corruption here is catastrophic and could silently affect every query. Changes to this package require:

- **Exhaustive unit tests**: Every encoding/decoding path must be round-trip tested with edge cases (empty data, single value, max values, NaN, zero-length strings, etc.)
- **Fuzz testing**: Decoders that parse untrusted data (Thrift metadata, page data) should have fuzz tests
- **Bit-exact verification**: Verify output against files produced by Apache Parquet reference implementations (parquet-go, PyArrow)
- **No unsafe shortcuts**: Validate lengths and offsets before `unsafe.Slice` casts — an off-by-one corrupts memory silently
- **TPC-H correctness gate**: All 22 queries must pass at SF0.01 after any parquet change, before merging
- **Compaction gate**: `TestCompactionIsIdempotentOverTheTypeMatrix` (`internal/storage/compaction`) — all 22 types plus DECIMAL(9,2)/(18,4)/(38,10) and containers nested in containers, ingest → compact ×3, asserted on both read paths (row reader and native scan) with a PyArrow cross-check. Compaction REPLACES its inputs, so every read→write asymmetry there is silent data loss; run it after any change to the reader, the writer, or the compactor. It asserts VALUES and the declared SCHEMA, deliberately NOT the footer's statistics — a statistics defect is a wrong answer through the row-group prune, which `wadjet.TestTypeMatrixPruningNeverChangesTheAnswer` gates instead (ADR-0018).
- **ANALYZE gate**: `TestAnalyzeCoversEveryTypeMatrixColumn` (same package) — every type the sampler supports must produce a sketch, and the ones it does not are an explicit list asserted in both directions, so coverage cannot be lost by accident.
- **Writer-contract gates** (ADR-0018 §14): a writer that cannot write the file exactly refuses, and never finalizes a file a reader cannot read. After any change to `NewWriter`/`NewNativeWriter`, `ValidateWriteSchema`, the schema-element builders or `writeFooter`, run `go test -run 'TestAClosedWriterIsClosed|TestTheWriterOwnsItsSchema|TestEveryConstructorRefusesASchemaItCannotWrite|TestTheValidBoundaryDeclarationsStillWrite|TestVectorAndDecimalBoundaries|TestFooterTrailerLengthBoundary|TestAnOversizeFooterIsRefused|FuzzWriteSchemaShape' ./internal/storage/parquet/`. Both exported constructors are held to ONE validation; `Close` is terminal (`parquet.ErrWriterClosed`); the schema is deep-copied at construction. `FuzzWriteSchemaShape` is the writer's own untrusted-input target — an arbitrary schema declaration, not decoder bytes.
