# Metadata minmax statistics

Source: internal/planner/physical/metadata_minmax.go — metadata MIN/MAX, moved 2026-09-11 (#1026)

```go
// Metadata MIN/MAX: an un-grouped, un-filtered `SELECT MIN(a), MAX(b) FROM t`
// answered from parquet footer statistics instead of the scan pipeline. Every
// column chunk's footer already carries the chunk's min and max; a query whose
// only outputs are whole-table MIN/MAX (plus, optionally, COUNT(*)) is a fold
// over those numbers, not a decode of the column. ClickBench Q07
// (`SELECT MIN(EventDate), MAX(EventDate) FROM hits`) spent 3.79 CPU-seconds
// dictionary-decoding 100M values whose extremes sit in 100 footers.
//
// This is the MIN/MAX sibling of tryBuildMetadataCount (metadata_count.go) and
// shares its engagement rules: scalar aggregates (no DISTINCT, no GROUP BY /
// grouping sets) directly over a plain catalog scan — no filters, no table
// functions, no TABLESAMPLE, no partition filter — on the local planning path
// only, and only when the manifest carries no delete markers (a merge-on-read
// delete can remove the very row holding the extreme value, and the footer
// cannot know that). Kill switch: WADJET_META_MINMAX=0.
//
// Statistics are read from each file's footer rather than the catalog's
// persisted RG-metadata blob (catalog.TableRGMeta): the blob stores stat
// VALUES but not each file's parquet schema, and without the file's own column
// type we cannot rule out a scan-side type coercion (copyNativeCoercedDirect
// converts Int64→Int32, Int64→Float64, Date→String when the file and the
// catalog disagree) that would make the footer value and the scanned value
// different numbers. The footer read is metadata-only and concurrent — the
// same read buildRGUnits performs for any scan of a never-analyzed table.
//
// Type support is deliberately narrow, limited to types whose statistics are
// both exactly representable and ordered the way the engine orders them:
// Int32, Int64, Date, Timestamp (stats decode to int64) and Float32, Float64
// (stats decode to float64). Explicitly NOT supported:
//   - String/Bytes. Parquet min/max for BYTE_ARRAY may be TRUNCATED, with
//     is_min_value_exact / is_max_value_exact flagging it — and our
//     ColumnStats does not carry those flags out of the thrift layer
//     (parquet.Statistics has them; parquet.ColumnStats drops them). A
//     truncated max is a PREFIX, i.e. smaller than the true max, so trusting
//     it would return a wrong answer. Declined until the exactness flags are
//     plumbed through.
//   - Decimal. The engine finalizes MIN/MAX through a float64 conversion of a
//     scaled Int128; the footer holds an unscaled physical value whose logical
//     type our reader does not attach to the stat. Not provably identical.
//   - Bool, network types, UUID, nested types, Vector: no benefit, no
//     verified ordering, or (FIXED_LEN_BYTE_ARRAY) no stats emitted at all.
//
// NULL semantics: MIN/MAX ignore NULLs, and so do parquet stats — a chunk's
// min/max are computed over its non-null values only. A row group whose column
// is entirely NULL carries no min/max at all (see leafBuffer.buildStats: it
// emits null_count and nothing else), so it is skipped. When every row group
// is all-null — or the table is empty — no value is ever folded and the result
// is NULL, exactly what the aggregate produces. Any row group that is NOT
// all-null yet lacks readable min/max statistics aborts the optimization: we
// never guess, we scan.
```
