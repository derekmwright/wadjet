# Build cache scan column resolution

Source: internal/coordinator/build_cache.go — prunedScanColumns, moved 2026-09-11 (#1026)

prunedScanColumns returns the subset of stage.Columns that actually exist on
the table's schema. The optimizer's column-pruning pass over-approximates by
putting every referenced column from the surrounding join chain on every
scan node, so a partsupp:1 stage might list region/part/nation columns
alongside its own. The downstream scanner silently filters those out, but
the SQL parser used by the build-cache pre-scan task doesn't — so we have to
intersect against the real schema here before constructing SELECT lists.

Returns nil on any catalog lookup error or when stage.Columns is empty,
which causes the caller to fall back to SELECT *.

The intersection RESOLVES rather than byte-compares, and emits the SCHEMA's
spelling. stage.Columns is a plan-side list, so a name in it can still be a
reference in the lexer's folded spelling (#731) while the catalog keeps what
the parquet file gave it — `RegionID`. Byte-exact, that is not an
over-approximation this function filters out but a real column it DROPS: a
mixed-case schema (`RegionID` beside an already-folded `tier`) keeps the
names that happen to match and loses the rest, and the pre-scan task then
materializes a build side missing its join key. A total miss returns an
empty list, which the caller reads as SELECT * — the safe end.

Measured on the camel-case invariance battery with
WADJET_SCAN_COL_SANITIZE=0 and every other site fixed: reverting this
resolution alone costs 9 cells, all on the two DAG arms.
