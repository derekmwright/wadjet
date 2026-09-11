# Key only join swap rebuild boundary

Source: internal/engine/exec/join.go — HashJoin key-assignment repair, moved 2026-09-11 (#1026)

A build that stores NO ROWS has nothing to rebuild from, and
rebuilding anyway destroys what it does hold. SemiAntiKeyOnly —
every unfiltered semi/anti join (physical/plan.go, "enable key-only
build") — populates the key index and the bloom and leaves
h.buildBatches empty by design; the distinct-pair NE build
(join_semianti_ne.go) is the same shape. The rebuild below resets
buildRows to 0 and buildHasNullKey to false and then recomputes
them by walking h.buildBatches, which for these builds is zero
iterations: both facts stay at their zero values, and the fresh
empty index replaces the populated one.

buildHasNullKey is not bookkeeping. It is the whole of NOT IN's
three-valued rule (#507): a NULL anywhere in the build makes the
answer UNKNOWN for every probe row that did not otherwise match,
so losing it turns `x NOT IN (…)` from "no rows" into "every row"
— silently (#572).

Nothing here needs rebuilding. The key SWAP above stands, because
probe-side resolution needs the corrected names, and the
arrival-time index stays authoritative: the key-only builds
resolve their build key through columnIndexFallback, which maps the
misassigned name to the same physical build column (had it resolved
to nothing, the build itself would have failed). That is the same
argument the evicted-partition guard below makes.
