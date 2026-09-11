# Scan sanitize disabled spelling contract

Source: internal/planner/logical/optimizer.go — sanitizeScanNeeds, moved 2026-09-11 (#1026)

The switch is a POLLUTION A/B and nothing else. It restores the
pre-2026-07 list — every accumulated need, junk included — but it
does NOT restore the pre-2026-07 SPELLING, because the spelling is
not an optimization: RequiredColumns is a scan's read set, it names
columns OF THIS TABLE, and every consumer of it byte-compares
against the table's schema (physical.buildReadSchema, the
scan-cache projection, worker cachedFileStreamSource.projectColumns
via Stage.Columns, coordinator.prunedScanColumns).

Bundling the two behind one switch made an optimization knob
load-bearing for CORRECTNESS, which is the one thing a kill switch
must never be. The mechanism, measured on the camel-case invariance
battery with WADJET_SCAN_COL_SANITIZE=0: a MIXED-case schema
(`RegionID` beside an already-folded `counterid`) made the folded
needs match the folded columns and miss the CamelCase ones, so
buildReadSchema returned a PARTIAL projection — not the full-width
fallback a TOTAL miss reaches — and the scan silently dropped the
GROUP BY key, the join key and the ORDER BY key. That arm answered
30 of the battery's 63 cells differently from the identical
all-lower fixture. With every downstream consumer of this list
separately taught to RESOLVE rather than byte-compare, the property
is now owned JOINTLY: reverting this respelling alone diverges on NO
cell, because physical.buildReadSchema resolves the folded names it
then receives. The respelling stays here anyway — this is where the
schema's spelling is known — and the CamelCase battery drives BOTH
switch states, so a consumer that stops resolving is caught with the
arm and the state named rather than waiting for the next corpus that
happens to carry a mixed-case schema.
