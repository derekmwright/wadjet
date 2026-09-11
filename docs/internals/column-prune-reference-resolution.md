# Column prune reference resolution

Source: internal/engine/exec/column_prune.go — ColumnPrune.Execute, moved 2026-09-11 (#1026)

Lazy resolve column indices on first batch.

Two passes, and the second is the one that survives a name the batch
spells differently. The keep list is a set of column REFERENCES off
the plan: it arrives folded from the lexer (#731) while the batch
carries the catalog's own spelling (`WatchID` for a parquet-registered
table), and it may be alias-QUALIFIED (`a.regionid`) over a stream
that publishes the column bare — which is ordinary for a CTE read
twice under two aliases. columnIndexFallback is the resolver that owns
both of those, and it is what every other consumer of a planner-emitted
column name in this package already uses; a byte-exact map probe alone
is not that rule restated, it is a different one. The pass is ADDITIVE
— it can only mark a column kept that the exact pass did not — so a
schema carrying two columns of one name still keeps both, and an
ambiguous reference declines to resolve rather than guessing an arm.

A TOTAL miss passes the batch through at full width instead of
emitting a ZERO-COLUMN one. That is the fallback every other pruner on
this path already takes (worker.wshfShufflePruneKeep,
cachedFileStreamSource.projectColumns, physical.buildReadSchema): a
prune is a memory optimization, so declining to prune is always
available, while emitting no columns at all is not a narrower answer
but a broken stream — the shuffle sink above it refuses with
`key "a.regionid" not in schema` and the query fails.

Measured on the camel-case invariance battery with
WADJET_SCAN_COL_SANITIZE=0 and every other site fixed: reverting this
pass alone costs 6 cells, and 3 of those survive even when the scan's
read set carries the schema's spelling — a CTE read twice under two
aliases reaches here with a keep list that is entirely qualified, which
no spelling fix upstream can repair.
