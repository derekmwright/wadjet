# Worker parquet projection intersection

Source: internal/worker/stream_source.go — if len(s.projectColumns) > 0 {, moved 2026-09-11 (#1026)

Apply column projection to whichever requested names ARE present in
the file schema — the intersection — rather than reverting to the
full file schema the moment any one name is missing.

A name can be missing for two reasons, and only one of them needs the
other requested columns to survive pruning: (1) it is a genuine raw
column that this file's schema predates (schema evolution) — dropping
it here is fine, the file never had it to read; (2) it is a
derived/expression output or bookkeeping sentinel (RowCountOnlyColumn,
"__having_N", a materialized ORDER BY name — see optimizer.go
pushColumnNeeds) that no file schema will ever contain. Either way,
the OTHER requested names are real columns the query does reference,
and dropping them along with the unresolvable one used to route the
entire scan through the row reader whenever the table also carried a
nested-ROW column — up to 60x slower for a query that never touches
that column (#448/#449 F5). The raw columns an expression or having
clause actually reads arrive as their OWN entries in projectColumns
(collectASTColumnRefs walks into InputExpr/JoinFilter/etc. and adds
each leaf ColumnRef beside any synthetic name), so keeping the
intersection never starves a derivation of a column it needs — it
only stops requesting columns nothing downstream will read.

A previous, more aggressive variant of this fell back to full width
on ANY miss because a pre-projection-pushdown InputCol could name a
whole expression with no accompanying raw-column entries at all,
which under intersection alone would starve the derivation of every
column it needed and stalled Q01 at SF10. That gap is closed upstream
now (every ColumnRef inside an expression is pushed as its own
needed name), so the intersection here is safe; if that upstream
guarantee ever regresses, TestTPCHQueries/TestTPCHOptimizationInvariance
will show wrong aggregates, not just a slow scan.

A requested name is matched by batch.ResolveSchemaIndex, not by a
byte-exact set probe. projectColumns is a plan-side list, so a name in
it can still be a column REFERENCE in the lexer's folded spelling
(#731) while the parquet file carries the spelling it was written with
— `RegionID`. Byte-exact, the intersection above turns a MIXED-case
schema into the one shape it was written to avoid: the already-folded
names (`tier`, `counterid`) match, the CamelCase ones do not, and the
scan reads a strict subset of the columns the query references — the
join key among them. That is not a slow scan, it is a wrong answer,
and unlike a total miss it does not reach the full-width fallback
below. Measured on the camel-case invariance battery with
WADJET_SCAN_COL_SANITIZE=0 and every other site fixed, reverting this
resolution alone costs 12 cells — the largest single contributor on
the DAG arms.
