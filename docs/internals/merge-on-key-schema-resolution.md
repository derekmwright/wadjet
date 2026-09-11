# Merge on key schema resolution

Source: wadjet/dml.go — func (ev *mergeEvaluator) checkOnKeys(keys []onKeyPair) error {, moved 2026-09-11 (#1026)

checkOnKeys resolves the ON condition's key columns against the two tables.

parseOnKeys already refuses a qualifier that is neither alias; what it never
did was ask whether the COLUMN exists. `ON t.nosuchcol = s.id` matched
nothing and the MERGE reported success with zero rows affected — a wrong
answer dressed as a no-op, where PostgreSQL raises 42703 naming
`t.nosuchcol` (#678 review, residual 3).
It also REWRITES each key to the spelling its relation stores, which is
what makes `matchByKeys` able to find the value. The ON condition is parsed
as an expression, so its column names arrive FOLDED — an unquoted
identifier lower-cases at the lexer (#731) — while the target row is
`readMergeTarget`'s `batch.RecordBatch.RowAt`, keyed by the catalog
schema's spelling, and CamelCase column names are ordinary there. Comparing
the folded name against that map read nil for every row, so
`ON hits.WatchID = s.k` matched NOTHING: every WHEN MATCHED clause was
skipped and the source row fell through to WHEN NOT MATCHED, which
INSERTED a duplicate instead of updating the row that was already there.

The rewrite goes through batch.ResolveSchemaIndex rather than through a map
keyed by the fold, because the fold is only half the rule. A key column
arrives here from the ON condition's PARSE, so it carries the lexer's
verdict: unquoted names are already folded, and a name still carrying an
upper-case letter can only have been DELIMITED. A lowercasing lookup
resolves both alike, so `ON hits."WATCHID" = s.k` bound to `WatchID` and
the MATCHED branch fired, where PostgreSQL raises 42703 for a delimited
name that is not the column's own bytes. It is 42703 here now, and a
FOLDED `hits.watchid` still resolves — the concession a parquet-born
CamelCase schema needs, and the only one (batch/schema.go items 1-4).
