# Batch column name resolution

Source: internal/engine/batch/schema.go — func FoldIdent(s string) string {, moved 2026-09-11 (#1026)

Column-name resolution against a batch schema.

The SCHEMA is byte-exact — `ColumnIndex` compares `col.Name == name` and
nothing here changes that. A column called `WatchID` is called `WatchID`,
and every producer that writes a batch, a shuffle file or a parquet file
writes the name it was given. What folds is the RESOLVER: the step that
takes a reference the user wrote and decides which column of this batch it
names.

The rule is PostgreSQL's, plus one recorded divergence (ADR-0012):

 1. An UNQUOTED identifier folds to lower case at the lexer
    (`plansql.FoldIdent`), so by the time a reference reaches a batch it is
    already the folded spelling. A DELIMITED identifier keeps its bytes.
    The two are therefore distinguishable from the name alone: a reference
    carrying an ASCII upper-case letter can only have been delimited (or
    minted by the planner from a schema, which byte-matches anyway).
 2. PostgreSQL then matches the folded name EXACTLY against the catalog, so
    a column stored as `"WatchID"` is unreachable as `watchid`. Wadjet's
    tables come from parquet and ingest, where CamelCase column names are
    ordinary — ClickBench's `hits` has `WatchID`, `UserID`, `EventTime` —
    and refusing to resolve them would make those tables unqueryable
    without quoting every reference. So a FOLDED reference that misses
    byte-exact resolves case-insensitively when exactly one column matches.
 3. Two columns matching is ambiguous and resolves to nothing, which the
    caller reports as the miss it is. Within one table this cannot happen:
    `catalog.checkDistinctColumnNames` already refuses a schema whose
    columns collide under `parquet.FoldName`. Across relations it can, and
    the planner refuses it earlier with 42702.
 4. A reference carrying an upper-case letter is delimited and resolves
    byte-exact ONLY. That is what keeps `SELECT "G"` over a column `g` a
    miss — PostgreSQL's 42703 — rather than a silent read of `g`.
