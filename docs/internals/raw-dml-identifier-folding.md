# Raw dml identifier folding

Source: wadjet/dml.go — func dmlIdent(s string) string {, moved 2026-09-11 (#1026)

dmlIdent is the lexer's identifier step, applied to a name that never went
through the lexer — pgwire's `copyIdent` for the same reason, at the other
hand-split site. An unquoted name FOLDS; a delimited one keeps its bytes and
loses only its quotes, `""` inside meaning one quote.

A MERGE's SET list and its NOT-MATCHED INSERT column list are raw SQL TEXT:
`scanMergeClauseUntil` returns `l.input[start:l.pos]`. So the target reached
`ev.targetColumn` with the double quotes still ATTACHED and they became part
of the name. Measured over `hits(WatchID, counterid, UserAgent)`:

	MERGE ... WHEN MATCHED THEN UPDATE SET "UserAgent" = 'X'
	  before: applying SET: column "\"UserAgent\"" of relation "hits"
	          does not exist
	  after:  MERGE 1, the value written
	MERGE ... WHEN NOT MATCHED THEN INSERT ("WATCHID", ...)
	  before: building INSERT row: column "\"WATCHID\"" of relation "hits"
	          does not exist
	  after:  42703, naming WATCHID

The first was a WRONG REFUSAL and the loud kind: PostgreSQL accepts
`SET "UserAgent"` for a column named `UserAgent`, and `UPDATE hits SET
"UserAgent" = 'X'` succeeds ONE DOOR OVER — the two statements disagreed
about the same assignment, which is the asymmetry ResolveDMLSetClauses'
rewrite exists to remove.

Doing this BEFORE `batch.ResolveSchemaIndex` rather than instead of it is
the load-bearing order. The resolver's rule keys on whether the reference is
itself folded, and a raw `SET UserAgent` is not — it would be read as a
DELIMITED name and refused against a schema column `UserAgent`… which it
happens to byte-match, but `SET USERAGENT` would not, and that form is
ordinary SQL. Fold first, then resolve.
