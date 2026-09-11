# Insert column resolved spelling

Source: wadjet/dml.go — func resolveInsertColumns(named []string, table string, schema []parquet.Column) ([]string, []parquet.Column, error) {, moved 2026-09-11 (#1026)

resolveInsertColumns turns an INSERT's column list into the columns it
names, refusing one the table does not have.

It returns the STORED name for each position (so the row map is keyed the
way the schema is) beside the whole parquet.Column (so a DECIMAL literal is
judged against the declared (p, s) here rather than at the flush, which is
what names the row that carried it — #647).

The message and the class are PostgreSQL's, and the reference RESOLVES for
the same reason ResolveDMLSetClauses' does: INSERT was the one DML clause
that resolved case-SENSITIVELY, so `INSERT INTO t (ID)` failed on a table
whose column is `id` while `UPDATE t SET ID = …` succeeded.

Through batch.ResolveSchemaIndex, though, not through a map keyed by the
fold. This was the FIFTH site of the class ResolveDMLSetClauses, checkOnKeys
and checkDMLColumns were rewritten out of: `strings.ToLower(raw)` into a
fold-keyed map throws away the only evidence there is. The lexer has already
preserved the distinction here — `parseInsert` stores `colTok.val`, so an
unquoted name arrives FOLDED and a delimited one keeps its bytes — and
lowercasing both answers them the same. Measured over
`hits(WatchID, counterid, UserAgent)`:

	INSERT INTO hits ("WATCHID", counterid, "USERAGENT") VALUES (9, 9, 'x')
	  before: INSERT 1, the row STORED     after: 42703   (PostgreSQL: 42703)
	INSERT INTO hits ("WatchID", counterid, "UserAgent") VALUES (9, 9, 'x')
	  before: INSERT 1                     after: INSERT 1   (PostgreSQL: ok)

The write LANDED under a name PostgreSQL says does not exist — the same
disposition `SET "USERAGENT" = 'X'` had one door over. A FOLDED
`(watchid, counterid, useragent)` still resolves.
