package sqlerr

// AN EMPTY COLUMN LIST IS NEVER AN ANSWER.
//
// A statement that produces a result set produces COLUMNS: PostgreSQL sends a
// RowDescription with fields even when it returns no rows, and every client
// depends on it — psql prints a header, pgJDBC's executeQuery has metadata to
// read, a BI tool opens a table with it. A result carrying zero columns and no
// error is therefore not a small answer, it is a defect that reached the
// client wearing an answer's clothes, and it is INDISTINGUISHABLE from "the
// query legitimately found nothing".
//
// Two silent wrong answers arrived exactly that way and neither was noticed by
// any value comparison, because a comparison of two column lists that are both
// empty succeeds: two LATERALs whose inner block GROUPS answered `cols=[]
// rows=0` where PostgreSQL 17 answers eight rows (#1008), and nested LATERALs
// joined to a fourth relation did the same on the spilled arm where the other
// arms answer four (#1010). Both were rooted elsewhere and both are fixed;
// this refusal is what makes the CLASS unable to be silent again.
//
// XX000 — internal error — is the class, and it is deliberate: nothing about
// the STATEMENT is wrong. PostgreSQL answers these queries, so the code cannot
// blame the client (ADR-0012's rule for a wadjet-side bound), and unlike the
// 0A000 family this is not a feature the engine has declined to implement. It
// is the engine failing to describe its own output, which is what XX000 is
// for.
const EmptyResultSQLState = "XX000"

// EmptyResultColumns is the refusal a door makes when a statement that
// produces a result set produced one with no columns at all.
//
// door names the entry point ("embedded query", "coordinator query") the way
// every other refusal in this family does, so the message says which path
// failed to describe its output.
func EmptyResultColumns(door string) error {
	return New(EmptyResultSQLState,
		"%s: the result has no columns at all, which is never an answer — "+
			"a statement that produces a result set declares its columns or fails", door)
}
