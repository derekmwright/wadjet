package sql

import "github.com/derekmwright/wadjet/internal/sqlerr"

// StatementName is the SQL keyword phrase a QueryType stands for, in the
// spelling a client wrote. It is what a refusal names, so it has to be the
// statement and not the internal constant.
func (t QueryType) StatementName() string {
	switch t {
	case QuerySelect:
		return "SELECT"
	case QueryExplain:
		return "EXPLAIN"
	case QueryDescribe:
		return "DESCRIBE"
	case QueryCreateFunction:
		return "CREATE FUNCTION"
	case QueryDropFunction:
		return "DROP FUNCTION"
	case QueryShowFunctions:
		return "SHOW FUNCTIONS"
	case QueryCreateTable:
		return "CREATE TABLE"
	case QueryDropTable:
		return "DROP TABLE"
	case QueryAnalyzeTable:
		return "ANALYZE"
	case QueryShowTables:
		return "SHOW TABLES"
	case QueryUpdate:
		return "UPDATE"
	case QueryDelete:
		return "DELETE"
	case QueryInsert:
		return "INSERT"
	case QueryCreateView:
		return "CREATE VIEW"
	case QueryDropView:
		return "DROP VIEW"
	case QueryAlterTable:
		return "ALTER TABLE"
	case QueryMerge:
		return "MERGE"
	case QueryCreateAlert:
		return "CREATE ALERT"
	case QueryDropAlert:
		return "DROP ALERT"
	case QueryAlterAlert:
		return "ALTER ALERT"
	case QueryCreateSnapshot:
		return "CREATE SNAPSHOT"
	}
	return "this statement"
}

// String makes a QueryType printable as the statement it names, so a log line
// or a %v in an internal error reads as SQL rather than as an integer.
func (t QueryType) String() string { return t.StatementName() }

// RefuseUnsupportedStatement is the refusal a door owes a parsed statement no
// handler of that door accepts. It returns nil for a statement that IS a
// query, so a door calls it once, at the point it dispatches by statement
// type, immediately after its own handlers have had their turn.
//
// #860: `ALTER TABLE` — and `CREATE VIEW`, `DROP VIEW`, and on the HTTP door
// the alert and snapshot statements — parse into a ParsedQuery whose type no
// branch handles, so every door fell through to ExtractSelect and reported
// `no SELECT info in parsed query`: an internal invariant's wording, with no
// SQLSTATE on the HTTP door and the blanket 42000 through pgwire. A client
// cannot branch on that, and the message names nothing it wrote.
//
// The disposition is 0A000 (feature_not_supported), which is already this
// engine's class for a shape it parses and cannot execute (`RETURNING is not
// supported`, above), and the message names the STATEMENT. It is raised here
// rather than inside ExtractSelect because a better message from ExtractSelect
// would still be produced by planner code, one call site at a time, for a
// statement no planner should ever have been handed.
func RefuseUnsupportedStatement(pq *ParsedQuery) error {
	if pq == nil || pq.SelectInfo != nil {
		return nil
	}
	name := pq.Type.StatementName()
	if pq.Type == QueryExplain && pq.Explain != nil {
		// EXPLAIN keeps only the inner statement's SelectInfo, so a nil one
		// here means the inner statement is what cannot be run.
		name = "EXPLAIN " + pq.Explain.InnerType.StatementName()
	}
	return sqlerr.New("0A000", "%s is not supported", name)
}
