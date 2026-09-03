// Package sql provides SQL parsing using a custom recursive descent parser.
package sql

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// alertIntervalFloor is the minimum allowed interval for CREATE ALERT.
// Exposed as a var for integration tests that need sub-10s cadence.
var alertIntervalFloor = 10 * time.Second

// SetAlertIntervalFloorForTest lowers the CREATE ALERT interval floor for
// the duration of a test. Call the returned function (typically with defer)
// to restore the production floor.
func SetAlertIntervalFloorForTest(d time.Duration) func() {
	prev := alertIntervalFloor
	alertIntervalFloor = d
	return func() { alertIntervalFloor = prev }
}

// ParsedQuery represents a parsed SQL query.
type ParsedQuery struct {
	Type           QueryType
	TableName      string
	SQL            string
	Explain        *ExplainInfo
	Describe       *DescribeInfo
	CreateFunction *CreateFunctionInfo
	DropFunction   *DropFunctionInfo
	CreateTable    *CreateTableInfo
	DropTable      *DropTableInfo
	AnalyzeTable   *AnalyzeTableInfo
	CreateView     *CreateViewInfo
	DropView       *DropViewInfo
	AlterTable     *AlterTableInfo
	Merge          *MergeInfo
	Update         *UpdateInfo
	Delete         *DeleteInfo
	Insert         *InsertInfo
	CreateAlert    *CreateAlertInfo
	DropAlert      *DropAlertInfo
	AlterAlert     *AlterAlertInfo
	CreateSnapshot *CreateSnapshotInfo
	Windows        []WindowSpec // extracted window function specs
	CTEs           []CTEDef     // extracted CTE definitions
	SelectInfo     *SelectInfo  // parsed SELECT info (replaces AST)
}

// CreateViewInfo holds details for a CREATE VIEW statement.
type CreateViewInfo struct {
	Name    string
	SQL     string // the view definition SQL
	Replace bool   // CREATE OR REPLACE VIEW
}

// DropViewInfo holds details for a DROP VIEW statement.
type DropViewInfo struct {
	Name     string
	IfExists bool
}

// MergeInfo holds details for a MERGE statement.
type MergeInfo struct {
	Target          string // target table name
	TargetQualifier string // schema/catalog qualifier, "" when unqualified
	TargetAlias     string
	Source          string // source table/subquery
	SourceAlias     string
	OnCondition     string            // MERGE ON condition
	WhenClauses     []MergeWhenClause // WHEN MATCHED / NOT MATCHED clauses
}

// MergeWhenClause represents a WHEN clause in a MERGE statement.
type MergeWhenClause struct {
	Matched   bool   // true = WHEN MATCHED, false = WHEN NOT MATCHED
	Condition string // optional AND condition
	Action    string // "UPDATE", "DELETE", "INSERT"
	SQL       string // raw SET/VALUES clause
}

// AlterTableInfo holds details for an ALTER TABLE statement.
type AlterTableInfo struct {
	Table         string
	Action        string // "ADD COLUMN", "DROP COLUMN", "RENAME COLUMN"
	ColumnName    string
	NewColumnName string // for RENAME COLUMN
	ColumnType    string // for ADD COLUMN
	Nullable      bool   // for ADD COLUMN (default true)
}

// CTEDef represents a Common Table Expression definition.
type CTEDef struct {
	Name      string   // CTE name (lowercased for matching)
	SQL       string   // the CTE body SQL (the SELECT inside the parentheses)
	Columns   []string // optional column name list
	Recursive bool     // WITH RECURSIVE
}

// ExplainInfo holds details for an EXPLAIN statement.
type ExplainInfo struct {
	Verbose  bool
	Analyze  bool
	InnerSQL string
}

// DescribeInfo holds details for a DESCRIBE/SHOW COLUMNS statement.
type DescribeInfo struct {
	TableName string
}

// CreateFunctionInfo holds details for a CREATE FUNCTION statement.
type CreateFunctionInfo struct {
	Name    string
	Params  []string
	Body    string
	Replace bool // CREATE OR REPLACE
	Locked  bool // WITH LOCK
}

// DropFunctionInfo holds details for a DROP FUNCTION statement.
type DropFunctionInfo struct {
	Name     string
	IfExists bool
}

// CreateTableInfo holds details for a CREATE TABLE statement.
type CreateTableInfo struct {
	Name          string
	Columns       []ColumnDef
	PartitionKeys []string
}

// ColumnDef defines a column in a CREATE TABLE statement.
type ColumnDef struct {
	Name     string
	Type     string
	Nullable bool // true by default; NOT NULL sets it to false
}

// DropTableInfo holds details for a DROP TABLE statement.
type DropTableInfo struct {
	Name     string
	IfExists bool
}

// AnalyzeTableInfo holds details for an ANALYZE TABLE statement.
type AnalyzeTableInfo struct {
	Name string
}

// DMLTarget is the relation a DELETE or an UPDATE names, together with the
// statement text that named it.
//
// It is one type shared by both because both doors (the embedded API and the
// HTTP server) resolve the two identically, and because the STATEMENT TEXT
// has to travel with the clause for the empty-predicate backstop in
// wadjet.BuildDMLPredicate to have anything to check (#686).
type DMLTarget struct {
	Table     string // table name, as the statement spelled it
	Qualifier string // schema/catalog qualifier ("public"), "" when unqualified
	// Alias is the `[AS] a` the statement gave, "" when it gave none. When it
	// is set it HIDES the table name: PostgreSQL answers `DELETE FROM pr AS a
	// WHERE pr.id = 1` with 42P01, not with a delete.
	Alias string
	// WhereSQL is the raw WHERE clause. "" means the statement had NO WHERE
	// at all — which is a legal unconditional statement, and is why StmtSQL
	// is carried beside it: a clause the parser dropped looks identical here.
	WhereSQL string
	// StmtSQL is the whole statement, trimmed. The backstop re-lexes it to
	// ask whether a WHERE keyword the parsed clause does not account for was
	// written.
	StmtSQL string
}

// UpdateInfo holds details for an UPDATE statement.
type UpdateInfo struct {
	DMLTarget
	SetClauses []SetClause // SET column = value pairs
}

// SetClause represents a single SET column = value assignment.
type SetClause struct {
	Column string
	Value  string // raw expression text
}

// DeleteInfo holds details for a DELETE statement.
type DeleteInfo struct {
	DMLTarget
}

// InsertInfo holds details for an INSERT statement.
type InsertInfo struct {
	Table   string     // table name
	Columns []string   // target column names (empty = all columns)
	Values  [][]string // rows of value expressions
}

// CreateAlertInfo holds details for a CREATE ALERT statement.
type CreateAlertInfo struct {
	Name       string
	QueryText  string        // raw SELECT text, re-parsed at eval time
	Interval   time.Duration // validated >= 10s at parse time
	WebhookURL string        // "" if no webhook sink
	Headers    map[string]string
	InsertInto string // "" if no table sink; at least one sink required
}

// DropAlertInfo holds details for a DROP ALERT statement.
type DropAlertInfo struct {
	Name     string
	IfExists bool
}

// AlterAlertInfo holds details for ALTER ALERT ... ENABLE|DISABLE.
type AlterAlertInfo struct {
	Name   string
	Enable bool // true = ENABLE, false = DISABLE
}

// CreateSnapshotInfo is the AST for a CREATE SNAPSHOT statement.
// Empty in v1 — statement takes no arguments.
type CreateSnapshotInfo struct{}

// QueryType identifies the kind of SQL statement.
type QueryType int

const (
	QuerySelect QueryType = iota
	QueryExplain
	QueryDescribe
	QueryCreateFunction
	QueryDropFunction
	QueryShowFunctions
	QueryCreateTable
	QueryDropTable
	QueryAnalyzeTable
	QueryShowTables
	QueryUpdate
	QueryDelete
	QueryInsert
	QueryCreateView
	QueryDropView
	QueryAlterTable
	QueryMerge
	QueryCreateAlert
	QueryDropAlert
	QueryAlterAlert
	QueryCreateSnapshot
	QueryUnsupported
)

// Parse parses ONE SQL statement into a ParsedQuery.
//
// A string carrying SEVERAL statements is refused here, which is what makes
// this function the one-statement entry point every one-statement door needs:
// `wadjet.DB.Execute`, `wadjet.DB.Query`, the HTTP door and the CLI all reach
// it, and all of them return exactly one result. Only the pgwire SIMPLE query
// protocol runs a sequence, and it splits the string with SplitStatements
// before it gets here (#711). See CheckSingleStatement for what "refused"
// means and why the order matters.
func Parse(sql string) (*ParsedQuery, error) {
	stmts := SplitStatements(sql)
	if len(stmts) > 1 {
		return nil, multiStatementError(stmts)
	}
	// PARSE THE PIECE, NOT THE STRING. The splitter is what decides where the
	// statement ends, and handing parseDispatch the raw text instead put the
	// tail back: `DELETE … WHERE id = 1; -- audit note` parses as a DELETE
	// whose WHERE clause is the text `id = 1; -- audit note`, which
	// BuildDMLPredicate then refuses 42601 — the same right→loud regression
	// one layer down from the one the splitter fixed (#711 review B1).
	//
	// A string with NO statement in it keeps its own text, so a comment-only
	// or empty input reaches the dispatcher exactly as it did before.
	if len(stmts) == 1 {
		sql = stmts[0]
	}
	q, err := parseDispatch(sql)
	if err != nil {
		// A statement this parser cannot read is a syntax error from this
		// server's point of view, and 42601 is what a client branches on to
		// show "your SQL is broken" rather than a connection problem. The
		// original error chain is preserved (sqlerr.Wrap unwraps to it).
		//
		// An error that already carries its own SQLSTATE keeps it: StateOf
		// returns the OUTERMOST code, so wrapping unconditionally relabelled
		// a refusal that knew better. `DELETE ... RETURNING` is a legal
		// statement with an unimplemented feature (0A000), not broken SQL.
		if sqlerr.StateOf(err) != "" {
			return nil, err
		}
		return nil, sqlerr.Wrap("42601", err)
	}
	return q, nil
}

func parseDispatch(sql string) (*ParsedQuery, error) {
	trimmed := strings.TrimSpace(sql)
	// Strip trailing semicolons
	trimmed = strings.TrimRight(trimmed, ";")
	trimmed = strings.TrimSpace(trimmed)

	// Use the lexer to peek at the first token for dispatch.
	l := newLexer(trimmed)
	first := l.peekToken()

	switch first.typ {
	case TokenKWExplain:
		return lexParseExplain(trimmed, l)
	case TokenKWDescribe, TokenKWDesc:
		return lexParseDescribe(trimmed, l)
	case TokenKWShow:
		l.nextToken() // consume SHOW
		return lexParseShow(trimmed, l)
	case TokenKWCreate:
		l.nextToken() // consume CREATE
		return lexParseCreate(trimmed, l)
	case TokenKWDrop:
		l.nextToken() // consume DROP
		return lexParseDrop(trimmed, l)
	case TokenKWUpdate, TokenKWDelete, TokenKWInsert, TokenKWMerge:
		// RETURNING is checked for the whole statement, before any clause
		// parser can swallow it into the raw text it collects (#686 R2-4).
		if HasTopLevelReturning(trimmed) {
			return nil, sqlerr.New("0A000", "RETURNING is not supported")
		}
		switch first.typ {
		case TokenKWUpdate:
			return parseUpdate(trimmed, l)
		case TokenKWDelete:
			return parseDelete(trimmed, l)
		case TokenKWInsert:
			return parseInsert(trimmed, l)
		}
		return parseMerge(trimmed, l)
	case TokenKWAlter:
		l.nextToken() // consume ALTER
		return parseAlterTable(trimmed, l)
	case TokenKWAnalyze:
		l.nextToken() // consume ANALYZE
		return lexParseAnalyze(trimmed, l)
	}

	// Pre-parse CTEs — extract WITH ... AS (...) clauses
	var cteDefs []CTEDef
	if first.typ == TokenKWWith {
		l2 := newLexer(trimmed)
		var err error
		cteDefs, err = lexParseCTEs(l2)
		if err != nil {
			return nil, fmt.Errorf("parsing CTEs: %w", err)
		}
		trimmed = strings.TrimSpace(l2.rest())
	}

	// Parse using our recursive descent parser (window functions are now
	// parsed natively via maybeParseOver in the expression parser)
	sp := newSelectParser(trimmed)
	info, err := sp.parseSelectOrUnion()
	if err != nil {
		return nil, fmt.Errorf("parsing SQL: %w", err)
	}
	// The statement has to be consumed in full. Anything left over is input
	// this parser did not understand, and returning an answer computed from
	// the prefix would silently discard it (#337).
	if err := sp.expectEndOfStatement(); err != nil {
		return nil, fmt.Errorf("parsing SQL: %w", err)
	}

	// Resolve positional references (GROUP BY 1, ORDER BY 1 DESC)
	if err := resolvePositionalRefs(info); err != nil {
		return nil, fmt.Errorf("resolving positional refs: %w", err)
	}

	// Propagate CTE definitions
	info.CTEs = cteDefs

	// Collect window specs from parsed columns (populated by parseSelectColumn
	// when it encounters WindowFuncNode).
	var windowSpecs []WindowSpec
	for i := range info.Columns {
		if info.Columns[i].IsWindow && info.Columns[i].WindowSpec != nil {
			windowSpecs = append(windowSpecs, *info.Columns[i].WindowSpec)
		}
	}
	info.Windows = windowSpecs

	pq := &ParsedQuery{
		Type:       QuerySelect,
		SQL:        sql,
		Windows:    windowSpecs,
		CTEs:       cteDefs,
		SelectInfo: info,
	}

	return pq, nil
}

// ExtractSelect returns the SelectInfo from a parsed query.
func ExtractSelect(pq *ParsedQuery) (*SelectInfo, error) {
	if pq.SelectInfo != nil {
		return pq.SelectInfo, nil
	}
	return nil, fmt.Errorf("no SELECT info in parsed query")
}

// SetOp identifies the type of set operation.
type SetOp string

const (
	SetOpUnion     SetOp = "UNION"
	SetOpIntersect SetOp = "INTERSECT"
	SetOpExcept    SetOp = "EXCEPT"
)

// UnionInfo describes a set operation (UNION, INTERSECT, EXCEPT) with left and right sides.
type UnionInfo struct {
	Left  *SelectInfo
	Right *SelectInfo
	All   bool  // true for UNION ALL / INTERSECT ALL / EXCEPT ALL (no dedup)
	Op    SetOp // the set operation type (defaults to UNION for backwards compat)
}

// SelectInfo contains extracted information from a SELECT statement.
type SelectInfo struct {
	Tables       []TableRef
	Joins        []JoinInfo
	Columns      []SelectColumn
	Where        string
	WhereExpr    Node
	GroupBy      []string
	GroupByExprs []Node     // AST for GROUP BY expressions (parallel to GroupBy)
	GroupingSets [][]string // GROUPING SETS / CUBE / ROLLUP (nil = simple GROUP BY)
	// GroupByAliasOrigin records, per GROUP BY entry, the bare name the
	// parser SUBSTITUTED a SELECT alias's expression for — "" where it did
	// not. It exists because the substitution is PROVISIONAL: PostgreSQL
	// resolves a bare GROUP BY name against the INPUT COLUMNS FIRST and only
	// then against an output alias, and the parser has no schema, so it
	// cannot know which. The scope layer can (physical.colScope), and
	// RevertGroupByAliasesShadowedByInput undoes the entries the input
	// really provides.
	//
	// Recorded rather than decided-later-from-scratch so that an entry point
	// with no catalog — and therefore no scope — keeps exactly the answer it
	// had before the rule existed, instead of losing the substitution
	// entirely (#739).
	GroupByAliasOrigin []string
	Having             string
	HavingExpr         Node
	Distinct           bool
	Qualify            string
	QualifyExpr        Node
	OrderBy            []OrderByItem
	Limit              string
	Offset             string
	Windows            []WindowSpec // window function specs extracted during pre-parse
	CTEs               []CTEDef     // CTE definitions extracted during pre-parse
	Union              *UnionInfo   // non-nil if this is a UNION query
}

// TableRef is a reference to a table or table-producing function.
type TableRef struct {
	Name           string
	Qualifier      string // schema or catalog.schema written before the name
	Alias          string
	IsFunction     bool              // true for table functions like read_json(...)
	FuncArgs       []string          // positional arguments
	FuncNamedArgs  map[string]string // named arguments (key=value)
	WithOrdinality bool              // UNNEST(...) WITH ORDINALITY
	ColumnAliases  []string          // AS alias(col1, col2, ...)
	SampleMethod   string            // TABLESAMPLE method: BERNOULLI, SYSTEM
	SamplePercent  string            // percentage for TABLESAMPLE
}

// SelectColumn describes a column in a SELECT clause.
type SelectColumn struct {
	Expr        string
	Alias       string
	Star        bool
	IsAgg       bool
	AggFunc     string
	AggArg      string
	AggArgExpr  Node        // AST for aggregate argument expression
	AggArgs     []Node      // EVERY argument, AggArgExpr included (#353)
	AggDistinct bool        // COUNT(DISTINCT col)
	IsWindow    bool        // true if this is a window function
	WindowSpec  *WindowSpec // window function details
	ColumnRef   string
	TableRef    string
	ASTExpr     Node // our AST expression node
}

// JoinInfo describes a JOIN clause.
type JoinInfo struct {
	Type          string // join, left join, right join, full outer join, cross join
	LeftTable     string
	RightTable    string
	RightAlias    string
	RightTableRef *TableRef // full right-side table ref (includes function info)
	Condition     string
	CondExpr      Node
	// Using is the column list of a `JOIN ... USING (a, b)`, lower-cased, in
	// the order written. The join CONDITION is desugared into Condition /
	// CondExpr at parse time (`<left>.a = <right>.a AND ...`), because that
	// half needs no catalog; the list is kept because the OUTPUT half does —
	// USING merges the joined column into ONE output column under `SELECT *`,
	// and deciding which columns a star stands for is a catalog question
	// (#655).
	Using   []string
	Lateral bool // LATERAL join — right side can reference left side columns
	// FromItem is the index into SelectInfo.Tables of the comma-separated
	// FROM item this join EXTENDS. A FROM list is a list of items and an
	// explicit JOIN belongs to the item it follows, but the parser flattens
	// items into Tables and joins into Joins, which loses that association:
	// `FROM a JOIN b ON …, c` and `FROM a, b JOIN c ON …` produce two-entry
	// Tables and a one-entry Joins that are otherwise indistinguishable.
	// The builder needs it to attach each join to the right item — folding
	// the comma tables in first instead planned the former as `(a × c) ⋈ b`,
	// which buries a real cross product under the equi-join (#593) and
	// leaves the WHERE equality straddling that join's two sides, where the
	// key pair resolves to nothing and the query answers zero rows (#594).
	// Non-decreasing across Joins, since Tables only grows as parsing
	// advances. Zero for a hand-built JoinInfo, which attaches it to the
	// first item — the same tree the single-item case has always produced.
	FromItem int
}

// OrderByItem describes an ORDER BY element.
type OrderByItem struct {
	Column string
	// Expr is the parsed form of Column. A sort term that is not a plain
	// column reference — `year(d)`, `-id`, `a + b`, an ordinal — can only be
	// honoured by evaluating it, so the logical builder needs the tree, not
	// just its text (#320). Nil when the item was built without parsing.
	Expr       Node
	Desc       bool
	NullsFirst *bool // nil = default, true = NULLS FIRST, false = NULLS LAST
}

// --- Lexer-based pre-parse functions ---

// lexParseExplain handles: EXPLAIN [ANALYZE] [VERBOSE] <query>
func lexParseExplain(sql string, l *lexer) (*ParsedQuery, error) {
	l.nextToken() // consume EXPLAIN

	analyze := false
	if l.peekToken().typ == TokenKWAnalyze {
		l.nextToken() // consume ANALYZE
		analyze = true
	}

	verbose := false
	if l.peekToken().typ == TokenKWVerbose {
		l.nextToken() // consume VERBOSE
		verbose = true
	}

	// The rest of the input is the inner query
	rest := strings.TrimSpace(l.rest())
	inner, err := Parse(rest)
	if err != nil {
		return nil, fmt.Errorf("parsing EXPLAIN query: %w", err)
	}

	return &ParsedQuery{
		Type:       QueryExplain,
		SQL:        sql,
		SelectInfo: inner.SelectInfo,
		Explain: &ExplainInfo{
			Verbose:  verbose,
			Analyze:  analyze,
			InnerSQL: rest,
		},
	}, nil
}

// lexParseDescribe handles: DESCRIBE <table> / DESC <table>
func lexParseDescribe(sql string, l *lexer) (*ParsedQuery, error) {
	l.nextToken() // consume DESCRIBE/DESC

	tok := l.nextToken()
	if tok.typ != TokenIdent {
		return nil, fmt.Errorf("DESCRIBE requires a table name")
	}

	return &ParsedQuery{
		Type: QueryDescribe,
		SQL:  sql,
		Describe: &DescribeInfo{
			TableName: tok.val,
		},
	}, nil
}

// lexParseShow handles: SHOW FUNCTIONS / SHOW COLUMNS FROM <table> / SHOW TABLES
// SHOW has already been consumed.
func lexParseShow(sql string, l *lexer) (*ParsedQuery, error) {
	tok := l.nextToken()
	switch tok.typ {
	case TokenKWFunctions:
		return &ParsedQuery{
			Type: QueryShowFunctions,
			SQL:  sql,
		}, nil

	case TokenKWColumns:
		// Expect FROM <table>
		fromTok := l.nextToken()
		if fromTok.typ != TokenKWFrom {
			return nil, fmt.Errorf("expected FROM after SHOW COLUMNS")
		}
		nameTok := l.nextToken()
		if nameTok.typ != TokenIdent {
			return nil, fmt.Errorf("SHOW COLUMNS FROM requires a table name")
		}
		return &ParsedQuery{
			Type: QueryDescribe,
			SQL:  sql,
			Describe: &DescribeInfo{
				TableName: nameTok.val,
			},
		}, nil

	case TokenKWTables:
		return &ParsedQuery{
			Type: QueryShowTables,
			SQL:  sql,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported SHOW statement")
	}
}

// lexParseCreate handles: CREATE [OR REPLACE] FUNCTION ... | CREATE TABLE ...
// CREATE has already been consumed.
func lexParseCreate(sql string, l *lexer) (*ParsedQuery, error) {
	tok := l.nextToken()
	replace := false

	// Handle OR REPLACE
	if tok.typ == TokenKWOr {
		repTok := l.nextToken()
		if repTok.typ != TokenKWReplace {
			return nil, fmt.Errorf("expected REPLACE after OR")
		}
		replace = true
		tok = l.nextToken()
	}

	// Dispatch to TABLE, VIEW, or FUNCTION
	if tok.typ == TokenKWTable {
		return lexParseCreateTable(sql, l)
	}

	if tok.typ == TokenKWView {
		return lexParseCreateView(sql, l, replace)
	}

	if tok.typ == TokenKWAlert {
		return lexParseCreateAlert(sql, l)
	}

	if tok.typ == TokenKWSnapshot {
		return &ParsedQuery{
			Type:           QueryCreateSnapshot,
			SQL:            sql,
			CreateSnapshot: &CreateSnapshotInfo{},
		}, nil
	}

	if tok.typ != TokenKWFunction {
		return nil, fmt.Errorf("expected TABLE, VIEW, FUNCTION, ALERT, or SNAPSHOT after CREATE")
	}

	// Function name
	nameTok := l.nextToken()
	if nameTok.typ != TokenIdent {
		return nil, fmt.Errorf("CREATE FUNCTION: function name is required")
	}

	// Parameter list: ( param1, param2, ... )
	lparen := l.nextToken()
	if lparen.typ != TokenLParen {
		return nil, fmt.Errorf("CREATE FUNCTION: expected '(' after function name")
	}

	var params []string
	for {
		peek := l.peekToken()
		if peek.typ == TokenRParen {
			l.nextToken() // consume )
			break
		}
		if len(params) > 0 {
			comma := l.nextToken()
			if comma.typ != TokenComma {
				return nil, fmt.Errorf("CREATE FUNCTION: expected ',' between parameters")
			}
		}
		paramTok := l.nextToken()
		if paramTok.typ != TokenIdent {
			return nil, fmt.Errorf("CREATE FUNCTION: expected parameter name, got %q", paramTok.val)
		}
		params = append(params, paramTok.val)
	}

	// AS keyword
	asTok := l.nextToken()
	if asTok.typ != TokenKWAs {
		return nil, fmt.Errorf("CREATE FUNCTION: expected 'AS' after parameter list")
	}

	// Raw body capture — handles nested parens, string literals, WITH LOCK
	bodyTok := l.scanRawBody()
	if bodyTok.val == "" {
		return nil, fmt.Errorf("CREATE FUNCTION: function body is required")
	}

	// Check for WITH LOCK
	locked := false
	if l.peekToken().typ == TokenKWWith {
		l.nextToken() // consume WITH
		lockTok := l.nextToken()
		if lockTok.typ == TokenKWLock {
			locked = true
		} else {
			return nil, fmt.Errorf("CREATE FUNCTION: expected LOCK after WITH")
		}
	}

	return &ParsedQuery{
		Type: QueryCreateFunction,
		SQL:  sql,
		CreateFunction: &CreateFunctionInfo{
			Name:    nameTok.val,
			Params:  params,
			Body:    bodyTok.val,
			Replace: replace,
			Locked:  locked,
		},
	}, nil
}

// lexParseDrop handles: DROP FUNCTION [IF EXISTS] <name> | DROP TABLE [IF EXISTS] <name>
// DROP has already been consumed.
func lexParseDrop(sql string, l *lexer) (*ParsedQuery, error) {
	kindTok := l.nextToken()

	if kindTok.typ == TokenKWTable {
		return lexParseDropTable(sql, l)
	}

	if kindTok.typ == TokenKWView {
		return lexParseDropView(sql, l)
	}

	if kindTok.typ == TokenKWAlert {
		return lexParseDropAlert(sql, l)
	}

	if kindTok.typ != TokenKWFunction {
		return nil, fmt.Errorf("expected TABLE, VIEW, FUNCTION, or ALERT after DROP")
	}

	ifExists := false
	tok := l.nextToken()
	if tok.typ == TokenKWIf {
		existsTok := l.nextToken()
		if existsTok.typ != TokenKWExists {
			return nil, fmt.Errorf("expected EXISTS after IF")
		}
		ifExists = true
		tok = l.nextToken()
	}

	if tok.typ != TokenIdent {
		return nil, fmt.Errorf("DROP FUNCTION: function name is required")
	}

	return &ParsedQuery{
		Type: QueryDropFunction,
		SQL:  sql,
		DropFunction: &DropFunctionInfo{
			Name:     tok.val,
			IfExists: ifExists,
		},
	}, nil
}

// lexParseDropTable handles: DROP TABLE [IF EXISTS] <name>
// DROP TABLE has already been consumed except the table-specific part.
func lexParseDropTable(sql string, l *lexer) (*ParsedQuery, error) {
	ifExists := false
	tok := l.nextToken()
	if tok.typ == TokenKWIf {
		existsTok := l.nextToken()
		if existsTok.typ != TokenKWExists {
			return nil, fmt.Errorf("expected EXISTS after IF")
		}
		ifExists = true
		tok = l.nextToken()
	}

	if tok.typ != TokenIdent {
		return nil, fmt.Errorf("DROP TABLE: table name is required")
	}

	return &ParsedQuery{
		Type: QueryDropTable,
		SQL:  sql,
		DropTable: &DropTableInfo{
			Name:     tok.val,
			IfExists: ifExists,
		},
	}, nil
}

// lexParseAnalyze handles: ANALYZE [TABLE] <name>. The leading ANALYZE has
// already been consumed. The TABLE keyword is optional (ANALYZE foo == ANALYZE
// TABLE foo). EXPLAIN ANALYZE is a separate path (lexParseExplain) and is not
// affected — only a statement that STARTS with ANALYZE reaches here.
func lexParseAnalyze(sql string, l *lexer) (*ParsedQuery, error) {
	tok := l.nextToken()
	if tok.typ == TokenKWTable {
		tok = l.nextToken()
	}
	if tok.typ != TokenIdent {
		return nil, fmt.Errorf("ANALYZE: table name is required")
	}
	return &ParsedQuery{
		Type: QueryAnalyzeTable,
		SQL:  sql,
		AnalyzeTable: &AnalyzeTableInfo{
			Name: tok.val,
		},
	}, nil
}

// lexParseCreateTable handles: CREATE TABLE <name> (col1 TYPE [NOT NULL], ...) [PARTITION BY (col, ...)]
// CREATE TABLE has already been consumed.
// collectTypeParams consumes one balanced parenthesised type-parameter list
// and returns its text, parentheses included, in the compact spelling
// parquet.ResolveColumn reads: `(9,2)`, `(384)`, `(DECIMAL(9,2))`,
// `(a INT64, d DECIMAL(9,2))`.
//
// It does not judge what is inside. Which parameters a type takes, and what
// they mean, is parquet's grammar (ParseDecimalParams, parseVectorDim,
// parseRowFields), and duplicating a second opinion here is what let the
// DECIMAL parameters be read at one door and dropped at two others (#647).
// The lexer already tokenised the text, so the only structure this needs is
// the matching ')'.
func collectTypeParams(l *lexer) (string, error) {
	var b strings.Builder
	depth := 0
	prev := TokenError
	for {
		tok := l.nextToken()
		switch tok.typ {
		case TokenEOF, TokenError:
			return "", fmt.Errorf("unterminated type parameters")
		case TokenLParen:
			depth++
		case TokenRParen:
			depth--
		}
		// A space separates two WORDS and nothing else, so `DECIMAL(9,2)`
		// comes out byte-identical to what the old two-number reader
		// produced — nothing that reads a ColumnDef.Type sees a change —
		// while `ROW(a INT64,d DECIMAL(9,2))` keeps each field name apart
		// from its type. Whitespace around a comma is not significant to any
		// of parquet's parameter readers; all of them trim.
		if b.Len() > 0 && tok.typ != TokenRParen && tok.typ != TokenComma &&
			tok.typ != TokenLParen && prev != TokenLParen && prev != TokenComma {
			b.WriteByte(' ')
		}
		// source(), not val: a type name inside the parameters is not an
		// identifier reference either (#731).
		b.WriteString(tok.source())
		prev = tok.typ
		if depth == 0 {
			return b.String(), nil
		}
	}
}

func lexParseCreateTable(sql string, l *lexer) (*ParsedQuery, error) {
	// Table name
	nameTok := l.nextToken()
	if nameTok.typ != TokenIdent {
		return nil, fmt.Errorf("CREATE TABLE: table name is required")
	}

	// Opening paren for column definitions
	lparen := l.nextToken()
	if lparen.typ != TokenLParen {
		return nil, fmt.Errorf("CREATE TABLE: expected '(' after table name")
	}

	// Parse column definitions
	var columns []ColumnDef
	for {
		peek := l.peekToken()
		if peek.typ == TokenRParen {
			l.nextToken() // consume )
			break
		}
		if len(columns) > 0 {
			comma := l.nextToken()
			if comma.typ == TokenRParen {
				break
			}
			if comma.typ != TokenComma {
				return nil, fmt.Errorf("CREATE TABLE: expected ',' or ')' between column definitions")
			}
			// Check for trailing comma before )
			if l.peekToken().typ == TokenRParen {
				l.nextToken()
				break
			}
		}

		// Column name
		colNameTok := l.nextToken()
		if colNameTok.typ != TokenIdent {
			return nil, fmt.Errorf("CREATE TABLE: expected column name, got %q", colNameTok.val)
		}

		// Column type. ROW is a keyword elsewhere in the grammar (window
		// frames), so it arrives as TokenKWRow rather than TokenIdent and was
		// rejected outright as a column type.
		colTypeTok := l.nextToken()
		if colTypeTok.typ != TokenIdent && colTypeTok.typ != TokenKWRow {
			return nil, fmt.Errorf("CREATE TABLE: expected type for column %q, got %q", colNameTok.val, colTypeTok.val)
		}

		// The type's SOURCE spelling: a type name is not an identifier
		// reference, so the lexer's unquoted-identifier fold (#731) must not
		// rewrite `BIGINT` to `bigint` in what SHOW COLUMNS echoes back.
		typeName := colTypeTok.source()
		// Optional type parameters: DECIMAL(10,2), VECTOR(384),
		// ARRAY(DECIMAL(9,2)), ROW(a INT64, d DECIMAL(9,2)),
		// MAP(STRING, INT64).
		//
		// This used to accept ONE number and optionally a second, so DECIMAL
		// and VECTOR were the only parameterized types a SQL declaration could
		// spell at all — ARRAY, ROW and MAP were syntax errors here, which is
		// why the schema-side defect that dropped their parameters (#675) was
		// invisible from this door. parquet.ResolveColumn owns the type
		// grammar; this collects the balanced text and hands it over, so the
		// accept-set is decided in one place rather than two.
		if l.peekToken().typ == TokenLParen {
			params, err := collectTypeParams(l)
			if err != nil {
				return nil, fmt.Errorf("CREATE TABLE: column %q type %s: %w", colNameTok.val, typeName, err)
			}
			typeName += params
		}

		col := ColumnDef{
			Name:     colNameTok.val,
			Type:     typeName,
			Nullable: true,
		}

		// Check for NOT NULL
		if l.peekToken().typ == TokenKWNot {
			l.nextToken() // consume NOT
			nullTok := l.nextToken()
			if nullTok.typ != TokenKWNull {
				return nil, fmt.Errorf("CREATE TABLE: expected NULL after NOT")
			}
			col.Nullable = false
		}

		columns = append(columns, col)
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("CREATE TABLE: at least one column is required")
	}

	// Check for PARTITION BY (col, ...)
	var partitionKeys []string
	if l.peekToken().typ == TokenKWPartition {
		l.nextToken() // consume PARTITION
		byTok := l.nextToken()
		if byTok.typ != TokenKWBy {
			return nil, fmt.Errorf("CREATE TABLE: expected BY after PARTITION")
		}
		lparen := l.nextToken()
		if lparen.typ != TokenLParen {
			return nil, fmt.Errorf("CREATE TABLE: expected '(' after PARTITION BY")
		}
		for {
			peek := l.peekToken()
			if peek.typ == TokenRParen {
				l.nextToken()
				break
			}
			if len(partitionKeys) > 0 {
				comma := l.nextToken()
				if comma.typ == TokenRParen {
					break
				}
				if comma.typ != TokenComma {
					return nil, fmt.Errorf("CREATE TABLE: expected ',' or ')' in PARTITION BY")
				}
			}
			keyTok := l.nextToken()
			if keyTok.typ != TokenIdent {
				return nil, fmt.Errorf("CREATE TABLE: expected partition key name, got %q", keyTok.val)
			}
			partitionKeys = append(partitionKeys, keyTok.val)
		}
	}

	return &ParsedQuery{
		Type: QueryCreateTable,
		SQL:  sql,
		CreateTable: &CreateTableInfo{
			Name:          nameTok.val,
			Columns:       columns,
			PartitionKeys: partitionKeys,
		},
	}, nil
}

// resolvePositionalRefs replaces numeric positional references in GROUP BY
// and ORDER BY with the corresponding SELECT column expression (1-indexed),
// and resolves GROUP BY references to SELECT aliases of computed expressions.
// UNION queries are skipped since positions would be ambiguous.
func resolvePositionalRefs(info *SelectInfo) error {
	if info.Union != nil {
		return resolveSetOpOrderBy(info)
	}

	// Resolve GROUP BY positional refs
	for i, gb := range info.GroupBy {
		pos, err := strconv.Atoi(strings.TrimSpace(gb))
		if err != nil {
			// Not a number. A bare identifier naming a SELECT alias whose
			// expression is computed (CASE, function call, arithmetic —
			// anything but a plain column ref) must group by that
			// expression: previously only the alias STRING was kept, so
			// the aggregate grouped by a nonexistent column (one giant
			// group) and the projection above re-evaluated the expression
			// over the aggregate output, where its source columns no
			// longer exist — every row got the ELSE/NULL value.
			resolveGroupByAliasRef(info, i, gb)
			continue
		}
		if pos < 1 || pos > len(info.Columns) {
			return fmt.Errorf("GROUP BY position %d is out of range (1-%d)", pos, len(info.Columns))
		}
		col := info.Columns[pos-1]
		if col.Alias != "" {
			info.GroupBy[i] = col.Alias
		} else {
			info.GroupBy[i] = col.Expr
		}
		substituteGroupByExpr(info, i, col)
	}

	// Resolve ORDER BY positional refs
	//
	// firstStar is where counting stops being possible. A `*` stands for
	// however many columns its source has, which is a catalog question this
	// layer cannot ask, so a position at or after one is left as written for
	// logical.ResolveOrdinalSortKeys to answer after star expansion (#810).
	// Before that, `SELECT * FROM t ORDER BY 1` counted the star as ONE item,
	// rewrote the term to the literal text `*`, and the query was refused on
	// every arm — while PostgreSQL answers it, and it is what every
	// "preview this table" button emits.
	firstStar := -1
	for i, c := range info.Columns {
		if c.Star {
			firstStar = i
			break
		}
	}
	for i, ob := range info.OrderBy {
		pos, err := strconv.Atoi(strings.TrimSpace(ob.Column))
		if err != nil {
			continue // not a number, leave as-is
		}
		if pos < 1 {
			return fmt.Errorf("ORDER BY position %d is out of range (1-%d)", pos, len(info.Columns))
		}
		if firstStar >= 0 && pos > firstStar {
			continue // not countable here; resolved after the star expands
		}
		if pos > len(info.Columns) {
			return fmt.Errorf("ORDER BY position %d is out of range (1-%d)", pos, len(info.Columns))
		}
		col := info.Columns[pos-1]
		if col.IsWindow {
			// The name the projection really publishes. Naming it from the
			// expression TEXT rewrote `ORDER BY 1` to `sum(a) OVER (...)`,
			// which no sort key can resolve.
			info.OrderBy[i].Column = WindowOutputName(col)
		} else if col.Alias != "" {
			// A REFERENCE to the alias, not its text. Every consumer of a
			// sort term re-parses Column, and an alias is an arbitrary
			// string: `SELECT id AS "G + 1" … ORDER BY 1` came back as the
			// four tokens `G + 1` and was read as arithmetic over a column
			// `G` that does not exist, and `"Kk"` / `"A B"` reached the DAG's
			// sort as names no stage emits. QuoteIdent renders whatever
			// re-parses to this one identifier, and Expr carries the tree so
			// nothing has to re-parse at all.
			info.OrderBy[i].Column = QuoteIdent(col.Alias)
			info.OrderBy[i].Expr = &ColRef{Column: col.Alias}
		} else {
			info.OrderBy[i].Column = col.Expr
			if col.ASTExpr != nil {
				info.OrderBy[i].Expr = col.ASTExpr
			}
		}
	}

	return nil
}

// resolveSetOpOrderBy resolves `ORDER BY <n>` on a set operation.
//
// A UNION / INTERSECT / EXCEPT has no SELECT list of its own — its output
// columns are named by its leftmost arm, which is what an ordinal sort term
// over a set operation refers to in PostgreSQL and DuckDB alike. Positional
// refs used to be skipped outright for these queries, so the sort key stayed
// the literal "1", matched no column, and the rows came back in arrival order
// with no error (#337).
func resolveSetOpOrderBy(info *SelectInfo) error {
	cols := info
	for cols.Union != nil {
		cols = cols.Union.Left
	}
	for i, ob := range info.OrderBy {
		pos, err := strconv.Atoi(strings.TrimSpace(ob.Column))
		if err != nil {
			continue // not an ordinal, leave as written
		}
		if pos < 1 || pos > len(cols.Columns) {
			return fmt.Errorf("ORDER BY position %d is out of range (1-%d)", pos, len(cols.Columns))
		}
		col := cols.Columns[pos-1]
		if col.IsWindow {
			info.OrderBy[i].Column = WindowOutputName(col)
		} else if col.Alias != "" {
			info.OrderBy[i].Column = col.Alias
		} else {
			info.OrderBy[i].Column = col.Expr
		}
	}
	return nil
}

// resolveGroupByAliasRef resolves GROUP BY <alias> where <alias> names a
// SELECT column, PROVISIONALLY.
//
// It substitutes unconditionally, because it runs in the parser and the
// parser has no schema: it cannot ask whether an input column already owns
// the name. Its own comment used to claim that "a table column with the same
// name keeps precedence over the alias", which was the intended rule written
// down as though it were the present one — there was no such check anywhere.
// The precedence is applied by RevertGroupByAliasesShadowedByInput, at the
// layer that knows the FROM sources; the name substituted is recorded in
// SelectInfo.GroupByAliasOrigin so that undoing it needs no re-derivation
// (#739).
func resolveGroupByAliasRef(info *SelectInfo, i int, gb string) {
	// Asked of the parsed term, not of its spelling. A bare name is a
	// *ColRef with no qualifier however it was written, so a DELIMITED one —
	// `GROUP BY "g + 1"` naming `SELECT c_str AS "g + 1"` — resolves to its
	// select item like any other, where the old string test ("contains a
	// space, so it must be an expression") refused it and the aggregate then
	// looked for a column no input carries (#725).
	var name string
	if i < len(info.GroupByExprs) && info.GroupByExprs[i] != nil {
		ref, ok := Unparen(info.GroupByExprs[i]).(*ColRef)
		if !ok || ref.Table != "" {
			return // qualified or expression-shaped — not a bare alias
		}
		name = ref.Column
	} else {
		name = strings.TrimSpace(gb)
		if strings.ContainsAny(name, ". ()") {
			return
		}
	}
	for _, col := range info.Columns {
		if col.Alias != "" && strings.EqualFold(col.Alias, name) && !col.IsAgg && !col.IsWindow {
			substituteGroupByExpr(info, i, col)
			recordGroupByAliasOrigin(info, i, name)
			return
		}
	}
}

// recordGroupByAliasOrigin notes that GROUP BY entry i was a bare name the
// parser replaced with a SELECT alias's expression, so the scope layer can
// undo it where an INPUT COLUMN of that name exists — PostgreSQL's precedence
// (#739). See SelectInfo.GroupByAliasOrigin.
func recordGroupByAliasOrigin(info *SelectInfo, i int, name string) {
	for len(info.GroupByAliasOrigin) < len(info.GroupBy) {
		info.GroupByAliasOrigin = append(info.GroupByAliasOrigin, "")
	}
	if i < len(info.GroupByAliasOrigin) {
		info.GroupByAliasOrigin[i] = name
	}
}

// RevertGroupByAliasesShadowedByInput applies PostgreSQL's precedence for a
// bare GROUP BY name: an INPUT COLUMN wins over a SELECT alias.
//
// The parser substitutes such a name with the alias's defining expression
// unconditionally, and its own doc comment claimed the opposite ("a table
// column with the same name keeps precedence over the alias") — protocol item
// 9's exact failure mode, a record describing intended behaviour as present
// behaviour. There is no precedence check in the parser and there cannot be
// one: it has no schema and no scope. So the substitution is provisional and
// this undoes it, called from the layer that knows what the FROM sources
// provide.
//
// provides reports whether one of this block's own sources carries the bare
// name. It must answer only where it is CERTAIN: an unenumerable source (a
// table function, a SELECT *, a table absent from the catalog) has to answer
// false, which keeps the substitution and the pre-#739 answer.
//
// The wrong-answer shape this closes: `SELECT h AS g, COUNT(*) FROM gcov
// GROUP BY g, h` grouped by (h, h) and answered 2 rows where PostgreSQL — which
// groups by (g, h) — answers 6. Both engines answered, and they answered
// different numbers.
func RevertGroupByAliasesShadowedByInput(info *SelectInfo, provides func(bare string) bool) {
	if info == nil || provides == nil || len(info.GroupByAliasOrigin) == 0 {
		return
	}
	for i, origin := range info.GroupByAliasOrigin {
		if origin == "" || i >= len(info.GroupBy) {
			continue
		}
		if !provides(origin) {
			continue
		}
		ref := &ColRef{Column: origin}
		if i < len(info.GroupByExprs) {
			info.GroupByExprs[i] = ref
		}
		info.GroupBy[i] = GroupKeyName(ref)
		info.GroupByAliasOrigin[i] = ""
	}
}

// substituteGroupByExpr replaces GROUP BY entry i with the select column's
// underlying expression, so downstream column pruning, the aggregate's
// synthetic pre-projection, and the projection's group-expression matching
// all see the real expression. A plain renamed column (URL AS Dst) resolves
// to its source column name — the alias is not a column of the input, so
// grouping by it found nothing and collapsed to a single NULL-keyed group.
func substituteGroupByExpr(info *SelectInfo, i int, col SelectColumn) {
	if col.ASTExpr == nil || i >= len(info.GroupByExprs) {
		return
	}
	// Canonical, for the same reason the GROUP BY clause itself is: this is
	// the name every consumer above the aggregate resolves against, and
	// `SELECT (g + 1) AS k … GROUP BY k` must reach the same one as
	// `GROUP BY g + 1` (#723).
	info.GroupByExprs[i] = Unparen(col.ASTExpr)
	info.GroupBy[i] = GroupKeyName(info.GroupByExprs[i])
}

// lexParseCTEs parses CTE definitions from a lexer.
// The caller has verified the first token is WITH.
func lexParseCTEs(l *lexer) ([]CTEDef, error) {
	// Consume WITH
	l.nextToken()

	// Check for RECURSIVE
	recursive := false
	if l.peekToken().typ == TokenIdent && strings.EqualFold(l.peekToken().val, "RECURSIVE") {
		l.nextToken() // consume RECURSIVE
		recursive = true
	}

	var defs []CTEDef
	for {
		// Read CTE name
		nameTok := l.nextToken()
		if nameTok.typ != TokenIdent {
			return nil, fmt.Errorf("expected CTE name, got %q", nameTok.val)
		}
		cteName := nameTok.val

		// Check for optional column list: name(col1, col2, ...) AS (...)
		// vs name AS (SELECT ...)
		var columns []string
		peek := l.peekToken()
		if peek.typ == TokenLParen {
			// Save position to backtrack if this is AS (SELECT ...)
			savedPos := l.pos
			savedStart := l.start
			savedWidth := l.width

			l.nextToken() // consume (
			// Check if this is the start of the CTE body (SELECT keyword) or a column list
			inner := l.peekToken()
			if inner.typ == TokenKWSelect || inner.typ == TokenKWWith {
				// It's the CTE body, not a column list. Restore position.
				l.pos = savedPos
				l.start = savedStart
				l.width = savedWidth
			} else {
				// Parse column list
				for {
					colTok := l.nextToken()
					if colTok.typ != TokenIdent {
						return nil, fmt.Errorf("expected column name in CTE %q column list, got %q", cteName, colTok.val)
					}
					columns = append(columns, colTok.val)
					next := l.nextToken()
					if next.typ == TokenRParen {
						break
					}
					if next.typ != TokenComma {
						return nil, fmt.Errorf("expected ',' or ')' in CTE %q column list, got %q", cteName, next.val)
					}
				}
			}
		}

		// Expect AS
		asTok := l.nextToken()
		if asTok.typ != TokenKWAs {
			return nil, fmt.Errorf("expected AS after CTE name %q, got %q", cteName, asTok.val)
		}

		// Expect opening paren
		lparenTok := l.nextToken()
		if lparenTok.typ != TokenLParen {
			return nil, fmt.Errorf("expected '(' after AS in CTE %q", cteName)
		}

		// Capture body using balanced paren counting
		bodyStart := l.pos
		depth := 1
		for l.pos < len(l.input) && depth > 0 {
			ch := l.input[l.pos]
			if ch == '\'' {
				// Skip string literal
				l.pos++
				for l.pos < len(l.input) {
					if l.input[l.pos] == '\'' {
						l.pos++
						if l.pos < len(l.input) && l.input[l.pos] == '\'' {
							l.pos++ // escaped quote
							continue
						}
						break
					}
					l.pos++
				}
				continue
			}
			if ch == '(' {
				depth++
			} else if ch == ')' {
				depth--
			}
			if depth > 0 {
				l.pos++
			}
		}
		if depth != 0 {
			return nil, fmt.Errorf("unmatched parenthesis in CTE %q", cteName)
		}
		body := strings.TrimSpace(l.input[bodyStart:l.pos])
		l.pos++ // skip closing ')'
		l.start = l.pos

		defs = append(defs, CTEDef{
			Name:      cteName,
			SQL:       body,
			Columns:   columns,
			Recursive: recursive,
		})

		// Check for comma (more CTEs) or end
		peek = l.peekToken()
		if peek.typ == TokenComma {
			l.nextToken() // consume comma
			continue
		}
		break
	}

	return defs, nil
}

// parseAlterTable handles: ALTER TABLE name ADD/DROP/RENAME COLUMN ...
// ALTER has already been consumed.
func parseAlterTable(sql string, l *lexer) (*ParsedQuery, error) {
	// Peek at kind: TABLE or ALERT
	kindTok := l.peekToken()
	if kindTok.typ == TokenKWAlert {
		l.nextToken() // consume ALERT
		return lexParseAlterAlert(sql, l)
	}

	tableTok := l.nextToken()
	if tableTok.typ != TokenKWTable {
		return nil, fmt.Errorf("expected TABLE after ALTER")
	}

	nameTok := l.nextToken()
	if nameTok.typ != TokenIdent {
		return nil, fmt.Errorf("ALTER TABLE: table name is required")
	}

	info := &AlterTableInfo{
		Table:    nameTok.val,
		Nullable: true,
	}

	actionTok := l.nextToken()
	switch actionTok.typ {
	case TokenKWAdd:
		// ADD [COLUMN] name TYPE [NOT NULL]
		info.Action = "ADD COLUMN"
		tok := l.nextToken()
		if tok.typ == TokenKWColumn {
			tok = l.nextToken() // skip optional COLUMN keyword
		}
		if tok.typ != TokenIdent {
			return nil, fmt.Errorf("ALTER TABLE ADD: column name is required")
		}
		info.ColumnName = tok.val

		typeTok := l.nextToken()
		if typeTok.typ != TokenIdent {
			return nil, fmt.Errorf("ALTER TABLE ADD: column type is required")
		}
		info.ColumnType = strings.ToLower(typeTok.val)

		// Optional type precision
		if l.peekToken().typ == TokenLParen {
			l.nextToken() // (
			prec := l.nextToken()
			info.ColumnType += "(" + prec.val
			if l.peekToken().typ == TokenComma {
				l.nextToken()
				scale := l.nextToken()
				info.ColumnType += "," + scale.val
			}
			l.nextToken() // )
			info.ColumnType += ")"
		}

		// Optional NOT NULL
		if l.peekToken().typ == TokenKWNot {
			l.nextToken()
			l.nextToken() // NULL
			info.Nullable = false
		}

	case TokenKWDrop:
		// DROP [COLUMN] name
		info.Action = "DROP COLUMN"
		tok := l.nextToken()
		if tok.typ == TokenKWColumn {
			tok = l.nextToken()
		}
		if tok.typ != TokenIdent {
			return nil, fmt.Errorf("ALTER TABLE DROP: column name is required")
		}
		info.ColumnName = tok.val

	case TokenKWRename:
		// RENAME [COLUMN] old TO new
		info.Action = "RENAME COLUMN"
		tok := l.nextToken()
		if tok.typ == TokenKWColumn {
			tok = l.nextToken()
		}
		if tok.typ != TokenIdent {
			return nil, fmt.Errorf("ALTER TABLE RENAME: column name is required")
		}
		info.ColumnName = tok.val

		toTok := l.nextToken()
		if toTok.typ != TokenKWTo {
			return nil, fmt.Errorf("ALTER TABLE RENAME: expected TO after column name")
		}

		newTok := l.nextToken()
		if newTok.typ != TokenIdent {
			return nil, fmt.Errorf("ALTER TABLE RENAME: new column name is required")
		}
		info.NewColumnName = newTok.val

	default:
		return nil, fmt.Errorf("ALTER TABLE: expected ADD, DROP, or RENAME, got %q", actionTok.val)
	}

	return &ParsedQuery{
		Type:       QueryAlterTable,
		SQL:        sql,
		AlterTable: info,
	}, nil
}

// parseMerge handles: MERGE INTO target USING source ON condition WHEN ...
func parseMerge(sql string, l *lexer) (*ParsedQuery, error) {
	l.nextToken() // consume MERGE

	// INTO (optional)
	if l.peekToken().typ == TokenKWInto {
		l.nextToken()
	}

	// Target table, optionally schema-qualified. Reading only the first
	// identifier made `MERGE INTO public.pr ...` a merge into a table named
	// "public" and then failed on the DOT with "MERGE: expected USING", which
	// named the wrong token entirely (#686 review).
	targetTok := l.nextToken()
	if targetTok.typ != TokenIdent {
		return nil, fmt.Errorf("MERGE: expected target table name")
	}
	targetName := targetTok.val
	targetQualifier := ""
	for l.peekToken().typ == TokenDot {
		l.nextToken() // consume .
		partTok := l.nextToken()
		if partTok.typ != TokenIdent {
			return nil, fmt.Errorf("MERGE: expected name after %q. in the target, got %q", targetName, partTok.val)
		}
		if targetQualifier == "" {
			targetQualifier = targetName
		} else {
			targetQualifier += "." + targetName
		}
		targetName = partTok.val
	}
	info := &MergeInfo{Target: targetName, TargetQualifier: targetQualifier}

	// Optional alias. AS must be followed by a NAME: taking whatever came
	// next made `MERGE INTO t AS USING s ...` an alias of "USING" and then
	// failed on the missing USING, which named the wrong token (#686 sweep).
	peek := l.peekToken()
	if peek.typ == TokenKWAs {
		l.nextToken()
		aliasTok := l.nextToken()
		if aliasTok.typ != TokenIdent {
			return nil, fmt.Errorf("MERGE: expected target alias after AS, got %q", aliasTok.val)
		}
		info.TargetAlias = aliasTok.val
	} else if peek.typ == TokenIdent && !isClauseKeyword(peek) {
		l.nextToken()
		info.TargetAlias = peek.val
	}

	// USING
	if l.peekToken().typ != TokenKWUsing {
		return nil, fmt.Errorf("MERGE: expected USING")
	}
	l.nextToken()

	// Source table or subquery
	sourceTok := l.nextToken()
	if sourceTok.typ == TokenLParen {
		// Subquery — collect balanced parens THROUGH THE LEXER.
		//
		// It used to be a character loop, which cannot tell a parenthesis
		// from one inside a string literal: `USING (SELECT id, ')' AS c FROM
		// src) s` closed the subquery at the quoted `)` and failed with
		// "expected ON", and the `'('` spelling failed as an unterminated
		// subquery. PostgreSQL runs both. It is the same "a scan that does
		// not know its own nesting" line #722 set out to close, and the one
		// MERGE scan that fix left on the old pattern (review P16).
		depth := 1
		start := l.pos
		end := l.pos
		for depth > 0 {
			tok := l.nextToken()
			switch tok.typ {
			case TokenLParen:
				depth++
			case TokenRParen:
				depth--
			case TokenEOF:
				return nil, fmt.Errorf("MERGE: unterminated subquery in USING")
			}
			if depth > 0 {
				end = l.pos
			}
		}
		info.Source = "(" + strings.TrimSpace(l.input[start:end]) + ")"
		l.start = l.pos
	} else if sourceTok.typ == TokenIdent {
		info.Source = sourceTok.val
	} else {
		return nil, fmt.Errorf("MERGE: expected source table or subquery after USING")
	}

	// Optional source alias
	peek = l.peekToken()
	if peek.typ == TokenKWAs {
		l.nextToken()
		aliasTok := l.nextToken()
		if aliasTok.typ != TokenIdent {
			return nil, fmt.Errorf("MERGE: expected source alias after AS, got %q", aliasTok.val)
		}
		info.SourceAlias = aliasTok.val
	} else if peek.typ == TokenIdent && !isClauseKeyword(peek) {
		l.nextToken()
		info.SourceAlias = peek.val
	}

	// ON condition
	if l.peekToken().typ != TokenKWOn {
		return nil, fmt.Errorf("MERGE: expected ON")
	}
	l.nextToken()

	// Read condition until the clause's own WHEN — not a CASE expression's.
	info.OnCondition = scanMergeClauseUntil(l, TokenKWWhen)

	// Parse WHEN clauses
	for l.peekToken().typ == TokenKWWhen {
		l.nextToken() // consume WHEN

		clause := MergeWhenClause{}

		// MATCHED or NOT MATCHED
		if l.peekToken().typ == TokenKWNot {
			l.nextToken()
			clause.Matched = false
		} else {
			clause.Matched = true
		}
		if l.peekToken().typ != TokenKWMatched {
			return nil, fmt.Errorf("MERGE: expected MATCHED after WHEN [NOT]")
		}
		l.nextToken()

		// PostgreSQL 17's WHEN NOT MATCHED BY SOURCE / BY TARGET. Both are
		// real clause kinds with different meanings (BY SOURCE walks the
		// TARGET rows no source row matched), so reading past the BY and
		// treating the clause as an ordinary NOT MATCHED would act on the
		// wrong rows. It is an unimplemented FEATURE, not bad SQL, so it is
		// 0A000 and it refuses (#686 R2-3, wadjet#718).
		//
		// DEFERRED — #718. Re-examined by arc D3 against PostgreSQL 17.11 and
		// deferred again: the MERGE builder does not make it cheap, because
		// its clause SCOPE is a boolean by construction and BY SOURCE needs a
		// third value. Three changes, and the third is the one that is not
		// local:
		//
		//  1. The parser needs a CLAUSE-KIND field. MergeWhenClause carries
		//     only `Matched bool`, which cannot express the difference
		//     between NOT MATCHED, NOT MATCHED BY SOURCE and NOT MATCHED BY
		//     TARGET. BY TARGET is a synonym for plain NOT MATCHED and maps
		//     onto the existing branch.
		//
		//  2. The executor needs a SECOND set beside matchedTargetIndices.
		//     That one records the targets a clause FIRED on, and BY SOURCE's
		//     complement is "no source row matched this target AT ALL",
		//     fired or not. PostgreSQL confirms the distinction is real:
		//     `WHEN MATCHED AND t.n > 99 THEN DELETE WHEN NOT MATCHED BY
		//     SOURCE THEN UPDATE SET n = 0` leaves the matched row ALONE even
		//     though its MATCHED clause did not fire (measured, MERGE 2).
		//
		//  3. A BY SOURCE clause's scope is TARGET-ONLY, and that is a THIRD
		//     scope. `wadjet.mergeEvaluator` threads a `matched bool` through
		//     resolveRefIn, checkClauseColumns, checkConditionType, condition
		//     and value, where false means SOURCE-only and true means the
		//     merged namespace. Under BY SOURCE, `UPDATE SET n = s.n` is
		//     42P01 "invalid reference to FROM-clause entry for table s" and
		//     a BARE `n` resolves to the TARGET without ambiguity (both
		//     measured). Every one of those signatures changes.
		//
		// PostgreSQL also answers `WHEN NOT MATCHED BY SOURCE THEN INSERT`
		// and `WHEN NOT MATCHED BY TARGET THEN DELETE` with a SYNTAX error
		// (42601) rather than a feature refusal — the action sets differ per
		// clause kind.
		//
		// This is a FEATURE, not a wrong answer. It is pinned three ways: the
		// ELEVEN #718 rows in the DML census, each carrying PostgreSQL 17's
		// own answer beside the 0A000 so the implementing arc measures
		// nothing; TestMergeNotMatchedBySourceIsReportedAsUnsupported; and the
		// limitation bullet in docs/sql-reference.md.
		if l.peekToken().typ == TokenKWBy {
			l.nextToken()
			side := l.nextToken()
			// Only the NOT MATCHED forms exist. `WHEN MATCHED BY SOURCE` is
			// not an unimplemented feature, it is not SQL at all, and
			// PostgreSQL answers it with a syntax error at the BY.
			if clause.Matched {
				return nil, fmt.Errorf("MERGE: unexpected BY after WHEN MATCHED")
			}
			return nil, sqlerr.New("0A000",
				"MERGE: WHEN NOT MATCHED BY %s is not supported", strings.ToUpper(side.val))
		}

		// Optional AND condition, scanned to the clause's own THEN.
		if l.peekToken().typ == TokenKWAnd {
			l.nextToken()
			clause.Condition = scanMergeClauseUntil(l, TokenKWThen)
		}

		// THEN
		if l.peekToken().typ != TokenKWThen {
			return nil, fmt.Errorf("MERGE: expected THEN")
		}
		l.nextToken()

		// Action: UPDATE SET ..., DELETE, INSERT ...
		actionTok := l.peekToken()
		switch actionTok.typ {
		case TokenKWUpdate:
			l.nextToken()
			clause.Action = "UPDATE"
			// Read until the next clause's WHEN — not a CASE's.
			clause.SQL = scanMergeClauseUntil(l, TokenKWWhen, TokenSemicolon)
		case TokenKWDelete:
			l.nextToken()
			clause.Action = "DELETE"
		case TokenKWInsert:
			l.nextToken()
			clause.Action = "INSERT"
			clause.SQL = scanMergeClauseUntil(l, TokenKWWhen, TokenSemicolon)
		default:
			return nil, fmt.Errorf("MERGE: expected UPDATE, DELETE, or INSERT after THEN")
		}

		info.WhenClauses = append(info.WhenClauses, clause)
	}

	return &ParsedQuery{
		Type:  QueryMerge,
		SQL:   sql,
		Merge: info,
	}, nil
}

// scanMergeClauseUntil consumes input up to the next stop token that is at
// DEPTH ZERO — outside every parenthesis and every CASE … END — and returns
// the raw text it consumed.
//
// All four of a MERGE's scans used to stop at the first stop token whatever
// its nesting, and a CASE expression carries the very keywords they stop on
// (#722):
//
//	ON        stops at WHEN   broken by `ON CASE WHEN … END`
//	AND       stops at THEN   broken by `AND CASE … THEN … END`
//	THEN UPDATE SET  at WHEN  broken by `SET n = CASE WHEN … END`
//	THEN INSERT …    at WHEN  broken by `VALUES (CASE WHEN … END)`
//
// The issue names the first two. A fix that patched only those would leave
// `ON` and `THEN INSERT` broken, which is why all four go through one
// function: the nesting rule is a property of a MERGE clause, not of one
// clause position.
//
// The pattern is collectUntil's (dml_parser.go): the stop test is
// `depth == 0 && stop`, never `stop` alone, and EOF breaks unconditionally so
// an unbalanced `(` or a CASE with no END cannot spin at depth > 0 forever —
// FuzzParseSQL found that shape once already.
func scanMergeClauseUntil(l *lexer, stop ...TokenType) string {
	stopAt := make(map[TokenType]bool, len(stop))
	for _, t := range stop {
		stopAt[t] = true
	}
	start := l.pos
	depth := 0
	for {
		pk := l.peekToken()
		if pk.typ == TokenEOF {
			break
		}
		if depth == 0 && stopAt[pk.typ] {
			break
		}
		switch pk.typ {
		case TokenLParen, TokenKWCase:
			depth++
		case TokenRParen, TokenKWEnd:
			if depth > 0 {
				depth--
			}
		}
		l.nextToken()
	}
	return strings.TrimSpace(l.input[start:l.pos])
}

// isClauseKeyword returns true if a token is a clause-level keyword.
func isClauseKeyword(t token) bool {
	switch t.typ {
	case TokenKWUsing, TokenKWOn, TokenKWWhen, TokenKWThen,
		TokenKWUpdate, TokenKWDelete, TokenKWInsert, TokenKWSet:
		return true
	}
	return false
}

// lexParseCreateView handles: CREATE [OR REPLACE] VIEW <name> AS <query>
func lexParseCreateView(sql string, l *lexer, replace bool) (*ParsedQuery, error) {
	nameTok := l.nextToken()
	if nameTok.typ != TokenIdent {
		return nil, fmt.Errorf("CREATE VIEW: view name is required")
	}

	asTok := l.nextToken()
	if asTok.typ != TokenKWAs {
		return nil, fmt.Errorf("CREATE VIEW: expected AS after view name")
	}

	viewSQL := strings.TrimSpace(l.rest())
	if viewSQL == "" {
		return nil, fmt.Errorf("CREATE VIEW: view definition is required")
	}

	return &ParsedQuery{
		Type: QueryCreateView,
		SQL:  sql,
		CreateView: &CreateViewInfo{
			Name:    nameTok.val,
			SQL:     viewSQL,
			Replace: replace,
		},
	}, nil
}

// lexParseCreateAlert handles:
//
//	CREATE ALERT <name> AS <SELECT ...> EVERY <N> {SECONDS|MINUTES|HOURS}
//	  [WEBHOOK '<url>' [HEADERS { 'K' = 'V', ... }]]
//	  [INSERT INTO <table>]
//
// CREATE ALERT has already been consumed.
func lexParseCreateAlert(sql string, l *lexer) (*ParsedQuery, error) {
	nameTok := l.nextToken()
	if nameTok.typ != TokenIdent {
		return nil, fmt.Errorf("CREATE ALERT: alert name is required")
	}
	name := nameTok.val

	asTok := l.nextToken()
	if asTok.typ != TokenKWAs {
		return nil, fmt.Errorf("CREATE ALERT: expected AS after alert name")
	}

	// Scan SELECT body up to the top-level EVERY keyword using the raw input
	// (preserves whitespace/punctuation which the token stream throws away).
	rest := l.rest()
	before, after, ok := splitAtTopLevelKeyword(rest, "EVERY")
	if !ok {
		return nil, fmt.Errorf("CREATE ALERT: expected EVERY after SELECT body; example: CREATE ALERT x AS SELECT ... EVERY 5 MINUTES WEBHOOK 'https://...'")
	}
	queryText := strings.TrimSpace(before)
	if queryText == "" {
		return nil, fmt.Errorf("CREATE ALERT: SELECT body is required")
	}
	// Rebuild the lexer starting at EVERY so the rest of this function can
	// consume tokens as usual.
	l = newLexer(after)
	everyTok := l.nextToken()
	if everyTok.typ != TokenKWEvery {
		return nil, fmt.Errorf("CREATE ALERT: expected EVERY, got %q", everyTok.val)
	}

	// EVERY consumed; now parse: <N> <unit>
	nTok := l.nextToken()
	if nTok.typ != TokenNumber {
		return nil, fmt.Errorf("CREATE ALERT: expected number after EVERY; example: EVERY 5 MINUTES")
	}
	n, err := strconv.ParseInt(nTok.val, 10, 64)
	if err != nil || n <= 0 {
		return nil, fmt.Errorf("CREATE ALERT: interval must be a positive integer, got %q", nTok.val)
	}
	unitTok := l.nextToken()
	var interval time.Duration
	switch unitTok.typ {
	case TokenKWSeconds:
		interval = time.Duration(n) * time.Second
	case TokenKWMinutes:
		interval = time.Duration(n) * time.Minute
	case TokenKWHours:
		interval = time.Duration(n) * time.Hour
	default:
		return nil, fmt.Errorf("CREATE ALERT: expected SECONDS|MINUTES|HOURS, got %q", unitTok.val)
	}
	if interval < alertIntervalFloor {
		return nil, fmt.Errorf("CREATE ALERT: interval must be >= %v, got %v", alertIntervalFloor, interval)
	}

	info := &CreateAlertInfo{
		Name:      name,
		QueryText: queryText,
		Interval:  interval,
	}

	// Optional sinks: WEBHOOK, INSERT INTO. Order WEBHOOK-first allowed; also INSERT-first.
doneSinks:
	for {
		peek := l.peekToken()
		switch peek.typ {
		case TokenKWWebhook:
			l.nextToken() // consume WEBHOOK
			urlTok := l.nextToken()
			if urlTok.typ != TokenString {
				return nil, fmt.Errorf("CREATE ALERT: WEBHOOK expects a string URL literal")
			}
			u, err := url.Parse(urlTok.val)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
				return nil, fmt.Errorf("CREATE ALERT: WEBHOOK URL must be http:// or https://, got %q", urlTok.val)
			}
			info.WebhookURL = urlTok.val

			// Optional HEADERS { 'K' = 'V', ... }
			if l.peekToken().typ == TokenKWHeaders {
				l.nextToken() // consume HEADERS
				if l.nextToken().typ != TokenLBrace {
					return nil, fmt.Errorf("CREATE ALERT: expected '{' after HEADERS")
				}
				info.Headers = make(map[string]string)
				for {
					if l.peekToken().typ == TokenRBrace {
						l.nextToken()
						break
					}
					if len(info.Headers) > 0 {
						if l.nextToken().typ != TokenComma {
							return nil, fmt.Errorf("CREATE ALERT: expected ',' between headers")
						}
					}
					keyTok := l.nextToken()
					if keyTok.typ != TokenString {
						return nil, fmt.Errorf("CREATE ALERT: header key must be a string literal")
					}
					if l.nextToken().typ != TokenEq {
						return nil, fmt.Errorf("CREATE ALERT: expected '=' in header pair")
					}
					valTok := l.nextToken()
					if valTok.typ != TokenString {
						return nil, fmt.Errorf("CREATE ALERT: header value must be a string literal")
					}
					info.Headers[keyTok.val] = valTok.val
				}
			}
		case TokenKWInsert:
			l.nextToken() // consume INSERT
			if l.nextToken().typ != TokenKWInto {
				return nil, fmt.Errorf("CREATE ALERT: expected INTO after INSERT")
			}
			tTok := l.nextToken()
			if tTok.typ != TokenIdent {
				return nil, fmt.Errorf("CREATE ALERT: expected table name after INSERT INTO")
			}
			info.InsertInto = tTok.val
		case TokenEOF:
			break doneSinks
		default:
			return nil, fmt.Errorf("CREATE ALERT: unexpected token %q, expected WEBHOOK or INSERT INTO", peek.val)
		}
	}

	if info.WebhookURL == "" && info.InsertInto == "" {
		return nil, fmt.Errorf("CREATE ALERT: at least one sink (WEBHOOK or INSERT INTO) is required")
	}
	if !isValidIdent(name) {
		return nil, fmt.Errorf("CREATE ALERT: invalid alert name %q (must match [a-zA-Z_][a-zA-Z0-9_]*, len<=128)", name)
	}

	return &ParsedQuery{
		Type:        QueryCreateAlert,
		SQL:         sql,
		CreateAlert: info,
	}, nil
}

// isValidIdent reports whether s is a valid identifier (first char letter/_, rest alnum/_, len<=128).
func isValidIdent(s string) bool {
	if s == "" || len(s) > 128 {
		return false
	}
	for i, r := range s {
		first := i == 0
		isLetter := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_'
		isDigit := r >= '0' && r <= '9'
		if first && !isLetter {
			return false
		}
		if !first && !isLetter && !isDigit {
			return false
		}
	}
	return true
}

// splitAtTopLevelKeyword scans s left-to-right and returns the split point
// at the first occurrence of kw (case-insensitive) that is (a) at paren-depth
// zero, (b) outside any single-quoted string literal, and (c) bounded by
// non-identifier characters on both sides. Returns before (text preceding kw)
// and after (text starting at kw, kw inclusive). ok=false if not found.
func splitAtTopLevelKeyword(s, kw string) (before, after string, ok bool) {
	depth := 0
	inString := false
	kwLen := len(kw)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if inString {
			if c == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					i++ // doubled quote, skip
					continue
				}
				inString = false
			}
			continue
		}
		switch c {
		case '\'':
			inString = true
			continue
		case '(':
			depth++
			continue
		case ')':
			depth--
			continue
		}
		if depth != 0 {
			continue
		}
		if i+kwLen > len(s) {
			break
		}
		if !strings.EqualFold(s[i:i+kwLen], kw) {
			continue
		}
		if !isKWBoundary(s, i, kwLen) {
			continue
		}
		return s[:i], s[i:], true
	}
	return "", "", false
}

// isKWBoundary reports whether s[start:start+n] is bounded on both sides
// by characters that cannot appear in an identifier (or is at the edge).
func isKWBoundary(s string, start, n int) bool {
	if start > 0 {
		p := s[start-1]
		if (p >= 'a' && p <= 'z') || (p >= 'A' && p <= 'Z') || (p >= '0' && p <= '9') || p == '_' {
			return false
		}
	}
	end := start + n
	if end < len(s) {
		c := s[end]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			return false
		}
	}
	return true
}

// lexParseDropView handles: DROP VIEW [IF EXISTS] <name>
func lexParseDropView(sql string, l *lexer) (*ParsedQuery, error) {
	ifExists := false
	tok := l.nextToken()
	if tok.typ == TokenKWIf {
		existsTok := l.nextToken()
		if existsTok.typ != TokenKWExists {
			return nil, fmt.Errorf("expected EXISTS after IF")
		}
		ifExists = true
		tok = l.nextToken()
	}

	if tok.typ != TokenIdent {
		return nil, fmt.Errorf("DROP VIEW: view name is required")
	}

	return &ParsedQuery{
		Type: QueryDropView,
		SQL:  sql,
		DropView: &DropViewInfo{
			Name:     tok.val,
			IfExists: ifExists,
		},
	}, nil
}

// lexParseDropAlert handles: DROP ALERT [IF EXISTS] <name>
// DROP ALERT has already been consumed.
func lexParseDropAlert(sql string, l *lexer) (*ParsedQuery, error) {
	ifExists := false
	tok := l.nextToken()
	if tok.typ == TokenKWIf {
		existsTok := l.nextToken()
		if existsTok.typ != TokenKWExists {
			return nil, fmt.Errorf("expected EXISTS after IF")
		}
		ifExists = true
		tok = l.nextToken()
	}
	if tok.typ != TokenIdent {
		return nil, fmt.Errorf("DROP ALERT: alert name is required")
	}
	return &ParsedQuery{
		Type:      QueryDropAlert,
		SQL:       sql,
		DropAlert: &DropAlertInfo{Name: tok.val, IfExists: ifExists},
	}, nil
}

// lexParseAlterAlert handles: ALTER ALERT <name> {ENABLE|DISABLE}
// ALTER ALERT has already been consumed.
func lexParseAlterAlert(sql string, l *lexer) (*ParsedQuery, error) {
	nameTok := l.nextToken()
	if nameTok.typ != TokenIdent {
		return nil, fmt.Errorf("ALTER ALERT: alert name is required")
	}
	actionTok := l.nextToken()
	var enable bool
	switch actionTok.typ {
	case TokenKWEnable:
		enable = true
	case TokenKWDisable:
		enable = false
	default:
		return nil, fmt.Errorf("ALTER ALERT: expected ENABLE or DISABLE, got %q", actionTok.val)
	}
	return &ParsedQuery{
		Type:       QueryAlterAlert,
		SQL:        sql,
		AlterAlert: &AlterAlertInfo{Name: nameTok.val, Enable: enable},
	}, nil
}
