// Package sql provides SQL parsing using a custom recursive descent parser.
package sql

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
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
	Windows        []WindowSpec   // extracted window function specs
	CTEs           []CTEDef       // extracted CTE definitions
	SelectInfo     *SelectInfo    // parsed SELECT info (replaces AST)
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
	Target      string // target table name
	TargetAlias string
	Source      string // source table/subquery
	SourceAlias string
	OnCondition string           // MERGE ON condition
	WhenClauses []MergeWhenClause // WHEN MATCHED / NOT MATCHED clauses
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

// UpdateInfo holds details for an UPDATE statement.
type UpdateInfo struct {
	Table      string            // table name
	SetClauses []SetClause       // SET column = value pairs
	WhereSQL   string            // raw WHERE clause SQL (empty = update all rows)
}

// SetClause represents a single SET column = value assignment.
type SetClause struct {
	Column string
	Value  string // raw expression text
}

// DeleteInfo holds details for a DELETE statement.
type DeleteInfo struct {
	Table    string // table name
	WhereSQL string // raw WHERE clause SQL (empty = delete all rows)
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
	QueryText  string            // raw SELECT text, re-parsed at eval time
	Interval   time.Duration     // validated >= 10s at parse time
	WebhookURL string            // "" if no webhook sink
	Headers    map[string]string
	InsertInto string            // "" if no table sink; at least one sink required
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

// Parse parses a SQL string into a ParsedQuery.
func Parse(sql string) (*ParsedQuery, error) {
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
	case TokenKWUpdate:
		return parseUpdate(trimmed, l)
	case TokenKWDelete:
		return parseDelete(trimmed, l)
	case TokenKWInsert:
		return parseInsert(trimmed, l)
	case TokenKWAlter:
		l.nextToken() // consume ALTER
		return parseAlterTable(trimmed, l)
	case TokenKWMerge:
		return parseMerge(trimmed, l)
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
	GroupByExprs []Node // AST for GROUP BY expressions (parallel to GroupBy)
	GroupingSets [][]string // GROUPING SETS / CUBE / ROLLUP (nil = simple GROUP BY)
	Having       string
	HavingExpr   Node
	Distinct     bool
	Qualify      string
	QualifyExpr  Node
	OrderBy      []OrderByItem
	Limit        string
	Offset       string
	Windows      []WindowSpec // window function specs extracted during pre-parse
	CTEs         []CTEDef     // CTE definitions extracted during pre-parse
	Union        *UnionInfo   // non-nil if this is a UNION query
}

// TableRef is a reference to a table or table-producing function.
type TableRef struct {
	Name            string
	Qualifier       string // schema or catalog.schema written before the name
	Alias           string
	IsFunction      bool              // true for table functions like read_json(...)
	FuncArgs        []string          // positional arguments
	FuncNamedArgs   map[string]string // named arguments (key=value)
	WithOrdinality  bool              // UNNEST(...) WITH ORDINALITY
	ColumnAliases   []string          // AS alias(col1, col2, ...)
	SampleMethod    string            // TABLESAMPLE method: BERNOULLI, SYSTEM
	SamplePercent   string            // percentage for TABLESAMPLE
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
	Lateral       bool // LATERAL join — right side can reference left side columns
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
			TableName: strings.ToLower(tok.val),
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
				TableName: strings.ToLower(nameTok.val),
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

		// Column type
		colTypeTok := l.nextToken()
		if colTypeTok.typ != TokenIdent {
			return nil, fmt.Errorf("CREATE TABLE: expected type for column %q, got %q", colNameTok.val, colTypeTok.val)
		}

		typeName := colTypeTok.val
		// Optional type precision: DECIMAL(10,2), VARCHAR(255)
		if l.peekToken().typ == TokenLParen {
			l.nextToken() // consume (
			precTok := l.nextToken()
			if precTok.typ != TokenNumber {
				return nil, fmt.Errorf("CREATE TABLE: expected precision in %s()", typeName)
			}
			typeName += "(" + precTok.val
			if l.peekToken().typ == TokenComma {
				l.nextToken() // consume ,
				scaleTok := l.nextToken()
				if scaleTok.typ != TokenNumber {
					return nil, fmt.Errorf("CREATE TABLE: expected scale in %s(n,)", typeName)
				}
				typeName += "," + scaleTok.val
			}
			rp := l.nextToken()
			if rp.typ != TokenRParen {
				return nil, fmt.Errorf("CREATE TABLE: expected ) after type precision")
			}
			typeName += ")"
		}

		col := ColumnDef{
			Name:     strings.ToLower(colNameTok.val),
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
			partitionKeys = append(partitionKeys, strings.ToLower(keyTok.val))
		}
	}

	return &ParsedQuery{
		Type: QueryCreateTable,
		SQL:  sql,
		CreateTable: &CreateTableInfo{
			Name:          strings.ToLower(nameTok.val),
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
	for i, ob := range info.OrderBy {
		pos, err := strconv.Atoi(strings.TrimSpace(ob.Column))
		if err != nil {
			continue // not a number, leave as-is
		}
		if pos < 1 || pos > len(info.Columns) {
			return fmt.Errorf("ORDER BY position %d is out of range (1-%d)", pos, len(info.Columns))
		}
		col := info.Columns[pos-1]
		if col.Alias != "" {
			info.OrderBy[i].Column = col.Alias
		} else {
			info.OrderBy[i].Column = col.Expr
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
		if col.Alias != "" {
			info.OrderBy[i].Column = col.Alias
		} else {
			info.OrderBy[i].Column = col.Expr
		}
	}
	return nil
}

// resolveGroupByAliasRef resolves GROUP BY <alias> where <alias> names a
// SELECT column with a computed expression. Plain column refs (including
// renamed ones) are left alone — the aggregate resolves those by name, and
// a table column with the same name keeps precedence over the alias.
func resolveGroupByAliasRef(info *SelectInfo, i int, gb string) {
	name := strings.TrimSpace(gb)
	if strings.ContainsAny(name, ". ()") {
		return // qualified or expression-shaped — not a bare alias
	}
	for _, col := range info.Columns {
		if col.Alias != "" && strings.EqualFold(col.Alias, name) && !col.IsAgg && !col.IsWindow {
			substituteGroupByExpr(info, i, col)
			return
		}
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
	info.GroupByExprs[i] = col.ASTExpr
	info.GroupBy[i] = col.ASTExpr.String()
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
		cteName := strings.ToLower(nameTok.val)

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
					columns = append(columns, strings.ToLower(colTok.val))
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
		Table:    strings.ToLower(nameTok.val),
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
		info.ColumnName = strings.ToLower(tok.val)

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
		info.ColumnName = strings.ToLower(tok.val)

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
		info.ColumnName = strings.ToLower(tok.val)

		toTok := l.nextToken()
		if toTok.typ != TokenKWTo {
			return nil, fmt.Errorf("ALTER TABLE RENAME: expected TO after column name")
		}

		newTok := l.nextToken()
		if newTok.typ != TokenIdent {
			return nil, fmt.Errorf("ALTER TABLE RENAME: new column name is required")
		}
		info.NewColumnName = strings.ToLower(newTok.val)

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

	// Target table
	targetTok := l.nextToken()
	if targetTok.typ != TokenIdent {
		return nil, fmt.Errorf("MERGE: expected target table name")
	}
	info := &MergeInfo{Target: strings.ToLower(targetTok.val)}

	// Optional alias
	peek := l.peekToken()
	if peek.typ == TokenKWAs {
		l.nextToken()
		aliasTok := l.nextToken()
		info.TargetAlias = strings.ToLower(aliasTok.val)
	} else if peek.typ == TokenIdent && !isClauseKeyword(peek) {
		l.nextToken()
		info.TargetAlias = strings.ToLower(peek.val)
	}

	// USING
	if l.peekToken().typ != TokenKWUsing {
		return nil, fmt.Errorf("MERGE: expected USING")
	}
	l.nextToken()

	// Source table or subquery
	sourceTok := l.nextToken()
	if sourceTok.typ == TokenLParen {
		// Subquery — collect balanced parens
		depth := 1
		start := l.pos
		for depth > 0 {
			ch := l.next()
			if ch == '(' {
				depth++
			} else if ch == ')' {
				depth--
			} else if ch == eof {
				return nil, fmt.Errorf("MERGE: unterminated subquery in USING")
			}
		}
		info.Source = "(" + strings.TrimSpace(l.input[start:l.pos-1]) + ")"
		l.start = l.pos
	} else if sourceTok.typ == TokenIdent {
		info.Source = strings.ToLower(sourceTok.val)
	} else {
		return nil, fmt.Errorf("MERGE: expected source table or subquery after USING")
	}

	// Optional source alias
	peek = l.peekToken()
	if peek.typ == TokenKWAs {
		l.nextToken()
		aliasTok := l.nextToken()
		info.SourceAlias = strings.ToLower(aliasTok.val)
	} else if peek.typ == TokenIdent && !isClauseKeyword(peek) {
		l.nextToken()
		info.SourceAlias = strings.ToLower(peek.val)
	}

	// ON condition
	if l.peekToken().typ != TokenKWOn {
		return nil, fmt.Errorf("MERGE: expected ON")
	}
	l.nextToken()

	// Read condition until WHEN
	condStart := l.pos
	for {
		pk := l.peekToken()
		if pk.typ == TokenKWWhen || pk.typ == TokenEOF {
			break
		}
		l.nextToken()
	}
	info.OnCondition = strings.TrimSpace(l.input[condStart:l.pos])

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

		// Optional AND condition
		if l.peekToken().typ == TokenKWAnd {
			l.nextToken()
			andStart := l.pos
			for {
				pk := l.peekToken()
				if pk.typ == TokenKWThen || pk.typ == TokenEOF {
					break
				}
				l.nextToken()
			}
			clause.Condition = strings.TrimSpace(l.input[andStart:l.pos])
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
			// Read until next WHEN or EOF
			sqlStart := l.pos
			for {
				pk := l.peekToken()
				if pk.typ == TokenKWWhen || pk.typ == TokenEOF || pk.typ == TokenSemicolon {
					break
				}
				l.nextToken()
			}
			clause.SQL = strings.TrimSpace(l.input[sqlStart:l.pos])
		case TokenKWDelete:
			l.nextToken()
			clause.Action = "DELETE"
		case TokenKWInsert:
			l.nextToken()
			clause.Action = "INSERT"
			sqlStart := l.pos
			for {
				pk := l.peekToken()
				if pk.typ == TokenKWWhen || pk.typ == TokenEOF || pk.typ == TokenSemicolon {
					break
				}
				l.nextToken()
			}
			clause.SQL = strings.TrimSpace(l.input[sqlStart:l.pos])
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
			Name:    strings.ToLower(nameTok.val),
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
			Name:     strings.ToLower(tok.val),
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

