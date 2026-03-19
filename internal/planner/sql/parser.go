// Package sql provides SQL parsing using a custom recursive descent parser.
package sql

import (
	"fmt"
	"strconv"
	"strings"
)

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
	CreateView     *CreateViewInfo
	DropView       *DropViewInfo
	Update         *UpdateInfo
	Delete         *DeleteInfo
	Insert         *InsertInfo
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

// CTEDef represents a Common Table Expression definition.
type CTEDef struct {
	Name    string   // CTE name (lowercased for matching)
	SQL     string   // the CTE body SQL (the SELECT inside the parentheses)
	Columns []string // optional column name list
}

// ExplainInfo holds details for an EXPLAIN statement.
type ExplainInfo struct {
	Verbose  bool
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
	QueryShowTables
	QueryUpdate
	QueryDelete
	QueryInsert
	QueryCreateView
	QueryDropView
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
	Name          string
	Alias         string
	IsFunction    bool              // true for table functions like read_json(...)
	FuncArgs      []string          // positional arguments
	FuncNamedArgs map[string]string // named arguments (key=value)
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
	Type       string // join, left join, right join, full outer join, cross join
	LeftTable  string
	RightTable string
	RightAlias string
	Condition  string
	CondExpr   Node
}

// OrderByItem describes an ORDER BY element.
type OrderByItem struct {
	Column     string
	Desc       bool
	NullsFirst *bool // nil = default, true = NULLS FIRST, false = NULLS LAST
}

// --- Lexer-based pre-parse functions ---

// lexParseExplain handles: EXPLAIN [VERBOSE] <query>
func lexParseExplain(sql string, l *lexer) (*ParsedQuery, error) {
	l.nextToken() // consume EXPLAIN

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

	if tok.typ != TokenKWFunction {
		return nil, fmt.Errorf("expected TABLE, VIEW, or FUNCTION after CREATE")
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

	if kindTok.typ != TokenKWFunction {
		return nil, fmt.Errorf("expected TABLE, VIEW, or FUNCTION after DROP")
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
// and ORDER BY with the corresponding SELECT column expression (1-indexed).
// UNION queries are skipped since positions would be ambiguous.
func resolvePositionalRefs(info *SelectInfo) error {
	if info.Union != nil {
		return nil // skip UNION queries
	}

	// Resolve GROUP BY positional refs
	for i, gb := range info.GroupBy {
		pos, err := strconv.Atoi(strings.TrimSpace(gb))
		if err != nil {
			continue // not a number, leave as-is
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

// lexParseCTEs parses CTE definitions from a lexer.
// The caller has verified the first token is WITH.
func lexParseCTEs(l *lexer) ([]CTEDef, error) {
	// Consume WITH
	l.nextToken()

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
			Name:    cteName,
			SQL:     body,
			Columns: columns,
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

