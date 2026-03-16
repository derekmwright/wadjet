// Package sql provides SQL parsing using a custom recursive descent parser.
package sql

import (
	"fmt"
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
	Windows        []WindowSpec   // extracted window function specs
	CTEs           []CTEDef       // extracted CTE definitions
	SelectInfo     *SelectInfo    // parsed SELECT info (replaces AST)
}

// CTEDef represents a Common Table Expression definition.
type CTEDef struct {
	Name string // CTE name (lowercased for matching)
	SQL  string // the CTE body SQL (the SELECT inside the parentheses)
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
	}

	// Pre-parse CTEs — extract WITH ... AS (...) clauses
	var cteDefs []CTEDef
	if first.typ == TokenKWWith {
		var remaining string
		var err error
		cteDefs, remaining, err = extractCTEs(trimmed)
		if err != nil {
			return nil, fmt.Errorf("extracting CTEs: %w", err)
		}
		trimmed = remaining
	}

	// Pre-parse window functions
	rewritten, windowSpecs, err := rewriteWindowFunctions(trimmed)
	if err != nil {
		return nil, fmt.Errorf("rewriting window functions: %w", err)
	}

	// Parse using our recursive descent parser
	sp := newSelectParser(rewritten)
	info, err := sp.parseSelectOrUnion()
	if err != nil {
		return nil, fmt.Errorf("parsing SQL: %w", err)
	}

	// Propagate CTE definitions
	info.CTEs = cteDefs

	// Propagate window specs and mark window columns
	info.Windows = windowSpecs
	windowAliases := make(map[string]*WindowSpec)
	for i := range info.Windows {
		windowAliases[info.Windows[i].Alias] = &info.Windows[i]
	}
	for i := range info.Columns {
		alias := info.Columns[i].Alias
		if alias == "" {
			alias = info.Columns[i].ColumnRef
		}
		if ws, ok := windowAliases[alias]; ok {
			info.Columns[i].IsWindow = true
			info.Columns[i].WindowSpec = ws
		}
	}

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

// UnionInfo describes a UNION query with left and right sides.
type UnionInfo struct {
	Left  *SelectInfo
	Right *SelectInfo
	All   bool // true for UNION ALL (no dedup)
}

// SelectInfo contains extracted information from a SELECT statement.
type SelectInfo struct {
	Tables     []TableRef
	Joins      []JoinInfo
	Columns    []SelectColumn
	Where      string
	WhereExpr  Node
	GroupBy    []string
	Having     string
	HavingExpr Node
	Distinct   bool
	OrderBy    []OrderByItem
	Limit      string
	Offset     string
	Windows    []WindowSpec // window function specs extracted during pre-parse
	CTEs       []CTEDef     // CTE definitions extracted during pre-parse
	Union      *UnionInfo   // non-nil if this is a UNION query
}

// TableRef is a reference to a table.
type TableRef struct {
	Name  string
	Alias string
}

// SelectColumn describes a column in a SELECT clause.
type SelectColumn struct {
	Expr        string
	Alias       string
	Star        bool
	IsAgg       bool
	AggFunc     string
	AggArg      string
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
	Column string
	Desc   bool
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

	// Dispatch to TABLE or FUNCTION
	if tok.typ == TokenKWTable {
		return lexParseCreateTable(sql, l)
	}

	if tok.typ != TokenKWFunction {
		return nil, fmt.Errorf("expected TABLE or FUNCTION after CREATE")
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

	if kindTok.typ != TokenKWFunction {
		return nil, fmt.Errorf("expected TABLE or FUNCTION after DROP")
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

		col := ColumnDef{
			Name:     strings.ToLower(colNameTok.val),
			Type:     colTypeTok.val,
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

// extractCTEs parses WITH cte_name AS (body) [, cte_name2 AS (body2)] SELECT ...
// It returns the CTE definitions and the remaining SQL (the final SELECT statement).
// The input must start with WITH (case-insensitive).
func extractCTEs(sql string) ([]CTEDef, string, error) {
	var defs []CTEDef
	n := len(sql)

	// Skip past "WITH"
	i := skipWS(sql, 0)
	if i+4 > n || !strings.EqualFold(sql[i:i+4], "WITH") {
		return nil, sql, nil
	}
	i += 4

	// Make sure WITH isn't part of a longer word
	if i < n && isIdentByte(sql[i]) {
		return nil, sql, nil
	}

	for {
		// Skip whitespace to find CTE name
		i = skipWS(sql, i)
		if i >= n {
			return nil, "", fmt.Errorf("unexpected end of input after WITH")
		}

		// Read CTE name
		if !isIdentStartByte(sql[i]) {
			return nil, "", fmt.Errorf("expected CTE name, got %q", string(sql[i]))
		}
		nameStart := i
		for i < n && isIdentByte(sql[i]) {
			i++
		}
		cteName := sql[nameStart:i]

		// Check if this "name" is actually SELECT — that means there are no
		// more CTEs and the final query starts here.
		if strings.EqualFold(cteName, "SELECT") {
			return nil, "", fmt.Errorf("expected CTE definition, found SELECT")
		}

		// Expect AS
		i = skipWS(sql, i)
		if i+2 > n || !strings.EqualFold(sql[i:i+2], "AS") {
			return nil, "", fmt.Errorf("expected AS after CTE name %q", cteName)
		}
		i += 2
		// Make sure AS isn't part of a longer word
		if i < n && isIdentByte(sql[i]) {
			return nil, "", fmt.Errorf("expected AS after CTE name %q", cteName)
		}

		// Expect opening paren for the CTE body
		i = skipWS(sql, i)
		if i >= n || sql[i] != '(' {
			return nil, "", fmt.Errorf("expected '(' after AS in CTE %q", cteName)
		}
		i++ // skip '('

		// Find matching closing paren, respecting nested parens and string literals
		bodyStart := i
		depth := 1
		for i < n && depth > 0 {
			if sql[i] == '\'' {
				i = skipStringLit(sql, i)
				continue
			}
			if sql[i] == '(' {
				depth++
			}
			if sql[i] == ')' {
				depth--
			}
			if depth > 0 {
				i++
			}
		}
		if depth != 0 {
			return nil, "", fmt.Errorf("unmatched parenthesis in CTE %q", cteName)
		}
		bodyEnd := i
		i++ // skip closing ')'

		body := strings.TrimSpace(sql[bodyStart:bodyEnd])
		defs = append(defs, CTEDef{
			Name: strings.ToLower(cteName),
			SQL:  body,
		})

		// After the CTE body, check for comma (more CTEs) or the start of
		// the final SELECT statement.
		i = skipWS(sql, i)
		if i >= n {
			return nil, "", fmt.Errorf("expected SELECT after CTE definitions")
		}

		if sql[i] == ',' {
			i++ // consume comma, loop for next CTE
			continue
		}

		// No comma — the rest is the final query
		remaining := strings.TrimSpace(sql[i:])
		return defs, remaining, nil
	}
}

// skipWSInline skips spaces and tabs (not newlines, for safety) starting at i.
func skipWSInline(s string, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') {
		i++
	}
	return i
}
