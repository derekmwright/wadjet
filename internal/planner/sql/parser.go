// Package sql provides SQL parsing using vitess-sqlparser.
package sql

import (
	"fmt"
	"strings"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
)

// ParsedQuery represents a parsed SQL query.
type ParsedQuery struct {
	AST            sqlparser.Statement
	Type           QueryType
	TableName      string
	SQL            string
	Explain        *ExplainInfo
	Describe       *DescribeInfo
	CreateFunction *CreateFunctionInfo
	DropFunction   *DropFunctionInfo
	Windows        []WindowSpec   // extracted window function specs
	CTEs           []CTEDef       // extracted CTE definitions
	JoinOverrides  map[int]string // join index -> original type for joins rewritten before vitess parsing
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

// QueryType identifies the kind of SQL statement.
type QueryType int

const (
	QuerySelect QueryType = iota
	QueryExplain
	QueryDescribe
	QueryCreateFunction
	QueryDropFunction
	QueryShowFunctions
	QueryUnsupported
)

// Parse parses a SQL string into a ParsedQuery.
func Parse(sql string) (*ParsedQuery, error) {
	trimmed := strings.TrimSpace(sql)

	// Use the lexer to peek at the first token for dispatch.
	// Statements that vitess-sqlparser doesn't support are handled here;
	// everything else falls through to vitess.
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

	// Pre-parse CTEs — extract WITH ... AS (...) clauses before vitess parsing.
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

	// Pre-parse: rewrite join types that vitess doesn't support natively.
	// FULL [OUTER] JOIN → LEFT JOIN (tagged via JoinOverrides).
	// CROSS JOIN → JOIN (vitess does this anyway, tagged via JoinOverrides).
	trimmed, joinOverrides := rewriteJoinTypes(trimmed)

	// Pre-parse window functions (vitess doesn't support OVER clauses)
	rewritten, windowSpecs, err := rewriteWindowFunctions(trimmed)
	if err != nil {
		return nil, fmt.Errorf("rewriting window functions: %w", err)
	}

	stmt, err := sqlparser.Parse(rewritten)
	if err != nil {
		return nil, fmt.Errorf("parsing SQL: %w", err)
	}

	pq := &ParsedQuery{
		AST:           stmt,
		SQL:           sql,
		Windows:       windowSpecs,
		CTEs:          cteDefs,
		JoinOverrides: joinOverrides,
	}

	switch stmt.(type) {
	case *sqlparser.Select:
		pq.Type = QuerySelect
	case *sqlparser.Union:
		pq.Type = QuerySelect
	default:
		pq.Type = QueryUnsupported
		return pq, fmt.Errorf("unsupported statement type: %T", stmt)
	}

	return pq, nil
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
		AST:  inner.AST,
		Type: QueryExplain,
		SQL:  sql,
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

// lexParseShow handles: SHOW FUNCTIONS / SHOW COLUMNS FROM <table>
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

	default:
		return nil, fmt.Errorf("unsupported SHOW statement")
	}
}

// lexParseCreate handles: CREATE [OR REPLACE] FUNCTION ...
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

	if tok.typ != TokenKWFunction {
		return nil, fmt.Errorf("expected FUNCTION after CREATE")
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

// lexParseDrop handles: DROP FUNCTION [IF EXISTS] <name>
// DROP has already been consumed.
func lexParseDrop(sql string, l *lexer) (*ParsedQuery, error) {
	funcTok := l.nextToken()
	if funcTok.typ != TokenKWFunction {
		return nil, fmt.Errorf("expected FUNCTION after DROP")
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
		// more CTEs and the final query starts here. This shouldn't happen in
		// valid syntax because AS must follow the name, but guard against it.
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

// ExtractSelect extracts details from a SELECT or UNION statement.
func ExtractSelect(pq *ParsedQuery) (*SelectInfo, error) {
	// Handle UNION statements by recursively extracting left and right sides.
	if u, ok := pq.AST.(*sqlparser.Union); ok {
		return extractUnion(pq, u)
	}

	sel, ok := pq.AST.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("not a SELECT statement")
	}

	info := &SelectInfo{}

	// Extract FROM tables
	for _, expr := range sel.From {
		switch t := expr.(type) {
		case *sqlparser.AliasedTableExpr:
			name, ok := t.Expr.(sqlparser.TableName)
			if !ok {
				continue
			}
			ti := TableRef{
				Name:  name.Name.String(),
				Alias: t.As.String(),
			}
			if ti.Alias == "" {
				ti.Alias = ti.Name
			}
			info.Tables = append(info.Tables, ti)
		case *sqlparser.JoinTableExpr:
			info.Joins = append(info.Joins, extractJoin(t))
			// Also extract the left table
			if alias, ok := t.LeftExpr.(*sqlparser.AliasedTableExpr); ok {
				name, ok := alias.Expr.(sqlparser.TableName)
				if ok {
					ti := TableRef{
						Name:  name.Name.String(),
						Alias: alias.As.String(),
					}
					if ti.Alias == "" {
						ti.Alias = ti.Name
					}
					info.Tables = append(info.Tables, ti)
				}
			}
		}
	}

	// Apply join type overrides from pre-parse rewriting (CROSS JOIN, FULL OUTER JOIN)
	for idx, origType := range pq.JoinOverrides {
		if idx < len(info.Joins) {
			info.Joins[idx].Type = origType
		}
	}

	// Extract SELECT columns
	for _, expr := range sel.SelectExprs {
		switch e := expr.(type) {
		case *sqlparser.StarExpr:
			info.Columns = append(info.Columns, SelectColumn{
				Expr: "*",
				Star: true,
			})
		case *sqlparser.AliasedExpr:
			col := SelectColumn{
				Expr:    sqlparser.String(e.Expr),
				Alias:   e.As.String(),
				ASTExpr: e.Expr,
			}
			// Check if it's an aggregate function
			if fn, ok := e.Expr.(*sqlparser.FuncExpr); ok {
				col.IsAgg = true
				col.AggFunc = fn.Name.Lowered()
				col.AggDistinct = fn.Distinct
				if len(fn.Exprs) > 0 {
					col.AggArg = sqlparser.String(fn.Exprs[0])
				}
			}
			// Check if it's a simple column reference
			if colName, ok := e.Expr.(*sqlparser.ColName); ok {
				col.ColumnRef = colName.Name.String()
				col.TableRef = colName.Qualifier.Name.String()
			}
			info.Columns = append(info.Columns, col)
		}
	}

	// Extract WHERE
	if sel.Where != nil {
		info.Where = sqlparser.String(sel.Where.Expr)
		info.WhereExpr = sel.Where.Expr
	}

	// Extract GROUP BY
	for _, expr := range sel.GroupBy {
		info.GroupBy = append(info.GroupBy, sqlparser.String(expr))
	}

	// Extract HAVING
	if sel.Having != nil {
		info.Having = sqlparser.String(sel.Having.Expr)
		info.HavingExpr = sel.Having.Expr
	}

	// Extract DISTINCT
	info.Distinct = sel.Distinct != ""

	// Extract ORDER BY
	for _, order := range sel.OrderBy {
		info.OrderBy = append(info.OrderBy, OrderByItem{
			Column: sqlparser.String(order.Expr),
			Desc:   order.Direction == sqlparser.DescScr,
		})
	}

	// Extract LIMIT
	if sel.Limit != nil {
		if sel.Limit.Rowcount != nil {
			info.Limit = sqlparser.String(sel.Limit.Rowcount)
		}
		if sel.Limit.Offset != nil {
			info.Offset = sqlparser.String(sel.Limit.Offset)
		}
	}

	// Propagate CTE definitions
	info.CTEs = pq.CTEs

	// Propagate window specs and mark window columns
	info.Windows = pq.Windows
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

	return info, nil
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
	WhereExpr  sqlparser.Expr
	GroupBy    []string
	Having     string
	HavingExpr sqlparser.Expr
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
	AggDistinct bool           // COUNT(DISTINCT col)
	IsWindow    bool           // true if this is a window function
	WindowSpec  *WindowSpec    // window function details
	ColumnRef   string
	TableRef    string
	ASTExpr     sqlparser.Expr // original AST expression node
}

// JoinInfo describes a JOIN clause.
type JoinInfo struct {
	Type      string // inner, left, right, full, cross
	LeftTable string
	RightTable string
	RightAlias string
	Condition  string
	CondExpr   sqlparser.Expr
}

// OrderByItem describes an ORDER BY element.
type OrderByItem struct {
	Column string
	Desc   bool
}

func extractJoin(join *sqlparser.JoinTableExpr) JoinInfo {
	ji := JoinInfo{
		Type: join.Join,
	}

	if alias, ok := join.RightExpr.(*sqlparser.AliasedTableExpr); ok {
		if name, ok := alias.Expr.(sqlparser.TableName); ok {
			ji.RightTable = name.Name.String()
			ji.RightAlias = alias.As.String()
			if ji.RightAlias == "" {
				ji.RightAlias = ji.RightTable
			}
		}
	}

	if join.On != nil {
		ji.Condition = sqlparser.String(join.On)
		ji.CondExpr = join.On
	}

	return ji
}

// extractUnion builds a SelectInfo from a *sqlparser.Union node.
// It recursively extracts the left and right sides and populates the Union field.
// ORDER BY and LIMIT on the overall UNION are placed on the outer SelectInfo.
func extractUnion(pq *ParsedQuery, u *sqlparser.Union) (*SelectInfo, error) {
	leftInfo, err := extractSelectSide(pq, u.Left)
	if err != nil {
		return nil, fmt.Errorf("extracting UNION left side: %w", err)
	}
	rightInfo, err := extractSelectSide(pq, u.Right)
	if err != nil {
		return nil, fmt.Errorf("extracting UNION right side: %w", err)
	}

	all := strings.Contains(strings.ToLower(u.Type), "all")

	info := &SelectInfo{
		Union: &UnionInfo{
			Left:  leftInfo,
			Right: rightInfo,
			All:   all,
		},
		CTEs: pq.CTEs,
	}

	// Extract ORDER BY on the overall UNION
	for _, order := range u.OrderBy {
		info.OrderBy = append(info.OrderBy, OrderByItem{
			Column: sqlparser.String(order.Expr),
			Desc:   order.Direction == sqlparser.DescScr,
		})
	}

	// Extract LIMIT on the overall UNION
	if u.Limit != nil {
		if u.Limit.Rowcount != nil {
			info.Limit = sqlparser.String(u.Limit.Rowcount)
		}
		if u.Limit.Offset != nil {
			info.Offset = sqlparser.String(u.Limit.Offset)
		}
	}

	return info, nil
}

// extractSelectSide extracts a SelectInfo from one side of a UNION.
// The side can be either a *sqlparser.Select or a nested *sqlparser.Union.
func extractSelectSide(pq *ParsedQuery, stmt sqlparser.SelectStatement) (*SelectInfo, error) {
	sidePQ := &ParsedQuery{
		AST:     stmt,
		SQL:     pq.SQL,
		Windows: pq.Windows,
		CTEs:    pq.CTEs,
	}
	return ExtractSelect(sidePQ)
}

// rewriteJoinTypes rewrites join type keywords that vitess-sqlparser doesn't
// support into forms it does, returning the rewritten SQL and a map of
// join indices to their original type strings.
//
// Rewrites performed:
//   - "FULL OUTER JOIN" / "FULL JOIN" → "LEFT JOIN" (override: "full outer join")
//   - "CROSS JOIN" → "JOIN" (override: "cross join")
//
// vitess already handles RIGHT [OUTER] JOIN natively as "right join".
func rewriteJoinTypes(sql string) (string, map[int]string) {
	upper := strings.ToUpper(sql)
	overrides := map[int]string{}
	joinIndex := 0

	// We scan through the SQL looking for JOIN keywords and count each join
	// to track the index. We also replace unsupported forms.
	var result strings.Builder
	result.Grow(len(sql))
	i := 0
	n := len(sql)

	for i < n {
		// Skip string literals
		if sql[i] == '\'' {
			end := skipStringLit(sql, i)
			result.WriteString(sql[i:end])
			i = end
			continue
		}

		// Look for FULL [OUTER] JOIN
		if i+9 <= n && upper[i:i+4] == "FULL" && (i == 0 || !isIdentByte(sql[i-1])) {
			// Check for "FULL OUTER JOIN" or "FULL JOIN"
			rest := i + 4
			rest = skipWSInline(sql, rest)
			if rest+5 <= n && upper[rest:rest+5] == "OUTER" && !isIdentByte(sql[min(rest+5, n-1)]) {
				afterOuter := skipWSInline(sql, rest+5)
				if afterOuter+4 <= n && upper[afterOuter:afterOuter+4] == "JOIN" && (afterOuter+4 >= n || !isIdentByte(sql[afterOuter+4])) {
					overrides[joinIndex] = "full outer join"
					joinIndex++
					result.WriteString("left join")
					i = afterOuter + 4
					continue
				}
			}
			if rest+4 <= n && upper[rest:rest+4] == "JOIN" && (rest+4 >= n || !isIdentByte(sql[rest+4])) {
				overrides[joinIndex] = "full outer join"
				joinIndex++
				result.WriteString("left join")
				i = rest + 4
				continue
			}
		}

		// Look for CROSS JOIN
		if i+10 <= n && upper[i:i+5] == "CROSS" && (i == 0 || !isIdentByte(sql[i-1])) {
			rest := skipWSInline(sql, i+5)
			if rest+4 <= n && upper[rest:rest+4] == "JOIN" && (rest+4 >= n || !isIdentByte(sql[rest+4])) {
				overrides[joinIndex] = "cross join"
				joinIndex++
				result.WriteString("join")
				i = rest + 4
				continue
			}
		}

		// Count other join types to keep the index accurate.
		// Match: [LEFT|RIGHT|INNER|NATURAL|STRAIGHT_] JOIN
		if i+4 <= n && upper[i:i+4] == "JOIN" && (i == 0 || !isIdentByte(sql[i-1])) && (i+4 >= n || !isIdentByte(sql[i+4])) {
			joinIndex++
			result.WriteString(sql[i : i+4])
			i += 4
			continue
		}

		result.WriteByte(sql[i])
		i++
	}

	if len(overrides) == 0 {
		return sql, nil
	}
	return result.String(), overrides
}

// skipWSInline skips spaces and tabs (not newlines, for safety) starting at i.
func skipWSInline(s string, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') {
		i++
	}
	return i
}


