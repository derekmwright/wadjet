package sql

import (
	"fmt"
	"strconv"
	"strings"
)

// selectParser is a recursive descent parser for SELECT statements.
// It parses SELECT/UNION queries into our own AST (Node types from ast.go).
type selectParser struct {
	lex       *lexer
	cur       token
	lookahead []token // buffered tokens for lookahead
}

func newSelectParser(input string) *selectParser {
	p := &selectParser{lex: newLexer(input)}
	p.advance()
	return p
}

func (p *selectParser) advance() token {
	prev := p.cur
	if len(p.lookahead) > 0 {
		p.cur = p.lookahead[0]
		p.lookahead = p.lookahead[1:]
	} else {
		p.cur = p.lex.nextToken()
	}
	return prev
}

func (p *selectParser) peek() TokenType {
	return p.cur.typ
}

// peekN returns the token type n positions ahead (0 = current token).
func (p *selectParser) peekN(n int) TokenType {
	return p.peekTok(n).typ
}

// peekTok returns the token n positions ahead (0 = current token), buffering
// from the lexer as needed. Past end of input the lexer keeps returning EOF,
// so lookahead beyond the last token is safe.
func (p *selectParser) peekTok(n int) token {
	if n == 0 {
		return p.cur
	}
	// Buffer enough tokens
	for len(p.lookahead) < n {
		p.lookahead = append(p.lookahead, p.lex.nextToken())
	}
	return p.lookahead[n-1]
}

// isBareWord reports whether the token n positions ahead is the unquoted word
// w, compared case-insensitively. Used for the multi-word operators whose
// pieces this lexer does not reserve (AT TIME ZONE).
func (p *selectParser) isBareWord(n int, w string) bool {
	tok := p.peekTok(n)
	return tok.typ == TokenIdent && !tok.quoted && strings.EqualFold(tok.val, w)
}

func (p *selectParser) expect(typ TokenType) (token, error) {
	if p.cur.typ != typ {
		return token{}, fmt.Errorf("expected %d, got %q at position %d", typ, p.cur.val, p.cur.pos)
	}
	return p.advance(), nil
}

func (p *selectParser) isKeyword(kw TokenType) bool {
	return p.cur.typ == kw
}

// expectEndOfStatement reports an error unless the parser consumed the whole
// statement: only a statement separator and end of input may remain.
//
// Without it every clause this parser does not recognise was discarded in
// silence, because each parse step returns as soon as it has what it knows
// and nothing ever asked what was left over (#337). That turned a clause the
// parser cannot honour — OFFSET before LIMIT, NATURAL JOIN, or a typo — into
// a wrong answer instead of an error: `... WHERE n_regionkey = 1 GARBAGE`
// answered as though the garbage were not there, and `... ORDER BY 1 OFFSET
// 5` returned the whole table, which is the worst shape for pagination since
// the first page looks right.
//
// The token named here is where parsing stopped, which is the clause a client
// has to fix, not the end of the text.
func (p *selectParser) expectEndOfStatement() error {
	for p.cur.typ == TokenSemicolon {
		p.advance()
	}
	switch p.cur.typ {
	case TokenEOF:
		return nil
	case TokenError:
		return fmt.Errorf("syntax error at position %d: %s", p.cur.pos, p.cur.val)
	default:
		return fmt.Errorf("syntax error at or near %q (position %d): trailing input after the end of the statement",
			p.cur.val, p.cur.pos)
	}
}

// parseSelectOrUnion parses a SELECT with optional set operations (UNION, INTERSECT, EXCEPT).
func (p *selectParser) parseSelectOrUnion() (*SelectInfo, error) {
	left, err := p.parseSingleSelect()
	if err != nil {
		return nil, err
	}

	// Check for set operations: UNION, INTERSECT, EXCEPT
	for {
		var op SetOp
		switch {
		case p.isKeyword(TokenKWUnion):
			op = SetOpUnion
		case p.isKeyword(TokenKWIntersect):
			op = SetOpIntersect
		case p.isKeyword(TokenKWExcept):
			op = SetOpExcept
		default:
			goto done
		}
		p.advance() // consume UNION/INTERSECT/EXCEPT
		all := false
		if p.isKeyword(TokenKWAll) {
			p.advance()
			all = true
		}
		right, err := p.parseSingleSelect()
		if err != nil {
			return nil, fmt.Errorf("parsing %s right side: %w", op, err)
		}
		left = &SelectInfo{
			Union: &UnionInfo{Left: left, Right: right, All: all, Op: op},
		}
	}
done:

	// ORDER BY (applies to outermost query or UNION result)
	if p.isKeyword(TokenKWOrder) {
		p.advance() // consume ORDER
		if _, err := p.expect(TokenKWBy); err != nil {
			return nil, fmt.Errorf("expected BY after ORDER")
		}
		orderBy, err := p.parseOrderByList()
		if err != nil {
			return nil, err
		}
		left.OrderBy = orderBy
	}

	// LIMIT and OFFSET, in either order, each at most once. PostgreSQL and
	// DuckDB both accept `OFFSET n LIMIT m`, and reading the two in a fixed
	// order left whichever came second unconsumed — which used to mean the
	// whole table came back, the worst failure a paginating client can get
	// since the first page still looks right (#337). A repeat of either
	// keyword falls out of the loop and the end-of-statement guard rejects
	// it.
limitOffset:
	for {
		switch {
		case p.isKeyword(TokenKWLimit) && left.Limit == "":
			p.advance()
			tok, err := p.expect(TokenNumber)
			if err != nil {
				return nil, fmt.Errorf("expected number after LIMIT")
			}
			left.Limit = tok.val
		case p.isKeyword(TokenKWOffset) && left.Offset == "":
			p.advance()
			tok, err := p.expect(TokenNumber)
			if err != nil {
				return nil, fmt.Errorf("expected number after OFFSET")
			}
			left.Offset = tok.val
			// Optional ROW/ROWS noise word (SQL standard spelling).
			if p.isKeyword(TokenKWRow) || p.isKeyword(TokenKWRows) {
				p.advance()
			}
		default:
			break limitOffset
		}
	}

	// FETCH FIRST/NEXT N ROW(S) ONLY — SQL standard alternative to LIMIT
	if p.isKeyword(TokenKWFetch) {
		p.advance() // consume FETCH
		if p.isKeyword(TokenKWFirst) {
			p.advance()
		} else if p.peek() == TokenIdent && !p.cur.quoted && strings.EqualFold(p.cur.val, "NEXT") {
			p.advance()
		} else {
			return nil, fmt.Errorf("expected FIRST or NEXT after FETCH")
		}
		tok, err := p.expect(TokenNumber)
		if err != nil {
			return nil, fmt.Errorf("expected number after FETCH FIRST/NEXT")
		}
		if left.Limit == "" {
			left.Limit = tok.val
		}
		if p.isKeyword(TokenKWRow) || p.isKeyword(TokenKWRows) {
			p.advance()
		}
		if p.peek() == TokenIdent && !p.cur.quoted && strings.EqualFold(p.cur.val, "ONLY") {
			p.advance()
		}
	}

	return left, nil
}

func (p *selectParser) parseSingleSelect() (*SelectInfo, error) {
	if _, err := p.expect(TokenKWSelect); err != nil {
		return nil, fmt.Errorf("expected SELECT")
	}

	info := &SelectInfo{}

	// DISTINCT
	if p.isKeyword(TokenKWDistinct) {
		p.advance()
		info.Distinct = true
	}

	// SELECT columns
	cols, err := p.parseSelectColumns()
	if err != nil {
		return nil, err
	}
	info.Columns = cols

	// FROM
	if p.isKeyword(TokenKWFrom) {
		p.advance()
		if err := p.parseFromClause(info); err != nil {
			return nil, err
		}
	}

	// WHERE
	if p.isKeyword(TokenKWWhere) {
		p.advance()
		whereExpr, err := p.parseExpr()
		if err != nil {
			return nil, fmt.Errorf("parsing WHERE: %w", err)
		}
		info.Where = whereExpr.String()
		info.WhereExpr = whereExpr
	}

	// GROUP BY (with optional GROUPING SETS, CUBE, ROLLUP)
	if p.isKeyword(TokenKWGroup) {
		p.advance()
		if _, err := p.expect(TokenKWBy); err != nil {
			return nil, fmt.Errorf("expected BY after GROUP")
		}

		// Check for GROUPING SETS, CUBE, or ROLLUP
		switch {
		case p.isKeyword(TokenKWGrouping):
			// GROUPING SETS ((...), (...), ...)
			p.advance() // consume GROUPING
			if _, err := p.expect(TokenKWSets); err != nil {
				return nil, fmt.Errorf("expected SETS after GROUPING")
			}
			sets, allCols, err := p.parseGroupingSets()
			if err != nil {
				return nil, err
			}
			info.GroupingSets = sets
			info.GroupBy = allCols
		case p.isKeyword(TokenKWCube):
			// CUBE(a, b, c) → all 2^n subsets
			p.advance()
			cols, err := p.parseGroupingColList()
			if err != nil {
				return nil, fmt.Errorf("parsing CUBE: %w", err)
			}
			info.GroupingSets = expandCube(cols)
			info.GroupBy = cols
		case p.isKeyword(TokenKWRollup):
			// ROLLUP(a, b, c) → (a,b,c), (a,b), (a), ()
			p.advance()
			cols, err := p.parseGroupingColList()
			if err != nil {
				return nil, fmt.Errorf("parsing ROLLUP: %w", err)
			}
			info.GroupingSets = expandRollup(cols)
			info.GroupBy = cols
		default:
			// Simple GROUP BY
			for {
				gbExpr, err := p.parseExpr()
				if err != nil {
					return nil, fmt.Errorf("parsing GROUP BY: %w", err)
				}
				info.GroupBy = append(info.GroupBy, gbExpr.String())
				info.GroupByExprs = append(info.GroupByExprs, gbExpr)
				if p.peek() != TokenComma {
					break
				}
				p.advance() // consume comma
			}
		}
	}

	// HAVING
	if p.isKeyword(TokenKWHaving) {
		p.advance()
		havingExpr, err := p.parseExpr()
		if err != nil {
			return nil, fmt.Errorf("parsing HAVING: %w", err)
		}
		info.Having = havingExpr.String()
		info.HavingExpr = havingExpr
	}

	// QUALIFY (window function filter — Snowflake/BigQuery/Teradata extension)
	if p.peek() == TokenIdent && !p.cur.quoted && strings.EqualFold(p.cur.val, "QUALIFY") {
		p.advance()
		qualifyExpr, err := p.parseExpr()
		if err != nil {
			return nil, fmt.Errorf("parsing QUALIFY: %w", err)
		}
		info.Qualify = qualifyExpr.String()
		info.QualifyExpr = qualifyExpr
	}

	return info, nil
}

func (p *selectParser) parseSelectColumns() ([]SelectColumn, error) {
	var cols []SelectColumn
	for {
		col, err := p.parseSelectColumn()
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
		if p.peek() != TokenComma {
			break
		}
		p.advance() // consume comma
	}
	return cols, nil
}

func (p *selectParser) parseSelectColumn() (SelectColumn, error) {
	// Check for *
	if p.peek() == TokenStar {
		p.advance()
		return SelectColumn{Expr: "*", Star: true}, nil
	}

	// Check for table.* (ident DOT STAR)
	if p.peek() == TokenIdent {
		saved := p.cur
		savedPos := p.lex.pos
		savedStart := p.lex.start
		savedWidth := p.lex.width
		ident := p.advance()
		if p.peek() == TokenDot {
			p.advance() // consume dot
			if p.peek() == TokenStar {
				p.advance() // consume star
				return SelectColumn{
					Expr:     ident.val + ".*",
					Star:     true,
					TableRef: ident.val,
				}, nil
			}
			// Not table.*, restore state — but we already consumed, so put
			// ident.col back together below in the normal path.
			// Reset fully to re-parse as expression
			p.cur = saved
			p.lex.pos = savedPos
			p.lex.start = savedStart
			p.lex.width = savedWidth
			p.cur = saved
		} else {
			// Not a dot, restore and parse as expression
			p.cur = saved
			p.lex.pos = savedPos
			p.lex.start = savedStart
			p.lex.width = savedWidth
			p.cur = saved
		}
	}

	expr, err := p.parseExpr()
	if err != nil {
		return SelectColumn{}, err
	}

	col := SelectColumn{
		Expr:    expr.String(),
		ASTExpr: expr,
	}

	// Check if it's a window function
	if wfn, ok := expr.(*WindowFuncNode); ok {
		// Parse alias first (AS alias or implicit alias)
		alias := ""
		if p.isKeyword(TokenKWAs) {
			p.advance()
			aliasTok, err := p.expect(TokenIdent)
			if err != nil {
				return SelectColumn{}, fmt.Errorf("expected alias after AS")
			}
			alias = aliasTok.val
		} else if p.peek() == TokenIdent && !p.isFromKeyword() {
			alias = p.advance().val
		}
		col.Alias = alias
		col.IsWindow = true
		col.WindowSpec = windowSpecFromNode(wfn, alias)
		return col, nil
	}

	// Check if it's a column reference
	if ref, ok := expr.(*ColRef); ok {
		col.ColumnRef = ref.Column
		col.TableRef = ref.Table
	}

	// Check if it's an aggregate function (top-level or nested)
	if fn, ok := expr.(*FuncCallNode); ok && IsAggregate(fn.Name) {
		col.IsAgg = true
		col.AggFunc = strings.ToLower(fn.Name)
		col.AggDistinct = fn.Distinct
		if fn.Star {
			col.AggArg = "*"
		} else if len(fn.Args) > 0 {
			col.AggArg = fn.Args[0].String()
			col.AggArgExpr = fn.Args[0]
			col.AggArgs = fn.Args
		}
	} else if fn := FindNestedAggregate(expr); fn != nil {
		// Aggregate nested inside a binary expression (e.g., SUM(x) * 0.0001)
		col.IsAgg = true
		col.AggFunc = strings.ToLower(fn.Name)
		col.AggDistinct = fn.Distinct
		if fn.Star {
			col.AggArg = "*"
		} else if len(fn.Args) > 0 {
			col.AggArg = fn.Args[0].String()
			col.AggArgExpr = fn.Args[0]
			col.AggArgs = fn.Args
		}
	}

	// Check for AS alias
	if p.isKeyword(TokenKWAs) {
		p.advance()
		alias, ok := p.takeAliasAfterAs()
		if !ok {
			return SelectColumn{}, fmt.Errorf("expected alias after AS")
		}
		col.Alias = alias
	} else if p.peek() == TokenIdent && !p.isFromKeyword() {
		// Implicit alias (no AS keyword)
		col.Alias = p.advance().val
	}

	// Unaliased session information function: label the output column the way
	// PostgreSQL does (`current_user`, not `current_user()`).
	if col.Alias == "" {
		if label := sessionInfoLabel(expr); label != "" {
			col.Alias = label
		}
	}

	return col, nil
}

// takeAliasAfterAs consumes the alias in an explicit `AS <alias>` position
// and reports whether there was one.
//
// A KEYWORD is a legal alias there. That is PostgreSQL's own rule — writing
// AS is what removes the ambiguity that makes a word reserved — and it is not
// exotic: `COUNT(*) AS rows` and `COUNT(x) AS matched` both failed to parse
// with "expected alias after AS", and a BI tool naming an output column after
// a source column called `rows`, `value`, `key` or `end` hits it immediately.
//
// The keyword's original spelling is restored: the lexer uppercases val for
// comparison, so taking it verbatim would rename the user's column to
// MATCHED. Implicit aliases (no AS) are unchanged — there the reserved-word
// check is what stops the parser swallowing FROM.
func (p *selectParser) takeAliasAfterAs() (string, bool) {
	tok := p.cur
	if tok.typ != TokenIdent {
		if _, isKeyword := keywords[tok.val]; !isKeyword {
			return "", false
		}
	}
	p.advance()
	if tok.raw != "" {
		return tok.raw, true
	}
	return tok.val, true
}

// isFromKeyword returns true if the current token is a keyword that
// terminates a select column list.
func (p *selectParser) isFromKeyword() bool {
	switch p.cur.typ {
	case TokenKWFrom, TokenKWWhere, TokenKWGroup, TokenKWHaving,
		TokenKWOrder, TokenKWLimit, TokenKWUnion, TokenKWIntersect,
		TokenKWExcept, TokenKWJoin,
		TokenKWInner, TokenKWLeft, TokenKWRight, TokenKWFull,
		TokenKWCross, TokenKWOn, TokenKWOffset, TokenKWOver, TokenKWFetch,
		TokenKWNulls, TokenKWRows, TokenKWRange, TokenKWUnbounded,
		TokenKWPreceding, TokenKWFollowing, TokenKWCurrent, TokenKWRow:
		return true
	}
	if p.cur.typ == TokenIdent && !p.cur.quoted {
		upper := strings.ToUpper(p.cur.val)
		if upper == "QUALIFY" || upper == "LATERAL" {
			return true
		}
	}
	return false
}

func (p *selectParser) parseFromClause(info *SelectInfo) error {
	// Parse first table
	table, err := p.parseTableRef()
	if err != nil {
		return err
	}
	info.Tables = append(info.Tables, table)

	// Parse JOINs or comma-separated tables
	for {
		joinType := ""
		switch p.peek() {
		case TokenKWJoin:
			p.advance()
			joinType = "join"
		case TokenKWInner:
			p.advance()
			if _, err := p.expect(TokenKWJoin); err != nil {
				return fmt.Errorf("expected JOIN after INNER")
			}
			joinType = "join"
		case TokenKWLeft:
			p.advance()
			if p.isKeyword(TokenKWOuter) {
				p.advance()
			}
			if _, err := p.expect(TokenKWJoin); err != nil {
				return fmt.Errorf("expected JOIN after LEFT [OUTER]")
			}
			joinType = "left join"
		case TokenKWRight:
			p.advance()
			if p.isKeyword(TokenKWOuter) {
				p.advance()
			}
			if _, err := p.expect(TokenKWJoin); err != nil {
				return fmt.Errorf("expected JOIN after RIGHT [OUTER]")
			}
			joinType = "right join"
		case TokenKWFull:
			p.advance()
			if p.isKeyword(TokenKWOuter) {
				p.advance()
			}
			if _, err := p.expect(TokenKWJoin); err != nil {
				return fmt.Errorf("expected JOIN after FULL [OUTER]")
			}
			joinType = "full outer join"
		case TokenKWCross:
			p.advance()
			if _, err := p.expect(TokenKWJoin); err != nil {
				return fmt.Errorf("expected JOIN after CROSS")
			}
			joinType = "cross join"
		case TokenKWNatural:
			// Not implemented: the join keys are the columns the two sides
			// happen to share, which is a catalog question the parser cannot
			// answer. Say so here rather than leaving the clause for the
			// end-of-statement guard to report as stray input — and rather
			// than dropping it, which is what used to happen and answered
			// `FROM nation NATURAL JOIN region` as plain `FROM nation`
			// (#337). Same shape as JOIN ... USING: an error a client can
			// act on, not a wrong answer.
			return fmt.Errorf("NATURAL JOIN is not supported at position %d; write the join condition with ON", p.cur.pos)
		case TokenComma:
			// Cross join via comma
			p.advance()
			// Check for LATERAL after comma
			if p.peek() == TokenIdent && !p.cur.quoted && strings.EqualFold(p.cur.val, "LATERAL") {
				p.advance()
				rightTable, err := p.parseTableRef()
				if err != nil {
					return err
				}
				ji := JoinInfo{
					Type:          "cross join",
					RightTable:    rightTable.Name,
					RightAlias:    rightTable.Alias,
					RightTableRef: &rightTable,
					Lateral:       true,
				}
				if p.isKeyword(TokenKWOn) {
					p.advance()
					condExpr, err := p.parseExpr()
					if err != nil {
						return fmt.Errorf("parsing JOIN ON: %w", err)
					}
					ji.Condition = condExpr.String()
					ji.CondExpr = condExpr
				}
				info.Joins = append(info.Joins, ji)
				continue
			}
			table, err := p.parseTableRef()
			if err != nil {
				return err
			}
			info.Tables = append(info.Tables, table)
			continue
		default:
			return nil
		}

		// Check for LATERAL modifier
		lateral := false
		if p.peek() == TokenIdent && !p.cur.quoted && strings.EqualFold(p.cur.val, "LATERAL") {
			lateral = true
			p.advance()
		}

		// Parse the right side table
		rightTable, err := p.parseTableRef()
		if err != nil {
			return err
		}

		ji := JoinInfo{
			Type:          joinType,
			RightTable:    rightTable.Name,
			RightAlias:    rightTable.Alias,
			RightTableRef: &rightTable,
			Lateral:       lateral,
		}

		// Parse ON condition
		if p.isKeyword(TokenKWOn) {
			p.advance()
			condExpr, err := p.parseExpr()
			if err != nil {
				return fmt.Errorf("parsing JOIN ON: %w", err)
			}
			ji.Condition = condExpr.String()
			ji.CondExpr = condExpr
		}

		info.Joins = append(info.Joins, ji)
	}
}

func (p *selectParser) parseTableRef() (TableRef, error) {
	// Handle a VALUES row list as a table source: (VALUES (...), (...)) —
	// standard SQL's literal-rows table constructor. Peek past the '(' to
	// tell it from a subquery before consuming anything.
	if p.peek() == TokenLParen && p.peekN(1) == TokenKWValues {
		return p.parseValuesTableRef()
	}
	// Handle subquery: (SELECT ...)
	if p.peek() == TokenLParen {
		p.advance() // consume (
		subSQL := p.consumeBalancedParens()
		if _, err := p.expect(TokenRParen); err != nil {
			return TableRef{}, fmt.Errorf("expected ) after subquery")
		}
		tr := TableRef{Name: "(" + subSQL + ")"}
		// Alias is mandatory for derived tables
		if p.isKeyword(TokenKWAs) {
			p.advance()
		}
		if p.peek() == TokenIdent {
			tr.Alias = p.advance().val
		}
		if tr.Alias == "" {
			tr.Alias = tr.Name
		}
		return tr, nil
	}

	return p.parseTableRefTail()
}

// parseValuesTableRef parses a VALUES row list used as a table source —
// FROM (VALUES ('a'), ('b')) AS t(v) — and desugars it into the equivalent
// UNION ALL of single-row SELECTs, reusing the derived-table subquery path
// verbatim rather than teaching the planner a new node type. VALUES is
// standard SQL's sugar for exactly that: a row constructor list is the same
// answer as unioning one SELECT per row, and every engine downstream of the
// parser already knows how to run that.
//
// PostgreSQL's default column names are column1, column2, ...; an explicit
// AS alias(col, ...) list overrides them left to right, same as it does for
// a table function's column aliases.
func (p *selectParser) parseValuesTableRef() (TableRef, error) {
	p.advance() // consume (
	p.advance() // consume VALUES

	var rows [][]Node
	ncols := -1
	for {
		if _, err := p.expect(TokenLParen); err != nil {
			return TableRef{}, fmt.Errorf("expected ( to start a VALUES row")
		}
		var row []Node
		for {
			expr, err := p.parseExpr()
			if err != nil {
				return TableRef{}, fmt.Errorf("parsing VALUES row: %w", err)
			}
			row = append(row, expr)
			if p.peek() != TokenComma {
				break
			}
			p.advance() // consume ,
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return TableRef{}, fmt.Errorf("expected ) to close a VALUES row")
		}
		if ncols == -1 {
			ncols = len(row)
		} else if len(row) != ncols {
			return TableRef{}, fmt.Errorf("VALUES rows have differing column counts (%d vs %d)", len(row), ncols)
		}
		rows = append(rows, row)
		if p.peek() != TokenComma {
			break
		}
		p.advance() // consume , between rows
	}
	if _, err := p.expect(TokenRParen); err != nil {
		return TableRef{}, fmt.Errorf("expected ) after VALUES list")
	}

	// Alias and optional column alias list — same grammar parseTableFunction
	// accepts for AS alias(col1, col2, ...).
	alias := ""
	var colAliases []string
	if p.isKeyword(TokenKWAs) {
		p.advance()
		aliasTok, err := p.expect(TokenIdent)
		if err != nil {
			return TableRef{}, fmt.Errorf("expected alias after AS")
		}
		alias = aliasTok.val
		if p.peek() == TokenLParen {
			p.advance()
			for {
				colTok, err := p.expect(TokenIdent)
				if err != nil {
					return TableRef{}, fmt.Errorf("expected column alias")
				}
				colAliases = append(colAliases, colTok.val)
				if p.peek() != TokenComma {
					break
				}
				p.advance()
			}
			if _, err := p.expect(TokenRParen); err != nil {
				return TableRef{}, fmt.Errorf("expected ) after column aliases")
			}
		}
	} else if p.peek() == TokenIdent && !p.isJoinKeyword() {
		alias = p.advance().val
	}
	if len(colAliases) > ncols {
		return TableRef{}, fmt.Errorf("VALUES has %d columns, but %d column aliases were given", ncols, len(colAliases))
	}

	colNames := make([]string, ncols)
	for i := range colNames {
		colNames[i] = fmt.Sprintf("column%d", i+1)
	}
	for i, a := range colAliases {
		colNames[i] = a
	}

	var sb strings.Builder
	for i, row := range rows {
		if i > 0 {
			sb.WriteString(" UNION ALL ")
		}
		sb.WriteString("SELECT ")
		for j, expr := range row {
			if j > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(expr.String())
			sb.WriteString(" AS ")
			sb.WriteString(colNames[j])
		}
	}

	tr := TableRef{Name: "(" + sb.String() + ")", Alias: alias}
	if tr.Alias == "" {
		tr.Alias = tr.Name
	}
	return tr, nil
}

// parseTableRefTail parses everything after the initial "is this a VALUES
// list or a parenthesized subquery" dispatch in parseTableRef: a bare or
// qualified table name, a table function call, TABLESAMPLE, and the alias.
func (p *selectParser) parseTableRefTail() (TableRef, error) {
	nameTok, err := p.expect(TokenIdent)
	if err != nil {
		return TableRef{}, fmt.Errorf("expected table name")
	}

	// Check for table function syntax: name(args...)
	if p.peek() == TokenLParen {
		return p.parseTableFunction(nameTok.val)
	}

	// A qualified name: schema.table, or catalog.schema.table. PostgreSQL
	// clients write them constantly — DataGrip opens a table with
	// `SELECT ... FROM public.customer t` — and reading only the first
	// identifier made that a scan of a table named "public", which resolved
	// to nothing. The qualifier is kept rather than dropped so resolution can
	// reject one that names a schema this server does not have.
	name := nameTok.val
	qualifier := ""
	for p.peek() == TokenDot {
		p.advance() // consume .
		partTok, err := p.expect(TokenIdent)
		if err != nil {
			return TableRef{}, fmt.Errorf("expected name after %q.", name)
		}
		if qualifier == "" {
			qualifier = name
		} else {
			qualifier += "." + name
		}
		name = partTok.val
	}

	tr := TableRef{Name: name, Qualifier: qualifier, Alias: name}

	// Optional TABLESAMPLE method(percent)
	if p.peek() == TokenIdent && !p.cur.quoted && strings.EqualFold(p.cur.val, "TABLESAMPLE") {
		p.advance() // consume TABLESAMPLE
		methodTok, err := p.expect(TokenIdent)
		if err != nil {
			return TableRef{}, fmt.Errorf("expected TABLESAMPLE method (BERNOULLI or SYSTEM)")
		}
		tr.SampleMethod = strings.ToUpper(methodTok.val)
		if _, err := p.expect(TokenLParen); err != nil {
			return TableRef{}, fmt.Errorf("expected ( after TABLESAMPLE %s", tr.SampleMethod)
		}
		pctTok, err := p.expect(TokenNumber)
		if err != nil {
			return TableRef{}, fmt.Errorf("expected percentage in TABLESAMPLE")
		}
		tr.SamplePercent = pctTok.val
		if _, err := p.expect(TokenRParen); err != nil {
			return TableRef{}, fmt.Errorf("expected ) after TABLESAMPLE percentage")
		}
	}

	// Optional alias
	if p.isKeyword(TokenKWAs) {
		p.advance()
		aliasTok, err := p.expect(TokenIdent)
		if err != nil {
			return TableRef{}, fmt.Errorf("expected alias after AS")
		}
		tr.Alias = aliasTok.val
	} else if p.peek() == TokenIdent && !p.isJoinKeyword() {
		tr.Alias = p.advance().val
	}
	return tr, nil
}

// parseTableFunction parses a table function call: name(arg1, key=val, ...) [AS alias]
// Supports both positional arguments and named parameters (key=value).
func (p *selectParser) parseTableFunction(name string) (TableRef, error) {
	p.advance() // consume (

	var args []string
	namedArgs := make(map[string]string)
	argCount := 0
	for p.peek() != TokenRParen && p.peek() != TokenEOF {
		if argCount > 0 {
			if _, err := p.expect(TokenComma); err != nil {
				return TableRef{}, fmt.Errorf("expected , between function arguments")
			}
		}
		argCount++

		tok := p.cur
		switch tok.typ {
		case TokenIdent:
			// Advance past ident, then check if next is = (named param) or . (qualified name)
			p.advance()
			if p.cur.typ == TokenEq {
				key := tok.val
				p.advance() // consume =
				namedArgs[key] = p.cur.val
				p.advance() // consume value
			} else {
				argVal := tok.val
				// Handle dot-qualified names: t.col
				for p.cur.typ == TokenDot {
					p.advance() // consume .
					argVal += "." + p.cur.val
					p.advance() // consume name after dot
				}
				args = append(args, argVal)
			}
		case TokenString, TokenNumber:
			args = append(args, tok.val)
			p.advance()
		default:
			args = append(args, tok.val)
			p.advance()
		}
	}

	if _, err := p.expect(TokenRParen); err != nil {
		return TableRef{}, fmt.Errorf("expected ) after function arguments")
	}

	tr := TableRef{
		Name:          name,
		Alias:         name,
		IsFunction:    true,
		FuncArgs:      args,
		FuncNamedArgs: namedArgs,
	}

	// Optional WITH ORDINALITY (for UNNEST)
	if p.peek() == TokenKWWith {
		saved := p.cur
		savedPos := p.lex.pos
		savedStart := p.lex.start
		savedWidth := p.lex.width
		p.advance() // consume WITH
		if p.peek() == TokenIdent && !p.cur.quoted && strings.EqualFold(p.cur.val, "ORDINALITY") {
			p.advance() // consume ORDINALITY
			tr.WithOrdinality = true
		} else {
			// Not WITH ORDINALITY, restore
			p.cur = saved
			p.lex.pos = savedPos
			p.lex.start = savedStart
			p.lex.width = savedWidth
		}
	}

	// Optional alias with column list: AS alias(col1, col2)
	if p.isKeyword(TokenKWAs) {
		p.advance()
		aliasTok, err := p.expect(TokenIdent)
		if err != nil {
			return TableRef{}, fmt.Errorf("expected alias after AS")
		}
		tr.Alias = aliasTok.val
		// Optional column alias list
		if p.peek() == TokenLParen {
			p.advance() // consume (
			for {
				colTok, err := p.expect(TokenIdent)
				if err != nil {
					return TableRef{}, fmt.Errorf("expected column alias")
				}
				tr.ColumnAliases = append(tr.ColumnAliases, colTok.val)
				if p.peek() != TokenComma {
					break
				}
				p.advance() // consume ,
			}
			if _, err := p.expect(TokenRParen); err != nil {
				return TableRef{}, fmt.Errorf("expected ) after column aliases")
			}
		}
	} else if p.peek() == TokenIdent && !p.isJoinKeyword() {
		tr.Alias = p.advance().val
	}
	return tr, nil
}

// isJoinKeyword returns true if the current token is a join/clause keyword.
func (p *selectParser) isJoinKeyword() bool {
	switch p.cur.typ {
	case TokenKWJoin, TokenKWInner, TokenKWLeft, TokenKWRight, TokenKWFull,
		TokenKWCross, TokenKWOn, TokenKWWhere, TokenKWGroup, TokenKWHaving,
		TokenKWOrder, TokenKWLimit, TokenKWUnion, TokenKWIntersect,
		TokenKWExcept, TokenKWOffset, TokenKWFetch,
		TokenKWNulls, TokenKWRows, TokenKWRange:
		return true
	}
	if p.cur.typ == TokenIdent && !p.cur.quoted {
		upper := strings.ToUpper(p.cur.val)
		if upper == "QUALIFY" || upper == "LATERAL" {
			return true
		}
	}
	return false
}

// consumeBalancedParens reads tokens inside parentheses and returns the raw SQL.
// It assumes the opening '(' has already been consumed.
// It reads until the matching ')' is found (not consumed).
func (p *selectParser) consumeBalancedParens() string {
	start := p.cur.pos
	depth := 1
	for depth > 0 && p.cur.typ != TokenEOF {
		switch p.cur.typ {
		case TokenLParen:
			depth++
		case TokenRParen:
			depth--
			if depth == 0 {
				end := p.cur.pos
				return strings.TrimSpace(p.lex.input[start:end])
			}
		}
		p.advance()
	}
	return strings.TrimSpace(p.lex.input[start:p.cur.pos])
}

func (p *selectParser) parseOrderByList() ([]OrderByItem, error) {
	var items []OrderByItem
	for {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, fmt.Errorf("parsing ORDER BY: %w", err)
		}
		item := OrderByItem{Column: expr.String(), Expr: expr}
		if p.isKeyword(TokenKWDesc) {
			p.advance()
			item.Desc = true
		} else if p.isKeyword(TokenKWAsc) {
			p.advance()
		}
		if p.isKeyword(TokenKWNulls) {
			p.advance()
			if p.isKeyword(TokenKWFirst) {
				p.advance()
				v := true
				item.NullsFirst = &v
			} else if p.isKeyword(TokenKWLast) {
				p.advance()
				v := false
				item.NullsFirst = &v
			} else {
				return nil, fmt.Errorf("expected FIRST or LAST after NULLS")
			}
		}
		items = append(items, item)
		if p.peek() != TokenComma {
			break
		}
		p.advance() // consume comma
	}
	return items, nil
}

// --- Expression parser (precedence climbing) ---

// Precedence levels (low to high):
// 1: OR
// 2: AND
// 3: NOT
// 4: IS, comparison (=, !=, <, <=, >, >=), IN, BETWEEN, LIKE
// 5: addition (+, -, ||)
// 6: multiplication (*, /, %)
// 7: unary (-, +)
// 8: primary (literal, column, function, paren, case, cast, exists)

func (p *selectParser) parseExpr() (Node, error) {
	return p.parseOr()
}

func (p *selectParser) parseOr() (Node, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.isKeyword(TokenKWOr) {
		p.advance()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &OrNode{Left: left, Right: right}
	}
	return left, nil
}

func (p *selectParser) parseAnd() (Node, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.isKeyword(TokenKWAnd) {
		p.advance()
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = &AndNode{Left: left, Right: right}
	}
	return left, nil
}

func (p *selectParser) parseNot() (Node, error) {
	if p.isKeyword(TokenKWNot) {
		p.advance()
		// Check for NOT EXISTS
		if p.isKeyword(TokenKWExists) {
			return p.parseExists(true)
		}
		inner, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &NotNode{Inner: inner}, nil
	}
	// Check for EXISTS
	if p.isKeyword(TokenKWExists) {
		return p.parseExists(false)
	}
	return p.parseComparison()
}

func (p *selectParser) parseExists(not bool) (Node, error) {
	p.advance() // consume EXISTS
	if _, err := p.expect(TokenLParen); err != nil {
		return nil, fmt.Errorf("expected ( after EXISTS")
	}
	// Capture the subquery SQL
	subSQL := p.consumeBalancedParens()
	if _, err := p.expect(TokenRParen); err != nil {
		return nil, fmt.Errorf("expected ) after EXISTS subquery")
	}
	return &ExistsNode{Not: not, SQL: subSQL}, nil
}

func (p *selectParser) parseComparison() (Node, error) {
	left, err := p.parseAddition()
	if err != nil {
		return nil, err
	}

	// IS [NOT] NULL / IS [NOT] TRUE / IS [NOT] FALSE / IS [NOT] DISTINCT FROM
	if p.isKeyword(TokenKWIs) {
		p.advance()
		not := false
		if p.isKeyword(TokenKWNot) {
			p.advance()
			not = true
		}
		switch p.peek() {
		case TokenKWNull:
			p.advance()
			return &IsExpr{Left: left, Not: not, Check: "null"}, nil
		case TokenKWTrue:
			p.advance()
			return &IsExpr{Left: left, Not: not, Check: "true"}, nil
		case TokenKWFalse:
			p.advance()
			return &IsExpr{Left: left, Not: not, Check: "false"}, nil
		case TokenKWDistinct:
			// IS [NOT] DISTINCT FROM: SQL's NULL-safe (in)equality — the
			// only correct way to write "these differ, counting NULL as a
			// value" (#374). NULL IS DISTINCT FROM NULL is FALSE, never
			// NULL/UNKNOWN, which is why this compiles to a comparison
			// rather than reusing IsExpr's NULL/TRUE/FALSE check shape.
			p.advance() // consume DISTINCT
			if _, err := p.expect(TokenKWFrom); err != nil {
				return nil, fmt.Errorf("expected FROM after IS [NOT] DISTINCT")
			}
			right, err := p.parseAddition()
			if err != nil {
				return nil, fmt.Errorf("parsing IS [NOT] DISTINCT FROM: %w", err)
			}
			op := "is distinct from"
			if not {
				op = "is not distinct from"
			}
			return &CmpExpr{Left: left, Op: op, Right: right}, nil
		default:
			return nil, fmt.Errorf("expected NULL, TRUE, FALSE, or DISTINCT FROM after IS [NOT]")
		}
	}

	// [NOT] IN
	not := false
	if p.isKeyword(TokenKWNot) {
		// Peek ahead to see if this is NOT IN, NOT BETWEEN, or NOT LIKE
		saved := p.cur
		savedPos := p.lex.pos
		savedStart := p.lex.start
		savedWidth := p.lex.width
		p.advance() // consume NOT
		switch p.peek() {
		case TokenKWIn:
			not = true
			// fall through to IN handling below
		case TokenKWBetween:
			not = true
			// fall through to BETWEEN handling below
		case TokenKWLike:
			not = true
			// fall through to LIKE handling below
		case TokenKWILike:
			not = true
			// fall through to ILIKE handling below
		default:
			// Check for NOT SIMILAR TO
			if p.peek() == TokenIdent && !p.cur.quoted && strings.EqualFold(p.cur.val, "SIMILAR") {
				not = true
				break // fall through to SIMILAR TO handling below
			}
			// Not a NOT IN/BETWEEN/LIKE/SIMILAR — restore and return left
			p.cur = saved
			p.lex.pos = savedPos
			p.lex.start = savedStart
			p.lex.width = savedWidth
			return left, nil
		}
	}

	if p.isKeyword(TokenKWIn) {
		p.advance()
		if _, err := p.expect(TokenLParen); err != nil {
			return nil, fmt.Errorf("expected ( after IN")
		}
		// Check if it's a subquery
		if p.isKeyword(TokenKWSelect) {
			subSQL := p.consumeBalancedParens()
			if _, err := p.expect(TokenRParen); err != nil {
				return nil, fmt.Errorf("expected ) after IN subquery")
			}
			return &InExpr{Left: left, Not: not, Values: []Node{&SubqueryNode{SQL: subSQL}}}, nil
		}
		// Value list
		var values []Node
		for {
			val, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			values = append(values, val)
			if p.peek() != TokenComma {
				break
			}
			p.advance()
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, fmt.Errorf("expected ) after IN list")
		}
		return &InExpr{Left: left, Not: not, Values: values}, nil
	}

	if p.isKeyword(TokenKWBetween) {
		p.advance()
		low, err := p.parseAddition()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenKWAnd); err != nil {
			return nil, fmt.Errorf("expected AND in BETWEEN")
		}
		high, err := p.parseAddition()
		if err != nil {
			return nil, err
		}
		return &BetweenExpr{Left: left, Not: not, Low: low, High: high}, nil
	}

	if p.isKeyword(TokenKWLike) {
		p.advance()
		pattern, err := p.parseAddition()
		if err != nil {
			return nil, err
		}
		return &LikeExpr{Left: left, Not: not, Pattern: pattern}, nil
	}

	// ILIKE — case-insensitive LIKE, rewritten to LOWER(left) LIKE LOWER(pattern)
	if p.isKeyword(TokenKWILike) {
		p.advance()
		pattern, err := p.parseAddition()
		if err != nil {
			return nil, err
		}
		return &LikeExpr{
			Left:    &FuncCallNode{Name: "lower", Args: []Node{left}},
			Not:     not,
			Pattern: &FuncCallNode{Name: "lower", Args: []Node{pattern}},
		}, nil
	}

	// SIMILAR TO — rewrite to regexp_like(left, pattern)
	if p.peek() == TokenIdent && !p.cur.quoted && strings.EqualFold(p.cur.val, "SIMILAR") {
		savedSim := p.cur
		savedSimPos := p.lex.pos
		savedSimStart := p.lex.start
		savedSimWidth := p.lex.width
		p.advance() // consume SIMILAR
		if (p.peek() == TokenIdent || p.peek() == TokenKWTo) && strings.EqualFold(p.cur.val, "TO") {
			p.advance() // consume TO
			pattern, err := p.parseAddition()
			if err != nil {
				return nil, err
			}
			var node Node = &FuncCallNode{Name: "regexp_like", Args: []Node{left, pattern}}
			if not {
				node = &NotNode{Inner: node}
			}
			return node, nil
		}
		// Not "SIMILAR TO", restore
		p.cur = savedSim
		p.lex.pos = savedSimPos
		p.lex.start = savedSimStart
		p.lex.width = savedSimWidth
	}

	// Comparison operators
	var op string
	switch p.peek() {
	case TokenEq:
		op = "="
		p.advance()
	case TokenNotEq:
		op = "!="
		p.advance()
	case TokenLT:
		op = "<"
		p.advance()
	case TokenLTEq:
		op = "<="
		p.advance()
	case TokenGT:
		op = ">"
		p.advance()
	case TokenGTEq:
		op = ">="
		p.advance()
	default:
		return left, nil
	}

	// Check for ANY/ALL/SOME modifier: expr op ANY(...) / ALL(...) / SOME(...)
	if p.peek() == TokenIdent || p.peek() == TokenKWAll {
		upper := strings.ToUpper(p.cur.val)
		if upper == "ANY" || upper == "ALL" || upper == "SOME" {
			p.advance() // consume ANY/ALL/SOME
			if _, err := p.expect(TokenLParen); err != nil {
				return nil, fmt.Errorf("expected ( after %s", upper)
			}
			// Check for subquery
			if p.isKeyword(TokenKWSelect) {
				subSQL := p.consumeBalancedParens()
				if _, err := p.expect(TokenRParen); err != nil {
					return nil, fmt.Errorf("expected ) after %s subquery", upper)
				}
				return &AnyAllExpr{Left: left, Op: op, Modifier: upper, Values: []Node{&SubqueryNode{SQL: subSQL}}}, nil
			}
			// Value list
			var values []Node
			for {
				val, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				values = append(values, val)
				if p.peek() != TokenComma {
					break
				}
				p.advance()
			}
			if _, err := p.expect(TokenRParen); err != nil {
				return nil, fmt.Errorf("expected ) after %s value list", upper)
			}
			return &AnyAllExpr{Left: left, Op: op, Modifier: upper, Values: values}, nil
		}
	}

	right, err := p.parseAddition()
	if err != nil {
		return nil, err
	}
	return &CmpExpr{Left: left, Op: op, Right: right}, nil
}

func (p *selectParser) parseAddition() (Node, error) {
	left, err := p.parseMultiplication()
	if err != nil {
		return nil, err
	}
	for {
		var op string
		switch p.peek() {
		case TokenPlus:
			op = "+"
		case TokenMinus:
			op = "-"
		case TokenConcat:
			op = "||"
		default:
			return left, nil
		}
		p.advance()
		right, err := p.parseMultiplication()
		if err != nil {
			return nil, err
		}
		left = &BinaryOp{Left: left, Op: op, Right: right}
	}
}

func (p *selectParser) parseMultiplication() (Node, error) {
	left, err := p.parseAtTimeZone()
	if err != nil {
		return nil, err
	}
	for {
		var op string
		switch p.peek() {
		case TokenStar:
			op = "*"
		case TokenSlash:
			op = "/"
		case TokenPercent:
			op = "%"
		default:
			return left, nil
		}
		p.advance()
		right, err := p.parseAtTimeZone()
		if err != nil {
			return nil, err
		}
		left = &BinaryOp{Left: left, Op: op, Right: right}
	}
}

// parseAtTimeZone parses the infix `<expr> AT TIME ZONE <zone>` operator and
// rewrites it to PostgreSQL's own canonical form, the function call
// `timezone(<zone>, <expr>)` — zone first, exactly as PostgreSQL's parser
// does. Engine semantics (which of the operator's two directions this
// implements, and which zones it accepts) live with fnTimezone in
// internal/engine/expr.
//
// Precedence mirrors PostgreSQL's grammar, where `%left AT` sits above the
// multiplicative operators and below unary minus. So this level sits between
// parseMultiplication and parseUnary, which gives:
//
//	a * ts AT TIME ZONE z   →  a * (ts AT TIME ZONE z)      (AT binds tighter than *)
//	ts AT TIME ZONE z + i   →  (ts AT TIME ZONE z) + i      (and tighter than +)
//	ts AT TIME ZONE z = x   →  (ts AT TIME ZONE z) = x      (and tighter than =)
//	-ts AT TIME ZONE z      →  (-ts) AT TIME ZONE z         (looser than unary minus)
//	ts AT TIME ZONE 'a'::text → zone is the whole cast      (looser than ::)
//
// The operator is left associative, so the zone operand is parsed one level
// tighter (parseUnary) and a chain groups as `(ts AT TIME ZONE a) AT TIME
// ZONE b`.
func (p *selectParser) parseAtTimeZone() (Node, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for p.matchAtTimeZone() {
		zone, err := p.parseUnary()
		if err != nil {
			return nil, fmt.Errorf("expected time zone after AT TIME ZONE: %w", err)
		}
		if lit, ok := zone.(*Lit); ok && lit.Kind == LitString && !isUTCZoneName(lit.Value) {
			return nil, fmt.Errorf("AT TIME ZONE: only UTC is supported, got %q", lit.Value)
		}
		left = &FuncCallNode{Name: "timezone", Args: []Node{zone, left}}
	}
	return left, nil
}

// isUTCZoneName reports whether a zone name is one of the spellings of UTC.
// A written-out zone this engine cannot convert correctly is rejected here,
// where the operator is spelled and the zone can be named in the message,
// because a planner-stage expression-compile error is not user-visible: both
// the projection and the filter builder fall back to a column reference when
// compilation fails. A zone that is not a literal (a column, a parameter) is
// only knowable at run time and comes back NULL instead.
//
// Kept in step with isUTCZone in internal/engine/expr, which backs the
// timezone() call this rewrites to and carries the reasoning for the
// restriction. That package imports this one, so the list cannot be shared.
func isUTCZoneName(zone string) bool {
	switch strings.ToUpper(strings.TrimSpace(zone)) {
	case "UTC", "GMT", "Z", "ETC/UTC", "ETC/GMT":
		return true
	}
	return false
}

// matchAtTimeZone consumes the AT TIME ZONE word triple when the next three
// tokens are exactly that, and reports whether it did. None of the three is a
// reserved word in this lexer, so a partial match must consume nothing:
// `SELECT ts at` is a column aliased `at`, and `SELECT ts at time` is that
// alias followed by a syntax error, not a half-parsed operator.
func (p *selectParser) matchAtTimeZone() bool {
	if !p.isBareWord(0, "AT") || !p.isBareWord(1, "TIME") || !p.isBareWord(2, "ZONE") {
		return false
	}
	p.advance() // AT
	p.advance() // TIME
	p.advance() // ZONE
	return true
}

func (p *selectParser) parseUnary() (Node, error) {
	switch p.peek() {
	case TokenMinus:
		p.advance()
		inner, err := p.parsePostfix()
		if err != nil {
			return nil, err
		}
		return &UnaryOp{Op: "-", Inner: inner}, nil
	case TokenPlus:
		p.advance()
		return p.parsePostfix()
	}
	return p.parsePostfix()
}

// jsonPathArg converts a -> / ->> right-hand side to a json_extract path argument.
// Integer keys become "$[N]" (array index), string/ident keys become "$.key" (object field).
func jsonPathArg(node Node) Node {
	switch n := node.(type) {
	case *Lit:
		if n.Kind == LitString {
			return &Lit{Value: "$." + n.Value, Kind: LitString}
		}
		if n.Kind == LitNumber {
			return &Lit{Value: "$[" + n.Value + "]", Kind: LitString}
		}
		return node
	case *ColRef:
		// Unquoted identifier treated as field name
		name := n.Column
		if n.Table != "" {
			name = n.Table + "." + name
		}
		return &Lit{Value: "$." + name, Kind: LitString}
	default:
		return node
	}
}

// parsePostfix handles :: type cast and subscript access (left to right).
func (p *selectParser) parsePostfix() (Node, error) {
	expr, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	for {
		switch p.peek() {
		case TokenDoubleColon:
			// PostgreSQL-style type cast: expr::type
			p.advance() // consume ::
			typeTok, err := p.expect(TokenIdent)
			if err != nil {
				return nil, fmt.Errorf("expected type name after ::")
			}
			typeName := strings.ToLower(typeTok.val)
			typeName = p.maybeExtendTwoWordType(typeName)
			// Optional precision: ::decimal(10,2)
			if p.peek() == TokenLParen {
				typeName += p.consumeTypeParams()
			}
			expr = &CastNode{Inner: expr, TypeName: typeName}
		case TokenLBracket:
			// Subscript: expr[index] → element_at(expr, index)
			p.advance() // consume [
			index, err := p.parseExpr()
			if err != nil {
				return nil, fmt.Errorf("expected expression inside []")
			}
			if _, err := p.expect(TokenRBracket); err != nil {
				return nil, fmt.Errorf("expected ] after subscript")
			}
			expr = &FuncCallNode{
				Name: "element_at",
				Args: []Node{expr, index},
			}
		case TokenJSONArrow:
			// JSON field access: expr -> key → json_extract(expr, key)
			p.advance()
			right, err := p.parsePrimary()
			if err != nil {
				return nil, fmt.Errorf("expected expression after ->")
			}
			expr = &FuncCallNode{
				Name: "json_extract",
				Args: []Node{expr, jsonPathArg(right)},
			}
		case TokenJSONDoubleArrow:
			// JSON text access: expr ->> key → json_extract_scalar(expr, key)
			p.advance()
			right, err := p.parsePrimary()
			if err != nil {
				return nil, fmt.Errorf("expected expression after ->>")
			}
			expr = &FuncCallNode{
				Name: "json_extract_scalar",
				Args: []Node{expr, jsonPathArg(right)},
			}
		default:
			return expr, nil
		}
	}
}

func (p *selectParser) parsePrimary() (Node, error) {
	switch p.peek() {
	case TokenNumber:
		tok := p.advance()
		return &Lit{Value: tok.val, Kind: LitNumber}, nil

	case TokenString:
		tok := p.advance()
		return &Lit{Value: tok.val, Kind: LitString}, nil

	case TokenKWNull:
		p.advance()
		return &Lit{Value: "null", Kind: LitNull}, nil

	case TokenKWTrue:
		p.advance()
		return &Lit{Value: "true", Kind: LitBool}, nil

	case TokenKWFalse:
		p.advance()
		return &Lit{Value: "false", Kind: LitBool}, nil

	case TokenStar:
		p.advance()
		return &StarNode{}, nil

	case TokenKWCase:
		return p.parseCaseExpr()

	case TokenKWEvery:
		// EVERY is a lexer keyword only for CREATE ALERT's schedule clause.
		// In an expression, EVERY(...) is SQL's spelling of the BOOL_AND
		// aggregate — the planner already maps "every", but the keyword
		// token never reached the function-call path, so the spelling
		// failed to parse at all (#371).
		if p.peekN(1) == TokenLParen {
			p.advance() // consume EVERY
			return p.parseFuncCall("every")
		}
		return nil, fmt.Errorf("unexpected token %q at position %d", p.cur.val, p.cur.pos)

	case TokenKWCast:
		return p.parseCastExpr()

	case TokenLParen:
		p.advance()
		// Check for subquery
		if p.isKeyword(TokenKWSelect) {
			subSQL := p.consumeBalancedParens()
			if _, err := p.expect(TokenRParen); err != nil {
				return nil, fmt.Errorf("expected ) after subquery")
			}
			return &SubqueryNode{SQL: subSQL}, nil
		}
		inner, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		// Check for tuple: (a, b, c)
		if p.peek() == TokenComma {
			elements := []Node{inner}
			for p.peek() == TokenComma {
				p.advance() // consume ,
				elem, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				elements = append(elements, elem)
			}
			if _, err := p.expect(TokenRParen); err != nil {
				return nil, fmt.Errorf("expected ) after tuple")
			}
			return &TupleNode{Elements: elements}, nil
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, fmt.Errorf("expected )")
		}
		return &ParenNode{Inner: inner}, nil

	case TokenIdent:
		// A double-quoted identifier is always a plain name: the special
		// spellings below (and any keyword spelling) are not recognised
		// inside quotes.
		if p.cur.quoted {
			return p.parseIdentExpr()
		}

		upper := strings.ToUpper(p.cur.val)

		// Niladic SQL functions: standard SQL spells these without
		// parentheses (CURRENT_DATE, CURRENT_USER, ...). A following '('
		// means the client wrote the parenthesized spelling — let the normal
		// function-call path consume it — and a following '.' means the name
		// is a qualifier (an alias spelled `user`, say), not the function.
		if isNiladicFunc(upper) && p.peekN(1) != TokenLParen && p.peekN(1) != TokenDot {
			p.advance()
			return &FuncCallNode{Name: strings.ToLower(upper)}, nil
		}

		// INTERVAL 'N days' or INTERVAL 'N' DAY
		if upper == "INTERVAL" {
			return p.parseIntervalLiteral()
		}

		// Typed literals: DATE '1992-01-01', TIMESTAMP '1992-01-01 10:00',
		// TIME '10:00:00'. Standard SQL, and the spelling analysts and BI
		// tools reach for first — DataGrip's generated date-range filters use
		// it. Without this the type name parsed as a column reference and the
		// query failed with `unknown column "DATE"`.
		//
		// It means exactly what CAST('...' AS <type>) means, so it lowers to
		// the same node rather than introducing a second representation. A
		// following string literal is what distinguishes the literal from a
		// column that happens to be named date or time, which still parses as
		// a plain identifier.
		if isTypedLiteralType(upper) && p.peekN(1) == TokenString {
			p.advance() // type name
			lit := p.advance()
			return &CastNode{
				Inner: &Lit{Value: lit.val, Kind: LitString},
				// Lowercase, matching parseCastExpr — the printed form has to
				// re-parse to itself, which the fuzz round-trip checks.
				TypeName: strings.ToLower(upper),
			}, nil
		}

		// EXTRACT(field FROM expr) — rewrite to field(expr) function call
		if upper == "EXTRACT" {
			return p.parseExtractExpr()
		}

		// TRIM with optional LEADING/TRAILING/BOTH syntax
		if upper == "TRIM" && p.peekN(1) == TokenLParen {
			return p.parseTrimExpr()
		}

		// POSITION(needle IN haystack) — the standard spelling of strpos
		// (#374). The IN inside the call is this operator's own grammar,
		// not set membership, so it cannot fall through to the generic
		// function-call path: that reads a plain argument list and hands
		// "needle" to the expression parser, which then reads IN as
		// membership and expects "(" for a value list or subquery — the
		// "expected ( after IN" error this rewrite exists to avoid.
		if upper == "POSITION" && p.peekN(1) == TokenLParen {
			return p.parsePositionExpr()
		}

		// ARRAY[...] literal
		if upper == "ARRAY" && p.peekN(1) == TokenLBracket {
			p.advance() // consume ARRAY
			return p.parseArrayLiteral()
		}
		return p.parseIdentExpr()

	default:
		return nil, fmt.Errorf("unexpected token %q at position %d", p.cur.val, p.cur.pos)
	}
}

// niladicFuncs are the SQL functions spelled without an argument list.
// PostgreSQL reserves every one of these names, so a bare occurrence in an
// expression position is the function and never a column reference. The
// double-quoted spelling ("user", "current_schema") is a plain column
// reference — the quoted branch in parsePrimary returns before this check.
var niladicFuncs = map[string]bool{
	"CURRENT_DATE":      true,
	"CURRENT_TIME":      true,
	"CURRENT_TIMESTAMP": true,
	"CURRENT_USER":      true,
	"SESSION_USER":      true,
	"USER":              true,
	"CURRENT_ROLE":      true,
	"CURRENT_CATALOG":   true,
	"CURRENT_SCHEMA":    true,
}

func isNiladicFunc(upper string) bool { return niladicFuncs[upper] }

// sessionInfoFuncs are the session/catalog information functions whose result
// column PostgreSQL labels with the bare function name: `SELECT current_user`
// reports the column as `current_user`, not `current_user()`. JDBC and BI
// clients read these results by label, so the label has to match.
var sessionInfoFuncs = map[string]bool{
	"current_user":     true,
	"session_user":     true,
	"user":             true,
	"current_role":     true,
	"current_catalog":  true,
	"current_schema":   true,
	"current_schemas":  true,
	"current_database": true,
	"version":          true,
}

// sessionInfoLabel returns the PostgreSQL output column name for a session
// information function call, or "" when expr is not one.
func sessionInfoLabel(expr Node) string {
	fn, ok := expr.(*FuncCallNode)
	if !ok || fn.Star || fn.Distinct {
		return ""
	}
	if !sessionInfoFuncs[fn.Name] {
		return ""
	}
	return fn.Name
}

func (p *selectParser) parseIdentExpr() (Node, error) {
	name := p.advance() // consume the identifier

	// Check for function call: ident(
	if p.peek() == TokenLParen {
		return p.parseFuncCall(name.val)
	}

	// Check for table.column or table.*
	if p.peek() == TokenDot {
		p.advance() // consume dot
		if p.peek() == TokenStar {
			p.advance()
			return &StarNode{Table: name.val}, nil
		}
		colTok, err := p.expect(TokenIdent)
		if err != nil {
			return nil, fmt.Errorf("expected column name after '.'")
		}
		col := &ColRef{Table: name.val, Column: colTok.val}
		// Check for table.column.func() — not supported, but just return the ref
		return col, nil
	}

	return &ColRef{Column: name.val}, nil
}

func (p *selectParser) parseFuncCall(name string) (Node, error) {
	p.advance() // consume (

	fn := &FuncCallNode{Name: strings.ToLower(name)}

	// Check for COUNT(*)
	if p.peek() == TokenStar {
		p.advance()
		fn.Star = true
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, fmt.Errorf("expected ) after *")
		}
		return p.maybeParseOver(fn)
	}

	// Check for empty arg list
	if p.peek() == TokenRParen {
		p.advance()
		return p.maybeParseOver(fn)
	}

	// Check for DISTINCT
	if p.isKeyword(TokenKWDistinct) {
		p.advance()
		fn.Distinct = true
	}

	// Parse arguments
	for {
		arg, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		fn.Args = append(fn.Args, arg)
		if p.peek() != TokenComma {
			break
		}
		p.advance()
	}

	if _, err := p.expect(TokenRParen); err != nil {
		return nil, fmt.Errorf("expected ) after function arguments")
	}

	return p.maybeParseOver(fn)
}

// maybeParseFilterAndOver checks for FILTER (WHERE ...) and OVER (...) after
// a function call. FILTER is rewritten to CASE WHEN at the AST level:
//
//	COUNT(*) FILTER (WHERE cond) → SUM(CASE WHEN cond THEN 1 ELSE 0 END)
//	AGG(expr) FILTER (WHERE cond) → AGG(CASE WHEN cond THEN expr END)
func (p *selectParser) maybeParseOver(fn *FuncCallNode) (Node, error) {
	// Check for FILTER (WHERE ...) — standard SQL aggregate filtering
	if p.peek() == TokenIdent && !p.cur.quoted && strings.EqualFold(p.cur.val, "FILTER") {
		rewritten, err := p.parseAggFilter(fn)
		if err != nil {
			return nil, err
		}
		fn = rewritten
	}

	if !p.isKeyword(TokenKWOver) {
		return fn, nil
	}
	return p.parseWindowFunc(fn)
}

// parseAggFilter parses FILTER (WHERE condition) after an aggregate function
// and rewrites it to the equivalent CASE WHEN expression.
func (p *selectParser) parseAggFilter(fn *FuncCallNode) (*FuncCallNode, error) {
	p.advance() // consume FILTER

	if _, err := p.expect(TokenLParen); err != nil {
		return nil, fmt.Errorf("expected ( after FILTER")
	}
	if !p.isKeyword(TokenKWWhere) {
		return nil, fmt.Errorf("expected WHERE after FILTER(")
	}
	p.advance() // consume WHERE

	cond, err := p.parseExpr()
	if err != nil {
		return nil, fmt.Errorf("FILTER WHERE condition: %w", err)
	}

	if _, err := p.expect(TokenRParen); err != nil {
		return nil, fmt.Errorf("expected ) after FILTER(WHERE ...)")
	}

	// Rewrite: COUNT(*) FILTER (WHERE cond) → SUM(CASE WHEN cond THEN 1 ELSE 0 END)
	if fn.Star && strings.ToLower(fn.Name) == "count" {
		return &FuncCallNode{
			Name: "sum",
			Args: []Node{
				&CaseNode{
					Whens: []WhenClause{{Cond: cond, Result: &Lit{Value: "1", Kind: LitNumber}}},
					Else:  &Lit{Value: "0", Kind: LitNumber},
				},
			},
		}, nil
	}

	// Rewrite: AGG(expr) FILTER (WHERE cond) → AGG(CASE WHEN cond THEN expr END)
	// NULL values from non-matching rows are ignored by all standard aggregates.
	if len(fn.Args) >= 1 {
		return &FuncCallNode{
			Name:     fn.Name,
			Distinct: fn.Distinct,
			Args: []Node{
				&CaseNode{
					Whens: []WhenClause{{Cond: cond, Result: fn.Args[0]}},
					Else:  nil, // NULL → excluded by aggregates
				},
			},
		}, nil
	}

	return nil, fmt.Errorf("FILTER clause on %s() with no arguments", fn.Name)
}

// parseWindowFunc parses OVER (...) after a function call.
func (p *selectParser) parseWindowFunc(fn *FuncCallNode) (Node, error) {
	p.advance() // consume OVER

	if _, err := p.expect(TokenLParen); err != nil {
		return nil, fmt.Errorf("expected '(' after OVER")
	}

	wfn := &WindowFuncNode{Func: fn}

	// PARTITION BY
	if p.isKeyword(TokenKWPartition) {
		p.advance() // consume PARTITION
		if _, err := p.expect(TokenKWBy); err != nil {
			return nil, fmt.Errorf("expected BY after PARTITION")
		}
		for {
			expr, err := p.parseExpr()
			if err != nil {
				return nil, fmt.Errorf("parsing PARTITION BY: %w", err)
			}
			wfn.PartitionBy = append(wfn.PartitionBy, expr)
			if p.peek() != TokenComma {
				break
			}
			// Check if next comma is part of PARTITION BY or start of ORDER BY
			// Peek ahead to see if after comma we get ORDER
			if p.isOrderByNext() {
				break
			}
			p.advance() // consume comma
		}
	}

	// ORDER BY
	if p.isKeyword(TokenKWOrder) {
		p.advance() // consume ORDER
		if _, err := p.expect(TokenKWBy); err != nil {
			return nil, fmt.Errorf("expected BY after ORDER in OVER")
		}
		for {
			expr, err := p.parseExpr()
			if err != nil {
				return nil, fmt.Errorf("parsing ORDER BY in OVER: %w", err)
			}
			wob := WindowOrderBy{Expr: expr}
			if p.isKeyword(TokenKWDesc) {
				p.advance()
				wob.Desc = true
			} else if p.isKeyword(TokenKWAsc) {
				p.advance()
			}
			if p.isKeyword(TokenKWNulls) {
				p.advance()
				if p.isKeyword(TokenKWFirst) {
					p.advance()
					v := true
					wob.NullsFirst = &v
				} else if p.isKeyword(TokenKWLast) {
					p.advance()
					v := false
					wob.NullsFirst = &v
				} else {
					return nil, fmt.Errorf("expected FIRST or LAST after NULLS in OVER")
				}
			}
			wfn.OrderBy = append(wfn.OrderBy, wob)
			if p.peek() != TokenComma {
				break
			}
			p.advance() // consume comma
		}
	}

	// Frame spec: ROWS|RANGE ...
	if p.isKeyword(TokenKWRows) || p.isKeyword(TokenKWRange) {
		frame, err := p.parseWindowFrame()
		if err != nil {
			return nil, err
		}
		wfn.Frame = frame
	}

	if _, err := p.expect(TokenRParen); err != nil {
		return nil, fmt.Errorf("expected ')' after OVER clause")
	}

	return wfn, nil
}

// isOrderByNext checks if the current token starts ORDER BY (used to stop
// PARTITION BY parsing at the right place).
func (p *selectParser) isOrderByNext() bool {
	return p.isKeyword(TokenKWOrder)
}

// parseWindowFrame parses a frame specification: ROWS|RANGE ...
func (p *selectParser) parseWindowFrame() (*WindowFrame, error) {
	frame := &WindowFrame{}
	if p.isKeyword(TokenKWRows) {
		frame.Mode = FrameRows
	} else {
		frame.Mode = FrameRange
	}
	p.advance() // consume ROWS/RANGE

	// Check for BETWEEN ... AND ...
	if p.isKeyword(TokenKWBetween) {
		p.advance() // consume BETWEEN
		start, err := p.parseFrameBound()
		if err != nil {
			return nil, fmt.Errorf("parsing frame start: %w", err)
		}
		frame.Start = start
		if _, err := p.expect(TokenKWAnd); err != nil {
			return nil, fmt.Errorf("expected AND in frame BETWEEN")
		}
		end, err := p.parseFrameBound()
		if err != nil {
			return nil, fmt.Errorf("parsing frame end: %w", err)
		}
		frame.End = &end
	} else {
		// Single bound (start only, end defaults to CURRENT ROW)
		start, err := p.parseFrameBound()
		if err != nil {
			return nil, fmt.Errorf("parsing frame bound: %w", err)
		}
		frame.Start = start
	}

	// A RANGE bound with a value offset (`RANGE BETWEEN 5 PRECEDING …`)
	// measures in ORDER-BY VALUES, not rows: it selects every row whose key
	// is within 5 of this row's key, which is a different set from the 5
	// rows before it whenever keys repeat or skip. The executor resolves
	// RANGE bounds by peer group and has no value arithmetic, so it would
	// answer this query with a frame the query did not ask for. Say so
	// instead — a rejected query is recoverable, a plausible wrong number
	// is not.
	if frame.Mode == FrameRange {
		if rangeOffsetBound(frame.Start) || (frame.End != nil && rangeOffsetBound(*frame.End)) {
			return nil, fmt.Errorf("RANGE frame with a value offset is not supported; use ROWS for a row-count frame")
		}
	}

	return frame, nil
}

// rangeOffsetBound reports whether b is a PRECEDING/FOLLOWING bound carrying
// an offset, the spelling RANGE mode cannot evaluate.
func rangeOffsetBound(b FrameBound) bool {
	return b.Type == BoundPreceding || b.Type == BoundFollowing
}

// parseFrameBound parses a single frame bound:
// UNBOUNDED PRECEDING, UNBOUNDED FOLLOWING, CURRENT ROW, N PRECEDING, N FOLLOWING
func (p *selectParser) parseFrameBound() (FrameBound, error) {
	if p.isKeyword(TokenKWUnbounded) {
		p.advance()
		if p.isKeyword(TokenKWPreceding) {
			p.advance()
			return FrameBound{Type: BoundUnboundedPreceding}, nil
		}
		if p.isKeyword(TokenKWFollowing) {
			p.advance()
			return FrameBound{Type: BoundUnboundedFollowing}, nil
		}
		return FrameBound{}, fmt.Errorf("expected PRECEDING or FOLLOWING after UNBOUNDED")
	}
	if p.isKeyword(TokenKWCurrent) {
		p.advance()
		if _, err := p.expect(TokenKWRow); err != nil {
			return FrameBound{}, fmt.Errorf("expected ROW after CURRENT")
		}
		return FrameBound{Type: BoundCurrentRow}, nil
	}
	// N PRECEDING or N FOLLOWING
	if p.peek() == TokenNumber {
		numTok := p.advance()
		offset := &Lit{Value: numTok.val, Kind: LitNumber}
		if p.isKeyword(TokenKWPreceding) {
			p.advance()
			return FrameBound{Type: BoundPreceding, Offset: offset}, nil
		}
		if p.isKeyword(TokenKWFollowing) {
			p.advance()
			return FrameBound{Type: BoundFollowing, Offset: offset}, nil
		}
		return FrameBound{}, fmt.Errorf("expected PRECEDING or FOLLOWING after number")
	}
	return FrameBound{}, fmt.Errorf("unexpected frame bound token %q", p.cur.val)
}

func (p *selectParser) parseCaseExpr() (Node, error) {
	p.advance() // consume CASE

	c := &CaseNode{}

	// Simple CASE: CASE subject WHEN ...
	// Searched CASE: CASE WHEN ...
	if !p.isKeyword(TokenKWWhen) {
		subject, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		c.Subject = subject
	}

	// Parse WHEN clauses
	for p.isKeyword(TokenKWWhen) {
		p.advance()
		cond, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenKWThen); err != nil {
			return nil, fmt.Errorf("expected THEN in CASE")
		}
		result, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		c.Whens = append(c.Whens, WhenClause{Cond: cond, Result: result})
	}

	if len(c.Whens) == 0 {
		return nil, fmt.Errorf("CASE requires at least one WHEN clause")
	}

	// ELSE
	if p.isKeyword(TokenKWElse) {
		p.advance()
		elseExpr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		c.Else = elseExpr
	}

	if _, err := p.expect(TokenKWEnd); err != nil {
		return nil, fmt.Errorf("expected END in CASE")
	}

	return c, nil
}

// maybeExtendTwoWordType consumes a second type-name word when typeName is
// the first half of a PostgreSQL two-word spelling this engine treats as one
// type — "double precision" and "character varying" — and returns the
// merged, single-word canonical name the rest of the engine already
// resolves. pg_dump, JDBC and SQLAlchemy all emit these spellings;
// SQLAlchemy renders CAST(x AS DOUBLE PRECISION) for its Float type (#374).
// Anything else passes through unchanged with nothing consumed. Shared by
// both places a CAST type name is read: CAST(x AS t) and x::t.
func (p *selectParser) maybeExtendTwoWordType(typeName string) string {
	if p.peek() != TokenIdent || p.cur.quoted {
		return typeName
	}
	switch {
	case typeName == "double" && strings.EqualFold(p.cur.val, "precision"):
		p.advance()
		return "double"
	case typeName == "character" && strings.EqualFold(p.cur.val, "varying"):
		p.advance()
		return "varchar"
	}
	return typeName
}

func (p *selectParser) parseCastExpr() (Node, error) {
	p.advance() // consume CAST
	if _, err := p.expect(TokenLParen); err != nil {
		return nil, fmt.Errorf("expected ( after CAST")
	}

	inner, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	if _, err := p.expect(TokenKWAs); err != nil {
		return nil, fmt.Errorf("expected AS in CAST")
	}

	typeTok, err := p.expect(TokenIdent)
	if err != nil {
		return nil, fmt.Errorf("expected type name in CAST")
	}

	typeName := strings.ToLower(typeTok.val)
	typeName = p.maybeExtendTwoWordType(typeName)

	// Handle parameterized types: ARRAY(...), ROW(...), MAP(...), DECIMAL(...)
	if p.cur.typ == TokenLParen {
		typeName += p.consumeTypeParams()
	}

	if _, err := p.expect(TokenRParen); err != nil {
		return nil, fmt.Errorf("expected ) after CAST")
	}

	return &CastNode{Inner: inner, TypeName: typeName}, nil
}

// parseExtractExpr parses EXTRACT(field FROM expr) and rewrites to field(expr).
// Examples: EXTRACT(YEAR FROM col) → year(col), EXTRACT(DOW FROM ts) → day_of_week(ts)
func (p *selectParser) parseExtractExpr() (Node, error) {
	p.advance() // consume EXTRACT
	if _, err := p.expect(TokenLParen); err != nil {
		return nil, fmt.Errorf("expected ( after EXTRACT")
	}

	// Field name — accept any token (ident or keyword) as field
	fieldTok := p.advance()
	field := strings.ToLower(fieldTok.val)

	if _, err := p.expect(TokenKWFrom); err != nil {
		return nil, fmt.Errorf("expected FROM in EXTRACT")
	}

	source, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	if _, err := p.expect(TokenRParen); err != nil {
		return nil, fmt.Errorf("expected ) after EXTRACT")
	}

	// Map non-standard field names to function names
	switch field {
	case "dow":
		field = "day_of_week"
	case "doy":
		field = "day_of_year"
	}
	return &FuncCallNode{Name: field, Args: []Node{source}}, nil
}

// parsePositionExpr parses POSITION(needle IN haystack) and rewrites it to
// strpos(haystack, needle) — the standard SQL spelling of strpos, with its
// two arguments in the opposite order from how POSITION spells them (#374).
// needle and haystack parse at addition precedence, the same level a
// comparison's operands take: anything looser and IN would be swallowed as
// this expression's own top-level operator instead of being left for this
// function to consume explicitly.
func (p *selectParser) parsePositionExpr() (Node, error) {
	p.advance() // consume POSITION
	if _, err := p.expect(TokenLParen); err != nil {
		return nil, fmt.Errorf("expected ( after POSITION")
	}
	needle, err := p.parseAddition()
	if err != nil {
		return nil, fmt.Errorf("parsing POSITION needle: %w", err)
	}
	if _, err := p.expect(TokenKWIn); err != nil {
		return nil, fmt.Errorf("expected IN in POSITION(needle IN haystack)")
	}
	haystack, err := p.parseAddition()
	if err != nil {
		return nil, fmt.Errorf("parsing POSITION haystack: %w", err)
	}
	if _, err := p.expect(TokenRParen); err != nil {
		return nil, fmt.Errorf("expected ) after POSITION")
	}
	return &FuncCallNode{Name: "strpos", Args: []Node{haystack, needle}}, nil
}

// parseTrimExpr parses TRIM with optional LEADING/TRAILING/BOTH syntax.
// Examples:
//
//	TRIM(col)                      → trim(col)
//	TRIM(LEADING '0' FROM col)     → ltrim(col, '0')
//	TRIM(TRAILING FROM col)        → rtrim(col)
//	TRIM(BOTH ' ' FROM col)        → trim(col, ' ')
func (p *selectParser) parseTrimExpr() (Node, error) {
	p.advance() // consume TRIM
	if _, err := p.expect(TokenLParen); err != nil {
		return nil, fmt.Errorf("expected ( after TRIM")
	}

	// Check for LEADING/TRAILING/BOTH — use lookahead to confirm extended syntax
	mode := ""
	if p.peek() == TokenIdent {
		upper := strings.ToUpper(p.cur.val)
		if upper == "LEADING" || upper == "TRAILING" || upper == "BOTH" {
			// Only treat as mode if FROM appears within the next 2 tokens
			if p.peekN(1) == TokenKWFrom || p.peekN(2) == TokenKWFrom {
				mode = upper
				p.advance() // consume mode
			}
		}
	}

	if mode != "" {
		// Extended TRIM syntax
		var trimChar Node
		if !p.isKeyword(TokenKWFrom) {
			var err error
			trimChar, err = p.parseExpr()
			if err != nil {
				return nil, err
			}
		}
		if _, err := p.expect(TokenKWFrom); err != nil {
			return nil, fmt.Errorf("expected FROM in TRIM(%s ...)", mode)
		}
		source, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, fmt.Errorf("expected ) after TRIM")
		}
		fn := "trim"
		switch mode {
		case "LEADING":
			fn = "ltrim"
		case "TRAILING":
			fn = "rtrim"
		}
		args := []Node{source}
		if trimChar != nil {
			args = append(args, trimChar)
		}
		return &FuncCallNode{Name: fn, Args: args}, nil
	}

	// Regular TRIM(expr[, char])
	var args []Node
	for {
		arg, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
		if p.peek() != TokenComma {
			break
		}
		p.advance()
	}
	if _, err := p.expect(TokenRParen); err != nil {
		return nil, fmt.Errorf("expected ) after TRIM")
	}
	return &FuncCallNode{Name: "trim", Args: args}, nil
}

// parseIntervalLiteral parses INTERVAL 'N' UNIT or INTERVAL 'N unit[s]'.
// Examples: INTERVAL '30' DAY, INTERVAL '30 days', INTERVAL '1' YEAR
func (p *selectParser) parseIntervalLiteral() (Node, error) {
	p.advance() // consume INTERVAL

	if p.peek() != TokenString {
		return nil, fmt.Errorf("expected string literal after INTERVAL")
	}
	valStr := p.advance().val

	// Try to parse combined form: INTERVAL '30 days'
	valStr = strings.TrimSpace(valStr)
	parts := strings.Fields(valStr)

	var value int
	var unit string

	if len(parts) == 2 {
		// Combined: '30 days'
		n, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, fmt.Errorf("invalid interval value %q: %w", parts[0], err)
		}
		value = n
		unit = normalizeIntervalUnit(parts[1])
	} else if len(parts) == 1 {
		// Separate: 'N' followed by keyword unit
		n, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, fmt.Errorf("invalid interval value %q: %w", parts[0], err)
		}
		value = n
		// Look for trailing unit keyword (DAY, MONTH, YEAR, etc.)
		if p.peek() == TokenIdent {
			unit = normalizeIntervalUnit(p.advance().val)
		} else {
			unit = "day" // default
		}
	} else {
		return nil, fmt.Errorf("invalid INTERVAL literal %q", valStr)
	}

	return &IntervalLit{Value: value, Unit: unit}, nil
}

// normalizeIntervalUnit maps plural/mixed-case units to canonical lowercase singular.
func normalizeIntervalUnit(s string) string {
	switch strings.ToLower(strings.TrimSuffix(strings.ToLower(s), "s")) {
	case "day":
		return "day"
	case "month":
		return "month"
	case "year":
		return "year"
	case "hour":
		return "hour"
	case "minute":
		return "minute"
	case "second":
		return "second"
	case "week":
		return "week"
	default:
		return strings.ToLower(s)
	}
}

// parseArrayLiteral parses ARRAY[expr, expr, ...]. The ARRAY keyword has been consumed;
// the current token is [.
func (p *selectParser) parseArrayLiteral() (Node, error) {
	p.advance() // consume [
	var elements []Node
	for p.peek() != TokenRBracket && p.peek() != TokenEOF {
		if len(elements) > 0 {
			if _, err := p.expect(TokenComma); err != nil {
				return nil, fmt.Errorf("expected , or ] in ARRAY literal")
			}
		}
		elem, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		elements = append(elements, elem)
	}
	if _, err := p.expect(TokenRBracket); err != nil {
		return nil, fmt.Errorf("expected ] to close ARRAY literal")
	}
	return &ArrayLitNode{Elements: elements}, nil
}

// consumeTypeParams reads a parenthesized type parameter list like "(INT64)" or "(name STRING, age INT32)".
// Returns the consumed text including the parens. Handles nested parens for types like ARRAY(ROW(...)).
func (p *selectParser) consumeTypeParams() string {
	if p.cur.typ != TokenLParen {
		return ""
	}
	var buf strings.Builder
	buf.WriteByte('(')
	p.advance() // consume (
	depth := 1
	for depth > 0 && p.cur.typ != TokenEOF {
		switch p.cur.typ {
		case TokenLParen:
			depth++
			buf.WriteByte('(')
		case TokenRParen:
			depth--
			if depth > 0 {
				buf.WriteByte(')')
			}
		case TokenComma:
			buf.WriteString(", ")
		default:
			if buf.Len() > 1 && buf.String()[buf.Len()-1] != '(' && buf.String()[buf.Len()-1] != ' ' {
				buf.WriteByte(' ')
			}
			buf.WriteString(strings.ToLower(p.cur.val))
		}
		p.advance()
	}
	buf.WriteByte(')')
	return buf.String()
}

// windowSpecFromNode converts a parsed WindowFuncNode into a WindowSpec
// that the logical plan builder expects.
func windowSpecFromNode(wfn *WindowFuncNode, alias string) *WindowSpec {
	ws := &WindowSpec{
		FuncName: wfn.Func.Name,
		Alias:    alias,
	}

	// Build args string
	if wfn.Func.Star {
		ws.Args = "*"
	} else if len(wfn.Func.Args) > 0 {
		args := make([]string, len(wfn.Func.Args))
		for i, a := range wfn.Func.Args {
			args[i] = a.String()
		}
		ws.Args = strings.Join(args, ", ")
	}

	// Partition By
	for _, pb := range wfn.PartitionBy {
		ws.PartitionBy = append(ws.PartitionBy, pb.String())
	}

	// Order By
	for _, ob := range wfn.OrderBy {
		ws.OrderBy = append(ws.OrderBy, WindowOrderItem{
			Column:     ob.Expr.String(),
			Desc:       ob.Desc,
			NullsFirst: ob.NullsFirst,
		})
	}

	// Frame
	if wfn.Frame != nil {
		ws.Frame = wfn.Frame
	}

	return ws
}

// ParseExpression parses a single expression from a SQL string.
// Used for standalone expression parsing (e.g., UDF bodies, WHERE clauses).
func ParseExpression(sql string) (Node, error) {
	p := newSelectParser(sql)
	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	return expr, nil
}

// --- GROUPING SETS / CUBE / ROLLUP helpers ---

// parseGroupingSets parses GROUPING SETS ((...), (...), ...).
// Returns the list of grouping sets and the union of all columns referenced.
func (p *selectParser) parseGroupingSets() ([][]string, []string, error) {
	if _, err := p.expect(TokenLParen); err != nil {
		return nil, nil, fmt.Errorf("expected ( after GROUPING SETS")
	}

	allCols := make(map[string]bool)
	var sets [][]string

	for {
		if p.peek() == TokenLParen {
			// Parenthesized set: (col1, col2)
			p.advance() // consume (
			var cols []string
			if p.peek() != TokenRParen {
				for {
					expr, err := p.parseExpr()
					if err != nil {
						return nil, nil, fmt.Errorf("parsing grouping set: %w", err)
					}
					col := expr.String()
					cols = append(cols, col)
					allCols[col] = true
					if p.peek() != TokenComma {
						break
					}
					p.advance()
				}
			}
			if _, err := p.expect(TokenRParen); err != nil {
				return nil, nil, fmt.Errorf("expected ) in grouping set")
			}
			sets = append(sets, cols)
		} else {
			// Single column without parens (e.g., GROUPING SETS (a, (a,b)))
			expr, err := p.parseExpr()
			if err != nil {
				return nil, nil, fmt.Errorf("parsing grouping set column: %w", err)
			}
			col := expr.String()
			sets = append(sets, []string{col})
			allCols[col] = true
		}

		if p.peek() != TokenComma {
			break
		}
		p.advance() // consume comma
	}

	if _, err := p.expect(TokenRParen); err != nil {
		return nil, nil, fmt.Errorf("expected ) after GROUPING SETS")
	}

	var all []string
	for col := range allCols {
		all = append(all, col)
	}
	return sets, all, nil
}

// parseGroupingColList parses a parenthesized column list: (a, b, c).
func (p *selectParser) parseGroupingColList() ([]string, error) {
	if _, err := p.expect(TokenLParen); err != nil {
		return nil, fmt.Errorf("expected ( after CUBE/ROLLUP")
	}
	var cols []string
	for {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		cols = append(cols, expr.String())
		if p.peek() != TokenComma {
			break
		}
		p.advance()
	}
	if _, err := p.expect(TokenRParen); err != nil {
		return nil, fmt.Errorf("expected ) after column list")
	}
	return cols, nil
}

// expandCube expands CUBE(a, b, c) into all 2^n subsets.
// CUBE(a, b, c) = GROUPING SETS ((a,b,c), (a,b), (a,c), (b,c), (a), (b), (c), ())
func expandCube(cols []string) [][]string {
	n := len(cols)
	total := 1 << n // 2^n
	sets := make([][]string, 0, total)
	// Iterate from all-bits-set down to 0 for conventional ordering
	for mask := total - 1; mask >= 0; mask-- {
		var set []string
		for i := 0; i < n; i++ {
			if mask&(1<<i) != 0 {
				set = append(set, cols[i])
			}
		}
		sets = append(sets, set)
	}
	return sets
}

// expandRollup expands ROLLUP(a, b, c) into hierarchical subsets.
// ROLLUP(a, b, c) = GROUPING SETS ((a,b,c), (a,b), (a), ())
func expandRollup(cols []string) [][]string {
	sets := make([][]string, 0, len(cols)+1)
	for i := len(cols); i >= 0; i-- {
		set := make([]string, i)
		copy(set, cols[:i])
		sets = append(sets, set)
	}
	return sets
}

// isTypedLiteralType reports whether name may prefix a string literal to form
// a typed literal (DATE '...'). INTERVAL has its own parser above: its
// payload is a quantity with units rather than a value to cast.
func isTypedLiteralType(name string) bool {
	switch name {
	case "DATE", "TIMESTAMP", "TIME", "TIMESTAMPTZ":
		return true
	}
	return false
}
