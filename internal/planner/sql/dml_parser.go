package sql

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// parseDMLRelation reads the relation a DELETE or an UPDATE targets:
// `[qualifier.]table [[AS] alias]`, the same table reference the SELECT
// parser reads (select_parser.go, parseTableRefTail).
//
// Neither the qualifier nor the alias used to be read at all, and the alias
// was the one that lost data. The token after the table name ENDED the
// statement, so `DELETE FROM pr AS a WHERE a.id = 1` left WhereSQL empty and
// the executor — for which empty means "every row" — deleted the whole table
// and reported DELETE 3, where PostgreSQL reports DELETE 1 (#686). The
// UPDATE spelling was at least loud ("expected SET after UPDATE pr").
//
// Whatever follows the relation is the CALLER's to check, and both callers
// refuse a token they do not expect rather than stopping quietly: a
// statement whose tail this parser cannot read is a statement whose meaning
// it does not know, and running it unconditionally is the worst available
// answer.
func parseDMLRelation(l *lexer, after string) (DMLTarget, error) {
	// PostgreSQL's `ONLY t` says "do not descend into inheritance children".
	// This server has no table inheritance, so every table IS only itself and
	// the keyword is accepted and ignored — which is what it MEANS here. It
	// used to be read as the table name, so `DELETE FROM ONLY pr` failed with
	// `table "ONLY" not found` and no SQLSTATE at all (#686 review).
	if peek := l.peekToken(); peek.typ == TokenIdent && !peek.quoted && strings.EqualFold(peek.val, "ONLY") {
		l.nextToken()
	}
	tableTok := l.nextToken()
	if tableTok.typ != TokenIdent {
		return DMLTarget{}, fmt.Errorf("expected table name after %s, got %q", after, tableTok.val)
	}
	t := DMLTarget{Table: tableTok.val}

	// A qualified name: schema.table, or catalog.schema.table. PostgreSQL
	// clients write them constantly (`DELETE FROM public.orders`), and
	// reading only the first identifier made that a DELETE against a table
	// named "public" — which failed with "table public not found", loud but
	// about the wrong name. The qualifier is kept rather than dropped so the
	// executor can reject one that names a schema this server does not have.
	for l.peekToken().typ == TokenDot {
		l.nextToken() // consume .
		partTok := l.nextToken()
		if partTok.typ != TokenIdent {
			return DMLTarget{}, fmt.Errorf("expected name after %q. in %s, got %q", t.Table, after, partTok.val)
		}
		if t.Qualifier == "" {
			t.Qualifier = t.Table
		} else {
			t.Qualifier += "." + t.Table
		}
		t.Table = partTok.val
	}

	// Optional alias, with or without AS. A keyword after the relation is
	// never the alias — SET ends an UPDATE's relation, WHERE ends either —
	// which is what keeps `UPDATE t SET ...` and `DELETE FROM t WHERE ...`
	// reading exactly as they did.
	if l.peekToken().typ == TokenKWAs {
		l.nextToken() // consume AS
		aliasTok := l.nextToken()
		if aliasTok.typ != TokenIdent {
			return DMLTarget{}, fmt.Errorf("expected alias after AS in %s, got %q", after, aliasTok.val)
		}
		t.Alias = aliasTok.val
	} else if peek := l.peekToken(); peek.typ == TokenIdent && !isDMLTailKeyword(peek) {
		l.nextToken()
		t.Alias = peek.val
	}
	return t, nil
}

// isDMLTailKeyword reports whether an unquoted identifier after the relation
// begins a CLAUSE this parser knows by name rather than an alias.
//
// RETURNING is not a lexer keyword, so it was taken as the table's alias and
// the refusal then named the token AFTER it ("unexpected \"*\"") instead of
// the feature that is missing. A DOUBLE-QUOTED "returning" is a name and
// stays one.
func isDMLTailKeyword(t token) bool {
	return !t.quoted && strings.EqualFold(t.val, "RETURNING")
}

// parseDMLWhere reads the optional trailing `WHERE <condition>` of a DELETE
// or an UPDATE and refuses anything else that is left over.
//
// The two refusals are the point. An EMPTY condition (`DELETE FROM t WHERE`)
// used to leave WhereSQL empty, which the executor reads as "every row" —
// PostgreSQL answers 42601. And a token that is neither WHERE nor the end of
// the statement (`DELETE FROM t x y`, `DELETE FROM t RETURNING *`) used to be
// dropped on the floor together with the rest of the statement, so a DELETE
// the parser only half understood still ran, unconditionally.
func parseDMLWhere(l *lexer, t *DMLTarget, stmt string) error {
	switch next := l.peekToken(); next.typ {
	case TokenKWWhere:
		l.nextToken() // consume WHERE
		t.WhereSQL = strings.TrimSpace(l.rest())
		if t.WhereSQL == "" {
			return fmt.Errorf("%s: WHERE requires a condition", stmt)
		}
	case TokenEOF, TokenSemicolon:
		// No WHERE: the statement is unconditional, and says so.
	default:
		// RETURNING is a legal statement this server has not implemented, so
		// it is 0A000 and not a syntax error — and it must still REFUSE,
		// because dropping the clause silently ran the DELETE and handed the
		// client back no rows where PostgreSQL returns the deleted ones.
		if next.typ == TokenIdent && !next.quoted && strings.EqualFold(next.val, "RETURNING") {
			return sqlerr.New("0A000", "%s: RETURNING is not supported", stmt)
		}
		return fmt.Errorf("%s: unexpected %q; expected WHERE or the end of the statement", stmt, next.val)
	}
	return nil
}

// parseUpdate parses: UPDATE table [[AS] alias] SET col1 = val1, col2 = val2 [WHERE condition]
func parseUpdate(sql string, l *lexer) (*ParsedQuery, error) {
	l.nextToken() // consume UPDATE

	target, err := parseDMLRelation(l, "UPDATE")
	if err != nil {
		return nil, err
	}
	target.StmtSQL = sql

	// SET keyword
	setTok := l.nextToken()
	if setTok.typ != TokenKWSet {
		return nil, fmt.Errorf("expected SET after UPDATE %s, got %q", target.Table, setTok.val)
	}

	// Parse SET clauses: col = expr [, col = expr ...]
	var clauses []SetClause
	for {
		colTok := l.nextToken()
		if colTok.typ != TokenIdent {
			return nil, fmt.Errorf("expected column name in SET clause, got %q", colTok.val)
		}

		eqTok := l.nextToken()
		if eqTok.typ != TokenEq {
			return nil, fmt.Errorf("expected = after column %s, got %q", colTok.val, eqTok.val)
		}

		// Collect value expression tokens until comma, WHERE, semicolon or EOF
		valParts := collectUntil(l, TokenComma, TokenKWWhere, TokenSemicolon, TokenEOF)

		clauses = append(clauses, SetClause{
			Column: colTok.val,
			Value:  strings.TrimSpace(valParts),
		})

		// Check what ended the expression
		next := l.peekToken()
		if next.typ == TokenComma {
			l.nextToken() // consume comma
			continue
		}
		break
	}

	if len(clauses) == 0 {
		return nil, fmt.Errorf("UPDATE requires at least one SET clause")
	}

	if err := parseDMLWhere(l, &target, "UPDATE "+target.Table); err != nil {
		return nil, err
	}

	return &ParsedQuery{
		Type:      QueryUpdate,
		TableName: target.Table,
		SQL:       sql,
		Update: &UpdateInfo{
			DMLTarget:  target,
			SetClauses: clauses,
		},
	}, nil
}

// parseDelete parses: DELETE FROM table [[AS] alias] [WHERE condition]
func parseDelete(sql string, l *lexer) (*ParsedQuery, error) {
	l.nextToken() // consume DELETE

	// FROM keyword
	fromTok := l.nextToken()
	if fromTok.typ != TokenKWFrom {
		return nil, fmt.Errorf("expected FROM after DELETE, got %q", fromTok.val)
	}

	target, err := parseDMLRelation(l, "DELETE FROM")
	if err != nil {
		return nil, err
	}
	target.StmtSQL = sql

	if err := parseDMLWhere(l, &target, "DELETE FROM "+target.Table); err != nil {
		return nil, err
	}

	return &ParsedQuery{
		Type:      QueryDelete,
		TableName: target.Table,
		SQL:       sql,
		Delete:    &DeleteInfo{DMLTarget: target},
	}, nil
}

// HasTopLevelReturning reports whether a DML statement writes a RETURNING
// clause outside any parentheses.
//
// RETURNING is not a lexer keyword, so every clause that collects raw text
// swallowed it and then answered differently: bare `DELETE FROM t RETURNING *`
// took it as the table's ALIAS, `DELETE ... WHERE id = 1 RETURNING *` fed it
// to the WHERE's complete-parse and called legal SQL a syntax error, and
// `INSERT ... RETURNING id` dropped it in silence and reported INSERT 1. One
// check over the whole statement gives all four doors the same answer: it is
// a legal statement whose feature this server has not implemented, so 0A000
// (#686 R2-4).
//
// An unquoted RETURNING is always the clause — PostgreSQL reserves the word,
// so a column of that name must be double-quoted, and a quoted identifier is
// not matched here.
func HasTopLevelReturning(sql string) bool {
	l := newLexer(sql)
	depth := 0
	for {
		tok := l.nextToken()
		switch tok.typ {
		case TokenEOF, TokenError:
			return false
		case TokenLParen:
			depth++
		case TokenRParen:
			depth--
		case TokenIdent:
			if depth == 0 && !tok.quoted && strings.EqualFold(tok.val, "RETURNING") {
				return true
			}
		}
	}
}

// HasTopLevelWhereToken reports whether sql spells a WHERE keyword outside
// any parentheses.
//
// It exists for one caller: the backstop in wadjet.BuildDMLPredicate, which
// has to tell "this DELETE is unconditional because it was written that way"
// from "this DELETE is unconditional because the parser dropped its WHERE".
// The two are indistinguishable in DMLTarget.WhereSQL, and the second one
// empties tables (#686).
//
// The lexer decides, not strings.Contains: a WHERE inside a string literal
// ('WHERE') or a quoted identifier ("where") is not a clause, and a WHERE
// belonging to a SUBQUERY (`SET n = (SELECT ... WHERE ...)`) is not this
// statement's clause either — hence the depth counter.
func HasTopLevelWhereToken(sql string) bool {
	l := newLexer(sql)
	depth := 0
	for {
		tok := l.nextToken()
		switch tok.typ {
		case TokenEOF, TokenError:
			return false
		case TokenLParen:
			depth++
		case TokenRParen:
			depth--
		case TokenKWWhere:
			if depth == 0 {
				return true
			}
		}
	}
}

// parseInsert parses: INSERT INTO table [(col1, col2)] VALUES (v1, v2), (v3, v4)
func parseInsert(sql string, l *lexer) (*ParsedQuery, error) {
	l.nextToken() // consume INSERT

	// INTO keyword
	intoTok := l.nextToken()
	if intoTok.typ != TokenKWInto {
		return nil, fmt.Errorf("expected INTO after INSERT, got %q", intoTok.val)
	}

	// Table name
	tableTok := l.nextToken()
	if tableTok.typ != TokenIdent {
		return nil, fmt.Errorf("expected table name after INSERT INTO, got %q", tableTok.val)
	}
	tableName := tableTok.val

	// Optional column list
	var columns []string
	next := l.peekToken()
	if next.typ == TokenLParen {
		l.nextToken() // consume (
		for {
			colTok := l.nextToken()
			if colTok.typ == TokenRParen {
				break
			}
			// Unterminated column list: at EOF/error the ')' never comes,
			// and nextToken keeps returning EOF, so the loop must stop
			// itself. Return a parse error instead. (Found by
			// FuzzParseSQL on the truncated input "INSERT INTO t( VA".)
			if colTok.typ == TokenEOF || colTok.typ == TokenError {
				return nil, fmt.Errorf("unterminated column list in INSERT INTO %s", tableName)
			}
			if colTok.typ == TokenComma {
				continue
			}
			if colTok.typ == TokenIdent {
				columns = append(columns, colTok.val)
			}
		}
	}

	// VALUES keyword
	valTok := l.nextToken()
	if valTok.typ != TokenKWValues {
		return nil, fmt.Errorf("expected VALUES after INSERT INTO %s, got %q", tableName, valTok.val)
	}

	// Parse value rows: (v1, v2), (v3, v4), ...
	var valueRows [][]string
	for {
		next = l.peekToken()
		if next.typ != TokenLParen {
			break
		}
		l.nextToken() // consume (

		row, err := parseValuesRow(l, tableName)
		if err != nil {
			return nil, err
		}
		valueRows = append(valueRows, row)

		// Check for comma between rows
		next = l.peekToken()
		if next.typ == TokenComma {
			l.nextToken()
			continue
		}
		break
	}

	if len(valueRows) == 0 {
		return nil, fmt.Errorf("INSERT requires at least one VALUES row")
	}

	return &ParsedQuery{
		Type:      QueryInsert,
		TableName: tableName,
		SQL:       sql,
		Insert: &InsertInfo{
			Table:   tableName,
			Columns: columns,
			Values:  valueRows,
		},
	}, nil
}

// parseValuesRow reads one `(v1, v2, ...)` tuple, the opening paren already
// consumed, and returns one entry per VALUE.
//
// The loop this replaces appended one entry per TOKEN and merely SKIPPED
// commas, so the entry count was the token count rather than the comma count.
// A unary minus is its own token — the lexer is right to make it one, and
// lexNumber deliberately never absorbs a sign — so `VALUES (4, -3)` produced
// ["4", "-", "3"] and failed with "expected 2 values, got 3" (#447). It also
// broke on the FIRST ')' at any depth, with no depth counter, so a nested
// parenthesis left the tuple's own ')' in the stream and the statement parsed
// SUCCESSFULLY with truncated values.
//
// Splitting on TOP-LEVEL commas with a depth counter is what collectUntil in
// this same file already does for UPDATE SET.
//
// Every refusal about one value names that value's 1-based position in the
// tuple. The reason alone ("VALUES accepts literals, not the expression ...")
// told the author of `VALUES (1, 'a', <bad>, 4)` what was wrong but not which
// entry it was, so finding it meant re-reading the tuple by hand.
func parseValuesRow(l *lexer, tableName string) ([]string, error) {
	var row []string
	var cur []token
	depth := 0
	for {
		tok := l.nextToken()
		switch {
		case tok.typ == TokenEOF || tok.typ == TokenError:
			return nil, fmt.Errorf("unterminated VALUES row in INSERT INTO %s: input ended at value %d of the VALUES tuple",
				tableName, len(row)+1)
		case tok.typ == TokenLParen:
			depth++
		case tok.typ == TokenRParen && depth > 0:
			depth--
		case tok.typ == TokenRParen:
			// The tuple's own closing paren.
			if len(cur) == 0 && len(row) == 0 {
				return nil, fmt.Errorf("empty VALUES row in INSERT INTO %s", tableName)
			}
			v, err := insertValueText(cur, tableName, len(row)+1)
			if err != nil {
				return nil, err
			}
			return append(row, v), nil
		case tok.typ == TokenComma && depth == 0:
			v, err := insertValueText(cur, tableName, len(row)+1)
			if err != nil {
				return nil, err
			}
			row = append(row, v)
			cur = cur[:0]
			continue
		}
		cur = append(cur, tok)
	}
}

// insertValueText renders one VALUES entry as the literal text the executors'
// converters read.
//
// A single token keeps its own val, which is what makes a string literal
// arrive without its quotes and `NULL` arrive as the keyword — the behaviour
// the converters (wadjet/dml.go convertValue, server.go convertDMLValue) are
// written against. The two shapes above that are a signed numeric literal and
// a value wrapped in redundant parentheses.
//
// Anything else is an EXPRESSION, and this path has no evaluator: the old loop
// answered `VALUES (coalesce(a, b))` with a truncated row and no error at all.
// Naming it is the honest answer, and it costs nothing that worked before.
//
// ordinal is the value's 1-based position in the tuple and is used only in the
// refusals, so that a rejected entry can be found without counting commas.
func insertValueText(toks []token, tableName string, ordinal int) (string, error) {
	toks = stripRedundantParens(toks)
	switch {
	case len(toks) == 0:
		return "", fmt.Errorf("value %d of the VALUES tuple in INSERT INTO %s: empty value", ordinal, tableName)
	case len(toks) == 1 && toks[0].typ == TokenString:
		// RE-QUOTED. The lexer strips a string literal's quotes and resolves
		// its doubled apostrophes, so a bare val cannot be told from the NULL
		// keyword or from an identifier: `VALUES (9, 'NULL')` and
		// `VALUES (9, NULL)` arrived at the converter as the same three
		// letters, and the converter reads the word `null` as the keyword, so
		// both stored a SQL NULL (#690). Quoting it back is what carries the
		// literal's KIND through a []string, and convertValue reverses it
		// exactly.
		return "'" + strings.ReplaceAll(toks[0].val, "'", "''") + "'", nil
	case len(toks) == 1:
		return toks[0].val, nil
	case len(toks) == 2 && toks[1].typ == TokenNumber && toks[0].typ == TokenMinus:
		return "-" + toks[1].val, nil
	case len(toks) == 2 && toks[1].typ == TokenNumber && toks[0].typ == TokenPlus:
		return toks[1].val, nil
	}
	var b strings.Builder
	for i, t := range toks {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(t.val)
	}
	return "", fmt.Errorf("value %d of the VALUES tuple in INSERT INTO %s: VALUES accepts literals, not the expression %q",
		ordinal, tableName, b.String())
}

// stripRedundantParens removes parentheses that wrap the WHOLE value, so
// `((-3))` is the literal -3. A leading '(' that closes before the end — the
// `(1) + (2)` shape — wraps nothing and is left alone.
func stripRedundantParens(toks []token) []token {
	for len(toks) >= 2 && toks[0].typ == TokenLParen && toks[len(toks)-1].typ == TokenRParen {
		depth := 0
		wraps := true
		for i, t := range toks {
			switch t.typ {
			case TokenLParen:
				depth++
			case TokenRParen:
				depth--
			}
			if depth == 0 && i < len(toks)-1 {
				wraps = false
			}
		}
		if !wraps {
			return toks
		}
		toks = toks[1 : len(toks)-1]
	}
	return toks
}

// collectUntil reads tokens from the lexer and returns their text until one
// of the stop tokens is found. Does not consume the stop token.
func collectUntil(l *lexer, stops ...TokenType) string {
	stopSet := make(map[TokenType]bool, len(stops))
	for _, s := range stops {
		stopSet[s] = true
	}

	var parts []string
	depth := 0 // track parentheses depth
	for {
		next := l.peekToken()
		if depth == 0 && stopSet[next.typ] {
			break
		}
		// EOF/error always stop, whatever the paren depth: with an
		// unbalanced '(' (e.g. "UPDATE a SET a=(") the stop token can
		// never arrive at depth>0, so peekToken keeps returning EOF and
		// the loop would not terminate on its own. Callers see a
		// truncated collection and fail their own structural checks.
		// (Found by FuzzParseSQL.)
		if next.typ == TokenEOF || next.typ == TokenError {
			break
		}
		tok := l.nextToken()
		if tok.typ == TokenLParen {
			depth++
		} else if tok.typ == TokenRParen {
			depth--
		}
		// A string token's value arrives with its quotes STRIPPED and its ''
		// escapes resolved (lexer.go, TokenString). Joining that verbatim
		// makes `SET s = 'archived'` and `SET s = archived` the same text, so
		// nothing downstream can tell a string LITERAL from a COLUMN
		// REFERENCE — which is what forced the SET resolver to guess, and what
		// made it store the source text of an expression it could not read
		// (#678). Re-quoting restores the one thing the caller needs: an
		// expression it can PARSE.
		if tok.typ == TokenString {
			parts = append(parts, "'"+strings.ReplaceAll(tok.val, "'", "''")+"'")
			continue
		}
		parts = append(parts, tok.val)
	}
	return strings.Join(parts, " ")
}
