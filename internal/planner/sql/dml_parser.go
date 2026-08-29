package sql

import (
	"fmt"
	"strings"
)

// parseUpdate parses: UPDATE table SET col1 = val1, col2 = val2 [WHERE condition]
func parseUpdate(sql string, l *lexer) (*ParsedQuery, error) {
	l.nextToken() // consume UPDATE

	// Table name
	tableTok := l.nextToken()
	if tableTok.typ != TokenIdent {
		return nil, fmt.Errorf("expected table name after UPDATE, got %q", tableTok.val)
	}
	tableName := tableTok.val

	// SET keyword
	setTok := l.nextToken()
	if setTok.typ != TokenKWSet {
		return nil, fmt.Errorf("expected SET after UPDATE %s, got %q", tableName, setTok.val)
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

		// Collect value expression tokens until comma, WHERE, or EOF
		valParts := collectUntil(l, TokenComma, TokenKWWhere, TokenEOF)

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

	// Optional WHERE
	var whereSQL string
	next := l.peekToken()
	if next.typ == TokenKWWhere {
		l.nextToken() // consume WHERE
		whereSQL = strings.TrimSpace(l.rest())
	}

	return &ParsedQuery{
		Type:      QueryUpdate,
		TableName: tableName,
		SQL:       sql,
		Update: &UpdateInfo{
			Table:      tableName,
			SetClauses: clauses,
			WhereSQL:   whereSQL,
		},
	}, nil
}

// parseDelete parses: DELETE FROM table [WHERE condition]
func parseDelete(sql string, l *lexer) (*ParsedQuery, error) {
	l.nextToken() // consume DELETE

	// FROM keyword
	fromTok := l.nextToken()
	if fromTok.typ != TokenKWFrom {
		return nil, fmt.Errorf("expected FROM after DELETE, got %q", fromTok.val)
	}

	// Table name
	tableTok := l.nextToken()
	if tableTok.typ != TokenIdent {
		return nil, fmt.Errorf("expected table name after DELETE FROM, got %q", tableTok.val)
	}
	tableName := tableTok.val

	// Optional WHERE
	var whereSQL string
	next := l.peekToken()
	if next.typ == TokenKWWhere {
		l.nextToken() // consume WHERE
		whereSQL = strings.TrimSpace(l.rest())
	}

	return &ParsedQuery{
		Type:      QueryDelete,
		TableName: tableName,
		SQL:       sql,
		Delete: &DeleteInfo{
			Table:    tableName,
			WhereSQL: whereSQL,
		},
	}, nil
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
