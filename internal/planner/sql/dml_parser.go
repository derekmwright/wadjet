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

		var row []string
		for {
			tok := l.nextToken()
			if tok.typ == TokenRParen {
				break
			}
			if tok.typ == TokenComma {
				continue
			}
			row = append(row, tok.val)
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
		tok := l.nextToken()
		if tok.typ == TokenLParen {
			depth++
		} else if tok.typ == TokenRParen {
			depth--
		}
		parts = append(parts, tok.val)
	}
	return strings.Join(parts, " ")
}
