package sql

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// This file holds the two grammars that let a QUERY be the source of a write
// (#1024): `CREATE TABLE … AS <select>` and `INSERT INTO … <select>`. Both
// carry a whole SELECT statement, and both hand it to the ONE
// parseSelectStatement a bare SELECT goes through — there is no second SELECT
// dialect inside a write.

// parseIfNotExists consumes `IF NOT EXISTS` when the statement carries it.
//
// It is written against the lexer's save/restore rather than three
// peek-and-maybe-consume steps because a bare `IF` is not otherwise legal
// here: a statement that begins `IF` and does not continue `NOT EXISTS` must
// leave the stream exactly as it found it so the name parser can report what
// it actually saw.
func parseIfNotExists(l *lexer) bool {
	pos, start, width := l.pos, l.start, l.width
	if l.nextToken().typ != TokenKWIf {
		l.pos, l.start, l.width = pos, start, width
		return false
	}
	if l.nextToken().typ != TokenKWNot {
		l.pos, l.start, l.width = pos, start, width
		return false
	}
	if l.nextToken().typ != TokenKWExists {
		l.pos, l.start, l.width = pos, start, width
		return false
	}
	return true
}

// looksLikeColumnNameList reports whether the `(` the lexer is sitting on
// opens a CTAS rename list — `(a, b)` — rather than a column DEFINITION list
// — `(a INT64, b STRING)`.
//
// The two grammars are distinguished by the token AFTER the first name, and by
// nothing else: a rename list has `,` or `)` there, a definition list has a
// type. That is the same one-token discrimination PostgreSQL's grammar makes,
// and it is exact — a column definition always carries a type, and a rename
// never can. The lexer is left untouched either way.
func looksLikeColumnNameList(l *lexer) bool {
	pos, start, width := l.pos, l.start, l.width
	defer func() { l.pos, l.start, l.width = pos, start, width }()
	if l.nextToken().typ != TokenLParen {
		return false
	}
	if l.nextToken().typ != TokenIdent {
		return false
	}
	switch l.nextToken().typ {
	case TokenComma, TokenRParen:
		return true
	}
	return false
}

// parseColumnNameList reads `(a, b, c)`, the opening paren not yet consumed.
func parseColumnNameList(l *lexer, statement string) ([]string, error) {
	if tok := l.nextToken(); tok.typ != TokenLParen {
		return nil, sqlerr.New("42601", "%s: expected '(' before the column name list, got %q", statement, tok.val)
	}
	var names []string
	for {
		tok := l.nextToken()
		if tok.typ == TokenRParen {
			break
		}
		if tok.typ == TokenEOF || tok.typ == TokenError {
			return nil, sqlerr.New("42601", "%s: unterminated column name list", statement)
		}
		if tok.typ == TokenComma {
			continue
		}
		if tok.typ != TokenIdent {
			return nil, sqlerr.New("42601", "%s: expected a column name, got %q", statement, tok.val)
		}
		names = append(names, tok.val)
	}
	if len(names) == 0 {
		return nil, sqlerr.New("42601", "%s: the column name list is empty", statement)
	}
	return names, nil
}

// splitWithDataSuffix removes a trailing `WITH DATA` / `WITH NO DATA` from a
// CTAS body and reports which one was written.
//
// It is a TOKEN scan and not a string suffix test, for the reason every other
// top-level clause detector in this package is one (HasTopLevelReturning): a
// query may end in a string literal or an identifier that spells the words,
// and `… WHERE note = 'with no data'` must keep its predicate. Only a WITH at
// parenthesis depth zero whose remaining tokens are exactly `DATA` or
// `NO DATA` is the clause.
//
// A bare trailing `WITH` that is not this clause is left alone: it is then the
// SELECT parser's business to refuse it, with its own message.
func splitWithDataSuffix(body string) (rest string, withData bool) {
	l := newLexer(body)
	depth := 0
	for {
		before := l.pos
		tok := l.nextToken()
		switch tok.typ {
		case TokenEOF, TokenError:
			return body, true
		case TokenLParen:
			depth++
			continue
		case TokenRParen:
			depth--
			continue
		case TokenKWWith:
			if depth != 0 {
				continue
			}
		default:
			continue
		}
		// A top-level WITH. It is the clause only when what follows is
		// `DATA` or `NO DATA` and then nothing.
		data := true
		next := l.nextToken()
		if next.typ == TokenKWNot {
			// `WITH NOT DATA` is not the clause; leave it to the SELECT parser.
			continue
		}
		if next.typ == TokenIdent && strings.EqualFold(next.val, "no") {
			data = false
			next = l.nextToken()
		}
		if next.typ != TokenIdent || !strings.EqualFold(next.val, "data") {
			continue
		}
		if end := l.nextToken(); end.typ != TokenEOF {
			continue
		}
		return strings.TrimSpace(body[:before]), data
	}
}

// parseCreateTableAs finishes `CREATE TABLE [IF NOT EXISTS] name [(a, b)] AS
// <select> [WITH [NO] DATA]`, the `AS` not yet consumed.
//
// The SELECT is parsed WHOLE, by parseSelectStatement, and kept as its own
// ParsedQuery: every door that executes this statement re-plans that query the
// way it plans a bare SELECT, so the arm the planner picks for the query is the
// arm the CTAS runs on and the schema it writes is that plan's declared output
// (ADR-0026 §8).
func parseCreateTableAs(stmt string, l *lexer, name string, ifNotExists bool, renames []string) (*ParsedQuery, error) {
	if tok := l.nextToken(); tok.typ != TokenKWAs {
		return nil, sqlerr.New("42601", "CREATE TABLE: expected AS before the query, got %q", tok.val)
	}
	body, withData := splitWithDataSuffix(strings.TrimSpace(l.rest()))
	if body == "" {
		return nil, sqlerr.New("42601", "CREATE TABLE %s AS: a query is required after AS", name)
	}
	inner, err := parseSelectStatement(body, body)
	if err != nil {
		return nil, err
	}
	return &ParsedQuery{
		Type:      QueryCreateTable,
		TableName: name,
		SQL:       stmt,
		CreateTable: &CreateTableInfo{
			Name:          name,
			IfNotExists:   ifNotExists,
			AsSelect:      inner,
			AsColumnNames: renames,
			WithData:      withData,
		},
		// The inner query's windows and CTEs ride on the inner ParsedQuery.
		// They are deliberately NOT copied up: this statement is a CREATE
		// TABLE, and a door that read Windows off it would be describing the
		// DDL with the query's parts.
	}, nil
}

// parseInsertSelect finishes `INSERT INTO t [(cols)] <select>`, the query text
// not yet consumed.
//
// PostgreSQL 17.11 matches the query's columns to the target's POSITIONALLY —
// by the explicit list when one is written, by the table's own column order
// otherwise — and a query with FEWER expressions than the target has columns
// is accepted, the remaining columns taking their default (measured:
// `INSERT INTO t(id,n,s) SELECT id, n FROM src` is `INSERT 0 3` with `s` NULL).
// MORE expressions than target columns is 42601. Those are the executor's
// rules; this function only records which query was written.
func parseInsertSelect(stmt string, l *lexer, table string, columns []string) (*ParsedQuery, error) {
	body := strings.TrimSpace(l.rest())
	if body == "" {
		return nil, sqlerr.New("42601", "INSERT INTO %s: a VALUES list or a query is required", table)
	}
	inner, err := parseSelectStatement(body, body)
	if err != nil {
		return nil, err
	}
	return &ParsedQuery{
		Type:      QueryInsert,
		TableName: table,
		SQL:       stmt,
		Insert: &InsertInfo{
			Table:   table,
			Columns: columns,
			Select:  inner,
		},
	}, nil
}

// startsAQuery reports whether the token that follows an INSERT's target
// begins a query rather than a VALUES list.
//
// `TABLE t` — PostgreSQL's spelling for `SELECT * FROM t` — is NOT in the set:
// this engine does not parse it as a statement either, so admitting it here
// would produce a message about a SELECT the client did not write.
func startsAQuery(t TokenType) bool {
	return t == TokenKWSelect || t == TokenKWWith
}

// RefuseUnnamedCTASOutput is the refusal a CTAS owes a query whose output
// column list cannot be named.
//
// PostgreSQL stores the name `?column?` for an unaliased expression and
// refuses a DUPLICATE one with 42701 (`column "?column?" specified more than
// once`, measured on 17.11) — so two unaliased expressions in one CTAS is an
// error there, while the identical SELECT answers fine. The engine's catalog
// raises the same 42701 from checkDistinctColumnNames, which is where this
// statement's duplicate names are caught; this helper exists for the one case
// the catalog cannot see, an EMPTY name, which no relation may carry.
func RefuseUnnamedCTASOutput(names []string) error {
	for i, n := range names {
		if strings.TrimSpace(n) == "" {
			return sqlerr.New("42601",
				"CREATE TABLE AS: output column %d of the query has no name; alias it", i+1)
		}
	}
	return nil
}

// IsCreateTableAsSelect reports whether a statement is
// `CREATE TABLE … AS <select>` without parsing the query it carries.
//
// It exists for the doors that route on the STATEMENT KIND before they parse:
// pgwire decides between its write path and its query path from the text,
// because extended-query Bind/Execute reuses one cached statement across
// several code paths and a full Parse there is wasted work when the answer is
// the query path anyway. This is a bounded token scan — at most
// `CREATE TABLE IF NOT EXISTS name (a, b, …) AS` — and it stops at the first
// token that cannot belong to the prefix.
//
// A CTAS routed as a query would report the tag of the wrong statement: its
// answer is `SELECT <n>` or `CREATE TABLE AS` over NO rows, and the query path
// sends `SELECT 1` over the one row the embedded door boxes a write's tag into
// (#816's shape, for a statement #816 did not yet have).
func IsCreateTableAsSelect(sql string) bool {
	l := newLexer(strings.TrimSpace(sql))
	if l.nextToken().typ != TokenKWCreate {
		return false
	}
	if l.nextToken().typ != TokenKWTable {
		return false
	}
	parseIfNotExists(l)
	if l.nextToken().typ != TokenIdent {
		return false
	}
	switch l.peekToken().typ {
	case TokenKWAs:
		return true
	case TokenLParen:
		if !looksLikeColumnNameList(l) {
			return false
		}
		if _, err := parseColumnNameList(l, "CREATE TABLE"); err != nil {
			return false
		}
		return l.peekToken().typ == TokenKWAs
	}
	return false
}
