package sql

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// SplitStatements splits a SQL string into the statements a client wrote,
// separated by TOP-LEVEL semicolons.
//
// It exists because PostgreSQL's simple query protocol carries a whole script
// in one message and runs it as a SEQUENCE, one CommandComplete per statement.
// Everything here that reached a semicolon before this did
// `strings.TrimRight(sql, ";")` and nothing else, so a two-statement string was
// ONE statement to every parser below it — and what happened to the tail then
// depended on which sub-parser consumed it. `INSERT …; INSERT …` ran the first
// and silently dropped the second, and `INSERT …; ZZZ NOT SQL` ran the INSERT
// and silently ignored the garbage, where PostgreSQL runs neither (#711).
//
// THE LEXER DECIDES, not strings.Split. A semicolon is a separator only where
// it is a semicolon TOKEN at paren depth zero, so
//
//	UPDATE t SET name = 'a;b' WHERE id = 1     -- one statement
//	SELECT "a;b" FROM t                        -- one statement
//	SELECT 1 -- ; not a separator
//	SELECT $$a;b$$                             -- one statement
//	SELECT /* ; */ 1                           -- one statement
//
// all stay whole: the lexer reads string literals, dollar-quoted strings,
// double-quoted identifiers and both comment forms, so none of them can emit a
// TokenSemicolon.
//
// The DEPTH counter is the same guard HasTopLevelWhereToken uses. A semicolon
// inside parentheses cannot separate statements in any legal SQL, and treating
// one as a separator would cut a statement in half and report the halves'
// errors instead of the statement's. When the parens never balance — the input
// is malformed — depth never returns to zero, nothing is split, and the whole
// string goes to Parse, which is where a malformed statement's error belongs.
//
// A lex ERROR (an unterminated string literal, say) stops the scan and the
// unconsumed remainder is returned as one final piece, for the same reason:
// the error is the statement's, and Parse is what reports it.
//
// Empty pieces are dropped, so a trailing `;`, a doubled `;;` and a string of
// only whitespace and semicolons all yield what they mean. An input with no
// statement at all yields nil, which callers read as PostgreSQL's EmptyQuery.
func SplitStatements(sql string) []string {
	l := newLexer(sql)
	depth := 0
	start := 0
	var out []string
	add := func(s string) {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	for {
		tok := l.nextToken()
		switch tok.typ {
		case TokenEOF, TokenError:
			add(sql[start:])
			return out
		case TokenLParen:
			depth++
		case TokenRParen:
			if depth > 0 {
				depth--
			}
		case TokenSemicolon:
			if depth == 0 {
				add(sql[start:tok.pos])
				start = tok.pos + 1
			}
		}
	}
}

// IsMultiStatement reports whether sql carries more than one statement.
//
// It is the predicate every ONE-STATEMENT entry point owes: the extended
// protocol, a prepared statement, and the embedded `wadjet.DB` API each return
// exactly one result, so a string carrying two statements has no answer they
// can give. PostgreSQL refuses it there with 42601, `cannot insert multiple
// commands into a prepared statement`, and accepts it only on the simple query
// protocol.
func IsMultiStatement(sql string) bool {
	return len(SplitStatements(sql)) > 1
}

// CheckSingleStatement refuses a string that carries more than one statement,
// the way PostgreSQL refuses one at an entry point that answers with a single
// result.
//
// THE ORDER IS PART OF THE ANSWER, and it is measured against 17.11 rather
// than remembered. PostgreSQL parses the whole string first, so
//
//	INSERT INTO t (id) VALUES (1); INSERT INTO t (id) VALUES (2)
//	    -> 42601  cannot insert multiple commands into a prepared statement
//	INSERT INTO t (id) VALUES (1); ZZZ NOT SQL
//	    -> 42601  syntax error at or near "ZZZ"
//
// A syntax error anywhere in the string outranks the multi-command refusal,
// and reporting the multi-command message for the second one would tell a
// client its SQL was fine when it was not. Both carry 42601.
//
// A single statement costs nothing here: the string is not parsed twice,
// because the caller is about to parse it anyway.
func CheckSingleStatement(sql string) error {
	stmts := SplitStatements(sql)
	if len(stmts) < 2 {
		return nil
	}
	for _, stmt := range stmts {
		if _, err := parseDispatch(stmt); err != nil {
			if sqlerr.StateOf(err) != "" {
				return err
			}
			return sqlerr.Wrap("42601", err)
		}
	}
	return sqlerr.New("42601", "cannot insert multiple commands into a prepared statement")
}
