package sql

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// SplitStatements uses LEXER semicolon tokens at paren depth zero (#711).
// Never split inside strings, dollar quotes, delimited identifiers or comments.
// Unbalanced parentheses retain the unsplit remainder; lexer errors also leave
// one final piece so Parse reports the statement's error.
// Drop pieces containing only whitespace/semicolons/comments (first token EOF).
// No statement yields nil for EmptyQuery; comment-only input is empty too.
// Clients can then run the script as a sequence with one completion per statement.
// See docs/internals/sql-statement-splitting.md for the design.
func SplitStatements(sql string) []string {
	l := newLexer(sql)
	depth := 0
	start := 0
	var out []string
	add := func(s string) {
		if s = strings.TrimSpace(s); s == "" || !holdsAStatement(s) {
			return
		}
		out = append(out, s)
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

// holdsAStatement reports whether a piece contains anything to run, which is
// the lexer's question and not a regexp's: a comment can carry a semicolon, a
// quote and a `/*` of its own, and the lexer is where this file already trusts
// that knowledge (skipWhitespace treats both comment forms as whitespace).
//
// An UNTERMINATED block or line comment reads to end of input and leaves EOF,
// so `SELECT 1; /* never closed` is one statement here. PostgreSQL raises
// `unterminated /* comment` for it; wadjet answered it before this arc and
// still does, which is a "PostgreSQL rejects, wadjet answers" superset and not
// something this function narrows or widens.
func holdsAStatement(piece string) bool {
	return newLexer(piece).nextToken().typ != TokenEOF
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
	return multiStatementError(stmts)
}

// multiStatementError is the refusal itself, shared with Parse so the two
// doors cannot word the same rule differently.
func multiStatementError(stmts []string) error {
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
