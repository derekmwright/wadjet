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
// A piece with NO STATEMENT IN IT is dropped, and that means whitespace,
// semicolons AND COMMENTS. `SELECT 1; -- trailing comment` is one statement in
// PostgreSQL and was one statement here before this function existed; treating
// the comment as a second statement made that string — and `…; /* banner */`,
// and a stray `--` from an editor, and the `-- query tag` every ORM appends —
// a two-statement string that every one-statement door then refused with 42601.
// Trimming whitespace alone is not enough for the same reason `strings.Split`
// is not enough one paragraph up: only the lexer knows what a comment is.
// A piece whose first token is EOF holds nothing to run.
//
// An input with no statement at all yields nil, which callers read as
// PostgreSQL's EmptyQuery — and PostgreSQL answers a comment-only query string
// with exactly that.
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
