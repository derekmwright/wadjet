# Sql statement splitting

Source: internal/planner/sql/split_statements.go — func SplitStatements(sql string) []string {, moved 2026-09-11 (#1026)

SplitStatements splits a SQL string into the statements a client wrote,
separated by TOP-LEVEL semicolons.

It exists because PostgreSQL's simple query protocol carries a whole script
in one message and runs it as a SEQUENCE, one CommandComplete per statement.
Everything here that reached a semicolon before this did
`strings.TrimRight(sql, ";")` and nothing else, so a two-statement string was
ONE statement to every parser below it — and what happened to the tail then
depended on which sub-parser consumed it. `INSERT …; INSERT …` ran the first
and silently dropped the second, and `INSERT …; ZZZ NOT SQL` ran the INSERT
and silently ignored the garbage, where PostgreSQL runs neither (#711).

THE LEXER DECIDES, not strings.Split. A semicolon is a separator only where
it is a semicolon TOKEN at paren depth zero, so

	UPDATE t SET name = 'a;b' WHERE id = 1     -- one statement
	SELECT "a;b" FROM t                        -- one statement
	SELECT 1 -- ; not a separator
	SELECT $$a;b$$                             -- one statement
	SELECT /* ; */ 1                           -- one statement

all stay whole: the lexer reads string literals, dollar-quoted strings,
double-quoted identifiers and both comment forms, so none of them can emit a
TokenSemicolon.

The DEPTH counter is the same guard HasTopLevelWhereToken uses. A semicolon
inside parentheses cannot separate statements in any legal SQL, and treating
one as a separator would cut a statement in half and report the halves'
errors instead of the statement's. When the parens never balance — the input
is malformed — depth never returns to zero, nothing is split, and the whole
string goes to Parse, which is where a malformed statement's error belongs.

A lex ERROR (an unterminated string literal, say) stops the scan and the
unconsumed remainder is returned as one final piece, for the same reason:
the error is the statement's, and Parse is what reports it.

A piece with NO STATEMENT IN IT is dropped, and that means whitespace,
semicolons AND COMMENTS. `SELECT 1; -- trailing comment` is one statement in
PostgreSQL and was one statement here before this function existed; treating
the comment as a second statement made that string — and `…; /* banner */`,
and a stray `--` from an editor, and the `-- query tag` every ORM appends —
a two-statement string that every one-statement door then refused with 42601.
Trimming whitespace alone is not enough for the same reason `strings.Split`
is not enough one paragraph up: only the lexer knows what a comment is.
A piece whose first token is EOF holds nothing to run.

An input with no statement at all yields nil, which callers read as
PostgreSQL's EmptyQuery — and PostgreSQL answers a comment-only query string
with exactly that.
