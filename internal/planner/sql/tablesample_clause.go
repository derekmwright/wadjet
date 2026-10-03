// SPDX-License-Identifier: MIT

package sql

import "strings"

// HasTablesample reports whether a statement's text holds a TABLESAMPLE
// clause anywhere — the outer FROM, a derived table, a CTE body, or an
// expression subquery the plan carries only as text. It reads tokens, so the
// word inside a string literal or a delimited identifier does not count.
//
// A caller that hands the same statement text to several tasks, each of which
// re-plans it, uses this to decline: every task would draw its own sample of
// each sampled relation, where the statement draws one (#1411).
func HasTablesample(sql string) bool {
	l := newLexer(sql)
	for {
		t := l.nextToken()
		switch {
		case t.typ == TokenEOF || t.typ == TokenError:
			return false
		case t.typ == TokenIdent && !t.quoted && strings.EqualFold(t.val, "TABLESAMPLE"):
			return true
		}
	}
}
