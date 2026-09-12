package sql

import "strings"

// SplitLastTopLevelUnionAll splits a query's TEXT at the LAST top-level
// `UNION ALL`, returning the text to its left and to its right.
//
// It exists so that a caller which has already decided something from the
// PARSED tree can recover the two arms' text without writing a second scanner.
// It runs the LEXER, so it cannot match the letters `union all` inside an
// identifier, a delimited name, a string literal or a comment — a hand-rolled
// scan did, and `WITH RECURSIVE r AS (SELECT 1 AS v UNION ALL SELECT v+1 AS
// unionall FROM r WHERE v<3)` was refused as "not the form" where PostgreSQL
// answers 1, 2, 3 (arc C1 round-3 review, B1). A scanner that disagrees with
// the parse can only ever be wrong.
//
// The LAST one is what a left-associative parse puts at the root, so the right
// half is the final arm. `ok` is false when the text holds no top-level
// `UNION ALL` at all.
func SplitLastTopLevelUnionAll(sql string) (left, right string, ok bool) {
	lx := newLexer(sql)
	depth := 0
	for {
		t := lx.nextToken()
		switch t.typ {
		case TokenEOF, TokenError:
			return left, right, ok
		case TokenLParen:
			depth++
			continue
		case TokenRParen:
			depth--
			continue
		case TokenKWUnion:
			if depth != 0 {
				continue
			}
		default:
			continue
		}
		// A top-level UNION: the NEXT token decides whether it is UNION ALL,
		// and where the right arm starts.
		start := t.pos
		next := lx.nextToken()
		if next.typ != TokenKWAll {
			continue
		}
		end := next.pos + len(next.source())
		if end > len(sql) {
			end = len(sql)
		}
		left, right, ok = strings.TrimSpace(sql[:start]), strings.TrimSpace(sql[end:]), true
	}
}
