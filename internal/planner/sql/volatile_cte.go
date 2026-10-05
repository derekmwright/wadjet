// SPDX-License-Identifier: MIT

package sql

import (
	"strings"
	"sync/atomic"
)

// volatileOracle answers whether a call to a function may answer differently
// for the same arguments. The function registry owns the answer (a builtin by
// its mark, a CREATE FUNCTION by its body — expr.IsVolatileFunction) and
// installs it here; this package cannot import the registry.
var volatileOracle atomic.Pointer[func(name string) bool]

// SetVolatileFunctionOracle installs the registry's volatility answer.
func SetVolatileFunctionOracle(f func(name string) bool) {
	volatileOracle.Store(&f)
}

func callIsVolatile(name string) bool {
	if f := volatileOracle.Load(); f != nil {
		return (*f)(name)
	}
	return false
}

// TextCallsAny reports whether the text calls a function for which pred
// answers true. It reads TOKENS, so the words inside a string literal do not
// count; a call is a name followed by `(` (a qualified `pg_catalog.random(`
// included), so a column named `random` does not either.
func TextCallsAny(sql string, pred func(name string) bool) bool {
	l := newLexer(sql)
	prev := token{typ: TokenEOF}
	for {
		t := l.nextToken()
		switch {
		case t.typ == TokenEOF || t.typ == TokenError:
			return false
		case t.typ == TokenLParen && prev.typ == TokenIdent && !prev.quoted && pred(strings.ToLower(prev.val)):
			return true
		}
		prev = t
	}
}

// TextIsVolatile reports whether a block's text calls a volatile function (as
// the function registry answers it) or samples a relation (TABLESAMPLE)
// anywhere — its SELECT list, its WHERE, a derived table, a nested WITH, or
// an expression subquery the plan carries only as text.
func TextIsVolatile(sql string) bool {
	l := newLexer(sql)
	for {
		t := l.nextToken()
		if t.typ == TokenEOF || t.typ == TokenError {
			break
		}
		if t.typ == TokenIdent && !t.quoted && strings.EqualFold(t.val, "TABLESAMPLE") {
			return true
		}
	}
	return TextCallsAny(sql, callIsVolatile)
}

// relationRefs counts the places a text names `name` as a RELATION: an
// identifier after FROM, JOIN, TABLE, a comma or an opening parenthesis that
// is neither a call (`name(`) nor a qualifier (`name.` / `.name`). It reads
// TOKENS: a string literal never counts.
//
// The count may only err HIGH. A name that is a column in a SELECT list
// (`SELECT a, s FROM …`) counts, and so does a reference a nested WITH
// shadows; an over-count costs a shared evaluation of a body read once, which
// answers the same rows. An under-count would evaluate a volatile body per
// reference. So a reference inside a block that declares its OWN WITH counts
// TWICE: that block's WITH items are inlined at each of their references, and
// a reference in one of their bodies is read once per such reference.
func relationRefs(sql, name string) int {
	l := newLexer(sql)
	var toks []token
	for {
		t := l.nextToken()
		if t.typ == TokenEOF || t.typ == TokenError {
			break
		}
		toks = append(toks, t)
	}
	n, depth := 0, 0
	var withDepths []int // the paren depth of each WITH still open
	for i, t := range toks {
		switch t.typ {
		case TokenLParen:
			depth++
		case TokenRParen:
			depth--
			for len(withDepths) > 0 && withDepths[len(withDepths)-1] > depth {
				withDepths = withDepths[:len(withDepths)-1]
			}
		case TokenKWWith:
			withDepths = append(withDepths, depth)
		case TokenIdent:
			if !strings.EqualFold(t.val, name) || i == 0 {
				continue
			}
			switch toks[i-1].typ {
			case TokenKWFrom, TokenKWJoin, TokenKWTable, TokenComma, TokenLParen:
			default:
				continue
			}
			if i+1 < len(toks) && (toks[i+1].typ == TokenLParen || toks[i+1].typ == TokenDot) {
				continue
			}
			if len(withDepths) > 0 {
				n += 2
			} else {
				n++
			}
		}
	}
	return n
}

// stampCTEReads records, on each item of one WITH list, how many times the
// statement reads it and whether its body is volatile — the two facts
// EvaluatedOnce decides by. main is the statement after the list.
//
// An item is read by the statement's text (every nested block and expression
// subquery included) and by the bodies of the items after it. An item whose
// body reads an earlier volatile item is volatile too: it is the earlier
// one's reader, and if it were inlined at each of its references the earlier
// body would be read once per reference.
func stampCTEReads(defs []CTEDef, main string) {
	for i := range defs {
		if defs[i].Recursive {
			continue
		}
		reads := relationRefs(main, defs[i].Name)
		for j := i + 1; j < len(defs); j++ {
			reads += relationRefs(defs[j].SQL, defs[i].Name)
		}
		defs[i].reads = reads
		volatile := TextIsVolatile(defs[i].SQL)
		for j := 0; j < i && !volatile; j++ {
			volatile = defs[j].volatile && relationRefs(defs[i].SQL, defs[j].Name) > 0
		}
		defs[i].volatile = volatile
	}
}

// CTEIdentity is the identity of ONE WITH-list item as the statement wrote
// it. CTEDef is passed by VALUE through every scope (an enclosing block's list
// is copied into each nested block's, and into each expression subquery's),
// so neither a pointer to a CTEDef nor its name tells two references of the
// same item from references of two items: the identity rides inside the value
// and every copy carries the same one.
type CTEIdentity struct{ _ byte } // non-zero size: distinct allocations, distinct pointers

// Identity is the item's identity, or nil for a CTEDef that the parser did not
// produce (a synthesized definition has no statement position to share).
func (c *CTEDef) Identity() *CTEIdentity {
	if c == nil {
		return nil
	}
	return c.ident
}

// EvaluatedOnce reports whether every reference to this WITH item must read
// ONE evaluation of its body: a non-recursive body that is volatile (a
// volatile function, TABLESAMPLE, or a read of an earlier volatile item) and
// that the statement reads MORE THAN ONCE. PostgreSQL materializes a CTE
// referenced more than once, so every reference sees the same rows; a CTE
// read once is read by one reader, and is planned as any other block. A
// recursive CTE is always materialized to its fixed point and is not this
// rule's.
func (c *CTEDef) EvaluatedOnce() bool {
	return c != nil && !c.Recursive && c.ident != nil && c.volatile && c.reads >= 2
}

// Reads is how many times the statement reads this WITH item (an upper
// bound: see relationRefs).
func (c *CTEDef) Reads() int { return c.reads }
