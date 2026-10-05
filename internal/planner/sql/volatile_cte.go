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
// function the registry answers volatile, or TABLESAMPLE). PostgreSQL materializes such a CTE —
// it never inlines a body that contains a volatile function, and it
// materializes a CTE referenced more than once — so every reference sees the
// same rows. A recursive CTE is always materialized to its fixed point and is
// not this rule's.
func (c *CTEDef) EvaluatedOnce() bool {
	return c != nil && !c.Recursive && c.ident != nil && TextIsVolatile(c.SQL)
}
