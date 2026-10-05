// SPDX-License-Identifier: MIT

package sql

import "strings"

// VolatileFunctions are the functions this engine registers whose value is
// not fixed by their arguments: two calls with the same arguments may answer
// differently. PostgreSQL marks its own spellings of these VOLATILE
// (random(), gen_random_uuid()); rand() and uuid() are this engine's aliases.
// The clock functions are NOT here: a statement reads its clock once (arc SC).
var VolatileFunctions = map[string]bool{
	"rand":            true,
	"random":          true,
	"uuid":            true,
	"gen_random_uuid": true,
}

// TextIsVolatile reports whether a block's text calls a volatile function or
// samples a relation (TABLESAMPLE) anywhere — its SELECT list, its WHERE, a
// derived table, a nested WITH, or an expression subquery the plan carries
// only as text. It reads TOKENS, so the words inside a string literal do not
// count; a function is a name followed by `(` (a qualified
// `pg_catalog.random(` included), so a column named `random` does not either.
func TextIsVolatile(sql string) bool {
	l := newLexer(sql)
	prev := token{typ: TokenEOF}
	for {
		t := l.nextToken()
		switch {
		case t.typ == TokenEOF || t.typ == TokenError:
			return false
		case t.typ == TokenIdent && !t.quoted && strings.EqualFold(t.val, "TABLESAMPLE"):
			return true
		case t.typ == TokenLParen && prev.typ == TokenIdent && VolatileFunctions[strings.ToLower(prev.val)]:
			return true
		}
		prev = t
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
// volatile function or TABLESAMPLE). PostgreSQL materializes such a CTE —
// it never inlines a body that contains a volatile function, and it
// materializes a CTE referenced more than once — so every reference sees the
// same rows. A recursive CTE is always materialized to its fixed point and is
// not this rule's.
func (c *CTEDef) EvaluatedOnce() bool {
	return c != nil && !c.Recursive && c.ident != nil && TextIsVolatile(c.SQL)
}
