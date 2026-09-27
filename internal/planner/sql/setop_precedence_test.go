// SPDX-License-Identifier: MIT

package sql

import (
	"strings"
	"testing"
)

// SET-OPERATION PRECEDENCE (#1349): the parser builds the tree PostgreSQL's
// grammar builds. INTERSECT [ALL] binds tighter than UNION [ALL] and EXCEPT
// [ALL]; within one level the chain is left-associative; a parenthesised arm
// is a leaf. Every consumer reads this tree as it stands, so the shape
// asserted here is the answer's shape on every arm.
//
// Each arm names its own relation (a, b, c, d, e), and setOpTree renders the
// tree with every operator's operands parenthesised, so a wrong grouping is a
// different string. An arm-local ORDER BY / LIMIT renders as `[o]` / `[l]` on
// the leaf that carries it; the statement's own renders after the tree.
func setOpTree(info *SelectInfo) string {
	var b strings.Builder
	var walk func(n *SelectInfo)
	walk = func(n *SelectInfo) {
		if n.Union != nil {
			b.WriteString("(")
			walk(n.Union.Left)
			b.WriteString(" " + string(n.Union.Op))
			if n.Union.All {
				b.WriteString(" ALL")
			}
			b.WriteString(" ")
			walk(n.Union.Right)
			b.WriteString(")")
		} else if len(n.Tables) > 0 {
			b.WriteString(n.Tables[0].Name)
		} else {
			b.WriteString("?")
		}
		if n != info {
			if len(n.OrderBy) > 0 {
				b.WriteString("[o]")
			}
			if n.Limit != "" {
				b.WriteString("[l]")
			}
		}
	}
	walk(info)
	if len(info.OrderBy) > 0 {
		b.WriteString(" ORDER")
	}
	if info.Limit != "" {
		b.WriteString(" LIMIT " + info.Limit)
	}
	if info.Offset != "" {
		b.WriteString(" OFFSET " + info.Offset)
	}
	return b.String()
}

func TestSetOpChainIsBuiltByPrecedence(t *testing.T) {
	s := func(name string) string { return "SELECT x FROM " + name }
	a, b, c, d, e := s("a"), s("b"), s("c"), s("d"), s("e")
	cases := []struct{ sql, want string }{
		// ---- one level: left-associative, as before.
		{a + " UNION " + b + " UNION " + c, "((a UNION b) UNION c)"},
		{a + " UNION ALL " + b + " EXCEPT " + c, "((a UNION ALL b) EXCEPT c)"},
		{a + " EXCEPT " + b + " UNION " + c, "((a EXCEPT b) UNION c)"},
		{a + " EXCEPT " + b + " EXCEPT " + c, "((a EXCEPT b) EXCEPT c)"},
		{a + " EXCEPT ALL " + b + " EXCEPT " + c, "((a EXCEPT ALL b) EXCEPT c)"},
		{a + " INTERSECT " + b + " INTERSECT ALL " + c, "((a INTERSECT b) INTERSECT ALL c)"},
		// ---- INTERSECT binds tighter, on either side.
		{a + " UNION " + c + " INTERSECT " + b, "(a UNION (c INTERSECT b))"},
		{a + " UNION ALL " + b + " INTERSECT ALL " + c, "(a UNION ALL (b INTERSECT ALL c))"},
		{a + " EXCEPT " + b + " INTERSECT " + c, "(a EXCEPT (b INTERSECT c))"},
		{a + " EXCEPT ALL " + b + " INTERSECT ALL " + c, "(a EXCEPT ALL (b INTERSECT ALL c))"},
		{a + " INTERSECT " + b + " UNION " + c, "((a INTERSECT b) UNION c)"},
		{a + " INTERSECT " + b + " EXCEPT " + c, "((a INTERSECT b) EXCEPT c)"},
		{a + " INTERSECT ALL " + b + " EXCEPT ALL " + c, "((a INTERSECT ALL b) EXCEPT ALL c)"},
		// ---- four and five arms.
		{a + " INTERSECT " + b + " UNION " + c + " INTERSECT " + d, "((a INTERSECT b) UNION (c INTERSECT d))"},
		{a + " UNION " + b + " INTERSECT " + c + " INTERSECT " + d, "(a UNION ((b INTERSECT c) INTERSECT d))"},
		{a + " UNION " + b + " INTERSECT " + c + " EXCEPT " + d, "((a UNION (b INTERSECT c)) EXCEPT d)"},
		{a + " EXCEPT " + b + " INTERSECT ALL " + c + " UNION ALL " + d + " INTERSECT " + e,
			"((a EXCEPT (b INTERSECT ALL c)) UNION ALL (d INTERSECT e))"},
		{a + " INTERSECT " + b + " INTERSECT " + c + " UNION " + d + " EXCEPT " + e,
			"((((a INTERSECT b) INTERSECT c) UNION d) EXCEPT e)"},
		{a + " UNION ALL " + b + " UNION ALL " + c + " INTERSECT " + d + " INTERSECT ALL " + e,
			"((a UNION ALL b) UNION ALL ((c INTERSECT d) INTERSECT ALL e))"},
		// ---- parentheses override both rules.
		{"(" + a + " UNION " + c + ") INTERSECT " + b, "((a UNION c) INTERSECT b)"},
		{a + " UNION (" + c + " INTERSECT " + b + ")", "(a UNION (c INTERSECT b))"},
		{a + " EXCEPT (" + b + " EXCEPT " + c + ")", "(a EXCEPT (b EXCEPT c))"},
		{a + " INTERSECT (" + b + " UNION " + c + ")", "(a INTERSECT (b UNION c))"},
		{"(" + a + " INTERSECT " + b + " UNION " + c + ") INTERSECT " + d, "(((a INTERSECT b) UNION c) INTERSECT d)"},
		{"((" + a + ")) UNION (" + b + ") INTERSECT (" + c + ")", "(a UNION (b INTERSECT c))"},
		// ---- the statement's ORDER BY / LIMIT / OFFSET attach to the outermost
		// result; an arm's own stays on its (parenthesised) leaf.
		{a + " UNION " + c + " INTERSECT " + b + " ORDER BY 1 LIMIT 3 OFFSET 1",
			"(a UNION (c INTERSECT b)) ORDER LIMIT 3 OFFSET 1"},
		{a + " INTERSECT " + b + " EXCEPT " + c + " OFFSET 2 LIMIT 4",
			"((a INTERSECT b) EXCEPT c) LIMIT 4 OFFSET 2"},
		{a + " UNION (" + c + " ORDER BY 1 LIMIT 2) INTERSECT " + b + " ORDER BY 1",
			"(a UNION (c[o][l] INTERSECT b)) ORDER"},
		{a + " UNION " + c + " INTERSECT " + b + " FETCH FIRST 5 ROWS ONLY",
			"(a UNION (c INTERSECT b)) LIMIT 5"},
	}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			parsed, err := Parse(tc.sql)
			if err != nil {
				t.Fatal(err)
			}
			info, err := ExtractSelect(parsed)
			if err != nil {
				t.Fatal(err)
			}
			if got := setOpTree(info); got != tc.want {
				t.Errorf("tree\n  got  %s\n  want %s", got, tc.want)
			}
		})
	}
}

// A chain inside a derived table, a CTE body, an IN body and a scalar subquery
// is parsed from its own text by the same parseSelectOrUnion, so it takes the
// same shape: asserted on the derived table's and the CTE's parsed body.
func TestSetOpChainInASubBlockIsBuiltByPrecedence(t *testing.T) {
	const chain = "SELECT x FROM a UNION SELECT x FROM c INTERSECT SELECT x FROM b"
	const want = "(a UNION (c INTERSECT b))"

	t.Run("derived table", func(t *testing.T) {
		parsed, err := Parse("SELECT s.x FROM (" + chain + ") s")
		if err != nil {
			t.Fatal(err)
		}
		info, err := ExtractSelect(parsed)
		if err != nil {
			t.Fatal(err)
		}
		if len(info.Tables) != 1 {
			t.Fatalf("tables: %d", len(info.Tables))
		}
		body, err := info.Tables[0].SubSelect()
		if err != nil || body == nil {
			t.Fatalf("derived body: %v %v", body, err)
		}
		if got := setOpTree(body); got != want {
			t.Errorf("tree\n  got  %s\n  want %s", got, want)
		}
	})
	t.Run("CTE body", func(t *testing.T) {
		parsed, err := Parse("WITH q AS (" + chain + ") SELECT x FROM q")
		if err != nil {
			t.Fatal(err)
		}
		info, err := ExtractSelect(parsed)
		if err != nil {
			t.Fatal(err)
		}
		if len(info.CTEs) != 1 {
			t.Fatalf("ctes: %d", len(info.CTEs))
		}
		body, err := info.CTEs[0].BodySelect()
		if err != nil {
			t.Fatal(err)
		}
		if got := setOpTree(body); got != want {
			t.Errorf("tree\n  got  %s\n  want %s", got, want)
		}
	})
}
