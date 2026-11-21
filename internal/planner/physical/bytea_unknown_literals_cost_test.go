// SPDX-License-Identifier: MIT

package physical

import (
	"strings"
	"sync/atomic"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestByteaLiteralTypingIsFreeWithoutQuotedLiteral pins #1590 r4: the bytea
// literal typing pass computes NO declaration for a statement holding no
// quoted literal. Before it, every join asked both sides' declarations
// whatever its condition held (joinSideDecls), which put 264 declaration
// computations on a twelve-way join and 36 on the derived-depth shapes of
// wadjet.TestArcCI2DeclarationPlanningBound. The statements cover each
// construct the pass visits: a join (condition and keys), a set operation,
// an aggregate's GROUP BY key and a window over it, a derived table, and
// COALESCE / CASE / comparisons / || over a BYTES column with no literal.
// The last two cases are the probe's positive controls: a statement that
// does hold a bytea literal computes declarations and is typed.
func TestByteaLiteralTypingIsFreeWithoutQuotedLiteral(t *testing.T) {
	cols := map[string]map[string]parquet.TypeID{
		"u_b": {"id": parquet.TypeInt32, "b": parquet.TypeBytes, "b2": parquet.TypeBytes, "s": parquet.TypeString},
		"u_k": {"id": parquet.TypeInt32, "kb": parquet.TypeBytes},
	}
	build := func(sql string) *logical.Node {
		t.Helper()
		parsed, err := plansql.Parse(sql)
		if err != nil {
			t.Fatalf("parse %q: %v", sql, err)
		}
		info, err := plansql.ExtractSelect(parsed)
		if err != nil {
			t.Fatalf("extract %q: %v", sql, err)
		}
		lp, err := logical.BuildFromSelect(info)
		if err != nil {
			t.Fatalf("build %q: %v", sql, err)
		}
		var stamp func(n *logical.Node)
		stamp = func(n *logical.Node) {
			if n == nil {
				return
			}
			if n.Type == logical.NodeScan {
				if c, ok := cols[n.TableName]; ok {
					n.ScanColTypes = map[string]parquet.TypeID{}
					n.ScanColumns = nil
					for _, name := range []string{"id", "b", "b2", "s", "kb"} {
						if typ, ok := c[name]; ok {
							n.ScanColTypes[name] = typ
							n.ScanColumns = append(n.ScanColumns, name)
						}
					}
				}
			}
			for _, ch := range n.Children {
				stamp(ch)
			}
		}
		stamp(lp)
		return lp
	}
	count := func(sql string) (int, string) {
		t.Helper()
		lp := build(sql)
		// Every declaration computed while the pass runs, its own walk's and
		// any fresh walk a helper starts (the set operation's arm types):
		// the probe wadjet.TestArcCI2DeclarationPlanningBound counts. No
		// test in this package runs in parallel, so the count is the pass's.
		var n atomic.Int64
		restore := SetDeclWalkProbe(func(string) { n.Add(1) })
		(&Planner{}).typeByteaLiterals(lp)
		restore()
		var texts []string
		var collect func(n *logical.Node)
		collect = func(n *logical.Node) {
			if n == nil {
				return
			}
			for _, pr := range n.Projections {
				if pr.ASTExpr != nil {
					texts = append(texts, pr.ASTExpr.String())
				}
			}
			texts = append(texts, n.JoinCond)
			texts = append(texts, n.LeftKeys...)
			texts = append(texts, n.RightKeys...)
			for _, ch := range n.Children {
				collect(ch)
			}
		}
		collect(lp)
		return int(n.Load()), strings.Join(texts, " | ")
	}
	free := []string{
		"SELECT a.id, a.b FROM u_b a JOIN u_k k ON a.id = k.id",
		"SELECT a.id FROM u_b a JOIN u_k k ON a.b = k.kb AND a.id > k.id",
		"SELECT b FROM u_b UNION ALL SELECT kb FROM u_k",
		"SELECT b FROM u_b UNION SELECT kb FROM u_k EXCEPT SELECT b2 FROM u_b",
		"SELECT COALESCE(b, b2) AS k, count(*), sum(count(*)) OVER (PARTITION BY COALESCE(b, b2)) FROM u_b GROUP BY COALESCE(b, b2)",
		"SELECT q.k FROM (SELECT b || b2 AS k FROM u_b WHERE b = b2) q JOIN u_k ON q.k = u_k.kb",
		"SELECT CASE WHEN id > 1 THEN b ELSE b2 END, GREATEST(b, b2), NULLIF(b, b2) FROM u_b WHERE b IN (b2) AND b BETWEEN b2 AND b",
		"SELECT id, LAG(b, 1) OVER (ORDER BY id) FROM u_b",
	}
	for _, sql := range free {
		if n, _ := count(sql); n != 0 {
			t.Errorf("%s\n  the bytea literal typing pass computed %d declarations, want 0: a statement with no quoted literal pays for a pass that cannot change it", sql, n)
		}
	}
	// Positive controls: the probe sees the pass's computations, and the
	// literal is typed.
	for _, sql := range []string{
		"SELECT id, COALESCE(b, '\\x6869') FROM u_b",
		"SELECT a.id FROM u_b a JOIN u_k k ON COALESCE(a.b, '\\x41') = k.kb",
	} {
		n, plan := count(sql)
		if n == 0 {
			t.Errorf("%s\n  computed no declaration: the probe does not see the pass", sql)
		}
		if !strings.Contains(plan, "as bytes)") {
			t.Errorf("%s\n  the literal was not typed bytea:\n%s", sql, plan)
		}
	}
}
