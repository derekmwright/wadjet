// SPDX-License-Identifier: MIT

package logical

import (
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// Unit cover for ResolveFilterThroughProjects' rules, which the two-path
// gates in internal/coordinator exercise end to end but cannot localize:
// which relation a qualified reference names, what a dotted reference through
// a rename means, and where the walk has to stop.

// resolveTopFilter builds sql, finds the outermost Filter, and returns the
// re-spelling the DAG would ship for its first predicate ("" = unchanged).
func resolveTopFilter(t *testing.T, sql string) string {
	t.Helper()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	plan, err := BuildFromSelect(info)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	filter := findTopFilter(Optimize(plan))
	if filter == nil {
		t.Fatalf("no Filter node in the optimized plan for %q", sql)
	}
	ast, _, ok := ResolveFilterThroughProjects(filter.Predicates[0], filter.Children[0], nil)
	if !ok {
		return ""
	}
	return ast.String()
}

func findTopFilter(n *Node) *Node {
	if n == nil {
		return nil
	}
	if n.Type == NodeFilter && len(n.Children) == 1 && len(n.Predicates) > 0 {
		return n
	}
	for _, c := range n.Children {
		if f := findTopFilter(c); f != nil {
			return f
		}
	}
	return nil
}

func TestResolveFilterThroughProjectsScoping(t *testing.T) {
	for _, c := range []struct {
		name, sql, want string
	}{
		// The rename resolves; the sibling arm's reference does not move.
		// Rewriting `d.k` with the CTE's `id AS k` was a silent wrong answer
		// (#653 follow-up): the qualifier is what decides.
		{"QualifiedToEachArm",
			`WITH c AS (SELECT id AS k, g AS gg FROM t) SELECT COUNT(*) FROM c JOIN dim d ON c.gg = d.k WHERE d.k > 3 OR c.gg > 100`,
			"d.k > 3 or g > 100"},
		{"QualifiedToSiblingArmOnly",
			`WITH c AS (SELECT id AS k, g AS gg FROM t) SELECT COUNT(*) FROM c JOIN dim d ON c.gg = d.k WHERE d.k > 3 OR d.k < 1`,
			""},
		// ADR-0022 §1's order, which is expr.ResolveColumnRef's: the BARE
		// column after dropping the qualifier is tried BEFORE the qualifier
		// is read as a ROW container. `b` is a column of this projection, so
		// `rw.b` is `id` — the run-time lookup finds it before it considers a
		// field, and resolving in the other order described a different
		// column on the DAG than the single-process engine evaluated.
		{"DottedRefPrefersTheColumnOverTheField",
			`WITH c AS (SELECT c_row AS rw, id AS b FROM t) SELECT COUNT(*) FROM c WHERE rw.b > 100`,
			"id > 100"},
		// With no competing column the same spelling IS the field path, and
		// the QUALIFIER is what gets substituted.
		{"DottedRefFallsToTheFieldPath",
			`WITH c AS (SELECT c_row AS rw, id FROM t) SELECT COUNT(*) FROM c WHERE rw.b > 100`,
			"c_row.b > 100"},
		{"FieldPathThroughPassthrough",
			`WITH c AS (SELECT c_row, id FROM t) SELECT COUNT(*) FROM c WHERE c_row.b > 100`,
			""},
		// Chained renames still chain.
		{"NestedRename",
			`WITH a AS (SELECT c_i64 AS u FROM t), b AS (SELECT u AS v FROM a) SELECT COUNT(*) FROM b WHERE v > 0`,
			"c_i64 > 0"},
		// A Sort or a LIMIT emits a stage, and that stage's STREAM is
		// whatever its producer emits — source columns, unless some pass put
		// an alias-naming projection on the producing fragment. This walk
		// cannot know which, so it resolves through both and the alias
		// spelling travels beside the result (dagplan.Stage.FilterAliases);
		// dagplan.resolveFilterAliasSpelling picks once that is settled.
		//
		// Declining here instead was what made #656 shapes a–d silent: the
		// predicate kept an alias no stage under the sort emitted, and — on
		// the merge_sort a later pass deletes — was dropped outright.
		{"ResolvesThroughSort",
			`WITH c AS (SELECT id, c_i64 AS v FROM t ORDER BY id) SELECT id FROM c WHERE v > 0`,
			"c_i64 > 0"},
		{"ResolvesThroughLimit",
			`WITH c AS (SELECT id, c_i64 AS v FROM t ORDER BY id LIMIT 10) SELECT id FROM c WHERE v > 0`,
			"c_i64 > 0"},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := resolveTopFilter(t, c.sql); !strings.EqualFold(got, c.want) {
				t.Fatalf("resolved to %q, want %q\n  SQL: %s", got, c.want, c.sql)
			}
		})
	}
}

// A JOIN'S OWN STAGE QUALIFIES WHAT THE BUILD ARM BROUGHT IN (#1678 line j).
// The build arm's definition `upper(s)` was spelled in the arm's scope; on the
// join's stream a bare `s` binds the PROBE's, so the predicate compared the
// probe with itself. With a qualifier only the build arm's substituted bare
// references take it: the query's own references, the probe arm's
// substitutions and a definition already qualified stay as written, no
// qualifier (every carrier but a join stage) leaves the old spelling, and the
// logical plan's own definitions are never written to.
func TestResolveFilterThroughProjectsQualifiesBuildArmDefinitions(t *testing.T) {
	qual := func(string) string { return "bq" }
	defs := func(n *Node) string {
		var b strings.Builder
		var walk func(*Node)
		walk = func(n *Node) {
			for _, p := range n.Projections {
				if p.ASTExpr != nil {
					b.WriteString(p.ASTExpr.String() + ";")
				}
			}
			for _, c := range n.Children {
				walk(c)
			}
		}
		walk(n)
		return b.String()
	}
	for _, c := range []struct {
		name, sql string
		qualify   func(string) string
		want      string
	}{
		{"BuildExpression", `SELECT a.id FROM t a, (SELECT id, upper(s) AS k FROM t) b WHERE upper(a.s) = b.k`,
			qual, "upper(a.s) = (upper(bq.s))"},
		{"BuildRename", `SELECT a.id FROM t a, (SELECT id, s AS k FROM t) b WHERE a.s = b.k OR a.id = b.id`,
			qual, "a.s = bq.s or a.id = b.id"},
		{"ProbeArmStaysBare", `SELECT a.id FROM (SELECT id, upper(s) AS k FROM t) a, (SELECT id, lower(s) AS k FROM t) b WHERE lower(a.k) = b.k`,
			qual, "lower((upper(s))) = (lower(bq.s))"},
		{"AlreadyQualified", `SELECT a.id FROM t a, (SELECT x.id, upper(x.s) AS k FROM t x) b WHERE upper(a.s) = b.k`,
			qual, "upper(a.s) = (upper(x.s))"},
		{"NoQualifier", `SELECT a.id FROM t a, (SELECT id, upper(s) AS k FROM t) b WHERE upper(a.s) = b.k`,
			nil, "upper(a.s) = (upper(s))"},
	} {
		t.Run(c.name, func(t *testing.T) {
			parsed, err := plansql.Parse(c.sql)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			info, err := plansql.ExtractSelect(parsed)
			if err != nil {
				t.Fatalf("extract: %v", err)
			}
			plan, err := BuildFromSelect(info)
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			filter := findTopFilter(Optimize(plan))
			if filter == nil {
				t.Fatalf("no Filter node in the optimized plan for %q", c.sql)
			}
			before := defs(filter.Children[0])
			ast, _, ok := ResolveFilterThroughProjects(filter.Predicates[0], filter.Children[0], c.qualify)
			if !ok {
				t.Fatalf("unchanged, want %q", c.want)
			}
			if got := ast.String(); !strings.EqualFold(got, c.want) {
				t.Fatalf("resolved to %q, want %q\n  SQL: %s", got, c.want, c.sql)
			}
			if after := defs(filter.Children[0]); after != before {
				t.Fatalf("the logical plan's definitions were written to:\n  before %s\n  after  %s", before, after)
			}
		})
	}
}
