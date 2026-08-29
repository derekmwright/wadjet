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
	ast, ok := ResolveFilterThroughProjects(filter.Predicates[0], filter.Children[0])
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
		// A Sort or a LIMIT EMITS a stage carrying the names above it, so the
		// walk must not re-spell past one — the filter lands on that stage,
		// not on the scan below the Project.
		{"StopsAtSort",
			`WITH c AS (SELECT id, c_i64 AS v FROM t ORDER BY id) SELECT id FROM c WHERE v > 0`,
			""},
		{"StopsAtLimit",
			`WITH c AS (SELECT id, c_i64 AS v FROM t ORDER BY id LIMIT 10) SELECT id FROM c WHERE v > 0`,
			""},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := resolveTopFilter(t, c.sql); !strings.EqualFold(got, c.want) {
				t.Fatalf("resolved to %q, want %q\n  SQL: %s", got, c.want, c.sql)
			}
		})
	}
}
