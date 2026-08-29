package logical

import (
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// Unit cover for ResolveFilterThroughProjects' three rules, which the
// two-path gates in internal/coordinator exercise end to end but cannot
// localize: which relation a qualified reference names, what a ROW field path
// through a rename means, and where the walk has to stop.

// resolveTopFilter builds sql, finds the outermost Filter, and returns the
// re-spelling the DAG would ship for its first predicate ("" = unchanged).
func resolveTopFilter(t *testing.T, sql string) (string, error) {
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
	ast, ok, rerr := ResolveFilterThroughProjects(filter.Predicates[0], filter.Children[0])
	if rerr != nil {
		return "", rerr
	}
	if !ok {
		return "", nil
	}
	return ast.String(), nil
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
		// A ROW field path is not a table-qualified column (ADR-0022): the
		// QUALIFIER is substituted and the field kept.
		{"RowFieldPathThroughRename",
			`WITH c AS (SELECT c_row AS rw, id FROM t) SELECT COUNT(*) FROM c WHERE rw.b > 100`,
			"c_row.b > 100"},
		{"RowFieldPathThroughPassthrough",
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
			got, err := resolveTopFilter(t, c.sql)
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			if !strings.EqualFold(got, c.want) {
				t.Fatalf("resolved to %q, want %q\n  SQL: %s", got, c.want, c.sql)
			}
		})
	}
}

// A bare name both join arms can emit has no attribution in the predicate's
// text, and declining silently would leave a spelling no stage resolves —
// UNKNOWN on every row, zero rows in silence. It is a plan-time 42702, which
// is also what PostgreSQL answers.
func TestResolveFilterThroughProjectsRefusesAnAmbiguousBareName(t *testing.T) {
	sql := `WITH c AS (SELECT g AS k, c_i64 AS v FROM t) SELECT COUNT(*) FROM c JOIN dim d ON c.k = d.k WHERE k > 3`
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
	// The sibling arm is a bare Scan, so the ambiguity is only visible once
	// the catalog has told the plan what that scan emits.
	opt := Optimize(plan)
	filter := findTopFilter(opt)
	if filter == nil {
		t.Fatal("no Filter node in the optimized plan")
	}
	setScanColumnsForTest(filter.Children[0], "dim", []string{"k", "label"})

	_, _, rerr := ResolveFilterThroughProjects(filter.Predicates[0], filter.Children[0])
	if rerr == nil {
		t.Fatal("a bare name both arms emit was resolved silently; PostgreSQL refuses it with 42702")
	}
	var amb *ErrAmbiguousFilterColumn
	if !asAmbiguous(rerr, &amb) {
		t.Fatalf("error is %T (%v), want *ErrAmbiguousFilterColumn", rerr, rerr)
	}
	if !strings.EqualFold(amb.Column, "k") {
		t.Fatalf("the refusal names %q, want %q", amb.Column, "k")
	}
	if amb.SQLState() != "42702" {
		t.Fatalf("SQLSTATE is %q, want 42702", amb.SQLState())
	}
}

func setScanColumnsForTest(n *Node, table string, cols []string) {
	if n == nil {
		return
	}
	if n.Type == NodeScan && strings.EqualFold(n.TableName, table) {
		n.ScanColumns = cols
	}
	for _, c := range n.Children {
		setScanColumnsForTest(c, table, cols)
	}
}

func asAmbiguous(err error, target **ErrAmbiguousFilterColumn) bool {
	e, ok := err.(*ErrAmbiguousFilterColumn)
	if ok {
		*target = e
	}
	return ok
}
