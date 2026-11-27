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
//
// Which arm a reference came from must not be read off the definition's NODE
// identity: a CTE read on both sides of the join hands both arms the same
// definition nodes, and round 1 of this line, which did that, left the build
// arm's `upper(s)` bare there — `(upper(s)) < (upper(s))`, zero rows on every
// DAG arm. sharedDefs makes the two arms share their definition nodes outright,
// whatever the plan builder does for a CTE. And a definition qualified by a
// name the join's stream does not carry (the table's own, `upper(t.s)`) is
// re-qualified: left as written the worker read it as the probe's bare `s`.
func TestResolveFilterThroughProjectsQualifiesBuildArmDefinitions(t *testing.T) {
	// The stream carries the build alias bq and, for the AlreadyQualified
	// case, the inner scan alias x (the stage's build alias there).
	qual := func(table, _ string) string {
		if strings.EqualFold(table, "x") {
			return table
		}
		return "bq"
	}
	// sharedDefs points every build-arm definition at the probe arm's node of
	// the same text, so the two arms share definition nodes.
	sharedDefs := func(t *testing.T, n *Node) {
		var join func(*Node) *Node
		join = func(n *Node) *Node {
			if n == nil {
				return nil
			}
			if n.Type == NodeJoin {
				return n
			}
			for _, c := range n.Children {
				if j := join(c); j != nil {
					return j
				}
			}
			return nil
		}
		var proj func(*Node) *Node
		proj = func(n *Node) *Node {
			for ; n != nil; n = n.Children[0] {
				if n.Type == NodeProject {
					return n
				}
				if len(n.Children) == 0 {
					return nil
				}
			}
			return nil
		}
		j := join(n)
		if j == nil || len(j.Children) != 2 {
			t.Fatal("no two-armed join below the filter")
		}
		probe, build := proj(j.Children[0]), proj(j.Children[1])
		if probe == nil || build == nil {
			t.Fatal("an arm has no Project")
		}
		shared := 0
		for i := range build.Projections {
			for k := range probe.Projections {
				bp, pp := build.Projections[i].ASTExpr, probe.Projections[k].ASTExpr
				if bp != nil && pp != nil && bp != pp && bp.String() == pp.String() {
					build.Projections[i].ASTExpr = pp
					shared++
					break
				}
			}
		}
		if shared == 0 {
			t.Fatal("no definition shared between the arms")
		}
	}
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
		qualify   func(table, column string) string
		want      string
		share     bool
	}{
		{"SharedDefinitionNodes", `SELECT a.id FROM (SELECT id, upper(s) AS k FROM t) a, (SELECT id, upper(s) AS k FROM t) b WHERE a.k < b.k`,
			qual, "(upper(s)) < (upper(bq.s))", true},
		{"SharedDefinitionNodesUnderCalls", `SELECT a.id FROM (SELECT id, upper(s) AS k FROM t) a, (SELECT id, upper(s) AS k FROM t) b WHERE lower(a.k) = lower(b.k) || '' AND a.id < b.id`,
			qual, "lower((upper(s))) = lower((upper(bq.s))) || ''", true},
		{"CTEOnBothSides", `WITH c AS (SELECT id, upper(s) AS k FROM t) SELECT a.id FROM c a, c b WHERE a.k < b.k`,
			qual, "(upper(s)) < (upper(bq.s))", false},
		{"TableQualified", `SELECT a.id FROM t a, (SELECT t.id, upper(t.s) AS k FROM t) b WHERE upper(a.s) = b.k`,
			qual, "upper(a.s) = (upper(bq.s))", false},
		{"BuildExpression", `SELECT a.id FROM t a, (SELECT id, upper(s) AS k FROM t) b WHERE upper(a.s) = b.k`,
			qual, "upper(a.s) = (upper(bq.s))", false},
		{"BuildRename", `SELECT a.id FROM t a, (SELECT id, s AS k FROM t) b WHERE a.s = b.k OR a.id = b.id`,
			qual, "a.s = bq.s or a.id = b.id", false},
		{"ProbeArmStaysBare", `SELECT a.id FROM (SELECT id, upper(s) AS k FROM t) a, (SELECT id, lower(s) AS k FROM t) b WHERE lower(a.k) = b.k`,
			qual, "lower((upper(s))) = (lower(bq.s))", false},
		{"AlreadyQualified", `SELECT a.id FROM t a, (SELECT x.id, upper(x.s) AS k FROM t x) b WHERE upper(a.s) = b.k`,
			qual, "upper(a.s) = (upper(x.s))", false},
		{"NoQualifier", `SELECT a.id FROM t a, (SELECT id, upper(s) AS k FROM t) b WHERE upper(a.s) = b.k`,
			nil, "upper(a.s) = (upper(s))", false},
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
			if c.share {
				sharedDefs(t, filter.Children[0])
			}
			before := defs(filter.Children[0])
			var qualifyFor func(*Node) func(table, column string) string
			if c.qualify != nil {
				// One join below each of these filters: every join qualifies
				// by the same answer.
				qualifyFor = func(*Node) func(table, column string) string { return c.qualify }
			}
			ast, _, ok := ResolveFilterThroughProjects(filter.Predicates[0], filter.Children[0], qualifyFor)
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

// EVERY JOIN THE WALK CROSSES QUALIFIES WHAT ITS OWN BUILD ARM BROUGHT IN, BY
// THE NAME THAT JOIN'S STREAM GIVES IT (#1678 line j, round 3). Round 2 handed
// the qualifier only to the FIRST join: a derived table that is a LOWER
// join's build, read by a filter an upper join carries, arrived with its
// definition bare, and the bare name bound the probe on the upper stream —
// `upper(a.s) <> b.k OR …` over `a JOIN b … JOIN w` became
// `upper(a.s) != (upper(s)) or …`, the probe compared with itself.
//
// qualifyFor answers each join by its build arm's name plus "_s", so the
// expected spelling says WHICH join qualified each reference: in a three-join
// tree every derived build is qualified by its own join, a derived PROBE stays
// bare, and a join inside a build arm qualifies first and keeps its answer —
// the enclosing join's qualifier, which knows its build's scans only by bare
// name, never re-qualifies it, nor the query's own references beside it.
func TestResolveFilterThroughProjectsQualifiesEveryJoinsBuildArm(t *testing.T) {
	armName := func(n *Node) string {
		for ; n != nil; n = n.Children[0] {
			if n.DerivedAlias != "" {
				return n.DerivedAlias
			}
			if n.Type == NodeScan && n.TableAlias != "" {
				return n.TableAlias
			}
			if len(n.Children) == 0 {
				return ""
			}
		}
		return ""
	}
	qualifyFor := func(j *Node) func(table, column string) string {
		name := armName(j.Children[1])
		if name == "" {
			return nil
		}
		stream := name + "_s"
		return func(table, _ string) string {
			if strings.EqualFold(table, stream) {
				return table
			}
			return stream
		}
	}
	const (
		b = `(SELECT id, upper(s) AS k FROM t) b`
		c = `(SELECT id, lower(s) AS k FROM t) c`
		d = `(SELECT id, s || 'x' AS k FROM t) d`
	)
	for _, tc := range []struct{ name, sql, want string }{
		{"ThreeJoinsDerivedBuildAtEveryLevel",
			`SELECT a.id FROM t a JOIN ` + b + ` ON a.id = b.id JOIN ` + c + ` ON a.id = c.id JOIN ` + d + ` ON a.id = d.id
			WHERE a.s = b.k OR a.s = c.k OR a.s = d.k`,
			"a.s = (upper(b_s.s)) or a.s = (lower(c_s.s)) or a.s = (d_s.s || 'x')"},
		{"ThreeJoinsLowestBuildReadAlone",
			`SELECT a.id FROM t a JOIN ` + b + ` ON a.id = b.id JOIN t w ON a.id = w.id JOIN t v ON a.id = v.id
			WHERE upper(a.s) <> b.k OR lower(v.s) = lower(b.k)`,
			"upper(a.s) != (upper(b_s.s)) or lower(v.s) = lower((upper(b_s.s)))"},
		{"ThreeJoinsDerivedProbeAtTheBottom",
			`SELECT a.id FROM ` + b + ` JOIN t a ON b.id = a.id JOIN ` + c + ` ON a.id = c.id JOIN ` + d + ` ON a.id = d.id
			WHERE a.s = b.k OR a.s = c.k OR a.s = d.k`,
			"a.s = (upper(s)) or a.s = (lower(c_s.s)) or a.s = (d_s.s || 'x')"},
		// LEFT, so the optimizer keeps b as the BUILD (an inner join here is
		// reordered to make b the probe).
		{"JoinInsideTheBuildArmKeepsItsAnswer",
			`SELECT a.id FROM t a LEFT JOIN (SELECT x.id, y.k FROM t x JOIN (SELECT id, upper(s) AS k FROM t) y ON x.id = y.id) b
			ON a.id = b.id JOIN t w ON a.id = w.id WHERE upper(a.s) = b.k OR w.id > 5`,
			"upper(a.s) = (upper(y_s.s)) or w.id > 5"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := plansql.Parse(tc.sql)
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
				t.Fatalf("no Filter node in the optimized plan for %q", tc.sql)
			}
			joins := 0
			var count func(*Node)
			count = func(n *Node) {
				if n.Type == NodeJoin {
					joins++
				}
				for _, ch := range n.Children {
					count(ch)
				}
			}
			count(filter.Children[0])
			if joins < 3 {
				t.Fatalf("%d joins below the filter, want at least 3", joins)
			}
			ast, _, ok := ResolveFilterThroughProjects(filter.Predicates[0], filter.Children[0], qualifyFor)
			if !ok {
				t.Fatalf("unchanged, want %q", tc.want)
			}
			if got := ast.String(); !strings.EqualFold(got, tc.want) {
				t.Fatalf("resolved to %q, want %q\n  SQL: %s", got, tc.want, tc.sql)
			}
		})
	}
}
