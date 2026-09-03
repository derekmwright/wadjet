package logical

import (
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// Regression for #320: ORDER BY over anything that is not a plain SELECT-list
// column was silently ignored.
//
// A Sort reads columns by NAME, and the Project below it narrows the schema to
// exactly the select list. `SELECT d FROM t ORDER BY year(d)` therefore keyed
// on a name nothing had computed, `SELECT a FROM t ORDER BY b` keyed on one the
// Project had just dropped — both matched no column, and exec.Sort skips a key
// that matches nothing, so the rows came back in arbitrary order with no error.
// Adding the term to the SELECT list made each query correct, which is what
// made the bug look like an aliasing problem (#313, #316) rather than the
// missing evaluation it is.
//
// Two invariants are asserted here. Every sort key names a column the Sort's
// input really emits — materialized as a hidden projection when the select list
// does not carry it. And a key that can be neither resolved nor materialized is
// an ERROR: an ORDER BY the engine cannot honour must fail, not return an
// arbitrary order that looks like an answer.

func planOf(tb testing.TB, sql string) (*Node, error) {
	tb.Helper()
	pq, err := plansql.Parse(sql)
	if err != nil {
		tb.Fatalf("Parse(%q): %v", sql, err)
	}
	info, err := plansql.ExtractSelect(pq)
	if err != nil {
		tb.Fatalf("ExtractSelect(%q): %v", sql, err)
	}
	return BuildFromSelect(info)
}

// sortNodeOf returns the plan's outermost Sort and the node it reads.
func sortNodeOf(tb testing.TB, plan *Node) (*Node, *Node) {
	tb.Helper()
	n := plan
	for n != nil && n.Type != NodeSort {
		if len(n.Children) != 1 {
			tb.Fatalf("plan carries no Sort node:\n%s", plan.PrettyPrint(0))
		}
		n = n.Children[0]
	}
	if n == nil || len(n.Children) != 1 {
		tb.Fatalf("plan carries no Sort node:\n%s", plan.PrettyPrint(0))
	}
	return n, n.Children[0]
}

// emittedNames is the column set a Sort's input produces. A Project narrows to
// exactly its projections; anything else is not modeled here and returns nil,
// which callers read as "not checkable".
func emittedNames(n *Node) []string {
	if n == nil || n.Type != NodeProject {
		return nil
	}
	out := make([]string, 0, len(n.Projections))
	for _, p := range n.Projections {
		name := p.Alias
		if name == "" {
			name = p.Column
		}
		if name == "" {
			name = strings.TrimSpace(p.Expr)
		}
		out = append(out, name)
	}
	return out
}

func TestOrderByExpressionIsMaterialized(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		// wantKeys is the sort key spelling per ORDER BY term.
		wantKeys []string
		// wantHidden is the (expression, output name) of each projection the
		// planner added for its own use, in order. Nil means the plan must be
		// left exactly as it was: the select list already carries every key.
		wantHidden [][2]string
	}{
		{
			// #320 verbatim, function form.
			name:       "function over a selected column",
			sql:        "SELECT d FROM t ORDER BY year(d)",
			wantKeys:   []string{"__sortkey_0"},
			wantHidden: [][2]string{{"year(d)", "__sortkey_0"}},
		},
		{
			// #320 verbatim, negation form.
			name:       "negated column",
			sql:        "SELECT id FROM t ORDER BY -id",
			wantKeys:   []string{"__sortkey_0"},
			wantHidden: [][2]string{{"-id", "__sortkey_0"}},
		},
		{
			name:       "arithmetic over two columns",
			sql:        "SELECT a, b FROM t ORDER BY a + b",
			wantKeys:   []string{"__sortkey_0"},
			wantHidden: [][2]string{{"a + b", "__sortkey_0"}},
		},
		{
			// DESC rides on the key, not on the materialization.
			name:       "expression DESC",
			sql:        "SELECT d FROM t ORDER BY year(d) DESC",
			wantKeys:   []string{"__sortkey_0"},
			wantHidden: [][2]string{{"year(d)", "__sortkey_0"}},
		},
		{
			// The same failure with no expression at all: a plain column the
			// select list dropped.
			name:       "column not in the select list",
			sql:        "SELECT a FROM t ORDER BY b",
			wantKeys:   []string{"__sortkey_0"},
			wantHidden: [][2]string{{"b", "__sortkey_0"}},
		},
		{
			// Mixed: the first key is carried by the select list, the second
			// is not. Materializing one must not disturb the other.
			name:       "mixed carried and computed keys",
			sql:        "SELECT a, b FROM t ORDER BY a, upper(b)",
			wantKeys:   []string{"a", "__sortkey_0"},
			wantHidden: [][2]string{{"upper(b)", "__sortkey_0"}},
		},
		{
			// Two computed keys get one column each, numbered in key order.
			name:       "two computed keys",
			sql:        "SELECT a, b FROM t ORDER BY upper(a), -b",
			wantKeys:   []string{"__sortkey_0", "__sortkey_1"},
			wantHidden: [][2]string{{"upper(a)", "__sortkey_0"}, {"-b", "__sortkey_1"}},
		},
		{
			// Positional, resolved by the parser to the select item it names
			// — the planner must leave it alone rather than treat the literal
			// as an expression to materialize.
			name:     "ordinal",
			sql:      "SELECT a, b FROM t ORDER BY 2",
			wantKeys: []string{"b"},
		},
		{
			// The controls: shapes that already worked. Each must plan
			// EXACTLY as before — no hidden column, key unchanged — or the
			// fix has widened past the bug.
			name:     "bare column in the select list",
			sql:      "SELECT a, b FROM t ORDER BY a",
			wantKeys: []string{"a"},
		},
		{
			name:     "select-list alias",
			sql:      "SELECT a AS x FROM t ORDER BY x",
			wantKeys: []string{"x"},
		},
		{
			name:     "expression that is itself a select item",
			sql:      "SELECT year(d) AS y FROM t ORDER BY year(d)",
			wantKeys: []string{"y"},
		},
		{
			name:     "aggregate alias",
			sql:      "SELECT a, COUNT(*) AS c FROM t GROUP BY a ORDER BY c DESC",
			wantKeys: []string{"c"},
		},
		{
			// A grouped column the select list does not carry. The Project
			// dropped it, so the single-process sort lost it — but the key
			// must stay a passthrough of the group key so the DAG can still
			// map it back to the aggregate's own output.
			name:       "grouped column not in the select list",
			sql:        "SELECT COUNT(*) AS c FROM t GROUP BY a ORDER BY a",
			wantKeys:   []string{"__sortkey_0"},
			wantHidden: [][2]string{{"a", "__sortkey_0"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := planOf(t, tt.sql)
			if err != nil {
				t.Fatalf("BuildFromSelect: %v", err)
			}
			sortNode, child := sortNodeOf(t, plan)

			if len(sortNode.OrderBy) != len(tt.wantKeys) {
				t.Fatalf("plan carries %d sort keys, want %d: %+v", len(sortNode.OrderBy), len(tt.wantKeys), sortNode.OrderBy)
			}
			for i, want := range tt.wantKeys {
				if got := sortNode.OrderBy[i].Column; got != want {
					t.Errorf("sort key %d = %q, want %q", i, got, want)
				}
			}

			// The invariant the whole family kept breaking: every key names a
			// column the Sort's input really emits.
			if emitted := emittedNames(child); emitted != nil {
				for _, ob := range sortNode.OrderBy {
					found := false
					for _, e := range emitted {
						if strings.EqualFold(e, ob.Column) {
							found = true
							break
						}
					}
					if !found {
						t.Errorf("sort key %q names no output of the %s below it, which emits %v — "+
							"the sort finds no column and silently returns its input unsorted",
							ob.Column, child.Type, emitted)
					}
				}
			}

			var hidden [][2]string
			if child.Type == NodeProject {
				for _, p := range child.Projections {
					if p.Hidden {
						hidden = append(hidden, [2]string{p.Expr, p.Alias})
					}
				}
			}
			if len(hidden) != len(tt.wantHidden) {
				t.Fatalf("plan materialized %d hidden sort columns %v, want %d %v", len(hidden), hidden, len(tt.wantHidden), tt.wantHidden)
			}
			for i, want := range tt.wantHidden {
				if hidden[i] != want {
					t.Errorf("hidden column %d = %v, want %v", i, hidden[i], want)
				}
			}
			// Hidden columns must be LAST: the DAG's gather aligns its
			// SELECT-list renames against this slice by index, and only a
			// trailing tail leaves that alignment intact.
			if child.Type == NodeProject {
				seenHidden := false
				for _, p := range child.Projections {
					if p.Hidden {
						seenHidden = true
					} else if seenHidden {
						t.Errorf("a visible projection (%q) follows a hidden one; hidden columns must be appended last", p.Alias)
					}
				}
			}
		})
	}
}

// TestOrderByStarSelectMaterializes covers the shape with no SELECT-list
// projection to hang the key on: the planner has to create one that keeps the
// star, which star expansion then rewrites into the scan's columns.
func TestOrderByStarSelectMaterializes(t *testing.T) {
	plan, err := planOf(t, "SELECT * FROM t ORDER BY year(d)")
	if err != nil {
		t.Fatalf("BuildFromSelect: %v", err)
	}
	sortNode, child := sortNodeOf(t, plan)
	if child.Type != NodeProject {
		t.Fatalf("sort reads a %s; `SELECT *` with a computed key needs a projection created for it:\n%s", child.Type, plan.PrettyPrint(0))
	}
	if len(child.Projections) != 2 {
		t.Fatalf("projection = %+v, want the star plus one hidden column", child.Projections)
	}
	if !HasStarProjection(child) {
		t.Errorf("created projection %+v dropped the star — `SELECT *` must still return every column", child.Projections)
	}
	if p := child.Projections[1]; !p.Hidden || p.Alias != "__sortkey_0" {
		t.Errorf("second projection = %+v, want the hidden sort column", p)
	}
	if got := sortNode.OrderBy[0].Column; got != "__sortkey_0" {
		t.Errorf("sort key = %q, want %q", got, "__sortkey_0")
	}

	// A star with a bare column key needs nothing: the Sort reads the whole
	// relation, so the column is already there.
	plan, err = planOf(t, "SELECT * FROM t ORDER BY d")
	if err != nil {
		t.Fatalf("BuildFromSelect: %v", err)
	}
	sortNode, child = sortNodeOf(t, plan)
	if child.Type == NodeProject {
		t.Errorf("`SELECT * ... ORDER BY d` grew a projection %+v; this shape needs none", child.Projections)
	}
	if got := sortNode.OrderBy[0].Column; got != "d" {
		t.Errorf("sort key = %q, want %q", got, "d")
	}
}

// TestOrderByUnhonourableKeyErrors pins rule 2: a sort key that resolves to
// nothing must fail the query. Every earlier member of this family shipped
// because it failed silently instead — right rows, arbitrary order, no error —
// so each shape the planner cannot honour is listed here by name, with the
// reason it is excluded.
func TestOrderByUnhonourableKeyErrors(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		// want is a distinctive fragment of the error, so the message stays
		// specific enough to tell a user what to do instead.
		want string
	}{
		{
			// Materializing here would widen the DISTINCT's dedup key and
			// change which rows survive. SQL rejects the shape for the same
			// reason.
			name: "SELECT DISTINCT with a computed key",
			sql:  "SELECT DISTINCT a FROM t ORDER BY -a",
			want: "for SELECT DISTINCT, ORDER BY expressions must appear in select list",
		},
		{
			name: "SELECT DISTINCT ordering on an unselected column",
			sql:  "SELECT DISTINCT a FROM t ORDER BY b",
			want: "for SELECT DISTINCT, ORDER BY expressions must appear in select list",
		},
		{
			// The DAG's sort runs between the aggregate and the gather with
			// nothing in between that could evaluate an expression, so a
			// computed key would be honoured on small inputs and lost on
			// large ones. Failing on both beats a routing-dependent answer.
			name: "computed key over a GROUP BY",
			sql:  "SELECT a, COUNT(*) AS c FROM t GROUP BY a ORDER BY length(a)",
			want: "only a grouped column, a grouping expression, or a select-list alias",
		},
		{
			name: "aggregate expression not in the select list",
			sql:  "SELECT a, COUNT(*) AS c FROM t GROUP BY a ORDER BY COUNT(*) * 2",
			want: "an aggregate expression that is not itself a select item",
		},
		{
			name: "ungrouped column over a GROUP BY",
			sql:  "SELECT a, COUNT(*) AS c FROM t GROUP BY a ORDER BY b",
			want: "only a grouped column, a grouping expression, or a select-list alias",
		},
		// `SELECT * FROM t ORDER BY 1` USED to be here. It is answered now
		// (#810): the ordinal is deferred past the parser, where a star's
		// width is not knowable, and resolved after ExpandStarProjections
		// against the columns the star produced. This layer no longer refuses
		// it — the star has not expanded here either, because that needs the
		// catalog, so the key simply carries its Position and the physical
		// planner resolves or refuses it.
		//
		// What is still LOUD, and where, is gated by
		// coordinator.TestOrderByResolvesAPositionAfterTheStarExpands: an
		// out-of-range position is 42P10 with PostgreSQL's own wording, and a
		// star over a join is 42P10 saying the column list cannot be counted.
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := planOf(t, tt.sql)
			if err == nil {
				t.Fatalf("planned without error; an ORDER BY the engine cannot honour must fail, "+
					"not return an arbitrary order:\n%s", plan.PrettyPrint(0))
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %q, want it to contain %q", err.Error(), tt.want)
			}
			if !strings.HasPrefix(err.Error(), "ORDER BY ") {
				t.Errorf("error = %q, want it to name the offending ORDER BY term first", err.Error())
			}
		})
	}
}

// TestHiddenSortColumnNotPushedToScans: the materialized name is computed by a
// Project, not stored by any scan. Pushing it into a scan's RequiredColumns
// would land a "__"-prefixed name there, which the distributed worker's
// all-or-nothing parquet projection guard answers by reading FULL WIDTH — the
// difference between a 1-column and a 100-column read on ClickBench. The
// columns the expression really needs must still arrive, through the
// projection's own AST references.
func TestHiddenSortColumnNotPushedToScans(t *testing.T) {
	plan, err := planOf(t, "SELECT a FROM t ORDER BY upper(b)")
	if err != nil {
		t.Fatalf("BuildFromSelect: %v", err)
	}
	computeRequiredColumns(plan)
	scan := findNodeType(plan, NodeScan)
	if scan == nil {
		t.Fatalf("plan carries no scan:\n%s", plan.PrettyPrint(0))
	}
	sawB := false
	for _, c := range scan.RequiredColumns {
		if IsHiddenSortColumn(c) {
			t.Errorf("scan requires %q — a synthetic sort column no scan stores; "+
				"required columns are %v", c, scan.RequiredColumns)
		}
		if strings.EqualFold(c, "b") {
			sawB = true
		}
	}
	if !sawB {
		t.Errorf("scan does not require %q, the column the sort expression reads; required columns are %v",
			"b", scan.RequiredColumns)
	}
}
