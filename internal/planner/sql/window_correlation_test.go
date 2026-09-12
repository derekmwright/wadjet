package sql

import (
	"sort"
	"testing"
)

// A WINDOW CALL IS A POSITION AN OUTER REFERENCE CAN SIT IN (#1045).
//
// walkForOuterRefs had no case for a window node, so the whole call was a leaf
// and `SUM(u.id) OVER ()` reported no correlated reference at all. Two readers
// went blind together: the CLASSIFIER, which then planned the subquery
// uncorrelated and let it run once against no outer row, and
// DanglingTableRefs, which walks this same function with anyOuter set and is
// what makes the three `window_*_correlated` shapes LOUD. That shared walk is
// the whole difference between this shape and its siblings.
func TestAWindowCallIsAPositionAnOuterReferenceCanSitIn(t *testing.T) {
	outer := map[string]bool{"u": true}
	for _, tc := range []struct {
		name string
		sql  string
		want []string
	}{
		{"an argument", `SELECT SUM(u.id) OVER () FROM users x WHERE x.id=1`, []string{"u.id"}},
		{"an argument under arithmetic",
			`SELECT 1+SUM(u.id) OVER () FROM users x WHERE x.id=1`, []string{"u.id"}},
		{"an expression argument",
			`SELECT SUM(u.id + u.visits) OVER () FROM users x WHERE x.id=1`,
			[]string{"u.id", "u.visits"}},
		{"PARTITION BY",
			`SELECT SUM(x.id) OVER (PARTITION BY u.id) FROM users x WHERE x.id=1`,
			[]string{"u.id"}},
		{"ORDER BY",
			`SELECT ROW_NUMBER() OVER (ORDER BY u.id) FROM users x WHERE x.id=1`,
			[]string{"u.id"}},
		{"an aggregate over a window",
			`SELECT MAX(SUM(u.id) OVER ()) FROM users x WHERE x.id=1`, []string{"u.id"}},
		{"the inner relation's own column is not correlated",
			`SELECT SUM(x.id) OVER (PARTITION BY x.id) FROM users x WHERE x.id=1`, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			refs, err := FindCorrelatedRefs(tc.sql, outer)
			if err != nil {
				t.Fatalf("FindCorrelatedRefs: %v", err)
			}
			if got := refNames(refs); !sameStrings(got, tc.want) {
				t.Errorf("FindCorrelatedRefs\n  got  %v\n  want %v\n  SQL: %s", got, tc.want, tc.sql)
			}
			// The dangling walk shares the recursion, and its verdict is what
			// makes an UNCORRELATED evaluator refuse instead of answering a
			// constant (ADR-0021 §1c).
			if got := refNames(DanglingTableRefs(tc.sql)); !sameStrings(got, tc.want) {
				t.Errorf("DanglingTableRefs\n  got  %v\n  want %v\n  SQL: %s", got, tc.want, tc.sql)
			}
		})
	}
}

// A FRAME OFFSET IS A POSITION TOO, and no query can reach it through this
// parser today — a non-literal frame bound is a parse error — so the claim is
// attempted against the AST directly. Method 10: an "impossible" position that
// no fixture produces is untested code on the default path, and the day the
// parser accepts `ROWS BETWEEN u.id PRECEDING` the walk must already be right.
func TestAWindowFrameOffsetIsAPositionToo(t *testing.T) {
	if _, err := Parse(`SELECT SUM(x.id) OVER (ORDER BY x.id ` +
		`ROWS BETWEEN u.id PRECEDING AND CURRENT ROW) FROM users x`); err == nil {
		t.Fatal("the parser now accepts an expression frame bound; this test's premise is " +
			"stale — reach the frame offsets through a parsed query instead of building the AST")
	}
	end := FrameBound{Type: BoundCurrentRow}
	node := &WindowFuncNode{
		Func: &FuncCallNode{Name: "sum", Args: []Node{&ColRef{Table: "x", Column: "id"}}},
		Frame: &WindowFrame{
			Mode:  FrameRows,
			Start: FrameBound{Type: BoundPreceding, Offset: &ColRef{Table: "u", Column: "id"}},
			End:   &end,
		},
	}
	var refs []OuterRef
	walkForOuterRefs(node, &outerRefScope{
		outerTables: map[string]bool{"u": true},
		innerTables: map[string]bool{"x": true},
	}, &refs)
	if got := refNames(dedup(refs)); !sameStrings(got, []string{"u.id"}) {
		t.Errorf("a frame offset's outer reference was not collected: got %v, want [u.id]", got)
	}
}

// The PRUNING collector walks the same positions, so the outer query still
// projects a column only a window call reads. Dropping one is not a missed
// optimization: readOuterValues fails loudly on a column the batch does not
// carry, which is the error a user would see instead of an answer.
func TestOuterColumnCandidatesReachIntoAWindowCall(t *testing.T) {
	got := OuterColumnCandidates(
		`SELECT SUM(u.id) OVER (PARTITION BY u.visits) FROM users x WHERE x.id=1`)
	sort.Strings(got)
	if !sameStrings(got, []string{"id", "visits"}) {
		t.Errorf("OuterColumnCandidates = %v, want [id visits]", got)
	}
}

// A CORRELATED SUBQUERY'S TEXT IS REBUILT, AND A WINDOW CALL DOES NOT SURVIVE
// THE REBUILD. HoldsWindowCall is what the three correlated constructs ask
// before they build an evaluator that would rebuild it.
func TestHoldsWindowCallSeesEveryClauseTheRebuildReEmits(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		want bool
	}{
		{`SELECT SUM(u.id) OVER () FROM users x`, true},
		{`SELECT 1 + SUM(u.id) OVER () FROM users x`, true},
		{`SELECT MAX(SUM(x.id) OVER ()) FROM users x`, true},
		{`SELECT x.id FROM users x QUALIFY SUM(x.id) OVER () > 1`, true},
		{`SELECT x.id FROM users x UNION ALL SELECT SUM(y.id) OVER () FROM users y`, true},
		{`SELECT SUM(x.id) FROM users x WHERE x.id = u.id`, false},
		{`SELECT x.id FROM users x`, false},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			parsed, err := Parse(tc.sql)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			info, err := ExtractSelect(parsed)
			if err != nil {
				t.Fatalf("extract: %v", err)
			}
			if got := HoldsWindowCall(info); got != tc.want {
				t.Errorf("HoldsWindowCall = %v, want %v", got, tc.want)
			}
		})
	}
}

func refNames(refs []OuterRef) []string {
	var out []string
	for _, r := range refs {
		out = append(out, r.Table+"."+r.Column)
	}
	sort.Strings(out)
	return out
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
