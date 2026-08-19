package logical

import (
	"strings"
	"testing"
)

// TestRejectsNulls pins the analysis that decides whether a WHERE predicate
// over an outer join's null-supplying side may move below the join (#335).
//
// The direction of a wrong answer matters: a predicate wrongly called
// rejecting is pushed below the NULL-padding and DELETES rows; a predicate
// wrongly called tolerant only stays above the join, which costs a pushdown
// and nothing else. Every "want: false" entry below is therefore a
// correctness case and every "want: true" entry is an optimization case.
func TestRejectsNulls(t *testing.T) {
	cases := []struct {
		expr string
		want bool
		why  string
	}{
		{"r.r_regionkey = 2", true, "a comparison against a padded NULL is UNKNOWN"},
		{"r.r_regionkey <> 2", true, "so is an inequality"},
		{"r.r_regionkey > 2", true, ""},
		{"r.r_name LIKE 'EU%'", true, ""},
		{"r.r_regionkey BETWEEN 1 AND 3", true, ""},
		{"r.r_regionkey IN (1, 2)", true, ""},
		{"r.r_regionkey NOT IN (1, 2)", true, "NULL NOT IN (...) is NULL, not true"},
		{"r.r_regionkey IS NOT NULL", true, ""},
		{"r.r_regionkey + 1 = 3", true, "arithmetic propagates the NULL into the comparison"},
		{"(r.r_regionkey = 2)", true, "parens are transparent"},
		{"r.r_regionkey = 2 AND r.r_name = 'X'", true, "one rejecting conjunct settles the AND"},
		{"r.r_regionkey = 2 OR r.r_name = 'X'", true, "both arms reject"},
		{"r.r_regionkey = r.r_comment", true, "column against column, same side"},

		// The cases a careless fix breaks. Each of these HOLDS for a
		// NULL-padded row, so pushing it below the join deletes the
		// unmatched rows the query exists to return.
		{"r.r_regionkey IS NULL", false, "the anti-join idiom — the whole point of the outer join"},
		{"r.r_regionkey IS NULL OR r.r_regionkey = 2", false, "one tolerant arm makes the OR tolerant"},
		{"r.r_regionkey = 2 AND r.r_regionkey IS NULL", true, "but a rejecting conjunct still settles the AND"},
		{"COALESCE(r.r_name, 'none') = 'none'", false, "COALESCE is not strict in its first argument"},
		{"CASE WHEN r.r_name IS NULL THEN 1 ELSE 0 END = 1", false, "CASE decides its own NULL handling"},
		{"r.r_regionkey IS NOT TRUE", false, "NULL IS NOT TRUE is true"},
		{"r.r_regionkey IS TRUE", true, "NULL IS TRUE is false"},
	}

	for _, c := range cases {
		t.Run(c.expr, func(t *testing.T) {
			pred := Predicate{Raw: c.expr, ASTExpr: tryParseExpr(c.expr)}
			if pred.ASTExpr == nil {
				t.Fatalf("could not parse %q", c.expr)
			}
			if got := rejectsNulls(pred); got != c.want {
				t.Errorf("rejectsNulls(%q) = %v, want %v — %s", c.expr, got, c.want, c.why)
			}
		})
	}
}

func TestDemoteJoinKind(t *testing.T) {
	cases := []struct {
		kind        string
		left, right bool
		want        string
	}{
		{"left", false, true, "inner"},
		{"left", false, false, "left"},
		{"right", true, false, "inner"},
		{"right", false, false, "right"},
		{"full", true, true, "inner"},
		{"full", true, false, "left"},
		{"full", false, true, "right"},
		{"full", false, false, "full"},
		{"inner", true, true, "inner"},
	}
	for _, c := range cases {
		if got := demoteJoinKind(c.kind, c.left, c.right); got != c.want {
			t.Errorf("demoteJoinKind(%q, %v, %v) = %q, want %q", c.kind, c.left, c.right, got, c.want)
		}
	}
}

// TestPushFilterThroughLeftJoin is the plan-level statement of #335: a WHERE
// predicate over a LEFT JOIN's null-supplying side either demotes the join
// and moves below it, or stays above it — never pushed below a join that is
// still outer, which is what made `WHERE r.r_regionkey = 2` return all 25
// nations instead of 5.
func TestPushFilterThroughLeftJoin(t *testing.T) {
	build := func(pred string) *Node {
		nation := NewScan("nation", "n")
		nation.ScanColumns = []string{"n_nationkey", "n_name", "n_regionkey"}
		region := NewScan("region", "r")
		region.ScanColumns = []string{"r_regionkey", "r_name"}
		join := NewJoin(nation, region, "left", "n.n_regionkey = r.r_regionkey")
		return NewFilter(join, []Predicate{{Raw: pred, ASTExpr: tryParseExpr(pred)}})
	}

	t.Run("null-rejecting predicate demotes the join and pushes", func(t *testing.T) {
		got := pushdownPredicates(build("r.r_regionkey = 2"))
		if got.Type != NodeJoin {
			t.Fatalf("expected the filter to be consumed, got %s over %s", got.Type, got.Children[0].Type)
		}
		if got.JoinType != "inner" {
			t.Errorf("join type = %q, want inner: a WHERE that rejects the padded NULLs leaves no unmatched row", got.JoinType)
		}
		if right := got.Children[1]; right.Type != NodeFilter {
			t.Errorf("right child = %s, want a Filter carrying the pushed predicate", right.Type)
		}
	})

	t.Run("IS NULL stays above a join that stays outer", func(t *testing.T) {
		got := pushdownPredicates(build("r.r_regionkey IS NULL"))
		if got.Type != NodeFilter {
			t.Fatalf("expected the filter to remain above the join, got %s", got.Type)
		}
		join := got.Children[0]
		if join.Type != NodeJoin || join.JoinType != "left" {
			t.Fatalf("child = %s/%q, want an untouched left join", join.Type, join.JoinType)
		}
		if right := join.Children[1]; right.Type == NodeFilter {
			t.Error("the IS NULL predicate was pushed into the null-supplying side — that empties it and " +
				"the join then pads every left row back in, which is the #335 answer")
		}
	})

	t.Run("a predicate on the preserved side pushes without demoting", func(t *testing.T) {
		got := pushdownPredicates(build("n.n_nationkey = 2"))
		if got.Type != NodeJoin {
			t.Fatalf("expected the filter to be consumed, got %s", got.Type)
		}
		if got.JoinType != "left" {
			t.Errorf("join type = %q, want left preserved: filtering the preserved side removes output rows, "+
				"it does not make unmatched rows disappear", got.JoinType)
		}
		if left := got.Children[0]; left.Type != NodeFilter {
			t.Errorf("left child = %s, want a Filter carrying the pushed predicate", left.Type)
		}
	})
}

// TestLiftInnerJoinOnResiduals is the plan-level statement of #336: an ON
// conjunct the join condition cannot represent has to leave JoinCond, or
// physical.parseJoinKeys drops it in silence.
func TestLiftInnerJoinOnResiduals(t *testing.T) {
	build := func(joinType, cond string) *Node {
		a := NewScan("supplier", "a")
		a.ScanColumns = []string{"s_suppkey", "s_nationkey"}
		b := NewScan("supplier", "b")
		b.ScanColumns = []string{"s_suppkey", "s_nationkey"}
		return NewJoin(a, b, joinType, cond)
	}

	t.Run("column-vs-column inequality is lifted", func(t *testing.T) {
		got := liftInnerJoinOnResiduals(build("inner", "a.s_nationkey = b.s_nationkey AND a.s_suppkey < b.s_suppkey"))
		if got.Type != NodeFilter {
			t.Fatalf("expected a filter above the join, got %s — the conjunct is still in JoinCond, "+
				"where parseJoinKeys drops everything without an '='", got.Type)
		}
		if len(got.Predicates) != 1 || !strings.Contains(got.Predicates[0].Raw, "<") {
			t.Errorf("lifted predicates = %+v, want the inequality", got.Predicates)
		}
		if join := got.Children[0]; join.JoinCond != "a.s_nationkey = b.s_nationkey" {
			t.Errorf("JoinCond = %q, want the equality alone", join.JoinCond)
		}
	})

	t.Run("an equality-only condition is untouched", func(t *testing.T) {
		got := liftInnerJoinOnResiduals(build("inner", "a.s_nationkey = b.s_nationkey"))
		if got.Type != NodeJoin {
			t.Fatalf("expected the join unchanged, got %s", got.Type)
		}
	})

	t.Run("a column-vs-literal conjunct is left to extractJoinCondPredicates", func(t *testing.T) {
		got := liftInnerJoinOnResiduals(build("inner", "a.s_nationkey = b.s_nationkey AND a.s_suppkey = 5"))
		if got.Type != NodeJoin {
			t.Fatalf("expected the join unchanged, got %s", got.Type)
		}
	})

	t.Run("lifting every conjunct leaves an explicit cross join", func(t *testing.T) {
		got := liftInnerJoinOnResiduals(build("inner", "a.s_suppkey < b.s_suppkey"))
		if got.Type != NodeFilter {
			t.Fatalf("expected a filter above the join, got %s", got.Type)
		}
		join := got.Children[0]
		if join.JoinCond != "" || joinKind(join.JoinType) != "cross" {
			t.Errorf("join = %q ON %q, want a cross join with no condition", join.JoinType, join.JoinCond)
		}
	})

	t.Run("outer joins keep their ON clause", func(t *testing.T) {
		// An outer join's ON is evaluated BEFORE the NULL-padding, so the
		// residual cannot move above the join without deleting the rows the
		// join is required to preserve. Leaving it in place is not a fix —
		// it is the boundary of this one.
		got := liftInnerJoinOnResiduals(build("left", "a.s_nationkey = b.s_nationkey AND a.s_suppkey < b.s_suppkey"))
		if got.Type != NodeJoin {
			t.Fatalf("expected the join untouched, got %s", got.Type)
		}
		if !strings.Contains(got.JoinCond, "<") {
			t.Errorf("JoinCond = %q, want the residual still on the join", got.JoinCond)
		}
	})

	t.Run("the lifted residual merges into the WHERE filter above", func(t *testing.T) {
		join := build("inner", "a.s_nationkey = b.s_nationkey AND a.s_suppkey < b.s_suppkey")
		plan := NewFilter(join, []Predicate{{Raw: "a.s_suppkey < 5", ASTExpr: tryParseExpr("a.s_suppkey < 5")}})
		got := liftInnerJoinOnResiduals(plan)
		if got.Type != NodeFilter || got.Children[0].Type != NodeJoin {
			t.Fatalf("expected one filter directly over the join, got %s over %s", got.Type, got.Children[0].Type)
		}
		if len(got.Predicates) != 2 {
			t.Errorf("filter carries %d predicates, want 2 — a second filter node between the WHERE and the "+
				"join would strand the WHERE's own conjuncts above it", len(got.Predicates))
		}
	})
}
