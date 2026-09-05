package logical

import (
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

func ppoPolicy(cols ...ColumnPolicy) func(string) []ColumnPolicy {
	return func(table string) []ColumnPolicy {
		if strings.EqualFold(table, "emp") {
			return cols
		}
		return nil
	}
}

func ppoBarrier(child *Node, projections ...Projection) *Node {
	return &Node{Type: NodeProject, Children: []*Node{child},
		Projections: projections, SecurityBarrier: true}
}

func ppoFilter(child *Node, raw string, policy bool) *Node {
	ast, err := plansql.ParseExpression(raw)
	if err != nil {
		panic(err)
	}
	n := NewFilter(child, []Predicate{{Raw: raw, ASTExpr: ast}})
	n.PolicyFilter = policy
	return n
}

// TestCheckPolicyPlanOrderRefusesAScanWithNoProjection is the invariant's own
// gate, and it exists because the claim "the invariant catches that" was
// untested: the round-4 reviewer tried to falsify it by deleting one
// projection and got internal schema errors from downstream instead of this
// refusal, so nothing proved the check itself fires (review P2(r4)).
func TestCheckPolicyPlanOrderRefusesAScanWithNoProjection(t *testing.T) {
	policed := ppoPolicy(ColumnPolicy{Column: "ssn", MaskExpr: "'***'"})

	t.Run("bare scan of a policed relation", func(t *testing.T) {
		plan := &Node{Type: NodeProject, Children: []*Node{NewScan("emp", "e")}}
		err := CheckPolicyPlanOrder(plan, policed)
		if err == nil {
			t.Fatal("a scan of a policed relation with no security projection was accepted")
		}
		if !strings.Contains(err.Error(), "carries no security projection") {
			t.Fatalf("error %v does not name the missing projection", err)
		}
	})

	t.Run("scan buried under a join arm", func(t *testing.T) {
		good := ppoBarrier(NewScan("emp", "a"), Projection{Column: "id", Alias: "id"})
		plan := &Node{Type: NodeJoin, Children: []*Node{good, NewScan("emp", "b")}}
		if err := CheckPolicyPlanOrder(plan, policed); err == nil {
			t.Fatal("the second arm's unprotected scan was accepted")
		}
	})

	t.Run("a scan of an UNPOLICED relation needs nothing", func(t *testing.T) {
		plan := &Node{Type: NodeProject, Children: []*Node{NewScan("other", "o")}}
		if err := CheckPolicyPlanOrder(plan, policed); err != nil {
			t.Fatalf("an unpoliced scan was refused: %v", err)
		}
	})
}

// TestCheckPolicyPlanOrderRefusesAUserPredicateBelowTheProjection is the other
// half: the policy's own filter belongs there, a user's does not.
func TestCheckPolicyPlanOrderRefusesAUserPredicateBelowTheProjection(t *testing.T) {
	policed := ppoPolicy(ColumnPolicy{Column: "ssn", MaskExpr: "'***'"})
	proj := Projection{Alias: "ssn", Expr: "'***'"}

	t.Run("user predicate over the masked column", func(t *testing.T) {
		plan := ppoBarrier(ppoFilter(NewScan("emp", "e"), "ssn = 'x'", false), proj)
		err := CheckPolicyPlanOrder(plan, policed)
		if err == nil {
			t.Fatal("a user predicate below the projection read the stored column")
		}
		if !strings.Contains(err.Error(), "predicate over") {
			t.Fatalf("error %v does not name the predicate", err)
		}
	})

	t.Run("the POLICY's own filter is exempt", func(t *testing.T) {
		plan := ppoBarrier(ppoFilter(NewScan("emp", "e"), "ssn = 'x'", true), proj)
		if err := CheckPolicyPlanOrder(plan, policed); err != nil {
			t.Fatalf("a row filter was refused; it is supposed to read the stored row: %v", err)
		}
	})

	t.Run("a predicate over an UNPOLICED column is fine below it", func(t *testing.T) {
		plan := ppoBarrier(ppoFilter(NewScan("emp", "e"), "id > 1", false), proj)
		if err := CheckPolicyPlanOrder(plan, policed); err != nil {
			t.Fatalf("a predicate naming no policed column was refused: %v", err)
		}
	})

	t.Run("an already-substituted predicate is fine below it", func(t *testing.T) {
		// What predicate substitution produces when it pushes a user filter
		// through the barrier: the reference is gone, the mask is inline.
		plan := ppoBarrier(ppoFilter(NewScan("emp", "e"), "('***') = 'x'", false), proj)
		if err := CheckPolicyPlanOrder(plan, policed); err != nil {
			t.Fatalf("a substituted predicate was refused: %v", err)
		}
	})
}
