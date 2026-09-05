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

// ppoScanPred is the shape attachScanPredicates leaves behind: a structured
// `col <op> literal` copied onto the scan for row-group pruning.
func ppoScanPred(raw string, fromPolicy bool) Predicate {
	ast, err := plansql.ParseExpression(raw)
	if err != nil {
		panic(err)
	}
	return Predicate{Column: "ssn", Op: "=", Value: "x", Raw: raw, ASTExpr: ast, FromPolicy: fromPolicy}
}

// TestCheckPolicyPlanOrderRefusesAPredicateATTACHEDToAPolicedScan is the
// round-5 P2 half of the invariant. A predicate copied ONTO the scan is below
// the projection whatever the node order above it says, and a scan predicate
// prunes row groups by the STORED column's statistics: `… IN (SELECT id FROM
// emp WHERE ssn = '***')` pruned every row group whose stored ssn range
// excluded the mask and answered no rows, while the DAG — which attaches
// nothing there — answered every row. The client moves the constant and reads
// the hidden column's range off the row set.
func TestCheckPolicyPlanOrderRefusesAPredicateATTACHEDToAPolicedScan(t *testing.T) {
	policed := ppoPolicy(ColumnPolicy{Column: "ssn", MaskExpr: "'***'"})
	proj := Projection{Alias: "ssn", Expr: "'***'"}

	t.Run("a user conjunct attached to the scan", func(t *testing.T) {
		scan := NewScan("emp", "e")
		scan.ScanPredicates = []Predicate{ppoScanPred("ssn = 'x'", false)}
		err := CheckPolicyPlanOrder(ppoBarrier(scan, proj), policed)
		if err == nil {
			t.Fatal("a scan predicate over the masked column was accepted below the projection")
		}
		if !strings.Contains(err.Error(), "attached to the scan") {
			t.Fatalf("error %v does not name the attachment", err)
		}
	})

	t.Run("the POLICY's own conjunct is exempt", func(t *testing.T) {
		scan := NewScan("emp", "e")
		scan.ScanPredicates = []Predicate{ppoScanPred("ssn = 'x'", true)}
		if err := CheckPolicyPlanOrder(ppoBarrier(scan, proj), policed); err != nil {
			t.Fatalf("a row filter's own conjunct was refused: %v", err)
		}
	})

	t.Run("an unpoliced conjunct stays", func(t *testing.T) {
		scan := NewScan("emp", "e")
		pred := ppoScanPred("id > 1", false)
		pred.Column = "id"
		scan.ScanPredicates = []Predicate{pred}
		if err := CheckPolicyPlanOrder(ppoBarrier(scan, proj), policed); err != nil {
			t.Fatalf("a conjunct naming no policed column was refused: %v", err)
		}
	})

	t.Run("a partition filter over a policed column", func(t *testing.T) {
		scan := NewScan("emp", "e")
		scan.PartitionFilter = map[string]string{"ssn": "x"}
		if err := CheckPolicyPlanOrder(ppoBarrier(scan, proj), policed); err == nil {
			t.Fatal("a partition filter over the masked column was accepted")
		}
	})
}

// TestInjectColumnPoliciesStripsWhatItCovers is the repair itself: the pass
// that puts the barrier on takes the policed attachments off the scan, so the
// invariant above never has cause to fire on a plan this planner built.
func TestInjectColumnPoliciesStripsWhatItCovers(t *testing.T) {
	policies := []ColumnPolicy{
		{Column: "ssn", MaskExpr: "'***'"},
		{Column: "salary", Denied: true},
	}
	scan := NewScan("emp", "e")
	scan.ScanPredicates = []Predicate{
		ppoScanPred("ssn = 'x'", false),
		func() Predicate { p := ppoScanPred("id > 1", false); p.Column = "id"; return p }(),
		ppoScanPred("ssn = 'row-filter'", true),
	}
	scan.PartitionFilter = map[string]string{"ssn": "x", "dept": "d1"}

	out, unprotected := InjectColumnPolicies(scan, "emp", policies, []string{"id", "dept", "ssn", "salary"})
	if unprotected != 0 {
		t.Fatalf("unprotected = %d, want 0", unprotected)
	}
	if !out.SecurityBarrier {
		t.Fatal("no security projection was injected")
	}
	kept := out.Children[0].ScanPredicates
	if len(kept) != 2 {
		t.Fatalf("scan kept %d predicates, want 2 (the unpoliced one and the policy's own): %+v", len(kept), kept)
	}
	for _, pred := range kept {
		if pred.Column == "ssn" && !pred.FromPolicy {
			t.Fatalf("a user predicate over the masked column survived under the barrier: %+v", pred)
		}
	}
	if _, ok := out.Children[0].PartitionFilter["ssn"]; ok {
		t.Fatal("a partition filter over the masked column survived under the barrier")
	}
	if _, ok := out.Children[0].PartitionFilter["dept"]; !ok {
		t.Fatal("the unpoliced partition filter was dropped")
	}
	if err := CheckPolicyPlanOrder(out, ppoPolicy(policies...)); err != nil {
		t.Fatalf("the stripped plan does not satisfy the invariant: %v", err)
	}
}
