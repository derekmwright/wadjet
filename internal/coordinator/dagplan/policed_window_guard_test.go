// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"errors"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// A WINDOW OVER A POLICED SCAN THAT FEEDS A JOIN IS REFUSED ON THE DISTRIBUTED
// PATH, and its two neighbours are not — arc LT, policed_window_guard.go.
//
// The refusal is the plan-level half of the nine-door gate
// server.TestArcLTAPerOuterRowBodyReadsThePublishedValueOnEveryDoor: every
// shape below that refuses here ran on the DAG doors and answered the stored
// column's pairing under the mask at 51addfb6. Fails at 51addfb6 (the guard
// did not exist); the two controls planned there and plan here.
func TestArcLTAWindowOverAPolicedScanFeedingAJoinIsRefusedDistributed(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	cols := []string{"c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"}
	mask := []logical.ColumnPolicy{{Column: "c_acctbal", MaskExpr: "0"}}
	cases := []struct {
		name, sql string
		refused   bool
	}{
		{"the planner-minted bound over a self-correlated policed body",
			`SELECT b.c_custkey AS a, s.m AS m FROM customer b JOIN LATERAL (SELECT c.c_custkey AS m FROM customer c WHERE c.c_acctbal = b.c_acctbal ORDER BY c.c_custkey LIMIT 2) s ON true`, true},
		{"the planner-minted bound over another outer relation",
			`SELECT b.n_nationkey AS a, s.m AS m FROM nation b JOIN LATERAL (SELECT c.c_custkey AS m FROM customer c WHERE c.c_acctbal = b.n_nationkey ORDER BY c.c_custkey LIMIT 2) s ON true`, true},
		{"a user window inside the body",
			`SELECT b.c_custkey AS a, s.m AS m, s.rn AS rn FROM customer b JOIN LATERAL (SELECT c.c_custkey AS m, ROW_NUMBER() OVER (PARTITION BY c.c_acctbal ORDER BY c.c_custkey) AS rn FROM customer c WHERE c.c_acctbal = b.c_acctbal) s ON true`, true},
		{"a user QUALIFY in a derived table joined on the masked column",
			`SELECT b.c_custkey AS a, s.m AS m FROM customer b JOIN (SELECT c.c_acctbal AS k, c.c_custkey AS m FROM customer c QUALIFY ROW_NUMBER() OVER (PARTITION BY c.c_acctbal ORDER BY c.c_custkey) <= 2) s ON s.k = b.c_acctbal`, true},
		{"control: the same body with no window",
			`SELECT b.c_custkey AS a, s.m AS m FROM customer b JOIN LATERAL (SELECT c.c_custkey AS m FROM customer c WHERE c.c_acctbal = b.c_acctbal AND c.c_custkey <= 2) s ON true`, false},
		{"control: a window over the policed scan with no join above it",
			`SELECT c_custkey, ROW_NUMBER() OVER (PARTITION BY c_acctbal ORDER BY c_custkey) AS rn FROM customer`, false},
		{"control: the bound over an UNPOLICED self-correlated body",
			`SELECT b.n_nationkey AS a, s.m AS m FROM nation b JOIN LATERAL (SELECT c.n_nationkey AS m FROM nation c WHERE c.n_regionkey = b.n_regionkey ORDER BY c.n_nationkey LIMIT 2) s ON true`, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			parsed, err := plansql.Parse(c.sql)
			if err != nil {
				t.Fatal(err)
			}
			info, err := plansql.ExtractSelect(parsed)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := logical.BuildFromSelect(info)
			if err != nil {
				t.Fatalf("logical plan: %v", err)
			}
			plan, unprotected := logical.InjectColumnPolicies(plan, "customer", mask, cols)
			if unprotected != 0 {
				t.Fatalf("%d scans of customer left unprotected", unprotected)
			}
			annot := func(p *logical.Node) { physical.NewPlanner(cat).AnnotateScanColumns(ctx, p) }
			annot(plan)
			plan = logical.Optimize(plan, annot)
			planner := NewStagePlanner(physical.NewPlanner(cat))
			planner.WorkerCount = 3
			_, err = planner.PlanDistributed(ctx, plan)
			if c.refused {
				if !errors.Is(err, ErrPolicedWindowUnderJoinDistributed) {
					t.Fatalf("planned (err=%v) where the distributed path must refuse: the DAG doors "+
						"answered the stored column's pairing under the mask for this shape\n  %s", err, c.sql)
				}
				return
			}
			if err != nil {
				t.Fatalf("the control refused: %v\n  %s", err, c.sql)
			}
		})
	}
}
