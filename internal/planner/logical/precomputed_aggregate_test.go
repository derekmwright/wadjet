package logical

import (
	"testing"
)

func TestSubstitutePreComputedAggregates_MatchesQ17Shape(t *testing.T) {
	// Build the Q17 post-decorrelation shape in miniature:
	//   LEFT JOIN
	//     ├── outer (part × lineitem inner join)
	//     └── Aggregate(GROUP BY l_partkey, AVG(l_quantity)) ← target for substitution
	//           └── Scan(lineitem)
	innerAgg := &Node{
		Type:    NodeAggregate,
		GroupBy: []string{"l_partkey"},
		AggExprs: []AggExpr{
			{Func: "avg", InputCol: "l_quantity", OutputCol: "__scalar_0"},
		},
		Children: []*Node{
			{Type: NodeScan, TableName: "lineitem"},
		},
	}
	outer := &Node{
		Type:     NodeJoin,
		JoinType: "inner",
		Children: []*Node{
			{Type: NodeScan, TableName: "lineitem"},
			{Type: NodeScan, TableName: "part"},
		},
	}
	root := &Node{
		Type:     NodeJoin,
		JoinType: "left",
		Children: []*Node{outer, innerAgg},
	}

	sigs := []PreComputedAggregate{{
		InputTable:     "lineitem",
		GroupByCols:    []string{"l_partkey"},
		AggOutputCols:  []string{"__scalar_0"},
		SyntheticAlias: "__precomp_agg_0",
	}}

	used, err := SubstitutePreComputedAggregates(root, sigs)
	if err != nil {
		t.Fatalf("SubstitutePreComputedAggregates: %v", err)
	}
	if !used["__precomp_agg_0"] {
		t.Errorf("expected substitution to fire; used=%v", used)
	}
	// The LEFT JOIN's right child should now be a Scan of the synthetic alias
	// rather than the Aggregate.
	got := root.Children[1]
	if got.Type != NodeScan || got.TableName != "__precomp_agg_0" {
		t.Errorf("expected right child = Scan(__precomp_agg_0), got Type=%v TableName=%q",
			got.Type, got.TableName)
	}
}

func TestSubstitutePreComputedAggregates_NoMatchWhenGroupByDiffers(t *testing.T) {
	agg := &Node{
		Type:    NodeAggregate,
		GroupBy: []string{"l_orderkey"}, // signature wants l_partkey
		AggExprs: []AggExpr{
			{Func: "avg", InputCol: "l_quantity", OutputCol: "__scalar_0"},
		},
		Children: []*Node{{Type: NodeScan, TableName: "lineitem"}},
	}
	sigs := []PreComputedAggregate{{
		InputTable:     "lineitem",
		GroupByCols:    []string{"l_partkey"},
		AggOutputCols:  []string{"__scalar_0"},
		SyntheticAlias: "__precomp_agg_0",
	}}
	used, err := SubstitutePreComputedAggregates(agg, sigs)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	// Root itself is the aggregate; the substitute-in-children pass does NOT
	// rewrite root directly (caller's tree always has a container). The
	// signature should remain unused.
	if len(used) != 0 {
		t.Errorf("expected no substitution for mismatched GroupBy; used=%v", used)
	}
}

func TestSubstitutePreComputedAggregates_NoMatchWithFilter(t *testing.T) {
	// Scans with pushed-down predicates are not simple pass-throughs and
	// Phase 1 rejects them — the pre-computed aggregate was computed over
	// the full table, so a filtered scan would produce different rows.
	scan := &Node{
		Type:           NodeScan,
		TableName:      "lineitem",
		ScanPredicates: []Predicate{{Column: "l_shipdate", Op: ">=", Value: "1995-01-01"}},
	}
	parent := &Node{
		Type:     NodeJoin,
		JoinType: "left",
		Children: []*Node{
			{Type: NodeScan, TableName: "part"},
			{
				Type:    NodeAggregate,
				GroupBy: []string{"l_partkey"},
				AggExprs: []AggExpr{
					{Func: "avg", InputCol: "l_quantity", OutputCol: "__scalar_0"},
				},
				Children: []*Node{scan},
			},
		},
	}
	sigs := []PreComputedAggregate{{
		InputTable:     "lineitem",
		GroupByCols:    []string{"l_partkey"},
		AggOutputCols:  []string{"__scalar_0"},
		SyntheticAlias: "__precomp_agg_0",
	}}
	used, err := SubstitutePreComputedAggregates(parent, sigs)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(used) != 0 {
		t.Errorf("expected no substitution when scan has filter predicates; used=%v", used)
	}
	// Right child must still be the Aggregate, unchanged.
	if parent.Children[1].Type != NodeAggregate {
		t.Errorf("expected right child unchanged, got Type=%v", parent.Children[1].Type)
	}
}
