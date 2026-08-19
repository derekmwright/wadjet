package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// aggNodeOver builds an Aggregate node over the given scans, standing in for
// the subtree walkStages hands aggSpecOutputType.
func aggNodeOver(scans ...*logical.Node) *logical.Node {
	children := make([]*logical.Node, len(scans))
	copy(children, scans)
	return &logical.Node{Type: logical.NodeAggregate, Children: children}
}

func typedScanNode(table string, cols map[string]parquet.TypeID) *logical.Node {
	return &logical.Node{Type: logical.NodeScan, TableName: table, ScanColTypes: cols}
}

// TestAggSpecOutputType covers the declaration the distributed AggSpec now
// carries. Every family except MIN/MAX is input-independent in this engine,
// so the function name alone settles it; MIN/MAX follow the input column and
// are declared only when it resolves to exactly one catalog type. An
// undeclared (zero) answer is the contract for "the worker must not guess",
// so the cases that produce one are as load-bearing as the ones that don't.
func TestAggSpecOutputType(t *testing.T) {
	orders := typedScanNode("orders", map[string]parquet.TypeID{
		"o_totalprice":   parquet.TypeFloat64,
		"o_orderdate":    parquet.TypeDate,
		"o_orderkey":     parquet.TypeInt64,
		"o_shippriority": parquet.TypeInt32,
	})
	customer := typedScanNode("customer", map[string]parquet.TypeID{
		"c_name":     parquet.TypeString,
		"c_custkey":  parquet.TypeInt64,
		"o_orderkey": parquet.TypeString, // deliberate conflict with orders
	})

	tests := []struct {
		name string
		node *logical.Node
		agg  logical.AggExpr
		want parquet.TypeID
	}{
		{"sum is float64 whatever it summed", aggNodeOver(orders),
			logical.AggExpr{Func: "sum", InputCol: "o_totalprice"}, parquet.TypeFloat64},
		{"sum over an int column is still float64", aggNodeOver(orders),
			logical.AggExpr{Func: "sum", InputCol: "o_orderkey"}, parquet.TypeFloat64},
		{"avg is float64", aggNodeOver(orders),
			logical.AggExpr{Func: "avg", InputCol: "o_totalprice"}, parquet.TypeFloat64},
		{"count is int64", aggNodeOver(orders),
			logical.AggExpr{Func: "count"}, parquet.TypeInt64},
		{"count distinct is int64 too", aggNodeOver(orders),
			logical.AggExpr{Func: "count", InputCol: "o_orderkey", Distinct: true}, parquet.TypeInt64},
		{"string_agg is a string", aggNodeOver(customer),
			logical.AggExpr{Func: "string_agg", InputCol: "c_name"}, parquet.TypeString},

		// MIN/MAX: resolved from the input column.
		{"min over a string column is a string", aggNodeOver(customer),
			logical.AggExpr{Func: "min", InputCol: "c_name"}, parquet.TypeString},
		{"max over a date column is a date", aggNodeOver(orders),
			logical.AggExpr{Func: "max", InputCol: "o_orderdate"}, parquet.TypeDate},
		{"min over a float column is a float", aggNodeOver(orders),
			logical.AggExpr{Func: "min", InputCol: "o_totalprice"}, parquet.TypeFloat64},
		// exec widens int32 to int64 on the way out; the declaration has to
		// say the same thing or the identity row and a populated one disagree.
		{"min over an int32 column widens to int64", aggNodeOver(orders),
			logical.AggExpr{Func: "min", InputCol: "o_shippriority"}, parquet.TypeInt64},
		{"a qualified input column resolves on its bare name",
			aggNodeOver(customer), logical.AggExpr{Func: "min", InputCol: "customer.c_name"}, parquet.TypeString},
		{"min resolves through a join below the aggregate",
			aggNodeOver(&logical.Node{Type: logical.NodeJoin, Children: []*logical.Node{orders, customer}}),
			logical.AggExpr{Func: "min", InputCol: "c_name"}, parquet.TypeString},

		// Undeclared: the worker keeps its conservative behaviour.
		{"min over a column no scan below carries", aggNodeOver(orders),
			logical.AggExpr{Func: "min", InputCol: "nowhere"}, 0},
		{"min over a column two scans type differently",
			aggNodeOver(&logical.Node{Type: logical.NodeJoin, Children: []*logical.Node{orders, customer}}),
			logical.AggExpr{Func: "min", InputCol: "o_orderkey"}, 0},
		{"min over a derived expression", aggNodeOver(orders),
			logical.AggExpr{Func: "min", InputCol: "expr", InputExpr: &plansql.BinaryOp{
				Op:    "+",
				Left:  &plansql.ColRef{Column: "o_totalprice"},
				Right: &plansql.Lit{Value: "1", Kind: plansql.LitNumber},
			}}, 0},
		{"min over an unannotated scan (no catalog entry)",
			aggNodeOver(typedScanNode("mystery", nil)),
			logical.AggExpr{Func: "min", InputCol: "x"}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := aggSpecOutputType(tt.node, tt.agg); got != tt.want {
				t.Errorf("aggSpecOutputType(%s(%s)) = %v, want %v",
					tt.agg.Func, tt.agg.InputCol, got, tt.want)
			}
		})
	}
}
