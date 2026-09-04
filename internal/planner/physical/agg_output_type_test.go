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

	// One scan carrying every type, for the MIN_BY/MAX_BY sweep: their
	// output type is their VALUE argument's, for all 22 of them (#392).
	allTypes := map[string]parquet.TypeID{}
	for _, tc := range allTypeCases() {
		allTypes[tc.col] = tc.typ
	}
	allTypes["id"] = parquet.TypeInt64
	everyType := typedScanNode("typemx", allTypes)

	tests := []struct {
		name string
		node *logical.Node
		agg  logical.AggExpr
		want parquet.TypeID
		// undeclared marks the cases whose contract is "the worker must not
		// be told a type" — as load-bearing as the ones that declare, and
		// no longer expressible as a zero TypeID now that BOOL (which is
		// zero) is a real MIN_BY declaration.
		undeclared bool
	}{
		{"sum is float64 whatever it summed", aggNodeOver(orders),
			logical.AggExpr{Func: "sum", InputCol: "o_totalprice"}, parquet.TypeFloat64, false},
		// PostgreSQL's own types for an integer input (#784), from the live
		// server: `pg_typeof(sum(int8))` is numeric — an int64 sum WRAPS past
		// 2^63 — and `pg_typeof(sum(int4))` is bigint, which has a wider
		// integer type to grow into. o_orderkey is INT64.
		{"sum over a bigint column is numeric", aggNodeOver(orders),
			logical.AggExpr{Func: "sum", InputCol: "o_orderkey"}, parquet.TypeDecimal, false},
		{"avg is float64", aggNodeOver(orders),
			logical.AggExpr{Func: "avg", InputCol: "o_totalprice"}, parquet.TypeFloat64, false},
		{"avg over a bigint column is numeric", aggNodeOver(orders),
			logical.AggExpr{Func: "avg", InputCol: "o_orderkey"}, parquet.TypeDecimal, false},
		{"count is int64", aggNodeOver(orders),
			logical.AggExpr{Func: "count"}, parquet.TypeInt64, false},
		{"count distinct is int64 too", aggNodeOver(orders),
			logical.AggExpr{Func: "count", InputCol: "o_orderkey", Distinct: true}, parquet.TypeInt64, false},
		{"string_agg is a string", aggNodeOver(customer),
			logical.AggExpr{Func: "string_agg", InputCol: "c_name"}, parquet.TypeString, false},

		// MIN/MAX: resolved from the input column.
		{"min over a string column is a string", aggNodeOver(customer),
			logical.AggExpr{Func: "min", InputCol: "c_name"}, parquet.TypeString, false},
		{"max over a date column is a date", aggNodeOver(orders),
			logical.AggExpr{Func: "max", InputCol: "o_orderdate"}, parquet.TypeDate, false},
		{"min over a float column is a float", aggNodeOver(orders),
			logical.AggExpr{Func: "min", InputCol: "o_totalprice"}, parquet.TypeFloat64, false},
		// exec widens int32 to int64 on the way out; the declaration has to
		// say the same thing or the identity row and a populated one disagree.
		{"min over an int32 column widens to int64", aggNodeOver(orders),
			logical.AggExpr{Func: "min", InputCol: "o_shippriority"}, parquet.TypeInt64, false},
		{"a qualified input column resolves on its bare name",
			aggNodeOver(customer), logical.AggExpr{Func: "min", InputCol: "customer.c_name"}, parquet.TypeString, false},
		{"min resolves through a join below the aggregate",
			aggNodeOver(&logical.Node{Type: logical.NodeJoin, Children: []*logical.Node{orders, customer}}),
			logical.AggExpr{Func: "min", InputCol: "c_name"}, parquet.TypeString, false},

		// Undeclared: the worker keeps its conservative behaviour.
		{"min over a column no scan below carries", aggNodeOver(orders),
			logical.AggExpr{Func: "min", InputCol: "nowhere"}, 0, true},
		{"min over a column two scans type differently",
			aggNodeOver(&logical.Node{Type: logical.NodeJoin, Children: []*logical.Node{orders, customer}}),
			logical.AggExpr{Func: "min", InputCol: "o_orderkey"}, 0, true},
		// A DERIVED argument is typed from the EXPRESSION, over the
		// aggregate's own input declarations — the same source the runtime
		// AggColumn and the DAG's AggSpec read (#867). This entry used to
		// record the opposite (undeclared, so FLOAT64 by fall-through), which
		// is what made `SUM(c_i64 * 3000000) + 1` float8 and dropped its
		// outer operand.
		{"min over a derived expression", aggNodeOver(orders),
			logical.AggExpr{Func: "min", InputCol: "expr", InputExpr: &plansql.BinaryOp{
				Op:    "+",
				Left:  &plansql.ColRef{Column: "o_totalprice"},
				Right: &plansql.Lit{Value: "1", Kind: plansql.LitNumber},
			}}, parquet.TypeFloat64, false},
		// An argument whose own type is UNDECIDED keeps the old answer: a
		// column no scan below carries declares nothing, so neither does an
		// aggregate over an expression built on it. The boundary, from the
		// other side.
		{"min over a derived expression on a column nothing declares", aggNodeOver(orders),
			logical.AggExpr{Func: "min", InputCol: "expr", InputExpr: &plansql.BinaryOp{
				Op:    "||",
				Left:  &plansql.ColRef{Column: "nowhere"},
				Right: &plansql.ColRef{Column: "alsonowhere"},
			}}, parquet.TypeString, false},
		{"min over an unannotated scan (no catalog entry)",
			aggNodeOver(typedScanNode("mystery", nil)),
			logical.AggExpr{Func: "min", InputCol: "x"}, 0, true},
		{"min_by over a column no scan below carries", aggNodeOver(orders),
			logical.AggExpr{Func: "min_by", InputCol: "nowhere", InputCol2: "o_orderkey"}, 0, true},
	}
	// #392: MIN_BY/MAX_BY over EVERY type declare that type. Generated from
	// the same table the exec-side and end-to-end gates use, so a 23rd type
	// is covered by adding one row there.
	for _, tc := range allTypeCases() {
		for _, fn := range []string{"min_by", "max_by"} {
			tests = append(tests, struct {
				name       string
				node       *logical.Node
				agg        logical.AggExpr
				want       parquet.TypeID
				undeclared bool
			}{
				name: fn + " over " + tc.col + " declares " + tc.col + "'s own type",
				node: aggNodeOver(everyType),
				agg:  logical.AggExpr{Func: fn, InputCol: tc.col, InputCol2: "id"},
				want: tc.typ,
			})
		}
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, known := aggSpecOutputType(tt.node, tt.agg)
			if known == tt.undeclared {
				t.Fatalf("aggSpecOutputType(%s(%s)) declared=%v, want declared=%v",
					tt.agg.Func, tt.agg.InputCol, known, !tt.undeclared)
			}
			if got != tt.want {
				t.Errorf("aggSpecOutputType(%s(%s)) = %v, want %v",
					tt.agg.Func, tt.agg.InputCol, got, tt.want)
			}
		})
	}
}

// typeCase is one column of the all-types fixture: the column name and the
// catalog type it carries.
type typeCase struct {
	col string
	typ parquet.TypeID
}

// allTypeCases is the 22-type matrix. MIN_BY/MAX_BY must declare each of
// these verbatim — the six-case switch that fell through to FLOAT64 for the
// other sixteen is #392.
func allTypeCases() []typeCase {
	return []typeCase{
		{"c_bool", parquet.TypeBool},
		{"c_i32", parquet.TypeInt32},
		{"c_i64", parquet.TypeInt64},
		{"c_f32", parquet.TypeFloat32},
		{"c_f64", parquet.TypeFloat64},
		{"c_str", parquet.TypeString},
		{"c_bytes", parquet.TypeBytes},
		{"c_ts", parquet.TypeTimestamp},
		{"c_ipv4", parquet.TypeIPv4},
		{"c_ipv6", parquet.TypeIPv6},
		{"c_cidr", parquet.TypeCIDR},
		{"c_mac", parquet.TypeMAC},
		{"c_port", parquet.TypePort},
		{"c_proto", parquet.TypeProtocol},
		{"c_dur", parquet.TypeDuration},
		{"c_uuid", parquet.TypeUUID},
		{"c_date", parquet.TypeDate},
		{"c_dec", parquet.TypeDecimal},
		{"c_arr", parquet.TypeArray},
		{"c_row", parquet.TypeRow},
		{"c_map", parquet.TypeMap},
		{"c_vec", parquet.TypeVector},
	}
}
