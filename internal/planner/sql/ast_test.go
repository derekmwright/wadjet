package sql

import (
	"testing"
)

func TestFindNestedAggregate(t *testing.T) {
	tests := []struct {
		name    string
		node    Node
		wantNil bool
		wantAgg string // expected aggregate function name
	}{
		{
			name:    "direct aggregate FuncCallNode",
			node:    &FuncCallNode{Name: "sum", Args: []Node{&ColRef{Column: "x"}}},
			wantAgg: "sum",
		},
		{
			name: "aggregate through non-aggregate FuncCallNode",
			node: &FuncCallNode{
				Name: "format_bytes",
				Args: []Node{
					&FuncCallNode{Name: "sum", Args: []Node{&ColRef{Column: "rx_bytes"}}},
				},
			},
			wantAgg: "sum",
		},
		{
			name: "aggregate in BinaryOp left",
			node: &BinaryOp{
				Left:  &FuncCallNode{Name: "sum", Args: []Node{&ColRef{Column: "x"}}},
				Op:    "*",
				Right: &Lit{Value: "0.001", Kind: LitNumber},
			},
			wantAgg: "sum",
		},
		{
			name: "aggregate in BinaryOp right",
			node: &BinaryOp{
				Left:  &Lit{Value: "100", Kind: LitNumber},
				Op:    "*",
				Right: &FuncCallNode{Name: "avg", Args: []Node{&ColRef{Column: "y"}}},
			},
			wantAgg: "avg",
		},
		{
			name: "aggregate in ParenNode",
			node: &ParenNode{
				Inner: &FuncCallNode{Name: "count", Star: true},
			},
			wantAgg: "count",
		},
		{
			name: "aggregate in UnaryOp",
			node: &UnaryOp{
				Op:    "-",
				Inner: &FuncCallNode{Name: "min", Args: []Node{&ColRef{Column: "val"}}},
			},
			wantAgg: "min",
		},
		{
			name: "aggregate in CastNode",
			node: &CastNode{
				Inner:    &FuncCallNode{Name: "max", Args: []Node{&ColRef{Column: "score"}}},
				TypeName: "integer",
			},
			wantAgg: "max",
		},
		{
			name: "aggregate in CaseNode when",
			node: &CaseNode{
				Whens: []WhenClause{
					{
						Cond:   &Lit{Value: "true", Kind: LitBool},
						Result: &FuncCallNode{Name: "sum", Args: []Node{&ColRef{Column: "a"}}},
					},
				},
			},
			wantAgg: "sum",
		},
		{
			name: "aggregate in CmpExpr",
			node: &CmpExpr{
				Left:  &FuncCallNode{Name: "count", Star: true},
				Op:    ">",
				Right: &Lit{Value: "5", Kind: LitNumber},
			},
			wantAgg: "count",
		},
		{
			name:    "no aggregate - simple ColRef",
			node:    &ColRef{Column: "name"},
			wantNil: true,
		},
		{
			name:    "no aggregate - literal",
			node:    &Lit{Value: "42", Kind: LitNumber},
			wantNil: true,
		},
		{
			name: "no aggregate - non-aggregate function",
			node: &FuncCallNode{
				Name: "upper",
				Args: []Node{&ColRef{Column: "name"}},
			},
			wantNil: true,
		},
		{
			name:    "nil node",
			node:    nil,
			wantNil: true,
		},
		{
			name: "deeply nested aggregate through scalar and BinaryOp",
			node: &BinaryOp{
				Left: &FuncCallNode{
					Name: "format_bytes",
					Args: []Node{
						&BinaryOp{
							Left:  &FuncCallNode{Name: "sum", Args: []Node{&ColRef{Column: "bytes"}}},
							Op:    "/",
							Right: &Lit{Value: "1024", Kind: LitNumber},
						},
					},
				},
				Op:    "||",
				Right: &Lit{Value: " per KB", Kind: LitString},
			},
			wantAgg: "sum",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FindNestedAggregate(tt.node)
			if tt.wantNil {
				if got != nil {
					t.Errorf("FindNestedAggregate() = %v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatal("FindNestedAggregate() = nil, want non-nil")
			}
			if got.Name != tt.wantAgg {
				t.Errorf("FindNestedAggregate().Name = %q, want %q", got.Name, tt.wantAgg)
			}
		})
	}
}

func TestReplaceAggregate(t *testing.T) {
	tests := []struct {
		name    string
		node    Node
		aggName string
		want    string // expected String() of result
	}{
		{
			name:    "direct aggregate replaced with ColRef",
			node:    &FuncCallNode{Name: "sum", Args: []Node{&ColRef{Column: "x"}}},
			aggName: "__agg_0",
			want:    "__agg_0",
		},
		{
			name: "scalar function wrapping aggregate",
			node: &FuncCallNode{
				Name: "format_bytes",
				Args: []Node{
					&FuncCallNode{Name: "sum", Args: []Node{&ColRef{Column: "rx_bytes"}}},
				},
			},
			aggName: "__agg_1",
			want:    "format_bytes(__agg_1)",
		},
		{
			name: "BinaryOp with aggregate on left",
			node: &BinaryOp{
				Left:  &FuncCallNode{Name: "sum", Args: []Node{&ColRef{Column: "x"}}},
				Op:    "*",
				Right: &Lit{Value: "0.001", Kind: LitNumber},
			},
			aggName: "__agg_2",
			want:    "__agg_2 * 0.001",
		},
		{
			name: "BinaryOp with aggregate on right",
			node: &BinaryOp{
				Left:  &Lit{Value: "100", Kind: LitNumber},
				Op:    "/",
				Right: &FuncCallNode{Name: "count", Star: true},
			},
			aggName: "cnt",
			want:    "100 / cnt",
		},
		{
			name: "ParenNode wrapping aggregate",
			node: &ParenNode{
				Inner: &FuncCallNode{Name: "avg", Args: []Node{&ColRef{Column: "val"}}},
			},
			aggName: "avg_val",
			want:    "(avg_val)",
		},
		{
			name: "CastNode wrapping aggregate",
			node: &CastNode{
				Inner:    &FuncCallNode{Name: "sum", Args: []Node{&ColRef{Column: "x"}}},
				TypeName: "bigint",
			},
			aggName: "total",
			want:    "cast(total as bigint)",
		},
		{
			name: "UnaryOp wrapping aggregate",
			node: &UnaryOp{
				Op:    "-",
				Inner: &FuncCallNode{Name: "sum", Args: []Node{&ColRef{Column: "x"}}},
			},
			aggName: "neg_sum",
			want:    "-neg_sum",
		},
		{
			name:    "no aggregate returns original node unchanged",
			node:    &ColRef{Column: "name"},
			aggName: "__agg_0",
			want:    "name",
		},
		{
			name: "non-aggregate function with no aggregate args returns unchanged",
			node: &FuncCallNode{
				Name: "upper",
				Args: []Node{&ColRef{Column: "name"}},
			},
			aggName: "__agg_0",
			want:    "upper(name)",
		},
		{
			name:    "nil node returns nil",
			node:    nil,
			aggName: "__agg_0",
		},
		{
			name: "nested scalar functions with aggregate deep inside",
			node: &FuncCallNode{
				Name: "concat",
				Args: []Node{
					&FuncCallNode{
						Name: "format_bytes",
						Args: []Node{
							&FuncCallNode{Name: "sum", Args: []Node{&ColRef{Column: "bytes"}}},
						},
					},
					&Lit{Value: "/s", Kind: LitString},
				},
			},
			aggName: "__agg_deep",
			want:    "concat(format_bytes(__agg_deep), '/s')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ReplaceAggregate(tt.node, tt.aggName)
			if tt.node == nil {
				if got != nil {
					t.Errorf("ReplaceAggregate(nil) = %v, want nil", got)
				}
				return
			}
			gotStr := got.String()
			if gotStr != tt.want {
				t.Errorf("ReplaceAggregate().String() = %q, want %q", gotStr, tt.want)
			}
		})
	}
}

func TestReplaceAggregatePreservesNonAggFuncFields(t *testing.T) {
	// Ensure that when ReplaceAggregate rebuilds a non-aggregate FuncCallNode,
	// it preserves the Distinct and Star fields.
	node := &FuncCallNode{
		Name: "my_scalar",
		Args: []Node{
			&FuncCallNode{Name: "sum", Args: []Node{&ColRef{Column: "x"}}},
		},
		Distinct: true,
	}
	got := ReplaceAggregate(node, "__agg_0")
	fn, ok := got.(*FuncCallNode)
	if !ok {
		t.Fatalf("expected *FuncCallNode, got %T", got)
	}
	if fn.Name != "my_scalar" {
		t.Errorf("Name = %q, want %q", fn.Name, "my_scalar")
	}
	if !fn.Distinct {
		t.Error("Distinct should be preserved as true")
	}
	// The inner aggregate should have been replaced with a ColRef
	if len(fn.Args) != 1 {
		t.Fatalf("expected 1 arg, got %d", len(fn.Args))
	}
	colRef, ok := fn.Args[0].(*ColRef)
	if !ok {
		t.Fatalf("expected arg to be *ColRef, got %T", fn.Args[0])
	}
	if colRef.Column != "__agg_0" {
		t.Errorf("ColRef.Column = %q, want %q", colRef.Column, "__agg_0")
	}
}
