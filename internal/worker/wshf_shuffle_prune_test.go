package worker

import (
	"reflect"
	"testing"

	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

func schemaOf(names ...string) []parquet.Column {
	cols := make([]parquet.Column, len(names))
	for i, n := range names {
		cols[i] = parquet.Column{Name: n, Type: parquet.TypeInt64}
	}
	return cols
}

func TestWSHFShufflePruneKeep(t *testing.T) {
	tests := []struct {
		name     string
		schema   []parquet.Column
		declared []string
		keys     []string
		want     []string // nil = no prune
	}{
		{
			name:     "drops undeclared column",
			schema:   schemaOf("o_custkey", "o_orderkey", "o_comment"),
			declared: []string{"o_custkey", "o_orderkey"},
			keys:     []string{"o_custkey"},
			want:     []string{"o_custkey", "o_orderkey"},
		},
		{
			name:     "sibling-table names in declaration are ignored",
			schema:   schemaOf("o_custkey", "o_orderkey", "o_comment"),
			declared: []string{"c_custkey", "o_custkey", "o_orderkey"},
			keys:     []string{"o_custkey"},
			want:     []string{"o_custkey", "o_orderkey"},
		},
		{
			name:     "keys kept even when not declared",
			schema:   schemaOf("o_custkey", "o_orderkey", "o_comment"),
			declared: []string{"o_orderkey"},
			keys:     []string{"o_custkey"},
			want:     []string{"o_custkey", "o_orderkey"},
		},
		{
			name:     "nothing to drop returns nil",
			schema:   schemaOf("o_custkey", "o_orderkey"),
			declared: []string{"o_custkey", "o_orderkey"},
			keys:     []string{"o_custkey"},
			want:     nil,
		},
		{
			name:     "empty declaration returns nil",
			schema:   schemaOf("a", "b"),
			declared: nil,
			keys:     []string{"a"},
			want:     nil,
		},
		{
			name:     "total mismatch falls back to full width",
			schema:   schemaOf("x", "y"),
			declared: []string{"a", "b"},
			keys:     []string{"a"},
			want:     nil,
		},
		{
			name:     "qualified schema column matches unqualified declaration",
			schema:   schemaOf("n1.n_name", "n2.n_name", "s_suppkey", "s_comment"),
			declared: []string{"n_name", "s_suppkey"},
			keys:     []string{"s_suppkey"},
			want:     []string{"n1.n_name", "n2.n_name", "s_suppkey"},
		},
		{
			name:     "qualified declaration matches unqualified schema column",
			schema:   schemaOf("n_name", "s_suppkey", "s_comment"),
			declared: []string{"n1.n_name", "s_suppkey"},
			keys:     []string{"s_suppkey"},
			want:     []string{"n_name", "s_suppkey"},
		},
		{
			name:     "expression names with dots are not treated as qualifiers",
			schema:   schemaOf("sum(x * 0.5)", "k", "extra"),
			declared: []string{"sum(x * 0.5)", "k"},
			keys:     []string{"k"},
			want:     []string{"sum(x * 0.5)", "k"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := wshfShufflePruneKeep(tt.schema, tt.declared, tt.keys)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("wshfShufflePruneKeep() = %v, want %v", got, tt.want)
			}
		})
	}
}
