package coordinator

import (
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestReAggregatePartialsKeepsContainerGroupsApart gates the coordinator half
// of #566/#576: the cross-worker GROUP BY re-aggregation key for an ARRAY,
// ROW, MAP or VECTOR column has to be INJECTIVE.
//
// reAggregatePartials keyed a container by `fmt.Appendf("%v", ...)`, and no
// container's rendering is one-to-one with its value: ARRAY['a b'] and
// ARRAY['a','b'] both print `[a b]`, ROW{a:'b c:d'} and ROW{a:'b',c:'d'} both
// print `map[a:b c:d]`, and a nested list loses its brackets the same way. Two
// groups every worker — and the whole single-process engine — keeps apart
// merged into one here, and the same query answered differently depending on
// whether it ran distributed.
//
// It was unreachable while a container GROUP BY failed outright at the
// aggregate's partial-state drain; fixing that is what opened this path, so
// the gate lands with it. The key is now the engine's own boxed merge key
// (exec.AppendBoxedGroupKey), which is where the element framing that makes
// it injective already lives.
func TestReAggregatePartialsKeepsContainerGroupsApart(t *testing.T) {
	strElem := &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}

	for _, tc := range []struct {
		name   string
		col    parquet.Column
		a, b   any
		render string // the shared %v rendering that used to collapse them
	}{
		{
			name:   "array element boundary",
			col:    parquet.Column{Name: "k", Type: parquet.TypeArray, ElementType: strElem},
			a:      []any{"a b"},
			b:      []any{"a", "b"},
			render: "[a b]",
		},
		{
			// A ROW's own field list is fixed by the schema, so the boundary
			// a ROW loses is the one INSIDE a container-valued field.
			name: "row over an array field",
			col: parquet.Column{Name: "k", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "l", Type: parquet.TypeArray, Nullable: true, ElementType: strElem},
			}},
			a:      map[string]any{"l": []any{"a b"}},
			b:      map[string]any{"l": []any{"a", "b"}},
			render: "map[l:[a b]]",
		},
		{
			name: "nested array element boundary",
			col: parquet.Column{Name: "k", Type: parquet.TypeArray,
				ElementType: &parquet.Column{Name: "element", Type: parquet.TypeArray, ElementType: strElem}},
			a:      []any{[]any{"a b"}},
			b:      []any{[]any{"a", "b"}},
			render: "[[a b]]",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The premise: these two values DO render alike. If a future
			// change to fmt or to GetValue's boxing broke that, the entry
			// would stop testing anything and should be re-chosen.
			if ra, rb := fmt.Sprintf("%v", tc.a), fmt.Sprintf("%v", tc.b); ra != rb {
				t.Fatalf("the two values no longer render alike (%q vs %q) — pick a colliding pair", ra, rb)
			}

			schema := []parquet.Column{tc.col, {Name: "n", Type: parquet.TypeFloat64}}
			b1 := batch.FromRows(schema, []map[string]any{{"k": tc.a, "n": float64(1)}})
			b2 := batch.FromRows(schema, []map[string]any{{"k": tc.b, "n": float64(1)}})

			mi := &logical.MergeInfo{
				GroupBy:  []string{"k"},
				AggExprs: []logical.AggExpr{{Func: "sum", OutputCol: "n"}},
			}
			c := &Coordinator{}
			out, err := c.reAggregatePartials([]*batch.RecordBatch{b1, b2},
				[]string{"k", "n"}, map[string]int{"k": 0, "n": 1}, mi)
			if err != nil {
				t.Fatalf("reAggregatePartials: %v", err)
			}
			var rows []map[string]any
			for _, o := range out {
				rows = append(rows, o.ToRows()...)
			}
			if len(rows) != 2 {
				t.Fatalf("two distinct container keys re-aggregated into %d group(s) — they both render %q, "+
					"and the merge key must not: %#v", len(rows), tc.render, rows)
			}
			for _, r := range rows {
				if got := r["n"]; got != float64(1) {
					t.Errorf("SUM(n) = %#v, want 1 per group: %#v", got, rows)
				}
			}
		})
	}
}
