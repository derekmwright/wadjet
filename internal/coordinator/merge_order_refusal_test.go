package coordinator

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// THE MERGE'S TWO REFUSALS, AND THE BOUNDARY BETWEEN THEM — #1002.
//
// `sortBatches` and `topKBatches` apply the query's ordering or say they
// cannot. There are exactly two ways they cannot, and an EMPTY result is
// neither of them:
//
//   - the partials do not share one schema, so `coalesceForOrdering` declines
//     and there is no one relation to order;
//   - a key does not resolve over the merged columns, which used to be a
//     silent skip and the whole of #1002.
//
// Both are 0A000: PostgreSQL ANSWERS these statements and what wadjet is saying
// is that its own merge cannot, which is the class the rest of this family uses
// (#811 family C). A refusal with no SQLSTATE reaches a client as an opaque
// string.
//
// It is a UNIT fixture because no SQL I can build produces partials that
// disagree on their schema — every merge input in this engine comes from one
// aggregate or one fragment shape. The refusal exists for the day one does; a
// gate at the level the condition is written is what keeps it honest meanwhile.
func TestTheMergeRefusesAnOrderingItCannotApply(t *testing.T) {
	c := &Coordinator{}
	schemaA := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "k", Type: parquet.TypeInt64, Nullable: true},
	}
	schemaB := []parquet.Column{{Name: "z", Type: parquet.TypeInt64}}
	mismatched := func() []*batch.RecordBatch {
		return []*batch.RecordBatch{
			batch.FromRows(schemaA, []map[string]any{
				{"id": int64(1), "k": int64(30)},
				{"id": int64(2), "k": int64(10)},
			}),
			batch.FromRows(schemaB, []map[string]any{{"z": int64(9)}}),
		}
	}
	emptyMismatched := func() []*batch.RecordBatch {
		return []*batch.RecordBatch{
			batch.FromRows(schemaA, nil),
			batch.FromRows(schemaB, nil),
		}
	}
	one := func() []*batch.RecordBatch {
		return []*batch.RecordBatch{batch.FromRows(schemaA, []map[string]any{
			{"id": int64(1), "k": int64(30)},
			{"id": int64(2), "k": int64(10)},
		})}
	}
	byK := []logical.OrderExpr{{Column: "k"}}

	for _, tc := range []struct {
		name string
		run  func() ([]*batch.RecordBatch, error)
		// want is a substring of the refusal, or "" for no refusal at all.
		want string
	}{
		{
			name: "sortBatches: partials that do not share one schema",
			run:  func() ([]*batch.RecordBatch, error) { return c.sortBatches(mismatched(), byK) },
			want: "2 partial batches carrying 3 rows do not share one schema",
		},
		{
			name: "topKBatches: the same, through the other comparator",
			run:  func() ([]*batch.RecordBatch, error) { return c.topKBatches(mismatched(), byK, 1) },
			want: "2 partial batches carrying 3 rows do not share one schema",
		},
		{
			name: "sortBatches: a key that does not resolve",
			run: func() ([]*batch.RecordBatch, error) {
				return c.sortBatches(one(), []logical.OrderExpr{{Column: "q.nope"}})
			},
			want: `ORDER BY key "q.nope" does not resolve in the merged columns [id k]`,
		},
		{
			name: "topKBatches: a key that does not resolve",
			run: func() ([]*batch.RecordBatch, error) {
				return c.topKBatches(one(), []logical.OrderExpr{{Column: "q.nope"}}, 1)
			},
			want: `ORDER BY key "q.nope" does not resolve in the merged columns [id k]`,
		},
		{
			// THE BOUNDARY: batches that do not share one schema but carry no
			// ROWS between them. An empty result has no order to get wrong, so
			// the refusal must not fire — the defect `b311edd7` fixed.
			name: "boundary: mismatched schemas carrying no rows is not a refusal",
			run:  func() ([]*batch.RecordBatch, error) { return c.sortBatches(emptyMismatched(), byK) },
		},
		{
			// CONTROL: one batch, a key that resolves. The ordering is applied
			// and nothing is said.
			name: "control: one batch and a key that resolves",
			run:  func() ([]*batch.RecordBatch, error) { return c.sortBatches(one(), byK) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.run()
			if tc.want == "" {
				if err != nil {
					t.Fatalf("got a refusal where none is owed: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("no refusal; want one carrying %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("refusal = %q, want it to carry %q", err, tc.want)
			}
			// A refusal carries its CLASS, or a client cannot act on it
			// (#811 family C, ADR-0012). PostgreSQL answers these statements;
			// what wadjet is saying is that its own merge cannot.
			if got := sqlerr.StateOf(err); got != "0A000" {
				t.Errorf("SQLSTATE = %q, want 0A000: %v", got, err)
			}
		})
	}
}
