package exec

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A KEY THAT NAMES NOTHING IS 0A000, AND THAT IS THE OPERATOR'S PROPERTY.
//
// `exec.Sort`, `exec.Window` and `exec.HashAggregate` each refuse a key or an
// argument their input does not carry, rather than skipping it — a skipped
// sort key is an arbitrary order and a skipped aggregate argument is a wrong
// number, both silent (#313/#314/#316/#386). Every one of those refusals
// reaches a CLIENT for a query PostgreSQL ANSWERS, so #649's invariant covers
// them: it must carry a SQLSTATE, and 0A000 is the class, because the query is
// not wrong — this engine did not implement the shape.
//
// It is asserted HERE, at the operator, rather than only through a query that
// happens to reach it. The two cells that used to carry this in
// `coordinator.TestATaskErrorCarriesItsSQLStateOverTheDAG` were the #807/#658
// shapes, and the planner no longer builds a plan that reaches either
// operator with an unresolvable key — so the classification would have gone
// untested the moment those shapes started answering. A gate whose trigger is
// a CONDITION cannot be relied on to fire (CLAUDE.md, the seam gate's own
// rule); this one needs no plan and no budget.
func TestAnUnresolvableKeyCarriesTheNotImplementedClass(t *testing.T) {
	ctx := context.Background()
	input := func() *batch.RecordBatch {
		b := batch.NewRecordBatch([]parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "g", Type: parquet.TypeInt64},
		}, 2)
		b.Columns[0].Int64Data = []int64{1, 2}
		b.Columns[1].Int64Data = []int64{7, 8}
		b.Columns[0].Nulls.SetValid(0)
		b.Columns[0].Nulls.SetValid(1)
		b.Columns[1].Nulls.SetValid(0)
		b.Columns[1].Nulls.SetValid(1)
		b.Len = 2
		return b
	}

	t.Run("sort", func(t *testing.T) {
		s := &Sort{Keys: []SortKey{{Column: "w"}}, Limit: int(NoLimit)}
		if err := s.Init(ctx); err != nil {
			t.Fatalf("init: %v", err)
		}
		err := s.Consume(ctx, input())
		assertNotImplemented(t, err, `key column "w"`)
	})

	t.Run("window_partition_by", func(t *testing.T) {
		w := NewWindow([]WindowColumn{{
			Func: WinRowNumber, OutputCol: "rn", OutputType: parquet.TypeInt64,
			PartitionBy: []string{"gk"},
		}})
		if err := w.Init(ctx); err != nil {
			t.Fatalf("init: %v", err)
		}
		err := consumeAndFinalize(ctx, w, input())
		assertNotImplemented(t, err, `PARTITION BY "gk"`)
	})

	t.Run("window_order_by", func(t *testing.T) {
		w := NewWindow([]WindowColumn{{
			Func: WinRowNumber, OutputCol: "rn", OutputType: parquet.TypeInt64,
			OrderBy: []SortKey{{Column: "gk"}},
		}})
		if err := w.Init(ctx); err != nil {
			t.Fatalf("init: %v", err)
		}
		err := consumeAndFinalize(ctx, w, input())
		assertNotImplemented(t, err, `ORDER BY "gk"`)
	})

	// The control: a key the input DOES carry is not refused, so the check
	// above is not passing because every key fails.
	t.Run("ctl_a_resolvable_key_is_not_refused", func(t *testing.T) {
		s := &Sort{Keys: []SortKey{{Column: "g"}}, Limit: int(NoLimit)}
		if err := s.Init(ctx); err != nil {
			t.Fatalf("init: %v", err)
		}
		if err := s.Consume(ctx, input()); err != nil {
			t.Fatalf("a key the input carries was refused: %v", err)
		}
	})
}

func consumeAndFinalize(ctx context.Context, w *Window, b *batch.RecordBatch) error {
	if err := w.Consume(ctx, b); err != nil {
		return err
	}
	return w.Finalize(ctx)
}

func assertNotImplemented(t *testing.T, err error, wantIn string) {
	t.Helper()
	if err == nil {
		t.Fatalf("a key naming no column was accepted — a skipped key is an arbitrary " +
			"order or a wrong number, and it is silent")
	}
	if !strings.Contains(err.Error(), wantIn) {
		t.Errorf("error %v\n  want one naming %s", err, wantIn)
	}
	if st := sqlerr.StateOf(err); st != "0A000" {
		t.Errorf("SQLSTATE %q, want 0A000 — this reaches a client for a query "+
			"PostgreSQL answers, so the class owed is \"not implemented here\" "+
			"(#649); a refusal a client cannot classify is half a refusal\n  error: %v",
			st, err)
	}
}
