package exec

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestContainerGroupByPastASpillFails pins #566: a GROUP BY over an ARRAY,
// ROW, MAP or VECTOR column answers correctly while the partial aggregate fits
// in memory and FAILS THE QUERY the moment memory pressure forces a
// partial-state drain.
//
// The cause is at the far end of the drain, not in the key: a container group
// key is captured as a partialTagString — its DISPLAY form — and
// writePartialKeyToColumn dispatches on the DESTINATION column's type, where a
// container is in neither the verbatim-bytes arm nor the typed-scalar switch.
// It lands in the default, writePartialKeyFallback does
// `dst.SetValue(i, string(kv.Bytes))`, and a container vector refuses a
// string. That refusal is #361's silent-write guard doing its job: the
// alternative is a silently wrong group key.
//
// It is pinned rather than fixed here because the repair is a way to
// reconstruct a container VALUE from a drained partial state, which the
// display form cannot carry and the merge key is not for — a different piece
// of work from the merge key's own framing bug this file's neighbours
// (TestSerializedKeyMetaMatchesPlainEncoding,
// TestSerializedKeyMetaKeepsNestedValuesDistinct) gate.
//
// The pin has two halves and both are load-bearing:
//
//   - the NO-SPILL run is asserted CORRECT, so the half that works cannot
//     regress behind the pin, and
//   - the spilling run is asserted to FAIL, with the error naming the
//     destination type. It fails the moment a type starts succeeding, which is
//     #566's fix's proof. Delete this test then and gate the spilling run's
//     values against the reference run.
func TestContainerGroupByPastASpillFails(t *testing.T) {
	for _, tc := range []struct {
		name string
		col  parquet.Column
		val  func(g int64) any
		// wantErr is the substring today's failure carries. The destination
		// type is in it on purpose: the same fallback would raise a different
		// one if the capture side changed.
		wantErr string
	}{
		{
			name: "array of int64",
			col: parquet.Column{Name: "k", Type: parquet.TypeArray,
				ElementType: &parquet.Column{Name: "element", Type: parquet.TypeInt64}},
			val:     func(g int64) any { return []any{g} },
			wantErr: "cannot store string into ARRAY vector",
		},
		{
			name: "array of cidr",
			col: parquet.Column{Name: "k", Type: parquet.TypeArray,
				ElementType: &parquet.Column{Name: "element", Type: parquet.TypeCIDR}},
			val:     func(g int64) any { return []any{fmt.Sprintf("10.0.0.%d/32", g%256)} },
			wantErr: "cannot store string into ARRAY vector",
		},
		{
			name: "row",
			col: parquet.Column{Name: "k", Type: parquet.TypeRow,
				Fields: []parquet.Column{{Name: "n", Type: parquet.TypeInt64}}},
			val:     func(g int64) any { return map[string]any{"n": g} },
			wantErr: "cannot store string into ROW vector",
		},
		{
			name: "map",
			col: parquet.Column{Name: "k", Type: parquet.TypeMap,
				ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
					{Name: "key", Type: parquet.TypeString},
					{Name: "value", Type: parquet.TypeInt64},
				}}},
			val:     func(g int64) any { return map[string]any{"a": g} },
			wantErr: "cannot store string into MAP vector",
		},
		{
			name:    "vector",
			col:     parquet.Column{Name: "k", Type: parquet.TypeVector, Dimension: 2},
			val:     func(g int64) any { return []float32{float32(g), 1} },
			wantErr: "cannot store string into VECTOR vector",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const numBatches, rowsPerBatch, numGroups = 25, 200, 200
			schema := []parquet.Column{tc.col, {Name: "v", Type: parquet.TypeInt64}}
			batches := make([]*batch.RecordBatch, 0, numBatches)
			for bi := 0; bi < numBatches; bi++ {
				rows := make([]map[string]any, 0, rowsPerBatch)
				for ri := 0; ri < rowsPerBatch; ri++ {
					g := int64((bi*rowsPerBatch + ri) % numGroups)
					rows = append(rows, map[string]any{"k": tc.val(g), "v": int64(bi*1000 + ri + 1)})
				}
				batches = append(batches, batch.FromRows(schema, rows))
			}
			mk := func(spill *memory.SpillManager) *HashAggregate {
				h := NewHashAggregate([]string{"k"}, []AggColumn{
					{Func: AggSum, InputCol: "v", OutputCol: "total", OutputType: parquet.TypeInt64},
				})
				h.Spill = spill
				return h
			}

			// The half that works, gated so it cannot regress behind the pin.
			if got := len(runHashAggToMap(t, mk(nil), batches)); got != numGroups {
				t.Fatalf("in-memory run: %d groups, want %d", got, numGroups)
			}

			// The half that does not. A tight budget with the reservation
			// already made forces the partial-drain path from the first batch,
			// the way TestExternalMergeSpill_SingleIntKeySum does.
			tracker := memory.NewTracker("test", 1_000)
			sm, err := memory.NewSpillManager(t.TempDir(), tracker)
			if err != nil {
				t.Fatal(err)
			}
			tracker.ForceReserve(900)
			failure := runHashAggFailure(mk(sm), batches)
			if failure == "" {
				t.Errorf("a %s group key now survives a partial-aggregate spill — #566 is FIXED.\n"+
					"Delete this pin and gate the spilling run's values against the in-memory run.",
					tc.col.Type)
				return
			}
			if !strings.Contains(failure, tc.wantErr) {
				t.Errorf("#566's shape changed — re-read it before re-pinning\n  got  %s\n  want a failure containing %q",
					failure, tc.wantErr)
				return
			}
			t.Logf("known failure, NOT gated (#566): %s", failure)
		})
	}
}

// runHashAggFailure drives the aggregate to completion and returns how it
// failed, as an empty string when it did not.
//
// It takes both spellings of failure because this one arrives as a PANIC: the
// silent-write guard raises through batch.Vector.SetValue, which a pipeline
// driver converts back into a query error but a direct operator call does not.
func runHashAggFailure(h *HashAggregate, batches []*batch.RecordBatch) (failure string) {
	defer func() {
		if r := recover(); r != nil {
			failure = fmt.Sprint(r)
		}
	}()
	ctx := context.Background()
	if err := h.Init(ctx); err != nil {
		return err.Error()
	}
	defer func() { _ = h.Close() }()
	for _, b := range batches {
		if err := h.Consume(ctx, b); err != nil {
			return err.Error()
		}
	}
	if err := h.Finalize(ctx); err != nil {
		return err.Error()
	}
	for {
		out, err := h.Next(ctx)
		if err != nil {
			return err.Error()
		}
		if out == nil {
			return ""
		}
	}
}
