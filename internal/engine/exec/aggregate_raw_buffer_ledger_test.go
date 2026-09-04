package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The raw-row buffer charges the tracker for what it holds, and releases
// exactly that (#862).
//
// `spillBufferBytes` was accumulated at the buffer's append site and never
// charged, while THREE sites released it: `flushSpillBuffer` ("release the
// tracker bytes charged for them" — which nothing had charged),
// `Finalize`'s drain of the unflushed tail, and `Close`. So every query that
// took the raw-row branch gave the tracker back bytes it never took, and the
// query's ledger went NEGATIVE — 931,840 released against zero charged on the
// filing's shape, `used` at -165,652. From there every admission on that query
// is measured against a floor below the memory that exists, so the budget
// stops bounding anything.
//
// The rows are the operator's: `ToRows` copies them out of the batch, which
// the pipeline releases as soon as Consume returns. They are exactly the kind
// of resident state ADR-0006 charges.
func TestTheRawRowBufferChargesWhatItReleases(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64, Nullable: true},
		{Name: "v", Type: parquet.TypeInt64, Nullable: true},
	}
	newBatches := func() []*batch.RecordBatch {
		var out []*batch.RecordBatch
		for bi := 0; bi < 6; bi++ {
			b := batch.NewRecordBatch(schema, 400)
			for i := 0; i < 400; i++ {
				b.Columns[0].Int64Data[i] = int64((bi*400 + i) % 37)
				b.Columns[1].Int64Data[i] = int64(bi*400 + i)
			}
			b.Len = 400
			out = append(out, b)
		}
		return out
	}

	// Two shapes: one that FLUSHES the buffer to a file (so the
	// flushSpillBuffer release runs) and one that keeps it to the end (so
	// Finalize's tail release runs). Both released bytes nothing charged.
	for _, tc := range []struct {
		name          string
		fileTarget    int64
		wantSpillFile bool
	}{
		{"flushed-to-a-file", 4000, true},
		{"kept-to-finalize", 1 << 40, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func(v int64) { spillFileTargetBytes = v }(spillFileTargetBytes)
			spillFileTargetBytes = tc.fileTarget

			tracker := memory.NewTracker("query", 1024)
			sm, err := memory.NewSpillManager(t.TempDir(), tracker)
			if err != nil {
				t.Fatal(err)
			}
			defer sm.Cleanup()
			// The pressure this aggregate reacts to belongs to somebody else,
			// which is the real shape: an operator spills because the QUERY is
			// over budget. It is never released, so it is the baseline every
			// figure below is measured against.
			const baseline = 900
			tracker.ForceReserve(baseline)

			// COUNT(DISTINCT) is non-simple, so canUseExternalMerge is false
			// and a grouped shape under pressure takes the raw-row buffer.
			h := NewHashAggregate([]string{"k"}, []AggColumn{
				{Func: AggCountDistinct, InputCol: "v", OutputCol: "nd", OutputType: parquet.TypeInt64},
			})
			h.Spill = sm

			ctx := context.Background()
			if err := h.Init(ctx); err != nil {
				t.Fatalf("Init: %v", err)
			}
			buffered := false
			for i, b := range newBatches() {
				if err := h.Consume(ctx, b); err != nil {
					t.Fatalf("Consume #%d: %v", i, err)
				}
				if h.spillBufferBytes > 0 || len(h.spillFiles) > 0 {
					buffered = true
				}
			}
			// ENGAGEMENT: without this the cell is a no-op on an aggregate
			// that never took the branch (ADR-0027 decision 5).
			if !buffered {
				t.Fatalf("the raw-row buffer was never used, so this cell asserts nothing")
			}
			if got := len(h.spillFiles) > 0; got != tc.wantSpillFile {
				t.Fatalf("spill files present = %v, this cell is about %v", got, tc.wantSpillFile)
			}
			if used := tracker.Used(); used < baseline {
				t.Fatalf("mid-consume the query tracker holds %d, below the %d somebody else "+
					"reserved: this operator has already given back bytes it never took", used, baseline)
			}

			if err := h.Finalize(ctx); err != nil {
				t.Fatalf("Finalize: %v", err)
			}
			for {
				out, err := h.Next(ctx)
				if err != nil {
					t.Fatalf("Next: %v", err)
				}
				if out == nil {
					break
				}
			}
			if err := h.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			// CONSERVED: the operator took bytes and gave back exactly those.
			// A closed aggregate holds nothing of its own, so what is left is
			// the baseline somebody else reserved — no more, and no less.
			if used := tracker.Used(); used != baseline {
				t.Fatalf("after Close the query tracker holds %d, want the %d that was reserved "+
					"before this aggregate ran (delta %+d)", used, baseline, used-baseline)
			}
			// THE SYMPTOM, as behaviour rather than as a number. A negative
			// ledger is not a cosmetic error: from that point every admission
			// is measured against a floor below the memory that exists, so a
			// request the budget cannot hold is granted. 900 of 1,024 are
			// still held, so 200 does not fit and must be refused.
			if err := tracker.Reserve(200); err == nil {
				tracker.Release(200)
				t.Fatalf("a 200-byte reservation was GRANTED against a 1024-byte budget "+
					"holding %d: the ledger is measuring admissions against a floor lower "+
					"than the memory that exists", tracker.Used())
			}
		})
	}
}
