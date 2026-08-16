package worker

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// staticBatchSource yields count copies of the same (g, v) batch content,
// each a freshly-allocated RecordBatch.
type staticBatchSource struct {
	schema []parquet.Column
	rows   [][2]int64
	count  int
	served int
}

func (s *staticBatchSource) Init(context.Context) error { return nil }
func (s *staticBatchSource) Close() error               { return nil }
func (s *staticBatchSource) Next(context.Context) (*batch.RecordBatch, error) {
	if s.served >= s.count {
		return nil, nil
	}
	s.served++
	b := batch.NewRecordBatch(s.schema, len(s.rows))
	for i, r := range s.rows {
		b.Columns[0].Int64Data[i] = r[0]
		b.Columns[0].Nulls.SetValid(i)
		b.Columns[1].Int64Data[i] = r[1]
		b.Columns[1].Nulls.SetValid(i)
	}
	return b, nil
}

// dropFirstNOp drops its first n input batches and passes the rest through.
// Clones pass everything — the discriminator that pins the fully-filtered
// stream onto the ORIGINAL chain (the primary sink's chain, worker 0 and the
// serial continuation) while clone sinks accumulate state.
type dropFirstNOp struct {
	n    int
	seen int
}

func (o *dropFirstNOp) Init(context.Context) error { return nil }
func (o *dropFirstNOp) Close() error               { return nil }
func (o *dropFirstNOp) Execute(_ context.Context, b *batch.RecordBatch) (*batch.RecordBatch, error) {
	if o.seen < o.n {
		o.seen++
		return nil, nil
	}
	return b, nil
}
func (o *dropFirstNOp) Clone() exec.UnaryOperator { return &dropFirstNOp{n: 0} }

// TestRunBreakerConsumeParallel_AdoptedPrimaryConsumesSerially is the
// deterministic #279 repro at the runBreakerConsumeParallel layer (SF100 Q18
// join-8 under fuseJoinShuffle):
//
//   - the shared pool is pre-charged over the SpillCheap threshold, so every
//     consumer collapses after exactly one morsel;
//   - the ORIGINAL chain drops its first two batches (the warmup + worker 0's
//     single pre-collapse morsel), so the PRIMARY sink never consumes and
//     never runs resolveIndices;
//   - the clone chains pass everything, so each clone sink holds group state
//     at the merge — the first merge takes adoptStateFrom into the empty
//     primary (resolved=true adopted);
//   - the serial continuation then consumes the remaining batches through the
//     primary. Pre-fix its nil batchUpdaters scratch panicked with
//     index-out-of-range on the first post-merge batch.
//
// Every batch is identical and exactly two are dropped, so the expected
// totals are deterministic regardless of which worker consumed which morsel.
func TestRunBreakerConsumeParallel_AdoptedPrimaryConsumesSerially(t *testing.T) {
	const numBatches = 32
	const droppedBatches = 2 // warmup + worker 0's one pre-collapse morsel
	const groups = 10
	const rowsPerBatch = 40

	schema := []parquet.Column{
		{Name: "g", Type: parquet.TypeInt64, Nullable: true},
		{Name: "v", Type: parquet.TypeInt64, Nullable: true},
	}
	rows := make([][2]int64, rowsPerBatch)
	perBatchGroupSum := map[int64]int64{}
	for i := range rows {
		g := int64(i % groups)
		v := int64(i + 1)
		rows[i] = [2]int64{g, v}
		perBatchGroupSum[g] += v
	}

	store := objstore.NewMemStore()
	executor := NewExecutor(store, NewLRUCache(1024*1024), nil)
	executor.SetSharedPoolBudget(1 << 20)
	// Past the 40% SpillCheap threshold before the task starts: the collapse
	// check trips on every consumer's first morsel.
	if err := executor.sharedTracker.Reserve(600 << 10); err != nil {
		t.Fatalf("pre-charging shared pool: %v", err)
	}

	ctx := context.Background()
	src := &staticBatchSource{schema: schema, rows: rows, count: numBatches}
	ops := []exec.UnaryOperator{&dropFirstNOp{n: droppedBatches}}
	for _, op := range ops {
		if err := op.Init(ctx); err != nil {
			t.Fatalf("op init: %v", err)
		}
	}
	sink := exec.NewHashAggregate([]string{"g"}, []exec.AggColumn{
		{Func: exec.AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64},
	})
	if err := sink.Init(ctx); err != nil {
		t.Fatalf("sink init: %v", err)
	}
	task := distributed.Task{ID: "brk-adopt", QueryID: "q-brk-adopt", StageID: "join-8", StageType: "join"}

	// Pre-fix: runtime panic "index out of range [0] with length 0" at
	// consumeBatch's updater selection, from the serial continuation.
	if err := executor.runBreakerConsumeParallel(ctx, task, src, ops, sink, 4, nil); err != nil {
		t.Fatalf("runBreakerConsumeParallel: %v", err)
	}
	if got := executor.morselCollapses.Load(); got < 1 {
		t.Fatalf("expected a pressure collapse (repro precondition), counter = %d", got)
	}

	got := map[int64]int64{}
	for {
		b, err := sink.Next(ctx)
		if err != nil {
			t.Fatalf("drain: %v", err)
		}
		if b == nil {
			break
		}
		gi, si := b.ColumnIndex("g"), b.ColumnIndex("s")
		if gi < 0 || si < 0 {
			t.Fatalf("output schema missing g/s: %v", b.Schema)
		}
		for i := 0; i < b.ActiveLen(); i++ {
			row := i
			if b.Sel != nil {
				row = int(b.Sel[i])
			}
			got[b.Columns[gi].Int64Data[row]] += b.Columns[si].Int64Data[row]
		}
	}
	if len(got) != groups {
		t.Fatalf("output groups = %d, want %d (%v)", len(got), groups, got)
	}
	const consumed = numBatches - droppedBatches
	for g, per := range perBatchGroupSum {
		want := per * consumed
		if got[g] != want {
			t.Errorf("group %d: sum = %d, want %d (batch content ×%d)", g, got[g], want, consumed)
		}
	}
}
