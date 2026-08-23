package exec

import (
	"context"
	"sync"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// sharedClosureSource hands out one warm-up batch whose rows the leading
// conjunct rejects, then many identical data batches whose rows it accepts.
//
// That shape is the point of the test: Pipeline.runParallel pushes the FIRST
// batch through the original operator chain single-threaded, which is what
// normally fills a predicate's lazy column-index cache before any clone runs.
// When the leading filter drops every row of that batch, the chain stops
// there and the predicates below it are still unresolved when 100 workers
// start calling them at once.
type sharedClosureSource struct {
	schema  []parquet.Column
	warmup  []map[string]any
	data    []map[string]any
	batches int

	mu     sync.Mutex
	served int
}

func (s *sharedClosureSource) Init(_ context.Context) error { return nil }

func (s *sharedClosureSource) Next(_ context.Context) (*batch.RecordBatch, error) {
	s.mu.Lock()
	if s.served >= s.batches {
		s.mu.Unlock()
		return nil, nil
	}
	rows := s.data
	if s.served == 0 {
		rows = s.warmup
	}
	s.served++
	s.mu.Unlock()
	return batch.FromRows(s.schema, rows), nil
}

func (s *sharedClosureSource) Close() error { return nil }

// sharedClosureSchema covers every literal conversion ColumnCompare used to
// memoize inside its closure: the int form (IPV4, MAC) and the string form
// (IPV6, UUID), whose two-word header made a torn read a garbage comparison
// rather than a stale value.
func sharedClosureSchema() []parquet.Column {
	return []parquet.Column{
		{Name: "gate", Type: parquet.TypeInt64},
		{Name: "ip", Type: parquet.TypeIPv4},
		{Name: "mac", Type: parquet.TypeMAC},
		{Name: "ip6", Type: parquet.TypeIPv6},
		{Name: "uu", Type: parquet.TypeUUID},
		{Name: "s", Type: parquet.TypeString},
	}
}

const (
	sharedClosureIPv4 = "10.0.0.7"
	sharedClosureMAC  = "aa:bb:cc:dd:ee:ff"
	sharedClosureIPv6 = "2001:db8::1"
	sharedClosureUUID = "550e8400-e29b-41d4-a716-446655440000"
	sharedClosureStr  = "wadjet-42"
)

func sharedClosureRows(gate int64, n int) []map[string]any {
	rows := make([]map[string]any, n)
	for i := range rows {
		rows[i] = map[string]any{
			"gate": gate,
			"ip":   sharedClosureIPv4,
			"mac":  sharedClosureMAC,
			"ip6":  sharedClosureIPv6,
			"uu":   sharedClosureUUID,
			"s":    sharedClosureStr,
		}
	}
	return rows
}

// runSharedClosurePipeline runs ops behind a leading gate filter over 100
// parallel workers and returns the number of surviving rows.
func runSharedClosurePipeline(t *testing.T, ops []UnaryOperator) int {
	t.Helper()
	const rowsPerBatch = 64
	const dataBatches = 256

	schema := sharedClosureSchema()
	src := &sharedClosureSource{
		schema: schema,
		// gate = 0: the leading conjunct rejects every warm-up row, so the
		// predicates under test are NOT resolved single-threaded.
		warmup:  sharedClosureRows(0, rowsPerBatch),
		data:    sharedClosureRows(1, rowsPerBatch),
		batches: dataBatches + 1,
	}

	gate := NewFilter(ColumnCompare("gate", OpEq, int64(1)))
	chain := append([]UnaryOperator{gate}, ops...)

	sink := &CollectSink{}
	pipe := &Pipeline{Source: src, Ops: chain, Sink: sink, Workers: 100}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatalf("pipeline: %v", err)
	}
	return len(sink.ToRows())
}

// TestColumnCompareSharedClosureParallelWorkers is the regression test for the
// unsynchronized state ColumnCompare's predicate closure used to carry.
//
// Filter.Clone shares one Pred across every parallel worker by design, so the
// closure must be immutable once built. It was not: `cachedIdx`, and the
// `netResolved`/`cachedNetInt`/`cachedNetStr` literal memo, were written by
// whichever worker got there first and read by all the others. Before the fix
// this fails under -race at filter.go's closure; without -race it can also
// answer the wrong row count, because a torn cachedNetStr compares against
// garbage.
//
// The name carries "Parallel" so CI's -race arm selects it.
func TestColumnCompareSharedClosureParallelWorkers(t *testing.T) {
	ops := []UnaryOperator{
		NewFilter(ColumnCompare("ip", OpEq, sharedClosureIPv4)),
		NewFilter(ColumnCompare("mac", OpEq, sharedClosureMAC)),
		NewFilter(ColumnCompare("ip6", OpEq, sharedClosureIPv6)),
		NewFilter(ColumnCompare("uu", OpEq, sharedClosureUUID)),
	}
	got := runSharedClosurePipeline(t, ops)
	if want := 256 * 64; got != want {
		t.Fatalf("surviving rows: got %d, want %d — a shared predicate closure "+
			"resolved its column index or its literal under a race", got, want)
	}
}

// TestColumnLikeSharedClosureParallelWorkers is the same test for ColumnLike,
// whose closure carried the identical captured `cachedIdx`.
func TestColumnLikeSharedClosureParallelWorkers(t *testing.T) {
	ops := []UnaryOperator{
		NewFilter(ColumnLike("s", "wadjet%", false)),
		NewFilter(ColumnLike("s", "%zzz%", true)),
	}
	got := runSharedClosurePipeline(t, ops)
	if want := 256 * 64; got != want {
		t.Fatalf("surviving rows: got %d, want %d", got, want)
	}
}

// TestColumnRefSharedClosureParallelWorkers covers the third closure with the
// same shape: Project.Clone copies the ProjectColumn structs but SHARES the
// Expression, so ColumnRef's index cache is read and written by every worker.
//
// This one drives the closure directly rather than through a Pipeline: which
// of Project's several evaluation routes reaches Expr depends on the output
// type and the selection vector, and the contract under test belongs to the
// closure, not to the route.
func TestColumnRefSharedClosureParallelWorkers(t *testing.T) {
	schema := sharedClosureSchema()
	expr := ColumnRef("s")

	const workers = 100
	var wg sync.WaitGroup
	errs := make([]string, workers)
	start := make(chan struct{})
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			b := batch.FromRows(schema, sharedClosureRows(1, 8))
			<-start
			for row := 0; row < b.Len; row++ {
				if got, _ := expr(b, row).(string); got != sharedClosureStr {
					errs[w] = got
					return
				}
			}
		}(w)
	}
	close(start)
	wg.Wait()
	for w, e := range errs {
		if e != "" {
			t.Fatalf("worker %d read %q, want %q", w, e, sharedClosureStr)
		}
	}
}
