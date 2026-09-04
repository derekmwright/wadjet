package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestBuildFromRowsIndexesATwoIntegerKey is #498's regression test.
//
// tryEnableIntKey has THREE outcomes: a single integer key sets useIntKey, a
// two-integer key sets useDualIntKey (and nils strIndex), and everything else
// leaves both false. BuildFromRows tested only useIntKey, so a two-integer-key
// build took the `else` branch and populated strIndex — while lookupBuild and
// existsInBuild test useDualIntKey FIRST and probe the intIndex nothing had
// filled. Every probe missed: the join returned zero rows, and its anti twin
// returned every row.
//
// BuildFromRows is the entry point the worker uses, so this was reachable from
// the distributed path rather than test-only — which is why the assertion is
// on the ANSWER and not just on which index got populated.
func TestBuildFromRowsIndexesATwoIntegerKey(t *testing.T) {
	buildSchema := []parquet.Column{
		{Name: "k1", Type: parquet.TypeInt64},
		{Name: "k2", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString},
	}
	buildRows := []map[string]any{
		{"k1": int64(1), "k2": int64(10), "name": "a"},
		{"k1": int64(2), "k2": int64(20), "name": "b"},
		{"k1": int64(3), "k2": int64(30), "name": "c"},
	}

	hj := NewHashJoin(InnerJoin, []string{"k1", "k2"}, []string{"k1", "k2"})
	hj.BuildFromRows(buildSchema, buildRows)

	if !hj.useDualIntKey {
		t.Fatal("two integer key columns did not enable the dual-int key path")
	}
	if hj.parts[0].ints == nil || hj.parts[0].ints.Len() == 0 {
		t.Fatalf("dual-int build populated no intIndex (len=%v) — the probe reads this one",
			hj.parts[0].ints)
	}
	if hj.BuildRows() != 3 {
		t.Fatalf("BuildRows() = %d, want 3", hj.BuildRows())
	}

	probeSchema := []parquet.Column{
		{Name: "k1", Type: parquet.TypeInt64},
		{Name: "k2", Type: parquet.TypeInt64},
		{Name: "amount", Type: parquet.TypeInt64},
	}
	probeRows := []map[string]any{
		{"k1": int64(1), "k2": int64(10), "amount": int64(100)}, // matches "a"
		{"k1": int64(2), "k2": int64(20), "amount": int64(200)}, // matches "b"
		{"k1": int64(2), "k2": int64(99), "amount": int64(300)}, // second key differs
		{"k1": int64(9), "k2": int64(90), "amount": int64(400)}, // no match
	}

	sink := &CollectSink{}
	pipe := &Pipeline{
		Source: NewSliceSource(probeSchema, probeRows),
		Ops:    []UnaryOperator{hj.Probe()},
		Sink:   sink,
	}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatalf("pipeline: %v", err)
	}
	if got := len(sink.Rows); got != 2 {
		t.Fatalf("two-integer-key inner join emitted %d rows, want 2 (both keys must match)", got)
	}
}

// The anti twin: the same miss made a NOT-EXISTS-shaped anti join keep every
// probe row instead of the one that genuinely has no partner.
func TestBuildFromRowsTwoIntegerKeyAntiJoin(t *testing.T) {
	buildSchema := []parquet.Column{
		{Name: "k1", Type: parquet.TypeInt64},
		{Name: "k2", Type: parquet.TypeInt64},
	}
	buildRows := []map[string]any{
		{"k1": int64(1), "k2": int64(10)},
		{"k1": int64(2), "k2": int64(20)},
	}
	hj := NewHashJoin(AntiJoin, []string{"k1", "k2"}, []string{"k1", "k2"})
	hj.BuildFromRows(buildSchema, buildRows)

	probeRows := []map[string]any{
		{"k1": int64(1), "k2": int64(10)}, // matched → dropped
		{"k1": int64(2), "k2": int64(21)}, // second key differs → kept
		{"k1": int64(7), "k2": int64(70)}, // no match → kept
	}
	sink := &CollectSink{}
	pipe := &Pipeline{
		Source: NewSliceSource(buildSchema, probeRows),
		Ops:    []UnaryOperator{hj.Probe()},
		Sink:   sink,
	}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatalf("pipeline: %v", err)
	}
	if got := len(sink.Rows); got != 2 {
		t.Fatalf("two-integer-key anti join emitted %d rows, want 2", got)
	}
}
