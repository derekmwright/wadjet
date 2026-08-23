package exec

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestParallelEmitPanicBecomesQueryError is the #400 regression for the
// parallel aggregate emitter.
//
// emitDrain.produce runs on a goroutine the caller does not own, so
// Pipeline.Run's recover (pipeline.go:82) cannot see a panic raised there and
// the process DIES — in a server that is every connected client's query, not
// just the offending one. The panic this reproduces is the #361 silent-write
// guard, whose entire design is "an error, never the server".
//
// The original repro forced this via MIN_BY's declared output type (#392's
// shape, which killed the process on 16 corpus entries). #392 is fixed now
// at the schema level too: HashAggregate.OutputSchema derives MIN_BY/MAX_BY's
// output type from the value's own observed type (aggregate.go, the
// AggMinBy/AggMaxBy case), so a wrong declaration for that function can no
// longer reach SetValue. MIN/MAX get the same correction for every type
// minMaxOutputType answers ok for — since #417 that is all eighteen flat
// types — so the stand-in has to be a type it still DECLINES. DECIMAL is
// one: its accumulator finalizes MinDec through ToFloat64, so the output is
// already a float64 and the planner's declaration is left standing.
// Declaring it as something SetValue does not accept a float64 into (IPv6,
// whose arm takes only string/[]byte) still hits the #361 guard on the
// parallel-emit goroutine, so it stands in for "some future declaration bug
// reaches this write" — which is what this test is actually about,
// independent of which specific bug last supplied the reproduction. It was
// DURATION until #417 put DURATION on the corrected list.
//
// Before the fix this test does not fail — it takes the test binary down.
func TestParallelEmitPanicBecomesQueryError(t *testing.T) {
	ctx := context.Background()
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64, Nullable: true},
		{Name: "s", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true},
	}
	aggs := []AggColumn{
		{Func: AggMin, InputCol: "s", OutputCol: "mb", OutputType: parquet.TypeIPv6},
	}

	const nUnits = 4
	prim := NewHashAggregate([]string{"k"}, aggs)
	prim.PartitionedDisjoint = true
	if err := prim.Init(ctx); err != nil {
		t.Fatal(err)
	}
	units := []*HashAggregate{prim}
	for i := 1; i < nUnits; i++ {
		c := prim.CloneSink().(*HashAggregate)
		c.PartitionedDisjoint = true
		if err := c.Init(ctx); err != nil {
			t.Fatal(err)
		}
		units = append(units, c)
	}
	for g := 0; g < 64; g++ {
		u := g % nUnits
		rows := []map[string]any{{"k": int64(g), "s": int64(g)}}
		if err := units[u].Consume(ctx, batch.FromRows(schema, rows)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 1; i < nUnits; i++ {
		prim.MergeSink(units[i])
	}
	if err := prim.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	defer prim.Close()

	if !prim.parallelEmitEligible() {
		t.Skip("this shape did not take the parallel-emit path; the drain is what is under test")
	}

	var err error
	for {
		var b *batch.RecordBatch
		b, err = prim.Next(ctx)
		if err != nil || b == nil {
			break
		}
		b.Release()
	}
	if err == nil {
		t.Fatal("the emission answered without error; want the #361 type mismatch reported as a query error")
	}
	if !strings.Contains(err.Error(), "draining aggregate partition") {
		t.Fatalf("error %q does not come from the drain's recover — the panic escaped somewhere else", err)
	}
}
