package exec

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// BenchmarkProbeEmitViewOutput measures the allocation the hash-join
// late-materialization emit path charges to the Go heap per output batch, at
// TPC-H-ish widths: an int64 key, a ~25-byte string, a float64 and a date on
// the build side; key plus two carried columns on the probe side.
//
// The multiBuild arms are the ones that matter for the 2026-08-22 SF100
// finding — with more than one build batch the build columns are gathered
// eagerly instead of viewed, and that gather (NewColumnVector +
// BytesColumn.PreAllocBytes) is 208 GB per suite run of large-object
// allocation behind 19.4% of all worker mutex delay.
//
// The reuse arm is the ambient WADJET_VECTOR_REUSE setting, so before/after
// is one benchstat over the same binary:
//
//	WADJET_VECTOR_REUSE=0 go test -bench=ProbeEmit -benchmem -count=6 ./internal/engine/exec > before.txt
//	go test -bench=ProbeEmit -benchmem -count=6 ./internal/engine/exec > after.txt
//	benchstat before.txt after.txt
func BenchmarkProbeEmitViewOutput(b *testing.B) {
	for _, multiBuild := range []bool{false, true} {
		b.Run(fmt.Sprintf("multiBuild=%v", multiBuild), func(b *testing.B) {
			benchProbeEmit(b, multiBuild)
		})
	}
}

func benchProbeEmit(b *testing.B, multiBuild bool) {
	b.Helper()
	ctx := context.Background()
	buildSchema := []parquet.Column{
		{Name: "o_orderkey", Type: parquet.TypeInt64},
		{Name: "o_comment", Type: parquet.TypeString},
		{Name: "o_total", Type: parquet.TypeFloat64},
		{Name: "o_date", Type: parquet.TypeDate},
	}
	probeSchema := []parquet.Column{
		{Name: "l_orderkey", Type: parquet.TypeInt64},
		{Name: "l_price", Type: parquet.TypeFloat64},
		{Name: "l_ship", Type: parquet.TypeString},
	}

	const buildRows = 20000
	brows := make([]map[string]any, buildRows)
	for i := range brows {
		brows[i] = map[string]any{
			"o_orderkey": int64(i),
			"o_comment":  fmt.Sprintf("order comment %012d", i), // 27 bytes
			"o_total":    float64(i) * 3.25,
			"o_date":     int32(9000 + i%3000),
		}
	}
	hj := NewHashJoin(InnerJoin, []string{"l_orderkey"}, []string{"o_orderkey"})
	if multiBuild {
		tr := memory.NewTracker("emit-bench", 1<<30)
		tr.ForceReserve(1 << 29) // >30% used ⇒ consolidateBuild is skipped
		hj.MemTracker = tr
	}
	if err := hj.Build(ctx, NewSliceSource(buildSchema, brows)); err != nil {
		b.Fatal(err)
	}
	hj.FixKeyAssignment()
	if multiBuild && len(hj.buildBatches) < 2 {
		b.Fatalf("wanted a multi-batch build, got %d", len(hj.buildBatches))
	}

	probe := hj.Probe()
	probe.LateMaterialize = true
	if err := probe.Init(ctx); err != nil {
		b.Fatal(err)
	}

	prows := make([]map[string]any, batch.DefaultBatchSize)
	for i := range prows {
		prows[i] = map[string]any{
			"l_orderkey": int64(i * 7 % buildRows),
			"l_price":    float64(i) * 1.5,
			"l_ship":     fmt.Sprintf("ship-%06d", i),
		}
	}
	in := batch.FromRows(probeSchema, prows)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := probe.Execute(ctx, in)
		if err != nil {
			b.Fatal(err)
		}
		if out == nil {
			b.Fatal("no output")
		}
		// A copying consumer: reads every column and keeps nothing, which is
		// the stage/shuffle sink shape and what makes the storage reusable.
		consumeCopying(out)
	}
}

// consumeCopying touches every output column the way a serializing sink does
// — through the view indirection, without boxing and without retaining the
// batch — so what the benchmark measures is the emit path's own allocation.
func consumeCopying(b *batch.RecordBatch) {
	var sink int64
	for _, c := range b.Columns {
		for i := 0; i < b.Len; i++ {
			src, row := c, i
			if c.Base != nil {
				src, row = c.Base, int(c.Indices[i])
			}
			switch src.Type {
			case batch.TypeInt64:
				sink += src.Int64Data[row]
			case batch.TypeDate, batch.TypeInt32:
				sink += int64(src.Int32Data[row])
			case batch.TypeFloat64:
				sink += int64(src.Float64Data[row])
			case batch.TypeString, batch.TypeBytes:
				sink += int64(src.BytesData.Offsets[row+1] - src.BytesData.Offsets[row])
			}
		}
	}
	benchEmitSink = sink
}

var benchEmitSink int64
