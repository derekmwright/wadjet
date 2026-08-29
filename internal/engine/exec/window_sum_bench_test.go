package exec

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The windowed SUM/AVG hot loop, over the two accumulators it can take.
//
// #586 gave a DECIMAL input an exact Int128 accumulator. The FLOAT64 arm is
// the one that must not pay for it: the dispatch between the two is per
// PARTITION, and the float slide reads its cells through a type resolved once
// per slide rather than through vecFloat64's two per-row type switches
// (numericPromotable + numericFloat64), so the float arm should be no slower
// than it was — the sliding shapes measurably faster, since they are the ones
// that call the slide most.
//
// The DECIMAL arms have no "before" to compare against: they did not exist.
// They are here as the standing cost of an exact accumulator against the
// float one, over the same row count and the same frames.

// wsbRows builds n rows of one numeric column plus a partition key and a
// unique order key.
func wsbRows(n, groups int, dec bool) ([]parquet.Column, []map[string]any) {
	val := parquet.Column{Name: "v", Type: parquet.TypeFloat64, Nullable: true}
	if dec {
		val = parquet.Column{Name: "v", Type: parquet.TypeDecimal, Precision: 38, Scale: 10, Nullable: true}
	}
	schema := []parquet.Column{
		{Name: "g", Type: parquet.TypeInt64},
		{Name: "ts", Type: parquet.TypeInt64},
		val,
	}
	rows := make([]map[string]any, n)
	for i := range rows {
		r := map[string]any{"g": int64(i % groups), "ts": int64(i)}
		switch {
		case i%17 == 16:
			r["v"] = nil // NULLs, so the null branch is on the measured path
		case dec:
			r["v"] = batch.Int128From(int64(i+1) * 12345678901).FormatDecimal(10)
		default:
			r["v"] = float64(i) * 1.25
		}
		rows[i] = r
	}
	return schema, rows
}

func BenchmarkWindowSum(b *testing.B) {
	const (
		rows   = 32768
		groups = 16
	)
	for _, dec := range []bool{false, true} {
		typ := "float64"
		if dec {
			typ = "decimal"
		}
		schema, data := wsbRows(rows, groups, dec)
		batches := make([]*batch.RecordBatch, 0, (rows+batch.DefaultBatchSize-1)/batch.DefaultBatchSize)
		for pos := 0; pos < rows; pos += batch.DefaultBatchSize {
			end := min(pos+batch.DefaultBatchSize, rows)
			batches = append(batches, batch.FromRows(schema, data[pos:end]))
		}
		for _, shape := range []struct {
			name  string
			order bool
			frame *WindowFrameSpec
		}{
			// The WHOLE partition: no ORDER BY, so the default frame widens
			// to the partition and every row of it takes one answer.
			{name: "whole_partition"},
			// The RUNNING total: the default frame under a unique ORDER BY,
			// so the upper end advances one row at a time and the lower end
			// never moves — the shape this branch computed before frames
			// existed.
			{name: "running", order: true},
			// A SLIDING frame: every row past the first also RETRACTS one,
			// which is the subtraction #586 had to make exact.
			{name: "sliding_8", order: true, frame: &WindowFrameSpec{Mode: "rows",
				Start: WindowBound{Type: "preceding", Offset: 8},
				End:   WindowBound{Type: "current_row"}}},
		} {
			for _, fn := range []struct {
				name string
				f    WindowFunc
			}{{"sum", WinSum}, {"avg", WinAvg}} {
				b.Run(fmt.Sprintf("%s/%s/%s", typ, shape.name, fn.name), func(b *testing.B) {
					wc := WindowColumn{
						Func: fn.f, InputCol: "v", OutputCol: "w",
						OutputType:  parquet.TypeFloat64,
						PartitionBy: []string{"g"},
						Frame:       shape.frame,
					}
					if shape.order {
						wc.OrderBy = []SortKey{{Column: "ts", Order: Ascending}}
					}
					cols := []WindowColumn{wc}
					ctx := context.Background()
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						w := NewWindow(cols)
						if err := w.Init(ctx); err != nil {
							b.Fatal(err)
						}
						// The input batches are REUSED across iterations:
						// the window copies them into its own combined
						// batch (windowConcatBatches) and never writes
						// back, so rebuilding them per iteration would
						// measure batch.FromRows instead of the window.
						for _, src := range batches {
							if err := w.Consume(ctx, src); err != nil {
								b.Fatal(err)
							}
						}
						if err := w.Finalize(ctx); err != nil {
							b.Fatal(err)
						}
						for {
							out, err := w.Next(ctx)
							if err != nil {
								b.Fatal(err)
							}
							if out == nil {
								break
							}
						}
						w.Close()
					}
				})
			}
		}
	}
}
