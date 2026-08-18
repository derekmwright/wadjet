package physical

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestPipelineSourceBoundsProbeFanOut is the pipelineSource half of #317.
// pipelineSource drives an operator chain as a Source — the build side of a
// join, and the probe pipeline behind joinFlushSource — so a probe running
// under it used to materialise a whole input batch's fan-out in one live
// allocation, which GOMEMLIMIT cannot reclaim because it is not garbage.
//
// The shape below is the reported one at unit scale: every probe row matches
// every build row, so one 2048-row probe batch fans out to 2,048,000 rows.
func TestPipelineSourceBoundsProbeFanOut(t *testing.T) {
	const buildN = 1000
	const probeN = 2 * batch.DefaultBatchSize

	buildSchema := []parquet.Column{
		{Name: "rk", Type: parquet.TypeInt64},
		{Name: "rv", Type: parquet.TypeInt64},
	}
	probeSchema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "amount", Type: parquet.TypeInt64},
	}
	toBatches := func(schema []parquet.Column, rows []map[string]any) []*batch.RecordBatch {
		var out []*batch.RecordBatch
		for i := 0; i < len(rows); i += batch.DefaultBatchSize {
			end := i + batch.DefaultBatchSize
			if end > len(rows) {
				end = len(rows)
			}
			out = append(out, batch.FromRows(schema, rows[i:end]))
		}
		return out
	}

	buildRows := make([]map[string]any, buildN)
	for i := range buildRows {
		buildRows[i] = map[string]any{"rk": int64(7), "rv": int64(i)}
	}
	probeRows := make([]map[string]any, probeN)
	for i := range probeRows {
		probeRows[i] = map[string]any{"k": int64(7), "amount": int64(i)}
	}

	ctx := context.Background()
	hj := exec.NewHashJoin(exec.InnerJoin, []string{"k"}, []string{"rk"})
	if err := hj.Build(ctx, exec.NewBatchSource(toBatches(buildSchema, buildRows))); err != nil {
		t.Fatalf("build: %v", err)
	}

	ps := &pipelineSource{
		source: exec.NewBatchSource(toBatches(probeSchema, probeRows)),
		ops:    []exec.UnaryOperator{hj.Probe()},
	}
	if err := ps.Init(ctx); err != nil {
		t.Fatalf("init: %v", err)
	}

	var rows, batches, maxRows int
	var sum int64
	for {
		b, err := ps.Next(ctx)
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if b == nil {
			break
		}
		exec.FlattenForConsumer(b, nil)
		n := b.ActiveLen()
		rows += n
		batches++
		if n > maxRows {
			maxRows = n
		}
		ai := -1
		for i, c := range b.Schema {
			if c.Name == "amount" {
				ai = i
			}
		}
		if ai < 0 {
			t.Fatalf("output schema missing amount: %+v", b.Schema)
		}
		for i := 0; i < n; i++ {
			row := i
			if b.Sel != nil {
				row = int(b.Sel[i])
			}
			sum += b.Columns[ai].Int64Data[row]
		}
	}

	if want := buildN * probeN; rows != want {
		t.Fatalf("got %d joined rows, want %d (rows lost or duplicated across a suspension)", rows, want)
	}
	var wantSum int64
	for i := 0; i < probeN; i++ {
		wantSum += int64(i) * int64(buildN)
	}
	if sum != wantSum {
		t.Fatalf("checksum %d, want %d (rows joined to the wrong partners)", sum, wantSum)
	}
	if maxRows > exec.MaxProbeOutputRows {
		t.Fatalf("pipelineSource emitted a %d-row batch; the per-call bound is %d rows "+
			"(the fan-out of a whole input batch was materialised at once)",
			maxRows, exec.MaxProbeOutputRows)
	}
	if batches < 2 {
		t.Fatalf("expected the fan-out to arrive as multiple bounded batches, got %d", batches)
	}
}
