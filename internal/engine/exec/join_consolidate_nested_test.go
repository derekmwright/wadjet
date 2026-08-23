package exec

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestConsolidateBuildKeepsNestedPayloads is the regression for
// consolidateBuild's missing container arms.
//
// consolidateBuild copies every build batch into one contiguous batch and then
// DISCARDS the originals. Its typed switch had no arm for ARRAY, ROW, MAP or
// VECTOR and no default, so for those columns the null bitmap was copied and
// the VALUES were not — every row of such a payload column came back empty
// once the originals were dropped.
//
// It fires only when the build has more than one batch, at most 2M rows, and
// under 30% tracker use, so the same join answered correctly or emptily
// depending on how much memory the query had.
func TestConsolidateBuildKeepsNestedPayloads(t *testing.T) {
	ctx := context.Background()
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "c_arr", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
		{Name: "c_row", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
			{Name: "a", Type: parquet.TypeString, Nullable: true},
			{Name: "b", Type: parquet.TypeInt64, Nullable: true},
		}},
		{Name: "c_vec", Type: parquet.TypeVector, Nullable: true, Dimension: 2},
	}

	const batches, perBatch = 4, 5
	want := map[int64]map[string]any{}
	src := &testBatchSource{}
	for bn := 0; bn < batches; bn++ {
		rows := make([]map[string]any, 0, perBatch)
		for i := 0; i < perBatch; i++ {
			id := int64(bn*perBatch + i)
			row := map[string]any{"id": id}
			if id%5 == 4 {
				// A NULL container on every fifth row: the offsets still have
				// to advance, or every later row shifts.
				row["c_arr"], row["c_row"], row["c_vec"] = nil, nil, nil
			} else {
				row["c_arr"] = []any{fmt.Sprintf("a-%03d", id), "tail"}
				row["c_row"] = map[string]any{"a": fmt.Sprintf("r-%03d", id), "b": id * 11}
				row["c_vec"] = []float32{float32(id), float32(id) + 0.5}
			}
			want[id] = row
			rows = append(rows, row)
		}
		src.batches = append(src.batches, batch.FromRows(schema, rows))
	}

	hj := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
	if err := hj.Build(ctx, src); err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(hj.buildBatches) != 1 {
		t.Fatalf("build did not consolidate (%d batches) — the defect is unreachable, "+
			"so this test would prove nothing", len(hj.buildBatches))
	}

	probe := hj.Probe()
	if err := probe.Init(ctx); err != nil {
		t.Fatalf("Probe.Init: %v", err)
	}
	pb := batch.NewRecordBatch([]parquet.Column{{Name: "id", Type: parquet.TypeInt64}}, batches*perBatch)
	for i := 0; i < batches*perBatch; i++ {
		pb.Columns[0].Int64Data[i] = int64(i)
	}
	pb.Len = batches * perBatch
	out, err := probe.Execute(ctx, pb)
	if err != nil {
		t.Fatalf("Probe.Execute: %v", err)
	}
	if out == nil {
		t.Fatal("the join matched nothing")
	}

	seen := 0
	for _, row := range out.ToRows() {
		id, ok := row["id"].(int64)
		if !ok {
			t.Fatalf("row has no id: %v", row)
		}
		exp := want[id]
		seen++
		for _, col := range []string{"c_arr", "c_row", "c_vec"} {
			got := fmt.Sprintf("%v", row[col])
			expect := fmt.Sprintf("%v", exp[col])
			if got != expect {
				t.Errorf("id %d column %s = %s, want %s — consolidateBuild dropped the payload",
					id, col, got, expect)
			}
		}
	}
	if seen != batches*perBatch {
		t.Errorf("matched %d rows, want %d", seen, batches*perBatch)
	}
}
