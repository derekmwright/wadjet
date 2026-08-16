package worker

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

var viewTestSchema = []parquet.Column{
	{Name: "k", Type: parquet.TypeInt64, Nullable: true},
	{Name: "name", Type: parquet.TypeString, Nullable: true},
	{Name: "price", Type: parquet.TypeFloat64, Nullable: true},
}

// viewTestBatches builds an owned base batch and a view batch over it with
// permuted + duplicated indices (the join-output shape). The price column
// carries an own-null override (outer-join fill) to exercise the flatten
// fallback; k and name are pure views.
func viewTestBatches(tb testing.TB) (base, viewBatch *batch.RecordBatch) {
	tb.Helper()
	base = batch.NewRecordBatch(viewTestSchema, 4)
	for i := range 4 {
		base.Columns[0].Int64Data[i] = int64((i + 1) * 100)
		base.Columns[1].BytesData.Set(i, fmt.Appendf(nil, "row%d", i))
		base.Columns[2].Float64Data[i] = float64(i) + 0.5
	}
	base.Columns[1].Nulls.SetNull(2) // base null rides through views

	indices := []uint32{3, 0, 0, 2, 1, 3}
	kv := batch.NewViewVector(base.Columns[0], indices)
	nv := batch.NewViewVector(base.Columns[1], indices)
	pv := batch.NewViewVector(base.Columns[2], indices)
	pv.Nulls.SetNull(1) // own-null override on one row
	pv.Nulls.SetNull(4)
	viewBatch = &batch.RecordBatch{
		Columns: []*batch.Vector{kv, nv, pv},
		Schema:  viewTestSchema,
		Len:     len(indices),
	}
	return base, viewBatch
}

// eagerClone materializes an independent owned copy of a (possibly view)
// batch for the control arm.
func eagerClone(tb testing.TB, b *batch.RecordBatch) *batch.RecordBatch {
	tb.Helper()
	out := batch.NewRecordBatch(b.Schema, b.Len)
	for ci := range b.Columns {
		for i := 0; i < b.Len; i++ {
			out.Columns[ci].CopyValueFrom(i, b.Columns[ci], i)
		}
	}
	return out
}

func TestShuffleWriteChunkViewColumns(t *testing.T) {
	_, viewBatch := viewTestBatches(t)
	control := eagerClone(t, viewBatch)

	writeArm := func(b *batch.RecordBatch) []*batch.RecordBatch {
		var buf bytes.Buffer
		sw := newShuffleWriter(&buf, viewTestSchema)
		if err := sw.writeHeader(); err != nil {
			t.Fatal(err)
		}
		// One chunk with a selection (incl. a duplicated view row), one dense.
		if err := sw.writeChunk(b.Columns, []uint32{5, 1, 3}, 3); err != nil {
			t.Fatal(err)
		}
		if err := sw.writeChunk(b.Columns, nil, b.Len); err != nil {
			t.Fatal(err)
		}
		data := buf.Bytes()
		data[4] = byte(sw.numChunks)
		batches, err := shuffleReadBatches(data)
		if err != nil {
			t.Fatal(err)
		}
		return batches
	}

	serializedBefore := exec.LateMatViewColumnsSerialized.Load()
	got := writeArm(viewBatch)
	if exec.LateMatViewColumnsSerialized.Load() == serializedBefore {
		t.Fatal("no view column serialized through its indirection")
	}
	// The own-null price column had to flatten (its null bits override the
	// base's); the pure view columns must NOT have been flattened.
	if viewBatch.Columns[2].IsView() {
		t.Fatal("own-null view column was not flattened by writeChunk")
	}
	if !viewBatch.Columns[0].IsView() || !viewBatch.Columns[1].IsView() {
		t.Fatal("pure view column was flattened — indirect serialize did not engage")
	}

	want := writeArm(control)
	if len(got) != len(want) {
		t.Fatalf("chunk count: got %d want %d", len(got), len(want))
	}
	for i := range want {
		w, g := want[i].ToRows(), got[i].ToRows()
		if !reflect.DeepEqual(w, g) {
			t.Fatalf("chunk %d differs:\nwant %v\ngot  %v", i, w, g)
		}
	}
}

func TestPartitionedShuffleSinkViewBatch(t *testing.T) {
	runArm := func(b *batch.RecordBatch) map[int][]map[string]any {
		dir := t.TempDir()
		s := newPartitionedShuffleSink(dir, []string{"k"}, 3, viewTestSchema)
		if err := s.Init(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := s.Consume(context.Background(), b); err != nil {
			t.Fatal(err)
		}
		if err := s.Finalize(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		out := make(map[int][]map[string]any)
		for p := range 3 {
			data, err := os.ReadFile(filepath.Join(dir, fmt.Sprintf("part-%04d.wshf", p)))
			if err != nil {
				t.Fatal(err)
			}
			if len(data) == 0 {
				continue
			}
			batches, err := shuffleReadBatches(data)
			if err != nil {
				t.Fatalf("partition %d: %v", p, err)
			}
			for _, ob := range batches {
				out[p] = append(out[p], ob.ToRows()...)
			}
		}
		return out
	}

	_, viewBatch := viewTestBatches(t)
	control := eagerClone(t, viewBatch)

	serializedBefore := exec.LateMatViewColumnsSerialized.Load()
	got := runArm(viewBatch)
	if exec.LateMatViewColumnsSerialized.Load() == serializedBefore {
		t.Fatal("partitioned sink never serialized a view column through translation")
	}
	// The key column must have been flattened (hash pass reads it
	// positionally); the name column must have stayed a view.
	if viewBatch.Columns[0].IsView() {
		t.Fatal("key view column was not flattened before hashing")
	}
	if !viewBatch.Columns[1].IsView() {
		t.Fatal("non-key pure view column was flattened — translation did not engage")
	}

	want := runArm(control)
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("partition contents differ:\nwant %v\ngot  %v", want, got)
	}
}

func TestUnpartitionedStageSinkViewBatch(t *testing.T) {
	runArm := func(b *batch.RecordBatch) []map[string]any {
		dir := t.TempDir()
		s := newUnpartitionedStageSink(dir, "viewtest")
		if err := s.Init(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := s.Consume(context.Background(), b); err != nil {
			t.Fatal(err)
		}
		if err := s.Finalize(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		data, err := os.ReadFile(filepath.Join(dir, "stage-viewtest.wshf"))
		if err != nil {
			t.Fatal(err)
		}
		batches, err := shuffleReadBatches(data)
		if err != nil {
			t.Fatal(err)
		}
		var rows []map[string]any
		for _, ob := range batches {
			rows = append(rows, ob.ToRows()...)
		}
		return rows
	}

	_, viewBatch := viewTestBatches(t)
	control := eagerClone(t, viewBatch)
	got := runArm(viewBatch)
	want := runArm(control)
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("stage file contents differ:\nwant %v\ngot  %v", want, got)
	}
}
