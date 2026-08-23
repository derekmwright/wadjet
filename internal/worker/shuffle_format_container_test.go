package worker

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/wshf"
)

// #397: ARRAY, ROW, MAP and VECTOR columns could not cross a shuffle at all
// — writeColumnData had arms for the 18 flat types and errored on these
// four, so every distributed plan that had to move one failed while the
// single-process engine answered the same query.
//
// The fixture below is the part that matters: it carries a NULL container
// and an EMPTY container in the same column (they encode identically in the
// offsets and are told apart ONLY by the column's null bit), a nested NULL
// leaf inside a non-null container, and a zero-length string element next
// to a null one. Assertions are on VALUES via GetValue, not row counts.

const containerRows = 5

// containerSchema is the four container types plus an id, with the full
// nested shape (ARRAY element type, ROW fields, MAP entry ROW, VECTOR
// dimension) the WSHF header cannot carry — proving the payload, not the
// schema, is what reconstructs the shape on the far side.
func containerSchema() []parquet.Column {
	return []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
		{Name: "c_arr", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
		{Name: "c_row", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
			{Name: "a", Type: parquet.TypeString, Nullable: true},
			{Name: "b", Type: parquet.TypeInt64, Nullable: true},
		}},
		{Name: "c_map", Type: parquet.TypeMap, Nullable: true,
			ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "key", Type: parquet.TypeString},
				{Name: "value", Type: parquet.TypeInt64, Nullable: true},
			}}},
		{Name: "c_vec", Type: parquet.TypeVector, Nullable: true, Dimension: 3},
	}
}

// containerValues is the expected GetValue form of each column, row by row.
// The writer builds the batch from exactly these, and every reader is
// checked against them.
func containerValues() map[string][]any {
	return map[string][]any{
		"id": {int64(0), int64(1), int64(2), int64(3), int64(4)},
		"c_arr": {
			[]any{"a", "bb"},    // ordinary
			[]any{},             // EMPTY, not null
			nil,                 // NULL container
			[]any{"", "c", nil}, // zero-length element, then a NULL leaf
			[]any{"solo"},
		},
		"c_row": {
			map[string]any{"a": "x", "b": int64(1)},
			map[string]any{"a": nil, "b": int64(2)}, // NULL leaf in a live row
			nil,                                     // NULL container
			map[string]any{"a": "z", "b": nil},
			map[string]any{"a": "", "b": int64(-9)},
		},
		"c_map": {
			[]any{map[string]any{"key": "k1", "value": int64(10)}},
			[]any{}, // EMPTY map, not null
			nil,     // NULL container
			[]any{map[string]any{"key": "k2", "value": nil}}, // NULL value leaf
			[]any{
				map[string]any{"key": "k3", "value": int64(30)},
				map[string]any{"key": "k4", "value": int64(40)},
			},
		},
		"c_vec": {
			[]float32{1, 2, 3},
			[]float32{0, 0, 0}, // all-zero is NOT null
			nil,                // NULL container
			[]float32{-1.5, 2.5, 3.5},
			[]float32{1e-8, -0, 1e8},
		},
	}
}

func buildContainerBatch(tb testing.TB) *batch.RecordBatch {
	tb.Helper()
	schema := containerSchema()
	vals := containerValues()
	b := batch.NewRecordBatch(schema, containerRows)
	for ci, col := range schema {
		column := vals[col.Name]
		for i := 0; i < containerRows; i++ {
			b.Columns[ci].SetValue(i, column[i])
		}
	}
	return b
}

// assertContainerBatch checks every column of every row against the fixture,
// following rows through sel (nil = all rows in order).
func assertContainerBatch(tb testing.TB, got *batch.RecordBatch, sel []uint32, what string) {
	tb.Helper()
	schema := containerSchema()
	vals := containerValues()
	rows := sel
	if rows == nil {
		rows = make([]uint32, containerRows)
		for i := range rows {
			rows[i] = uint32(i)
		}
	}
	if got.Len != len(rows) {
		tb.Fatalf("%s: %d rows, want %d", what, got.Len, len(rows))
	}
	for ci, col := range schema {
		want := vals[col.Name]
		gc := got.Columns[ci]
		if gc.Type != col.Type {
			tb.Errorf("%s: column %s decoded as %v, want %v", what, col.Name, gc.Type, col.Type)
			continue
		}
		if col.Type == parquet.TypeVector && gc.VectorDim != col.Dimension {
			tb.Errorf("%s: column %s dimension %d, want %d", what, col.Name, gc.VectorDim, col.Dimension)
		}
		if col.Type == parquet.TypeRow {
			var names []string
			for _, f := range col.Fields {
				names = append(names, f.Name)
			}
			if !reflect.DeepEqual(gc.FieldNames, names) {
				tb.Errorf("%s: column %s field names %v, want %v", what, col.Name, gc.FieldNames, names)
			}
		}
		for i, src := range rows {
			w := want[src]
			g := gc.GetValue(i)
			if (w == nil) != gc.Nulls.IsNull(i) {
				tb.Errorf("%s: %s row %d (src %d): null=%v, want null=%v",
					what, col.Name, i, src, gc.Nulls.IsNull(i), w == nil)
				continue
			}
			if !reflect.DeepEqual(w, g) {
				tb.Errorf("%s: %s row %d (src %d): got %#v, want %#v", what, col.Name, i, src, g, w)
			}
		}
	}
}

// writeContainerWSHF writes the fixture as WSHF: one chunk under sel, one
// chunk with every row, exactly as the stage sinks do.
func writeContainerWSHF(tb testing.TB, b *batch.RecordBatch, sel []uint32) []byte {
	tb.Helper()
	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, containerSchema())
	if err := sw.writeHeader(); err != nil {
		tb.Fatalf("writeHeader: %v", err)
	}
	if err := sw.writeChunk(b.Columns, sel, len(sel)); err != nil {
		tb.Fatalf("writeChunk (selected): %v", err)
	}
	if err := sw.writeChunk(b.Columns, nil, containerRows); err != nil {
		tb.Fatalf("writeChunk (all rows): %v", err)
	}
	data := buf.Bytes()
	binary.LittleEndian.PutUint32(data[4:], sw.numChunks)
	return data
}

// TestShuffleContainerColumnsRoundTrip is the #397 regression: the four
// container types through the WSHF writer and back out of every worker-side
// reader. The coordinator's independent decoder is covered by
// internal/coordinator.TestShuffleReaderContainerColumns, which decodes
// bytes in this same format.
func TestShuffleContainerColumnsRoundTrip(t *testing.T) {
	b := buildContainerBatch(t)
	// Rows 1..3 put the EMPTY container, the NULL container and the
	// nested-NULL-leaf row inside the selection, so the gather path sees
	// all three.
	sel := []uint32{1, 2, 3}
	data := writeContainerWSHF(t, b, sel)

	t.Run("eager", func(t *testing.T) {
		batches, err := wshf.DecodeBatches(data)
		if err != nil {
			t.Fatalf("shuffleReadBatches: %v", err)
		}
		if len(batches) != 2 {
			t.Fatalf("got %d batches, want 2", len(batches))
		}
		assertContainerBatch(t, batches[0], sel, "eager selected chunk")
		assertContainerBatch(t, batches[1], nil, "eager full chunk")
	})

	t.Run("chunk_reader", func(t *testing.T) {
		r, err := wshf.NewChunkReader(data)
		if err != nil {
			t.Fatalf("wshf.NewChunkReader: %v", err)
		}
		var got []*batch.RecordBatch
		for {
			rb, err := r.Next()
			if err != nil {
				t.Fatalf("Next: %v", err)
			}
			if rb == nil {
				break
			}
			got = append(got, rb)
		}
		if len(got) != 2 {
			t.Fatalf("got %d chunks, want 2", len(got))
		}
		assertContainerBatch(t, got[0], sel, "chunk reader selected chunk")
		assertContainerBatch(t, got[1], nil, "chunk reader full chunk")
	})

	t.Run("streaming", func(t *testing.T) {
		// The streaming walk skips each column by its length prefix without
		// decoding it, so a container payload it cannot size would desync
		// the whole chunk rather than fail on the column.
		got := drain(t, openStreaming(t, data).Next)
		if len(got) != 2 {
			t.Fatalf("got %d chunks, want 2", len(got))
		}
		assertContainerBatch(t, got[0], sel, "streaming selected chunk")
		assertContainerBatch(t, got[1], nil, "streaming full chunk")
	})
}

// TestShuffleContainerNullVsEmptyAreDistinct states the invariant the
// fixture exists for on its own: a NULL ARRAY and a zero-length ARRAY have
// identical offsets, so only the column bitmap separates them, and an
// encoder that dropped the bitmap or a decoder that read the offsets alone
// would collapse the two into one value.
func TestShuffleContainerNullVsEmptyAreDistinct(t *testing.T) {
	b := buildContainerBatch(t)
	data := writeContainerWSHF(t, b, []uint32{1, 2})
	batches, err := wshf.DecodeBatches(data)
	if err != nil {
		t.Fatalf("shuffleReadBatches: %v", err)
	}
	arr := batches[0].Columns[1] // c_arr, rows [empty, null]
	if arr.Nulls.IsNull(0) {
		t.Error("the EMPTY array decoded as NULL")
	}
	if v := arr.GetValue(0); !reflect.DeepEqual(v, []any{}) {
		t.Errorf("empty array decoded as %#v, want an empty slice", v)
	}
	if !arr.Nulls.IsNull(1) {
		t.Error("the NULL array decoded as non-null")
	}
	if v := arr.GetValue(1); v != nil {
		t.Errorf("null array decoded as %#v, want nil", v)
	}
	if got, want := arr.Offsets[0], arr.Offsets[1]; got != want {
		t.Errorf("empty array spans elements %d..%d", got, want)
	}
}

// TestPartitionedSinkContainerColumns covers the second hole #397 names:
// appendBatchRowsBulk/growBatchTo had no container arm AND no default, so
// the partitioned sink advanced Len over storage it never appended to and
// the failure surfaced later, at the writer. Rows go in through the sink's
// real accumulator path and come back out of the partition files.
func TestPartitionedSinkContainerColumns(t *testing.T) {
	const numParts = 2
	schema := containerSchema()
	sink := newPartitionedShuffleSink(t.TempDir(), []string{"id"}, numParts, schema)
	if err := sink.Init(context.Background()); err != nil {
		t.Fatalf("init: %v", err)
	}
	b := buildContainerBatch(t)
	if err := sink.Consume(context.Background(), b); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if err := sink.Finalize(context.Background()); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	t.Cleanup(func() { sink.Close() })

	vals := containerValues()
	seen := map[int64]bool{}
	for _, path := range sink.PartitionFiles() {
		if path == "" {
			continue
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %v", path, err)
		}
		batches, err := wshf.DecodeBatches(raw)
		if err != nil {
			t.Fatalf("decoding %s: %v", path, err)
		}
		for _, rb := range batches {
			for i := 0; i < rb.Len; i++ {
				id := rb.Columns[0].Int64Data[i]
				if seen[id] {
					t.Errorf("id %d appeared in two partitions", id)
				}
				seen[id] = true
				for ci, col := range schema[1:] {
					want := vals[col.Name][id]
					got := rb.Columns[ci+1].GetValue(i)
					if !reflect.DeepEqual(want, got) {
						t.Errorf("%s for id %d: got %#v, want %#v", col.Name, id, got, want)
					}
				}
			}
		}
	}
	if len(seen) != containerRows {
		t.Errorf("recovered %d rows across partitions, want %d", len(seen), containerRows)
	}
}

// TestGrowBatchToRejectsUnknownType pins the loud default: a 23rd column
// type must never take the silent Len-without-storage path the four
// container types spent #397 in.
func TestGrowBatchToRejectsUnknownType(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("growBatchTo accepted an unknown column type without panicking")
		}
		if msg := fmt.Sprint(r); !bytes.Contains([]byte(msg), []byte("no arm for column type")) {
			t.Fatalf("panic message %q does not name the missing arm", msg)
		}
	}()
	rb := batch.NewRecordBatch([]parquet.Column{{Name: "x", Type: parquet.TypeInt64}}, 0)
	rb.Columns[0].Type = parquet.TypeID(99)
	growBatchTo(rb, 4)
}
