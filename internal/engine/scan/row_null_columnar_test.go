package scan

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #425: a NULL ROW came back from the native columnar reader as a PRESENT
// ROW with every field null.
//
// A ROW is a parquet GROUP and a group has no column chunk: whether the
// group itself was absent is recorded ONLY in its leaves' definition
// levels. readRowGroupNative read the leaves and stopped there, so
//
//	r = NULL                        (group absent)
//	r = ROW(a => NULL, b => NULL)   (group present, both fields null)
//
// decoded identically — both as the second. The row-based reader, which is
// where the single-process planner sends any schema with a nested column
// (readBatchDirect's HasNestedColumns gate), got it right, so the defect
// surfaced only on the paths that reach the native reader: the DAG worker's
// stream source, where the projected schema carries the ROW but no
// ARRAY/MAP to route it away.
//
// The two readers are the oracle for each other here, per ADR-0018's rule
// that "the paths agree" is only a property if both are asked.

// rowNullFixture is the four states a nullable ROW column can be in, times
// enough rows to cross page and row-group boundaries.
func rowNullFixture(n int) (pqt.Schema, []map[string]any) {
	schema := pqt.Schema{Columns: []pqt.Column{
		{Name: "id", Type: pqt.TypeInt64},
		{Name: "r", Type: pqt.TypeRow, Nullable: true, Fields: []pqt.Column{
			{Name: "a", Type: pqt.TypeString, Nullable: true},
			{Name: "b", Type: pqt.TypeInt64, Nullable: true},
		}},
	}}
	rows := make([]map[string]any, n)
	for i := range rows {
		var r any
		switch i % 4 {
		case 0:
			r = map[string]any{"a": fmt.Sprintf("v-%05d", i), "b": int64(i)}
		case 1:
			// NULL ROW — the state #425 lost.
			r = nil
		case 2:
			// PRESENT ROW whose every field is NULL: identical leaf nulls to
			// the case above, and the only thing that separates them is the
			// group's own definition level.
			r = map[string]any{"a": nil, "b": nil}
		case 3:
			// One field null, one not.
			r = map[string]any{"a": nil, "b": int64(-i)}
		}
		rows[i] = map[string]any{"id": int64(i), "r": r}
	}
	return schema, rows
}

func writeRowNullParquet(tb testing.TB, schema pqt.Schema, rows []map[string]any) *pqt.Reader {
	tb.Helper()
	cfg := pqt.DefaultWriterConfig()
	// Several row groups, several pages each: the group-presence walk runs
	// per page and advances an offset, so a single-page fixture would not
	// see it get that wrong.
	cfg.RowGroupSize = 300
	cfg.PageBufferSize = 512
	var buf bytes.Buffer
	w, err := pqt.NewWriter(&buf, schema, cfg)
	if err != nil {
		tb.Fatalf("parquet writer: %v", err)
	}
	if err := w.WriteRows(rows); err != nil {
		tb.Fatalf("write rows: %v", err)
	}
	if err := w.Close(); err != nil {
		tb.Fatalf("close writer: %v", err)
	}
	r, err := pqt.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		tb.Fatalf("parquet reader: %v", err)
	}
	return r
}

func TestNativeReaderKeepsRowGroupNulls(t *testing.T) {
	const n = 1000
	schema, rows := rowNullFixture(n)
	reader := writeRowNullParquet(t, schema, rows)

	if HasUnsupportedColumnarTypes(schema.Columns) {
		t.Fatal("a ROW column must reach the NATIVE reader — this test proves nothing otherwise")
	}
	batches, err := ReadFileBatches(reader, schema.Columns, nil)
	if err != nil {
		t.Fatalf("ReadFileBatches: %v", err)
	}

	seen := 0
	for _, b := range batches {
		for i := 0; i < b.Len; i++ {
			id := b.Columns[0].Int64Data[i]
			want := rows[id]["r"]
			gotNull := b.Columns[1].Nulls.IsNull(i)
			if (want == nil) != gotNull {
				t.Fatalf("row id=%d: decoded null=%v, want null=%v (source %#v)",
					id, gotNull, want == nil, want)
			}
			got := b.Columns[1].GetValue(i)
			if want == nil {
				if got != nil {
					t.Fatalf("row id=%d: NULL ROW decoded as %#v", id, got)
				}
			} else if !reflect.DeepEqual(got, want) {
				t.Fatalf("row id=%d: decoded %#v, want %#v", id, got, want)
			}
			seen++
		}
	}
	if seen != n {
		t.Fatalf("read %d rows, want %d", seen, n)
	}
}

// TestNativeAndRowReadersAgreeOnRowNulls asks the two readers the same
// question. They are independent implementations — the native one walks
// column chunks, the row one walks records — and #425 was exactly a case
// where only one of them was asked.
func TestNativeAndRowReadersAgreeOnRowNulls(t *testing.T) {
	const n = 400
	schema, rows := rowNullFixture(n)

	native := writeRowNullParquet(t, schema, rows)
	batches, err := ReadFileBatches(native, schema.Columns, nil)
	if err != nil {
		t.Fatalf("ReadFileBatches: %v", err)
	}
	nativeNull := make(map[int64]bool, n)
	for _, b := range batches {
		for i := 0; i < b.Len; i++ {
			nativeNull[b.Columns[0].Int64Data[i]] = b.Columns[1].Nulls.IsNull(i)
		}
	}

	viaRows := writeRowNullParquet(t, schema, rows)
	got, err := viaRows.ReadRows([]string{"id", "r"})
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	if len(got) != n {
		t.Fatalf("row reader returned %d rows, want %d", len(got), n)
	}
	for _, row := range got {
		id, _ := row["id"].(int64)
		rowNull := row["r"] == nil
		if nativeNull[id] != rowNull {
			t.Errorf("row id=%d: native says null=%v, row reader says null=%v",
				id, nativeNull[id], rowNull)
		}
	}
}

// TestNativeReaderRowNullsUnderProjection is the shape the DAG worker
// actually takes: the ROW is projected on its own, so the schema the reader
// sees has no ARRAY/MAP to route it to the row reader.
func TestNativeReaderRowNullsUnderProjection(t *testing.T) {
	const n = 200
	schema, rows := rowNullFixture(n)
	reader := writeRowNullParquet(t, schema, rows)

	batches, err := ReadFileBatches(reader, schema.Columns, []string{"r"})
	if err != nil {
		t.Fatalf("ReadFileBatches: %v", err)
	}
	next := 0
	for _, b := range batches {
		for i := 0; i < b.Len; i++ {
			want := rows[next]["r"] == nil
			if got := b.Columns[0].Nulls.IsNull(i); got != want {
				t.Fatalf("projected row %d: null=%v, want %v", next, got, want)
			}
			next++
		}
	}
	if next != n {
		t.Fatalf("read %d rows, want %d", next, n)
	}
}
