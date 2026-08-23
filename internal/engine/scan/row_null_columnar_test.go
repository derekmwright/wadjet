package scan

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
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

// TestRowPresenceDeclineDoesNotBurnTheOneAttempt guards the loop in
// readRowGroupNative that measures a ROW's own presence from its FIRST
// leaf: `measured` used to be set true whenever the loop ATTEMPTED
// newRowPresence at all, even when it declined (returned nil). A ROW whose
// first field's leaf cannot carry presence — a bare REPEATED primitive
// directly under the group is the shape that declines, since definition
// levels there are per ELEMENT, not per row — then silently stopped the
// group from ever getting presence tracking, even though a LATER field of
// the same ROW could have supplied it.
//
// wadjet's own writer never emits a bare repeated primitive directly under
// a ROW group (Column.Fields always writes OPTIONAL/REQUIRED; REPEATED is
// used only inside ARRAY/MAP's own list/element wrapper, which routes to
// the row reader before this code ever runs), so there is no written file
// that exercises this. This tests newRowPresence's decline and the loop's
// `measured` bookkeeping directly, mirroring readRowGroupNative's own
// two-line pattern exactly.
func TestRowPresenceDeclineDoesNotBurnTheOneAttempt(t *testing.T) {
	group := &pqt.SchemaNode{Name: "s", MaxDefLevel: 1, MaxRepLevel: 0}
	repLeaf := &pqt.SchemaNode{Name: "rep_field", Parent: group, MaxRepLevel: 1}
	okLeaf := &pqt.SchemaNode{Name: "c", Parent: group, MaxRepLevel: 0}
	leaves := []*pqt.SchemaNode{repLeaf, okLeaf}

	vec := &batch.Vector{}

	measured := false
	var presRep *rowPresence
	if !measured {
		presRep = newRowPresence(vec, leaves, 0)
		measured = presRep != nil
	}
	if presRep != nil {
		t.Fatalf("newRowPresence over a REPEATED leaf should decline (nil), got %+v", presRep)
	}
	if measured {
		t.Fatalf("a declined attempt must not set measured — that burns the row's one chance at presence tracking")
	}

	var presC *rowPresence
	if !measured {
		presC = newRowPresence(vec, leaves, 1)
		measured = presC != nil
	}
	if presC == nil {
		t.Fatalf("the second field's own leaf qualifies (MaxDefLevel 1, not repeated) — it must still get a chance after the first declined")
	}
	if !measured {
		t.Fatalf("a successful attempt must set measured")
	}
}

// TestRowPresenceNoteRefusesTruncatedLevelStream is ADR-0018's rule applied
// to rowPresence.note: both plain and RLE-encoded definition-level streams
// decode exactly the page's NumValues, so a shorter slice is not a shape a
// well-formed file produces. note used to treat that shape as "every
// remaining row present" — the same silently-wrong-NULLs failure mode
// #425 fixed for the ordinary case, reopened for a truncated file.
func TestRowPresenceNoteRefusesTruncatedLevelStream(t *testing.T) {
	p := &rowPresence{vec: &batch.Vector{Nulls: batch.NewBitmap(4)}, defLevel: 1}

	// The page claims 4 rows; the decoded level stream carries only 2 —
	// hand-truncated, the shape a corrupt file produces.
	if err := p.note([]int32{1, 0}, 0, 4); err == nil {
		t.Fatal("note must refuse a definition-level stream shorter than the page's row count, not silently treat the missing levels as PRESENT")
	}

	// The well-formed shape — an exact-length level stream — must still
	// work and mark nulls correctly: defLevel[i] < p.defLevel(1) is absent.
	if err := p.note([]int32{1, 0, 1, 1}, 0, 4); err != nil {
		t.Fatalf("note on a well-formed level stream: %v", err)
	}
	for i, wantNull := range []bool{false, true, false, false} {
		if got := p.vec.Nulls.IsNull(i); got != wantNull {
			t.Errorf("row %d: null=%v, want %v", i, got, wantNull)
		}
	}
}
