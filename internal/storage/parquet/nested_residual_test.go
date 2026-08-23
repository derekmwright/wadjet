package parquet

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// The assembler's one cross-check: after the row group's records, every leaf
// that was paged in sits exactly at the end of its own level stream
// (checkDrained, ADR-0018 §1 — an EXACT bound, from the format's own
// invariants).
//
// It is what sees a wrong REPETITION level. The level walk itself cannot:
// it reads what the levels say, and a level that closes a container early
// just produces a shorter value. But closing early where the mistake
// desynchronises SIBLING leaves leaves entries behind in the ones that were
// not shortened, and that is visible. Wadjet's own writer wrote exactly that
// shape before #409 — the second element of a LIST inside the first entry of
// a MAP got the OUTER container's repetition level — so files written then
// are refused here rather than answered from with a value silently missing
// its tail.

// mapOfListLeaves writes one row of MAP<STRING, ARRAY<INT64>> = {"k": [1,2]}
// with wadjet's (fixed) writer and returns the file's assembly plan, its
// per-leaf streams, and the leaf index of the list's element.
func mapOfListLeaves(t *testing.T) (*nestedNode, []leafColumnData, []*SchemaNode, int) {
	t.Helper()
	schema := Schema{Columns: []Column{{
		Name: "m_list", Type: TypeMap, Nullable: true,
		ElementType: &Column{Name: "entry", Type: TypeRow, Fields: []Column{
			{Name: "key", Type: TypeString},
			{Name: "value", Type: TypeArray, Nullable: true,
				ElementType: &Column{Name: "element", Type: TypeInt64, Nullable: true}},
		}},
	}}}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, schema, DefaultWriterConfig())
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	if err := w.WriteRows([]map[string]any{
		{"m_list": map[string]any{"k": []any{int64(1), int64(2)}}},
	}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	r, err := NewReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	fr := r.FileReader()
	leaves := fr.Leaves()
	pages := make([]leafColumnData, len(leaves))
	elemIdx := -1
	for i := range leaves {
		if pages[i], err = readLeafColumn(fr, 0, i); err != nil {
			t.Fatalf("leaf %v: %v", leaves[i].Path, err)
		}
		if leaves[i].Path[len(leaves[i].Path)-1] == "element" {
			elemIdx = i
		}
	}
	if elemIdx < 0 {
		t.Fatalf("no element leaf in %v", leaves)
	}
	plan := buildAssemblyPlan(fr.SchemaRoot().Children[0])
	if plan == nil {
		t.Fatal("no assembly plan for m_list")
	}
	return plan, pages, leaves, elemIdx
}

func TestAssemblyLeavesNoResidualOnAGoodFile(t *testing.T) {
	plan, pages, leaves, _ := mapOfListLeaves(t)
	rows := []map[string]any{{}}
	asm := newRecordAssembler(pages)
	asm.assembleNestedColumn(plan, "m_list", rows)
	if err := asm.checkDrained(leaves); err != nil {
		t.Fatalf("good file reported a residual: %v", err)
	}
	want := map[string]any{"k": []any{int64(1), int64(2)}}
	if !reflect.DeepEqual(rows[0]["m_list"], want) {
		t.Fatalf("assembled %#v, want %#v", rows[0]["m_list"], want)
	}
}

// TestAssemblyRefusesTheOldWritersRepetitionLevels replays the pre-#409
// encoding: the second element of the inner LIST carried the MAP's
// repetition level instead of the list's. The map closes after one entry
// because its KEY leaf is exhausted, the value comes back as [1], and the
// element leaf still holds an entry nobody read.
func TestAssemblyRefusesTheOldWritersRepetitionLevels(t *testing.T) {
	plan, pages, leaves, elemIdx := mapOfListLeaves(t)
	rep := pages[elemIdx].repLevels
	if len(rep) != 2 || rep[1] != 2 {
		t.Fatalf("element repetition levels are %v, expected the second entry at level 2", rep)
	}
	corrupt := append([]int32(nil), rep...)
	corrupt[1] = 1 // the outer MAP's level: what the old writer stamped
	pages[elemIdx].repLevels = corrupt

	rows := []map[string]any{{}}
	asm := newRecordAssembler(pages)
	asm.assembleNestedColumn(plan, "m_list", rows)
	// The value the corrupt levels produce, for the record: the tail is gone.
	if got := rows[0]["m_list"]; !reflect.DeepEqual(got, map[string]any{"k": []any{int64(1)}}) {
		t.Logf("corrupt levels assembled %#v", got)
	}
	err := asm.checkDrained(leaves)
	if err == nil {
		t.Fatal("a leaf with an unread entry left was accepted")
	}
	if !strings.Contains(err.Error(), "left over") {
		t.Fatalf("error does not name the residual: %v", err)
	}
}

// TestTestdataFilesAssembleWithoutResidual is the false-positive guard: the
// check is only worth having if no file a real writer produces trips it.
// Every parquet fixture in testdata (PyArrow, parquet-go and wadjet's own
// writer) is read whole through the row path.
func TestTestdataFilesAssembleWithoutResidual(t *testing.T) {
	files, err := filepath.Glob("testdata/*.parquet")
	if err != nil || len(files) == 0 {
		t.Fatalf("no testdata parquet files: %v", err)
	}
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			data, err := os.ReadFile(f)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			r, err := NewReaderFromBytes(data)
			if err != nil {
				t.Fatalf("reader: %v", err)
			}
			if _, err := r.ReadRows(nil); err != nil {
				t.Fatalf("ReadRows: %v", err)
			}
		})
	}
}
