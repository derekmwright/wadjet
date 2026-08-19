package format

import (
	"bytes"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestWriteTypedRendersTimestamp is the CLI half of issue #321 defect 1: a
// TIMESTAMP column printed as its raw epoch integer in every output format.
func TestWriteTypedRendersTimestamp(t *testing.T) {
	columns := []string{"id", "ts"}
	types := []parquet.TypeID{parquet.TypeInt64, parquet.TypeTimestamp}
	rows := []map[string]any{{"id": int64(1), "ts": int64(826727136000)}}

	for _, f := range []Format{Table, JSON, CSV} {
		var buf bytes.Buffer
		if err := WriteTyped(&buf, f, columns, types, rows); err != nil {
			t.Fatalf("format %v: %v", f, err)
		}
		out := buf.String()
		if !strings.Contains(out, "1996-03-13 14:25:36") {
			t.Errorf("format %v: timestamp not rendered; output = %q", f, out)
		}
		if strings.Contains(out, "826727136000") {
			t.Errorf("format %v: raw epoch integer still present; output = %q", f, out)
		}
		// The plain int64 column must be untouched.
		if !strings.Contains(out, "1") {
			t.Errorf("format %v: int64 column lost; output = %q", f, out)
		}
	}
}

// TestWriteTypedDoesNotMutateCaller: formatting is this package's business.
// The caller's rows may be the query result the program goes on to use, so
// rendering must not rewrite them in place.
func TestWriteTypedDoesNotMutateCaller(t *testing.T) {
	rows := []map[string]any{{"ts": int64(826727136000)}}
	var buf bytes.Buffer
	if err := WriteTyped(&buf, Table, []string{"ts"}, []parquet.TypeID{parquet.TypeTimestamp}, rows); err != nil {
		t.Fatal(err)
	}
	if got, ok := rows[0]["ts"].(int64); !ok || got != 826727136000 {
		t.Errorf("caller's row was mutated: ts = %v (%T), want int64(826727136000)", rows[0]["ts"], rows[0]["ts"])
	}
}

// TestWriteUntypedUnchanged: the untyped entry point has no column types to
// work from, so it must leave values exactly as the engine boxed them.
func TestWriteUntypedUnchanged(t *testing.T) {
	var buf bytes.Buffer
	rows := []map[string]any{{"ts": int64(826727136000)}}
	if err := Write(&buf, CSV, []string{"ts"}, rows); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "826727136000") {
		t.Errorf("untyped Write changed the value; output = %q", buf.String())
	}
}

// TestWriteTypedShortTypes: a types slice shorter than columns means
// "unknown" for the remainder, not an index panic.
func TestWriteTypedShortTypes(t *testing.T) {
	var buf bytes.Buffer
	columns := []string{"ts", "extra"}
	rows := []map[string]any{{"ts": int64(0), "extra": int64(7)}}
	if err := WriteTyped(&buf, CSV, columns, []parquet.TypeID{parquet.TypeTimestamp}, rows); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "1970-01-01 00:00:00") {
		t.Errorf("first column not rendered; output = %q", out)
	}
	if !strings.Contains(out, "7") {
		t.Errorf("untyped trailing column lost; output = %q", out)
	}
}

// TestWriteTypedNullTimestamp: a NULL must stay NULL, not become the epoch.
func TestWriteTypedNullTimestamp(t *testing.T) {
	var buf bytes.Buffer
	rows := []map[string]any{{"ts": nil}, {"ts": int64(0)}}
	if err := WriteTyped(&buf, CSV, []string{"ts"}, []parquet.TypeID{parquet.TypeTimestamp}, rows); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "NULL") {
		t.Errorf("NULL timestamp lost; output = %q", out)
	}
	if !strings.Contains(out, "1970-01-01 00:00:00") {
		t.Errorf("epoch timestamp not rendered; output = %q", out)
	}
}
