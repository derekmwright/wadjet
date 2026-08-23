package parquet

import (
	"bytes"
	"os"
	"testing"
)

// Reading one row group must answer what reading the whole file answered for
// those rows (#428).
//
// ReadRowGroup used to be a SECOND reader with its own body: it resolved
// every column through leafByName, and no ARRAY/ROW/MAP has a leaf named
// after it, so a container column was filled with nils and its key dropped
// from every row. No error, no dispatch to the assembler. Compaction is
// ReadRowGroup → WriteRows over the table's full schema, so merging a table
// with any nested column wrote that column as NULL for every row and
// replaced the inputs with the result.
//
// The two entry points share one implementation now; these tests are what
// keeps them from drifting apart again.

// readByRowGroup concatenates every row group's rows, which is what a
// streaming consumer (compaction, ANALYZE) sees.
func readByRowGroup(t *testing.T, r *Reader, cols []string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for rg := 0; rg < r.NumRowGroups(); rg++ {
		rows, err := r.ReadRowGroup(rg, cols)
		if err != nil {
			t.Fatalf("ReadRowGroup(%d): %v", rg, err)
		}
		out = append(out, rows...)
	}
	return out
}

func TestReadRowGroupAssemblesNestedContainers(t *testing.T) {
	data, err := os.ReadFile(nestedFixture)
	if err != nil {
		t.Fatalf("fixture: %v (regenerate with gen_nested_containers.py)", err)
	}
	r, err := NewReaderFromBytes(data)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	assertRowsEqual(t, readByRowGroup(t, r, nil), nestedContainersWant())
}

// The same rows through wadjet's own writer, split across several row groups
// so the per-group assembly is exercised at every group and not only the
// first — and projected, because compaction always passes a column list.
func TestReadRowGroupMatchesReadRowsAcrossRowGroups(t *testing.T) {
	want := nestedContainersWant()
	var buf bytes.Buffer
	cfg := DefaultWriterConfig()
	cfg.RowGroupSize = 2
	pw, err := NewWriter(&buf, nestedContainersSchema(), cfg)
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	if err := pw.WriteRows(want); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	r, err := NewReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	if r.NumRowGroups() < 3 {
		t.Fatalf("expected several row groups, got %d", r.NumRowGroups())
	}

	for _, cols := range [][]string{
		nil,
		{"id", "m_list"},
		{"a_arr", "r_row"},
		{"id"},
	} {
		full, err := r.ReadRows(cols)
		if err != nil {
			t.Fatalf("ReadRows(%v): %v", cols, err)
		}
		assertRowsEqual(t, readByRowGroup(t, r, cols), full)
	}
}

// ReadRowGroupAs carries ReadRowsAs's contract — the CALLER's types — over
// one row group. Compaction needs it: the table's schema is the authority on
// the types a parquet file cannot describe, and reading without it hands the
// writer values it then refuses.
func TestReadRowGroupAsUsesTheCallersTypes(t *testing.T) {
	schema := Schema{Columns: []Column{
		{Name: "id", Type: TypeInt64},
		{Name: "n", Type: TypeString, Nullable: true},
	}}
	var buf bytes.Buffer
	pw, err := NewWriter(&buf, schema, DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := pw.WriteRows([]map[string]any{{"id": int64(1), "n": "abc"}}); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := NewReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	// The catalog says BYTES where the file recovered STRING.
	want := []Column{{Name: "id", Type: TypeInt64}, {Name: "n", Type: TypeBytes, Nullable: true}}
	rows, err := r.ReadRowGroupAs(0, want, nil)
	if err != nil {
		t.Fatalf("ReadRowGroupAs: %v", err)
	}
	if _, ok := rows[0]["n"].([]byte); !ok {
		t.Fatalf("column n came back as %T, want []byte — the caller's type was ignored", rows[0]["n"])
	}
}

func TestReadRowGroupIndexOutOfRange(t *testing.T) {
	data, err := os.ReadFile(nestedFixture)
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	r, err := NewReaderFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	for _, idx := range []int{-1, r.NumRowGroups(), 999} {
		if _, err := r.ReadRowGroup(idx, nil); err == nil {
			t.Errorf("ReadRowGroup(%d) succeeded, want an out-of-range error", idx)
		}
	}
}
