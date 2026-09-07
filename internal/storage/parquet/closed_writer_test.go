package parquet

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// pyArrowOpenScript answers the one question this arc's gates ask of the
// reference implementation: does PyArrow OPEN and read this file, and if not,
// what does it say? Every refusal in these tests was measured against it
// (ROUND0.md), so the gate has to be able to state the same fact.
const pyArrowOpenScript = `
import json, sys, pyarrow.parquet as pq
try:
    t = pq.ParquetFile(sys.argv[1]).read()
    print(json.dumps({"ok": True, "rows": t.num_rows, "schema": str(t.schema)}))
except Exception as e:
    print(json.dumps({"ok": False, "err": type(e).__name__ + ": " + str(e)}))
`

type pyArrowOpen struct {
	OK     bool   `json:"ok"`
	Rows   int    `json:"rows"`
	Schema string `json:"schema"`
	Err    string `json:"err"`
}

// pyArrowOpens runs PyArrow over data. The bool is whether PyArrow RAN at all;
// a machine without it skips the cross-check rather than failing, exactly as
// the other PyArrow gates in this package do.
func pyArrowOpens(t *testing.T, data []byte) (pyArrowOpen, bool) {
	t.Helper()
	if !havePyArrow() {
		return pyArrowOpen{}, false
	}
	path := filepath.Join(t.TempDir(), "subject.parquet")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("python3", "-c", pyArrowOpenScript, path).CombinedOutput()
	if err != nil {
		t.Fatalf("pyarrow failed to run: %v\n%s", err, out)
	}
	var res pyArrowOpen
	if err := json.Unmarshal(out, &res); err != nil {
		t.Fatalf("decoding pyarrow output %q: %v", out, err)
	}
	return res, true
}

// mustPyArrowRead asserts PyArrow opens data and sees wantRows rows.
func mustPyArrowRead(t *testing.T, data []byte, wantRows int, what string) {
	t.Helper()
	res, ran := pyArrowOpens(t, data)
	if !ran {
		t.Log("python3 with pyarrow is not importable here — skipping the PyArrow cross-check")
		return
	}
	if !res.OK {
		t.Fatalf("%s: pyarrow refuses the file: %s", what, res.Err)
	}
	if res.Rows != wantRows {
		t.Fatalf("%s: pyarrow read %d rows, want %d", what, res.Rows, wantRows)
	}
}

// #972: a closed writer is closed.
//
// Measured at f415faba on this exact fixture, with Close returning nil both
// times and the post-Close WriteRows returning nil:
//
//	RowGroupSize 100 — the row was buffered, the 292-byte file did not change,
//	                   and the accepted row was silently LOST.
//	RowGroupSize 1   — a whole column chunk was appended AFTER the trailer,
//	                   292 bytes became 347, and both the row reader
//	                   ("invalid magic") and pyarrow ("Parquet magic bytes not
//	                   found in footer") then refused the finalized file.
//
// Both row-group sizes are driven because they are the two different failures,
// and both exported doors are driven because #970's lesson is that a guarantee
// on Writer alone is not a guarantee.
func TestAClosedWriterIsClosed(t *testing.T) {
	schema := Schema{Columns: []Column{{Name: "x", Type: TypeInt64, Nullable: true}}}
	cfgFor := func(rg int) WriterConfig {
		return WriterConfig{RowGroupSize: rg, Compression: CompressionNone}
	}

	// writeOne / closeIt / postWrite are the door under test; everything else
	// about the case is identical.
	doors := []struct {
		name string
		run  func(t *testing.T, out *bytes.Buffer, rg int) (write func() error, close func() error)
	}{
		{"Writer", func(t *testing.T, out *bytes.Buffer, rg int) (func() error, func() error) {
			w, err := NewWriter(out, schema, cfgFor(rg))
			if err != nil {
				t.Fatal(err)
			}
			n := int64(0)
			return func() error {
					n++
					return w.WriteRows([]map[string]any{{"x": n}})
				}, func() error {
					return w.Close()
				}
		}},
		{"NativeWriter", func(t *testing.T, out *bytes.Buffer, rg int) (func() error, func() error) {
			w := NewNativeWriter(out, schema, cfgFor(rg))
			n := int64(0)
			return func() error {
					n++
					return w.WriteMapRows([]map[string]any{{"x": n}})
				}, func() error {
					return w.Close()
				}
		}},
	}

	for _, door := range doors {
		for _, rg := range []int{100, 1} {
			t.Run(fmt.Sprintf("%s/rowGroupSize=%d", door.name, rg), func(t *testing.T) {
				var out bytes.Buffer
				write, closeIt := door.run(t, &out, rg)

				if err := write(); err != nil {
					t.Fatalf("the first write: %v", err)
				}
				if err := closeIt(); err != nil {
					t.Fatalf("the first Close: %v", err)
				}
				finalized := append([]byte(nil), out.Bytes()...)
				if len(finalized) == 0 {
					t.Fatal("Close wrote nothing")
				}

				// The refused write.
				if err := write(); !errors.Is(err, ErrWriterClosed) {
					t.Fatalf("rowGroupSize %d: a write after Close returned %v, want ErrWriterClosed "+
						"(at f415faba it returned nil and either lost the row or appended a column "+
						"chunk past the trailer, #972)", rg, err)
				}
				if !bytes.Equal(out.Bytes(), finalized) {
					t.Fatalf("rowGroupSize %d: the file changed after the refused write: %d bytes, was %d",
						rg, out.Len(), len(finalized))
				}

				// The refused second Close.
				if err := closeIt(); !errors.Is(err, ErrWriterClosed) {
					t.Fatalf("rowGroupSize %d: a second Close returned %v, want ErrWriterClosed "+
						"(at f415faba it returned nil after appending a second footer and trailer)", rg, err)
				}
				if !bytes.Equal(out.Bytes(), finalized) {
					t.Fatalf("rowGroupSize %d: the file changed after the refused second Close: %d bytes, was %d",
						rg, out.Len(), len(finalized))
				}

				// And what stands on disk is exactly what the first Close
				// finalized: one row, readable by both this package and the
				// reference implementation.
				r, err := NewReaderFromBytes(out.Bytes())
				if err != nil {
					t.Fatalf("rowGroupSize %d: reopening the finalized file: %v", rg, err)
				}
				rows, err := r.ReadRows(nil)
				if err != nil {
					t.Fatalf("rowGroupSize %d: ReadRows: %v", rg, err)
				}
				if len(rows) != 1 || rows[0]["x"] != int64(1) {
					t.Fatalf("rowGroupSize %d: the finalized file holds %v, want exactly [{x:1}]", rg, rows)
				}
				mustPyArrowRead(t, out.Bytes(), 1, "the finalized file after a refused write")
			})
		}
	}
}

// #972, the other half of "writes NOTHING": Writer.WriteRows converts network
// and temporal values IN THE CALLER'S OWN MAPS (prepareRows), so a refused
// write that had already run the conversion would leave the caller holding a
// rewritten row for a write that never happened.
func TestARefusedWriteDoesNotTouchTheCallersRows(t *testing.T) {
	var out bytes.Buffer
	w, err := NewWriter(&out, Schema{Columns: []Column{
		{Name: "ip", Type: TypeIPv4, Nullable: true},
	}}, DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRows([]map[string]any{{"ip": "10.0.0.5"}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	rows := []map[string]any{{"ip": "10.0.0.9"}}
	if err := w.WriteRows(rows); !errors.Is(err, ErrWriterClosed) {
		t.Fatalf("a write after Close returned %v, want ErrWriterClosed", err)
	}
	if got := rows[0]["ip"]; got != "10.0.0.9" {
		t.Fatalf("the refused write rewrote the caller's row: ip = %#v (%T), want the string "+
			"10.0.0.9 — prepareRows ran for a write that was not performed (#972)", got, got)
	}
}
