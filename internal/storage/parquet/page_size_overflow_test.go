package parquet

import (
	"bytes"
	"math"
	"strings"
	"testing"
)

// #929: a page body over math.MaxInt32 bytes cannot be described by the Thrift
// header's int32 page-size fields; the cast wrapped it to a negative size and
// the file was finalized as success. checkPageSize refuses it at the boundary.
//
// Deterministic, no 2GB allocation. FAILS on e17e2b92 (no such guard exists).
func TestCheckPageSizeBoundary(t *testing.T) {
	cases := []struct {
		n      int
		wantOK bool
	}{
		{0, true},
		{1, true},
		{math.MaxInt32 - 1, true},
		{math.MaxInt32, true},
		{math.MaxInt32 + 1, false},
		{math.MaxInt32 + 1024, false},
	}
	for _, c := range cases {
		err := checkPageSize("x", c.n)
		if c.wantOK && err != nil {
			t.Errorf("checkPageSize(%d) = %v, want nil", c.n, err)
		}
		if !c.wantOK {
			if err == nil {
				t.Errorf("checkPageSize(%d) = nil, want an error", c.n)
			} else if !strings.Contains(err.Error(), "exceeds") || !strings.Contains(err.Error(), `"x"`) {
				t.Errorf("checkPageSize(%d) error = %q, want it to name the column and the limit", c.n, err)
			}
		}
	}
}

// Control: the value-size guard must not refuse ordinary data. A modest BYTES
// value writes and finalizes cleanly.
func TestWriterRefusesOversizeByteArrayValue_Small(t *testing.T) {
	s := Schema{Columns: []Column{{Name: "b", Type: TypeBytes}}}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s, DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRows([]map[string]any{{"b": bytes.Repeat([]byte{7}, 1024)}}); err != nil {
		t.Fatalf("ordinary 1KiB value refused: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if buf.Len() == 0 {
		t.Fatal("no file written")
	}
}

// The end-to-end refusal: a single >2GB BYTES value must make WriteRows/Close
// return a non-nil error rather than finalize a file whose page size wrapped
// negative. Behind -short and a large allocation, so CI never runs it.
//
// FAILS on e17e2b92: the write succeeds and RowGroups[0] carries a negative
// page size. Passes after: the byte-array value refusal fires in WriteRows.
func TestWriterRefusesOversizeByteArrayValue_Large(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates >2GB; skipped under -short")
	}
	const n = math.MaxInt32 + 16 // just over the int32 page-size limit
	big := make([]byte, n)       // ~2GB
	s := Schema{Columns: []Column{{Name: "b", Type: TypeBytes}}}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s, DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	werr := w.WriteRows([]map[string]any{{"b": big}})
	cerr := w.Close()
	if werr == nil && cerr == nil {
		t.Fatalf("oversize %d-byte value produced a file with no error — corrupt page size finalized as success", n)
	}
	got := ""
	if werr != nil {
		got = werr.Error()
	} else {
		got = cerr.Error()
	}
	if !strings.Contains(got, "exceeds") {
		t.Fatalf("error = %q, want it to name the size limit", got)
	}
}
