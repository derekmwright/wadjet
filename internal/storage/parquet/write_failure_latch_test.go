package parquet

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

// failingWriter fails the Nth Write and every one after it, and records what
// was written before that so a test can see whether the bytes form a file.
type failingWriter struct {
	buf bytes.Buffer
	// failAt is 1-based: the Write call that fails. oneShot makes ONLY that
	// call fail — the stream then recovers, which is the shape #888 measured
	// and the one where Close used to return nil over a file with a column
	// missing. Without oneShot every later Write fails too, so Close cannot
	// help reporting something.
	oneShot bool
	failAt  int
	writes  int
}

var errInjectedOutput = errors.New("injected output failure")

func (w *failingWriter) Write(p []byte) (int, error) {
	w.writes++
	hit := w.writes == w.failAt
	if !w.oneShot {
		hit = w.writes >= w.failAt
	}
	if w.failAt > 0 && hit {
		// A partial write, which is the realistic shape: the stream took some
		// bytes and then died.
		half := len(p) / 2
		w.buf.Write(p[:half])
		return half, errInjectedOutput
	}
	return w.buf.Write(p)
}

// #888: an output failure is LATCHED, and no later call acts as though the
// file could still be completed.
//
// Measured at de5bc970 with the error injected on the 4th Write of a one-row
// two-INT64 row group: WriteRows reported the I/O error, Close returned NIL,
// and the reader opened the result and returned {b:22} — column a simply gone,
// because flushRowGroup resets each leaf right after writing its chunk and
// neither the flush failure nor writeBytes latched anything.
//
// The injection point is swept across every Write of the file, so the property
// is asserted for the magic header, each column's page header and body, and
// the footer, rather than for the one offset the issue happened to name.
func TestAnOutputFailureIsLatchedAndNeverRetried(t *testing.T) {
	schema := Schema{Columns: []Column{
		{Name: "a", Type: TypeInt64, Nullable: true},
		{Name: "b", Type: TypeInt64, Nullable: true},
	}}
	rows := []map[string]any{{"a": int64(11), "b": int64(22)}}

	// How many writes a clean run takes — the sweep's upper bound.
	clean := &failingWriter{}
	nw := NewNativeWriter(clean, schema, DefaultWriterConfig())
	if err := nw.WriteMapRows(rows); err != nil {
		t.Fatalf("clean write: %v", err)
	}
	if err := nw.Close(); err != nil {
		t.Fatalf("clean close: %v", err)
	}
	total := clean.writes
	if total < 4 {
		t.Fatalf("a two-column row group took %d writes; this sweep assumes several", total)
	}

	for at := 1; at <= total; at++ {
		for _, oneShot := range []bool{false, true} {
			t.Run(fmt.Sprintf("fail_at_write_%d_oneshot_%v", at, oneShot), func(t *testing.T) {
				runLatchCase(t, schema, rows, at, oneShot)
			})
		}
	}
}

func runLatchCase(t *testing.T, schema Schema, rows []map[string]any, at int, oneShot bool) {
	w := &failingWriter{failAt: at, oneShot: oneShot}
	nw := NewNativeWriter(w, schema, DefaultWriterConfig())

	werr := nw.WriteMapRows(rows)
	cerr := nw.Close()

	// SOMETHING must report it. Which call depends on where the
	// injection lands (a flush only happens at RowGroupSize or at
	// Close), but Close must never be the one that says nil.
	if cerr == nil {
		t.Fatalf("Close returned nil after an output failure at write %d "+
			"(WriteMapRows said %v); the file cannot be complete", at, werr)
	}
	if !errors.Is(cerr, errInjectedOutput) {
		t.Errorf("Close reported %v, want the injected output failure", cerr)
	}
	if werr != nil && !errors.Is(werr, errInjectedOutput) {
		t.Errorf("WriteMapRows reported %v, want the injected output failure", werr)
	}

	// The latch holds: every later call answers the same failure.
	if again := nw.Close(); !errors.Is(again, errInjectedOutput) {
		t.Errorf("a second Close reported %v, want the same failure", again)
	}
	if later := nw.WriteMapRows(rows); !errors.Is(later, errInjectedOutput) {
		t.Errorf("a later WriteMapRows reported %v, want the same failure", later)
	}

	// And the bytes on the stream must not be a readable file. The
	// defect's whole shape was a VALID file with a column missing.
	if r, err := NewReaderFromBytes(w.buf.Bytes()); err == nil {
		got, rerr := r.ReadRows(nil)
		t.Errorf("the truncated stream opened as a valid file: rows %v (%v)", got, rerr)
	}
}
