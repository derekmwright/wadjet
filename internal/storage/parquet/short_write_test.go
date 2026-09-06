package parquet

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"
)

// shortNilWriter takes a partial slice on its Nth Write and returns nil — the
// io.Writer contract violation the Go docs say an implementation SHOULD report
// with a non-nil error but many do not, so the caller must defend the boundary
// with io.ErrShortWrite. Only the shortAt-th call is short; every other call is
// whole, which is the shape where a naive count-blind writeBytes would keep
// going and finalize a file with bytes dropped out of its middle.
type shortNilWriter struct {
	b       bytes.Buffer
	shortAt int
	writes  int
}

func (w *shortNilWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.writes == w.shortAt {
		n := len(p) / 2
		return w.b.Write(p[:n])
	}
	return w.b.Write(p)
}

// TestAShortNilWriteIsLatchedAndNeverRetried is the #926 companion to #888's
// TestAnOutputFailureIsLatchedAndNeverRetried. #888 sweeps a partial write that
// carries a non-nil error across every output call; this sweeps a partial write
// that returns NIL — the boundary writeBytes now defends with io.ErrShortWrite.
// The same three properties are asserted at every write position: SOMETHING
// reports it (never Close returning nil), the latch holds across a repeated
// Close and a later WriteMapRows, and the bytes left on the stream do not open
// as a valid file with a column silently gone.
func TestAShortNilWriteIsLatchedAndNeverRetried(t *testing.T) {
	schema := Schema{Columns: []Column{
		{Name: "a", Type: TypeInt64, Nullable: true},
		{Name: "b", Type: TypeInt64, Nullable: true},
	}}
	rows := []map[string]any{{"a": int64(11), "b": int64(22)}}

	// How many writes a clean run takes — the sweep's upper bound.
	clean := &shortNilWriter{}
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
		t.Run(fmt.Sprintf("short_at_write_%d", at), func(t *testing.T) {
			w := &shortNilWriter{shortAt: at}
			nw := NewNativeWriter(w, schema, DefaultWriterConfig())

			werr := nw.WriteMapRows(rows)
			cerr := nw.Close()

			if cerr == nil {
				t.Fatalf("Close returned nil after a short write at position %d "+
					"(WriteMapRows said %v); the file cannot be complete", at, werr)
			}
			if !errors.Is(cerr, io.ErrShortWrite) {
				t.Errorf("Close reported %v, want io.ErrShortWrite", cerr)
			}
			if werr != nil && !errors.Is(werr, io.ErrShortWrite) {
				t.Errorf("WriteMapRows reported %v, want io.ErrShortWrite", werr)
			}

			// The latch holds: every later call answers the same failure.
			if again := nw.Close(); !errors.Is(again, io.ErrShortWrite) {
				t.Errorf("a second Close reported %v, want the same failure", again)
			}
			if later := nw.WriteMapRows(rows); !errors.Is(later, io.ErrShortWrite) {
				t.Errorf("a later WriteMapRows reported %v, want the same failure", later)
			}

			// And the bytes on the stream must not open as a readable file:
			// the whole defect was a VALID file with bytes dropped from it.
			if r, err := NewReaderFromBytes(w.b.Bytes()); err == nil {
				got, rerr := r.ReadRows(nil)
				t.Errorf("the truncated stream opened as a valid file: rows %v (%v)", got, rerr)
			}
		})
	}
}

// #926: NativeWriter.writeBytes checked only the error io.Writer.Write returned
// and advanced nw.written by the partial count on (n < len(p), nil), reporting
// success. Both WriteMapRows and Close could then return nil after bytes were
// dropped — a truncated file reported as a good one, bypassing #888's latch.
//
// A one-shot short write is injected at each of the first six output calls of a
// two-column file. Every EXERCISED position must be reported by WriteMapRows or
// Close; Close must never be the call that returns nil over a short-written
// stream.
func TestNativeWriterDetectsShortNilWrite(t *testing.T) {
	s := Schema{Columns: []Column{{Name: "a", Type: TypeInt64}, {Name: "b", Type: TypeInt64}}}
	for at := 1; at <= 6; at++ {
		out := &shortNilWriter{shortAt: at}
		w := NewNativeWriter(out, s, DefaultWriterConfig())
		werr := w.WriteMapRows([]map[string]any{{"a": int64(11), "b": int64(22)}})
		cerr := w.Close()
		if out.writes < at {
			// The clean run took fewer writes than this position; nothing was
			// injected, so there is nothing to assert.
			continue
		}
		if werr == nil && cerr == nil {
			t.Errorf("short write %d/%d was reported as success", at, out.writes)
			continue
		}
		// Whichever call reported it must carry io.ErrShortWrite, and the
		// latch must hold: a repeated Close and a later WriteMapRows both stay
		// failed with the same cause.
		reported := cerr
		if reported == nil {
			reported = werr
		}
		if !errors.Is(reported, io.ErrShortWrite) {
			t.Errorf("short write %d/%d reported %v, want io.ErrShortWrite", at, out.writes, reported)
		}
		if again := w.Close(); !errors.Is(again, io.ErrShortWrite) {
			t.Errorf("short write %d/%d: a second Close reported %v, want the latched io.ErrShortWrite", at, out.writes, again)
		}
		if later := w.WriteMapRows([]map[string]any{{"a": int64(33), "b": int64(44)}}); !errors.Is(later, io.ErrShortWrite) {
			t.Errorf("short write %d/%d: a later WriteMapRows reported %v, want the latched io.ErrShortWrite", at, out.writes, later)
		}
	}
}
