package parquet

import (
	"bytes"
	"errors"
	"sync"
	"testing"
)

// Round-1 N1: two goroutines racing to Close cannot both finalize.
//
// A writer is not safe for concurrent use and does not claim to be — its leaf
// buffers, its error latch and its byte count are all unsynchronized. The
// closed latch is the exception, because the damage a lost race does THERE is
// a second footer and trailer written over a complete file: the same corruption
// #972 fixed for the sequential case, reached through a different door. The
// latch is claimed with CompareAndSwap, so exactly one caller finalizes and
// every other one gets ErrWriterClosed.
//
// Run under -race, where the plain bool this replaced reported
// "WARNING: DATA RACE" between the read and the write of the latch.
func TestOnlyOneConcurrentCloseFinalizesTheFile(t *testing.T) {
	for _, native := range []bool{false, true} {
		var out bytes.Buffer
		schema := Schema{Columns: []Column{{Name: "x", Type: TypeInt64, Nullable: true}}}
		cfg := WriterConfig{RowGroupSize: 100, Compression: CompressionNone}

		var closeIt func() error
		if native {
			w := NewNativeWriter(&out, schema, cfg)
			if err := w.WriteMapRows([]map[string]any{{"x": int64(1)}}); err != nil {
				t.Fatal(err)
			}
			closeIt = w.Close
		} else {
			w, err := NewWriter(&out, schema, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err := w.WriteRows([]map[string]any{{"x": int64(1)}}); err != nil {
				t.Fatal(err)
			}
			closeIt = w.Close
		}

		const goroutines = 8
		errs := make([]error, goroutines)
		var start, done sync.WaitGroup
		start.Add(1)
		done.Add(goroutines)
		for i := 0; i < goroutines; i++ {
			go func(i int) {
				defer done.Done()
				start.Wait()
				errs[i] = closeIt()
			}(i)
		}
		start.Done()
		done.Wait()

		finalizers, refusals := 0, 0
		for _, err := range errs {
			switch {
			case err == nil:
				finalizers++
			case errors.Is(err, ErrWriterClosed):
				refusals++
			default:
				t.Fatalf("native=%v: a concurrent Close returned an unexpected error: %v", native, err)
			}
		}
		if finalizers != 1 {
			t.Fatalf("native=%v: %d of %d concurrent Closes finalized the file; exactly one may "+
				"(round-1 N1)", native, finalizers, goroutines)
		}
		if refusals != goroutines-1 {
			t.Fatalf("native=%v: %d refusals, want %d", native, refusals, goroutines-1)
		}

		// One footer, one trailer, one readable file.
		r, err := NewReaderFromBytes(out.Bytes())
		if err != nil {
			t.Fatalf("native=%v: the file eight goroutines closed is unreadable: %v", native, err)
		}
		rows, err := r.ReadRows(nil)
		if err != nil {
			t.Fatalf("native=%v: ReadRows: %v", native, err)
		}
		if len(rows) != 1 || rows[0]["x"] != int64(1) {
			t.Fatalf("native=%v: the file holds %v, want exactly [{x:1}]", native, rows)
		}
		mustPyArrowRead(t, out.Bytes(), 1, "a file closed by eight goroutines at once")
	}
}
