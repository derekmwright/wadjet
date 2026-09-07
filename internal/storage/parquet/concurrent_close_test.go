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

// Round-2 P: the same race against a FAILING output stream, which is where the
// remaining one lived.
//
// The loser of the CAS used to read nw.err to decide what to report, while the
// winner was writing that same field through nw.fail() as its finalization
// died: "WARNING: DATA RACE" in 5 of 10 runs. The finalization now runs under a
// mutex, so a second caller WAITS for it and then reads a finished result
// rather than racing one — which keeps #888's answer ("every later call returns
// the latched failure") instead of trading it away for the fix.
func TestOnlyOneConcurrentCloseFinalizesAFailingStream(t *testing.T) {
	for _, failAt := range []int{1, 2, 3} {
		out := &failingWriter{failAt: failAt}
		w := NewNativeWriter(out, Schema{Columns: []Column{
			{Name: "x", Type: TypeInt64, Nullable: true},
		}}, WriterConfig{RowGroupSize: 100, Compression: CompressionNone})
		if err := w.WriteMapRows([]map[string]any{{"x": int64(1)}}); err != nil {
			t.Fatalf("failAt=%d: the row was refused before any output: %v", failAt, err)
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
				errs[i] = w.Close()
			}(i)
		}
		start.Done()
		done.Wait()

		// Every caller reports the finalization's own failure. #888's promise
		// is that the first output failure is latched and every later call
		// returns it, and the mutex makes that answer available to a
		// concurrent caller without a race: it waits for the finalization it
		// lost and then reads the finished result (round-2 P).
		for i, err := range errs {
			if err == nil {
				t.Fatalf("failAt=%d: Close %d returned nil over a failed output stream", failAt, i)
			}
			if !errors.Is(err, errInjectedOutput) {
				t.Fatalf("failAt=%d: Close %d reported %v, want the injected output failure",
					failAt, i, err)
			}
		}

		if err := w.WriteMapRows([]map[string]any{{"x": int64(2)}}); !errors.Is(err, errInjectedOutput) {
			t.Fatalf("failAt=%d: a write after the failed Close returned %v, want the output failure",
				failAt, err)
		}
	}
}
