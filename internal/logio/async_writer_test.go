package logio

import (
	"bytes"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// blockingWriter blocks every Write until released — a stand-in for a
// stalled journald pipe.
type blockingWriter struct {
	release chan struct{}
	mu      sync.Mutex
	buf     bytes.Buffer
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	<-w.release
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *blockingWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// TestAsyncWriter_PassThrough: records reach the sink in order.
func TestAsyncWriter_PassThrough(t *testing.T) {
	var buf bytes.Buffer
	var mu sync.Mutex
	w := NewAsyncWriter(writerFunc(func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return buf.Write(p)
	}), 16)
	for _, s := range []string{"a\n", "b\n", "c\n"} {
		if _, err := w.Write([]byte(s)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if got := buf.String(); got != "a\nb\nc\n" {
		t.Fatalf("sink got %q", got)
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// TestAsyncWriter_NeverBlocks is the regression test for the log-jam
// freeze (validation run 20260814-002639): with the sink fully stalled,
// writes past the buffer depth must return immediately and be counted
// as drops — the caller (slog handler holding its mutex) must never
// park on the sink.
func TestAsyncWriter_NeverBlocks(t *testing.T) {
	sink := &blockingWriter{release: make(chan struct{})}
	w := NewAsyncWriter(sink, 4)

	done := make(chan struct{})
	go func() {
		// depth 4 + 1 in-flight in the drainer; write far past it.
		for i := 0; i < 100; i++ {
			w.Write([]byte("x\n"))
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Write blocked on a stalled sink")
	}
	if w.Dropped() == 0 {
		t.Fatal("expected drops with stalled sink")
	}

	// Un-stall the sink; the drainer must surface the drop count.
	close(sink.release)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(sink.String(), "async log writer dropped records") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !strings.Contains(sink.String(), "async log writer dropped records") {
		t.Fatalf("drop report never surfaced; sink: %q", sink.String())
	}
	_ = w.Close()
}

// TestAsyncWriter_CloseDrains: buffered records are flushed on Close,
// and post-Close writes don't panic.
func TestAsyncWriter_CloseDrains(t *testing.T) {
	var buf bytes.Buffer
	var mu sync.Mutex
	var w io.Writer = writerFunc(func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return buf.Write(p)
	})
	a := NewAsyncWriter(w, 64)
	for i := 0; i < 10; i++ {
		a.Write([]byte("line\n"))
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	a.Write([]byte("after\n")) // must not panic
	mu.Lock()
	defer mu.Unlock()
	if got := strings.Count(buf.String(), "line"); got != 10 {
		t.Fatalf("want 10 flushed lines, got %d", got)
	}
}
