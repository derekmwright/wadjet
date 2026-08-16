package worker

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// sleepSource yields n empty-schema batches, sleeping per Next so the
// timedSource wrapper has measurable wall to charge.
type sleepSource struct {
	n     int
	sleep time.Duration
}

func (s *sleepSource) Init(context.Context) error { return nil }
func (s *sleepSource) Close() error               { return nil }
func (s *sleepSource) Next(context.Context) (*batch.RecordBatch, error) {
	if s.n == 0 {
		return nil, nil
	}
	s.n--
	time.Sleep(s.sleep)
	return batch.NewRecordBatch(nil, 0), nil
}

func TestTimedSourceChargesNextWall(t *testing.T) {
	fp := newFragmentProgress(slog.Default(), distributed.Task{ID: "t", StageID: "s"}, nil)
	src := fp.timeSource(&sleepSource{n: 3, sleep: 2 * time.Millisecond})
	for {
		b, err := src.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
	}
	if got := fp.srcNs.Load(); got < (6 * time.Millisecond).Nanoseconds() {
		t.Fatalf("srcNs = %d, want >= 6ms of charged Next wall", got)
	}
}

func TestFragmentPhasesLine(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	fp := newFragmentProgress(logger, distributed.Task{ID: "t1", StageID: "join-6", StageType: "broadcast_join"}, nil)
	fp.srcNs.Store((250 * time.Millisecond).Nanoseconds())
	fp.opsNs.Store((100 * time.Millisecond).Nanoseconds())
	fp.sinkNs.Store((50 * time.Millisecond).Nanoseconds())
	fp.finish(1234)

	out := buf.String()
	if !strings.Contains(out, "fragment task phases") {
		t.Fatalf("no phases line emitted: %q", out)
	}
	for _, want := range []string{"src_ms=250", "ops_ms=100", "sink_ms=50", "rows=1234"} {
		if !strings.Contains(out, want) {
			t.Errorf("phases line missing %s: %q", want, out)
		}
	}
	// Zero-valued phases stay off the line.
	if strings.Contains(out, "input_wait_ms") || strings.Contains(out, "src_blocked_ms") {
		t.Errorf("zero phases should be omitted: %q", out)
	}
}
