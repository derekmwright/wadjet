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

// fakeAggLayout is a minimal aggregateLayoutReporter stub — cheaper than
// driving a real *exec.HashAggregate through Consume just to pin the log
// line's formatting.
type fakeAggLayout struct {
	bornFlat bool
	reason   string
}

func (f fakeAggLayout) IndexBornFlat() bool     { return f.bornFlat }
func (f fakeAggLayout) IndexFlatReason() string { return f.reason }

// fakeSinkSource is a bare exec.SinkSource that does NOT implement
// aggregateLayoutReporter — the Sort breaker's shape.
type fakeSinkSource struct{}

func (fakeSinkSource) Init(context.Context) error                        { return nil }
func (fakeSinkSource) Consume(context.Context, *batch.RecordBatch) error { return nil }
func (fakeSinkSource) Finalize(context.Context) error                    { return nil }
func (fakeSinkSource) Close() error                                      { return nil }
func (fakeSinkSource) Next(context.Context) (*batch.RecordBatch, error)  { return nil, nil }

// fakeAggSinkSource is a SinkSource that ALSO reports a layout — the
// HashAggregate breaker's shape.
type fakeAggSinkSource struct {
	fakeSinkSource
	fakeAggLayout
}

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

// TestFragmentPhasesLineAggregateLayout pins the fragment-path equivalent of
// "shuffle partial agg"'s born_flat log line: the "fragment task phases"
// line must carry which construction-time bound (if either) pinned the
// fragment's HashAggregate group index flat.
func TestFragmentPhasesLineAggregateLayout(t *testing.T) {
	cases := []struct {
		name       string
		layout     aggregateLayoutReporter
		wantLayout string
		wantReason string
	}{
		{"pinned by epoch cap", fakeAggLayout{bornFlat: true, reason: "epoch-cap"}, "flat", "epoch-cap"},
		{"pinned by row bound", fakeAggLayout{bornFlat: true, reason: "row-bound"}, "flat", "row-bound"},
		{"not pinned: adaptive", fakeAggLayout{bornFlat: false, reason: ""}, "adaptive", "adaptive"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
			fp := newFragmentProgress(logger, distributed.Task{ID: "t1", StageID: "final_aggregate-7", StageType: "final_aggregate"}, nil)
			fp.aggLayout = tc.layout
			fp.finish(10)
			out := buf.String()
			if !strings.Contains(out, "agg_layout="+tc.wantLayout) {
				t.Errorf("missing agg_layout=%s: %q", tc.wantLayout, out)
			}
			if !strings.Contains(out, "agg_layout_reason="+tc.wantReason) {
				t.Errorf("missing agg_layout_reason=%s: %q", tc.wantReason, out)
			}
		})
	}
}

// TestFragmentPhasesLineOmitsAggregateLayoutForNonAggregateFragments: a join
// or sort-only fragment's phases line must not claim an aggregate layout it
// never decided.
func TestFragmentPhasesLineOmitsAggregateLayoutForNonAggregateFragments(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	fp := newFragmentProgress(logger, distributed.Task{ID: "t1", StageID: "join-6", StageType: "broadcast_join"}, nil)
	fp.finish(10)
	if strings.Contains(buf.String(), "agg_layout") {
		t.Errorf("non-aggregate fragment should not log agg_layout: %q", buf.String())
	}
}

// TestSetAggregateLayoutOnlyForReportingBreakers: setAggregateLayout must
// pick up a HashAggregate-shaped breaker (SinkSource + aggregateLayoutReporter)
// and leave aggLayout nil for a Sort-shaped one (SinkSource alone).
func TestSetAggregateLayoutOnlyForReportingBreakers(t *testing.T) {
	fp := newFragmentProgress(slog.Default(), distributed.Task{ID: "t"}, nil)
	fp.setAggregateLayout(fakeSinkSource{})
	if fp.aggLayout != nil {
		t.Fatalf("a plain SinkSource (Sort's shape) must not set aggLayout, got %v", fp.aggLayout)
	}
	fp.setAggregateLayout(fakeAggSinkSource{fakeAggLayout: fakeAggLayout{bornFlat: true, reason: "row-bound"}})
	if fp.aggLayout == nil {
		t.Fatal("a SinkSource that also reports a layout (HashAggregate's shape) must set aggLayout")
	}
	if !fp.aggLayout.IndexBornFlat() || fp.aggLayout.IndexFlatReason() != "row-bound" {
		t.Errorf("aggLayout = %+v, want bornFlat=true reason=row-bound", fp.aggLayout)
	}
}
