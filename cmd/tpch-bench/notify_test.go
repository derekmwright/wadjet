package main

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	tpch "github.com/derekmwright/wadjet/benchmarks/tpch"
	"github.com/derekmwright/wadjet/internal/benchnotify"
)

type captureEmitter struct {
	events []benchnotify.Event
	err    error
}

func (c *captureEmitter) Emit(_ context.Context, ev benchnotify.Event) error {
	if c.err != nil {
		return c.err
	}
	c.events = append(c.events, ev)
	return nil
}

func installNotifier(tb testing.TB, em benchnotify.Emitter) {
	tb.Helper()
	prev := notifier
	notifier = benchnotify.NewWithEmitter(em, benchnotify.Config{RunID: "20260817-142233"})
	tb.Cleanup(func() { notifier = prev })
}

// silenceStdout swallows runBenchmark's result table so the test output
// stays readable.
func silenceStdout(tb testing.TB) {
	tb.Helper()
	prev := os.Stdout
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		tb.Fatalf("opening %s: %v", os.DevNull, err)
	}
	os.Stdout = devNull
	tb.Cleanup(func() {
		os.Stdout = prev
		devNull.Close()
	})
}

// runBenchmarkSkippingAllBut runs the harness loop over a single query with
// a stubbed query function.
func runBenchmarkSkippingAllBut(tb testing.TB, keep int, runs int, qf queryFn) {
	tb.Helper()
	skip := make(map[int]bool)
	for n := range tpch.TPCHQueries {
		if n != keep {
			skip[n] = true
		}
	}
	silenceStdout(tb)
	runBenchmark(context.Background(), qf, tpch.ScaleFactor(0.01), runs, "", skip, time.Minute)
}

// The event stream a watcher consumes: per-query completions, a
// run_completed per run, then exactly one terminal suite_completed.
func TestRunBenchmarkEmitsLifecycleEvents(t *testing.T) {
	em := &captureEmitter{}
	installNotifier(t, em)

	runBenchmarkSkippingAllBut(t, 1, 2, func(context.Context, string) (int64, string, error) {
		return 42, "", nil
	})

	var kinds []string
	for _, ev := range em.events {
		kinds = append(kinds, ev.Event)
		if ev.RunID != "20260817-142233" {
			t.Errorf("event %+v carries run_id %q", ev, ev.RunID)
		}
		if _, err := time.Parse(time.RFC3339, ev.TS); err != nil {
			t.Errorf("event %+v has non-RFC3339 ts", ev)
		}
	}
	want := []string{
		benchnotify.EventQueryCompleted, benchnotify.EventRunCompleted,
		benchnotify.EventQueryCompleted, benchnotify.EventRunCompleted,
		benchnotify.EventSuiteCompleted,
	}
	if len(kinds) != len(want) {
		t.Fatalf("event sequence = %v, want %v", kinds, want)
	}
	for i := range want {
		if kinds[i] != want[i] {
			t.Fatalf("event sequence = %v, want %v", kinds, want)
		}
	}

	q := em.events[0]
	if q.Query != "Q01" {
		t.Errorf("query = %q, want Q01", q.Query)
	}
	if q.Rows == nil || *q.Rows != 42 {
		t.Errorf("rows = %v, want 42", q.Rows)
	}
	if q.OK == nil || !*q.OK {
		t.Errorf("ok = %v, want true", q.OK)
	}
	if q.WallSeconds == nil {
		t.Error("wall_seconds missing")
	}
	if q.RunIndex != 1 || q.TotalRuns != 2 {
		t.Errorf("run_index/total_runs = %d/%d, want 1/2", q.RunIndex, q.TotalRuns)
	}
	if last := em.events[len(em.events)-1]; last.TotalSeconds == nil {
		t.Error("suite_completed is missing total_seconds")
	}
}

func TestRunBenchmarkReportsFailedQuery(t *testing.T) {
	em := &captureEmitter{}
	installNotifier(t, em)

	runBenchmarkSkippingAllBut(t, 1, 1, func(context.Context, string) (int64, string, error) {
		return 0, "", errors.New("query timeout")
	})

	if len(em.events) == 0 {
		t.Fatal("no events emitted")
	}
	q := em.events[0]
	if q.Event != benchnotify.EventQueryCompleted {
		t.Fatalf("first event = %q", q.Event)
	}
	if q.OK == nil || *q.OK {
		t.Errorf("ok = %v, want false for a failed query", q.OK)
	}
}

// A dead queue must not stop the suite: emission failures are logged by the
// notifier and the benchmark completes normally. The test passes by
// returning — a panicking or aborting emission path would fail it.
func TestRunBenchmarkContinuesWhenEmissionFails(t *testing.T) {
	installNotifier(t, &captureEmitter{err: errors.New("queue unreachable")})

	runBenchmarkSkippingAllBut(t, 1, 1, func(context.Context, string) (int64, string, error) {
		return 1, "", nil
	})
}
