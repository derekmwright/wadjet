package main

import (
	"context"
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/benchnotify"
)

type captureEmitter struct{ events []benchnotify.Event }

func (c *captureEmitter) Emit(_ context.Context, ev benchnotify.Event) error {
	c.events = append(c.events, ev)
	return nil
}

// installNotifier points the package-level notifier at a capture double for
// the duration of the test.
func installNotifier(tb testing.TB) *captureEmitter {
	tb.Helper()
	em := &captureEmitter{}
	prev := notifier
	notifier = benchnotify.NewWithEmitter(em, benchnotify.Config{RunID: "20260817-142233"})
	tb.Cleanup(func() { notifier = prev })
	return em
}

func TestNotifyTriesEmitsOnePerTry(t *testing.T) {
	em := installNotifier(t)

	// The middle try failed: -1 is the sentinel the results JSON renders
	// as null.
	notifyTries(7, []float64{2.5, -1, 0.75})

	if len(em.events) != 3 {
		t.Fatalf("got %d events, want 3", len(em.events))
	}
	for i, ev := range em.events {
		if ev.Event != benchnotify.EventQueryCompleted {
			t.Errorf("event %d = %q, want %q", i, ev.Event, benchnotify.EventQueryCompleted)
		}
		if ev.Query != "Q07" {
			t.Errorf("event %d query = %q, want Q07", i, ev.Query)
		}
		if ev.Try != i+1 {
			t.Errorf("event %d try = %d, want %d", i, ev.Try, i+1)
		}
		if ev.RunID != "20260817-142233" {
			t.Errorf("event %d run_id = %q", i, ev.RunID)
		}
	}
	if ev := em.events[1]; ev.OK == nil || *ev.OK {
		t.Errorf("failed try ok = %v, want false", ev.OK)
	}
	if ev := em.events[1]; ev.WallSeconds != nil {
		t.Errorf("failed try wall_seconds = %v, want absent", *ev.WallSeconds)
	}
	if ev := em.events[0]; ev.WallSeconds == nil || *ev.WallSeconds != 2.5 {
		t.Errorf("cold try wall_seconds = %v, want 2.5", ev.WallSeconds)
	}
}

// A nil notifier is the disabled default (--notify-sqs-url unset); the
// suite must run identically.
func TestNotifyTriesWithoutNotifier(t *testing.T) {
	prev := notifier
	notifier = nil
	t.Cleanup(func() { notifier = prev })
	notifyTries(1, []float64{1, 2, 3})
}

func TestColdHotTotals(t *testing.T) {
	tests := []struct {
		name             string
		results          [][]float64
		cold, hot, total float64
	}{
		{
			// hot = min(try2, try3), matching benchmarks/clickbench/rank.py.
			name:    "official triples",
			results: [][]float64{{10, 4, 3}, {6, 2, 5}},
			cold:    16, hot: 5, total: 30,
		},
		{
			name:    "failed tries are skipped, not zeroed",
			results: [][]float64{{10, -1, 3}, {-1, -1, -1}},
			cold:    10, hot: 3, total: 13,
		},
		{
			name:    "single try suite has no hot side",
			results: [][]float64{{8}, {2}},
			cold:    10, hot: 0, total: 10,
		},
		{name: "empty", results: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cold, hot, total := coldHotTotals(tt.results)
			if !nearly(cold, tt.cold) || !nearly(hot, tt.hot) || !nearly(total, tt.total) {
				t.Errorf("cold/hot/total = %v/%v/%v, want %v/%v/%v",
					cold, hot, total, tt.cold, tt.hot, tt.total)
			}
		})
	}
}

func nearly(a, b float64) bool { return math.Abs(a-b) < 1e-9 }
