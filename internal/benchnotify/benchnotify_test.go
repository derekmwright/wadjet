package benchnotify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// captureEmitter is the test double: it records every event it is handed and
// optionally fails, so the fire-and-forget path can be observed.
type captureEmitter struct {
	mu     sync.Mutex
	events []Event
	err    error
	calls  int
}

func (c *captureEmitter) Emit(_ context.Context, ev Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.err != nil {
		return c.err
	}
	c.events = append(c.events, ev)
	return nil
}

func (c *captureEmitter) snapshot() ([]Event, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Event(nil), c.events...), c.calls
}

func newTestNotifier(tb testing.TB, em Emitter) (*Notifier, *[]string) {
	tb.Helper()
	var logs []string
	n := NewWithEmitter(em, Config{
		RunID: "20260817-142233",
		Logf:  func(f string, a ...any) { logs = append(logs, fmt.Sprintf(f, a...)) },
	})
	if n == nil {
		tb.Fatal("NewWithEmitter returned nil for a non-nil emitter")
	}
	return n, &logs
}

func TestEventMarshalShape(t *testing.T) {
	tests := []struct {
		name string
		ev   Event
		want map[string]any
	}{
		{
			name: "query completed ok",
			ev: Event{
				Event: EventQueryCompleted, Query: "Q01",
				WallSeconds: Seconds(1500 * time.Millisecond), Rows: Rows(4), OK: OK(true),
			},
			want: map[string]any{
				"run_id": "20260817-142233", "event": "query_completed", "query": "Q01",
				"wall_seconds": 1.5, "rows": float64(4), "ok": true,
				"ts": "2026-08-17T14:22:33Z",
			},
		},
		{
			// ok:false must survive: a plain bool under omitempty would
			// drop exactly the field a watcher cares about.
			name: "query completed failure keeps ok false and zero rows",
			ev: Event{
				Event: EventQueryCompleted, Query: "Q21",
				WallSeconds: Seconds(0), Rows: Rows(0), OK: OK(false),
			},
			want: map[string]any{
				"run_id": "20260817-142233", "event": "query_completed", "query": "Q21",
				"wall_seconds": 0.0, "rows": float64(0), "ok": false,
				"ts": "2026-08-17T14:22:33Z",
			},
		},
		{
			name: "clickbench try",
			ev: Event{
				Event: EventQueryCompleted, Query: "Q07", Try: 1,
				WallSeconds: Seconds(2500 * time.Millisecond), OK: OK(true),
			},
			want: map[string]any{
				"run_id": "20260817-142233", "event": "query_completed", "query": "Q07",
				"try": float64(1), "wall_seconds": 2.5, "ok": true,
				"ts": "2026-08-17T14:22:33Z",
			},
		},
		{
			name: "run completed",
			ev: Event{
				Event: EventRunCompleted, RunIndex: 2, TotalRuns: 3,
				TotalSeconds: Seconds(128800 * time.Millisecond),
			},
			want: map[string]any{
				"run_id": "20260817-142233", "event": "run_completed",
				"run_index": float64(2), "total_runs": float64(3), "total_seconds": 128.8,
				"ts": "2026-08-17T14:22:33Z",
			},
		},
		{
			name: "suite completed with cold and hot",
			ev: Event{
				Event:       EventSuiteCompleted,
				ColdSeconds: Seconds(256500 * time.Millisecond),
				HotSeconds:  Seconds(164400 * time.Millisecond),
			},
			want: map[string]any{
				"run_id": "20260817-142233", "event": "suite_completed",
				"cold_seconds": 256.5, "hot_seconds": 164.4,
				"ts": "2026-08-17T14:22:33Z",
			},
		},
		{
			name: "fatal",
			ev:   Event{Event: EventFatal, Error: "creating S3 store: boom"},
			want: map[string]any{
				"run_id": "20260817-142233", "event": "fatal",
				"error": "creating S3 store: boom", "ts": "2026-08-17T14:22:33Z",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev := tt.ev
			ev.RunID = "20260817-142233"
			ev.TS = "2026-08-17T14:22:33Z"
			body, err := ev.Marshal()
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			if strings.Contains(string(body), "\n") {
				t.Errorf("event body must be a single line: %q", body)
			}
			var got map[string]any
			if err := json.Unmarshal(body, &got); err != nil {
				t.Fatalf("Unmarshal(%s): %v", body, err)
			}
			if len(got) != len(tt.want) {
				t.Errorf("field set = %v, want %v", keys(got), keys(tt.want))
			}
			for k, want := range tt.want {
				if got[k] != want {
					t.Errorf("%s = %#v, want %#v (body %s)", k, got[k], want, body)
				}
			}
		})
	}
}

func keys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestSendStampsRunIDAndTimestamp(t *testing.T) {
	em := &captureEmitter{}
	n, _ := newTestNotifier(t, em)

	n.Send(Event{Event: EventRunStarted, TotalRuns: 3})

	events, _ := em.snapshot()
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	if events[0].RunID != "20260817-142233" {
		t.Errorf("run_id = %q, want the configured id", events[0].RunID)
	}
	if _, err := time.Parse(time.RFC3339, events[0].TS); err != nil {
		t.Errorf("ts %q is not RFC3339: %v", events[0].TS, err)
	}
}

// A nil *Notifier is what an unset --notify-sqs-url produces; every method
// must be a silent no-op rather than a nil dereference.
func TestNilNotifierIsDisabled(t *testing.T) {
	var n *Notifier
	n.Send(Event{Event: EventRunStarted})
	n.Fatal("boom %d", 1)
	if got := n.RunID(); got != "" {
		t.Errorf("RunID() = %q, want empty", got)
	}
}

func TestNewDisabledWithoutQueueURL(t *testing.T) {
	if n := New(context.Background(), Config{}); n != nil {
		t.Fatalf("New with empty QueueURL = %v, want nil (disabled)", n)
	}
}

// The whole point of the fire-and-forget contract: a failing transport logs
// and the caller keeps going.
func TestEmitFailureLogsAndContinues(t *testing.T) {
	em := &captureEmitter{err: errors.New("queue unreachable")}
	n, logs := newTestNotifier(t, em)

	n.Send(Event{Event: EventQueryCompleted, Query: "Q01"})

	_, calls := em.snapshot()
	if calls != 1 {
		t.Fatalf("emitter calls = %d, want 1", calls)
	}
	if len(*logs) != 1 || !strings.Contains((*logs)[0], "queue unreachable") {
		t.Fatalf("logs = %v, want one warning naming the transport error", *logs)
	}
}

// A queue that stays unreachable must not add timeout × events to the run:
// after maxFailures the notifier stops calling the transport.
func TestRepeatedFailuresDisableNotifier(t *testing.T) {
	em := &captureEmitter{err: errors.New("nope")}
	n, logs := newTestNotifier(t, em)

	for i := 0; i < maxFailures+5; i++ {
		n.Send(Event{Event: EventQueryCompleted, Query: "Q01"})
	}

	_, calls := em.snapshot()
	if calls != maxFailures {
		t.Errorf("emitter calls = %d, want %d (disabled after the threshold)", calls, maxFailures)
	}
	var disabledLogged bool
	for _, l := range *logs {
		if strings.Contains(l, "disabling notifications") {
			disabledLogged = true
		}
	}
	if !disabledLogged {
		t.Errorf("logs = %v, want a line announcing that notifications were disabled", *logs)
	}
}

func TestTransientFailureResetsCounter(t *testing.T) {
	em := &captureEmitter{}
	n, _ := newTestNotifier(t, em)

	for i := 0; i < maxFailures*3; i++ {
		em.mu.Lock()
		em.err = errors.New("flaky")
		em.mu.Unlock()
		n.Send(Event{Event: EventQueryCompleted})

		em.mu.Lock()
		em.err = nil
		em.mu.Unlock()
		n.Send(Event{Event: EventQueryCompleted})
	}

	events, _ := em.snapshot()
	if len(events) != maxFailures*3 {
		t.Fatalf("delivered %d events, want %d — a success must reset the failure counter",
			len(events), maxFailures*3)
	}
}

func TestFatalCarriesFormattedMessage(t *testing.T) {
	em := &captureEmitter{}
	n, _ := newTestNotifier(t, em)

	n.Fatal("create table %s: %v", "lineitem", errors.New("nope"))

	events, _ := em.snapshot()
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	if events[0].Event != EventFatal {
		t.Errorf("event = %q, want %q", events[0].Event, EventFatal)
	}
	if events[0].Error != "create table lineitem: nope" {
		t.Errorf("error = %q, want the formatted message", events[0].Error)
	}
}

func TestDefaultRunIDIsResultsTimestampShape(t *testing.T) {
	n := NewWithEmitter(&captureEmitter{}, Config{})
	if n == nil {
		t.Fatal("NewWithEmitter returned nil")
	}
	if _, err := time.Parse("20060102-150405", n.RunID()); err != nil {
		t.Errorf("default RunID %q does not match the results directory format: %v", n.RunID(), err)
	}
}

func TestSendRespectsTimeout(t *testing.T) {
	blocked := make(chan struct{})
	n := NewWithEmitter(emitterFunc(func(ctx context.Context, _ Event) error {
		<-ctx.Done()
		close(blocked)
		return ctx.Err()
	}), Config{Timeout: 20 * time.Millisecond})

	start := time.Now()
	n.Send(Event{Event: EventQueryCompleted})
	<-blocked
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Send blocked for %v; the configured timeout must bound it", elapsed)
	}
}

type emitterFunc func(context.Context, Event) error

func (f emitterFunc) Emit(ctx context.Context, ev Event) error { return f(ctx, ev) }
