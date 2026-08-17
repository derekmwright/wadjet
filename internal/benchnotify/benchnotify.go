// Package benchnotify pushes benchmark lifecycle events to an SQS queue so
// an operator can watch a remote run with a blocking receive loop instead of
// grepping a remembered log format over SSM. Producer (the bench binaries)
// and consumer (deploy/benchmark/watch-events.sh) ship in the same commit,
// so the two cannot drift.
//
// # Event schema
//
// One JSON object per SQS message, single line, no envelope:
//
//	{
//	  "run_id":        "20260817-142233",  // results timestamp id: results/<run_id>/
//	  "event":         "run_started" | "query_completed" | "run_completed" |
//	                   "suite_completed" | "fatal",
//	  "query":         "Q01",     // query_completed
//	  "try":           1,         // query_completed, clickbench only (1 = cold)
//	  "wall_seconds":  12.345,    // query_completed
//	  "rows":          4,         // query_completed, tpch only
//	  "ok":            true,      // query_completed
//	  "run_index":     2,         // run_completed (1-based)
//	  "total_runs":    3,         // run_started, run_completed
//	  "total_seconds": 198.4,     // run_completed, suite_completed
//	  "cold_seconds":  256.5,     // suite_completed, clickbench only (sum of try 1)
//	  "hot_seconds":   164.4,     // suite_completed, clickbench only (sum of min(try2,try3))
//	  "error":         "...",     // fatal
//	  "ts":            "2026-08-17T14:22:33Z"  // RFC3339, always present
//	}
//
// Omitted fields are absent from the JSON, not zero-valued — a consumer
// should treat a missing field as "not applicable to this event".
//
// Event order for a TPC-H run: one run_started, then per run N
// query_completed events followed by a run_completed, then one
// suite_completed. For ClickBench: one run_started, then per query one
// query_completed per try, then one suite_completed. Either suite can end
// on a fatal instead — a fatal is always terminal.
//
// # Fire-and-forget
//
// Emission never affects the benchmark. Sends happen outside every timed
// region, carry a short timeout (default 2s), take no retries beyond the
// SDK default, and log a warning on failure. After a few consecutive
// failures the notifier disables itself so an unreachable queue cannot add
// wall time to a long suite. A nil *Notifier is a valid disabled notifier:
// every method is a no-op, which is what an unset --notify-sqs-url yields.
package benchnotify

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Event names.
const (
	EventRunStarted     = "run_started"
	EventQueryCompleted = "query_completed"
	EventRunCompleted   = "run_completed"
	EventSuiteCompleted = "suite_completed"
	EventFatal          = "fatal"
)

// Event is one lifecycle message. See the package comment for which fields
// each event carries; every field but RunID/Event/TS is optional.
type Event struct {
	RunID string `json:"run_id"`
	Event string `json:"event"`

	Query        string   `json:"query,omitempty"`
	Try          int      `json:"try,omitempty"`
	WallSeconds  *float64 `json:"wall_seconds,omitempty"`
	Rows         *int64   `json:"rows,omitempty"`
	OK           *bool    `json:"ok,omitempty"`
	RunIndex     int      `json:"run_index,omitempty"`
	TotalRuns    int      `json:"total_runs,omitempty"`
	TotalSeconds *float64 `json:"total_seconds,omitempty"`
	ColdSeconds  *float64 `json:"cold_seconds,omitempty"`
	HotSeconds   *float64 `json:"hot_seconds,omitempty"`
	Error        string   `json:"error,omitempty"`

	TS string `json:"ts"`
}

// Marshal renders the event as the single-line JSON body that goes on the
// queue.
func (e Event) Marshal() ([]byte, error) { return json.Marshal(e) }

// OK returns a pointer to b, for Event.OK — a plain bool cannot be
// distinguished from "not set" under omitempty, and `"ok": false` is the
// field's most important value.
func OK(b bool) *bool { return &b }

// Seconds returns a pointer to d in seconds, for the duration fields.
func Seconds(d time.Duration) *float64 { s := d.Seconds(); return &s }

// SecondsFloat returns a pointer to v, for callers that already hold
// seconds (the ClickBench per-try triples are float64 seconds).
func SecondsFloat(v float64) *float64 { return &v }

// Rows returns a pointer to n, for Event.Rows (a zero row count is a real
// observation, not an unset field).
func Rows(n int64) *int64 { return &n }

// Emitter delivers one event to the transport. Implementations are
// synchronous; the Notifier bounds them with a timeout.
type Emitter interface {
	Emit(ctx context.Context, ev Event) error
}

// maxFailures is how many consecutive emission errors are tolerated before
// the notifier disables itself. An unreachable queue would otherwise add
// timeout × events to a suite's wall clock.
const maxFailures = 3

// Notifier stamps events with the run id and timestamp and hands them to an
// Emitter. The zero value is unusable; a nil *Notifier is a disabled one.
type Notifier struct {
	runID   string
	em      Emitter
	timeout time.Duration
	logf    func(string, ...any)

	mu       sync.Mutex
	failures int
	disabled bool
}

// Config configures a Notifier.
type Config struct {
	// QueueURL is the SQS queue URL. Empty disables notifications entirely
	// (New returns nil).
	QueueURL string
	// Region overrides the region derived from QueueURL.
	Region string
	// RunID identifies the run; defaults to a UTC timestamp in the same
	// format the bench result directories use (20060102-150405).
	RunID string
	// Timeout bounds a single send. Defaults to 2s.
	Timeout time.Duration
	// Logf receives warnings. Defaults to discarding them.
	Logf func(string, ...any)
}

// NewWithEmitter builds a Notifier over an arbitrary emitter. Used by tests
// and by New once it has an SQS client.
func NewWithEmitter(em Emitter, cfg Config) *Notifier {
	if em == nil {
		return nil
	}
	n := &Notifier{
		runID:   cfg.RunID,
		em:      em,
		timeout: cfg.Timeout,
		logf:    cfg.Logf,
	}
	if n.runID == "" {
		n.runID = time.Now().UTC().Format("20060102-150405")
	}
	if n.timeout <= 0 {
		n.timeout = 2 * time.Second
	}
	if n.logf == nil {
		n.logf = func(string, ...any) {}
	}
	return n
}

// RunID reports the id stamped on every event.
func (n *Notifier) RunID() string {
	if n == nil {
		return ""
	}
	return n.runID
}

// Send stamps and delivers one event. It never blocks longer than the
// configured timeout and never reports an error to the caller: a failed
// send logs a warning, and repeated failures disable the notifier.
func (n *Notifier) Send(ev Event) {
	if n == nil {
		return
	}
	n.mu.Lock()
	if n.disabled {
		n.mu.Unlock()
		return
	}
	n.mu.Unlock()

	ev.RunID = n.runID
	if ev.TS == "" {
		ev.TS = time.Now().UTC().Format(time.RFC3339)
	}

	ctx, cancel := context.WithTimeout(context.Background(), n.timeout)
	err := n.em.Emit(ctx, ev)
	cancel()

	n.mu.Lock()
	defer n.mu.Unlock()
	if err == nil {
		n.failures = 0
		return
	}
	n.failures++
	n.logf("WARNING: benchnotify: sending %s event: %v", ev.Event, err)
	if n.failures >= maxFailures {
		n.disabled = true
		n.logf("WARNING: benchnotify: disabling notifications after %d consecutive failures", n.failures)
	}
}

// Fatal sends a terminal fatal event carrying the formatted message. Call it
// immediately before the process aborts — the send is synchronous, so no
// flush is needed afterwards.
func (n *Notifier) Fatal(format string, args ...any) {
	if n == nil {
		return
	}
	n.Send(Event{Event: EventFatal, Error: fmt.Sprintf(format, args...)})
}
