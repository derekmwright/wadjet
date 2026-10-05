// SPDX-License-Identifier: AGPL-3.0-only

package worker

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/expr"
)

// TestTaskStatementClock (#1566): a task carries its statement's clock as
// ONE int64 on the wire (Unix nanoseconds), and the worker answers the clock
// functions with it; a task from a coordinator that predates the field
// (no statement_time) falls back to the task's own start and never panics.
func TestTaskStatementClock(t *testing.T) {
	at := time.Date(2026, 10, 4, 12, 34, 56, 789_000_000, time.UTC)

	data, err := distributed.Marshal(distributed.Task{ID: "t1", SQLText: "SELECT now()", StatementTime: at.UnixNano()})
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	raw, ok := fields["statement_time"]
	if !ok {
		t.Fatalf("no statement_time on the wire: %s", data)
	}
	var n int64
	if err := json.Unmarshal(raw, &n); err != nil || n != at.UnixNano() || strings.ContainsAny(string(raw), `".`) {
		t.Fatalf("statement_time on the wire is %s, want the int64 %d", raw, at.UnixNano())
	}
	// Its size on the wire: the key, the colon, nineteen digits and a comma.
	bare, err := distributed.Marshal(distributed.Task{ID: "t1", SQLText: "SELECT now()"})
	if err != nil {
		t.Fatal(err)
	}
	if n := len(data) - len(bare); n != 37 {
		t.Errorf("statement_time adds %d bytes to the task message, want 37", n)
	}
	var task distributed.Task
	if err := distributed.Unmarshal(data, &task); err != nil {
		t.Fatal(err)
	}
	got, ok := expr.StatementClock(taskStatementContext(context.Background(), task))
	if !ok || !got.Equal(at) {
		t.Errorf("worker statement clock %v (%v), want the task's %v", got, ok, at)
	}

	// An old coordinator's task: no field.
	old := []byte(`{"id":"t2","query_id":"q","stage_id":"s","type":"fragment","sql_text":"SELECT now()"}`)
	var oldTask distributed.Task
	if err := distributed.Unmarshal(old, &oldTask); err != nil {
		t.Fatal(err)
	}
	if oldTask.StatementTime != 0 {
		t.Fatalf("a task with no statement_time decoded %d", oldTask.StatementTime)
	}
	before := time.Now()
	got, ok = expr.StatementClock(taskStatementContext(context.Background(), oldTask))
	after := time.Now()
	if !ok || got.Before(before) || got.After(after) {
		t.Errorf("an unstamped task's clock %v (%v), want the task's own start in [%v, %v]", got, ok, before, after)
	}
	// And the task's own compiles bind it: one value per task, not per row.
	ctx := taskStatementContext(context.Background(), oldTask)
	if expr.WithStatementClock(ctx) == nil {
		t.Error("an unstamped task's context binds no clock")
	}
}
