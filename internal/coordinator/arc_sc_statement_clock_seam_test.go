// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"runtime/debug"
	"sync"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/expr"
)

func init() { scUnboundClockGuard = scNoUnboundClock }

// scNoUnboundClock fails t if any clock function is compiled with no
// statement clock bound while t runs — in this process that is every door:
// pgwire, the embedded engine, the coordinator and its in-process workers.
func scNoUnboundClock(t *testing.T) {
	t.Helper()
	var mu sync.Mutex
	var first string
	n := 0
	restore := expr.SetUnboundClockHookForTest(func(name string) {
		mu.Lock()
		defer mu.Unlock()
		if n == 0 {
			first = name + "\n" + string(debug.Stack())
		}
		n++
	})
	t.Cleanup(func() {
		restore()
		if n > 0 {
			t.Errorf("%d clock-function compiles had no statement clock bound; the first:\n%s", n, first)
		}
	})
}

// TestStampTaskStatementClock: every task a statement publishes carries the
// statement clock its context holds, and a task that already carries one —
// a retry re-publishing the task it was built with — keeps it (#1566).
func TestStampTaskStatementClock(t *testing.T) {
	at := time.Date(2026, 10, 4, 1, 2, 3, 0, time.UTC)
	ctx := expr.ContextWithStatementClock(context.Background(), at)
	task := distributed.Task{ID: "t"}
	stampTaskStatementClock(&task, ctx)
	if task.StatementTime != at.UnixNano() {
		t.Fatalf("stamped %d, want %d", task.StatementTime, at.UnixNano())
	}
	stampTaskStatementClock(&task, expr.ContextWithStatementClock(context.Background(), at.Add(time.Hour)))
	if task.StatementTime != at.UnixNano() {
		t.Errorf("a stamped task was re-stamped: %d, want %d", task.StatementTime, at.UnixNano())
	}
	bare := distributed.Task{ID: "u"}
	stampTaskStatementClock(&bare, context.Background())
	if bare.StatementTime != 0 {
		t.Errorf("a context with no statement clock stamped %d", bare.StatementTime)
	}
}
