package coordinator

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// Regression test for sweep finding #17: readFinalResults decoded every
// worker partial with no size cap — unbounded, uncharged coordinator heap
// on the probe-split merge path. Past the gather budget the query must now
// fail cleanly with an actionable error.
func TestReadFinalResults_BudgetCap(t *testing.T) {
	tracker := NewQueryTracker()
	stages := []physical.Stage{{ID: "s1", Type: "pipeline", Tasks: 1}}
	tracker.Register("q1", "SELECT 1", map[string]*StageInfo{
		"s1": {StageID: "s1", TotalTasks: 1},
	}, []string{"s1"})

	// One inline result of 2048 int64 rows ≈ 16 KiB decoded.
	tracker.RecordResult(distributed.ResultNotification{
		QueryID:    "q1",
		StageID:    "s1",
		TaskID:     "t1",
		Success:    true,
		NumRows:    2048,
		InlineData: wshfInt64Payload(t, 2048, 0),
	})

	c := &Coordinator{
		tracker: tracker,
		logger:  slog.Default(),
		config:  Config{GatherResultBudget: 1024}, // 1 KiB — far below the decoded size
	}
	_, _, _, err := c.readFinalResults(context.Background(), "q1", stages, true)
	if err == nil {
		t.Fatal("expected budget error, got nil")
	}
	if !strings.Contains(err.Error(), "gather budget") {
		t.Fatalf("error %q does not mention the gather budget", err)
	}

	// Same result set under a generous budget decodes fine.
	c.config.GatherResultBudget = 1 << 20
	batches, columns, rows, err := c.readFinalResults(context.Background(), "q1", stages, true)
	if err != nil {
		t.Fatal(err)
	}
	if rows != 2048 || len(batches) == 0 || len(columns) != 1 {
		t.Fatalf("decode under budget: rows=%d batches=%d columns=%v", rows, len(batches), columns)
	}
}
