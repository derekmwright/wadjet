package alerts

import (
	"context"
	"time"
)

// TableSink inserts one row into the alert_history table per fire by
// running INSERT INTO via an injected SQLExecutor.
type TableSink struct {
	Executor SQLExecutor
	// Now is a clock injection seam for tests. Defaults to time.Now.
	Now func() time.Time
	// Results captured from sibling sinks, embedded in the history row.
	// The scheduler sets this before calling Deliver.
	Results []SinkResult
}

func (*TableSink) Name() string { return "table" }

func (s *TableSink) Deliver(ctx context.Context, fire AlertFire) error {
	now := time.Now()
	if s.Now != nil {
		now = s.Now()
	}
	sql, err := BuildHistoryInsertSQL(fire, s.Results, now)
	if err != nil {
		return err
	}
	return s.Executor.Execute(ctx, sql)
}
