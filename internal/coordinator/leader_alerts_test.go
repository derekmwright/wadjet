package coordinator

import (
	"context"
	"testing"
	"time"
)

// TestStartStopAlertScheduler exercises the start/stop lifecycle directly
// (without real leader-election). Asserts Start+Stop completes cleanly
// and calling Stop again with no scheduler running is a no-op.
func TestStartStopAlertScheduler(t *testing.T) {
	c := newTestCoordinator(t)
	c.SetAlertsEnabled(true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c.StartAlertScheduler(ctx)
	if c.alertScheduler == nil {
		t.Fatal("scheduler not started")
	}
	time.Sleep(20 * time.Millisecond)

	c.StopAlertScheduler()
	if c.alertScheduler != nil {
		t.Error("scheduler not cleared after Stop")
	}
	c.StopAlertScheduler() // must be safe
}

// TestFlagGateBlocksScheduler asserts Start is a no-op when alertsEnabled=false.
func TestFlagGateBlocksScheduler(t *testing.T) {
	c := newTestCoordinator(t)
	c.SetAlertsEnabled(false)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c.StartAlertScheduler(ctx)
	if c.alertScheduler != nil {
		t.Error("scheduler started despite disabled flag")
	}
}
