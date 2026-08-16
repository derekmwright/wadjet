package worker

import (
	"os"

	"context"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The background-upload gate must run uploadSlotsBusy-wide while the
// foreground probe reports activity and open back up to uploadSlotsIdle
// when it clears (SF100 Q06 2026-08-05: 8 background compress+PUT
// streams starved the next query's scans to ~19s on a 2s plan).
func TestUploadSlotGateYieldsToForeground(t *testing.T) {
	m := newUploadManager(nil, nil, nil)
	busy := atomic.Bool{}
	busy.Store(true)
	m.SetForegroundProbe(busy.Load)
	m.NoteForegroundQuery("test-root") // open the protection window

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var cur, peak atomic.Int64
	var wg sync.WaitGroup
	release := make(chan struct{})
	for i := 0; i < uploadSlotsIdle; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if !m.acquireSlot(ctx, nil, nil) {
				return
			}
			defer m.releaseSlot()
			n := cur.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			<-release
			cur.Add(-1)
		}()
	}
	// Let the gate settle while busy: only uploadSlotsBusy should hold slots.
	time.Sleep(300 * time.Millisecond)
	if got := peak.Load(); got > uploadSlotsBusy {
		t.Fatalf("busy peak concurrency = %d, want <= %d", got, uploadSlotsBusy)
	}

	// Foreground clears: the remaining jobs should widen to uploadSlotsIdle.
	busy.Store(false)
	time.Sleep(300 * time.Millisecond)
	if got := peak.Load(); got != uploadSlotsIdle {
		t.Fatalf("idle peak concurrency = %d, want %d", got, uploadSlotsIdle)
	}
	close(release)
	wg.Wait()

	if m.UploadYieldNs() == 0 {
		t.Fatal("yieldNs should record time spent waiting on the busy gate")
	}
}

// Cancellation must abort waiters promptly (query teardown while yielding).
func TestUploadSlotGateCancel(t *testing.T) {
	m := newUploadManager(nil, nil, nil)
	m.SetForegroundProbe(func() bool { return true })
	m.NoteForegroundQuery("test-root")
	// Fill the busy width.
	ctx := context.Background()
	for i := 0; i < uploadSlotsBusy; i++ {
		if !m.acquireSlot(ctx, nil, nil) {
			t.Fatal("initial acquire failed")
		}
	}
	cctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool, 1)
	go func() { done <- m.acquireSlot(cctx, nil, nil) }()
	cancel()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("cancelled acquire must return false")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled acquire did not return")
	}
}

// Urgent roots (demand-released) and long-waiting jobs must escape the
// busy width — the v1 flat gate starved the backlog for whole suites.
func TestUploadSlotGateUrgencyAndBoundedYield(t *testing.T) {
	m := newUploadManager(nil, nil, nil)
	m.SetForegroundProbe(func() bool { return true })
	m.NoteForegroundQuery("test-root")
	ctx := context.Background()
	// Saturate the busy width.
	for i := 0; i < uploadSlotsBusy; i++ {
		if !m.acquireSlot(ctx, nil, nil) {
			t.Fatal("initial acquire failed")
		}
	}
	// Non-urgent job stays gated...
	blocked := make(chan bool, 1)
	go func() {
		c, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
		defer cancel()
		blocked <- m.acquireSlot(c, nil, nil)
	}()
	if ok := <-blocked; ok {
		t.Fatal("non-urgent job passed a saturated busy gate")
	}
	// ...an urgent root's job does not.
	qs := &queryUploadState{}
	qs.urgent.Store(true)
	done := make(chan bool, 1)
	go func() { done <- m.acquireSlot(ctx, qs, nil) }()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("urgent acquire returned false")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("urgent job did not bypass the busy gate")
	}
}

// The v3 epoch clock: the busy width applies only inside the protection
// window a NEW root query opens; the window expires (drains resume full
// width even while busy) and repeat tasks of the same root do not
// re-open it.
func TestUploadSlotGateEpochWindow(t *testing.T) {
	old := uploadProtectMs
	uploadProtectMs = 150
	defer func() { uploadProtectMs = old }()

	m := newUploadManager(nil, nil, nil)
	m.SetForegroundProbe(func() bool { return true })

	// No window opened yet: busy alone must NOT throttle.
	if got := m.slotCap(); got != uploadSlotsIdle {
		t.Fatalf("no-window cap = %d, want idle %d", got, uploadSlotsIdle)
	}
	m.NoteForegroundQuery("root-a")
	if got := m.slotCap(); got != uploadSlotsBusy {
		t.Fatalf("in-window cap = %d, want busy %d", got, uploadSlotsBusy)
	}
	time.Sleep(200 * time.Millisecond) // window expires
	if got := m.slotCap(); got != uploadSlotsIdle {
		t.Fatalf("expired-window cap = %d, want idle %d", got, uploadSlotsIdle)
	}
	// Same root again: no new window.
	m.NoteForegroundQuery("root-a")
	if got := m.slotCap(); got != uploadSlotsIdle {
		t.Fatalf("repeat-root cap = %d, want idle %d", got, uploadSlotsIdle)
	}
	// A NEW root re-opens it.
	m.NoteForegroundQuery("root-b")
	if got := m.slotCap(); got != uploadSlotsBusy {
		t.Fatalf("new-root cap = %d, want busy %d", got, uploadSlotsBusy)
	}
}

// v4: an IN-FLIGHT upload stream must freeze at the chunk gate while a
// protection window is open — v3 gated only admission, so jobs admitted
// during idle ran compression+PUT full-speed through the next query's
// window (the residual cold coin-flip).
func TestUploadChunkGatePausesInFlight(t *testing.T) {
	m := newUploadManager(nil, nil, nil)
	busy := atomic.Bool{}
	busy.Store(true)
	m.SetForegroundProbe(busy.Load)
	m.NoteForegroundQuery("test-root")
	ctx := context.Background()

	// Fill the progress quota with older in-flight jobs; the job under
	// test is then outside the oldest-busyWidth set and must freeze.
	for i := 0; i < uploadSlotsBusy; i++ {
		m.registerJob()
	}
	var jobYield int64
	done := make(chan bool, 1)
	go func() { done <- m.chunkGate(ctx, nil, m.registerJob(), &jobYield) }()
	select {
	case <-done:
		t.Fatal("chunk gate passed while the protection window was open")
	case <-time.After(300 * time.Millisecond):
	}
	busy.Store(false) // foreground clears → in-flight stream resumes
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("chunk gate returned false after foreground cleared")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("chunk gate did not release after foreground cleared")
	}
	if m.UploadPauseNs() == 0 || jobYield == 0 {
		t.Fatalf("pause accounting missing: mgr=%d job=%d", m.UploadPauseNs(), jobYield)
	}
}

// Urgent roots and the job-total hard cap both escape the chunk gate.
func TestUploadChunkGateEscapes(t *testing.T) {
	m := newUploadManager(nil, nil, nil)
	m.SetForegroundProbe(func() bool { return true })
	m.NoteForegroundQuery("test-root")
	ctx := context.Background()

	qs := &queryUploadState{}
	qs.urgent.Store(true)
	var y1 int64
	done := make(chan bool, 1)
	go func() { done <- m.chunkGate(ctx, qs, m.registerJob(), &y1) }()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("urgent chunk gate returned false")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("urgent root paused at the chunk gate")
	}

	// Job that already burned its yield budget passes immediately.
	spent := uploadHardCapMs * int64(time.Millisecond)
	go func() { done <- m.chunkGate(ctx, nil, m.registerJob(), &spent) }()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("hard-capped chunk gate returned false")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("job past the hard cap paused at the chunk gate")
	}
}

// Kill switch pins pre-v4 behavior: no in-flight pausing.
func TestUploadChunkGateKillSwitch(t *testing.T) {
	old := uploadQoSEnabled
	uploadQoSEnabled = false
	defer func() { uploadQoSEnabled = old }()
	m := newUploadManager(nil, nil, nil)
	m.SetForegroundProbe(func() bool { return true })
	m.NoteForegroundQuery("test-root")
	done := make(chan bool, 1)
	go func() { done <- m.chunkGate(context.Background(), nil, m.registerJob(), nil) }()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("gate returned false under kill switch")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("kill switch must disable in-flight pausing")
	}
}

// End-to-end through uploadOnce: a governed background job's PUT bytes
// stall during the window and complete after; the same file uploaded by
// the synchronous path (nil yield budget) never pauses.
func TestUploadOnceGovernedVsSync(t *testing.T) {
	oldChunk := uploadChunkBytes
	uploadChunkBytes = 1024
	defer func() { uploadChunkBytes = oldChunk }()

	dir := t.TempDir()
	src := dir + "/part.wshf"
	if err := os.WriteFile(src, make([]byte, 64*1024), 0o644); err != nil {
		t.Fatal(err)
	}
	store := objstore.NewMemStore()
	_ = store.MakeBucket(context.Background(), "b")
	m := newUploadManager(store, nil, nil)
	busy := atomic.Bool{}
	busy.Store(true)
	m.SetForegroundProbe(busy.Load)
	m.NoteForegroundQuery("test-root")

	// Synchronous path (nil budget): completes despite the open window.
	syncDone := make(chan error, 1)
	go func() {
		syncDone <- m.uploadOnce(context.Background(), nil, 0, uploadJob{
			bucket: "b", key: "sync", srcPath: src, tmpDir: dir,
		}, nil)
	}()
	select {
	case err := <-syncDone:
		if err != nil {
			t.Fatalf("sync upload failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("synchronous upload paused at the chunk gate")
	}

	// Governed background job OUTSIDE the progress quota: stalls while
	// the window is open.
	for i := 0; i < uploadSlotsBusy; i++ {
		m.registerJob()
	}
	var jobYield int64
	bgDone := make(chan error, 1)
	go func() {
		bgDone <- m.uploadOnce(context.Background(), nil, m.registerJob(), uploadJob{
			bucket: "b", key: "bg", srcPath: src, tmpDir: dir,
		}, &jobYield)
	}()
	select {
	case <-bgDone:
		t.Fatal("governed upload completed through an open protection window")
	case <-time.After(300 * time.Millisecond):
	}
	busy.Store(false)
	select {
	case err := <-bgDone:
		if err != nil {
			t.Fatalf("governed upload failed after window: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("governed upload did not resume after foreground cleared")
	}
	if _, _, err := store.Get(context.Background(), "b", "bg"); err != nil {
		t.Fatalf("governed upload key missing: %v", err)
	}
}

// The progress quota: the OLDEST busyWidth in-flight jobs advance
// through an open window (v3's throughput guarantee, preserved).
func TestUploadChunkGateQuotaOldestAdvance(t *testing.T) {
	m := newUploadManager(nil, nil, nil)
	m.SetForegroundProbe(func() bool { return true })
	m.NoteForegroundQuery("test-root")
	oldest := m.registerJob()
	for i := 0; i < uploadSlotsIdle; i++ {
		m.registerJob() // younger backlog
	}
	done := make(chan bool, 1)
	go func() { done <- m.chunkGate(context.Background(), nil, oldest, nil) }()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("oldest job's gate returned false")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("oldest in-flight job must advance through the window")
	}
}

// The window protects a query from the PREVIOUS queries' drains — the
// window-opening root's own uploads are exempt from the in-flight freeze.
func TestUploadChunkGateSameRootExempt(t *testing.T) {
	m := newUploadManager(nil, nil, nil)
	m.SetForegroundProbe(func() bool { return true })
	m.NoteForegroundQuery("current-root")
	for i := 0; i < uploadSlotsBusy; i++ {
		m.registerJob() // older backlog filling the quota
	}
	qs := m.queryState("current-root")
	done := make(chan bool, 1)
	go func() { done <- m.chunkGate(context.Background(), qs, m.registerJob(), nil) }()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("same-root gate returned false")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the window root's own uploads must not freeze")
	}
}

// Regression (SF100 pair 20260808-0224): admission waits and in-flight
// pauses have SEPARATE budgets — a job that burned its full admission
// budget must still pause at the chunk gate. runJob wires distinct
// counters; this pins the gate-level behavior a shared budget broke.
func TestUploadChunkGateBudgetSeparateFromAdmission(t *testing.T) {
	m := newUploadManager(nil, nil, nil)
	m.SetForegroundProbe(func() bool { return true })
	m.NoteForegroundQuery("test-root")
	for i := 0; i < uploadSlotsBusy; i++ {
		m.registerJob()
	}
	// Fresh pause budget — must pause even though the job "already
	// waited" its full admission budget (tracked separately by runJob).
	var pauseBudget int64
	done := make(chan bool, 1)
	go func() { done <- m.chunkGate(context.Background(), nil, m.registerJob(), &pauseBudget) }()
	select {
	case <-done:
		t.Fatal("chunk gate must pause on a fresh pause budget")
	case <-time.After(300 * time.Millisecond):
	}
	m.protectedUntil.Store(0) // close the window; the gate releases
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("gate returned false")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("gate did not release after the window closed")
	}
}
