package worker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
)

// uploadRetryAttempts bounds background upload retries per file. Failures
// past the cap mark the task's UploadComplete as Failed — the keys stay
// non-durable and the coordinator's ErrInputLost classification is the
// backstop if the producer also dies.
const uploadRetryAttempts = 3

// Background-upload QoS (docs/design/upload-foreground-qos.md): each job
// burns a core on s2 compression plus NVMe read-back plus PUT bandwidth,
// and at the old fixed concurrency of 8 a heavy query's drain starved the
// NEXT query's scans (SF100 Q06: 2s clean vs ~19s when the predecessor's
// drain overlapped — the dominant short-query cold-variance signature,
// 2026-08-05). Foreground tasks take priority: full width only while the
// worker is otherwise idle. Kill switch WADJET_UPLOAD_QOS=0 pins the idle
// width unconditionally.
const (
	uploadSlotsIdle  = 8
	uploadSlotsBusy  = 2
	uploadSlotPollMs = 50
)

// uploadChunkBytes is the granularity at which IN-FLIGHT upload streams
// consult the foreground gate (QoS v4). v3 gated only slot ACQUISITION:
// up to 8 jobs admitted during idle kept compressing and PUTting at full
// speed straight through the next query's protection window — the
// residual cold coin-flip (Q06 10.7s midpoint in the v3 arm; Q04/Q05
// same-plan cold swings every window since). A var so tests can shrink
// it; semantically constant.
var uploadChunkBytes = 1 << 20

// Yield-clock tunables (vars so tests can compress time; semantically
// constants). Three generations of this gate, each SF100-measured
// (docs/design/upload-foreground-qos.md):
//
//	v1 flat busy-width — backlog snowballed, durability lagged minutes,
//	  Q06/Q08 steady FAILED, 28 uploads/worker abandoned.
//	v2 per-job 10s wait cap — the budget burns while the PRODUCING query
//	  is still running, so protection for the NEXT query was a coin flip
//	  (Q06 cold 2.6s vs 20.6s across arms).
//	v3 (this): the clock references the FOREGROUND EPOCH — each new root
//	  query's first task opens a protection window during which drains
//	  run at the busy width; outside it they run full width even while
//	  busy. Every query gets the same head start; long queries release
//	  the drain after the window; per-job uploadHardCapMs guarantees
//	  progress even under a pathological stream of new queries.
var (
	uploadProtectMs int64 = 10_000
	uploadHardCapMs int64 = 60_000
)

// uploadRetryBackoff is the base delay between background upload retries
// (doubled per attempt). Background work — latency here is invisible to
// the query unless a consumer's S3 fallthrough is waiting on it.
const uploadRetryBackoff = 500 * time.Millisecond

// uploadManager runs the Phase-B background S3 uploads of stage-output
// files that were already finalized locally and adopted into the
// LocalStageCache (docs/design/streaming-exchange.md §5). One per Executor,
// dormant unless tasks arrive with AsyncUpload set.
//
// Cancellation: uploads are grouped per ROOT query ID; the worker's
// query-complete/cancel broadcast handlers call CancelQuery, aborting
// pending uploads for terminal queries — files a finished query never
// fetched from S3 never reach S3 at all. Residual race (a PUT landing
// after the coordinator's scratch purge) is bounded by one in-flight file
// per worker and reclaimed by the coordinator's periodic CleanStale GC.
//
// Durability policy (docs/design/shuffle-durability.md): each task carries
// an UploadPolicy. Eager jobs start immediately (the above). Lazy jobs are
// queued unstarted on the root's queryUploadState and run only when the
// root is released — by a coordinator SubjectUploadRelease broadcast (a
// consumer or the coordinator itself needs the durable copy) or by Flush
// at worker drain; jobs still queued when the query turns terminal are
// elided, never PUT. Off-policy jobs are elided at StartTask.
type uploadManager struct {
	store  objstore.Store
	nc     *nats.Conn
	logger *slog.Logger

	// Adaptive upload concurrency: uploadSlotsIdle wide while the worker
	// has no foreground task, uploadSlotsBusy while it does (foreground
	// scans must not fight 8 background compress+PUT streams for cores
	// and NVMe). busy is nil-safe (nil = never busy = fixed idle width).
	slots   atomic.Int64
	busy    func() bool
	yieldNs atomic.Int64 // time jobs spent waiting on the busy-width gate
	pauseNs atomic.Int64 // time IN-FLIGHT streams spent paused mid-chunk (v4)

	// v4 progress quota: during an open protection window the OLDEST
	// uploadSlotsBusy in-flight jobs keep advancing and the rest freeze
	// at their next chunk boundary. Oldest-first is deterministic (no
	// token churn, no round-robin burst where all 8 s2 streams advance
	// at 1/4 speed) and preserves v3's progress guarantee: at least
	// busyWidth streams are always draining.
	jobSeq     atomic.Int64
	activeMu   sync.Mutex
	activeJobs map[int64]struct{}

	// Foreground-epoch protection window (v3): protectedUntil holds the
	// unixnano until which drains yield; NoteForegroundQuery extends it
	// when a NEW root query's first task arrives. seenRoots dedupes so a
	// query's hundreds of tasks open exactly one window. windowRoot names
	// the root whose window is open: that query's OWN uploads are exempt
	// from the v4 in-flight freeze (the window protects a query from the
	// PREVIOUS queries' drains, not from its own critical-path uploads —
	// same-query consumers may need those durable copies, and freezing
	// them just converts progress into demand-release round-trips; SF1
	// q18 +6s when this exemption was missing).
	protectedUntil atomic.Int64
	windowRoot     atomic.Value // string
	seenMu         sync.Mutex
	seenRoots      map[string]struct{}

	mu      sync.Mutex
	queries map[string]*queryUploadState

	// Observability counters.
	completed      atomic.Int64 // files uploaded in the background
	cancelledFiles atomic.Int64 // uploads aborted by query completion
	failedFiles    atomic.Int64 // uploads abandoned after retries
	completedBytes atomic.Int64 // wire bytes PUT (post-compression)
	cancelledBytes atomic.Int64 // local file bytes of aborted uploads (pre-compression)
	elidedFiles    atomic.Int64 // lazy/off uploads that never started (query finished first)
	elidedBytes    atomic.Int64 // local file bytes of elided uploads (pre-compression)
}

type queryUploadState struct {
	root     string
	ctx      context.Context
	cancel   context.CancelFunc
	inflight sync.WaitGroup

	// urgent marks a demand signal (SubjectUploadRelease: a consumer or
	// the coordinator needs this root's durable copies NOW). Jobs for an
	// urgent root bypass the foreground-yield gate entirely.
	urgent atomic.Bool

	mu       sync.Mutex
	pending  []pendingUpload // lazy jobs queued unstarted
	released bool            // demand signal seen: later lazy jobs start immediately
}

// pendingUpload is one queued lazy job plus the task tracker that publishes
// its UploadComplete once the task's last job settles.
type pendingUpload struct {
	tu *taskUploads
	j  uploadJob
}

func newUploadManager(store objstore.Store, nc *nats.Conn, logger *slog.Logger) *uploadManager {
	if logger == nil {
		logger = slog.Default()
	}
	return &uploadManager{
		store:      store,
		nc:         nc,
		logger:     logger,
		queries:    make(map[string]*queryUploadState),
		seenRoots:  make(map[string]struct{}),
		activeJobs: make(map[int64]struct{}),
	}
}

// NoteForegroundQuery opens (or extends) the drain-protection window when
// a root query's FIRST task arrives. Later tasks of the same root are
// no-ops — one head start per query, so long queries release the drain
// after uploadProtectMs. Call from Executor.Execute.
func (m *uploadManager) NoteForegroundQuery(root string) {
	if m == nil || root == "" {
		return
	}
	m.seenMu.Lock()
	if _, ok := m.seenRoots[root]; ok {
		m.seenMu.Unlock()
		return
	}
	// Bound the dedupe set; roots are transient per-query IDs.
	if len(m.seenRoots) > 1024 {
		m.seenRoots = make(map[string]struct{}, 64)
	}
	m.seenRoots[root] = struct{}{}
	m.seenMu.Unlock()
	m.windowRoot.Store(root)
	until := time.Now().Add(time.Duration(uploadProtectMs) * time.Millisecond).UnixNano()
	for {
		cur := m.protectedUntil.Load()
		if until <= cur || m.protectedUntil.CompareAndSwap(cur, until) {
			return
		}
	}
}

// SetForegroundProbe wires the busy signal for the adaptive width. Call
// before the first StartTask (executor construction) — jobs read it
// per-acquire, nil means never busy.
func (m *uploadManager) SetForegroundProbe(busy func() bool) {
	m.busy = busy
}

// slotCap returns the current width of the upload gate: the busy width
// only while a foreground task is running AND a query's protection
// window is open.
func (m *uploadManager) slotCap() int64 {
	if m.busy != nil && uploadQoSEnabled && m.busy() &&
		time.Now().UnixNano() < m.protectedUntil.Load() {
		return uploadSlotsBusy
	}
	return uploadSlotsIdle
}

// uploadQoSEnabled gates the busy-width reduction. WADJET_UPLOAD_QOS=0
// pins the idle width (pre-QoS behavior) for A/B arms.
var uploadQoSEnabled = os.Getenv("WADJET_UPLOAD_QOS") != "0"

// acquireSlot blocks until a slot under the CURRENT width is free or ctx
// ends. Polling (50ms) is fine here: this is background work, and the
// width itself changes with foreground activity so a condvar would need
// external kicks anyway. Progress is guaranteed — the busy width is ≥1,
// and two escapes bound the yield: an URGENT root (demand-released — a
// consumer needs the durable copy) and a JOB-TOTAL yield past
// uploadHardCapMs (jobYieldNs accumulates across this gate and the v4
// in-flight chunk gate) both escalate to the idle width.
func (m *uploadManager) acquireSlot(ctx context.Context, qs *queryUploadState, jobYieldNs *int64) bool {
	var scratch int64
	if jobYieldNs == nil {
		jobYieldNs = &scratch
	}
	waited := int64(0)
	for {
		cap := m.slotCap()
		if (qs != nil && qs.urgent.Load()) || *jobYieldNs+waited >= uploadHardCapMs*int64(time.Millisecond) {
			cap = uploadSlotsIdle
		}
		cur := m.slots.Load()
		if cur < cap && m.slots.CompareAndSwap(cur, cur+1) {
			if waited > 0 {
				m.yieldNs.Add(waited)
				*jobYieldNs += waited
			}
			return true
		}
		select {
		case <-time.After(uploadSlotPollMs * time.Millisecond):
			waited += int64(uploadSlotPollMs) * int64(time.Millisecond)
		case <-ctx.Done():
			return false
		}
	}
}

// registerJob assigns an in-flight job its age rank for the v4 progress
// quota; unregisterJob releases it (deferred in runJob).
func (m *uploadManager) registerJob() int64 {
	seq := m.jobSeq.Add(1)
	m.activeMu.Lock()
	m.activeJobs[seq] = struct{}{}
	m.activeMu.Unlock()
	return seq
}

func (m *uploadManager) unregisterJob(seq int64) {
	m.activeMu.Lock()
	delete(m.activeJobs, seq)
	m.activeMu.Unlock()
}

// inProgressQuota reports whether seq is among the uploadSlotsBusy OLDEST
// in-flight jobs — the ones allowed to keep advancing during an open
// protection window. O(active) with active ≤ idle width + lane slack.
func (m *uploadManager) inProgressQuota(seq int64) bool {
	m.activeMu.Lock()
	defer m.activeMu.Unlock()
	older := 0
	for s := range m.activeJobs {
		if s < seq {
			older++
			if older >= uploadSlotsBusy {
				return false
			}
		}
	}
	return true
}

// chunkGate is the v4 in-flight pause point: between chunks, a running
// upload stream stops while a foreground protection window is open AND
// the job is outside the oldest-busyWidth progress quota — freezing both
// the s2 compression (its reads stop) and the PUT body (the connection
// idles; ResponseHeaderTimeout is 30m, far above any bounded pause). The
// quota preserves v3's throughput guarantee (busyWidth streams always
// draining); the freeze removes v3's residual (the OTHER admitted-while-
// idle jobs no longer burn cores/wire through the window). Escapes
// mirror acquireSlot: urgent roots never pause, and the JOB-TOTAL yield
// budget (uploadHardCapMs, shared with the admission gate) guarantees
// progress under any query arrival pattern. Returns false only when ctx
// ends.
func (m *uploadManager) chunkGate(ctx context.Context, qs *queryUploadState, seq int64, jobYieldNs *int64) bool {
	var scratch int64
	if jobYieldNs == nil {
		jobYieldNs = &scratch
	}
	waited := int64(0)
	for {
		windowRoot, _ := m.windowRoot.Load().(string)
		pause := uploadQoSEnabled && m.busy != nil && m.busy() &&
			time.Now().UnixNano() < m.protectedUntil.Load() &&
			!(qs != nil && (qs.urgent.Load() || qs.root == windowRoot)) &&
			*jobYieldNs+waited < uploadHardCapMs*int64(time.Millisecond) &&
			!m.inProgressQuota(seq)
		if !pause {
			if waited > 0 {
				m.pauseNs.Add(waited)
				*jobYieldNs += waited
			}
			return true
		}
		select {
		case <-time.After(uploadSlotPollMs * time.Millisecond):
			waited += int64(uploadSlotPollMs) * int64(time.Millisecond)
		case <-ctx.Done():
			return false
		}
	}
}

// governedReader threads chunkGate into an upload stream: every
// uploadChunkBytes of source reads re-consults the foreground gate.
// Wrapping the SOURCE governs everything downstream of it — the s2
// writer and the PUT body advance only as fast as their reads.
type governedReader struct {
	ctx        context.Context
	m          *uploadManager
	qs         *queryUploadState
	seq        int64
	r          io.Reader
	budget     int
	jobYieldNs *int64
}

func (g *governedReader) Read(p []byte) (int, error) {
	if g.budget <= 0 {
		if !g.m.chunkGate(g.ctx, g.qs, g.seq, g.jobYieldNs) {
			return 0, g.ctx.Err()
		}
		g.budget = uploadChunkBytes
	}
	if len(p) > g.budget {
		p = p[:g.budget]
	}
	n, err := g.r.Read(p)
	g.budget -= n
	return n, err
}

func (m *uploadManager) releaseSlot() { m.slots.Add(-1) }

// UploadYieldNs returns cumulative time background jobs spent yielding to
// foreground work. Observability for QoS A/B arms.
func (m *uploadManager) UploadYieldNs() int64 {
	if m == nil {
		return 0
	}
	return m.yieldNs.Load()
}

// UploadPauseNs returns cumulative time IN-FLIGHT upload streams spent
// paused at the v4 chunk gate — the engagement marker distinguishing
// in-flight freezes from admission-gate yields (upload_pause_ms).
func (m *uploadManager) UploadPauseNs() int64 {
	if m == nil {
		return 0
	}
	return m.pauseNs.Load()
}

// uploadJob is one file to land in S3: srcPath is a LocalStageCache-owned
// local file (the manager never deletes it — the cache does, on query
// cleanup), key/bucket the durable destination. compress applies the same
// ≥10% s2 heuristic as the synchronous path, staging the compressed
// artifact as a temp file next to tmpDir.
type uploadJob struct {
	bucket   string
	key      string
	srcPath  string
	compress bool
	tmpDir   string // where compressed temps go; must outlive the upload (NOT the task spill dir)
	size     int64  // srcPath size at job creation; 0 = unknown (stat fallback)
}

// taskUploads tracks one task's background uploads and publishes
// UploadComplete when the last one settles.
type taskUploads struct {
	m           *uploadManager
	root        string
	taskID      string
	workerID    string
	keys        []string
	remaining   atomic.Int64
	anyFailed   atomic.Bool
	anyCanceled atomic.Bool
}

// queryState returns (creating if needed) the cancel scope for a root query.
func (m *uploadManager) queryState(root string) *queryUploadState {
	m.mu.Lock()
	defer m.mu.Unlock()
	qs := m.queries[root]
	if qs == nil {
		ctx, cancel := context.WithCancel(context.Background())
		qs = &queryUploadState{root: root, ctx: ctx, cancel: cancel}
		m.queries[root] = qs
	}
	return qs
}

// CancelQuery aborts pending uploads for a terminal query and drops its
// scope. Queued lazy jobs are elided — counted as PUT work the query never
// needed. Safe for unknown roots, repeated calls, and nil receivers.
func (m *uploadManager) CancelQuery(root string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	qs := m.queries[root]
	delete(m.queries, root)
	m.mu.Unlock()
	if qs != nil {
		qs.cancel()
		m.elidePending(qs)
	}
}

// ReleaseQuery starts every queued lazy upload for a root query and marks
// the root released so later lazy jobs start immediately. Triggered by the
// coordinator's SubjectUploadRelease broadcast. No-op for unknown roots.
func (m *uploadManager) ReleaseQuery(root string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	qs := m.queries[root]
	m.mu.Unlock()
	if qs != nil {
		m.releasePending(qs)
	}
}

// releasePending flips the root to released and starts its queued jobs.
// The release IS a demand signal, so the root also turns urgent: its jobs
// (queued lazy AND already-waiting eager) bypass the foreground-yield gate.
func (m *uploadManager) releasePending(qs *queryUploadState) {
	qs.urgent.Store(true)
	qs.mu.Lock()
	qs.released = true
	pending := qs.pending
	qs.pending = nil
	qs.mu.Unlock()
	for _, p := range pending {
		m.startJob(qs, p.tu, p.j)
	}
}

// elidePending discards a terminal root's queued jobs, counting them on the
// elided side of the upload ledger. Their taskUploads never reach jobDone,
// so no UploadComplete is published — correct, since the durable copy will
// never exist and the query is done consuming anyway.
func (m *uploadManager) elidePending(qs *queryUploadState) {
	qs.mu.Lock()
	pending := qs.pending
	qs.pending = nil
	qs.mu.Unlock()
	for _, p := range pending {
		m.noteElided(p.j)
	}
}

// Flush waits for every pending background upload to complete (land in the
// object store or exhaust its retries) WITHOUT cancelling — the graceful
// counterpart of Drain, used by Worker.Drain so that consumers of this
// worker's stage outputs keep their durable S3 fallback after the
// peer-exchange server goes away. Queued lazy uploads are released first:
// drain is precisely the moment the local copies stop being servable, so
// the durable copies must exist after all. Callers must ensure no new
// StartTask calls can arrive (Worker.Drain runs it after the task
// WaitGroup drains), so a single snapshot of the query states covers
// everything. Returns ctx.Err() if the context expires first; the caller
// then falls back to Drain (cancel).
func (m *uploadManager) Flush(ctx context.Context) error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	states := make([]*queryUploadState, 0, len(m.queries))
	for _, qs := range m.queries {
		states = append(states, qs)
	}
	m.mu.Unlock()
	for _, qs := range states {
		m.releasePending(qs)
	}
	for _, qs := range states {
		done := make(chan struct{})
		go func(q *queryUploadState) {
			q.inflight.Wait()
			close(done)
		}(qs)
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// Drain cancels everything (worker shutdown).
func (m *uploadManager) Drain() {
	if m == nil {
		return
	}
	m.mu.Lock()
	states := make([]*queryUploadState, 0, len(m.queries))
	for root, qs := range m.queries {
		states = append(states, qs)
		delete(m.queries, root)
	}
	m.mu.Unlock()
	for _, qs := range states {
		qs.cancel()
		m.elidePending(qs)
	}
}

// StartTask begins tracking a task's background uploads and enqueues each
// job under the task's durability policy: eager jobs start now, lazy jobs
// queue until the root is released (already-released roots start now), off
// jobs are elided outright. Call after every job's srcPath is stable
// (adopted into the cache).
func (m *uploadManager) StartTask(root, taskID, workerID string, jobs []uploadJob, policy distributed.UploadPolicy) {
	if m == nil || len(jobs) == 0 {
		return
	}
	if policy == distributed.UploadOff {
		for _, j := range jobs {
			m.noteElided(j)
		}
		return
	}
	tu := &taskUploads{m: m, root: root, taskID: taskID, workerID: workerID}
	for _, j := range jobs {
		tu.keys = append(tu.keys, j.key)
	}
	tu.remaining.Store(int64(len(jobs)))
	qs := m.queryState(root)
	if policy == distributed.UploadLazy {
		qs.mu.Lock()
		if !qs.released {
			for _, j := range jobs {
				qs.pending = append(qs.pending, pendingUpload{tu: tu, j: j})
			}
			qs.mu.Unlock()
			return
		}
		qs.mu.Unlock()
	}
	for _, j := range jobs {
		m.startJob(qs, tu, j)
	}
}

// startJob spawns one upload goroutine inside the root's inflight scope.
func (m *uploadManager) startJob(qs *queryUploadState, tu *taskUploads, j uploadJob) {
	qs.inflight.Add(1)
	go func() {
		defer qs.inflight.Done()
		m.runJob(qs, tu, j)
	}()
}

func (m *uploadManager) runJob(qs *queryUploadState, tu *taskUploads, j uploadJob) {
	ctx := qs.ctx
	defer tu.jobDone()
	// SEPARATE budgets: admission waits and in-flight pauses each get
	// uploadHardCapMs. A deep backlog routinely burns the full admission
	// budget per job (upload_yield_ms ≈ 60-70s/job at SF100), and a
	// shared budget let every job enter its stream already at the escape
	// threshold — the v4 gate shipped engaged-never (pair 20260808-0224:
	// upload_pause_ms=0 on all workers). Total per-job delay stays
	// bounded at 2× the cap.
	var admitYieldNs, pauseBudgetNs int64
	if !m.acquireSlot(ctx, qs, &admitYieldNs) {
		tu.anyCanceled.Store(true)
		m.noteCancelled(j)
		return
	}
	defer m.releaseSlot()
	seq := m.registerJob()
	defer m.unregisterJob(seq)

	var lastErr error
	for attempt := 1; attempt <= uploadRetryAttempts; attempt++ {
		if ctx.Err() != nil {
			tu.anyCanceled.Store(true)
			m.noteCancelled(j)
			return
		}
		lastErr = m.uploadOnce(ctx, qs, seq, j, &pauseBudgetNs)
		if lastErr == nil {
			m.completed.Add(1)
			return
		}
		if ctx.Err() != nil {
			tu.anyCanceled.Store(true)
			m.noteCancelled(j)
			return
		}
		backoff := uploadRetryBackoff << (attempt - 1)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			tu.anyCanceled.Store(true)
			m.noteCancelled(j)
			return
		}
	}
	tu.anyFailed.Store(true)
	m.failedFiles.Add(1)
	m.logger.Error("background stage-output upload abandoned; key stays non-durable",
		"key", j.key, "task_id", tu.taskID, "attempts", uploadRetryAttempts, "error", lastErr)
}

// noteCancelled records an aborted upload: file count plus the local
// (pre-compression) size of the bytes that never had to move — the "S3
// PUT work a finished query saved" side of the upload ledger. Prefer the
// creation-time size: cancellation usually races query cleanup, and the
// cache-owned srcPath is often already unlinked by the time we get here.
func (m *uploadManager) noteCancelled(j uploadJob) {
	m.cancelledFiles.Add(1)
	if j.size > 0 {
		m.cancelledBytes.Add(j.size)
	} else if fi, err := os.Stat(j.srcPath); err == nil {
		m.cancelledBytes.Add(fi.Size())
	}
}

// noteElided records an upload that never started at all (off policy, or a
// lazy job whose query finished before any demand signal): the "S3 PUT work
// the durability knob saved" side of the upload ledger. Same pre-compression
// byte basis as cancelledBytes.
func (m *uploadManager) noteElided(j uploadJob) {
	m.elidedFiles.Add(1)
	if j.size > 0 {
		m.elidedBytes.Add(j.size)
	} else if fi, err := os.Stat(j.srcPath); err == nil {
		m.elidedBytes.Add(fi.Size())
	}
}

// uploadOnce mirrors the synchronous path: optional s2 compression (≥10%
// savings heuristic) staged as a temp file, then a streaming Put. The
// source file is cache-owned and never deleted here. Both the compression
// source and the PUT body read through governedReader (QoS v4), so an
// in-flight job freezes — CPU and wire alike — while a foreground
// protection window is open.
func (m *uploadManager) uploadOnce(ctx context.Context, qs *queryUploadState, seq int64, j uploadJob, jobYieldNs *int64) error {
	uploadPath := j.srcPath
	var tmpCompressed string
	// jobYieldNs == nil marks a SYNCHRONOUS caller (the query is blocked
	// on this PUT) — those run ungoverned; only background jobs pause.
	governed := jobYieldNs != nil
	if j.compress {
		tmp, err := os.CreateTemp(j.tmpDir, "async-upload-*.s2")
		if err == nil {
			tmpPath := tmp.Name()
			tmp.Close()
			var useCompressed bool
			var compErr error
			if governed {
				useCompressed, compErr = m.compressGoverned(ctx, qs, seq, j.srcPath, tmpPath, jobYieldNs)
			} else {
				_, useCompressed, compErr = CompressShuffleFile(j.srcPath, tmpPath)
			}
			if compErr != nil || !useCompressed {
				_ = os.Remove(tmpPath)
			} else {
				uploadPath = tmpPath
				tmpCompressed = tmpPath
			}
		}
		// Temp-file failure → upload raw; compression is an optimization.
	}
	if tmpCompressed != "" {
		defer os.Remove(tmpCompressed)
	}

	f, err := os.Open(uploadPath)
	if err != nil {
		return fmt.Errorf("opening %s: %w", uploadPath, err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat %s: %w", uploadPath, err)
	}
	var body io.Reader = f
	if governed {
		body = &governedReader{ctx: ctx, m: m, qs: qs, seq: seq, r: f, jobYieldNs: jobYieldNs}
	}
	if _, err := m.store.Put(ctx, j.bucket, j.key, body, fi.Size(), "application/octet-stream"); err != nil {
		return fmt.Errorf("put %s: %w", j.key, err)
	}
	m.completedBytes.Add(fi.Size())
	return nil
}

// compressGoverned is CompressShuffleFile with the source read through
// the v4 chunk gate — the s2 writer's CPU advances only as fast as its
// governed reads.
func (m *uploadManager) compressGoverned(ctx context.Context, qs *queryUploadState, seq int64, srcPath, dstPath string, jobYieldNs *int64) (bool, error) {
	src, err := os.Open(srcPath)
	if err != nil {
		return false, fmt.Errorf("open shuffle source: %w", err)
	}
	defer src.Close()
	info, err := src.Stat()
	if err != nil {
		return false, fmt.Errorf("stat shuffle source: %w", err)
	}
	gr := &governedReader{ctx: ctx, m: m, qs: qs, seq: seq, r: src, jobYieldNs: jobYieldNs}
	_, useCompressed, err := compressShuffleStream(gr, info.Size(), dstPath)
	return useCompressed, err
}

// jobDone publishes UploadComplete when the task's last upload settles.
func (tu *taskUploads) jobDone() {
	if tu.remaining.Add(-1) != 0 {
		return
	}
	if tu.anyCanceled.Load() {
		// Query is terminal — nobody consumes the durability bit anymore.
		return
	}
	msg := distributed.UploadComplete{
		RootQueryID: tu.root,
		TaskID:      tu.taskID,
		WorkerID:    tu.workerID,
		Keys:        tu.keys,
		Failed:      tu.anyFailed.Load(),
	}
	data, err := distributed.Marshal(msg)
	if err != nil {
		return
	}
	if tu.m.nc != nil {
		if err := tu.m.nc.Publish(distributed.SubjectUploadComplete, data); err != nil {
			tu.m.logger.Debug("UploadComplete publish failed (keys stay non-durable)",
				"task_id", tu.taskID, "error", err)
		}
	}
}

// UploadStats returns (completed, cancelled, failed) background upload
// file counts. Observability/test helper.
func (m *uploadManager) UploadStats() (completed, cancelled, failed int64) {
	return m.completed.Load(), m.cancelledFiles.Load(), m.failedFiles.Load()
}

// UploadByteStats returns the upload ledger's byte sides: wire bytes that
// landed in S3 (post-compression) and local bytes of uploads aborted by
// query completion (pre-compression).
func (m *uploadManager) UploadByteStats() (completedBytes, cancelledBytes int64) {
	if m == nil {
		return 0, 0
	}
	return m.completedBytes.Load(), m.cancelledBytes.Load()
}

// UploadElidedStats returns the durability knob's savings side of the
// ledger: uploads that never started at all (off policy, or lazy jobs whose
// query finished first) and their local pre-compression bytes.
func (m *uploadManager) UploadElidedStats() (files, bytes int64) {
	if m == nil {
		return 0, 0
	}
	return m.elidedFiles.Load(), m.elidedBytes.Load()
}
