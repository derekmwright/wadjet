package worker

import (
	"context"
	"fmt"
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
type uploadManager struct {
	store  objstore.Store
	nc     *nats.Conn
	logger *slog.Logger

	sem chan struct{} // global upload concurrency (matches the sync path's 8)

	mu      sync.Mutex
	queries map[string]*queryUploadState

	// Observability counters.
	completed      atomic.Int64 // files uploaded in the background
	cancelledFiles atomic.Int64 // uploads aborted by query completion
	failedFiles    atomic.Int64 // uploads abandoned after retries
	completedBytes atomic.Int64 // wire bytes PUT (post-compression)
	cancelledBytes atomic.Int64 // local file bytes of aborted uploads (pre-compression)
}

type queryUploadState struct {
	ctx      context.Context
	cancel   context.CancelFunc
	inflight sync.WaitGroup
}

func newUploadManager(store objstore.Store, nc *nats.Conn, logger *slog.Logger) *uploadManager {
	if logger == nil {
		logger = slog.Default()
	}
	return &uploadManager{
		store:   store,
		nc:      nc,
		logger:  logger,
		sem:     make(chan struct{}, 8),
		queries: make(map[string]*queryUploadState),
	}
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
		qs = &queryUploadState{ctx: ctx, cancel: cancel}
		m.queries[root] = qs
	}
	return qs
}

// CancelQuery aborts pending uploads for a terminal query and drops its
// scope. Safe for unknown roots, repeated calls, and nil receivers.
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
	}
}

// Flush waits for every pending background upload to complete (land in the
// object store or exhaust its retries) WITHOUT cancelling — the graceful
// counterpart of Drain, used by Worker.Drain so that consumers of this
// worker's stage outputs keep their durable S3 fallback after the
// peer-exchange server goes away. Callers must ensure no new StartTask
// calls can arrive (Worker.Drain runs it after the task WaitGroup drains),
// so a single snapshot of the query states covers everything. Returns
// ctx.Err() if the context expires first; the caller then falls back to
// Drain (cancel).
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
	}
}

// StartTask begins tracking a task's background uploads and enqueues each
// job. Call after every job's srcPath is stable (adopted into the cache).
func (m *uploadManager) StartTask(root, taskID, workerID string, jobs []uploadJob) {
	if m == nil || len(jobs) == 0 {
		return
	}
	tu := &taskUploads{m: m, root: root, taskID: taskID, workerID: workerID}
	for _, j := range jobs {
		tu.keys = append(tu.keys, j.key)
	}
	tu.remaining.Store(int64(len(jobs)))
	qs := m.queryState(root)
	for _, j := range jobs {
		qs.inflight.Add(1)
		go func(j uploadJob) {
			defer qs.inflight.Done()
			m.runJob(qs.ctx, tu, j)
		}(j)
	}
}

func (m *uploadManager) runJob(ctx context.Context, tu *taskUploads, j uploadJob) {
	defer tu.jobDone()
	select {
	case m.sem <- struct{}{}:
		defer func() { <-m.sem }()
	case <-ctx.Done():
		tu.anyCanceled.Store(true)
		m.noteCancelled(j)
		return
	}

	var lastErr error
	for attempt := 1; attempt <= uploadRetryAttempts; attempt++ {
		if ctx.Err() != nil {
			tu.anyCanceled.Store(true)
			m.noteCancelled(j)
			return
		}
		lastErr = m.uploadOnce(ctx, j)
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
// PUT work a finished query saved" side of the upload ledger.
func (m *uploadManager) noteCancelled(j uploadJob) {
	m.cancelledFiles.Add(1)
	if fi, err := os.Stat(j.srcPath); err == nil {
		m.cancelledBytes.Add(fi.Size())
	}
}

// uploadOnce mirrors the synchronous path: optional s2 compression (≥10%
// savings heuristic) staged as a temp file, then a streaming Put. The
// source file is cache-owned and never deleted here.
func (m *uploadManager) uploadOnce(ctx context.Context, j uploadJob) error {
	uploadPath := j.srcPath
	var tmpCompressed string
	if j.compress {
		tmp, err := os.CreateTemp(j.tmpDir, "async-upload-*.s2")
		if err == nil {
			tmpPath := tmp.Name()
			tmp.Close()
			_, useCompressed, compErr := CompressShuffleFile(j.srcPath, tmpPath)
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
	if _, err := m.store.Put(ctx, j.bucket, j.key, f, fi.Size(), "application/octet-stream"); err != nil {
		return fmt.Errorf("put %s: %w", j.key, err)
	}
	m.completedBytes.Add(fi.Size())
	return nil
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
