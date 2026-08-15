package worker

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/nats-io/nats.go"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/exec"
)

// manifestStreamSource is the eager-consumer input source
// (docs/design/eager-consumer-dispatch.md §3.2): it consumes one producer
// stage's shuffle files as each producer TASK completes, instead of a
// frozen file list built after the whole stage drained.
//
// Contract:
//   - The candidate set (spec.ProducerTaskIDs) is fixed at task build;
//     which files exist, and where, streams in as ProducerTaskManifests
//     (Replay for tasks completed before dispatch, NATS for the rest —
//     subscribe happens in Init, before any wait, so nothing is missed;
//     duplicates are idempotent).
//   - Files outside [PartitionStart, PartitionEnd] are ignored.
//   - Reads go through the standard tiered fetch (LocalStageCache → peer
//     → S3) by delegating each resolved manifest's in-range files to an
//     inner cachedFileStreamSource; manifest PeerAddr hints are
//     registered so the peer tier works for files the task spec could
//     not have hinted.
//   - Attempt fencing (§5): consuming any file of producer task T pins
//     T's attempt. A later manifest for T with a higher attempt poisons
//     the source; Next returns errStaleInputAttempt and the task fails
//     loudly (coordinator retries it against the stable attempt set).
//
// StaleInputAttemptMarker tags the poison error so the coordinator's
// result classification can retry the consumer task without burning the
// generic failure path's diagnostics.
const StaleInputAttemptMarker = "eager-dispatch stale input attempt"

type manifestFileSet struct {
	taskID  string
	attempt int
	files   []string // in-range files only; may be empty (still resolves the task)
}

type manifestStreamSource struct {
	executor *Executor
	queryID  string
	bucket   string
	spec     distributed.EagerInput
	nc       *nats.Conn
	sub      *nats.Subscription

	mu        sync.Mutex
	arrived   chan struct{}       // closed+recreated on every state change
	queue     []manifestFileSet   // resolved, not yet consumed
	resolved  map[string]int      // producer taskID → attempt of resolved manifest
	consumed  map[string]int      // producer taskID → attempt pinned at first read
	poisoned  error               // sticky fencing violation
	candidate map[string]struct{} // full producer task ID set
	ordinal   map[string]int      // producer taskID → dispatch-order index

	inner   *cachedFileStreamSource // current file-set reader
	current manifestFileSet
	initCtx context.Context

	// donorInner mirrors inner for producer token donation (§2.2): inner is
	// owned by the producer goroutine in Next, but tryDonateToken is called
	// from consumer goroutines yielding width. A stale target refuses the
	// donation itself (its decode-ahead is stopped), so the mirror only has
	// to be race-free, not transactional.
	donorInner atomic.Pointer[cachedFileStreamSource]
}

func newManifestStreamSource(e *Executor, queryID, bucket string, spec distributed.EagerInput) *manifestStreamSource {
	cand := make(map[string]struct{}, len(spec.ProducerTaskIDs))
	ord := make(map[string]int, len(spec.ProducerTaskIDs))
	for i, id := range spec.ProducerTaskIDs {
		cand[id] = struct{}{}
		ord[id] = i
	}
	return &manifestStreamSource{
		executor:  e,
		queryID:   queryID,
		bucket:    bucket,
		spec:      spec,
		nc:        e.nc,
		arrived:   make(chan struct{}),
		resolved:  make(map[string]int, len(cand)),
		consumed:  make(map[string]int, len(cand)),
		candidate: cand,
		ordinal:   ord,
	}
}

func (s *manifestStreamSource) Init(ctx context.Context) error {
	s.initCtx = ctx
	if s.nc != nil {
		subject := distributed.EagerManifestSubject(s.spec.RootQueryID, s.spec.StageID)
		sub, err := s.nc.Subscribe(subject, func(msg *nats.Msg) {
			var m distributed.ProducerTaskManifest
			if err := distributed.Unmarshal(msg.Data, &m); err != nil {
				return
			}
			s.observe(m)
		})
		if err != nil {
			return fmt.Errorf("eager source: subscribing %s: %w", subject, err)
		}
		s.sub = sub
	}
	// Replay after subscribe: observe() is idempotent per (task, attempt),
	// so the overlap window between replay-list capture and subscription
	// start cannot double-consume or drop a manifest.
	for _, m := range s.spec.Replay {
		s.observe(m)
	}
	return nil
}

// observe folds one manifest into the feed state. Idempotent for
// duplicate (task, attempt) pairs; higher attempts supersede unconsumed
// entries and poison consumed ones (fencing).
func (s *manifestStreamSource) observe(m distributed.ProducerTaskManifest) {
	if _, ok := s.candidate[m.TaskID]; !ok {
		return // different edge of a shared producer stage, or garbage
	}
	inRange := s.filesInRange(m)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poisoned != nil {
		return
	}
	if pinned, ok := s.consumed[m.TaskID]; ok && m.Attempt > pinned {
		s.poisoned = fmt.Errorf("%s: producer task %s attempt %d superseded consumed attempt %d",
			StaleInputAttemptMarker, m.TaskID, m.Attempt, pinned)
		s.signalLocked()
		return
	}
	if prev, ok := s.resolved[m.TaskID]; ok {
		if m.Attempt <= prev {
			return // duplicate or stale
		}
		// Supersede an unconsumed resolution: drop the queued entry.
		for i := range s.queue {
			if s.queue[i].taskID == m.TaskID {
				s.queue = append(s.queue[:i], s.queue[i+1:]...)
				break
			}
		}
	}
	s.resolved[m.TaskID] = m.Attempt
	if len(inRange) > 0 {
		// Register peer hints before the files become fetchable.
		if m.PeerAddr != "" {
			for _, f := range inRange {
				s.executor.peers.addHint(f, m.PeerAddr)
			}
		}
		s.queue = append(s.queue, manifestFileSet{taskID: m.TaskID, attempt: m.Attempt, files: inRange})
	}
	s.signalLocked()
}

func (s *manifestStreamSource) signalLocked() {
	close(s.arrived)
	s.arrived = make(chan struct{})
}

func (s *manifestStreamSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	for {
		if s.inner != nil {
			b, err := s.inner.Next(ctx)
			if err != nil {
				return nil, err
			}
			if b != nil {
				return b, nil
			}
			s.donorInner.Store(nil)
			s.inner.Close()
			s.inner = nil
		}

		s.mu.Lock()
		if s.poisoned != nil {
			err := s.poisoned
			s.mu.Unlock()
			return nil, err
		}
		if len(s.queue) > 0 {
			s.current = s.queue[0]
			s.queue = s.queue[1:]
			// Pin the attempt at first read (fencing).
			s.consumed[s.current.taskID] = s.current.attempt
			s.mu.Unlock()
			inner := newCachedFileStreamSource(s.executor, s.queryID, s.bucket, s.current.files)
			if err := inner.Init(ctx); err != nil {
				return nil, fmt.Errorf("eager source: inner init (%s): %w", s.current.taskID, err)
			}
			s.inner = inner
			s.donorInner.Store(inner)
			continue
		}
		if len(s.resolved) >= len(s.candidate) {
			s.mu.Unlock()
			return nil, nil // every producer task resolved and drained: EOF
		}
		wait := s.arrived
		s.mu.Unlock()

		select {
		case <-wait:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (s *manifestStreamSource) Close() error {
	if s.sub != nil {
		s.sub.Unsubscribe()
		s.sub = nil
	}
	s.donorInner.Store(nil)
	if s.inner != nil {
		s.inner.Close()
		s.inner = nil
	}
	return nil
}

// tryDonateToken implements producerTokenDonor by forwarding to the current
// inner file-set reader (§2.2 producer token donation).
func (s *manifestStreamSource) tryDonateToken() bool {
	inner := s.donorInner.Load()
	return inner != nil && inner.tryDonateToken()
}

var _ exec.Source = (*manifestStreamSource)(nil)

// filesInRange filters one manifest's files to [PartitionStart,
// PartitionEnd] (inclusive). A key's partition is its "partition=NNNN"
// path segment when present (partitioned shuffle / exchange-sink
// output). A key WITHOUT the segment is a plain per-task output
// (compute-stage producers: hash-partitioned joins and final_aggregates
// whose unpartitioned sink writes one file per task) — there, task
// DISPATCH ORDER is the partition, exactly the positional contract the
// barrier path's StageOutput.Files (retrier.Files, dispatch-ordered)
// gives partitionFilesForWorker. The ordinal map mirrors that order via
// spec.ProducerTaskIDs.
func (s *manifestStreamSource) filesInRange(m distributed.ProducerTaskManifest) []string {
	ord, hasOrd := s.ordinal[m.TaskID]
	var out []string
	for _, f := range m.Files {
		p, ok := partitionOfKey(f)
		if !ok {
			if !hasOrd {
				continue
			}
			p = ord
		}
		if p >= s.spec.PartitionStart && p <= s.spec.PartitionEnd {
			out = append(out, f)
		}
	}
	return out
}

func partitionOfKey(key string) (int, bool) {
	const tag = "partition="
	i := strings.Index(key, tag)
	if i < 0 {
		return 0, false
	}
	rest := key[i+len(tag):]
	if j := strings.IndexByte(rest, '/'); j >= 0 {
		rest = rest[:j]
	}
	n, err := strconv.Atoi(rest)
	if err != nil {
		return 0, false
	}
	return n, true
}
