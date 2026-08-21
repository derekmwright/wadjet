package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/derekmwright/wadjet/internal/distributed"
)

// Poison-task defense (#318).
//
// A task that OOM-kills its worker is redelivered by JetStream on restart
// and kills the worker again: MaxDeliver/AckWait bound *transient* failures
// but cannot tell "the worker died" from "the task killed the worker" —
// a deterministic OOM spends the whole retry budget crashing the process,
// and because the crash also prevents the terminal ack, a fast-enough kill
// can loop beyond it (the two consecutive OOM'd server lifetimes in #318,
// same task IDs in both).
//
// The distinction needs a delivery-attempt record persisted OUTSIDE the
// dying process. JetStream metadata already carries the delivery number;
// what it cannot say is whether the PRIOR delivery began executing and
// never finished. The worker therefore writes a breadcrumb to NATS KV
// (same store that survives the restart and replays the task in the first
// place) immediately before executing, and clears it on any graceful
// completion — success or a properly published failure. A crash leaves the
// record behind, so:
//
//	delivery 1                        → normal execution (record written)
//	delivery ≥2, no record            → prior delivery was lost before
//	                                    execution; normal (not poison)
//	delivery ≥2, record, not degraded → the prior attempt died mid-
//	                                    execution: retry under a REDUCED
//	                                    memory budget so the ADR-0006
//	                                    machinery (spill early, bounded
//	                                    state) engages instead of the heap
//	                                    profile that killed the worker
//	delivery ≥2, record, degraded     → the degraded attempt died too:
//	                                    QUARANTINE — fail the query with an
//	                                    error naming the task and its
//	                                    memory demand, and Term the message
//	                                    so redelivery stops
//
// The degraded rung is preferred over quarantining at first suspicion
// because most "poison" tasks are memory-shaped, and a reduced budget
// turns the retry into exactly the graceful degradation ADR-0006 promises;
// a task that then fails its degraded attempt GRACEFULLY (e.g. the #325
// non-convergence error) produces an actionable query error on its own.
// Known imprecision, accepted: a healthy-but-slow task redelivered at
// AckWait expiry while its first worker is still running also matches
// "record present" — it executes degraded, which is wasteful but safe
// (task outputs are TaskID-idempotent, and degradation changes scheduling
// and spill, never results).
//
// WADJET_POISON_DEFENSE=0 disables the whole mechanism.

var poisonDefenseDisabled = os.Getenv("WADJET_POISON_DEFENSE") == "0"

const (
	taskAttemptsBucket = "wadjet_task_attempts"
	// taskAttemptTTL bounds stale breadcrumbs (quarantined tasks, workers
	// that died and never came back). Comfortably above AckWait ×
	// MaxDeliver so a live redelivery chain never sees its record expire
	// mid-flight.
	taskAttemptTTL = 2 * time.Hour
)

// attemptRecord is the breadcrumb persisted before a task executes.
type attemptRecord struct {
	WorkerID  string    `json:"worker_id"`
	PID       int       `json:"pid"`
	Delivery  uint64    `json:"delivery"`
	Degraded  bool      `json:"degraded"`
	StartedAt time.Time `json:"started_at"`
}

// attemptStore persists attempt records outside the worker process.
type attemptStore interface {
	Lookup(ctx context.Context, taskID string) (attemptRecord, bool, error)
	Record(ctx context.Context, taskID string, rec attemptRecord) error
	Clear(ctx context.Context, taskID string) error
}

// kvAttemptStore is the production store: NATS KV, which lives in the same
// JetStream state that survives worker death and replays the task.
type kvAttemptStore struct {
	kv jetstream.KeyValue
}

func newKVAttemptStore(ctx context.Context, js jetstream.JetStream) (*kvAttemptStore, error) {
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: taskAttemptsBucket,
		TTL:    taskAttemptTTL,
	})
	if err != nil {
		return nil, fmt.Errorf("creating task-attempt KV bucket: %w", err)
	}
	return &kvAttemptStore{kv: kv}, nil
}

// attemptKey sanitizes a task ID into a valid KV key.
func attemptKey(taskID string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '-', r == '_', r == '.':
			return r
		default:
			return '_'
		}
	}, taskID)
}

func (s *kvAttemptStore) Lookup(ctx context.Context, taskID string) (attemptRecord, bool, error) {
	entry, err := s.kv.Get(ctx, attemptKey(taskID))
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return attemptRecord{}, false, nil
		}
		return attemptRecord{}, false, err
	}
	var rec attemptRecord
	if err := json.Unmarshal(entry.Value(), &rec); err != nil {
		return attemptRecord{}, false, err
	}
	return rec, true, nil
}

func (s *kvAttemptStore) Record(ctx context.Context, taskID string, rec attemptRecord) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	_, err = s.kv.Put(ctx, attemptKey(taskID), data)
	return err
}

func (s *kvAttemptStore) Clear(ctx context.Context, taskID string) error {
	err := s.kv.Delete(ctx, attemptKey(taskID))
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return err
	}
	return nil
}

// memAttemptStore is the in-process store for tests and for workers whose
// KV bucket creation failed (defense still works within one process
// lifetime — better than nothing, though a restart forgets it).
type memAttemptStore struct {
	mu   sync.Mutex
	recs map[string]attemptRecord
}

func newMemAttemptStore() *memAttemptStore {
	return &memAttemptStore{recs: make(map[string]attemptRecord)}
}

func (s *memAttemptStore) Lookup(_ context.Context, taskID string) (attemptRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.recs[taskID]
	return rec, ok, nil
}

func (s *memAttemptStore) Record(_ context.Context, taskID string, rec attemptRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recs[taskID] = rec
	return nil
}

func (s *memAttemptStore) Clear(_ context.Context, taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.recs, taskID)
	return nil
}

// poisonRung is the decision for one delivery.
type poisonRung int

const (
	rungNormal poisonRung = iota
	rungDegraded
	rungQuarantine
)

// poisonVerdict applies the decision rule. prior is the persisted record of
// an attempt that began executing and never gracefully finished (nil when
// absent); delivery is the JetStream delivery number for THIS delivery.
func poisonVerdict(prior *attemptRecord, delivery uint64) poisonRung {
	if delivery < 2 || prior == nil {
		return rungNormal
	}
	if prior.Degraded {
		return rungQuarantine
	}
	return rungDegraded
}

// applyPoisonDefense runs the decision rule for an incoming delivery,
// mutates the task for a degraded retry, and persists the new attempt
// record. It returns a non-nil ResultNotification when the task is
// quarantined and must NOT execute. Store errors fail open (normal
// execution, logged): the defense is an availability guard, not a
// correctness gate.
func (w *Worker) applyPoisonDefense(ctx context.Context, task *distributed.Task, delivery uint64) *distributed.ResultNotification {
	if poisonDefenseDisabled || w.attempts == nil || delivery == 0 {
		return nil
	}
	var prior *attemptRecord
	if delivery >= 2 {
		rec, ok, err := w.attempts.Lookup(ctx, task.ID)
		if err != nil {
			w.logger.Warn("poison defense: attempt lookup failed; executing normally",
				"task_id", task.ID, "error", err)
		} else if ok {
			prior = &rec
		}
	}

	switch poisonVerdict(prior, delivery) {
	case rungQuarantine:
		degradedMB := w.executor.DegradedTaskBudget() / (1 << 20)
		msg := fmt.Sprintf(
			"task %s (stage %s, query %s) quarantined as a poison task: delivery %d follows an attempt "+
				"on worker %s (pid %d, started %s) that died mid-execution under an already-degraded memory "+
				"budget of %d MiB. The task's memory demand exceeds what the degraded never-OOM machinery "+
				"can bound (estimated input %d MiB); failing the query instead of crash-looping the worker",
			task.ID, task.StageID, task.QueryID, delivery,
			prior.WorkerID, prior.PID, prior.StartedAt.Format(time.RFC3339),
			degradedMB, task.EstimatedBytes/(1<<20))
		w.logger.Error("poison task quarantined",
			"task_id", task.ID, "query_id", task.QueryID, "stage_id", task.StageID,
			"delivery", delivery, "prior_worker", prior.WorkerID, "prior_pid", prior.PID)
		return &distributed.ResultNotification{
			TaskID:    task.ID,
			QueryID:   task.QueryID,
			StageID:   task.StageID,
			WorkerID:  w.config.WorkerID,
			Error:     msg,
			Timestamp: time.Now(),
		}
	case rungDegraded:
		task.DegradedMemory = true
		w.logger.Warn("prior attempt of this task coincided with a worker death; retrying under a degraded memory budget",
			"task_id", task.ID, "query_id", task.QueryID,
			"delivery", delivery, "prior_worker", prior.WorkerID, "prior_pid", prior.PID,
			"prior_started", prior.StartedAt.Format(time.RFC3339),
			"degraded_budget_mb", w.executor.DegradedTaskBudget()/(1<<20))
	}

	rec := attemptRecord{
		WorkerID:  w.config.WorkerID,
		PID:       os.Getpid(),
		Delivery:  delivery,
		Degraded:  task.DegradedMemory,
		StartedAt: time.Now(),
	}
	if err := w.attempts.Record(ctx, task.ID, rec); err != nil {
		w.logger.Warn("poison defense: attempt record write failed",
			"task_id", task.ID, "error", err)
	}
	return nil
}

// clearPoisonRecord removes the attempt breadcrumb after a graceful
// completion (any published result — the worker survived the task).
func (w *Worker) clearPoisonRecord(taskID string) {
	if poisonDefenseDisabled || w.attempts == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.attempts.Clear(ctx, taskID); err != nil {
		w.logger.Warn("poison defense: attempt record clear failed",
			"task_id", taskID, "error", err)
	}
}
