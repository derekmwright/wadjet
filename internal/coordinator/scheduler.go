package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/citc-tech/wadjet/internal/dataplane"
	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/nats-io/nats.go"
)

// schedulerInflightTTL bounds how long a dispatched task's EstimatedBytes
// counts against its worker in pickLeastLoaded. The estimate covers the
// dispatch→pool-visible blind window: once the task is admitted its real
// allocations show up in the worker's heartbeat PoolUsed (10s cadence), so
// holding the estimate past that double-counts (conservative, never
// unsafe). Result arrival clears the entry early via TaskDone; the TTL is
// the backstop for lost results.
const schedulerInflightTTL = 60 * time.Second

// inflightTask is one dispatched-not-yet-done task's admission estimate,
// charged against a worker in pickLeastLoaded.
type inflightTask struct {
	workerID string
	bytes    int64
	expires  time.Time
}

// Scheduler publishes tasks to NATS and tracks stage DAGs.
type Scheduler struct {
	nc     *nats.Conn
	logger *slog.Logger

	// dpSrv is the optional data-plane gRPC server. When set, PublishTasks
	// pushes each task over a per-worker gRPC stream instead of NATS.
	// Placement: bin-packed by estimated memory fit when the task carries
	// an EstimatedBytes and heartbeat pool stats exist (see pickWorkerFor),
	// round-robin otherwise (Server.PickWorker). NATS is not used as a
	// fallback in this mode — gRPC dispatch is either all or nothing per
	// the Phase C design.
	dpSrv *dataplane.Server

	// registry provides heartbeat pool stats (PoolUsed/PoolBudget) for
	// bin-packed placement. nil = always round-robin.
	registry *WorkerRegistry

	inflightMu sync.Mutex
	inflight   map[string]inflightTask // task ID → estimate charged to a worker

	// eagerInflight tracks dispatched-not-yet-done eager consumer tasks
	// (Task.EagerInputs non-empty) per worker, for the §3.3 producer-lane
	// reservation: pickEagerWorker spreads these so no worker's task slots
	// fill up with manifest-blocked consumers while its producer tasks
	// queue behind them (dispatch is targeted gRPC — a saturated worker's
	// queue cannot be stolen). Long TTL: eager tasks legitimately run for
	// the whole producer stage, so the 60s estimate TTL would drop the
	// charge mid-flight.
	eagerInflight map[string]inflightTask // task ID → worker holding an eager task

	// annotate, when set, mutates each task immediately before it is
	// serialized for dispatch — the single egress every task (initial and
	// retried) passes through. Used by streaming exchange to attach
	// peer-location hints and fetch tokens. Must be cheap and thread-safe.
	annotate func(*distributed.Task)

	// localityPlacement enables input-locality placement (docs/design/
	// locality-placement.md): a task whose peer-location hints all point
	// at one connected worker is placed on that worker, converting its
	// exchange reads from peer gRPC streams into same-worker mmaps. Only
	// meaningful when the annotator supplies hints (streaming exchange).
	localityPlacement bool
}

// NewScheduler creates a new task scheduler.
func NewScheduler(nc *nats.Conn, logger *slog.Logger) *Scheduler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Scheduler{
		nc:            nc,
		logger:        logger,
		inflight:      make(map[string]inflightTask),
		eagerInflight: make(map[string]inflightTask),
	}
}

// eagerInflightTTL bounds how long an eager consumer task counts against
// its worker in pickEagerWorker. Sized to the stage idle timeout — an
// eager task can legitimately wait most of its producer stage's lifetime.
// Result arrival clears the entry early via TaskDone.
const eagerInflightTTL = 30 * time.Minute

// SetDataPlaneServer enables gRPC task dispatch. When set, PublishTasks
// pushes through the data-plane server instead of NATS publish.
func (s *Scheduler) SetDataPlaneServer(srv *dataplane.Server) {
	s.dpSrv = srv
}

// SetWorkerRegistry enables memory-aware placement for gRPC dispatch:
// tasks carrying an EstimatedBytes go to the worker with the most free
// pool (heartbeat budget − heartbeat used − in-flight estimates) instead
// of round-robin.
func (s *Scheduler) SetWorkerRegistry(wr *WorkerRegistry) {
	s.registry = wr
}

// SetTaskAnnotator installs the per-task pre-dispatch hook. Call before
// any query runs (same contract as the other Set<X> setters).
func (s *Scheduler) SetTaskAnnotator(fn func(*distributed.Task)) {
	s.annotate = fn
}

// SetLocalityPlacement toggles input-locality placement. Call before any
// query runs (same contract as the other Set<X> setters).
func (s *Scheduler) SetLocalityPlacement(on bool) {
	s.localityPlacement = on
}

// TaskDone clears the task's in-flight admission estimate. Called by the
// stage result subscriptions on every ResultNotification; the TTL covers
// lost results.
func (s *Scheduler) TaskDone(taskID string) {
	s.inflightMu.Lock()
	delete(s.inflight, taskID)
	delete(s.eagerInflight, taskID)
	s.inflightMu.Unlock()
}

// preparedTask is the per-task scratch built by PublishTasks before
// dispatch. Subject is used by the NATS path; data is the
// `distributed.Marshal(Task)` blob both transports carry.
type preparedTask struct {
	subject string
	data    []byte
	task    distributed.Task
}

// PublishTasks publishes a set of tasks for worker consumption. Routes
// via gRPC TaskDispatch (data-plane server) when configured, otherwise
// falls back to NATS JetStream publish. Tasks are serialized in batch
// before publishing to minimize time spent holding the transport.
func (s *Scheduler) PublishTasks(ctx context.Context, tasks []distributed.Task) error {
	if len(tasks) == 0 {
		return nil
	}

	// Carry the query deadline to workers so a dispatched task is bounded by
	// the same budget the coordinator enforces — a worker that wedges on a
	// task then releases its slot at the deadline instead of holding it until
	// the whole query times out (#101). 0 = no deadline rides the dispatch.
	var deadlineNano int64
	if dl, ok := ctx.Deadline(); ok {
		deadlineNano = dl.UnixNano()
	}

	batch := make([]preparedTask, 0, len(tasks))
	for _, task := range tasks {
		if s.annotate != nil {
			s.annotate(&task)
		}
		data, err := distributed.Marshal(task)
		if err != nil {
			return fmt.Errorf("marshaling task %s: %w", task.ID, err)
		}

		// Subject is only used by the NATS path; cheap to compute upfront.
		// Priority tasks ride the latency-critical lane (separate WorkQueue
		// stream + dedicated worker slots) so they never queue behind bulk
		// scan fan-out.
		var subject string
		if task.Priority {
			subject = distributed.PriTaskSubject(string(task.Type), task.QueryID, task.StageID)
			if task.ClusterID != "" {
				subject = distributed.ClusterPriTaskSubject(task.ClusterID, string(task.Type), task.QueryID, task.StageID)
			}
		} else {
			subject = distributed.TaskSubject(string(task.Type), task.QueryID, task.StageID)
			if task.ClusterID != "" {
				subject = distributed.ClusterTaskSubject(task.ClusterID, string(task.Type), task.QueryID, task.StageID)
			}
		}

		batch = append(batch, preparedTask{
			subject: subject,
			data:    data,
			task:    task,
		})
	}

	if s.dpSrv != nil {
		return s.publishViaDataPlane(batch, deadlineNano)
	}

	// Publish all pre-serialized messages
	for _, p := range batch {
		if err := s.nc.Publish(p.subject, p.data); err != nil {
			return fmt.Errorf("publishing task %s to %s: %w", p.task.ID, p.subject, err)
		}
	}

	if err := s.nc.Flush(); err != nil {
		return fmt.Errorf("flushing NATS: %w", err)
	}

	s.logger.Info("published tasks", "count", len(tasks), "transport", "nats")
	return nil
}

// publishViaDataPlane pushes each task to a selected worker. Tasks
// carrying an EstimatedBytes are bin-packed onto the worker with the most
// free pool; tasks without an estimate round-robin (see pickWorkerFor).
// Send blocks on HTTP/2 flow control when a worker is saturated, applying
// backpressure to the scheduler. The full set is rejected if no worker
// is connected — there is no NATS fallback in gRPC mode.
func (s *Scheduler) publishViaDataPlane(batch []preparedTask, deadlineNano int64) error {
	// batchAssigned counts this call's placements per worker. Bin-packed
	// tasks spread across it first (same-stage anti-affinity): one
	// PublishTasks call is one stage's fan-out, and stale heartbeats used
	// to let "most free pool" put an entire fan-out on one worker
	// (2026-07-20 Q20 diagnosis: 3×18M-row final-aggregate tasks on one
	// worker, +24s serialization; clump lotteries visible in every run).
	batchAssigned := make(map[string]int)
	methods := make(map[string]int)
	for _, p := range batch {
		workerID, method, ok := s.pickWorkerFor(p.task, batchAssigned, len(batch))
		if !ok {
			return fmt.Errorf("dispatching task %s: %w", p.task.ID, dataplane.ErrNoWorkers)
		}
		if err := s.dpSrv.SendTaskDispatch(workerID, p.task.ID, p.task.QueryID, p.task.StageID, p.data, deadlineNano); err != nil {
			if errors.Is(err, dataplane.ErrNotConnected) {
				// Worker dropped between Pick and Send; one retry on the
				// next round-robin position keeps the path lossless when
				// the cluster has more than one worker.
				workerID2, ok2 := s.dpSrv.PickWorker()
				if ok2 && workerID2 != workerID {
					if err2 := s.dpSrv.SendTaskDispatch(workerID2, p.task.ID, p.task.QueryID, p.task.StageID, p.data, deadlineNano); err2 == nil {
						s.noteInflight(p.task, workerID2)
						batchAssigned[workerID2]++
						methods[method]++
						continue
					}
				}
			}
			return fmt.Errorf("dispatching task %s to %s: %w", p.task.ID, workerID, err)
		}
		s.noteInflight(p.task, workerID)
		batchAssigned[workerID]++
		methods[method]++
	}
	s.logger.Info("published tasks", "count", len(batch), "transport", "grpc",
		"spread", formatCounts(batchAssigned), "placement", formatCounts(methods))
	return nil
}

// formatCounts renders a count map as "k1:v1,k2:v2" with sorted keys —
// compact enough for one log attr, stable enough to grep.
func formatCounts(m map[string]int) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%s:%d", k, m[k])
	}
	return b.String()
}

// pickWorkerFor selects a worker for one task. Memory-aware bin-packing
// applies only when the task carries an estimate AND heartbeat pool stats
// exist — otherwise placement is the pre-existing round-robin. Estimates
// of 0 deliberately round-robin: with no per-task charge, "most free"
// would be static between heartbeats and pile every task of a fan-out
// onto one worker.
//
// batchAssigned carries this publish call's placements so far: bin-packed
// tasks are restricted to the workers with the fewest same-batch tasks
// before "most free pool" ranks them. Without that restriction the same
// staleness pathology hits estimated tasks — heartbeats lag 10s and the
// per-task in-flight charge is small, so an entire fan-out lands on
// whichever worker last reported the most free pool.
//
// Eager consumer tasks take their own placement path first: they can
// block on producer manifests for the whole producer stage, so stacking
// them on one worker starves that worker's producer lanes (targeted gRPC
// dispatch has no work stealing).
//
// The returned method ("eager" | "local" | "binpack" | "rr") feeds the
// placement attr on the "published tasks" line.
func (s *Scheduler) pickWorkerFor(t distributed.Task, batchAssigned map[string]int, batchLen int) (workerID, method string, ok bool) {
	if len(t.EagerInputs) > 0 {
		if id, ok := s.pickEagerWorker(); ok {
			return id, "eager", true
		}
	}
	if s.localityPlacement && s.registry != nil {
		if id, ok := pickLocalityWorkerFrom(t, s.dpSrv.ConnectedWorkers(), s.registry.ActiveWorkers(), batchAssigned, batchLen); ok {
			return id, "local", true
		}
	}
	if t.EstimatedBytes > 0 && s.registry != nil {
		if id, ok := s.pickLeastLoadedSpread(batchAssigned); ok {
			return id, "binpack", true
		}
	}
	id, ok := s.dpSrv.PickWorker()
	return id, "rr", ok
}

// pickLocalityWorkerFrom places a task whose peer-location hints all point
// at ONE worker onto that worker — the 1:1 stage-chain case (consumer task
// i reading exactly producer task i's output), where a same-worker mmap
// replaces a peer gRPC stream for the entire hinted input set. Preference,
// not correctness: hints are best-effort and reads fall back through the
// peer/S3 tiers wherever the task lands.
//
// Repartition-exchange consumers read from every producer, carry hints on
// multiple workers, and fall through — their cross-worker bytes are
// irreducible (each producer contributes ~uniformly to every partition).
//
// The same-batch cap (ceil batch/connected) bounds clumping when many
// tasks of one fan-out share a hinted worker (replicated build files whose
// single producer would otherwise attract the whole batch), preserving the
// anti-affinity the binpack spread established (2026-07-20 Q20 diagnosis:
// a stacked fan-out cost +24s serialization). 1:1 chains inherit the
// producer stage's uniform spread, so the cap does not bite them.
func pickLocalityWorkerFrom(t distributed.Task, connected []string, active []*WorkerInfo, batchAssigned map[string]int, batchLen int) (string, bool) {
	if len(t.InputLocations) == 0 || len(connected) == 0 {
		return "", false
	}
	addr := ""
	for _, a := range t.InputLocations {
		if addr == "" {
			addr = a
		} else if a != addr {
			return "", false // inputs span workers: nothing to co-locate with
		}
	}
	var workerID string
	for _, w := range active {
		if w.PeerAddr != "" && w.PeerAddr == addr {
			workerID = w.WorkerID
			break
		}
	}
	if workerID == "" {
		return "", false
	}
	if !slices.Contains(connected, workerID) {
		return "", false
	}
	if batchAssigned[workerID] >= (batchLen+len(connected)-1)/len(connected) {
		return "", false
	}
	return workerID, true
}

// pickEagerWorker places one eager consumer task: the connected worker
// with the fewest in-flight eager tasks, preferring workers that keep at
// least one non-eager lane free (in-flight eager + this one ≤
// MaxConcurrent − 1 — the memo §3.3 producer-lane reservation). When no
// worker satisfies the reservation (tiny MaxConcurrent, missing stats),
// min-count placement still bounds the stacking; the coordinator-wide
// one-eager-stage slot plus numTasks ≤ workerCount eligibility keep the
// total at one per worker in practice. ok=false when no worker is
// connected (caller falls through to round-robin's error handling).
func (s *Scheduler) pickEagerWorker() (string, bool) {
	connected := s.dpSrv.ConnectedWorkers()
	if len(connected) == 0 {
		return "", false
	}
	maxConc := make(map[string]int)
	if s.registry != nil {
		for _, w := range s.registry.ActiveWorkers() {
			maxConc[w.WorkerID] = w.MaxConcurrent
		}
	}
	return pickEagerWorkerFrom(connected, maxConc, s.eagerInflightByWorker())
}

// pickEagerWorkerFrom is pickEagerWorker's pure scoring core: min in-flight
// eager count over connected workers, first pass restricted to workers with
// a spare non-eager lane (count+1 ≤ MaxConcurrent−1), second pass
// unrestricted so dispatch never fails on reservation grounds alone.
func pickEagerWorkerFrom(connected []string, maxConc, counts map[string]int) (string, bool) {
	pick := func(requireLane bool) (string, bool) {
		bestID, bestCount, found := "", 0, false
		for _, id := range connected {
			n := counts[id]
			if requireLane {
				mc := maxConc[id]
				if mc <= 0 || n+1 > mc-1 {
					continue
				}
			}
			if !found || n < bestCount {
				bestID, bestCount, found = id, n, true
			}
		}
		return bestID, found
	}
	if id, ok := pick(true); ok {
		return id, true
	}
	return pick(false)
}

// eagerInflightByWorker counts live eager tasks per worker, lazily
// expiring stale entries.
func (s *Scheduler) eagerInflightByWorker() map[string]int {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	now := time.Now()
	perWorker := make(map[string]int)
	for id, e := range s.eagerInflight {
		if now.After(e.expires) {
			delete(s.eagerInflight, id)
			continue
		}
		perWorker[e.workerID]++
	}
	return perWorker
}

// pickLeastLoaded returns the connected worker with the most free shared
// pool: heartbeat budget − heartbeat used − in-flight dispatch estimates.
// Workers without pool stats (legacy builds, pre-first-heartbeat) are
// skipped; returns ok=false when none qualify so the caller round-robins.
func (s *Scheduler) pickLeastLoaded() (string, bool) {
	return pickLeastLoadedWorker(s.dpSrv.ConnectedWorkers(), s.registry.ActiveWorkers(), s.inflightByWorker())
}

// pickLeastLoadedSpread is pickLeastLoaded restricted to the workers with
// the fewest tasks already placed by the current publish call — the
// same-stage anti-affinity pass. Pool ranking then breaks ties among
// them. Single-task publishes (retries, governed eager top-ups) see an
// all-zero map and degrade to plain pickLeastLoaded.
func (s *Scheduler) pickLeastLoadedSpread(batchAssigned map[string]int) (string, bool) {
	connected := minBatchWorkers(s.dpSrv.ConnectedWorkers(), batchAssigned)
	return pickLeastLoadedWorker(connected, s.registry.ActiveWorkers(), s.inflightByWorker())
}

// minBatchWorkers returns the subset of connected workers holding the
// fewest same-batch placements. Order is preserved.
func minBatchWorkers(connected []string, batchAssigned map[string]int) []string {
	if len(connected) == 0 {
		return connected
	}
	min := batchAssigned[connected[0]]
	for _, id := range connected[1:] {
		if n := batchAssigned[id]; n < min {
			min = n
		}
	}
	out := make([]string, 0, len(connected))
	for _, id := range connected {
		if batchAssigned[id] == min {
			out = append(out, id)
		}
	}
	return out
}

// pickLeastLoadedWorker is pickLeastLoaded's pure scoring core: max over
// connected workers of (PoolBudget − PoolUsed − in-flight estimate),
// skipping workers with no pool stats. Free space may legitimately go
// negative under burst dispatch — the least-overcommitted worker still
// wins (every placement choice exists; refusing here would just push the
// decision to round-robin, which picks blind).
func pickLeastLoadedWorker(connected []string, active []*WorkerInfo, inflight map[string]int64) (string, bool) {
	infos := make(map[string]*WorkerInfo, len(active))
	for _, w := range active {
		infos[w.WorkerID] = w
	}
	var bestID string
	var bestFree int64
	found := false
	for _, id := range connected {
		info, ok := infos[id]
		if !ok || info.PoolBudget <= 0 {
			continue
		}
		free := info.PoolBudget - info.PoolUsed - inflight[id]
		if !found || free > bestFree {
			bestID, bestFree, found = id, free, true
		}
	}
	return bestID, found
}

// inflightByWorker sums live in-flight estimates per worker, lazily
// expiring stale entries (no background goroutine).
func (s *Scheduler) inflightByWorker() map[string]int64 {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	now := time.Now()
	perWorker := make(map[string]int64)
	for id, e := range s.inflight {
		if now.After(e.expires) {
			delete(s.inflight, id)
			continue
		}
		perWorker[e.workerID] += e.bytes
	}
	return perWorker
}

// noteInflight charges a dispatched task's estimate to its worker until
// the result arrives (TaskDone) or the TTL hands accounting off to the
// worker's heartbeat PoolUsed. Eager consumer tasks are additionally
// tracked (regardless of estimate) for pickEagerWorker's lane accounting.
func (s *Scheduler) noteInflight(t distributed.Task, workerID string) {
	if len(t.EagerInputs) > 0 {
		s.inflightMu.Lock()
		s.eagerInflight[t.ID] = inflightTask{
			workerID: workerID,
			expires:  time.Now().Add(eagerInflightTTL),
		}
		s.inflightMu.Unlock()
	}
	if t.EstimatedBytes <= 0 {
		return
	}
	s.inflightMu.Lock()
	s.inflight[t.ID] = inflightTask{
		workerID: workerID,
		bytes:    t.EstimatedBytes,
		expires:  time.Now().Add(schedulerInflightTTL),
	}
	s.inflightMu.Unlock()
}
