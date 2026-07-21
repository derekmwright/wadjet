package coordinator

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"golang.org/x/sync/errgroup"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/expr"
	"github.com/citc-tech/wadjet/internal/planner/physical"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// gatherReceiveTimeout bounds the coordinator's wait for worker terminal
// gather messages. Matches shuffleStageTimeout so long-running final
// aggregation stages don't falsely time out.
const gatherReceiveTimeout = 10 * time.Minute

// gatherFusion carries the pre-installed gather subscription + reply subject
// through dispatch so that an upstream final_aggregate / merge_aggregate
// stage can emit OpGatherSink instead of OpUnpartitionedSink — collapsing
// the standalone gather stage into the aggregate fragment and saving one
// S3 PUT/GET + one task dispatch round-trip per query. The dispatcher
// updates recv's expected-terminals count once it knows numTasks. Nil = no
// fusion (legacy gather dispatch runs after the DAG completes).
type gatherFusion struct {
	recv         *gatherReceiver
	replySubject string
}

// canFuseGather reports whether a gather stage can be absorbed into its
// upstream stage. Returns the upstream stage ID when fusion is eligible.
//
// Two upstream-shape cases are eligible:
//
//  1. Aggregate (final_aggregate / merge_aggregate). The upstream emits
//     unordered groups (or pre-sorted groups when SortKeys are set via
//     fuseSortIntoPredecessor). Reject Ordering on the gather Exchange in
//     this case: ordered gather is the coordinator-side sort-merge of
//     pre-sorted streams; when the upstream agg already absorbs the sort
//     and runs single-task, the gather Ordering is redundant — but we
//     still require it to be unset to avoid silent semantic changes for
//     any caller relying on it.
//
//  2. Sort (sort / merge_sort). The upstream produces a single ordered
//     stream. Ordering on the gather Exchange is permitted IFF the
//     upstream SortKeys equal it (i.e., the legacy gather's coord-side
//     re-sort would be redundant). Otherwise the keys differ and fusion
//     would silently corrupt order.
//
// Common requirements for both cases:
//   - Exactly one upstream dependency.
//   - Upstream is in the pending dispatch set (not a leaf scan or pre-
//     computed input).
//   - Distribution.Kind == DistSingleton when the upstream emits ordered
//     output (sort, merge_sort, or sort-bearing aggregate). Multi-task
//     ordered upstreams sort their partition independently — streaming N
//     partial-sorted streams to gather concatenates them in arrival
//     order, losing global order.
//
// Amplification-safe: the gather sink publishes via NATS, not S3, so
// per-task fan-out doesn't multiply S3 GETs the way scan/join fragment
// fusion does.
func canFuseGather(gatherStage physical.Stage, pending map[string]physical.Stage) (depID string, ok bool) {
	if len(gatherStage.Dependencies) != 1 {
		return "", false
	}
	depID = gatherStage.Dependencies[0]
	dep, present := pending[depID]
	if !present {
		return "", false
	}
	gatherOrdering := gatherOrderingKeys(gatherStage)
	switch dep.Type {
	case "final_aggregate", "merge_aggregate":
		if len(gatherOrdering) > 0 {
			return "", false
		}
		if (len(dep.SortKeys) > 0 || dep.Limit > 0) && dep.Distribution.Kind != physical.DistSingleton {
			return "", false
		}
		return depID, true
	case "sort", "merge_sort":
		if dep.Distribution.Kind != physical.DistSingleton {
			return "", false
		}
		// When gather has Ordering, require it match the upstream sort's
		// keys exactly. The upstream already produces rows in that order;
		// the gather re-sort is redundant. When gather has no Ordering, the
		// caller is taking the sort's order as-is — also fine.
		if len(gatherOrdering) > 0 && !sortKeysEqual(gatherOrdering, dep.SortKeys) {
			return "", false
		}
		return depID, true
	}
	return "", false
}

func gatherOrderingKeys(gatherStage physical.Stage) []physical.SortKeySpec {
	if gatherStage.Exchange == nil {
		return nil
	}
	return gatherStage.Exchange.Ordering
}

func sortKeysEqual(a, b []physical.SortKeySpec) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// subscribeTaskResults installs a result-subject subscription AND flushes
// the connection so the server has registered the interest before the
// caller publishes the task. Subscribe alone only creates the client-side
// record; the interest reaches the NATS server on the next flush. Without
// the explicit flush, a fast worker (in-process at SF0.01, or any worker
// when the coordinator's flusher loses the scheduling race) can complete
// the task and publish its result to a raw subject with NO subscriber —
// the result is dropped, the task finished too fast to ever enter
// heartbeat liveness (watchStuckTasks has nothing to reap), and the stage
// idles out after 30 minutes. The race detector's scheduling perturbation
// made this reliably reachable (issue #143, TestTPCHNativeDAG_SF01 Q04
// hang), but the window exists in production whenever publish-to-result
// round-trips beat interest propagation. Mirrors subscribeGather's flush.
func (c *Coordinator) subscribeTaskResults(subject string, handler nats.MsgHandler) (*nats.Subscription, error) {
	sub, err := c.nc.Subscribe(subject, handler)
	if err != nil {
		return nil, err
	}
	if err := c.nc.Flush(); err != nil {
		sub.Unsubscribe()
		return nil, fmt.Errorf("flushing subscription %q: %w", subject, err)
	}
	return sub, nil
}

// awaitStageProgress blocks until either every dispatched task has reported
// a result (signalled via allDone) or progress has stalled for longer than
// stageIdleTimeout. Each task-completion subscription handler should send
// (non-blocking) to progress on every result. The absolute wall-clock cap
// shuffleStageTimeout still applies as a backstop for pathological cases
// where progress signals leak (lost NATS subscription).
//
// In-flight forward progress: a Coordinator may attach itself via the
// optional perTaskProgressBridge parameter. When non-nil, the bridge is
// expected to feed `progress` on TaskProgress messages from workers
// (rows being pushed through a long-running task), so a slow-but-healthy
// task keeps the stage alive. Pass nil for stages that don't benefit
// (terminal Gather, single-task stages).
//
// Returns nil on completion, ctx.Err() on cancellation, or a stuck/timeout
// error otherwise.
func awaitStageProgress(ctx context.Context, allDone <-chan struct{}, progress <-chan struct{}, label string) error {
	const tickEvery = 30 * time.Second
	ticker := time.NewTicker(tickEvery)
	defer ticker.Stop()
	lastProgress := time.Now()
	deadline := time.Now().Add(shuffleStageTimeout)
	for {
		select {
		case <-allDone:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-progress:
			lastProgress = time.Now()
		case now := <-ticker.C:
			if now.Sub(lastProgress) > stageIdleTimeout {
				return fmt.Errorf("stage %s: idle for %s with no task progress (likely worker crash, deadlock, or lost result publish)",
					label, stageIdleTimeout)
			}
			if now.After(deadline) {
				return fmt.Errorf("stage %s: exceeded absolute timeout %s", label, shuffleStageTimeout)
			}
		}
	}
}

// executeStageDAG is the native-DAG distributed executor. It walks the
// Exchange-annotated stage DAG in topological order, dispatches each
// stage via its Type-specific helper, records outputs in a per-query
// stageOutputs map, and terminates when it hits a Gather stage (which
// streams results to the coordinator directly).
//
// See spec:
// docs/archive/specs/2026-04-22-distribution-native-dag-execution-design.md
func (c *Coordinator) executeStageDAG(
	ctx context.Context,
	queryID, sql string,
	stages []physical.Stage,
	workerCount int,
) (*gatherResult, error) {
	if len(stages) == 0 {
		return nil, fmt.Errorf("executeStageDAG: empty stage list")
	}

	// Fail-fast on plan shapes the dispatchers can't consume — see
	// physical.ValidateNativeDAGShape. The 2026-04-23 SF10 A/B blew 10
	// minutes on a multi-merge-tree timeout before surfacing the shape
	// mismatch; this catches the same class at plan time.
	if err := physical.ValidateNativeDAGShape(stages); err != nil {
		return nil, err
	}

	// Per-task progress bridge: fans worker-emitted TaskProgress
	// messages out to per-stage progress channels so a slow-but-healthy
	// task keeps stageIdleTimeout from firing. Each dispatch helper
	// registers its progress channel under the stage_id; the bridge
	// routes by that field. Bridge is closed on query exit to drop
	// the underlying NATS subscription.
	bridge, err := newStageProgressBridge(c.nc, c.dpSrv, queryID)
	if err != nil {
		c.logger.Warn("task-progress bridge subscribe failed; falling back to completion-only progress",
			"query", queryID, "error", err)
	}
	defer bridge.Close()
	ctx = withStageProgressBridge(ctx, bridge)

	// Register parent queryID in the tracker so SubjectQueryActive replies
	// "active" for this query. Without this the worker's pre-execute
	// is-query-still-active probe (worker.go:402) gets "0" back and terms
	// the task message silently. Compute stages work because
	// dispatchComputeStage registers its own ephemeral stageQueryID; the
	// Gather task rides on the parent queryID which is ONLY registered by
	// the legacy ExecuteSQL code path that native-DAG bypasses at line 479.
	trackerStages := make(map[string]*StageInfo, len(stages))
	stageOrder := make([]string, 0, len(stages))
	for _, s := range stages {
		trackerStages[s.ID] = &StageInfo{
			StageID:      s.ID,
			Type:         distributed.TaskType(s.Type),
			TotalTasks:   s.Tasks,
			Dependencies: s.Dependencies,
		}
		stageOrder = append(stageOrder, s.ID)
	}
	c.tracker.Register(queryID, sql, trackerStages, stageOrder)
	c.tracker.Start(queryID)
	// Mark Complete (not Delete) so GetQueryStatus / GetQueryResults can
	// observe the finished query. ReapCompleted (cleanup.go) prunes old
	// completed entries on a periodic sweep — same pattern legacy uses.
	// cleanupQuery purges KV entries + queries/<id>/* in the object store;
	// without it the data dir grows unbounded across runs (the legacy
	// path at coordinator.go:1712 does the same call).
	defer func() {
		c.tracker.Complete(queryID)
		c.cleanupQuery(queryID)
	}()
	// Drop this query's eager feeds (stops manifest republishers). No-op
	// when eager dispatch is off or the plan registered none.
	defer c.dropEagerFeeds(queryID)

	// Separate the terminal Gather from the DAG body: Gather is always run
	// last, on the coordinator's NATS reply subscription, and returns the
	// final result batches. Everything else is dispatched in waves where
	// each wave runs all ready sibling stages concurrently (sibling = all
	// dependencies already satisfied). Without concurrent waves, plans with
	// fan-out patterns like the multi-level merge_aggregate/merge_sort tree
	// (80+ independent siblings per query at SF10) serialize to one stage
	// per coordinator-to-worker round-trip — catastrophically slow.
	var gatherStage physical.Stage
	var hasGather bool
	pending := make(map[string]physical.Stage, len(stages))
	stageByID := make(map[string]physical.Stage, len(stages))
	for _, s := range stages {
		stageByID[s.ID] = s
		if s.Type == physical.StageExchangeGather {
			if hasGather {
				return nil, fmt.Errorf("executeStageDAG: plan has multiple Gather stages")
			}
			gatherStage = s
			hasGather = true
			continue
		}
		pending[s.ID] = s
	}
	if !hasGather {
		return nil, fmt.Errorf("executeStageDAG: plan for query %s has no Gather stage", queryID)
	}

	// Gather fusion: if the gather stage's only upstream is a final_aggregate
	// or merge_aggregate that's already migrated to the fragment path, fold
	// OpGatherSink into that fragment so workers stream batches directly to
	// the coordinator's NATS reply subject — eliminates the standalone
	// gather task dispatch + the unpartitioned .wshf upload+download hop the
	// gather task would otherwise perform.
	//
	// Subscribed early (with expectedTerminals=0 sentinel) so no per-task
	// terminal can race past us; SetExpectedTerminals is called from the
	// upstream stage's dispatcher once numTasks is known. Defer Unsubscribe
	// covers any error path that returns before recv.wait() fires (wait()
	// also unsubscribes; nats Unsubscribe is idempotent).
	var fusion *gatherFusion
	var fuseStageID string
	if depID, ok := canFuseGather(gatherStage, pending); ok {
		replySubject := fmt.Sprintf("wadjet.gather.%s", queryID)
		recv, err := subscribeGather(c.nc, replySubject, 0, c.workers, c.gatherResultBudget())
		if err != nil {
			c.logger.Warn("gather fusion: subscribe failed; falling back to legacy gather",
				"query", queryID, "fuse_stage_id", depID, "error", err)
		} else {
			fusion = &gatherFusion{recv: recv, replySubject: replySubject}
			fuseStageID = depID
			c.logger.Info("gather fusion enabled",
				"query", queryID, "fuse_stage_id", depID,
				"reply_subject", replySubject)
			defer recv.sub.Unsubscribe()
			// Remove unclaimed spill scratch on every path that returns
			// before wait() hands the result off (dispatch errors, ctx
			// cancellation). No-op after a successful wait().
			defer recv.discard()
			defer recv.registerWithDataPlane(c.dpSrv, queryID)()
		}
	}

	// Mark every leaf scan that's the PROBE of a downstream broadcast_join.
	// dispatchPipelineStage uses this to row-group-shard a single oversized
	// probe file (e.g. SF10 partsupp at 1.4 GB pre-compacted) so the
	// downstream broadcast_join can probe-split across the cluster. We only
	// shard the probe side; build-side scans stay single-file because the
	// broadcast cache is replicated to every join task — sharding the build
	// would multiply the cache load N-fold and tank queries like Q05 that
	// depend on small-dim broadcast joins (observed on the 2026-05-03 SF10
	// fix-verify deploy of 970374a, where Q05 regressed 3m7s → 10m47s).
	probeOfBroadcast := make(map[string]bool, len(stages))
	for _, s := range stages {
		if s.Type != physical.StageBroadcastJoin {
			continue
		}
		probeDep := s.LeftDepStage
		if probeDep == "" && len(s.Dependencies) > 0 {
			probeDep = s.Dependencies[0]
		}
		if probeDep != "" {
			probeOfBroadcast[probeDep] = true
		}
	}

	outputs := make(map[string]StageOutput, len(stages))
	var outputsMu sync.Mutex

	// Per-stage completion signal. Each stage goroutine closes its `done`
	// channel when its output is published into `outputs`. Dependent
	// stages block on the union of their dep signals before dispatching
	// their own work. This is a straight DAG scheduler — each stage
	// starts as soon as its own dependencies are satisfied, instead of
	// waiting for an entire wave to drain. Plans with parallel branches
	// of unequal length (e.g., two sides of a shuffle join where one
	// side has an extra filter stage) now overlap properly instead of
	// stalling on the longest path in each wave.
	done := make(map[string]chan struct{}, len(pending))
	for id := range pending {
		done[id] = make(chan struct{})
	}
	// Cap how many stages can be dispatching concurrently to keep coord-side
	// result-collection state (NATS subscriptions, in-flight task batches,
	// per-stage buffers) bounded. Without this, every "ready" stage in a
	// wave dispatches simultaneously — for Q18 SF10 (17 stages, multiple
	// fan-outs ready at once) the resulting coord+worker total RSS routinely
	// overshoots the host's physical memory and the OS OOM-kills a process.
	//
	// 2 * workerCount keeps workers saturated (each worker has typically
	// max_concurrent>=2 tasks) while bounding the number of in-flight stage
	// pipelines coord must track to a small multiple of the cluster size.
	// The semaphore is acquired AFTER all upstream dependencies are
	// satisfied, so it can never deadlock waiting on a producer that itself
	// can't acquire a slot.
	// Source the slot count from the actual cluster capacity (sum of each
	// worker's auto-tuned max_concurrent reported in heartbeats) when
	// available. Workers downscale max_concurrent under memory pressure
	// (auto-detected memory budget logic in cmd/wadjet), so this gives the
	// dispatcher a memory-aware backpressure signal: if every worker has
	// shrunk to max_concurrent=2 because the box is tight, dispatch only
	// queues that many stages at a time instead of stampeding 8+ in a wave.
	// Falls back to 2 * workerCount when no worker has reported
	// MaxConcurrent yet (cluster startup, legacy workers).
	dispatchSlots := c.workers.ClusterCapacity()
	dispatchSource := "cluster_capacity"
	if dispatchSlots <= 0 {
		dispatchSlots = 2 * workerCount
		dispatchSource = "fallback_2x_workerCount"
	}
	if dispatchSlots < 2 {
		dispatchSlots = 2
		dispatchSource = "floor"
	}
	dispatchSem := make(chan struct{}, dispatchSlots)
	c.logger.Info("stage-DAG dispatch", "query", queryID,
		"stages", len(pending), "dispatch_concurrency", dispatchSlots,
		"dispatch_source", dispatchSource)

	g, gctx := errgroup.WithContext(ctx)
	for _, s := range pending {
		s := s
		g.Go(func() error {
			// Eager consumer dispatch (docs/design/eager-consumer-dispatch.md
			// §3.3): an eligible consumer may clear on its producers' feeds
			// (task layout fixed, manifests flowing) instead of the done
			// barrier. Feed handles are get-or-create so they exist before
			// the producers' dispatchers fill them in; a producer that never
			// dispatches (empty upstream, non-shuffle path) leaves its feed
			// inert and the done case fires as before.
			//
			// Two shapes: C1 non-join consumers clear on their single dep's
			// dispatch; C2 hash-join consumers clear on BOTH primary deps at
			// the memo-§6 early-skew decision point (feed threshold + a
			// projected planSkewSplitTasks decision — a would-split edge
			// keeps the barrier so the full-data split governs; slice 2 adds
			// frozen eager split layouts). Fused-build deps always keep the
			// barrier: their real outputs ride task.FusedJoins[i].BuildFiles.
			eagerFeeds := map[string]*eagerFeed{}
			eagerJoin := false
			if c.config.EagerDispatch {
				if eagerEligibleConsumer(s, stageByID, fuseStageID, workerCount) {
					eagerFeeds[s.Dependencies[0]] = c.eagerFeedHandle(queryID, s.Dependencies[0])
				} else if eagerEligibleJoinConsumer(s, stageByID, fuseStageID) {
					probeDep, buildDep := joinPrimaryDeps(s)
					eagerFeeds[probeDep] = c.eagerFeedHandle(queryID, probeDep)
					eagerFeeds[buildDep] = c.eagerFeedHandle(queryID, buildDep)
					eagerJoin = true
				}
				for dep, f := range eagerFeeds {
					if f == nil { // flag raced off / no root ID
						delete(eagerFeeds, dep)
					}
				}
				if eagerJoin && len(eagerFeeds) != 2 {
					eagerFeeds = map[string]*eagerFeed{}
				}
			}
			// Wait on each dependency's done signal. Deps not in the
			// `done` map are leaf stages / pre-computed outputs (e.g.,
			// the coordinator's initial inputs) and are treated as
			// immediately ready. Eager-capable deps race their feed's
			// dispatch against the barrier; if ANY of them completes
			// first, the whole stage takes the barrier path (it has real
			// outputs; mixing real and provisional gains nothing).
			eagerBroken := len(eagerFeeds) == 0
			for _, depID := range s.Dependencies {
				ch, tracked := done[depID]
				if !tracked {
					continue
				}
				if f, ok := eagerFeeds[depID]; ok && !eagerBroken {
					select {
					case <-ch:
						eagerBroken = true
					case <-f.dispatched:
					case <-gctx.Done():
						return gctx.Err()
					}
					continue
				}
				select {
				case <-ch:
				case <-gctx.Done():
					return gctx.Err()
				}
			}
			// Decision-point hold: EVERY eager consumer (C1 and C2) waits
			// for its feeds to cross the decision threshold before
			// clearing (decisionReady always closes at or before producer
			// completion, so this cannot outwait a healthy producer; a
			// failed producer cancels gctx). Joins needed this for the
			// projected skew decision from the start; C1 consumers now
			// hold too, because the projected-tail gate below needs a
			// wave of completion timestamps. The eagerness lost is the
			// first producer wave — exactly the population §9 predicted
			// (and the C3 pair measured) as yielding nothing.
			if !eagerBroken {
				for _, f := range eagerFeeds {
					select {
					case <-f.decisionReady:
					case <-gctx.Done():
						return gctx.Err()
					}
				}
			}
			// Projected-tail gate (C3 pair, eager-consumer-dispatch.md
			// §10): eager clearance converts only when a producer still
			// has a long completion tail to overlap — every measured edge
			// with spread ≥ ~12s won, every edge below ~10s paid slot-
			// occupancy tax. Gate on the LONGEST projected tail across
			// the stage's eager feeds; below the floor, take the barrier.
			if !eagerBroken {
				maxTail := 0.0
				for _, f := range eagerFeeds {
					if t := f.projectedTailSeconds(); t > maxTail {
						maxTail = t
					}
				}
				if maxTail < eagerMinTailSeconds {
					c.logger.Info("eager dispatch: projected tail below floor — keeping barrier",
						"query", queryID, "stage_id", s.ID,
						"projected_tail_s", maxTail, "floor_s", eagerMinTailSeconds)
					eagerBroken = true
				}
			}
			// C2 join clearance: apply the projected skew decision.
			// Would-split → barrier.
			if !eagerBroken && eagerJoin {
				numTasks := 1
				if s.Distribution.Kind == physical.DistHashPartitioned {
					numTasks = s.Distribution.Count
					if numTasks <= 0 {
						numTasks = workerCount
					}
				}
				probeDep, buildDep := joinPrimaryDeps(s)
				if c.eagerJoinWouldSplit(s, eagerFeeds[probeDep], eagerFeeds[buildDep], numTasks, workerCount) {
					c.logger.Info("eager dispatch: projected skew split — keeping barrier",
						"query", queryID, "stage_id", s.ID)
					eagerBroken = true
				}
			}
			// v1 producer-lane reservation: at most one eager stage in
			// flight per coordinator. A busy slot degrades to the barrier.
			eagerActive := false
			if !eagerBroken {
				select {
				case c.eagerStageSlot <- struct{}{}:
					eagerActive = true
				default:
				}
			}
			if eagerActive {
				defer func() { <-c.eagerStageSlot }()
				EagerEdgesPlanned.Add(1)
				producerTasks := 0
				for _, f := range eagerFeeds {
					producerTasks += len(f.producerTaskIDs)
				}
				c.logger.Info("eager dispatch: consumer cleared early",
					"query", queryID, "stage_id", s.ID, "stage_type", s.Type,
					"eager_deps", len(eagerFeeds), "join", eagerJoin,
					"producer_tasks", producerTasks)
			} else if len(eagerFeeds) > 0 {
				// Barrier fallback: wait out every eager dep's done signal
				// (non-eager deps were already awaited above).
				for depID := range eagerFeeds {
					ch, tracked := done[depID]
					if !tracked {
						continue
					}
					select {
					case <-ch:
					case <-gctx.Done():
						return gctx.Err()
					}
				}
				eagerFeeds = map[string]*eagerFeed{}
			}
			// Late-bind any scalar subqueries: await each producer stage
			// separately (producer IDs are NOT in Dependencies because
			// their output feeds into FilterExprs via string substitution
			// rather than flowing in as record batches), then extract the
			// single scalar and substitute the placeholder.
			if len(s.ScalarDependencies) > 0 {
				for _, pid := range s.ScalarDependencies {
					ch, tracked := done[pid]
					if !tracked {
						continue
					}
					select {
					case <-ch:
					case <-gctx.Done():
						return gctx.Err()
					}
				}
				outputsMu.Lock()
				prod := make(map[string]StageOutput, len(s.ScalarDependencies))
				prodStages := make(map[string]physical.Stage, len(s.ScalarDependencies))
				for _, pid := range s.ScalarDependencies {
					prod[pid] = outputs[pid]
					if ps, ok := stageByID[pid]; ok {
						prodStages[pid] = ps
					}
				}
				outputsMu.Unlock()
				var subErr error
				s, subErr = c.substituteScalarDependencies(gctx, s, prod, prodStages)
				if subErr != nil {
					return fmt.Errorf("stage %s scalar substitution: %w", s.ID, subErr)
				}
			}
			var inputs map[string]StageOutput
			var err error
			if eagerActive {
				// Eager deps' producers are still running; their slots in
				// `outputs` are empty. Synthesize provisional outputs (empty
				// layout, unknown bytes, feed attached) — dispatchComputeStage
				// routes those aliases through Task.EagerInputs. Non-eager
				// deps (fused builds) completed above and use real outputs.
				inputs = make(map[string]StageOutput, len(s.Dependencies))
				outputsMu.Lock()
				for _, depID := range s.Dependencies {
					if f, ok := eagerFeeds[depID]; ok {
						inputs[depID] = f.provisionalOutput()
						continue
					}
					out, ok := outputs[depID]
					if !ok {
						outputsMu.Unlock()
						return fmt.Errorf("stage %s: non-eager dependency %s has no recorded output", s.ID, depID)
					}
					inputs[depID] = out
				}
				outputsMu.Unlock()
			} else {
				outputsMu.Lock()
				inputs, err = collectInputs(s, outputs)
				outputsMu.Unlock()
				if err != nil {
					return fmt.Errorf("stage %s collect inputs: %w", s.ID, err)
				}
			}
			// Acquire a dispatch slot now that we have inputs and are
			// about to publish tasks. Held until the stage completes so
			// concurrent stage count never exceeds dispatchSlots. Released
			// via defer so it always fires, even on dispatch error.
			select {
			case dispatchSem <- struct{}{}:
			case <-gctx.Done():
				return gctx.Err()
			}
			defer func() { <-dispatchSem }()

			var stageFusion *gatherFusion
			if fusion != nil && s.ID == fuseStageID {
				stageFusion = fusion
			}
			var out StageOutput
			switch s.Type {
			case physical.StageExchangeRepartition:
				out, err = c.dispatchShuffleStage(gctx, queryID, s, inputs, workerCount)
			case physical.StageExchangeReplicate:
				out, err = c.dispatchReplicateStage(gctx, queryID, sql, s, inputs)
			default:
				out, err = c.dispatchPipelineStage(gctx, queryID, sql, s, inputs, workerCount, probeOfBroadcast[s.ID], stageFusion)
			}
			if err != nil {
				return fmt.Errorf("stage %s (%s): %w", s.ID, s.Type, err)
			}
			outputsMu.Lock()
			outputs[s.ID] = out
			outputsMu.Unlock()
			close(done[s.ID])
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Fused gather: workers already streamed batches + terminals to the
	// pre-installed receiver via OpGatherSink. The upstream stage's
	// dispatcher set expectedTerminals = numTasks before publishing, so by
	// the time errgroup completes every terminal has arrived and wait()
	// returns immediately. Skips the dispatchGatherStage round-trip
	// entirely.
	if fusion != nil {
		gr, waitErr := fusion.recv.wait(ctx, gatherReceiveTimeout)
		c.logger.Info("gather: fused wait returned",
			"query", queryID, "msg_count", fusion.recv.msgCount.Load(),
			"err", waitErr)
		if waitErr != nil {
			return gr, waitErr
		}
		if gr != nil && len(gatherStage.OutputRenames) > 0 {
			applyOutputRenames(gr, gatherStage.OutputRenames)
		}
		return gr, nil
	}

	// Terminal Gather (unfused): legacy single-task gather dispatch.
	inputs, err := collectInputs(gatherStage, outputs)
	if err != nil {
		return nil, fmt.Errorf("gather stage %s: %w", gatherStage.ID, err)
	}
	gr, gerr := c.dispatchGatherStage(ctx, queryID, sql, gatherStage, inputs, workerCount)
	if gerr != nil {
		return gr, gerr
	}
	// Apply SELECT-list aliases to the result schema. walkStages drops the
	// outer NodeProject's projections, so without this the user sees raw
	// worker column names ("n1.n_name", "substr(...)") instead of their
	// aliases ("supp_nation", "l_year"). The Gather worker is a pipe and
	// can't apply this — it has to happen here.
	if gr != nil && len(gatherStage.OutputRenames) > 0 {
		applyOutputRenames(gr, gatherStage.OutputRenames)
	}
	return gr, nil
}

// applyOutputRenames PROJECTS each gather batch (and gr.columns) to exactly
// the SELECT-list output schema described by renames: drops columns the
// worker emitted but the user didn't ask for (e.g., Q15's join carries
// supplier/lineitem internals), and renames each kept column to the user's
// alias. Match is case-insensitive to tolerate worker-side lowercasing of
// expression text.
//
// Only applies when renames is non-empty AND every entry's source column
// resolves in the batch — if any source is missing (wrapped aggregates not
// yet handled), falls back to a rename-only pass so the output is at least
// non-empty rather than truncated to nothing.
func applyOutputRenames(gr *gatherResult, renames []physical.OutputRename) {
	if gr == nil || len(renames) == 0 {
		return
	}
	// Decide project-vs-rename from the column list the receiver derived
	// from the first batch's schema (identical names). When the result is
	// fully in memory and empty, keep the historical rename-only behavior;
	// a spilled result always has decoded prefix batches (the budget is
	// only exceeded after at least one decode), so columns are present.
	br := newBatchRenamer(renames, gr.columns)
	if len(gr.batches) == 0 && gr.spillPath == "" && len(gr.columns) > 0 {
		// Columns but no batches (historical edge): rename-only. A fully
		// empty result (no columns either) keeps projecting the column
		// list to the SELECT aliases, as before.
		br.project = false
	}
	gr.columns = br.renameColumns(gr.columns)
	for bi, b := range gr.batches {
		gr.batches[bi] = br.apply(b)
	}
	if gr.spillPath != "" {
		// Replayed batches get the same transform lazily as they are
		// decoded from scratch (gatherReplayStream).
		gr.renamer = br
	}
}

// batchRenamer is the per-batch form of applyOutputRenames: one decision
// (project vs rename-only) made up front from the output column names, then
// applied to each batch — eagerly to the in-memory prefix, lazily to batches
// replayed from gather spill scratch. Both paths MUST use the same instance
// so a single query's batches all share one schema shape.
type batchRenamer struct {
	renames  []physical.OutputRename
	compiled map[int]expr.Expr // index → compiled expr; nil map = compilation failed
	project  bool
}

// newBatchRenamer compiles expression-bearing renames and decides whether a
// full projection is possible: compilation succeeded AND every non-expr
// source column resolves in columns (case-insensitive, tolerating worker-
// side lowercasing). Otherwise batches get a rename-only pass so the output
// is at least non-empty rather than truncated to nothing.
func newBatchRenamer(renames []physical.OutputRename, columns []string) *batchRenamer {
	br := &batchRenamer{renames: renames, project: true}
	br.compiled = make(map[int]expr.Expr, len(renames))
	for i, r := range renames {
		if r.Expr == nil {
			continue
		}
		e, cerr := expr.Compile(r.Expr)
		if cerr != nil {
			// Compilation failure → degrade to rename-only.
			br.compiled = nil
			br.project = false
			break
		}
		br.compiled[i] = e
	}
	if br.project && len(columns) > 0 {
		for _, r := range renames {
			if r.Expr != nil {
				continue // existence check uses expression evaluation
			}
			found := false
			for _, c := range columns {
				if strings.EqualFold(c, r.From) {
					found = true
					break
				}
			}
			if !found {
				br.project = false
				break
			}
		}
	}
	return br
}

// renameColumns returns the output column list: the renames' To names in
// order (project mode), or the input names with matches renamed in place.
func (br *batchRenamer) renameColumns(columns []string) []string {
	if br.project {
		out := make([]string, len(br.renames))
		for i, r := range br.renames {
			out[i] = r.To
		}
		return out
	}
	for i, c := range columns {
		columns[i] = br.renameOne(c)
	}
	return columns
}

func (br *batchRenamer) renameOne(name string) string {
	for _, r := range br.renames {
		if strings.EqualFold(name, r.From) {
			return r.To
		}
	}
	return name
}

// apply transforms one batch: project mode keeps only the renamed/computed
// columns in renames order; rename-only mode renames schema fields in place.
// Nil-safe (returns nil for nil input).
func (br *batchRenamer) apply(b *batch.RecordBatch) *batch.RecordBatch {
	if b == nil {
		return nil
	}
	if !br.project {
		for i := range b.Schema {
			b.Schema[i].Name = br.renameOne(b.Schema[i].Name)
		}
		return b
	}
	newCols := make([]*batch.Vector, len(br.renames))
	newSchema := make([]parquet.Column, len(br.renames))
	for i, r := range br.renames {
		if e, ok := br.compiled[i]; ok {
			// Expression-bearing rename: evaluate per row, build a new
			// column. Used for wrapped aggregates ("SUM(x)/7.0") whose
			// post-aggregate divisor needs to be applied at gather time.
			newCols[i] = evalExprColumn(e, b)
			newSchema[i] = parquet.Column{Name: r.To, Type: parquet.TypeFloat64, Nullable: true}
			continue
		}
		srcIdx := -1
		for j, c := range b.Schema {
			if strings.EqualFold(c.Name, r.From) {
				srcIdx = j
				break
			}
		}
		if srcIdx < 0 {
			// Should not happen — project was decided from these names.
			continue
		}
		newCols[i] = b.Columns[srcIdx]
		newSchema[i] = b.Schema[srcIdx]
		newSchema[i].Name = r.To
	}
	return &batch.RecordBatch{
		Schema:  newSchema,
		Columns: newCols,
		Len:     b.Len,
		Sel:     b.Sel,
	}
}

// evalExprColumn builds a new Vector by evaluating e against each row of b.
// Used by applyOutputRenames to materialize wrapped-aggregate columns at
// gather time. Output type is float64 (the dominant case for wrapped
// aggregates: SUM/N, AVG-like ratios). Other types may need extension when
// new query shapes surface.
func evalExprColumn(e expr.Expr, b *batch.RecordBatch) *batch.Vector {
	v := batch.NewVector(parquet.TypeFloat64, b.Len)
	if cap(v.Float64Data) < b.Len {
		v.Float64Data = make([]float64, b.Len)
	} else {
		v.Float64Data = v.Float64Data[:b.Len]
	}
	emit := func(row, dst int) {
		val := e.Eval(b, row)
		if val == nil {
			v.Nulls.SetNull(dst)
			return
		}
		switch x := val.(type) {
		case float64:
			v.Float64Data[dst] = x
			v.Nulls.SetValid(dst)
		case float32:
			v.Float64Data[dst] = float64(x)
			v.Nulls.SetValid(dst)
		case int64:
			v.Float64Data[dst] = float64(x)
			v.Nulls.SetValid(dst)
		case int32:
			v.Float64Data[dst] = float64(x)
			v.Nulls.SetValid(dst)
		case int:
			v.Float64Data[dst] = float64(x)
			v.Nulls.SetValid(dst)
		case bool:
			if x {
				v.Float64Data[dst] = 1
			}
			v.Nulls.SetValid(dst)
		default:
			v.Nulls.SetNull(dst)
		}
	}
	if b.Sel != nil {
		for i, src := range b.Sel {
			emit(int(src), i)
		}
	} else {
		for i := 0; i < b.Len; i++ {
			emit(i, i)
		}
	}
	return v
}

// dispatchShuffleStage executes a StageExchangeRepartition: reads upstream
// output, hash-partitions on stage.Exchange.Keys, writes one .wshf per
// (task, partition) to the query's shuffle prefix.
//
// Phase 3 scaffolding: wraps the existing runShuffleSide helper by adapting
// the upstream StageOutput's files into a synthetic source stage. The legacy
// helper expects a physical.Stage with ScanFiles populated — we satisfy
// that by flattening the upstream output.
//
// Known limitations (follow-up commits):
//   - Chained shuffles (Repartition consuming a previous Repartition's
//     partitioned output) use flattenStageFiles which loses partition
//     information. Correct end-to-end because the next shuffle rehashes,
//     but fails to exploit co-located partitions.
//   - Column pruning uses stage.Columns which may be over-approximated for
//     non-scan upstream stages.
func (c *Coordinator) dispatchShuffleStage(
	ctx context.Context,
	queryID string,
	stage physical.Stage,
	inputs map[string]StageOutput,
	workerCount int,
) (StageOutput, error) {
	if stage.Exchange == nil {
		return StageOutput{}, fmt.Errorf("repartition stage %s missing Exchange payload", stage.ID)
	}
	if len(stage.Dependencies) != 1 {
		return StageOutput{}, fmt.Errorf("repartition stage %s expects 1 dep, got %d", stage.ID, len(stage.Dependencies))
	}
	depID := stage.Dependencies[0]
	upstream, ok := inputs[depID]
	if !ok {
		return StageOutput{}, fmt.Errorf("missing input for dep %s", depID)
	}
	sourceFiles := flattenStageFiles(upstream)
	if len(sourceFiles) == 0 {
		numParts := stage.Exchange.Count
		return StageOutput{
			Kind:          OutputPartitioned,
			NumPartitions: numParts,
			Files:         make([][]string, numParts),
		}, nil
	}
	// Synthesize a source stage for runShuffleSide. EstimatedBytes carries
	// the upstream's worker-reported output size so the shuffle tasks get
	// per-task admission estimates (runShuffleSide splits it per task).
	// ScanTable/ScanColumns ride along when the upstream is a pass-through
	// leaf scan, so runShuffleSide's prunedScanColumns can narrow the
	// shuffle tasks' parquet reads — otherwise scan-absorbed legs shuffle
	// every column of the base table (memo §2 A2). Chained shuffles leave
	// both empty: WSHF inputs ignore column projection.
	synthTable := stage.TableName
	if synthTable == "" {
		synthTable = upstream.ScanTable
	}
	synthetic := physical.Stage{
		ID:             stage.ID + "-src",
		Type:           physical.StageScan,
		ScanFiles:      sourceFiles,
		TableName:      synthTable,
		Columns:        upstream.ScanColumns,
		EstimatedBytes: upstream.Bytes,
	}
	numParts := stage.Exchange.Count
	if numParts == 0 {
		numParts = workerCount * shufflePartitionMultiplier
	}
	// Forward any dynamic-filter specs the upstream (probe-side) leaf scan
	// resolved through pass-through. Each shuffle task gets a copy so its
	// parquet scan applies the bloom at row-group level.
	//
	// Eager dispatch: hand runShuffleSide this stage's feed so eligible
	// consumers can clear their barrier once the shuffle tasks are built.
	// nil when the flag is off. The empty-source early return above never
	// dispatches the feed — consumers just fall through on done[dep].
	shards, shardStats, err := c.runShuffleSide(ctx, queryID, "stage-"+stage.ID, synthetic, stage.Exchange.Keys, numParts, workerCount, upstream.DynamicFilters, c.eagerFeedHandle(queryID, stage.ID))
	if err != nil {
		return StageOutput{}, err
	}
	return StageOutput{
		Kind:           OutputPartitioned,
		NumPartitions:  numParts,
		Files:          shards,
		Bytes:          shardStats.TotalBytes,
		PartitionRows:  shardStats.PartitionRows,
		PartitionBytes: shardStats.PartitionBytes,
	}, nil
}

// replicateMaterializeMinFiles is the minimum upstream file count that
// triggers materialization in dispatchReplicateStage. Below this, the
// upstream is small enough that having every probe-split task read the
// raw files is fine; at or above, we consolidate so the per-task readers
// pay parquet decode cost once instead of N times.
//
// Declared as var (not const) so tests can lower it to force the
// materialization path on small fixtures.
var replicateMaterializeMinFiles = 2

// allWSHF reports whether every file in the list has the .wshf suffix.
// Native-DAG upstreams ALL write WSHF — the only path that would feed
// raw parquet into a replicate is a (legacy) plan that skipped the
// intermediate scan-filter stage, which the current planner never
// emits. When all upstreams are WSHF, the consolidation that
// materializeReplicate provides has no CPU payoff (mmap-walk a single
// 154 MB cache vs mmap-walk N WSHF chunks is essentially identical
// I/O cost), yet the serial single-task materialize burns wall time
// — 2 m 19 s on Q21 SF100 c9716f7 just to consolidate 5 WSHF files
// totalling ~150 MB (project_streaming_shuffle_sf100_win_2026-05-22
// follow-up profile analysis). Returns false for an empty list to
// preserve the existing "pass through" semantics — len() == 0 hits
// other guards.
func allWSHF(files []string) bool {
	if len(files) == 0 {
		return false
	}
	for _, f := range files {
		if !strings.HasSuffix(f, ".wshf") {
			return false
		}
	}
	return true
}

// dispatchReplicateStage executes a StageExchangeReplicate: reads upstream
// output and materializes it into a single broadcast cache that all
// downstream workers read in full.
//
// When the upstream has fewer than replicateMaterializeMinFiles files
// (or is already OutputReplicated, i.e. broadcast-shaped), we pass the
// file list through unchanged — every downstream task still reads the
// full set, but the consolidation overhead would not pay back. When the
// upstream is multi-file OutputSinglePart / OutputPartitioned, we spawn
// one scan task that reads every upstream file and writes a single WSHF
// cache. Downstream broadcast_join probe-split tasks all read that one
// cache instead of re-decoding the upstream parquet N times.
func (c *Coordinator) dispatchReplicateStage(
	ctx context.Context,
	queryID, sql string,
	stage physical.Stage,
	inputs map[string]StageOutput,
) (StageOutput, error) {
	_ = sql
	if len(stage.Dependencies) != 1 {
		return StageOutput{}, fmt.Errorf("replicate stage %s expects 1 dep, got %d", stage.ID, len(stage.Dependencies))
	}
	upstream := inputs[stage.Dependencies[0]]
	upstreamFiles := flattenStageFiles(upstream)

	if upstream.Kind == OutputReplicated || len(upstreamFiles) < replicateMaterializeMinFiles || allWSHF(upstreamFiles) {
		// allWSHF bypass: native-DAG upstreams write WSHF, mmap-readable
		// downstream with no per-file decode cost. Materialize provides no
		// CPU savings here and adds a 2 m+ serial-task wall on Q21 SF100.
		// Pass through; downstream probe-split tasks each open all N
		// upstream files directly via cachedFileStreamSource (mmap, cheap).
		return StageOutput{
			Kind:  OutputReplicated,
			Files: [][]string{upstreamFiles},
			Bytes: upstream.Bytes,
		}, nil
	}

	cacheFiles, cacheBytes, err := c.materializeReplicate(ctx, queryID, stage, stage.Dependencies[0], upstreamFiles, upstream.Bytes)
	if err != nil {
		// Materialization failure must not break the query — fall back to
		// pass-through. Workers can still read the raw upstream files; we
		// just lose the consolidation benefit. Logged at Warn so an unhealthy
		// cluster is visible.
		c.logger.Warn("dispatchReplicateStage: materialization failed, falling back to pass-through",
			"stage_id", stage.ID, "upstream_files", len(upstreamFiles), "err", err)
		return StageOutput{
			Kind:  OutputReplicated,
			Files: [][]string{upstreamFiles},
			Bytes: upstream.Bytes,
		}, nil
	}

	return StageOutput{
		Kind:  OutputReplicated,
		Files: [][]string{cacheFiles},
		Bytes: cacheBytes,
	}, nil
}

// materializeReplicate dispatches a single TaskTypeStage scan task that reads
// the upstream files and writes a consolidated WSHF that downstream readers
// can share. Returns the cache file paths (typically a single .wshf key).
//
// The consolidation task uses the same scan-stage handler as a leaf scan;
// it streams batches from upstream into writeUnpartitionedWSHF without
// running any operator. cachedFileStreamSource auto-detects parquet vs WSHF
// inputs, so the upstream's file kind is transparent.
func (c *Coordinator) materializeReplicate(
	ctx context.Context,
	queryID string,
	stage physical.Stage,
	upstreamID string,
	files []string,
	estBytes int64, // upstream output size; the task reads all of it
) ([]string, int64, error) {
	resultPrefix := fmt.Sprintf("queries/%s/%s/", queryID, stage.ID)
	task := distributed.Task{
		ID:             uuid.New().String()[:8],
		QueryID:        queryID,
		StageID:        stage.ID,
		Type:           distributed.TaskTypeStage,
		DataBucket:     c.config.ResultBucket,
		ResultBucket:   c.config.ResultBucket,
		ResultPrefix:   resultPrefix,
		EstimatedBytes: estBytes,
		CreatedAt:      time.Now(),
		// Fragment dispatch: stream upstream files through OpScan into an
		// unpartitioned WSHF sink. cachedFileStreamSource auto-detects
		// parquet vs WSHF, so the consolidation pattern (this caller's job)
		// drops in unchanged.
		Operators: []distributed.OpSpec{
			{
				Type:        distributed.OpScan,
				InputAlias:  upstreamID,
				InputFiles:  files,
				InputBucket: c.config.ResultBucket,
			},
			{
				Type: distributed.OpUnpartitionedSink,
			},
		},
	}
	if clusterID := c.catalog.ClusterID(); clusterID != "" {
		task.ClusterID = clusterID
	}
	c.mu.Lock()
	qm := c.queryMetas[queryID]
	c.mu.Unlock()
	c.enrichTaskWithQueryContext(qm, &task)

	stageQueryID := fmt.Sprintf("st-%s-%s", stage.ID, queryID)
	trackerStages := map[string]*StageInfo{
		stage.ID: {StageID: stage.ID, Type: distributed.TaskTypeStage, TotalTasks: 1},
	}
	c.tracker.Register(stageQueryID, "", trackerStages, []string{stage.ID})
	c.tracker.Start(stageQueryID)
	defer c.tracker.Delete(stageQueryID)
	task.QueryID = stageQueryID
	task.Attempt = 1

	subject := distributed.QueryResultSubject(stageQueryID)
	done := make(chan struct{}, 1)
	progress := make(chan struct{}, 1)
	defer stageProgressBridgeFromContext(ctx).Register(stage.ID, progress)()
	// Materialize sinks to S3 (OpUnpartitionedSink) — retry-safe.
	retrier := newTaskRetrier([]distributed.Task{task}, true, func(t distributed.Task) {
		if ctx.Err() != nil {
			return
		}
		if pubErr := c.scheduler.PublishTasks(ctx, []distributed.Task{t}); pubErr != nil {
			c.logger.Error("materialize task retry publish failed",
				"stage_id", stage.ID, "task_id", t.ID, "error", pubErr)
		}
	}, c.logger, stage.ID, c.classifyFatalResult)
	defer c.watchStuckTasks(ctx, retrier)()
	sub, err := c.subscribeTaskResults(subject, func(msg *nats.Msg) {
		var r distributed.ResultNotification
		if uerr := distributed.Unmarshal(msg.Data, &r); uerr != nil {
			return
		}
		c.noteTaskResult(r)
		allDone := retrier.Observe(r)
		select {
		case progress <- struct{}{}:
		default:
		}
		if allDone {
			select {
			case done <- struct{}{}:
			default:
			}
		}
	})
	if err != nil {
		return nil, 0, fmt.Errorf("subscribe materialize: %w", err)
	}
	defer sub.Unsubscribe()

	c.logger.Info("dispatchReplicateStage: materializing",
		"stage_id", stage.ID, "upstream_files", len(files), "task_id", task.ID)
	if err := c.scheduler.PublishTasks(ctx, []distributed.Task{task}); err != nil {
		return nil, 0, fmt.Errorf("publish materialize: %w", err)
	}
	if err := awaitStageProgress(ctx, done, progress, "replicate "+stage.ID); err != nil {
		return nil, 0, err
	}
	if _, errMsg, failed := retrier.FirstError(); failed {
		return nil, 0, fmt.Errorf("materialize task failed after %d attempts: %s", maxTaskAttempts, errMsg)
	}
	resultFiles := retrier.Files()[0]
	if len(resultFiles) == 0 {
		return nil, 0, fmt.Errorf("materialize task returned no files")
	}
	c.logger.Info("dispatchReplicateStage: materialized",
		"stage_id", stage.ID, "input_files", len(files), "cache_files", len(resultFiles))
	return resultFiles, retrier.TotalBytes(), nil
}

// dispatchPipelineStage executes a compute stage (scan/join/aggregate/etc.)
// by dispatching workerCount pipeline tasks that each read their assigned
// slice of upstream partitioned output and run the full SQL against it.
//
// Phase 3 scaffolding: wraps buildShufflePipelineTasks when the inputs look
// like a two-sided partitioned shuffle (build + probe). Leaf scan stages
// are passed through as StageOutput{Kind: OutputSinglePart, Files: scanFiles}
// without dispatching a task — they are consumed directly by the next
// Exchange stage. This matches the Phase 2a structure where leaf scans are
// not runtime-active stages.
//
// Known limitations: multi-step pipelines that need to run an intermediate
// join and emit WSHF-encoded partitioned output to a subsequent Exchange
// stage require worker-side SQL-fragment execution. That is a follow-up
// commit; the current path handles only the single-shuffle probe pipeline.
func (c *Coordinator) dispatchPipelineStage(
	ctx context.Context,
	queryID, sql string,
	stage physical.Stage,
	inputs map[string]StageOutput,
	workerCount int,
	isBroadcastJoinProbe bool,
	fusion *gatherFusion,
) (StageOutput, error) {
	_ = sql
	// Leaf scan stage.
	//
	// The `inputs` map may be non-empty when a stat-dep edge is present
	// (probe-side leaf with ConsumeDynamicFilters depends on a build-side
	// leaf). Stat-dep deps don't change the leaf-ness of the scan; they
	// only provide upstream BuildStats. ScanFiles is the structural marker:
	// any stage with ScanFiles populated IS a leaf scan regardless of how
	// many soft deps it has.
	if stage.Type == physical.StageScan && len(stage.ScanFiles) > 0 {
		// Fused scan-aggregate: the planner marked this scan to produce
		// partial aggregates (saves the scan→aggregate round-trip in the
		// legacy single-pipeline executor). Under native-DAG we must
		// honor that signal by dispatching N partial-aggregate tasks —
		// each worker reads its slice of scan files, aggregates, and
		// emits a partial result. The downstream final_aggregate stage
		// merges across partial outputs.
		//
		// Without this path, a Singleton final_aggregate over a large
		// leaf scan ran on ONE worker reading all scan files serially —
		// at SF10 that was the dominant cost on Q01 (87s of 87s wall
		// time was the single-worker lineitem scan).
		// FusedAggGroupBy alone (no aggregate functions) is a bare
		// GROUP BY — grouping IS the work, so it must route through the
		// scan-aggregate path too. Falling through to a plain scan here
		// skipped the partial dedup AND the fused hash-partition
		// exchange, so the downstream final_aggregate's N tasks each
		// read overlapping raw-row file slices and re-emitted every
		// group per task (#166: 3 groups × 3 tasks = 9 rows).
		if len(stage.FusedAggSpecs) > 0 || len(stage.FusedAggGroupBy) > 0 {
			return c.dispatchScanAggregateStage(ctx, queryID, stage, workerCount)
		}
		// Plain scan with no fused aggregate. When the planner pushed
		// filters onto this scan (e.g., Q05's `r_name = 'ASIA'`), we
		// must actually apply them — otherwise the downstream join
		// sees every row, cartesian-multiplies, and returns way too
		// many results. Dispatch a filter-scan task that reads the
		// parquet files, applies FilterExprs, and writes a WSHF output
		// the rest of the native-DAG pipeline can consume.
		//
		// ProjectExprs likewise forces the fragment path (#169): the
		// SELECT list carries expressions that must be COMPUTED before
		// the gather — raw-parquet passthrough would hand the gather
		// un-projected scan columns, and applyOutputRenames can only
		// rename/drop.
		if len(stage.FilterExprs) > 0 || len(stage.ProjectExprs) > 0 || len(stage.SecurityProjectExprs) > 0 {
			return c.dispatchScanFilterStage(ctx, queryID, stage, inputs, workerCount)
		}
		// No filter: by default pass raw parquet files through (saves the
		// wshf write+read hop). But when this scan is the PROBE of a
		// downstream broadcast_join AND it's a single oversized file
		// (SF10 partsupp pre-compacted at 1.4 GB → 1 file), the
		// pass-through cascades single-tasked through the join because
		// broadcastJoinProbeSplit's >=2-file gate skips. Routing through
		// scan-filter (with empty filters) row-group-shards the single
		// file across the cluster and emits multiple output files, which
		// the downstream broadcast_join can then probe-split.
		//
		// Build-side scans of broadcast_joins are explicitly NOT sharded:
		// the broadcast cache is replicated to every join task, so each
		// task would have to load N cache files instead of 1. Q05 SF10
		// regressed 3m7s → 10m47s when the prior attempt at this fix
		// sharded all single-file plain leaf scans regardless of role.
		if isBroadcastJoinProbe && len(stage.ScanFiles) == 1 {
			capacity := c.workers.ClusterCapacity()
			if scanShardCountForSingleFile(workerCount, capacity, stage.EstimatedBytes) > 1 {
				return c.dispatchScanFilterStage(ctx, queryID, stage, inputs, workerCount)
			}
		}
		// Build-side dynamic-filter emit must dispatch real tasks so the
		// emit op runs in the fragment pipeline and uploads its partial.
		// Consume-only (probe-side) stays as pass-through — the downstream
		// shuffle dispatcher threads the materialized DynamicFilters into
		// its shuffle tasks, which apply them at the row-group iterator
		// during their parquet scan. This avoids an extra dispatched scan-
		// filter hop that regressed Q18 SF1 +16% in the first wiring.
		if len(stage.EmitDynamicFilters) > 0 {
			return c.dispatchScanFilterStage(ctx, queryID, stage, inputs, workerCount)
		}
		files := append([]string(nil), stage.ScanFiles...)
		out := StageOutput{
			Kind:        OutputSinglePart,
			Files:       [][]string{files},
			Bytes:       stage.EstimatedBytes, // catalog-true file sizes
			ScanTable:   stage.TableName,
			ScanColumns: append([]string(nil), stage.Columns...),
		}
		// Materialize Consume specs into wire form so downstream shuffle/
		// stage dispatchers can ship them in their task descriptors
		// without having to re-walk the plan.
		if len(stage.ConsumeDynamicFilters) > 0 {
			out.DynamicFilters = dynamicFilterSpecsFromBuildStats(stage.ConsumeDynamicFilters, inputs)
			c.logger.Info("dynamic_filter: consume specs attached to pass-through scan output",
				"stage_id", stage.ID,
				"requested", len(stage.ConsumeDynamicFilters),
				"attached", len(out.DynamicFilters))
		}
		return out, nil
	}
	// Compute stage: emit workerCount TaskTypeStage tasks, each reading its
	// slice of partitioned input and writing unpartitioned .wshf output.
	// The stage's output is collected into a Partitioned StageOutput where
	// partition p = the files produced by worker p — downstream Exchange
	// stages treat this as partitioned if they need to re-hash.
	return c.dispatchComputeStage(ctx, queryID, stage, inputs, workerCount, fusion)
}

// scanFanOutTaskCount picks the number of scan-side tasks for stage fan-out.
// Returns max(workerCount, capacity), capped at fileCount and floored at 1.
//
// Prefers cluster capacity (sum of each worker's auto-tuned MaxConcurrent
// reported in heartbeats) over the raw worker count because workers
// downscale concurrency under memory pressure. With 3 workers each running
// MaxConcurrent=2 the cluster can run 6 tasks at once; sizing scan fan-out
// to 3 leaves half the cluster idle AND doubles per-task memory pressure
// (each task reads twice as many files).
//
// Falls back to workerCount when capacity == 0 — typical for the first
// query post-cluster-startup, before any heartbeat has reported MaxConcurrent.
func scanFanOutTaskCount(workerCount, capacity, fileCount int) int {
	n := workerCount
	if capacity > n {
		n = capacity
	}
	if n > fileCount {
		n = fileCount
	}
	if n < 1 {
		n = 1
	}
	return n
}

// singleFileShardThresholdBytes is the minimum file size that triggers
// row-group sharding for a single-file scan. Below this threshold the
// scan runs as one task (whole file); above it the dispatcher fans out
// N row-group shards so the downstream broadcast-join chain can probe-
// split (which requires probe upstream to have ≥ 2 files).
//
// 64 MB is small enough to catch any moderately-compacted production
// file (SF10 partsupp = 691 MB single file fans out cleanly) and large
// enough that small dimension tables (region/nation/supplier) stay
// single-task and don't waste a worker slot on a few KB of data.
//
// Var rather than const so tests can lower it to force the sharded path
// on small fixtures.
var singleFileShardThresholdBytes int64 = 64 * 1024 * 1024

// scanShardCountForSingleFile returns the number of row-group shard tasks
// to fan out for a single large file. Returns 1 (no sharding) when the
// file is below the threshold or when the cluster is too small to
// benefit. Caps at the cluster's effective task capacity so we don't
// over-shard a small cluster.
func scanShardCountForSingleFile(workerCount, capacity int, fileBytes int64) int {
	if fileBytes < singleFileShardThresholdBytes {
		return 1
	}
	n := workerCount
	if capacity > n {
		n = capacity
	}
	if n < 2 {
		return 1
	}
	return n
}

// dispatchScanAggregateStage dispatches N partial-aggregate tasks, one
// per worker, each reading a disjoint slice of stage.ScanFiles. Each task
// runs a HashAggregate on its file slice and emits a partial result as
// its stage output. Downstream final_aggregate stages (which are almost
// always immediately adjacent) merge the N partial outputs into the
// logical full aggregate.
//
// Why scan-side fan-out: scan is the only leaf at plan time, so a
// Singleton-distributed final_aggregate reading "the scan's output"
// serialises to one worker reading every parquet file. Splitting the
// scan N ways + computing partial aggregates in parallel is the same
// map-reduce shape the legacy executor used (via emitMergeAggregateTree
// → scan-level fused-agg + multi-level merge tree) before native-DAG
// collapsed the tree. Now we re-introduce the fan-out, but without the
// extra merge-tree levels: one partial step, one final merge.
func (c *Coordinator) dispatchScanAggregateStage(
	ctx context.Context,
	queryID string,
	stage physical.Stage,
	workerCount int,
) (StageOutput, error) {
	if workerCount <= 0 {
		workerCount = 1
	}
	// AVG cannot directly merge across partials (avg-of-averages is
	// arithmetically wrong) but it CAN be decomposed into per-partition
	// SUM and COUNT, which DO merge correctly. decomposeAvg expands
	// every AVG spec into (SUM, COUNT) twin specs with synthetic output
	// names — the worker's post-merge fold (avg_fold.go in package
	// worker) reconstructs AVG = SUM/COUNT after the final_aggregate
	// stage merges the partials. With this in place there's no
	// reason to fall back to single-task on AVG queries; Q01 SF10
	// drops from a single-worker scan of 60M rows to N-way fan-out.
	taskCount := workerCount
	capacity := c.workers.ClusterCapacity()
	var fileSets [][]string
	var shardCount int
	if len(stage.ScanFiles) == 1 {
		shardCount = scanShardCountForSingleFile(workerCount, capacity, stage.EstimatedBytes)
	}
	if shardCount > 1 {
		// Single-file row-group sharding for fused scan-aggregate. Each
		// shard task aggregates its row-group slice; the downstream
		// final_aggregate merges across the N partial outputs.
		fileSets = make([][]string, shardCount)
		for i := range fileSets {
			fileSets[i] = stage.ScanFiles
		}
		taskCount = shardCount
	} else {
		taskCount = scanFanOutTaskCount(workerCount, capacity, len(stage.ScanFiles))
		fileSets = splitFilesEvenly(stage.ScanFiles, taskCount)
	}
	actualTasks := len(fileSets)
	if actualTasks == 0 {
		return StageOutput{
			Kind:          OutputPartitioned,
			NumPartitions: 0,
			Files:         nil,
		}, nil
	}
	resultPrefix := fmt.Sprintf("queries/%s/%s/", queryID, stage.ID)
	c.logger.Info("dispatchScanAggregateStage",
		"stage_id", stage.ID, "files", len(stage.ScanFiles),
		"tasks", actualTasks, "group_by", stage.FusedAggGroupBy,
		"shard_count", shardCount)

	// Convert AggSpec → wire format once. decomposeAvg expands AVG into
	// SUM+COUNT pairs so the partial fan-out can run in parallel; the
	// worker's avg-fold step (executor_stage.go in package worker) folds
	// the synthetic columns back into AVG after the downstream
	// final_aggregate stage merges the partials.
	var aggs []distributed.AggSpec
	for _, a := range stage.FusedAggSpecs {
		aggs = append(aggs, distributed.AggSpec{
			Func:      a.Func,
			InputCol:  a.InputCol,
			OutputCol: a.OutputCol,
			InputExpr: a.InputExpr,
		})
	}
	aggs = decomposeAvg(aggs)

	tasks := make([]distributed.Task, 0, actualTasks)
	for shardIdx, files := range fileSets {
		t := distributed.Task{
			ID:          uuid.New().String()[:8],
			QueryID:     queryID,
			StageID:     stage.ID,
			Type:        distributed.TaskTypeStage,
			StageType:   "aggregate",
			TableName:   stage.TableName,
			Columns:     stage.Columns,
			GroupByCols: stage.FusedAggGroupBy,
			Aggregates:  aggs,
			// Propagate scan-pushed WHERE fragments. Without this the worker
			// aggregates every row in the file slice and ignores the query's
			// predicate — group counts match legacy but aggregate VALUES are
			// wrong. E.g. Q01's `WHERE l_shipdate <= '1998-09-02'` was being
			// silently dropped in native-DAG before this plumb.
			FilterExprs: append([]string(nil), stage.FilterExprs...),
			Inputs: map[string][]string{
				// Use the scan's table name as alias so the worker's
				// sourceForAlias opens these files via the parquet path.
				scanAliasForStage(stage): files,
			},
			DataBucket:   c.config.ResultBucket,
			ResultBucket: c.config.ResultBucket,
			ResultPrefix: resultPrefix,
			CreatedAt:    time.Now(),
		}
		if shardCount > 1 {
			t.ScanShardIndex = shardIdx
			t.ScanShardCount = shardCount
		}
		// Fragment migration: dispatch via the multi-op fragment runner.
		// Two terminal sink shapes:
		//   - downstream Repartition Exchange absorbed by
		//     fuseScanAggregateShuffle → OpExchangeSender (each task's
		//     K aggregate rows hash-partition directly to the consumer's
		//     partition layout, skipping the standalone exchange-repartition).
		//   - otherwise → OpUnpartitionedSink (one .wshf per task;
		//     downstream Singleton final_aggregate reads them all).
		var sinkOp distributed.OpSpec
		if stage.Exchange != nil && len(stage.Exchange.Keys) > 0 && stage.Exchange.Count > 0 {
			sinkOp = distributed.OpSpec{
				Type:          distributed.OpExchangeSender,
				ShuffleKeys:   append([]string(nil), stage.Exchange.Keys...),
				NumPartitions: stage.Exchange.Count,
			}
		} else {
			sinkOp = distributed.OpSpec{Type: distributed.OpUnpartitionedSink}
		}
		ops, ferr := buildScanAggregateFragment(stage, &t, files, aggs, shardIdx, shardCount, sinkOp)
		if ferr != nil {
			return StageOutput{}, fmt.Errorf("scan-agg stage %s: build fragment: %w", stage.ID, ferr)
		}
		t.Operators = ops
		if clusterID := c.catalog.ClusterID(); clusterID != "" {
			t.ClusterID = clusterID
		}
		c.mu.Lock()
		qm := c.queryMetas[queryID]
		c.mu.Unlock()
		c.enrichTaskWithQueryContext(qm, &t)
		tasks = append(tasks, t)
	}

	// Dispatch and collect (same pattern as dispatchComputeStage).
	stageQueryID := fmt.Sprintf("st-%s-%s", stage.ID, queryID)
	trackerStages := map[string]*StageInfo{
		stage.ID: {StageID: stage.ID, Type: distributed.TaskTypeStage, TotalTasks: len(tasks)},
	}
	c.tracker.Register(stageQueryID, "", trackerStages, []string{stage.ID})
	c.tracker.Start(stageQueryID)
	defer c.tracker.Delete(stageQueryID)

	// Per-task admission estimate: the catalog-true scan bytes split
	// across tasks (files are split evenly; row-group shards each read
	// ~1/N of the file).
	perTaskEst := int64(0)
	if stage.EstimatedBytes > 0 {
		perTaskEst = stage.EstimatedBytes / int64(len(tasks))
	}
	for i := range tasks {
		tasks[i].QueryID = stageQueryID
		tasks[i].Attempt = 1
		tasks[i].EstimatedBytes = perTaskEst
	}
	subject := distributed.QueryResultSubject(stageQueryID)
	done := make(chan struct{}, 1)
	progress := make(chan struct{}, len(tasks))
	// Register with the per-query progress bridge so worker-emitted
	// TaskProgress messages also count as forward progress for this
	// stage's idle detection.
	defer stageProgressBridgeFromContext(ctx).Register(stage.ID, progress)()
	// Scan-aggregate tasks sink to S3 (unpartitioned or exchange-sender),
	// never gather-fused — retry is always safe.
	retrier := newTaskRetrier(tasks, true, func(t distributed.Task) {
		if ctx.Err() != nil {
			return
		}
		if pubErr := c.scheduler.PublishTasks(ctx, []distributed.Task{t}); pubErr != nil {
			c.logger.Error("scan-agg task retry publish failed",
				"stage_id", stage.ID, "task_id", t.ID, "error", pubErr)
		}
	}, c.logger, stage.ID, c.classifyFatalResult)
	defer c.watchStuckTasks(ctx, retrier)()
	sub, err := c.subscribeTaskResults(subject, func(msg *nats.Msg) {
		var r distributed.ResultNotification
		if uerr := distributed.Unmarshal(msg.Data, &r); uerr != nil {
			return
		}
		c.noteTaskResult(r)
		allDone := retrier.Observe(r)
		select {
		case progress <- struct{}{}:
		default:
		}
		if allDone {
			select {
			case done <- struct{}{}:
			default:
			}
		}
	})
	if err != nil {
		return StageOutput{}, fmt.Errorf("scan-agg stage %s subscribe: %w", stage.ID, err)
	}
	defer sub.Unsubscribe()

	if err := c.scheduler.PublishTasks(ctx, tasks); err != nil {
		return StageOutput{}, fmt.Errorf("scan-agg stage %s publish: %w", stage.ID, err)
	}
	if err := awaitStageProgress(ctx, done, progress, "scan-agg "+stage.ID); err != nil {
		return StageOutput{}, err
	}

	if taskID, errMsg, failed := retrier.FirstError(); failed {
		return StageOutput{}, fmt.Errorf("scan-agg stage %s: task %s failed after %d attempts: %s", stage.ID, taskID, maxTaskAttempts, errMsg)
	}
	resultFiles := retrier.Files()
	if stage.Exchange != nil && len(stage.Exchange.Keys) > 0 && stage.Exchange.Count > 0 {
		// Fused scan-aggregate + shuffle: each task produced N partition
		// files at "<prefix>partition=NNNN/<task>.wshf". Bucket all files
		// across all tasks by partition number so the downstream consumer
		// reads each partition's full set. Same shape as
		// dispatchScanFilterStage's fuseShuffle path.
		numParts := stage.Exchange.Count
		shardFiles := make([][]string, numParts)
		for _, taskFiles := range resultFiles {
			for _, f := range taskFiles {
				p, parseErr := parsePartitionFromPath(f)
				if parseErr != nil {
					return StageOutput{}, fmt.Errorf("scan-agg stage %s: parsing partition from %q: %w", stage.ID, f, parseErr)
				}
				if p < 0 || p >= numParts {
					return StageOutput{}, fmt.Errorf("scan-agg stage %s: partition %d out of range [0,%d) in %q", stage.ID, p, numParts, f)
				}
				shardFiles[p] = append(shardFiles[p], f)
			}
		}
		return StageOutput{
			Kind:          OutputPartitioned,
			NumPartitions: numParts,
			Files:         shardFiles,
			Bytes:         retrier.TotalBytes(),
		}, nil
	}
	return StageOutput{
		Kind:          OutputPartitioned,
		NumPartitions: len(tasks),
		Files:         resultFiles,
		Bytes:         retrier.TotalBytes(),
	}, nil
}

// dispatchScanFilterStage runs a scan-and-filter pipeline fanned out across
// workers. Used when a leaf scan carries scan-pushed FilterExprs but no
// fused aggregate — each worker reads its slice of stage.ScanFiles, applies
// FilterExprs, and emits a partial output. Downstream stages consume the
// partitioned output the same way they consume the dispatchScanAggregateStage
// output: via partitionFilesForWorker.
//
// Pre-fan-out (single-task) was the bottleneck on Q04 SF10: scanning 600
// lineitem files in one task took >10m and tripped the new progress-based
// idle timeout (no completion signals can land for a 1-task stage). Fan-out
// makes scan-filter symmetric with scan-aggregate: workerCount tasks, each
// with a slice of files, each emits independently — completions stream in
// every minute or two, idle detection works as designed, and total wall
// drops linearly with worker count.
func (c *Coordinator) dispatchScanFilterStage(
	ctx context.Context,
	queryID string,
	stage physical.Stage,
	inputs map[string]StageOutput,
	workerCount int,
) (StageOutput, error) {
	if workerCount <= 0 {
		workerCount = 1
	}
	if len(stage.ScanFiles) == 0 {
		return StageOutput{Kind: OutputPartitioned, NumPartitions: 0, Files: nil}, nil
	}
	// Single-file sharding path: when the leaf scan has exactly one file
	// above the threshold, fan out N row-group shard tasks instead of one
	// whole-file task. Without this, a compacted single-file source
	// (e.g. SF10 partsupp = 691 MB) caps fan-out at fileCount=1, which
	// then cascades single-tasked through the entire downstream broadcast-
	// join chain (probe-split requires probe upstream to have ≥ 2 files).
	// The shards each emit one output file → the chain inherits parallelism
	// for free.
	capacity := c.workers.ClusterCapacity()
	var fileSets [][]string
	var shardCount int
	if len(stage.ScanFiles) == 1 {
		shardCount = scanShardCountForSingleFile(workerCount, capacity, stage.EstimatedBytes)
	}
	if shardCount > 1 {
		// One row-group shard per task, all reading the same single file.
		fileSets = make([][]string, shardCount)
		for i := range fileSets {
			fileSets[i] = stage.ScanFiles
		}
	} else {
		// Task count: prefer cluster capacity (workers × auto-tuned MaxConcurrent)
		// over raw worker count so each task stays under the per-worker memory
		// budget. With 3 workers × MaxConcurrent=2, that's 6 tasks instead of 3
		// — for SF10 lineitem (600 files) that's 100 files/task instead of 200,
		// which keeps each task's heap below the 32GB worker limit and avoids
		// the OOM-restart-then-idle-timeout pattern observed on Q04 SF10.
		taskCount := scanFanOutTaskCount(workerCount, capacity, len(stage.ScanFiles))
		fileSets = splitFilesEvenly(stage.ScanFiles, taskCount)
	}
	actualTasks := len(fileSets)
	if actualTasks == 0 {
		return StageOutput{Kind: OutputPartitioned, NumPartitions: 0, Files: nil}, nil
	}
	resultPrefix := fmt.Sprintf("queries/%s/%s/", queryID, stage.ID)
	c.logger.Info("dispatchScanFilterStage",
		"stage_id", stage.ID, "files", len(stage.ScanFiles),
		"tasks", actualTasks, "filters", len(stage.FilterExprs),
		"shard_count", shardCount,
		"df_emits", len(stage.EmitDynamicFilters),
		"df_consumes", len(stage.ConsumeDynamicFilters))

	// Fragment dispatch: when the planner's fuseScanShuffle pass absorbed a
	// downstream exchange-repartition into this scan, emit a 2-op (or 3-op
	// with filter) Operators[] pipeline so the worker streams the filtered
	// scan output directly into a partitioned shuffle sink. Saves one S3 PUT
	// + one S3 GET + one NATS round-trip vs the legacy scan→shuffle two-task
	// path. The pre-2026-05-03 narrow PR (#85) did this via stage.ShuffleKeys
	// on a single-op task, but that hit the worker's CollectSink memory blow-
	// up on Q05 SF10; the fragment path uses runStageScanPartitionedStreaming.
	fuseShuffle := stage.Exchange != nil && len(stage.Exchange.Keys) > 0 && stage.Exchange.Count > 0
	// Probe-side consume specs are identical for every task — materialize
	// once and log the attach outcome. attached < requested means the
	// build side produced no eligible partials for some FilterID and that
	// consume silently degraded to scan-without-filter (correct but worth
	// seeing in an A/B run).
	var consumeSpecs []distributed.DynamicFilterSpec
	if len(stage.ConsumeDynamicFilters) > 0 {
		consumeSpecs = dynamicFilterSpecsFromBuildStats(stage.ConsumeDynamicFilters, inputs)
		c.logger.Info("dynamic_filter: consume specs attached to probe scan",
			"stage_id", stage.ID,
			"requested", len(stage.ConsumeDynamicFilters),
			"attached", len(consumeSpecs))
	}
	tasks := make([]distributed.Task, 0, actualTasks)
	for shardIdx, files := range fileSets {
		t := distributed.Task{
			ID:           uuid.New().String()[:8],
			QueryID:      queryID,
			StageID:      stage.ID,
			Type:         distributed.TaskTypeStage,
			TableName:    stage.TableName,
			DataBucket:   c.config.ResultBucket,
			ResultBucket: c.config.ResultBucket,
			ResultPrefix: resultPrefix,
			CreatedAt:    time.Now(),
		}
		// Both branches emit fragment Operators[]. fuseShuffle terminates
		// in OpExchangeSender (writing partitioned shuffle output);
		// non-fuseShuffle terminates in OpUnpartitionedSink (one .wshf
		// per task) — same shape the legacy executeStageScan emitted.
		scanOp := distributed.OpSpec{
			Type:        distributed.OpScan,
			InputAlias:  scanAliasForStage(stage),
			InputFiles:  files,
			InputBucket: c.config.ResultBucket,
			Columns:     stage.Columns,
		}
		if shardCount > 1 {
			scanOp.ScanShardIndex = shardIdx
			scanOp.ScanShardCount = shardCount
		}
		// Build-side: each task emits a partial bloom+range artifact.
		if len(stage.EmitDynamicFilters) > 0 {
			emits := make([]distributed.DynamicFilterEmit, len(stage.EmitDynamicFilters))
			for i, e := range stage.EmitDynamicFilters {
				emits[i] = distributed.DynamicFilterEmit{
					FilterID:  e.FilterID,
					KeyColumn: e.KeyColumn,
					KeyType:   e.KeyType,
					BloomBits: e.BloomBits,
				}
			}
			scanOp.DynamicFilterEmits = emits
		}
		// Probe-side: coordinator-merged stats from the upstream build-scan.
		// dynamicFilterSpecsFromBuildStats walks ConsumeDynamicFilters and
		// pulls the matching BuildStats out of inputs[SourceStageID].
		if len(consumeSpecs) > 0 {
			scanOp.DynamicFilters = consumeSpecs
		}
		t.Operators = append(t.Operators, scanOp)
		if len(stage.FilterExprs) > 0 {
			t.Operators = append(t.Operators, distributed.OpSpec{
				Type:       distributed.OpFilter,
				Predicates: append([]string(nil), stage.FilterExprs...),
			})
		}
		// The ABAC security barrier projects FIRST (masks applied, denied
		// columns dropped), then the SELECT-list projection computes over
		// the policy-projected schema.
		if op, ok := projectOpFromSpecs(stage.SecurityProjectExprs); ok {
			t.Operators = append(t.Operators, op)
		}
		if op, ok := projectOpFromSpecs(stage.ProjectExprs); ok {
			t.Operators = append(t.Operators, op)
		}
		if fuseShuffle {
			t.Operators = append(t.Operators, distributed.OpSpec{
				Type:          distributed.OpExchangeSender,
				ShuffleKeys:   append([]string(nil), stage.Exchange.Keys...),
				NumPartitions: stage.Exchange.Count,
			})
		} else {
			t.Operators = append(t.Operators, distributed.OpSpec{
				Type: distributed.OpUnpartitionedSink,
			})
		}
		if clusterID := c.catalog.ClusterID(); clusterID != "" {
			t.ClusterID = clusterID
		}
		c.mu.Lock()
		qm := c.queryMetas[queryID]
		c.mu.Unlock()
		c.enrichTaskWithQueryContext(qm, &t)
		tasks = append(tasks, t)
	}

	stageQueryID := fmt.Sprintf("st-%s-%s", stage.ID, queryID)
	trackerStages := map[string]*StageInfo{
		stage.ID: {StageID: stage.ID, Type: distributed.TaskTypeStage, TotalTasks: len(tasks)},
	}
	c.tracker.Register(stageQueryID, "", trackerStages, []string{stage.ID})
	c.tracker.Start(stageQueryID)
	defer c.tracker.Delete(stageQueryID)

	// Per-task admission estimate: the catalog-true scan bytes split
	// across tasks (files are split evenly; row-group shards each read
	// ~1/N of the file).
	perTaskEst := int64(0)
	if stage.EstimatedBytes > 0 {
		perTaskEst = stage.EstimatedBytes / int64(len(tasks))
	}
	for i := range tasks {
		tasks[i].QueryID = stageQueryID
		tasks[i].Attempt = 1
		tasks[i].EstimatedBytes = perTaskEst
	}
	subject := distributed.QueryResultSubject(stageQueryID)
	done := make(chan struct{}, 1)
	progress := make(chan struct{}, len(tasks))
	// Register with the per-query progress bridge so worker-emitted
	// TaskProgress messages also count as forward progress for this
	// stage's idle detection.
	defer stageProgressBridgeFromContext(ctx).Register(stage.ID, progress)()
	// Scan-filter tasks sink to S3 (unpartitioned or fused-shuffle
	// partition files), never gather-fused — retry is always safe.
	// Dynamic-filter partials are collected from the successful attempt's
	// notification, same as result files.
	retrier := newTaskRetrier(tasks, true, func(t distributed.Task) {
		if ctx.Err() != nil {
			return
		}
		if pubErr := c.scheduler.PublishTasks(ctx, []distributed.Task{t}); pubErr != nil {
			c.logger.Error("scan-filter task retry publish failed",
				"stage_id", stage.ID, "task_id", t.ID, "error", pubErr)
		}
	}, c.logger, stage.ID, c.classifyFatalResult)
	defer c.watchStuckTasks(ctx, retrier)()
	sub, err := c.subscribeTaskResults(subject, func(msg *nats.Msg) {
		var r distributed.ResultNotification
		if uerr := distributed.Unmarshal(msg.Data, &r); uerr != nil {
			return
		}
		c.noteTaskResult(r)
		allDone := retrier.Observe(r)
		select {
		case progress <- struct{}{}:
		default:
		}
		if allDone {
			select {
			case done <- struct{}{}:
			default:
			}
		}
	})
	if err != nil {
		return StageOutput{}, fmt.Errorf("scan-filter stage %s subscribe: %w", stage.ID, err)
	}
	defer sub.Unsubscribe()

	if err := c.scheduler.PublishTasks(ctx, tasks); err != nil {
		return StageOutput{}, fmt.Errorf("scan-filter stage %s publish: %w", stage.ID, err)
	}
	if err := awaitStageProgress(ctx, done, progress, "scan-filter "+stage.ID); err != nil {
		return StageOutput{}, err
	}

	if taskID, errMsg, failed := retrier.FirstError(); failed {
		return StageOutput{}, fmt.Errorf("scan-filter stage %s: task %s failed after %d attempts: %s", stage.ID, taskID, maxTaskAttempts, errMsg)
	}
	resultFiles := retrier.Files()

	// Union per-task dynamic-filter partials into stage-level BuildStats.
	// Skipped if the stage didn't emit (no partials expected) — saves the
	// S3 round-trip for the common non-emit case.
	var buildStats map[string]*BuildStats
	if len(stage.EmitDynamicFilters) > 0 {
		allRefs := retrier.DynamicFilterPartials()
		merged, mergeErr := mergeBuildStatsFromPartials(ctx, c.catalog.Store(), allRefs)
		if mergeErr != nil {
			c.logger.Warn("dynamic_filter: partial merge failed; downstream consumes will see no filter",
				"stage_id", stage.ID, "error", mergeErr)
		}
		// Inline-vs-stage decision per FilterID.
		for filterID, stats := range merged {
			if stats == nil {
				continue
			}
			if estimateBuildStatsInlineSize(stats) > dynamicFilterInlineThresholdBytes {
				if err := stageBuildStats(ctx, c.catalog.Store(), c.config.ResultBucket, queryID, stage.ID, filterID, stats); err != nil {
					c.logger.Warn("dynamic_filter: stage upload failed; falling back to inline",
						"stage_id", stage.ID, "filter_id", filterID, "error", err)
				}
			}
		}
		staged := 0
		for _, stats := range merged {
			if stats != nil && stats.StagedKey != "" {
				staged++
			}
		}
		c.logger.Info("dynamic_filter: build partials merged",
			"stage_id", stage.ID,
			"partial_refs", len(allRefs),
			"merged_filters", len(merged),
			"staged_to_s3", staged)
		buildStats = merged
	}

	if fuseShuffle {
		// Fused scan+shuffle: each task produced N partition files at
		// "<prefix>partition=NNNN/<task>.wshf". Bucket all files across
		// all tasks by partition number so the downstream consumer reads
		// each partition's full set. Same shape as runShuffleSide.
		numParts := stage.Exchange.Count
		shardFiles := make([][]string, numParts)
		for _, taskFiles := range resultFiles {
			for _, f := range taskFiles {
				p, parseErr := parsePartitionFromPath(f)
				if parseErr != nil {
					return StageOutput{}, fmt.Errorf("scan-filter stage %s: parsing partition from %q: %w", stage.ID, f, parseErr)
				}
				if p < 0 || p >= numParts {
					return StageOutput{}, fmt.Errorf("scan-filter stage %s: partition %d out of range [0,%d) in %q", stage.ID, p, numParts, f)
				}
				shardFiles[p] = append(shardFiles[p], f)
			}
		}
		out := StageOutput{
			Kind:          OutputPartitioned,
			NumPartitions: numParts,
			Files:         shardFiles,
			BuildStats:    buildStats,
			Bytes:         retrier.TotalBytes(),
		}
		out.PartitionRows, out.PartitionBytes = retrier.PartitionAccounting(numParts)
		return out, nil
	}
	return StageOutput{
		Kind:          OutputPartitioned,
		NumPartitions: len(tasks),
		Files:         resultFiles,
		BuildStats:    buildStats,
		Bytes:         retrier.TotalBytes(),
	}, nil
}

// scanAliasForStage returns the alias key used when handing scan-fused
// files to a worker's Inputs map. Falls back to the stage's TableName
// or scan ID so the worker can resolve the parquet source.
func scanAliasForStage(stage physical.Stage) string {
	if stage.ScanAlias != "" {
		return stage.ScanAlias
	}
	if stage.TableName != "" {
		return stage.TableName
	}
	return stage.ID
}

// dispatchComputeStage handles non-leaf compute stages (hash_join,
// broadcast_join, aggregate, final_aggregate, sort, merge_sort) by
// emitting workerCount TaskTypeStage tasks. Each task's Inputs are
// sliced from upstream stage outputs via partitionFilesForWorker; each
// task writes unpartitioned .wshf output to a per-worker result key.
func (c *Coordinator) dispatchComputeStage(
	ctx context.Context,
	queryID string,
	stage physical.Stage,
	inputs map[string]StageOutput,
	workerCount int,
	fusion *gatherFusion,
) (StageOutput, error) {
	if workerCount <= 0 {
		workerCount = 1 // single-worker fallback for sparse test / bootstrap setups
	}
	// Singleton final_aggregate over a fanned-out upstream is a serialization
	// point — one worker re-aggregates every partial while the others idle.
	// Reshape into a parallel intermediate-merge phase + a 1-task final
	// merge when the preconditions hold (multi-worker, K>=2 upstream files,
	// no AVG). Falls back to the standard 1-task path otherwise.
	if _, _, ok := finalAggregateFanoutCandidate(stage, inputs, workerCount); ok {
		return c.dispatchFinalAggregateFanout(ctx, queryID, stage, inputs, workerCount, fusion)
	}
	// Task count derivation:
	//   - Singleton output: exactly 1 task. The stage produces one logical
	//     result; running workerCount tasks on the same unpartitioned input
	//     duplicates the output N× (caught on SF10 Q01: 18 rows instead of
	//     6 with workerCount=3).
	//   - Hash-partitioned output: one task per output partition. The
	//     planner's distribution.Count is authoritative; fall back to
	//     workerCount if unset.
	//   - Broadcast output: 1 task producing a file every consumer reads.
	//   - Anything else: correctness-first default of 1 task.
	numTasks := 1
	switch stage.Distribution.Kind {
	case physical.DistSingleton:
		numTasks = 1
	case physical.DistHashPartitioned:
		numTasks = stage.Distribution.Count
		if numTasks <= 0 {
			numTasks = workerCount
		}
	case physical.DistBroadcast:
		numTasks = 1
	default:
		// Unknown distribution — fall back to 1 rather than workerCount.
		numTasks = 1
	}
	// Probe-split for broadcast_join: when the planner picks DistSingleton
	// (because probe upstream is a leaf scan with DistSingleton output), the
	// join would otherwise run as a single task on one worker — the entire
	// probe scan + hash probe is serialized. When workerCount > 1 and the
	// probe upstream is a multi-file OutputSinglePart, fan out to
	// min(workerCount, len(probeFiles)) tasks each scanning 1/N of probe
	// files. The build is broadcast (every task reads the same OutputSinglePart
	// Files[0]), so memory pressure stays bounded — the per-task hash table
	// is identical to the single-task case. Only the probe scan + probe
	// compute is parallelized.
	probeSplit := false
	if n, ok := broadcastJoinProbeSplit(stage, inputs, workerCount, numTasks); ok {
		numTasks = n
		probeSplit = true
	}
	// Round-robin partial-aggregate fan-out (see aggregatePartialSplit).
	// Mutually exclusive with probe-split and skew-split by stage type.
	var rrAggGroups [][]string
	rrAggDep := ""
	if c.config.AggPartialSplit {
		if dep, groups, ok := aggregatePartialSplit(stage, inputs, workerCount, numTasks); ok {
			rrAggDep, rrAggGroups = dep, groups
			numTasks = len(groups)
		}
	}
	// Adaptive skew split (--skew-split, docs/design/skew-aware-shuffle.md):
	// a shuffled hash join whose deps report per-partition bytes may split
	// hot partition groups into k sub-tasks (probe files divided, build
	// files replicated). skewGroups keeps the UNSPLIT slot count so the
	// output below re-groups sub-task files by original partition range —
	// downstream partition-aligned consumers see the same layout shape
	// either way.
	skewGroups := numTasks
	var skewAssign []skewTaskAssignment
	if c.config.SkewSplit && !probeSplit {
		if a := c.planSkewSplitTasks(stage, inputs, numTasks, workerCount); a != nil {
			skewAssign = a
			numTasks = len(a)
		}
	}
	resultPrefix := fmt.Sprintf("queries/%s/%s/", queryID, stage.ID)
	dispatchAttrs := []any{
		"stage_id", stage.ID, "stage_type", stage.Type,
		"deps", stage.Dependencies, "num_tasks", numTasks,
		"distribution_kind", stage.Distribution.Kind,
		"distribution_count", stage.Distribution.Count,
		"rr_agg_split", rrAggGroups != nil,
		"probe_split", probeSplit,
		"skew_split", skewAssign != nil,
		"inputs_aliases", len(inputs),
	}
	// Plan-side engagement marker for late-materialization A/B arms: join
	// stages dispatched with view-column output enabled are grep-able from
	// benchmark.log (worker-side runtime counters live in worker logs, which
	// benchmark teardown discards). Flag-off logs stay byte-identical.
	if c.config.LateMaterialization &&
		(stage.Type == physical.StageHashJoin || stage.Type == physical.StageBroadcastJoin) {
		dispatchAttrs = append(dispatchAttrs, "late_mat", true)
	}
	c.logger.Info("dispatchComputeStage", dispatchAttrs...)

	// Observability for fused-chain probe-split: log the cluster-wide
	// broadcast-cache file count this stage will read across all shard
	// tasks. With N shards × (1 primary + M fused) caches, total S3 reads
	// scale linearly. Useful for spotting when amplification is dominating
	// wall time on bigger SF deploys.
	if len(stage.FusedJoins) > 0 || probeSplit {
		var primaryFiles, fusedFiles int
		if buildDep := stage.RightDepStage; buildDep != "" {
			primaryFiles = len(flattenStageFiles(inputs[buildDep]))
		}
		for _, fj := range stage.FusedJoins {
			fusedFiles += len(flattenStageFiles(inputs[fj.BuildDepStage]))
		}
		c.logger.Info("fused/probe-split broadcast cache load",
			"stage_id", stage.ID,
			"num_tasks", numTasks,
			"fused_count", len(stage.FusedJoins),
			"primary_cache_files", primaryFiles,
			"fused_cache_files_total", fusedFiles,
			"cluster_cache_reads_estimate", numTasks*(primaryFiles+fusedFiles))
	}

	// Build one task per output partition. Input slicing uses numTasks as
	// the divisor so each task reads its share of the upstream partitioned
	// input — for Singleton stages every task reads the full upstream; for
	// Hash-partitioned N-task stages each task reads its N-th partition.
	tasks := make([]distributed.Task, 0, numTasks)
	for w := 0; w < numTasks; w++ {
		var taskInputs map[string][]string
		var err error
		if probeSplit {
			taskInputs, err = buildTaskInputsForBroadcastJoinSplitProbe(stage, inputs, w, numTasks)
		} else if rrAggGroups != nil {
			// Partial-aggregate fan-out: task w aggregates its disjoint
			// file group; alias follows buildTaskInputsForStage's
			// single-input convention (dep ID).
			taskInputs = map[string][]string{rrAggDep: rrAggGroups[w]}
		} else if skewAssign != nil {
			taskInputs = skewAssign[w].inputs
		} else {
			taskInputs, err = buildTaskInputsForStage(stage, inputs, w, numTasks)
		}
		if err != nil {
			return StageOutput{}, fmt.Errorf("stage %s worker %d: %w", stage.ID, w, err)
		}
		// Convert stage.AggSpecs → distributed.AggSpec, decomposing AVG
		// into (SUM, COUNT) pairs so the merge step sees only mergable
		// aggregates. The worker's avg-fold (executor_stage.go) reads
		// the synthetic columns and emits the original AVG output names.
		var aggs []distributed.AggSpec
		for _, a := range stage.AggSpecs {
			aggs = append(aggs, distributed.AggSpec{
				Func:      a.Func,
				InputCol:  a.InputCol,
				OutputCol: a.OutputCol,
				InputExpr: a.InputExpr,
			})
		}
		aggs = decomposeAvg(aggs)
		// Convert stage.SortKeys → distributed.SortKeySpec.
		var sorts []distributed.SortKeySpec
		for _, s := range stage.SortKeys {
			sorts = append(sorts, distributed.SortKeySpec{Column: s.Column, Desc: s.Desc})
		}
		// Translate planner-side FusedJoinSpec (carries BuildDepStage) into
		// wire-side FusedJoinSpec (carries BuildFiles) by looking up each
		// fused build's upstream stage output. This is what lets a fused
		// broadcast-join chain run as one pipelined task instead of N
		// separate stages with S3 round-trips between them.
		var wireFused []distributed.FusedJoinSpec
		for fi, fj := range stage.FusedJoins {
			buildOut, ok := inputs[fj.BuildDepStage]
			if !ok {
				return StageOutput{}, fmt.Errorf("stage %s fused join %d: build dep %q output not found",
					stage.ID, fi, fj.BuildDepStage)
			}
			wireFused = append(wireFused, distributed.FusedJoinSpec{
				JoinType:        fj.JoinType,
				JoinLeftKeys:    append([]string(nil), fj.JoinLeftKeys...),
				JoinRightKeys:   append([]string(nil), fj.JoinRightKeys...),
				BuildFiles:      flattenStageFiles(buildOut),
				BuildTableAlias: fj.BuildTableAlias,
				BuildColOrigins: fj.BuildColOrigins,
				JoinFilter:      fj.JoinFilter,
				FilterExprs:     append([]string(nil), fj.FilterExprs...),
			})
		}
		t := distributed.Task{
			ID:                  uuid.New().String()[:8],
			QueryID:             queryID,
			StageID:             stage.ID,
			Type:                distributed.TaskTypeStage,
			StageType:           stage.Type,
			JoinType:            stage.JoinType,
			JoinLeftKeys:        stage.JoinLeftKeys,
			JoinRightKeys:       stage.JoinRightKeys,
			BuildTableAlias:     stage.BuildTableAlias,
			QualifyAllBuildCols: stage.QualifyAllBuildCols,
			BuildColOrigins:     stage.BuildColOrigins,
			JoinFilter:          stage.JoinFilter,
			FusedJoins:          wireFused,
			GroupByCols:         stage.GroupByCols,
			Aggregates:          aggs,
			SortKeys:            sorts,
			Limit:               stage.Limit,
			Inputs:              taskInputs,
			DataBucket:          c.config.ResultBucket,
			ResultBucket:        c.config.ResultBucket,
			ResultPrefix:        resultPrefix,
			CreatedAt:           time.Now(),
			// Output column projection for hash_join / broadcast_join stages.
			// The worker applies these as the probe operator's OutputFilter so
			// the join emits only the columns the downstream stage consumes,
			// instead of the full union of build+probe schemas. Without this
			// the wide post-join schema rides every chained shuffle (which
			// can't prune via prunedScanColumns because TableName="" upstream
			// of a join). Aggregate/sort stages ignore this field.
			Columns: append([]string(nil), stage.Columns...),
			// Filters attached to a compute stage (HAVING on
			// aggregate/final_aggregate, residual predicates on
			// hash_join) reference OUTPUT columns and must run after
			// the stage's main operator. FilterExprs on compute stages
			// would otherwise silently drop — that's the root cause of
			// Q15 ignoring `WHERE total_revenue = (SELECT MAX...)` and
			// Q18 ignoring `HAVING SUM(l_quantity) > 300`.
			PostFilterExprs: append([]string(nil), stage.FilterExprs...),
		}
		// Eager consumer dispatch: a provisional upstream (producer still
		// running) feeds this task's alias from producer-task manifests
		// (Task.EagerInputs) rather than the frozen — here empty — file
		// list in taskInputs. Aliases must match buildTaskInputsForStage's
		// convention (dep ID for single-input stages; build/probe aliases
		// for joins — eagerAliasForDep). Skew/probe-split slicing never
		// applies to eager stages (provisional accounting is nil and the
		// eager clearance already ruled out a projected split), so the
		// partition range matches what the frozen path would bind for
		// task w.
		for depID, in := range inputs {
			if in.eager == nil {
				continue
			}
			if t.EagerInputs == nil {
				t.EagerInputs = make(map[string]distributed.EagerInput, 2)
			}
			t.EagerInputs[eagerAliasForDep(stage, depID)] = in.eager.eagerInputForTask(w, numTasks)
		}
		// Fused join + downstream exchange-sender: emit a fragment task
		// pipeline that runs the entire chain in one worker call without
		// writing the join output to S3 only to immediately re-read+hash
		// it in a separate shuffle task. Worker dispatches via
		// executeFragment (executor_fragment.go); the legacy single-op
		// task fields above still populate so the wire format stays
		// stable for any downstream consumer that hasn't yet learned
		// about Operators[].
		//
		// Hash-join / broadcast_join migration: every join stage routes
		// through the multi-op fragment runner. Three terminal sink shapes:
		//   - downstream Repartition Exchange → OpExchangeSender
		//     (the fuseJoinShuffle case)
		//   - downstream is anything else → OpUnpartitionedSink
		//   - GroupByCols on a join stage isn't an emitted shape (joins
		//     and aggregates are separate stages in the planner today),
		//     so we don't model it.
		//
		// SortKeys after fuseSortIntoPredecessor: legacy applyPostSort ran
		// in-process; the fragment runner uses OpSort as a multi-breaker
		// chain element. probeSplit's task input slicing flows through
		// taskInputs unchanged; the fragment source op is a uniform
		// sourceForAliasWithProjection (auto-detects parquet vs WSHF).
		// probeSplit migration depends on the fragment runner's spilled-
		// partition flush phase (worker/executor_fragment.go); without it,
		// Q05 SF100 build-cache chains return 0 rows when the primary's
		// only build partition spills under cumulative chain pressure.
		canMigrateJoin := t.Operators == nil &&
			(stage.Type == physical.StageHashJoin || stage.Type == physical.StageBroadcastJoin) &&
			len(stage.GroupByCols) == 0
		if canMigrateJoin {
			var sinkOp distributed.OpSpec
			if stage.Exchange != nil && len(stage.Exchange.Keys) > 0 && stage.Exchange.Count > 0 {
				sinkOp = distributed.OpSpec{
					Type:          distributed.OpExchangeSender,
					ShuffleKeys:   append([]string(nil), stage.Exchange.Keys...),
					NumPartitions: stage.Exchange.Count,
				}
			} else {
				sinkOp = distributed.OpSpec{Type: distributed.OpUnpartitionedSink}
			}
			ops, ferr := buildJoinFragment(stage, &t, taskInputs, wireFused, sorts, sinkOp, c.config.LateMaterialization)
			if ferr != nil {
				return StageOutput{}, fmt.Errorf("stage %s: build join fragment: %w", stage.ID, ferr)
			}
			t.Operators = ops
		}
		// Sort-merge join stages always route through the fragment runner —
		// there is no legacy single-op execution path for them, so any
		// ineligibility is a planner bug and fails loudly.
		if stage.Type == physical.StageSortMergeJoin {
			if t.Operators != nil {
				return StageOutput{}, fmt.Errorf("stage %s: sort_merge_join stage already claimed by another migration", stage.ID)
			}
			ops, ferr := buildSortMergeJoinFragment(stage, &t, taskInputs, wireFused, sorts)
			if ferr != nil {
				return StageOutput{}, fmt.Errorf("stage %s: build sort-merge join fragment: %w", stage.ID, ferr)
			}
			t.Operators = ops
		}
		// Final/merge aggregate migration: dispatch via the fragment path
		// when the stage has no fused sort/limit. The fragment runs:
		//   [OpShuffleSource, OpHashAggregate(MergeMode), OpFilter? (HAVING),
		//    OpUnpartitionedSink]
		// Output shape (one .wshf per task) matches the legacy
		// executeStageAggregate path — same downstream consumers (gather,
		// further merge stages) read it identically. No file-count
		// amplification because the aggregate output is one row per group
		// per task and the sink is unpartitioned.
		//
		// Eligibility: skipped when SortKeys/Limit are set (post-aggregate
		// sort needs an OpSort breaker not yet in the fragment runner) or
		// when ReplySubject is set (gather output — handled by the legacy
		// gather-task path today; gather fusion is a follow-up).
		// Aggregate-fragment migration. SortKeys / Limit on a final_aggregate
		// stage come from fuseSortIntoPredecessor folding a downstream
		// Singleton sort into this aggregate; the multi-breaker fragment
		// runner handles the resulting `[ShuffleSource, HashAggregate,
		// Filter?(HAVING), Sort?, Sink]` chain in one task.
		//
		// "aggregate" (partial, MergeMode=false) is a standalone aggregate
		// stage consuming non-scan upstream input (e.g. join output);
		// "merge_aggregate" / "final_aggregate" (MergeMode=true) re-aggregate
		// partial outputs. buildAggregateFragment derives the mode from
		// stage.Type.
		canMigrateAggregate := t.Operators == nil &&
			(stage.Type == "aggregate" || stage.Type == "final_aggregate" || stage.Type == "merge_aggregate") &&
			t.ReplySubject == ""
		if canMigrateAggregate {
			var fusedSubject string
			if fusion != nil {
				fusedSubject = fusion.replySubject
			}
			ops, ferr := buildAggregateFragment(stage, &t, taskInputs, aggs, sorts, fusedSubject)
			if ferr != nil {
				return StageOutput{}, fmt.Errorf("stage %s: build aggregate fragment: %w", stage.ID, ferr)
			}
			t.Operators = ops
		}
		// Sort migration: dispatch standalone Sort / merge_sort stages via
		// the fragment path. The fragment runs:
		//   [OpShuffleSource, OpSort, OpUnpartitionedSink]
		// Output shape (one .wshf per task) matches the legacy
		// executeStageSort path. Eligibility excludes any stage already
		// claimed by canFuseJoin (joins have no SortKeys) or
		// canMigrateAggregate (mutually exclusive on stage.Type), so this
		// reads as "if no other migration claimed it, and it's a sort,
		// migrate it."
		canMigrateSort := t.Operators == nil &&
			(stage.Type == "sort" || stage.Type == "merge_sort") &&
			len(stage.SortKeys) > 0 &&
			t.ReplySubject == ""
		if canMigrateSort {
			var fusedSubject string
			if fusion != nil {
				fusedSubject = fusion.replySubject
			}
			ops, ferr := buildSortFragment(stage, &t, taskInputs, sorts, fusedSubject)
			if ferr != nil {
				return StageOutput{}, fmt.Errorf("stage %s: build sort fragment: %w", stage.ID, ferr)
			}
			t.Operators = ops
		}
		if clusterID := c.catalog.ClusterID(); clusterID != "" {
			t.ClusterID = clusterID
		}
		c.mu.Lock()
		qm := c.queryMetas[queryID]
		c.mu.Unlock()
		c.enrichTaskWithQueryContext(qm, &t)
		tasks = append(tasks, t)
	}

	// Dispatch via scheduler + shuffle-side-style result collection.
	stageQueryID := fmt.Sprintf("st-%s-%s", stage.ID, queryID)
	trackerStages := map[string]*StageInfo{
		stage.ID: {
			StageID:    stage.ID,
			Type:       distributed.TaskTypeStage,
			TotalTasks: len(tasks),
		},
	}
	c.tracker.Register(stageQueryID, "", trackerStages, []string{stage.ID})
	c.tracker.Start(stageQueryID)
	defer c.tracker.Delete(stageQueryID)

	subject := distributed.QueryResultSubject(stageQueryID)
	// Per-task admission estimate from upstream output sizes: partitioned
	// inputs (and the manually-sliced probe of a probe-split) divide across
	// tasks; replicated and single-part inputs are read in full by EVERY
	// task (partitionFilesForWorker hands non-partitioned deps to each task
	// whole), so they charge their full bytes.
	// skewGroups (== numTasks when no skew split) keeps the divisor at the
	// logical slot count: sub-tasks carry their own measured estimate below,
	// and unsplit tasks shouldn't see estimates diluted by the extra tasks.
	perTaskEst := estimateComputeTaskBytes(stage, inputs, skewGroups, probeSplit)
	for i := range tasks {
		tasks[i].QueryID = stageQueryID
		tasks[i].Attempt = 1
		tasks[i].EstimatedBytes = perTaskEst
		if skewAssign != nil && skewAssign[i].estBytes > 0 {
			tasks[i].EstimatedBytes = skewAssign[i].estBytes
		}
	}
	done := make(chan struct{}, 1)
	progress := make(chan struct{}, len(tasks))
	// Register with the per-query progress bridge so worker-emitted
	// TaskProgress messages also count as forward progress for this
	// stage's idle detection.
	defer stageProgressBridgeFromContext(ctx).Register(stage.ID, progress)()
	// Task retry: failed tasks are re-dispatched up to maxTaskAttempts
	// (deterministic durable inputs + overwrite-safe outputs make this
	// sound). DISABLED for gather-fused stages — those stream rows to the
	// client mid-task, so a retried task would duplicate streamed output.
	retrier := newTaskRetrier(tasks, fusion == nil, func(t distributed.Task) {
		if ctx.Err() != nil {
			return
		}
		// Eager consumers: retries are otherwise verbatim, but a stale
		// Replay list would make the retried task wait on the republisher
		// for manifests published since the original build (fencing
		// recovery depends on the retry seeing the stable attempt set
		// promptly). Refresh from the feed at re-dispatch.
		refreshEagerReplay(&t, inputs)
		if pubErr := c.scheduler.PublishTasks(ctx, []distributed.Task{t}); pubErr != nil {
			c.logger.Error("task retry publish failed",
				"stage_id", stage.ID, "task_id", t.ID, "error", pubErr)
		}
	}, c.logger, stage.ID, c.classifyFatalResult)
	defer c.watchStuckTasks(ctx, retrier)()
	// Eager stages with more tasks than the cluster can hold at once
	// publish in governed waves: eager tasks block on producer manifests
	// while HOLDING a worker slot, so a full join fan-out (numTasks =
	// partition count) dispatched at once could occupy every slot ahead
	// of the very producer tasks it waits on. Cap in-flight eager tasks
	// at capacity − workerCount (≥ one producer lane per worker on
	// average); top up as tasks reach terminal state. Never coexists
	// with fusion (eligibility excludes the gather-fused stage).
	publishNow := tasks
	var governor *eagerPublishGovernor
	if len(tasks) > 0 && len(tasks[0].EagerInputs) > 0 {
		capacity := c.workers.ClusterCapacity()
		eagerCap := workerCount // capacity unreported: assume ≥ 2 slots/worker
		if capacity > 0 {
			eagerCap = capacity - workerCount
		}
		publishNow, governor = newEagerPublishGovernor(tasks, eagerCap, func(t distributed.Task) {
			if ctx.Err() != nil {
				return
			}
			// Late-wave tasks get a current Replay snapshot — their
			// original one was taken at task build, before earlier waves'
			// producers reported.
			refreshEagerReplay(&t, inputs)
			if pubErr := c.scheduler.PublishTasks(ctx, []distributed.Task{t}); pubErr != nil {
				c.logger.Error("eager wave publish failed",
					"stage_id", stage.ID, "task_id", t.ID, "error", pubErr)
			}
		})
		if governor != nil {
			c.logger.Info("eager dispatch: governed waves",
				"stage_id", stage.ID, "tasks", len(tasks), "in_flight_cap", eagerCap)
		}
	}
	sub, err := c.subscribeTaskResults(subject, func(msg *nats.Msg) {
		var r distributed.ResultNotification
		if err := distributed.Unmarshal(msg.Data, &r); err != nil {
			return
		}
		c.noteTaskResult(r)
		allDone := retrier.Observe(r)
		if governor != nil && retrier.IsTerminal(r.TaskID) {
			// Off the subscription goroutine: the top-up publish flushes
			// NATS, same as the retry republish path.
			go governor.noteTerminal(r.TaskID)
		}
		select {
		case progress <- struct{}{}:
		default:
		}
		if allDone {
			select {
			case done <- struct{}{}:
			default:
			}
		}
	})
	if err != nil {
		return StageOutput{}, fmt.Errorf("stage %s: subscribe: %w", stage.ID, err)
	}
	defer sub.Unsubscribe()

	c.logger.Info("dispatching compute stage",
		"stage_id", stage.ID, "stage_type", stage.Type,
		"tasks", len(tasks), "subject", subject)
	// Arm gather-fusion terminal count before publish so workers can't race
	// the count past the receiver. Every task in this stage emits an
	// OpGatherSink terminal (canMigrateAggregate fires for the entire batch
	// when fusion is set — canFuseJoin is mutually exclusive on stage type,
	// canFuseGather requires no SortKeys/Limit). Tasks count == terminals
	// expected.
	if fusion != nil {
		fusion.recv.SetExpectedTerminals(len(tasks))
	}
	if err := c.scheduler.PublishTasks(ctx, publishNow); err != nil {
		return StageOutput{}, fmt.Errorf("stage %s: publish: %w", stage.ID, err)
	}

	if err := awaitStageProgress(ctx, done, progress, "compute "+stage.ID); err != nil {
		return StageOutput{}, fmt.Errorf("stage %s with %d/%d results: %w", stage.ID, retrier.Terminal(), len(tasks), err)
	}
	c.logger.Info("compute stage complete", "stage_id", stage.ID, "tasks", len(tasks))

	if taskID, errMsg, failed := retrier.FirstError(); failed {
		return StageOutput{}, fmt.Errorf("stage %s: task %s failed after %d attempts: %s", stage.ID, taskID, maxTaskAttempts, errMsg)
	}
	resultFiles := retrier.Files()
	// Fused join + downstream exchange-sender: each task produced N partition
	// files at "<prefix>partition=NNNN/<task>.wshf" (worker's executeFragment
	// → fragmentExchangeSink). Bucket all files across all tasks by partition
	// number — same shape as runShuffleSide / dispatchScanFilterStage's
	// fused-shuffle bucketing.
	if stage.Exchange != nil && len(stage.Exchange.Keys) > 0 && stage.Exchange.Count > 0 &&
		(stage.Type == physical.StageHashJoin || stage.Type == physical.StageBroadcastJoin) {
		numParts := stage.Exchange.Count
		shardFiles := make([][]string, numParts)
		for _, taskFiles := range resultFiles {
			for _, f := range taskFiles {
				p, parseErr := parsePartitionFromPath(f)
				if parseErr != nil {
					return StageOutput{}, fmt.Errorf("stage %s: parsing partition from %q: %w", stage.ID, f, parseErr)
				}
				if p < 0 || p >= numParts {
					return StageOutput{}, fmt.Errorf("stage %s: partition %d out of range [0,%d) in %q", stage.ID, p, numParts, f)
				}
				shardFiles[p] = append(shardFiles[p], f)
			}
		}
		out := StageOutput{
			Kind:          OutputPartitioned,
			NumPartitions: numParts,
			Files:         shardFiles,
			Bytes:         retrier.TotalBytes(),
		}
		out.PartitionRows, out.PartitionBytes = retrier.PartitionAccounting(numParts)
		return out, nil
	}
	// Skew-split output re-grouping: sub-task outputs collapse back into
	// their group's slot so the stage's OutputPartitioned layout matches
	// the unsplit shape (skewGroups slots; a hot group's slot just holds k
	// files). All of a hot group's rows carry that group's key range, so
	// downstream partition-aligned consumption stays correct. The Exchange
	// branch above needs no equivalent — it buckets by partition path,
	// which is task-count agnostic.
	if skewAssign != nil {
		grouped := make([][]string, skewGroups)
		for i, a := range skewAssign {
			grouped[a.group] = append(grouped[a.group], resultFiles[i]...)
		}
		return StageOutput{
			Kind:          OutputPartitioned,
			NumPartitions: skewGroups,
			Files:         grouped,
			Bytes:         retrier.TotalBytes(),
		}, nil
	}
	// resultFiles is in dispatch order (taskRetrier keys results by task ID),
	// which is deterministic — an improvement over the old arrival-order slice.
	files := resultFiles
	// Kind reflects what the planner said this stage produces. Single-
	// task Singleton stages label their output OutputSinglePart so
	// downstream consumers (partitionFilesForWorker) return the full file
	// list for every worker. Hash-partitioned stages produce Partitioned
	// output; broadcast produces Replicated; anything else falls back to
	// SinglePart (one worker consumed all input).
	kind := OutputSinglePart
	switch stage.Distribution.Kind {
	case physical.DistHashPartitioned:
		kind = OutputPartitioned
	case physical.DistBroadcast:
		kind = OutputReplicated
	}
	if probeSplit || rrAggGroups != nil {
		// Probe-split and partial-aggregate fan-out keep the planner's
		// labelling (DistSingleton / DistRoundRobin) but produce N physical
		// files (one per task). Collapse all per-task files into Files[0]
		// so OutputSinglePart consumers (which read only Files[0]) see the
		// full result. Downstream parallelism is preserved by
		// finalAggregateFanoutCandidate / flattenStageFiles callers that
		// iterate every Files[i].
		flat := make([]string, 0)
		for _, f := range files {
			flat = append(flat, f...)
		}
		return StageOutput{
			Kind:          OutputSinglePart,
			NumPartitions: 1,
			Files:         [][]string{flat},
			Bytes:         retrier.TotalBytes(),
		}, nil
	}
	return StageOutput{
		Kind:          kind,
		NumPartitions: len(tasks),
		Files:         files,
		Bytes:         retrier.TotalBytes(),
	}, nil
}

// buildJoinFragment translates a hash_join / broadcast_join stage's task into
// a fragment pipeline (Operators[]). Pipeline shape:
//
//	[ShuffleSource(probe), BroadcastProbe(fused_0..n), HashJoinProbe|BroadcastProbe(primary),
//	 OpFilter?(residual), OpSort?(folded sort), <terminalSink>]
//
// Each FusedJoin in wireFused becomes its own BroadcastProbe op (the existing
// chain semantics: every fused entry is a broadcast cache the probe stream
// passes through before reaching the primary).
//
// terminalSink is the caller-supplied sink — OpExchangeSender for the
// fuseJoinShuffle path, OpUnpartitionedSink for the standalone-terminal-join
// path. sorts is non-empty when fuseSortIntoPredecessor folded a downstream
// Singleton sort into this join (post-join sort runs in-process via the
// multi-breaker runner).
func buildJoinFragment(
	stage physical.Stage,
	t *distributed.Task,
	taskInputs map[string][]string,
	wireFused []distributed.FusedJoinSpec,
	sorts []distributed.SortKeySpec,
	terminalSink distributed.OpSpec,
	lateMat bool,
) ([]distributed.OpSpec, error) {
	if t.BuildTableAlias == "" {
		return nil, fmt.Errorf("BuildTableAlias required")
	}
	buildFiles, ok := taskInputs[t.BuildTableAlias]
	if !ok {
		return nil, fmt.Errorf("build alias %q missing from inputs", t.BuildTableAlias)
	}
	var probeAlias string
	var probeFiles []string
	for k, v := range taskInputs {
		if k != t.BuildTableAlias {
			probeAlias = k
			probeFiles = v
			break
		}
	}
	if probeAlias == "" {
		return nil, fmt.Errorf("no probe-side alias (only %q)", t.BuildTableAlias)
	}

	ops := make([]distributed.OpSpec, 0, 2+len(wireFused)+3)
	ops = append(ops, distributed.OpSpec{
		Type:        distributed.OpShuffleSource,
		InputAlias:  probeAlias,
		InputFiles:  probeFiles,
		InputBucket: t.DataBucket,
	})
	for _, fj := range wireFused {
		ops = append(ops, distributed.OpSpec{
			Type:            distributed.OpBroadcastProbe,
			JoinType:        fj.JoinType,
			LeftKeys:        fj.JoinLeftKeys,
			RightKeys:       fj.JoinRightKeys,
			BuildAlias:      fj.BuildTableAlias,
			BuildColOrigins: fj.BuildColOrigins,
			BuildFiles:      fj.BuildFiles,
			BuildBucket:     t.DataBucket,
			JoinFilter:      fj.JoinFilter,
			LateMaterialize: lateMat,
		})
	}
	primaryType := distributed.OpHashJoinProbe
	if stage.Type == physical.StageBroadcastJoin {
		primaryType = distributed.OpBroadcastProbe
	}
	ops = append(ops, distributed.OpSpec{
		Type:                primaryType,
		JoinType:            t.JoinType,
		LeftKeys:            t.JoinLeftKeys,
		RightKeys:           t.JoinRightKeys,
		BuildAlias:          t.BuildTableAlias,
		BuildFiles:          buildFiles,
		BuildBucket:         t.DataBucket,
		JoinFilter:          t.JoinFilter,
		BuildRowHint:        t.BuildRowHint,
		SemiAntiKeyOnly:     t.SemiAntiKeyOnly,
		QualifyAllBuildCols: t.QualifyAllBuildCols,
		BuildColOrigins:     t.BuildColOrigins,
		OutputColumns:       append([]string(nil), t.Columns...),
		LateMaterialize:     lateMat,
	})
	// Residual post-join filters (semi/anti residual or compute-stage
	// HAVING-equivalent) compile to an OpFilter applied AFTER the probe and
	// BEFORE any sort/sink. Mirrors the legacy applyPostFilter step.
	if len(t.PostFilterExprs) > 0 {
		ops = append(ops, distributed.OpSpec{
			Type:       distributed.OpFilter,
			Predicates: append([]string(nil), t.PostFilterExprs...),
		})
	}
	if len(sorts) > 0 {
		ops = append(ops, distributed.OpSpec{
			Type:         distributed.OpSort,
			SortKeySpecs: append([]distributed.SortKeySpec(nil), sorts...),
			SortLimit:    stage.Limit,
		})
	}
	ops = append(ops, terminalSink)
	return ops, nil
}

// buildSortMergeJoinFragment translates a sort_merge_join stage's task into
// a fragment pipeline:
//
//	[ShuffleSource(probe), OpSortMergeJoin(breaker; build from BuildFiles),
//	 OpFilter?(residual), OpSort?(folded sort), OpUnpartitionedSink]
//
// The probe side streams from this task's partition of the left exchange;
// the build side's partition files ride the op spec (BuildFiles) and are
// drained by the worker's breaker construction — the same co-partitioned
// input shape as buildJoinFragment, with the operator swapped. Fused
// broadcast chains never attach to sort_merge_join stages (every fusion
// pass gates on hash_join/broadcast_join), so wireFused must be empty.
func buildSortMergeJoinFragment(
	stage physical.Stage,
	t *distributed.Task,
	taskInputs map[string][]string,
	wireFused []distributed.FusedJoinSpec,
	sorts []distributed.SortKeySpec,
) ([]distributed.OpSpec, error) {
	if len(wireFused) > 0 {
		return nil, fmt.Errorf("sort_merge_join stage carries %d fused joins; fusion passes must not target it", len(wireFused))
	}
	if t.BuildTableAlias == "" {
		return nil, fmt.Errorf("BuildTableAlias required")
	}
	buildFiles, ok := taskInputs[t.BuildTableAlias]
	if !ok {
		return nil, fmt.Errorf("build alias %q missing from inputs", t.BuildTableAlias)
	}
	var probeAlias string
	var probeFiles []string
	for k, v := range taskInputs {
		if k != t.BuildTableAlias {
			probeAlias = k
			probeFiles = v
			break
		}
	}
	if probeAlias == "" {
		return nil, fmt.Errorf("no probe-side alias (only %q)", t.BuildTableAlias)
	}

	ops := make([]distributed.OpSpec, 0, 5)
	ops = append(ops, distributed.OpSpec{
		Type:        distributed.OpShuffleSource,
		InputAlias:  probeAlias,
		InputFiles:  probeFiles,
		InputBucket: t.DataBucket,
	})
	ops = append(ops, distributed.OpSpec{
		Type:                distributed.OpSortMergeJoin,
		JoinType:            t.JoinType,
		LeftKeys:            t.JoinLeftKeys,
		RightKeys:           t.JoinRightKeys,
		BuildAlias:          t.BuildTableAlias,
		BuildFiles:          buildFiles,
		BuildBucket:         t.DataBucket,
		QualifyAllBuildCols: t.QualifyAllBuildCols,
		BuildColOrigins:     t.BuildColOrigins,
		OutputColumns:       append([]string(nil), t.Columns...),
	})
	if len(t.PostFilterExprs) > 0 {
		ops = append(ops, distributed.OpSpec{
			Type:       distributed.OpFilter,
			Predicates: append([]string(nil), t.PostFilterExprs...),
		})
	}
	if len(sorts) > 0 {
		ops = append(ops, distributed.OpSpec{
			Type:         distributed.OpSort,
			SortKeySpecs: append([]distributed.SortKeySpec(nil), sorts...),
			SortLimit:    stage.Limit,
		})
	}
	ops = append(ops, distributed.OpSpec{Type: distributed.OpUnpartitionedSink})
	return ops, nil
}

// buildAggregateFragment translates a final_aggregate / merge_aggregate
// stage's task into a fragment Operators[] pipeline:
//
//	[OpShuffleSource, OpHashAggregate(MergeMode=true), OpFilter?(HAVING),
//	 OpSort?, OpUnpartitionedSink | OpGatherSink]
//
// FoldAvg is set only for "final_aggregate" — intermediate "merge_aggregate"
// tasks must keep __avg_sum#X / __avg_count#X synthetics intact for the
// downstream final to fold (see executor_stage.go for the same gate on the
// legacy path). Mirror behavior is byte-equivalent to the legacy
// executeStageAggregate path including the post-aggregate sort folded in
// by fuseSortIntoPredecessor.
//
// OpSort is appended only when the stage carries SortKeys (the planner's
// fuseSortIntoPredecessor pass set them by absorbing a downstream Singleton
// Sort). Stage.Limit propagates through as SortLimit so the Sort operator's
// post-Finalize Truncate fires for top-N. The multi-breaker runner chains
// HashAggregate's drain into Sort's consume in-process — no S3 hop between
// the two breakers.
//
// When gatherReplySubject is non-empty, the terminal sink is OpGatherSink
// streaming directly to the coordinator's NATS reply subscription instead
// of an unpartitioned .wshf upload — fuses the downstream gather stage
// into this fragment, eliminating one S3 PUT/GET hop and one coord round-
// trip. Each task publishes its own terminal marker; coord pre-subscribes
// with expectedTerminals = numTasks.
func buildAggregateFragment(stage physical.Stage, t *distributed.Task, taskInputs map[string][]string, aggs []distributed.AggSpec, sorts []distributed.SortKeySpec, gatherReplySubject string) ([]distributed.OpSpec, error) {
	if len(taskInputs) != 1 {
		return nil, fmt.Errorf("aggregate fragment: expected 1 input alias, got %d", len(taskInputs))
	}
	var alias string
	var files []string
	for k, v := range taskInputs {
		alias = k
		files = v
		break
	}
	ops := make([]distributed.OpSpec, 0, 5)
	ops = append(ops, distributed.OpSpec{
		Type:        distributed.OpShuffleSource,
		InputAlias:  alias,
		InputFiles:  files,
		InputBucket: t.DataBucket,
	})
	// MergeMode is true for stages that re-aggregate already-partial
	// outputs (final_aggregate, merge_aggregate). It's false for the
	// standalone "aggregate" stage, which consumes raw rows from upstream
	// non-scan input (e.g. a join output) and produces a partial. AVG
	// fold runs only on the final stage; partial / merge_aggregate
	// preserve __avg_sum#X / __avg_count#X synthetics for the downstream
	// final to fold. BuildProject is needed only on partial mode where
	// AggSpec.InputExpr might reference a derived expression that the
	// upstream hasn't pre-computed.
	mergeMode := stage.Type == "final_aggregate" || stage.Type == "merge_aggregate"
	ops = append(ops, distributed.OpSpec{
		Type:        distributed.OpHashAggregate,
		GroupByCols: append([]string(nil), stage.GroupByCols...),
		Aggregates:  append([]distributed.AggSpec(nil), aggs...),
		GroupByAll:  stage.GroupByAll,
		MergeMode:   mergeMode,
		// GroupByAll (DISTINCT) has no derived-input expressions to project and
		// no AVG synthetics to fold — keep both off regardless of stage role.
		FoldAvg:      stage.Type == "final_aggregate" && !stage.GroupByAll,
		BuildProject: !mergeMode && !stage.GroupByAll,
	})
	if len(t.PostFilterExprs) > 0 {
		ops = append(ops, distributed.OpSpec{
			Type:       distributed.OpFilter,
			Predicates: append([]string(nil), t.PostFilterExprs...),
		})
	}
	if len(sorts) > 0 {
		ops = append(ops, distributed.OpSpec{
			Type:         distributed.OpSort,
			SortKeySpecs: append([]distributed.SortKeySpec(nil), sorts...),
			SortLimit:    stage.Limit,
		})
	}
	if gatherReplySubject != "" {
		ops = append(ops, distributed.OpSpec{
			Type:         distributed.OpGatherSink,
			ReplySubject: gatherReplySubject,
		})
	} else {
		ops = append(ops, distributed.OpSpec{
			Type: distributed.OpUnpartitionedSink,
		})
	}
	return ops, nil
}

// projectOpFromSpecs converts a stage's projection spec list into an
// OpProject OpSpec. ok=false when the list is empty.
func projectOpFromSpecs(specs []physical.ProjectExprSpec) (distributed.OpSpec, bool) {
	if len(specs) == 0 {
		return distributed.OpSpec{}, false
	}
	projections := make([]distributed.ProjectSpec, len(specs))
	for i, p := range specs {
		projections[i] = distributed.ProjectSpec{Expr: p.Expr, Name: p.Name, Type: int(p.Type)}
	}
	return distributed.OpSpec{Type: distributed.OpProject, Projections: projections}, true
}

// buildScanAggregateFragment translates a fused scan + partial-aggregate
// stage's task into a fragment Operators[] pipeline:
//
//	[OpScan, OpFilter?(scan-pushed WHERE), OpHashAggregate(partial, BuildProject), terminalSink]
//
// terminalSink is the caller-supplied sink — OpExchangeSender for the
// fuseScanAggregateShuffle case (each task hash-partitions its K aggregate
// rows by group key directly, skipping the standalone exchange-repartition
// stage), OpUnpartitionedSink otherwise (one .wshf per task; downstream
// Singleton final_aggregate reads them all). Output shape under the
// unpartitioned terminal is byte-equivalent to the legacy
// executeStageAggregate path.
//
// BuildProject=true asks the worker's buildAggInputProjection to construct a
// derived-input Project for AggSpecs whose InputCol references an expression
// (e.g. SUM(l_extendedprice * (1 - l_discount))). The worker prepends the
// project to the unary chain ahead of HashAggregate's consume phase.
//
// ScanShardIndex/Count propagate single-file row-group sharding for fused
// scan-aggregate over a single compacted parquet file (e.g. SF10 lineitem
// at 5GB).
func buildScanAggregateFragment(stage physical.Stage, t *distributed.Task, files []string, aggs []distributed.AggSpec, shardIdx, shardCount int, terminalSink distributed.OpSpec) ([]distributed.OpSpec, error) {
	if len(files) == 0 {
		return nil, fmt.Errorf("scan-aggregate fragment: empty file list")
	}
	if len(stage.FusedAggGroupBy) == 0 && len(aggs) == 0 {
		return nil, fmt.Errorf("scan-aggregate fragment: at least one of FusedAggGroupBy or AggSpecs required")
	}
	if terminalSink.Type == "" {
		return nil, fmt.Errorf("scan-aggregate fragment: terminalSink.Type required")
	}
	scanOp := distributed.OpSpec{
		Type:        distributed.OpScan,
		InputAlias:  scanAliasForStage(stage),
		InputFiles:  append([]string(nil), files...),
		InputBucket: t.DataBucket,
		Columns:     append([]string(nil), stage.Columns...),
	}
	if shardCount > 1 {
		scanOp.ScanShardIndex = shardIdx
		scanOp.ScanShardCount = shardCount
	}
	ops := make([]distributed.OpSpec, 0, 5)
	ops = append(ops, scanOp)
	if len(stage.FilterExprs) > 0 {
		ops = append(ops, distributed.OpSpec{
			Type:       distributed.OpFilter,
			Predicates: append([]string(nil), stage.FilterExprs...),
		})
	}
	// ABAC security barrier runs BEFORE the partial aggregate so grouping
	// and aggregation see masked values and never see denied columns.
	if op, ok := projectOpFromSpecs(stage.SecurityProjectExprs); ok {
		ops = append(ops, op)
	}
	ops = append(ops, distributed.OpSpec{
		Type:         distributed.OpHashAggregate,
		GroupByCols:  append([]string(nil), stage.FusedAggGroupBy...),
		Aggregates:   append([]distributed.AggSpec(nil), aggs...),
		MergeMode:    false,
		BuildProject: true,
	})
	ops = append(ops, terminalSink)
	return ops, nil
}

// buildSortFragment translates a sort / merge_sort stage's task into a
// fragment Operators[] pipeline:
//
//	[OpShuffleSource, OpSort, OpUnpartitionedSink | OpGatherSink]
//
// Same one-output-file-per-task shape the legacy executeStageSort emitted
// when the terminal sink is OpUnpartitionedSink; downstream consumers
// (gather, further merges) read it identically. Limit is forwarded as
// SortLimit so the sort operator's Truncate fires after Finalize for
// top-N optimization.
//
// When gatherReplySubject is non-empty, the terminal sink is OpGatherSink
// streaming the (single, ordered) sorted output directly to the
// coordinator's NATS reply subscription instead of an unpartitioned .wshf
// upload — fuses the downstream gather stage into this fragment,
// eliminating one S3 PUT/GET hop and one coord round-trip. Only safe
// when the upstream stage is DistSingleton (one task → one ordered
// stream); canFuseGather enforces that.
func buildSortFragment(stage physical.Stage, t *distributed.Task, taskInputs map[string][]string, sorts []distributed.SortKeySpec, gatherReplySubject string) ([]distributed.OpSpec, error) {
	if len(taskInputs) != 1 {
		return nil, fmt.Errorf("sort fragment: expected 1 input alias, got %d", len(taskInputs))
	}
	if len(sorts) == 0 {
		return nil, fmt.Errorf("sort fragment: SortKeys required")
	}
	var alias string
	var files []string
	for k, v := range taskInputs {
		alias = k
		files = v
		break
	}
	terminal := distributed.OpSpec{Type: distributed.OpUnpartitionedSink}
	if gatherReplySubject != "" {
		terminal = distributed.OpSpec{
			Type:         distributed.OpGatherSink,
			ReplySubject: gatherReplySubject,
		}
	}
	return []distributed.OpSpec{
		{
			Type:        distributed.OpShuffleSource,
			InputAlias:  alias,
			InputFiles:  files,
			InputBucket: t.DataBucket,
		},
		{
			Type:         distributed.OpSort,
			SortKeySpecs: append([]distributed.SortKeySpec(nil), sorts...),
			SortLimit:    stage.Limit,
		},
		terminal,
	}, nil
}

// finalAggregateFanoutCandidate reports whether a stage qualifies for
// dispatchFinalAggregateFanout: a Singleton final_aggregate over multiple
// upstream files with parallel workers available.
//
// AVG was historically excluded here for the same reason
// dispatchScanAggregateStage forced single-task on AVG. With decomposeAvg
// (avg_decompose.go) handling the (SUM, COUNT) split + worker post-merge
// fold, AVG fan-out is now correct and the guard is gone.
//
// Returns the (single) dep ID and the flattened upstream file list when it
// qualifies; nil otherwise so callers fall through to the standard
// single-task dispatch.
func finalAggregateFanoutCandidate(
	stage physical.Stage,
	inputs map[string]StageOutput,
	workerCount int,
) (depID string, files []string, ok bool) {
	if stage.Type != "final_aggregate" {
		return "", nil, false
	}
	if stage.Distribution.Kind != physical.DistSingleton {
		return "", nil, false
	}
	if workerCount <= 1 {
		return "", nil, false
	}
	if len(stage.Dependencies) != 1 {
		// Multi-dep merges (the planner's two-level merge_aggregate tree
		// reaches here too) already encode parallelism via separate
		// intermediate stages — collapseMergeTreesForNativeDAG only
		// rewrites them when the upstream count is small. Don't double-
		// fan-out by re-splitting at dispatch.
		return "", nil, false
	}
	depID = stage.Dependencies[0]
	in, present := inputs[depID]
	if !present {
		return "", nil, false
	}
	files = flattenStageFiles(in)
	// Fanout only wins when each intermediate task performs a non-trivial
	// merge (>= 2 input files). With K <= workerCount, splitFilesEvenly
	// would hand each intermediate a single file, which is just an identity
	// re-emit + an extra final merge — pure overhead. Require K > workerCount
	// so the intermediate phase does real work.
	if len(files) <= workerCount {
		return "", nil, false
	}
	return depID, files, true
}

// dispatchFinalAggregateFanout splits a Singleton final_aggregate dispatch
// into N parallel intermediate merge tasks plus a 1-task final merge.
//
// Today a Singleton final_aggregate is a single task that reads every
// upstream partial-aggregate file serially. When the upstream is a fan-out
// (e.g. dispatchScanAggregateStage emits W partial outputs) the merge
// becomes the serialization point: one worker scans all W partials while
// the others idle. This helper reshapes the dispatch into:
//
//	N=min(K, workerCount) intermediate merge tasks (each over ⌈K/N⌉ inputs)
//	  ↓
//	1 final merge task (over the N intermediate outputs)
//
// Each intermediate runs in `final_aggregate` mode (worker rewrites InputCol
// → OutputCol per executeStageAggregate) so output preserves the partial-
// aggregate column shape the final merge expects. PostFilterExprs (HAVING)
// run only on the final task — applying them to intermediates would drop
// rows before all groups had been merged across partitions.
func (c *Coordinator) dispatchFinalAggregateFanout(
	ctx context.Context,
	queryID string,
	stage physical.Stage,
	inputs map[string]StageOutput,
	workerCount int,
	fusion *gatherFusion,
) (StageOutput, error) {
	depID, files, ok := finalAggregateFanoutCandidate(stage, inputs, workerCount)
	if !ok {
		return StageOutput{}, fmt.Errorf("final_aggregate fanout precondition failed for stage %s", stage.ID)
	}
	N := workerCount
	if N > len(files) {
		N = len(files)
	}
	groups := splitFilesEvenly(files, N)
	N = len(groups) // splitFilesEvenly may return fewer when files < N

	c.logger.Info("dispatchFinalAggregateFanout",
		"stage_id", stage.ID, "upstream_files", len(files),
		"intermediate_tasks", N)

	aggs := make([]distributed.AggSpec, 0, len(stage.AggSpecs))
	for _, a := range stage.AggSpecs {
		aggs = append(aggs, distributed.AggSpec{
			Func:      a.Func,
			InputCol:  a.InputCol,
			OutputCol: a.OutputCol,
			InputExpr: a.InputExpr,
		})
	}
	// Decompose AVG specs into (SUM, COUNT) pairs. Both intermediates
	// and the final task get the decomposed list; the worker's avg-fold
	// step reconstructs AVG only on the FINAL task (mergeMode), so
	// intermediate output schemas carry the synthetic columns end-to-
	// end up to the final's fold.
	aggs = decomposeAvg(aggs)

	// Phase 1: intermediates. Each consumes a slice of upstream files,
	// re-aggregates in merge mode, and emits its own partial output.
	// StageType="merge_aggregate" enables InputCol→OutputCol rewrite and
	// COUNT→SUM rewrite (the merge math) but skips the AVG fold — that
	// only fires on the FINAL task. Without this distinction, intermediate
	// tasks would fold AVG synthetic columns (__avg_sum#X / __avg_count#X)
	// into a single AVG column X, and the final task's HashAggregate
	// would then attempt to merge AVG values directly (mathematically
	// wrong: avg-of-avgs is unweighted) AND fail to find the synthetic
	// columns it expects in its specs. Q17 SF0.1 Brand#23 MED BOX
	// matched parts produced 0 rows because of this — bisect at
	// 8239db4 (2026-05-01).
	resultPrefix := fmt.Sprintf("queries/%s/%s/", queryID, stage.ID)
	intermTasks := make([]distributed.Task, 0, N)
	// Synthetic stage for the intermediate fragments: Type="merge_aggregate"
	// keeps FoldAvg=false (AVG synthetics survive end-to-end up to the
	// final task), no SortKeys / Limit / FilterExprs — those are final-only.
	intermStage := physical.Stage{
		Type:        "merge_aggregate",
		GroupByCols: stage.GroupByCols,
	}
	// Per-task admission estimate: each intermediate reads ~1/N of the
	// upstream output (worker-reported bytes; 0 = unknown).
	intermEst := int64(0)
	if b := inputs[depID].Bytes; b > 0 && N > 0 {
		intermEst = b / int64(N)
	}
	for i, group := range groups {
		t := distributed.Task{
			ID:             uuid.New().String()[:8],
			QueryID:        queryID,
			StageID:        fmt.Sprintf("%s-merge-%d", stage.ID, i),
			Type:           distributed.TaskTypeStage,
			DataBucket:     c.config.ResultBucket,
			ResultBucket:   c.config.ResultBucket,
			ResultPrefix:   resultPrefix,
			EstimatedBytes: intermEst,
			CreatedAt:      time.Now(),
		}
		ops, ferr := buildAggregateFragment(intermStage, &t, map[string][]string{depID: group}, aggs, nil, "")
		if ferr != nil {
			return StageOutput{}, fmt.Errorf("final_aggregate fanout %s: build interm fragment %d: %w", stage.ID, i, ferr)
		}
		t.Operators = ops
		if clusterID := c.catalog.ClusterID(); clusterID != "" {
			t.ClusterID = clusterID
		}
		c.mu.Lock()
		qm := c.queryMetas[queryID]
		c.mu.Unlock()
		c.enrichTaskWithQueryContext(qm, &t)
		intermTasks = append(intermTasks, t)
	}
	// Intermediates always sink to S3 (no fused subject) — retry-safe.
	intermFiles, intermBytes, err := c.runStageTasks(ctx,
		fmt.Sprintf("st-%s-interm-%s", stage.ID, queryID),
		stage.ID+"-interm",
		intermTasks,
		true)
	if err != nil {
		return StageOutput{}, fmt.Errorf("final_aggregate fanout %s: intermediate phase: %w", stage.ID, err)
	}
	// Flatten intermediate outputs into a single file list for the final.
	finalInputs := make([]string, 0)
	for _, fs := range intermFiles {
		finalInputs = append(finalInputs, fs...)
	}

	// Convert SortKeys once for both the legacy and fragment paths below.
	// fuseSortIntoPredecessor folds a downstream Singleton sort into the
	// final_aggregate stage; carrying SortKeys onto the final task lets
	// the worker apply the sort in-process instead of relying on a
	// separate sort task.
	var sorts []distributed.SortKeySpec
	for _, s := range stage.SortKeys {
		sorts = append(sorts, distributed.SortKeySpec{Column: s.Column, Desc: s.Desc})
	}

	// Phase 2: single final merge task over the intermediate outputs.
	// Always emits a fragment (HashAggregate(merge, FoldAvg) → Filter?(HAVING)
	// → Sort? → OpGatherSink|OpUnpartitionedSink). Gather fusion (when
	// the planner detected the chain) replaces the terminal sink with
	// OpGatherSink and pre-arms the receiver with one terminal.
	finalTask := distributed.Task{
		ID:              uuid.New().String()[:8],
		QueryID:         queryID,
		StageID:         stage.ID,
		Type:            distributed.TaskTypeStage,
		DataBucket:      c.config.ResultBucket,
		ResultBucket:    c.config.ResultBucket,
		ResultPrefix:    resultPrefix,
		EstimatedBytes:  intermBytes, // final merge reads every intermediate output
		CreatedAt:       time.Now(),
		PostFilterExprs: append([]string(nil), stage.FilterExprs...),
	}
	finalInputsMap := map[string][]string{depID: finalInputs}
	var fusedSubject string
	if fusion != nil {
		fusedSubject = fusion.replySubject
	}
	finalOps, ferr := buildAggregateFragment(stage, &finalTask, finalInputsMap, aggs, sorts, fusedSubject)
	if ferr != nil {
		return StageOutput{}, fmt.Errorf("final_aggregate fanout %s: build final fragment: %w", stage.ID, ferr)
	}
	finalTask.Operators = finalOps
	if fusion != nil {
		fusion.recv.SetExpectedTerminals(1)
	}
	if clusterID := c.catalog.ClusterID(); clusterID != "" {
		finalTask.ClusterID = clusterID
	}
	c.mu.Lock()
	qm := c.queryMetas[queryID]
	c.mu.Unlock()
	c.enrichTaskWithQueryContext(qm, &finalTask)

	// The final task streams to the client when gather-fused — retry only
	// when it sinks to S3 instead.
	finalFiles, finalBytes, err := c.runStageTasks(ctx,
		fmt.Sprintf("st-%s-%s", stage.ID, queryID),
		stage.ID,
		[]distributed.Task{finalTask},
		fusion == nil)
	if err != nil {
		return StageOutput{}, fmt.Errorf("final_aggregate fanout %s: final phase: %w", stage.ID, err)
	}
	return StageOutput{
		Kind:          OutputSinglePart,
		NumPartitions: 1,
		Files:         finalFiles,
		Bytes:         finalBytes,
	}, nil
}

// runStageTasks publishes the given tasks under stageQueryID, subscribes to
// the per-stage result subject, and waits for all completions. Returns one
// []string of result files per task in dispatch order, plus the worker-
// reported total output bytes. Used by dispatchFinalAggregateFanout for
// both phases; mirrors the inline dispatch+collect in
// dispatchScanAggregateStage / dispatchComputeStage.
//
// retryEnabled must be false when any task streams output mid-task
// (gather-fused terminal sinks) — a retried task would duplicate the
// streamed rows.
func (c *Coordinator) runStageTasks(
	ctx context.Context,
	stageQueryID, stageLabel string,
	tasks []distributed.Task,
	retryEnabled bool,
) ([][]string, int64, error) {
	if len(tasks) == 0 {
		return nil, 0, nil
	}
	trackerStages := map[string]*StageInfo{
		stageLabel: {StageID: stageLabel, Type: distributed.TaskTypeStage, TotalTasks: len(tasks)},
	}
	c.tracker.Register(stageQueryID, "", trackerStages, []string{stageLabel})
	c.tracker.Start(stageQueryID)
	defer c.tracker.Delete(stageQueryID)

	for i := range tasks {
		tasks[i].QueryID = stageQueryID
		tasks[i].Attempt = 1
	}
	subject := distributed.QueryResultSubject(stageQueryID)
	done := make(chan struct{}, 1)
	progress := make(chan struct{}, len(tasks))
	// Register with the per-query progress bridge so worker-emitted
	// TaskProgress messages also count as forward progress for this
	// stage's idle detection.
	defer stageProgressBridgeFromContext(ctx).Register(stageLabel, progress)()
	retrier := newTaskRetrier(tasks, retryEnabled, func(t distributed.Task) {
		if ctx.Err() != nil {
			return
		}
		if pubErr := c.scheduler.PublishTasks(ctx, []distributed.Task{t}); pubErr != nil {
			c.logger.Error("task retry publish failed",
				"stage_id", stageLabel, "task_id", t.ID, "error", pubErr)
		}
	}, c.logger, stageLabel, c.classifyFatalResult)
	defer c.watchStuckTasks(ctx, retrier)()
	sub, err := c.subscribeTaskResults(subject, func(msg *nats.Msg) {
		var r distributed.ResultNotification
		if uerr := distributed.Unmarshal(msg.Data, &r); uerr != nil {
			return
		}
		c.noteTaskResult(r)
		allDone := retrier.Observe(r)
		select {
		case progress <- struct{}{}:
		default:
		}
		if allDone {
			select {
			case done <- struct{}{}:
			default:
			}
		}
	})
	if err != nil {
		return nil, 0, fmt.Errorf("stage %s: subscribe: %w", stageLabel, err)
	}
	defer sub.Unsubscribe()

	if err := c.scheduler.PublishTasks(ctx, tasks); err != nil {
		return nil, 0, fmt.Errorf("stage %s: publish: %w", stageLabel, err)
	}
	if err := awaitStageProgress(ctx, done, progress, stageLabel); err != nil {
		return nil, 0, err
	}

	if taskID, errMsg, failed := retrier.FirstError(); failed {
		return nil, 0, fmt.Errorf("stage %s: task %s failed after %d attempts: %s", stageLabel, taskID, maxTaskAttempts, errMsg)
	}
	return retrier.Files(), retrier.TotalBytes(), nil
}

// estimateComputeTaskBytes estimates one compute task's input footprint
// from upstream output sizes, mirroring the slicing semantics of
// buildTaskInputsForStage / buildTaskInputsForBroadcastJoinSplitProbe:
// OutputPartitioned deps split across tasks; Replicated and SinglePart
// deps are read in full by every task; a probe-split's probe dep is
// manually sliced, so it splits too. Deps with unknown size (Bytes == 0,
// e.g. legacy workers) contribute nothing — the estimate degrades toward
// 0 = unknown rather than inventing numbers.
func estimateComputeTaskBytes(stage physical.Stage, inputs map[string]StageOutput, numTasks int, probeSplit bool) int64 {
	if numTasks <= 0 {
		numTasks = 1
	}
	probeDep := ""
	if probeSplit {
		probeDep = stage.LeftDepStage
		if probeDep == "" && len(stage.Dependencies) > 0 {
			probeDep = stage.Dependencies[0]
		}
	}
	var est int64
	for depID, out := range inputs {
		if out.Bytes <= 0 {
			continue
		}
		if depID == probeDep || out.Kind == OutputPartitioned {
			est += out.Bytes / int64(numTasks)
		} else {
			est += out.Bytes
		}
	}
	return est
}

// buildTaskInputsForStage maps a stage's Dependencies (upstream stage IDs)
// into Task.Inputs keyed by a per-stage alias convention:
//   - hash_join/broadcast_join: use stage.BuildTableAlias for the build
//     side (dep index 1) and "probe" for the other side (dep index 0).
//     With FusedJoins, additional dep entries are the fused builds — those
//     are NOT placed in task.Inputs because the worker reads
//     task.FusedJoins[i].BuildFiles directly (the dispatcher populates that
//     wire field by looking up each fused build's upstream output).
//   - aggregate/sort/etc: use the single dep's ID as alias.
func buildTaskInputsForStage(stage physical.Stage, upstreams map[string]StageOutput, workerIdx, workerCount int) (map[string][]string, error) {
	inputs := make(map[string][]string)
	switch stage.Type {
	case physical.StageHashJoin, physical.StageBroadcastJoin, physical.StageSortMergeJoin:
		expectedDeps := 2 + len(stage.FusedJoins)
		if len(stage.Dependencies) != expectedDeps {
			return nil, fmt.Errorf("join stage %s expects %d deps (2 primary + %d fused), got %d",
				stage.ID, expectedDeps, len(stage.FusedJoins), len(stage.Dependencies))
		}
		probeDep := stage.LeftDepStage
		buildDep := stage.RightDepStage
		if probeDep == "" {
			probeDep = stage.Dependencies[0]
		}
		if buildDep == "" {
			buildDep = stage.Dependencies[1]
		}
		buildAlias := stage.BuildTableAlias
		if buildAlias == "" {
			buildAlias = "build"
		}
		probeAlias := "probe"
		if probeAlias == buildAlias {
			probeAlias = "probe_side"
		}
		inputs[buildAlias] = partitionFilesForWorker(upstreams[buildDep], workerIdx, workerCount)
		inputs[probeAlias] = partitionFilesForWorker(upstreams[probeDep], workerIdx, workerCount)
		// Fused-build deps are intentionally NOT added to inputs[]; their
		// files flow through task.FusedJoins[i].BuildFiles, populated by
		// dispatchComputeStage from the upstream stage outputs.
	default:
		if len(stage.Dependencies) == 0 {
			return nil, fmt.Errorf("stage %s has no dependencies and no ScanFiles", stage.ID)
		}
		// Single-input stages (aggregate/sort): use dep ID as alias.
		depID := stage.Dependencies[0]
		inputs[depID] = partitionFilesForWorker(upstreams[depID], workerIdx, workerCount)
	}
	return inputs, nil
}

// broadcastJoinProbeSplit reports whether a broadcast_join stage qualifies
// for probe-split fan-out. Returns the desired numTasks and ok=true when:
//   - the stage is a broadcast_join the planner left at numTasks=1 (DistSingleton)
//   - the cluster has spare workers (workerCount > 1)
//   - the probe upstream produced at least 2 files (whether OutputSinglePart
//     or OutputPartitioned — for a broadcast join, the probe-side parallelism
//     is just "split probe rows N ways," and that is correct regardless of
//     whether the upstream pre-partitioned by some hash key, because the
//     broadcast cache is replicated to every shard task anyway)
//
// Triggers only for DistSingleton broadcast_join; the hash-partitioned path
// is already parallelized by the planner via Exchange{Repartition}.
//
// History: this check used to require probeIn.Kind == OutputSinglePart
// strictly. That was overly conservative — at SF10 with compacted single-
// file dimension scans, scan-sharding (commit 47630b3) produced N partial
// outputs as OutputPartitioned, which the strict check rejected, leaving
// every broadcast_join in the chain single-tasked. Relaxed 2026-04-30 after
// observing the regression on the SF10 deploy of 47630b3 (Q02 22m vs 12m
// baseline) — sharding was firing but probe-split wasn't picking it up.
func broadcastJoinProbeSplit(
	stage physical.Stage,
	inputs map[string]StageOutput,
	workerCount, currentNumTasks int,
) (numTasks int, ok bool) {
	if stage.Type != physical.StageBroadcastJoin {
		return 0, false
	}
	if currentNumTasks != 1 {
		return 0, false
	}
	if workerCount <= 1 {
		return 0, false
	}
	// A fused-chain broadcast_join has 2 + N deps (probe + primary build +
	// N fused builds). Probe-split semantics are identical: split probe
	// files across shard tasks, replicate the broadcast caches (primary +
	// fused) to every shard. The shard count gate just needs to verify the
	// stage has the expected dep count.
	expectedDeps := 2 + len(stage.FusedJoins)
	if len(stage.Dependencies) != expectedDeps {
		return 0, false
	}
	probeDep := stage.LeftDepStage
	if probeDep == "" {
		probeDep = stage.Dependencies[0]
	}
	probeIn, present := inputs[probeDep]
	if !present {
		return 0, false
	}
	if probeIn.Kind != OutputSinglePart && probeIn.Kind != OutputPartitioned {
		return 0, false
	}
	probeFiles := flattenStageFiles(probeIn)
	if len(probeFiles) < 2 {
		return 0, false
	}
	n := workerCount
	if n > len(probeFiles) {
		n = len(probeFiles)
	}
	return n, true
}

// aggregatePartialSplit reports whether a partial "aggregate" stage
// qualifies for round-robin fan-out, returning the dep ID and per-task
// input file groups when it does.
//
// The planner labels grouped partial aggregates DistRoundRobin when
// workerCount > 1 (OutputDistribution, StageAggregate case) so the
// property algebra forces a hash-shuffle ahead of the grouped final. The
// dispatcher's task-count switch, however, had no DistRoundRobin arm — the
// correctness-first default ran the partial as ONE task reading the entire
// upstream (e.g. a 24-partition join output) while the rest of the cluster
// idled. The 2026-07-19 arrival-waits evidence pass measured this as the
// exclusive serial leg on Q10 (12.9s partial + downstream effects) and
// Q13/Q02/Q03 at SF100. Partial aggregation is valid over any disjoint
// cover of its input, so the fix is the same shape as the scan-fused
// fan-out (dispatchScanAggregateStage): aggregate disjoint slices in
// parallel, let the existing downstream machinery (exchange-repartition
// from EnsureDistribution, or dispatchFinalAggregateFanout for Singleton
// finals) merge the partials.
//
// Slicing: an even split of the flattened upstream file list across at
// most workerCount tasks — the same shape as probe-split. The first cut
// of this fan-out used one task PER upstream partition (24-way at SF100)
// with no size gate; the 2026-07-19 SF100 validation run showed why the
// probe-split precedent caps at workerCount: small aggregates became
// swarms of ~2ms tasks whose per-task dispatch/result overhead (~0.5-1s)
// dwarfed the work, serialized through max_concurrent worker slots, and
// queued the NEXT query's scan tasks behind the swarm (+18% suite task
// count, steady pass +28% — slower than its own cold pass). Chunky
// tasks or no split.
//
// aggSplitMinBytes is the same "fanout only wins when each task performs
// non-trivial work" reasoning as finalAggregateFanoutCandidate's
// K > workerCount gate: below it, the single-task partial is already
// cheap and the split is pure scheduling overhead. Bytes are the
// worker-reported on-disk size of the upstream output; 0 (unknown,
// legacy worker) declines the split — degrade to the pre-split shape,
// never to a regression.
//
// Guards: SortKeys/Limit/FilterExprs never appear on a partial today
// (fuseSortIntoPredecessor folds into finals only; HAVING lands on the
// final) — the guard keeps the split sound if that ever changes, since a
// sort, limit, or post-filter applied per-slice would be wrong before the
// merge has seen all partials. Eager provisional inputs are excluded the
// same way probe-split/skew slicing is: their manifest-fed partition-range
// convention does not match custom file groups.
//
// WADJET_AGG_SPLIT_MIN_BYTES overrides the floor (small-scale harness runs
// force the split path everywhere with =1 so the correctness gate actually
// exercises it; SF1 upstreams are all under the production floor).
var aggSplitMinBytes = func() int64 {
	if v := os.Getenv("WADJET_AGG_SPLIT_MIN_BYTES"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return 64 << 20
}()

func aggregatePartialSplit(
	stage physical.Stage,
	inputs map[string]StageOutput,
	workerCount, currentNumTasks int,
) (depID string, groups [][]string, ok bool) {
	if stage.Type != physical.StageAggregate {
		return "", nil, false
	}
	if stage.Distribution.Kind != physical.DistRoundRobin {
		return "", nil, false
	}
	if currentNumTasks != 1 || workerCount <= 1 {
		return "", nil, false
	}
	if len(stage.Dependencies) != 1 || len(stage.FusedJoins) != 0 {
		return "", nil, false
	}
	if len(stage.SortKeys) != 0 || stage.Limit != 0 || len(stage.FilterExprs) != 0 {
		return "", nil, false
	}
	depID = stage.Dependencies[0]
	in, present := inputs[depID]
	if !present || in.eager != nil {
		return "", nil, false
	}
	if in.Bytes < aggSplitMinBytes {
		return "", nil, false
	}
	files := flattenStageFiles(in)
	if len(files) < 2 {
		return "", nil, false
	}
	n := workerCount
	if n > len(files) {
		n = len(files)
	}
	groups = splitFilesEvenly(files, n)
	if len(groups) < 2 {
		return "", nil, false
	}
	return depID, groups, true
}

// buildTaskInputsForBroadcastJoinSplitProbe is the probe-split variant of
// buildTaskInputsForStage for broadcast_join. Each task receives:
//   - build alias → the full broadcast set (every task sees every build row)
//   - probe alias → its 1/numTasks slice of probe upstream files
//
// Caller must verify the stage has 2 deps and probe upstream is single-part
// with multiple files; this helper assumes the dispatcher already vetted
// eligibility.
func buildTaskInputsForBroadcastJoinSplitProbe(stage physical.Stage, upstreams map[string]StageOutput, workerIdx, numTasks int) (map[string][]string, error) {
	expectedDeps := 2 + len(stage.FusedJoins)
	if len(stage.Dependencies) != expectedDeps {
		return nil, fmt.Errorf("broadcast_join split-probe stage %s expects %d deps (2 primary + %d fused), got %d",
			stage.ID, expectedDeps, len(stage.FusedJoins), len(stage.Dependencies))
	}
	probeDep := stage.LeftDepStage
	buildDep := stage.RightDepStage
	if probeDep == "" {
		probeDep = stage.Dependencies[0]
	}
	if buildDep == "" {
		buildDep = stage.Dependencies[1]
	}
	buildAlias := stage.BuildTableAlias
	if buildAlias == "" {
		buildAlias = "build"
	}
	probeAlias := "probe"
	if probeAlias == buildAlias {
		probeAlias = "probe_side"
	}
	out := make(map[string][]string, 2)
	out[buildAlias] = flattenStageFiles(upstreams[buildDep])
	probeFiles := flattenStageFiles(upstreams[probeDep])
	parts := splitFilesEvenly(probeFiles, numTasks)
	if workerIdx >= 0 && workerIdx < len(parts) {
		out[probeAlias] = parts[workerIdx]
	}
	// Fused build deps don't go in inputs[]; their files travel via
	// task.FusedJoins[i].BuildFiles, populated by dispatchComputeStage.
	// Each shard task gets the full broadcast cache for every fused build,
	// just like the primary build.
	return out, nil
}

// dispatchGatherStage is terminal: dispatches a single Gather task to one
// worker, subscribes to its reply subject, and returns the assembled
// result. GatherOrdering + GatherLimit semantics are carried on the task
// so the worker can pre-sort; the receiver concatenates in arrival order.
func (c *Coordinator) dispatchGatherStage(
	ctx context.Context,
	queryID, sql string,
	stage physical.Stage,
	inputs map[string]StageOutput,
	workerCount int,
) (*gatherResult, error) {
	if len(stage.Dependencies) != 1 {
		return nil, fmt.Errorf("gather stage %s expects 1 dep, got %d", stage.ID, len(stage.Dependencies))
	}
	upstream := inputs[stage.Dependencies[0]]
	// Dedicated reply subject: `wadjet.gather.<queryID>`. Avoids QueryResult
	// wildcard overlap (wadjet.results.<queryID>.>).
	replySubject := fmt.Sprintf("wadjet.gather.%s", queryID)

	// Subscribe before publishing so we don't miss early messages.
	var ordering []distributed.SortKeySpec
	var limit int
	if stage.Exchange != nil {
		for _, o := range stage.Exchange.Ordering {
			ordering = append(ordering, distributed.SortKeySpec{Column: o.Column, Desc: o.Desc})
		}
	}

	// Synthesize a task that runs the full SQL and streams its output.
	// Inputs alias: use the upstream stage ID so the worker's Inputs-based
	// source selection reads the upstream files.
	alias := stage.Dependencies[0]
	task := distributed.Task{
		ID:           uuid.New().String()[:8],
		QueryID:      queryID,
		StageID:      stage.ID,
		Type:         distributed.TaskTypeGather,
		SQLText:      sql,
		DataBucket:   c.config.ResultBucket,
		ResultBucket: c.config.ResultBucket,
		Inputs: map[string][]string{
			alias: flattenStageFiles(upstream),
		},
		ReplySubject:   replySubject,
		GatherOrdering: ordering,
		GatherLimit:    limit,
		CreatedAt:      time.Now(),
	}
	if clusterID := c.catalog.ClusterID(); clusterID != "" {
		task.ClusterID = clusterID
	}
	c.mu.Lock()
	qm := c.queryMetas[queryID]
	c.mu.Unlock()
	c.enrichTaskWithQueryContext(qm, &task)

	// Subscribe BEFORE publishing the task so the worker's batches and
	// terminal marker are not lost to a race. NATS does not buffer raw-
	// subject messages for late subscribers, so a goroutine that starts
	// subscribing after the publish can miss every reply (observed on
	// SF10: gather hung for 6+ minutes before the stage-level timeout
	// fired).
	c.logger.Info("gather: subscribing",
		"query_id", queryID, "reply_subject", replySubject)
	recv, err := subscribeGather(c.nc, replySubject, 1, c.workers, c.gatherResultBudget())
	if err != nil {
		return nil, fmt.Errorf("subscribing gather reply: %w", err)
	}
	// Remove unclaimed spill scratch on every path that returns before
	// wait() hands the result off. No-op after a successful wait().
	defer recv.discard()
	defer recv.registerWithDataPlane(c.dpSrv, queryID)()
	c.logger.Info("gather: publishing task",
		"query_id", queryID, "task_id", task.ID, "reply_subject", replySubject,
		"input_files", len(task.Inputs[alias]))
	if err := c.scheduler.PublishTasks(ctx, []distributed.Task{task}); err != nil {
		return nil, fmt.Errorf("publishing gather task: %w", err)
	}

	_ = workerCount // future: ordered gather dispatches one task per partition
	res, waitErr := recv.wait(ctx, gatherReceiveTimeout)
	c.logger.Info("gather: wait returned",
		"query_id", queryID, "msg_count", recv.msgCount.Load(),
		"err", waitErr)
	return res, waitErr
}
