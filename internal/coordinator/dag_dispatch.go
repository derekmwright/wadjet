// This file holds stage scheduling, dependency waits, gather fusion, and result subscriptions.
// ADR-0010 governs shuffle transport; ADR-0026 §8 governs ordering across the gather boundary.
package coordinator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/nats-io/nats.go"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

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
		if (len(dep.SortKeys) > 0 || dep.HasLimit) && dep.Distribution.Kind != physical.DistSingleton {
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
		// SameOrdering, not ==: a SortKeySpec also carries where a synthetic
		// __sortkey_N comes from (#424), and two keys that sort identically
		// must compare equal whether or not one of them records that.
		if !a[i].SameOrdering(b[i]) {
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
// docs/_archive/specs/2026-04-22-distribution-native-dag-execution-design.md
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

	// Register the stages whose outputs the COORDINATOR reads directly
	// (scalar-subquery producers, substituteScalarDependencies) before any
	// dispatch: the task annotator keeps their uploads eager under a
	// deferred ShuffleDurability policy, because the coordinator's only
	// read path is S3. Dropped by cleanupQuery with the rest of the
	// query's registries.
	if c.config.ShuffleDurability == distributed.UploadLazy || c.config.ShuffleDurability == distributed.UploadOff {
		coordRead := make(map[string]struct{})
		for _, s := range stages {
			for _, pid := range s.ScalarDependencies {
				coordRead[pid] = struct{}{}
			}
		}
		if len(coordRead) > 0 {
			c.coordReadStages.Store(queryID, coordRead)
		}
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

	// The query's merge-on-read DELETE state, unioned over the plan's
	// stages and parked on the dispatch context. Scheduler.PublishTasks
	// stamps it onto every task it sends — whatever dispatcher built it,
	// whatever carrier the base-table files arrive on, retries included
	// (#491). Nothing to do for a query over tables with no deletes.
	ctx = withQueryDeleteMarkers(ctx, collectStageDeletes(stages))

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
	c.tracker.Register(queryID, sql, auth.SnapshotIdentity(ctx), trackerStages, stageOrder)
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
					// Clearance-driven activation (§13 precondition 2):
					// from here on this feed's completions publish live.
					// The backlog flush covers completions that landed
					// before clearance; the republisher heals the
					// snapshot→subscribe window.
					if f.activate() {
						c.flushEagerManifestBacklog(f)
						c.startEagerRepublisher(f)
					}
					producerTasks += len(f.producerTaskIDs)
				}
				if len(s.ChainedJoins) > 0 {
					EagerChainedEdgesPlanned.Add(1)
				}
				c.logger.Info("eager dispatch: consumer cleared early",
					"query", queryID, "stage_id", s.ID, "stage_type", s.Type,
					"eager_deps", len(eagerFeeds), "join", eagerJoin,
					"chained_joins", len(s.ChainedJoins),
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
			//
			// Deferral (q11 serial-tail fix): when every placeholder is
			// consumed only by the final-merge phase (HAVING on merged
			// groups — scalarsDeferrableToFinalMerge), the await +
			// substitution moves INTO dispatch, between the fanout's
			// intermediate phase and its final task. The partials then
			// overlap the scalar producer chain instead of serializing
			// behind it (gap-closing diagnosis §q11: fa-8's partials
			// waited ~2s on the fa-17 chain for a filter only the merge
			// applies). A deferred stage blocks holding its dispatch
			// slot; deadlock would need >= dispatchSlots deferred stages
			// all gating their own producers — a query carries at most a
			// couple of scalar-consuming stages (TPC-H peaks at one).
			var deferredScalars scalarResolver
			if len(s.ScalarDependencies) > 0 {
				stage := s
				resolve := func(rctx context.Context) (physical.Stage, error) {
					for _, pid := range stage.ScalarDependencies {
						ch, tracked := done[pid]
						if !tracked {
							continue
						}
						select {
						case <-ch:
						case <-rctx.Done():
							return stage, rctx.Err()
						}
					}
					outputsMu.Lock()
					prod := make(map[string]StageOutput, len(stage.ScalarDependencies))
					prodStages := make(map[string]physical.Stage, len(stage.ScalarDependencies))
					for _, pid := range stage.ScalarDependencies {
						prod[pid] = outputs[pid]
						if ps, ok := stageByID[pid]; ok {
							prodStages[pid] = ps
						}
					}
					outputsMu.Unlock()
					return c.substituteScalarDependencies(rctx, stage, prod, prodStages)
				}
				if scalarsDeferrableToFinalMerge(s) {
					deferredScalars = resolve
				} else {
					var subErr error
					s, subErr = resolve(gctx)
					if subErr != nil {
						return fmt.Errorf("stage %s scalar substitution: %w", s.ID, subErr)
					}
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
			//
			// EXCEPTION: dyn-filter-emitting scan stages bypass the
			// semaphore. They are dimension-class tiny by the marking
			// passes' eligibility rules (cascade ≤2M rows, legacy ≤10M),
			// yet they gate the filtering of concurrent bulk scans — under
			// attach-on-arrival the consumer no longer waits for them, so
			// a bulk stage holding a slot for its full runtime would
			// starve exactly the stage whose output makes that bulk work
			// cheap (observed at SF10-local: supplier's dispatch pinned
			// behind lineitem's slot for 4s, bloom landed after the scan
			// ended). The semaphore's stampede/memory rationale does not
			// apply to single-digit-task dimension scans.
			if s.Type == physical.StageScan && len(s.EmitDynamicFilters) > 0 {
				c.logger.Info("stage-DAG dispatch: emitter scan bypasses dispatch slot",
					"query", queryID, "stage_id", s.ID)
			} else {
				select {
				case dispatchSem <- struct{}{}:
				case <-gctx.Done():
					return gctx.Err()
				}
				defer func() { <-dispatchSem }()
			}

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
				out, err = c.dispatchPipelineStage(gctx, queryID, sql, s, inputs, workerCount, probeOfBroadcast[s.ID], stageFusion, deferredScalars)
			}
			if err != nil {
				return fmt.Errorf("stage %s (%s): %w", s.ID, s.Type, err)
			}
			outputsMu.Lock()
			outputs[s.ID] = out
			outputsMu.Unlock()
			if eagerDispatchStageCompletionHook != nil {
				eagerDispatchStageCompletionHook(c, queryID, s.ID)
			}
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
	gr, gerr := c.dispatchGatherStage(ctx, queryID, sql, gatherStage, stageByID[gatherStage.Dependencies[0]], inputs, workerCount)
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
