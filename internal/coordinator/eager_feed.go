package coordinator

import (
	"sync"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/planner/physical"
)

// Eager consumer dispatch, Phase C1 slice 3 (docs/design/
// eager-consumer-dispatch.md §3.2-3.3): the coordinator-side feed that lets
// a consumer stage clear dispatch when its producer stage has *dispatched*
// (task IDs assigned, manifests flowing) instead of waiting for the
// producer's done channel.
//
// Lifecycle: the consumer stage goroutine and the producer's shuffle
// dispatcher race to create the feed (eagerFeedHandle is get-or-create);
// the producer fills in the dispatch info and closes `dispatched` right
// before publishing its tasks; executeStageDAG drops every feed of the
// query on exit. A feed whose producer never dispatches (empty upstream,
// flag raced off, legacy path) is inert — the consumer's select falls
// through on the producer's done channel as before.

// eagerManifestRepublishEvery is the cadence at which a live feed re-sends
// its accumulated manifests on the manifest subject. Republishing makes
// three loss windows self-heal within one tick, all with the same
// idempotent mechanism (manifestStreamSource.observe drops duplicates):
//   - a manifest published between the consumer task's Replay snapshot
//     (coordinator task build) and the worker's subscribe (task Init);
//   - a fire-and-forget Core NATS publish that never arrived;
//   - a consumer-task retry, which re-sends the task spec verbatim with
//     its original (now stale) Replay list.
//
// Without it, each of those degrades to a consumer-task deadline timeout
// followed by a retry — correct but painfully slow. Var for tests.
var eagerManifestRepublishEvery = 3 * time.Second

// eagerFeed is the coordinator-side record of one eager-capable producer
// stage. Created lazily by whichever side (consumer goroutine, producer
// dispatcher) asks first; immutable dispatch info is set exactly once by
// the producer before `dispatched` closes.
type eagerFeed struct {
	dispatched chan struct{} // closed by dispatch(); dispatch info valid after

	// Set by dispatch(), immutable afterwards.
	manifestStageID string   // stage ID used in EagerManifestSubject and manifests
	rootQueryID     string   // root query ID scoping the manifest subject
	producerTaskIDs []string // full candidate set (task IDs are stable across retries)
	numPartitions   int

	mu     sync.Mutex
	replay []distributed.ProducerTaskManifest // manifests published so far
	closed bool                               // query finished; republisher must stop
}

func newEagerFeed() *eagerFeed {
	return &eagerFeed{dispatched: make(chan struct{})}
}

// dispatch records the producer's task layout and releases consumers
// blocked on the feed. Call exactly once, before publishing producer tasks.
func (f *eagerFeed) dispatch(rootQueryID, manifestStageID string, producerTaskIDs []string, numPartitions int) {
	f.rootQueryID = rootQueryID
	f.manifestStageID = manifestStageID
	f.producerTaskIDs = producerTaskIDs
	f.numPartitions = numPartitions
	close(f.dispatched)
}

// appendReplay folds one published manifest into the replay list handed to
// consumers built after this point (and re-sent by the republisher).
func (f *eagerFeed) appendReplay(m distributed.ProducerTaskManifest) {
	f.mu.Lock()
	f.replay = append(f.replay, m)
	f.mu.Unlock()
}

// replaySnapshot returns a copy of the manifests published so far.
func (f *eagerFeed) replaySnapshot() []distributed.ProducerTaskManifest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]distributed.ProducerTaskManifest(nil), f.replay...)
}

// markClosed stops the republisher. Idempotent.
func (f *eagerFeed) markClosed() {
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
}

func (f *eagerFeed) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

// provisionalOutput synthesizes the StageOutput a consumer uses when it
// clears dispatch before the producer finishes. Everything unknown degrades
// to unknown, never to wrong: Bytes=0 (admission falls back to the pressure
// gate, estimateComputeTaskBytes skips it), PartitionRows/Bytes nil (skew
// detection off; C1 consumers are non-join anyway), no BuildStats or
// DynamicFilters (eligibility excludes stat-dep consumers). Files are the
// empty per-partition layout — partitionFilesForWorker yields empty task
// file lists and the eager alias feeds from manifests instead.
func (f *eagerFeed) provisionalOutput() StageOutput {
	return StageOutput{
		Kind:          OutputPartitioned,
		NumPartitions: f.numPartitions,
		Files:         make([][]string, f.numPartitions),
		eager:         f,
	}
}

// eagerInputForTask builds the EagerInput spec for consumer task w of
// numTasks, binding the same contiguous partition range the frozen path
// binds (partitionRangeForWorker) so eager and barrier dispatch read
// identical row sets. The Replay snapshot is taken at call time; anything
// published later reaches the worker via its subscription or the
// republisher.
func (f *eagerFeed) eagerInputForTask(w, numTasks int) distributed.EagerInput {
	start, end := partitionRangeForWorker(f.numPartitions, w, numTasks)
	return distributed.EagerInput{
		RootQueryID:     f.rootQueryID,
		StageID:         f.manifestStageID,
		ProducerTaskIDs: append([]string(nil), f.producerTaskIDs...),
		PartitionStart:  start,
		PartitionEnd:    end - 1, // half-open → inclusive; empty range = (0,-1), matches "no files"
		Replay:          f.replaySnapshot(),
	}
}

// refreshEagerReplay swaps each eager alias's Replay list for the feed's
// current snapshot before a retry re-dispatch. Builds a fresh map (never
// mutates the retrier's stored copy — Observe and RetryStuck may republish
// concurrently). No-op for tasks without eager inputs.
func refreshEagerReplay(t *distributed.Task, inputs map[string]StageOutput) {
	if len(t.EagerInputs) == 0 {
		return
	}
	fresh := make(map[string]distributed.EagerInput, len(t.EagerInputs))
	for alias, ei := range t.EagerInputs {
		if in, ok := inputs[alias]; ok && in.eager != nil {
			ei.Replay = in.eager.replaySnapshot()
		}
		fresh[alias] = ei
	}
	t.EagerInputs = fresh
}

// eagerFeedKey scopes feeds by query so concurrent queries with colliding
// plan-stage IDs (every plan numbers stages from 0) can't cross wires.
func eagerFeedKey(rootQueryID, planStageID string) string {
	return rootQueryID + "\x00" + planStageID
}

// eagerFeedHandle returns the feed for one producer stage of one query,
// creating it if absent. Returns nil when eager dispatch is off — callers
// treat nil as "barrier path only".
func (c *Coordinator) eagerFeedHandle(rootQueryID, planStageID string) *eagerFeed {
	if !c.config.EagerDispatch || !c.config.StreamingExchange || rootQueryID == "" {
		return nil
	}
	key := eagerFeedKey(rootQueryID, planStageID)
	c.eagerFeedsMu.Lock()
	defer c.eagerFeedsMu.Unlock()
	if c.eagerFeeds == nil {
		c.eagerFeeds = make(map[string]*eagerFeed)
	}
	if f, ok := c.eagerFeeds[key]; ok {
		return f
	}
	f := newEagerFeed()
	c.eagerFeeds[key] = f
	return f
}

// dropEagerFeeds removes and closes every feed of one query. Deferred by
// executeStageDAG so republishers never outlive their query.
func (c *Coordinator) dropEagerFeeds(rootQueryID string) {
	prefix := rootQueryID + "\x00"
	c.eagerFeedsMu.Lock()
	for key, f := range c.eagerFeeds {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			f.markClosed()
			delete(c.eagerFeeds, key)
		}
	}
	c.eagerFeedsMu.Unlock()
}

// startEagerRepublisher re-sends the feed's replay list on the manifest
// subject every eagerManifestRepublishEvery. Called by the shuffle
// dispatcher after feed.dispatch; runs until the QUERY ends (markClosed via
// dropEagerFeeds), not just until the producer stage drains — a consumer
// retry dispatched after the last producer finished still heals its stale
// Replay list from these re-sends. Metadata-only traffic on a root-scoped
// subject; a query's worth of manifests is tens of small messages.
func (c *Coordinator) startEagerRepublisher(f *eagerFeed) {
	if c.nc == nil {
		return
	}
	subject := distributed.EagerManifestSubject(f.rootQueryID, f.manifestStageID)
	go func() {
		ticker := time.NewTicker(eagerManifestRepublishEvery)
		defer ticker.Stop()
		for range ticker.C {
			if f.isClosed() {
				return
			}
			for _, m := range f.replaySnapshot() {
				data, err := distributed.Marshal(m)
				if err != nil {
					continue
				}
				// Fire-and-forget like the primary publish; a failed send
				// is retried by the next tick.
				if err := c.nc.Publish(subject, data); err != nil {
					break
				}
			}
		}
	}()
}

// eagerEligibleConsumer reports whether stage s may clear dispatch on its
// dependency's eager feed instead of the done barrier (memo §3.3, Phase C1
// scope: non-join consumers only).
//
// The gate requires:
//   - exactly one dependency and no scalar-substitution deps (those keep
//     the barrier: their values are extracted from completed outputs);
//   - a fragment-migrated single-input stage type: aggregate variants, or
//     sort variants with SortKeys (a keyless "sort" would fall to the
//     legacy task path, which reads Task.Inputs and cannot feed eagerly);
//   - the dependency is a standalone exchange-repartition — the only
//     producer the shuffle dispatcher registers feeds for in C1;
//   - not the gather-fused stage (fusion disables task retry, and retry is
//     the fencing recovery path — memo §5);
//   - no dynamic-filter participation (provisional outputs carry no
//     BuildStats);
//   - a task count no larger than workerCount, so with the scheduler's
//     eager-spread placement each worker holds at most one manifest-blocked
//     task and keeps ≥ MaxConcurrent−1 lanes for producer progress (the
//     §3.3 producer-lane reservation, v1 form).
//
// Join edges (56 of the 57 repartition edges in TPC-H plans) are Phase C2:
// they need the early skew decision before clearance.
func eagerEligibleConsumer(s physical.Stage, stageByID map[string]physical.Stage, fuseStageID string, workerCount int) bool {
	if len(s.Dependencies) != 1 || len(s.ScalarDependencies) > 0 {
		return false
	}
	if s.ID == fuseStageID {
		return false
	}
	if len(s.EmitDynamicFilters) > 0 || len(s.ConsumeDynamicFilters) > 0 {
		return false
	}
	switch s.Type {
	case "aggregate", "final_aggregate", "merge_aggregate":
	case "sort", "merge_sort":
		if len(s.SortKeys) == 0 {
			return false
		}
	default:
		return false
	}
	dep, ok := stageByID[s.Dependencies[0]]
	if !ok || dep.Type != physical.StageExchangeRepartition {
		return false
	}
	// Consumer task count (mirrors dispatchComputeStage's derivation for
	// the eligible types): Singleton → 1, HashPartitioned → Count.
	numTasks := 1
	if s.Distribution.Kind == physical.DistHashPartitioned {
		numTasks = s.Distribution.Count
		if numTasks <= 0 {
			numTasks = workerCount
		}
	}
	return numTasks <= workerCount
}
