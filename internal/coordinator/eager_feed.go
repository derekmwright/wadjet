package coordinator

import (
	"os"
	"strconv"
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

	// decisionReady closes once decisionThreshold producer tasks have
	// reached successful terminal state — the memo §6 early-skew decision
	// point. Join consumers block on it before clearing; non-join (C1)
	// consumers ignore it. Always closes at or before stage completion
	// (threshold ≤ total tasks).
	decisionReady chan struct{}

	// Set by dispatch(), immutable afterwards.
	manifestStageID   string   // stage ID used in EagerManifestSubject and manifests
	rootQueryID       string   // root query ID scoping the manifest subject
	producerTaskIDs   []string // full candidate set (task IDs are stable across retries)
	numPartitions     int
	decisionThreshold int // completions needed before decisionReady closes

	mu        sync.Mutex
	replay    []distributed.ProducerTaskManifest // manifests accumulated so far
	closed    bool                               // query finished; republisher must stop
	// active is set the first time a consumer stage clears dispatch on
	// this feed. Until then no NATS traffic leaves the coordinator for
	// this stage — the replay list and completion accounting still
	// accumulate (clearance decisions read them), but publication is
	// pure waste for the flag-on-no-clearance population (§12/§13: the
	// gated arm declined 37 of 46 stages and still paid for every one).
	// A consumer that clears later gets full history via its task-spec
	// Replay snapshot plus the republisher, both of which start at
	// activation.
	active             bool
	republisherStarted bool
	completed          int // producer tasks at successful terminal
	// firstDone/lastDone timestamp the first and most recent successful
	// producer-task completions. Feed the projected-tail eager gate
	// (projectedTailSeconds): the C3 SF100 pair (design doc §10) showed
	// eager clearance only converts when the producer still has a long
	// completion tail at clearance time, and costs slot occupancy when
	// it does not.
	firstDone time.Time
	lastDone  time.Time
	// observedPartBytes accumulates worker-reported per-partition output
	// bytes element-wise over completed tasks. nil until the first task
	// reports a vector; stays nil for legacy workers (skew decision
	// degrades to "would not split", matching planSkewSplitTasks on nil).
	observedPartBytes []int64
	decisionClosed    bool
}

func newEagerFeed() *eagerFeed {
	return &eagerFeed{
		dispatched:    make(chan struct{}),
		decisionReady: make(chan struct{}),
	}
}

// dispatch records the producer's task layout and releases consumers
// blocked on the feed. Call exactly once, before publishing producer tasks.
func (f *eagerFeed) dispatch(rootQueryID, manifestStageID string, producerTaskIDs []string, numPartitions, workerCount int) {
	f.rootQueryID = rootQueryID
	f.manifestStageID = manifestStageID
	f.producerTaskIDs = producerTaskIDs
	f.numPartitions = numPartitions
	// Memo §6 decision point: one full wave AND at least a quarter of the
	// producer tasks, so every producer's input-slice shape is represented
	// in the byte profile; capped at the task count so single-task or
	// tiny producers still reach a decision.
	total := len(producerTaskIDs)
	threshold := workerCount
	if q := (total + 3) / 4; q > threshold {
		threshold = q
	}
	if threshold > total {
		threshold = total
	}
	if threshold < 1 {
		threshold = 1
	}
	f.decisionThreshold = threshold
	close(f.dispatched)
}

// appendReplay folds one published manifest into the replay list handed to
// consumers built after this point (and re-sent by the republisher).
func (f *eagerFeed) appendReplay(m distributed.ProducerTaskManifest) {
	f.mu.Lock()
	f.replay = append(f.replay, m)
	f.mu.Unlock()
}

// noteCompletion records one producer task's successful terminal state and
// its per-partition output vector (nil when the worker didn't report).
// Closes decisionReady once the threshold is crossed. Called exactly once
// per producer task, from the manifest publisher hook.
func (f *eagerFeed) noteCompletion(partBytes []int64) {
	f.mu.Lock()
	f.completed++
	now := time.Now()
	if f.firstDone.IsZero() {
		f.firstDone = now
	}
	f.lastDone = now
	if len(partBytes) > 0 {
		if f.observedPartBytes == nil {
			f.observedPartBytes = make([]int64, f.numPartitions)
		}
		for p := 0; p < len(partBytes) && p < len(f.observedPartBytes); p++ {
			f.observedPartBytes[p] += partBytes[p]
		}
	}
	fire := !f.decisionClosed && f.completed >= f.decisionThreshold
	if fire {
		f.decisionClosed = true
	}
	f.mu.Unlock()
	if fire {
		close(f.decisionReady)
	}
}

// projectedPartitionBytes linearly extrapolates the observed per-partition
// bytes to the full producer task set (memo §6: hash partitioning makes
// each completed task an unbiased sample, so observed × total/completed
// converges fast). Returns nil when no task reported accounting — the
// caller treats that exactly like planSkewSplitTasks treats nil vectors:
// no split would fire on full data either.
func (f *eagerFeed) projectedPartitionBytes() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.observedPartBytes == nil || f.completed == 0 {
		return nil
	}
	total := int64(len(f.producerTaskIDs))
	out := make([]int64, len(f.observedPartBytes))
	for p, b := range f.observedPartBytes {
		out[p] = b * total / int64(f.completed)
	}
	return out
}

// projectedTailSeconds estimates the wall remaining until the producer's
// last task completes, from the completions observed so far: mean
// inter-arrival × tasks remaining. Called at consumer clearance time
// (after decisionReady, so at least a full wave of completions exists).
// Returns 0 when nothing remains or when fewer than two completions have
// landed (single-task producers, threshold==1) — no measurable tail means
// nothing worth overlapping, so the gate declines toward the barrier,
// never toward a wrong clearance.
//
// The C3 SF100 pair (eager-consumer-dispatch.md §10) is the calibration:
// every edge whose measured producer spread was ≥ ~12s converted under
// eager clearance (Q05 −29%, Q21, Q18, Q04, Q03); every edge below ~10s
// paid a slot-occupancy tax instead. eagerMinTailSeconds encodes that
// envelope; WADJET_EAGER_MIN_TAIL_SECONDS overrides it (0 = gate off,
// restoring the ungated C3 behavior for A/B).
func (f *eagerFeed) projectedTailSeconds() float64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	total := len(f.producerTaskIDs)
	remaining := total - f.completed
	if remaining <= 0 || f.completed < 2 || f.lastDone.Equal(f.firstDone) {
		return 0
	}
	meanGap := f.lastDone.Sub(f.firstDone).Seconds() / float64(f.completed-1)
	return meanGap * float64(remaining)
}

var eagerMinTailSeconds = func() float64 {
	if v := os.Getenv("WADJET_EAGER_MIN_TAIL_SECONDS"); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil && n >= 0 {
			return n
		}
	}
	return 12.0
}()

// replaySnapshot returns a copy of the manifests published so far.
func (f *eagerFeed) replaySnapshot() []distributed.ProducerTaskManifest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]distributed.ProducerTaskManifest(nil), f.replay...)
}

// activate marks the feed as having a cleared consumer, enabling live
// manifest publication. Returns true when the caller should start the
// republisher (first activation on an open feed). Idempotent.
func (f *eagerFeed) activate() (startRepublisher bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.active = true
	if f.republisherStarted || f.closed {
		return false
	}
	f.republisherStarted = true
	return true
}

func (f *eagerFeed) isActive() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.active
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
// current snapshot before a retry or governed-wave re-dispatch. Builds a
// fresh map (never mutates the retrier's stored copy — Observe and
// RetryStuck may republish concurrently). No-op for tasks without eager
// inputs.
//
// Task.EagerInputs is keyed by ALIAS (eagerAliasForDep: "probe"/build
// alias for joins, dep ID for single-input consumers) while the stage's
// inputs map is keyed by DEP ID — so the feed must be resolved through
// eagerAliasForDep, not by indexing inputs with the alias. The original
// alias-indexed lookup silently missed for every join consumer: late
// governed-wave tasks shipped their stale build-time Replay, lost every
// manifest published between clearance and their publish (before their
// subscription existed), and stalled one full republisher tick per wave
// — the §14.1 wave-quantization signature (3.0s beats in the A3 e2e;
// Q18 join-8 137s vs ~25s at SF100).
func refreshEagerReplay(t *distributed.Task, stage physical.Stage, inputs map[string]StageOutput) {
	if len(t.EagerInputs) == 0 {
		return
	}
	feedByAlias := make(map[string]*eagerFeed, len(t.EagerInputs))
	for depID, in := range inputs {
		if in.eager != nil {
			feedByAlias[eagerAliasForDep(stage, depID)] = in.eager
		}
	}
	fresh := make(map[string]distributed.EagerInput, len(t.EagerInputs))
	for alias, ei := range t.EagerInputs {
		if f, ok := feedByAlias[alias]; ok {
			ei.Replay = f.replaySnapshot()
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

// joinPrimaryDeps returns a join stage's (probe, build) dependency stage
// IDs, mirroring buildTaskInputsForStage / planSkewSplitTasks resolution.
func joinPrimaryDeps(s physical.Stage) (probeDep, buildDep string) {
	probeDep = s.LeftDepStage
	buildDep = s.RightDepStage
	if probeDep == "" && len(s.Dependencies) > 0 {
		probeDep = s.Dependencies[0]
	}
	if buildDep == "" && len(s.Dependencies) > 1 {
		buildDep = s.Dependencies[1]
	}
	return probeDep, buildDep
}

// eagerAliasForDep returns the Task.Inputs alias under which
// buildTaskInputsForStage files dependency depID for this stage.
// Task.EagerInputs must key identically, or the worker's fragment source /
// join-build routing won't find the feed spec.
func eagerAliasForDep(stage physical.Stage, depID string) string {
	switch stage.Type {
	case physical.StageHashJoin, physical.StageBroadcastJoin, physical.StageSortMergeJoin:
		_, buildDep := joinPrimaryDeps(stage)
		buildAlias := stage.BuildTableAlias
		if buildAlias == "" {
			buildAlias = "build"
		}
		if depID == buildDep {
			return buildAlias
		}
		probeAlias := "probe"
		if probeAlias == buildAlias {
			probeAlias = "probe_side"
		}
		return probeAlias
	default:
		return depID
	}
}

// eagerCapableComputeProducer reports whether compute stage d can back an
// eager feed (§14/A3): a hash-partitioned hash_join or final_aggregate
// whose plain unpartitioned sink writes one file per task, so task
// DISPATCH ORDER is the partition — the same positional contract the
// barrier path's StageOutput.Files gives partitionFilesForWorker, which
// the manifest source mirrors via the ProducerTaskIDs ordinal.
//
// Exchange-sink producers (stage.Exchange set) are out of A3 v1 scope;
// dynamic-filter and scalar participants keep the barrier (provisional
// outputs carry no BuildStats; scalar producers are read by the
// coordinator). Dispatch-time regroupings (skew split, rr-agg split,
// probe split, gather fusion) are gated at the wiring site — they remap
// task→partition and would break the ordinal contract.
func eagerCapableComputeProducer(d physical.Stage) bool {
	if d.Type != physical.StageHashJoin && d.Type != "final_aggregate" {
		return false
	}
	return d.Distribution.Kind == physical.DistHashPartitioned &&
		d.Distribution.Count > 0 &&
		d.Exchange == nil &&
		len(d.EmitDynamicFilters) == 0 && len(d.ConsumeDynamicFilters) == 0 &&
		len(d.ScalarDependencies) == 0
}

// eagerFeedableDep reports whether dependency stage d can be consumed
// through an eager feed: a standalone exchange-repartition (C1/C2) or an
// A3 compute producer.
func eagerFeedableDep(d physical.Stage) bool {
	if d.Type == physical.StageExchangeRepartition {
		return d.Exchange != nil && d.Exchange.Count > 0
	}
	return eagerCapableComputeProducer(d)
}

// eagerEligibleJoinConsumer reports whether join stage s may clear
// dispatch on its two producers' eager feeds (memo §3.3/§6, Phase C2
// scope). Requires:
//   - a hash_join on the fragment path (no GroupByCols — those take the
//     legacy task path, which reads frozen Task.Inputs). broadcast_join is
//     out of scope (broadcast edges stay S3+KV per memo §7);
//     sort_merge_join is dormant (gate off) and excluded from slice 1.
//   - both primary deps are standalone exchange-repartitions (the feeds'
//     producers); fused-build and chained-build deps keep the barrier
//     (their real outputs ride task.FusedJoins[i].BuildFiles /
//     task.Operators[i].BuildFiles — the dependency wait loop completes
//     them before the clearance decision runs, so an eager dispatch sees
//     real outputs for every non-primary dep).
//   - not the gather-fused stage, no scalar deps (joins never carry
//     dynamic-filter emits/consumes; checked defensively).
//
// Stage-chain fusion (§13, docs/design/stage-chain-fusion.md) grows
// Dependencies by exactly one per ChainedJoinSpec; a fused chain is
// eligible like any other hash join — only the primary probe/build feed
// eagerly, the chain's builds are complete by clearance time. A chain
// with an absorbed partial aggregate carries ChainedAgg* fields, not
// GroupByCols, so the fragment-path restriction above is unaffected.
//
// The skew decision itself happens later, at the feed threshold
// (eagerJoinWouldSplit) — this gate is structural only.
func eagerEligibleJoinConsumer(s physical.Stage, stageByID map[string]physical.Stage, fuseStageID string) bool {
	if s.Type != physical.StageHashJoin {
		return false
	}
	if len(s.GroupByCols) > 0 || s.ID == fuseStageID || len(s.ScalarDependencies) > 0 {
		return false
	}
	if len(s.EmitDynamicFilters) > 0 || len(s.ConsumeDynamicFilters) > 0 {
		return false
	}
	if len(s.Dependencies) != 2+len(s.FusedJoins)+len(s.ChainedJoins) {
		return false
	}
	probeDep, buildDep := joinPrimaryDeps(s)
	for _, dep := range []string{probeDep, buildDep} {
		d, ok := stageByID[dep]
		if !ok || !eagerFeedableDep(d) {
			return false
		}
	}
	return true
}

// eagerJoinWouldSplit is the memo §6 early skew decision: projects both
// feeds' observed per-partition bytes to the full producer set and asks
// whether ANY task group would split under the exact planSkewSplitTasks
// thresholds (absolute floor, ratio gate, build replication bound). A
// "yes" sends the consumer back to the barrier, where the full-data
// decision governs — eager dispatch may only ever cost a split win, never
// produce a wrong split (slice 1; frozen eager split layouts are slice 2).
//
// Differences from planSkewSplitTasks, both conservative toward "would
// split" (i.e. toward the barrier): no probe-file-count cap on the split
// factor (file counts are unknowable before completion), and missing
// accounting on either side returns false — nil vectors would disable the
// full-data split too, so no win is lost by clearing eagerly.
func (c *Coordinator) eagerJoinWouldSplit(stage physical.Stage, probeFeed, buildFeed *eagerFeed, numTasks, workerCount int) bool {
	if !c.config.SkewSplit {
		return false
	}
	if stage.Type != physical.StageHashJoin ||
		stage.Distribution.Kind != physical.DistHashPartitioned ||
		numTasks <= 0 || workerCount <= 1 {
		return false
	}
	// Partitioned chained builds bind build partition i to task i, so the
	// dispatcher never skew-splits such stages (dispatchComputeStage skips
	// planSkewSplitTasks). Mirror that: projecting a split the full-data
	// path would never take would send the consumer to the barrier for
	// nothing.
	for _, cj := range stage.ChainedJoins {
		if cj.Partitioned {
			return false
		}
	}
	switch stage.JoinType {
	case "inner", "left", "semi", "anti":
	default:
		return false
	}
	probeBytes := probeFeed.projectedPartitionBytes()
	buildBytes := buildFeed.projectedPartitionBytes()
	if probeFeed.numPartitions != buildFeed.numPartitions ||
		len(probeBytes) != probeFeed.numPartitions ||
		len(buildBytes) != buildFeed.numPartitions {
		return false
	}
	buildBound := skewSplitMaxBuildBytes
	if c.workers != nil {
		if t := broadcastThresholdFromCluster(c.workers.MinWorkerPoolBudget()); t > 0 {
			buildBound = t
		}
	}
	var totalProbeBytes int64
	for _, b := range probeBytes {
		totalProbeBytes += b
	}
	meanGroupBytes := totalProbeBytes / int64(numTasks)
	for w := 0; w < numTasks; w++ {
		start, end := partitionRangeForWorker(probeFeed.numPartitions, w, numTasks)
		var pb, bb int64
		for p := start; p < end; p++ {
			pb += probeBytes[p]
			bb += buildBytes[p]
		}
		k := 0
		if pb > 0 {
			k = int((pb + skewSplitTargetBytes - 1) / skewSplitTargetBytes)
		}
		if k > workerCount {
			k = workerCount
		}
		ratio := float64(0)
		if meanGroupBytes > 0 {
			ratio = float64(pb) / float64(meanGroupBytes)
		}
		if k > 1 && pb > skewSplitMinGroupBytes && ratio >= skewSplitMinRatio && bb <= buildBound {
			return true
		}
	}
	return false
}

// eagerPublishGovernor caps in-flight eager consumer tasks so producers
// always keep worker task slots. Eager tasks block on producer manifests
// while HOLDING their slot, and neither transport can reorder around them
// (gRPC dispatch is targeted with no work stealing; NATS pulls are FIFO) —
// so publishing a full join fan-out (numTasks = partition count, typically
// several × cluster capacity) at once could wedge every slot behind tasks
// waiting on producers that can no longer run. The governor publishes the
// first `cap` tasks and tops up one-for-one as published tasks reach a
// TERMINAL state (a failure that will retry keeps its slot — the retry
// re-occupies it).
//
// The residual hazard is a producer-task retry published after eager
// consumers (it queues behind them on one worker); the task deadline
// bounds that stall, same as any other slot contention. Documented in the
// memo §3.3 v1 notes.
type eagerPublishGovernor struct {
	mu      sync.Mutex
	pending []distributed.Task
	freed   map[string]bool // task IDs whose terminal state already topped up
	publish func(distributed.Task)
}

// newEagerPublishGovernor splits tasks into a first wave (returned for the
// caller's initial PublishTasks) and a governed remainder. inFlightCap is
// clamped to ≥ 1.
func newEagerPublishGovernor(tasks []distributed.Task, inFlightCap int, publish func(distributed.Task)) (firstWave []distributed.Task, g *eagerPublishGovernor) {
	if inFlightCap < 1 {
		inFlightCap = 1
	}
	if len(tasks) <= inFlightCap {
		return tasks, nil
	}
	return tasks[:inFlightCap], &eagerPublishGovernor{
		pending: append([]distributed.Task(nil), tasks[inFlightCap:]...),
		freed:   make(map[string]bool),
		publish: publish,
	}
}

// noteTerminal releases one slot for task taskID (idempotent per task) and
// publishes the next pending task, if any. Called from the stage's result
// subscription after retrier.Observe confirms the task is terminal; the
// publish runs on this goroutine — callers already tolerate the same NATS
// round-trip in the retry republish path.
func (g *eagerPublishGovernor) noteTerminal(taskID string) {
	g.mu.Lock()
	if g.freed[taskID] || len(g.pending) == 0 {
		g.mu.Unlock()
		return
	}
	g.freed[taskID] = true
	next := g.pending[0]
	g.pending = g.pending[1:]
	g.mu.Unlock()
	g.publish(next)
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
//   - the dependency backs an eager feed: a standalone
//     exchange-repartition, or an A3 compute producer
//     (eagerFeedableDep);
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
	if !ok || !eagerFeedableDep(dep) {
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
