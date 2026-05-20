package physical

// annotateMaxConcurrent assigns Stage.MaxConcurrentPerWorker for join
// stages whose per-task build-side memory footprint is large enough that
// running them concurrently on a worker would overcommit the worker heap.
//
// SF100 mechanism (project_q17_sf100_instrumented_2026-05-17): three
// concurrent fragment tasks at mc=3 each hold ~5.7 GB of
// HashJoin/build=lineitem state on a worker with 14.8 GB GOMEMLIMIT.
// Cumulative ~17 GB pins the worker in GC mark-assist. Reducing the
// per-stage concurrent count to fit within the budget avoids the pin
// without admission-gating on memory pressure (which deadlocks chained
// build/probe — see project_admission_control_rejected_2026-05-18).
//
// Why per-stage-ID, not per-memory: build and probe stages have distinct
// stage IDs. Throttling the build stage doesn't gate the probe, so the
// probe can still pull and release the build's hash-table memory through
// downstream consumption. The deadlock mode that took out
// archive/heap-admission-task-entry doesn't apply.
//
// workerHeapBudget is the smallest per-worker shared-pool budget reported
// by any active worker. Pass 0 to disable annotation entirely (no info →
// no throttle).
func annotateMaxConcurrent(stages []Stage, workerHeapBudget int64) {
	if workerHeapBudget <= 0 {
		return
	}
	// Reserve ~25% of the per-worker pool for non-build transients (batch
	// pool, runtime overhead, the operator's own probe state on the same
	// worker). 75% empirically lines up with the 11 GB usable budget on a
	// 14.8 GB GOMEMLIMIT SF100 worker.
	usable := workerHeapBudget * 75 / 100
	byID := make(map[string]*Stage, len(stages))
	for i := range stages {
		byID[stages[i].ID] = &stages[i]
	}
	for i := range stages {
		s := &stages[i]
		if s.Type != StageHashJoin && s.Type != StageBroadcastJoin {
			continue
		}
		build := buildSideEstimatedBytes(byID, s)
		if build <= 0 {
			// No estimate available — fail open, don't annotate. The
			// global worker MaxConcurrent still applies.
			continue
		}
		// Activation threshold: don't bother throttling small builds.
		// 512 MB chosen so SF1/SF10 build sizes stay unannotated (their
		// lineitem-side builds estimate well below this) while SF100's
		// 5-7 GB builds always trigger.
		const minBuildBytes = int64(512 * 1024 * 1024)
		if build < minBuildBytes {
			continue
		}
		// cap = floor(usable / build), floored at 1.
		capacity := int(usable / build)
		if capacity < 1 {
			capacity = 1
		}
		// Only annotate when the cap is below the conservative default
		// worker concurrency (4). Higher caps are no-ops since the
		// worker's own MaxConcurrent already bounds them, and annotating
		// would add lock churn for nothing.
		if capacity >= 4 {
			continue
		}
		s.MaxConcurrentPerWorker = capacity
	}
}

// buildSideEstimatedBytes returns the planner's bytes estimate for the
// build (right) side of a join stage, by walking Dependencies from
// RightDepStage looking for scan stages. Returns 0 when no scan with a
// populated EstimatedBytes is reachable within a small depth (fail-
// closed: a missing estimate disables annotation rather than over-
// throttling on guesswork).
//
// Typical shapes covered:
//   - join → scan (broadcast probe of a simple table)
//   - join → exchange-replicate → scan (broadcast cache materialization)
//   - join → exchange-repartition → scan (hash-join shuffle)
//   - join → exchange-repartition → aggregate → scan (Q17/Q18 with
//     partial aggregation between scan and join)
//
// Walk depth is capped at 4 — beyond that, the build subtree is unusual
// enough that the conservative path (no annotation) is safer than a guess.
func buildSideEstimatedBytes(byID map[string]*Stage, joinStage *Stage) int64 {
	if joinStage == nil {
		return 0
	}
	rd := joinStage.RightDepStage
	if rd == "" {
		return 0
	}
	visited := make(map[string]bool)
	frontier := []string{rd}
	var total int64
	for depth := 0; depth < 4 && len(frontier) > 0; depth++ {
		next := frontier
		frontier = nil
		for _, id := range next {
			if visited[id] {
				continue
			}
			visited[id] = true
			s, ok := byID[id]
			if !ok {
				continue
			}
			if s.Type == StageScan {
				if s.EstimatedBytes > 0 {
					total += s.EstimatedBytes
				}
				// Scans are leaves — don't follow their deps.
				continue
			}
			for _, dep := range s.Dependencies {
				if !visited[dep] {
					frontier = append(frontier, dep)
				}
			}
		}
	}
	return total
}
