// This file holds join plan for the physical planner, governed by ADR-0023 and ADR-0026.
package physical

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// isBroadcastCandidate returns true if the right (build) side of a join is
// small enough to broadcast to all workers. When broadcast, the build side
// is sent to every worker and the probe side is split round-robin across
// workers — no shuffle stages needed for either side.
func (p *Planner) isBroadcastCandidate(joinNode *logical.Node) bool {
	if len(joinNode.Children) < 2 {
		return false
	}
	totalBytes, ok := p.estimateSubtreeBytes(joinNode.Children[1])
	if !ok {
		return false
	}
	// Broadcast threshold: defaults to 100 MB (legacy behavior). Distributed
	// callers override via BroadcastBytesThreshold to adapt the decision to
	// per-worker pool budget so a moderate build (say 500 MB) on a tight
	// cluster falls back to hash-shuffle instead of multiplying memory
	// pressure N× across worker procs.
	threshold := int64(100 * 1024 * 1024)
	if p.BroadcastBytesThreshold > 0 {
		threshold = p.BroadcastBytesThreshold
	} else if p.BroadcastBytesThreshold < 0 {
		return false // broadcast disabled
	}
	return totalBytes <= threshold
}

// estimateSubtreeBytes estimates a join input's post-selectivity size by
// walking through Filter/Project/Limit wrappers to the underlying Scan and
// scaling the table's manifest bytes by the subtree's estimated selectivity.
// Returns ok=false when the subtree has no scan root (e.g. another join) or
// the manifest is unavailable — callers must treat "unknown" conservatively.
//
// Under BushyJoinReorder, join-shaped subtrees (composite build sides) are
// estimated too: output bytes ≈ estimated output rows × the combined
// per-row width of the join's visible inputs. Without this, a 25-row
// nation ⋈ region pre-join is "unknown" → never broadcast-eligible → the
// whole composite pays exchange-repartition for both sides (the Q08 SF10
// regression, 2026-07-09: +135% from shuffling what should replicate).
// Gated on the flag: flag-off keeps semi/anti-leaf builds on their
// SF100-validated shuffle plans.
func (p *Planner) estimateSubtreeBytes(n *logical.Node) (int64, bool) {
	// Distinct(Project[keys]) build sides (IN/EXISTS decorrelation and
	// scalar-agg-semijoin key sources) are sized by distinct KEY count,
	// not table bytes × row selectivity. The row-selectivity path is a
	// trap here: Q04's EXISTS build is Distinct(l_orderkey) over lineitem
	// filtered by the col-vs-col l_commitdate < l_receiptdate — heuristic
	// selectivity shrank 76 GB of lineitem under the broadcast threshold
	// and SF100 replicated a ~200M-key hash build to every worker
	// (observed 2026-07-21: Q04 37s → 81s). Key-count × key-width says
	// ~1.6 GB → correctly stays on the shuffle plan, while Q17's filtered
	// part key source (~20K keys) still broadcasts.
	if n != nil && n.Type == logical.NodeDistinct {
		return p.estimateDistinctKeyBytes(n)
	}
	scan := findScanNode(n)
	if scan == nil {
		if logical.BushyJoinReorder.Load() {
			return p.estimateJoinSubtreeBytes(n)
		}
		return 0, false
	}
	// Estimate size from file count
	manifest, err := p.getManifest(context.Background(), scan.TableName)
	if err != nil {
		return 0, false
	}
	var totalBytes, totalRows int64
	for _, part := range manifest.Partitions {
		for _, f := range part.Files {
			totalBytes += f.SizeBytes
			totalRows += f.NumRows
		}
	}
	// Apply filter selectivity to the bytes estimate. Q17's
	// `part WHERE p_brand=... AND p_container=...` filters 2M rows to
	// ~13K, dropping the build size from 200 MB raw to ~1.5 MB — well
	// below the broadcast threshold. Without this scaling, the raw
	// 200 MB exceeds the threshold and Q17 falls through to a
	// hash-shuffle that pays exchange-repartition for BOTH lineitem
	// (60M rows ≈ 6 GB at SF10) and part.
	//
	// Source of selectivity: RelStatsOf walks the subtree applying
	// histogram-driven predicate selectivity (CBO Phase 3 work). For
	// tables without HLL/histogram, the existing 0.33/0.1 heuristic
	// still applies so the scaling is at-worst-conservative.
	if totalRows > 0 {
		stats := logical.RelStatsOf(n)
		if stats.Rows > 0 && stats.Rows < float64(totalRows) {
			scale := stats.Rows / float64(totalRows)
			totalBytes = int64(float64(totalBytes) * scale)
		}
	}
	return totalBytes, true
}

// estimateDistinctKeyBytes sizes a Distinct(Project[cols]) subtree as
// distinct-key-count × a fixed per-key width. Distinct rows = min(input
// row estimate, product of the projected columns' NDVs); when any NDV is
// unavailable the input row estimate alone is the (looser) bound. Returns
// unknown for shapes other than Distinct→Project — bytes-based reasoning
// through a bare Distinct has no reliable width to scale by, and unknown
// degrades to the shuffle plan, the safe side.
func (p *Planner) estimateDistinctKeyBytes(d *logical.Node) (int64, bool) {
	if len(d.Children) != 1 {
		return 0, false
	}
	proj := d.Children[0]
	if proj == nil || proj.Type != logical.NodeProject ||
		len(proj.Children) != 1 || len(proj.Projections) == 0 {
		return 0, false
	}
	stats := logical.RelStatsOf(proj.Children[0])
	if stats.Rows <= 0 {
		return 0, false
	}
	for _, pr := range proj.Projections {
		if pr.Column == "" {
			return 0, false // expression projection — no width to reason from
		}
	}
	// Size by INPUT rows, not NDV: walkStages treats Distinct as a
	// passthrough (see the #163 note), so the replicated payload is the
	// UNdeduplicated projected scan output. Q22's anti build is
	// Distinct(o_custkey) over unfiltered orders — ~10M distinct keys but
	// 150M shipped rows; NDV-based sizing called it 160MB and SF100
	// replicated + hashed 150M rows on every worker (observed live
	// 2026-07-21: Q22 18.7s → 2m34s cold). Row-based sizing says 2.4GB →
	// correctly stays on the shuffle plan, while Q17's ~20K-row filtered
	// part key source still broadcasts. If replicate ever materializes
	// the dedup, this can tighten back toward NDV.
	//
	// 16 bytes per key column: int64 key + hash-table overhead. TPC-H
	// (and typical) semi-join keys are ints; a string-keyed build would
	// be underestimated, but the shuffle fallback on overflow is still
	// merely slower, not wrong.
	const bytesPerKeyCol = 16
	return int64(stats.Rows) * bytesPerKeyCol * int64(len(proj.Projections)), true
}

// estimateJoinSubtreeBytes estimates the output size of a join-shaped
// subtree: estimated output rows × the per-row width of the join's visible
// inputs (probe + build for inner/outer, probe only for semi/anti). Width
// derives from each input's own bytes/rows estimate (mutual recursion with
// estimateSubtreeBytes), so filter selectivity scaling composes through
// nesting; the plan tree is finite so the recursion terminates.
func (p *Planner) estimateJoinSubtreeBytes(n *logical.Node) (int64, bool) {
	if n == nil {
		return 0, false
	}
	// Unwrap single-child pass-through nodes to the join.
	for n != nil && n.Type != logical.NodeJoin {
		switch n.Type {
		case logical.NodeFilter, logical.NodeProject, logical.NodeLimit:
			if len(n.Children) == 1 {
				n = n.Children[0]
				continue
			}
		}
		return 0, false
	}
	if n == nil || len(n.Children) != 2 {
		return 0, false
	}

	sideWidth := func(child *logical.Node) (float64, bool) {
		bytes, ok := p.estimateSubtreeBytes(child)
		if !ok {
			return 0, false
		}
		rows := logical.RelStatsOf(child).Rows
		if rows < 1 {
			rows = 1
		}
		return float64(bytes) / rows, true
	}

	width, ok := sideWidth(n.Children[0])
	if !ok {
		return 0, false
	}
	jt := strings.ToLower(n.JoinType)
	if jt != "semi" && jt != "anti" {
		buildWidth, ok := sideWidth(n.Children[1])
		if !ok {
			return 0, false
		}
		width += buildWidth
	}
	rows := logical.RelStatsOf(n).Rows
	if rows < 1 {
		rows = 1
	}
	return int64(rows * width), true
}

// findScanNode walks through pass-through nodes (Filter, Project, Limit)
// to find the underlying Scan node, if any.
func findScanNode(n *logical.Node) *logical.Node {
	for n != nil {
		switch n.Type {
		case logical.NodeScan:
			return n
		case logical.NodeFilter, logical.NodeProject, logical.NodeLimit:
			if len(n.Children) == 1 {
				n = n.Children[0]
				continue
			}
		}
		return nil
	}
	return nil
}

func (p *Planner) buildJoin(ctx context.Context, node *logical.Node) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	if len(node.Children) < 2 {
		return nil, nil, nil, fmt.Errorf("join requires two children")
	}
	// A LATERAL body with NO FROM clause is a projection over the outer row,
	// not a join (#1033). See buildTableLessLateralJoin.
	if len(node.LateralDualItems) > 0 {
		return p.buildTableLessLateralJoin(ctx, node)
	}

	jt := mapJoinType(node.JoinType)
	// An inner join with no condition at all IS a cross join (#376): the
	// join reorderer emits this shape for a comma-joined relation with no
	// edge to the rest of the chain, and reading the absent condition as a
	// failed key extraction refused legal SQL.
	if jt == "inner" && strings.TrimSpace(node.JoinCond) == "" && node.JoinFilter == "" {
		jt = "cross"
	}
	joinType := mapExecJoinType(jt)
	// An outer join may carry an ON residual (#358) — routed there by
	// logical.routeOuterJoinOnResiduals — and with it, zero key pairs.
	outerResidual := node.JoinFilter != "" &&
		(jt == "left" || jt == "right" || jt == "full")

	// Parse join condition to extract key columns (cross joins have no ON clause)
	var leftKeys, rightKeys []string
	if jt != "cross" {
		var residual []string
		leftKeys, rightKeys, residual = parseJoinKeys(node.JoinCond)
		if len(residual) > 0 {
			return nil, nil, nil, refuseJoinCond(jt, node.JoinCond, residual)
		}
		if len(leftKeys) == 0 && !outerResidual {
			return nil, nil, nil, fmt.Errorf("could not extract join keys from: %s", node.JoinCond)
		}
		// Fix key assignment using plan-level column ownership: ensure left keys
		// are probe-side and right keys are build-side. This avoids the expensive
		// post-build FixKeyAssignment hash table rebuild.
		assignJoinKeySides(leftKeys, rightKeys,
			subtreeNamingOf(node.Children[0]), subtreeNamingOf(node.Children[1]))
	}

	// Big-vs-big inner equi-joins route to sort-merge join when BOTH sides'
	// estimated bytes reach SortMergeJoinBytes (0 = disabled, the shipped
	// default — this branch is dormant unless the deploy opts in). Small
	// builds keep the strictly-better hash path below, unchanged.
	if joinType == exec.InnerJoin && node.JoinFilter == "" && len(leftKeys) > 0 &&
		p.shouldSortMergeJoin(node, leftKeys, rightKeys) {
		return p.buildSortMergeJoin(ctx, node, leftKeys, rightKeys)
	}

	hj := exec.NewHashJoin(joinType, leftKeys, rightKeys)
	p.builtJoins = append(p.builtJoins, hj)
	// The pair's COMMON type, which both sides' key bytes are built at and
	// which the integer / bloom fast paths are gated on (#615, ADR-0023).
	// Nil for every join whose key types already agree — every TPC-H join —
	// and the operator then behaves exactly as it did.
	hj.KeyTypes = resolveJoinKeyTypes(node, leftKeys, rightKeys)

	// Set build-side table alias for column disambiguation in self-joins
	if alias := joinArmAlias(node.Children[1]); alias != "" {
		hj.BuildTableAlias = alias
	}
	// Multi-table build subtrees carry per-column origin aliases so each
	// duplicate qualifies under its OWNING scan (nil for single-scan builds).
	hj.BuildColOrigins = subtreeNamingOf(node.Children[1]).materializedBuildColOrigins()

	// Grace Hash Join spill-to-disk: prevents OOM on large build sides (e.g.
	// SF100 orders table at 150M rows). The shared MemTracker means multi-join
	// queries may spill earlier than strictly necessary, but OOM is worse.
	if sm := p.getSpillManager(); sm != nil {
		hj.Spill = sm
		hj.MemTracker = p.getMemTracker()
	}

	// Semi/anti join build/probe swap: when the inner (build) table is much
	// larger than the outer (probe) table, swap to RightSemiJoin/RightAntiJoin.
	// This builds the SMALL outer table as hash table and streams the LARGE
	// inner table as probe, then emits matched (semi) or unmatched (anti)
	// build rows. Dramatically reduces memory: e.g. Q04 at SF100 goes from
	// 16GB lineitem hash table to 1.7GB orders hash table per worker.
	rightEst := findScanRowEstimate(node.Children[1])
	leftEst := findScanRowEstimate(node.Children[0])
	// NOT IN's three-valued rule, which the anti join does not ask on its own
	// (#507). The logical rewrite is the only thing that knows this anti join
	// came from a NOT IN rather than a NOT EXISTS — and this is computed
	// BEFORE the swap below, because after it joinType is RightAntiJoin and
	// the flag would silently evaluate to false. It did: the local path
	// dropped the rule whenever the estimator chose the swap, while the DAG
	// (whose worker sets the flag from the spec) kept it — a two-path
	// divergence with PostgreSQL on the DAG's side.
	nullAwareAnti := node.NullAwareAnti && joinType == exec.AntiJoin

	// A filtered semi/anti join must NOT swap: the RightSemi/RightAnti probe
	// (markMatchedBuildEntries) marks every key-chain entry matched and never
	// evaluates SemiAntiFilter, so the non-equality condition would silently
	// vanish from the query — wrong results, not a performance trade.
	//
	// A NULL-AWARE anti join must not swap either, and for the same KIND of
	// reason: its two rules (a NULL probe key never survives; a NULL anywhere
	// in the build empties the answer) are applied on the semi/anti probe
	// path, which RightAntiJoin does not take — it marks build entries during
	// the probe and emits the unmatched ones from the arena afterwards. The
	// rules would vanish exactly as the filter would.
	if (joinType == exec.SemiJoin || joinType == exec.AntiJoin) && node.JoinFilter == "" && !nullAwareAnti &&
		rightEst > 0 && leftEst > 0 && rightEst > 3*leftEst {
		// Swap: build the small outer table, probe with the large inner table
		if joinType == exec.SemiJoin {
			joinType = exec.RightSemiJoin
		} else {
			joinType = exec.RightAntiJoin
		}
		hj.JoinType = joinType
		// Swap children
		node.Children[0], node.Children[1] = node.Children[1], node.Children[0]
		// Swap keys and update the hash join
		leftKeys, rightKeys = rightKeys, leftKeys
		hj.LeftKeys = leftKeys
		hj.RightKeys = rightKeys
		assignJoinKeySides(leftKeys, rightKeys,
			subtreeNamingOf(node.Children[0]), subtreeNamingOf(node.Children[1]))
		// Update build-side alias + origins after swap
		if alias := joinArmAlias(node.Children[1]); alias != "" {
			hj.BuildTableAlias = alias
		}
		hj.BuildColOrigins = subtreeNamingOf(node.Children[1]).materializedBuildColOrigins()
	}

	// Plan-declared schemas for the two sides, read only when a side delivers
	// no batch at all: an outer join still owes the rows the empty side
	// shapes and cannot name their columns without this (#348/#352). Computed
	// after the semi/anti swap above so the sides are final.
	// Declared by what each side PUBLISHES. On this path a derived block's
	// Project is a real operator, so a hint read from the scan below it
	// described an EMPTY side by columns the full side never emits — eight
	// columns for PostgreSQL's five (round-1 P2).
	hj.ProbeSchemaHint, hj.BuildSchemaHint = joinSideSchemas(node, hj.LeftKeys, hj.RightKeys,
		sideBlockProjections(node), p.subqueryOutputColumn)

	// For semi/anti joins without a filter, enable key-only build:
	// only build the key index and bloom filter, skip batch storage and arena refs.
	if (joinType == exec.SemiJoin || joinType == exec.AntiJoin) && node.JoinFilter == "" {
		hj.SemiAntiKeyOnly = true
	}

	hj.NullAwareAnti = nullAwareAnti

	// Filtered semi/anti builds must store rows for probe-time SemiAntiFilter
	// evaluation, but only the join keys + filter-referenced columns — narrow
	// the stored batches at arrival (see HashJoin.BuildStoreCols; the post-
	// build PruneBuildColumns below is a no-op for partition-on-arrival
	// builds, i.e. for every spill-eligible build).
	if (joinType == exec.SemiJoin || joinType == exec.AntiJoin) && node.JoinFilter != "" {
		hj.BuildStoreCols = SemiAntiBuildStoreCols(hj.RightKeys, node.JoinFilter)
	}

	// Pass build-side row estimate to pre-allocate arena and hash table.
	est := findScanRowEstimate(node.Children[1])
	if est > 0 {
		hj.BuildRowHint = est
	}

	// Determine if build should be deferred: large semi/anti builds overlap
	// with the early probe pipeline for better I/O utilization. Applies to
	// key-only builds and to filtered builds (whose arrival-time projection
	// keeps the deferred build's storage narrow); the deferred path's post-
	// build FixKeyAssignment is safe for partitioned builds via the nil'd-
	// entry guard, and PruneBuildColumns self-skips them.
	const deferBuildThreshold int64 = 1_000_000
	deferBuild := est > deferBuildThreshold &&
		(hj.SemiAntiKeyOnly || len(hj.BuildStoreCols) > 0)

	// Reverse bloom: for very large builds (>10M rows), run the probe side
	// first, build a bloom from its join key values, then filter the build
	// scan. Sacrifices I/O overlap for massive scan reduction.
	//
	// For semi/anti joins: bloom has no false negatives, correctness preserved.
	// For inner joins with large builds (>50M rows): each probe-split worker
	// only sees 1/N of the probe keys, so ~(N-1)/N of build rows are filtered
	// out. At SF100 with 3 workers, this reduces orders (150M) to ~50M and
	// partsupp (80M) to ~27M per worker — fitting in 4GB memory budget.
	useReverseBloom := reverseBloomToggle.On() &&
		((est > ReverseBloomThreshold && (joinType == exec.SemiJoin || joinType == exec.AntiJoin)) ||
			(est > ReverseBloomInnerThreshold && joinType == exec.InnerJoin))

	// Pre-compute post-build operations that can run in the build goroutine.
	var keepCols []string
	if joinType == exec.SemiJoin || joinType == exec.AntiJoin {
		keepCols = extractFilterBuildColumns(node.JoinFilter)
	}
	if (joinType == exec.SemiJoin || joinType == exec.AntiJoin) && node.JoinFilter != "" {
		hj.SemiAntiFilter = BuildSemiAntiFilter(node.JoinFilter)
		if pc, bc, ok := ParseSemiAntiNE(node.JoinFilter); ok {
			hj.SemiAntiNEProbeCol, hj.SemiAntiNEBuildCol = pc, bc
		}
	}
	if outerResidual {
		hj.Residual = BuildJoinResidualFilter(node.JoinFilter, hj.BuildTableAlias)
		if hj.Residual == nil {
			// Refuse loudly rather than answer with the conjunct dropped —
			// the pre-#358 failure mode this path replaced.
			return nil, nil, nil, fmt.Errorf("join ON residual %q on a %s join: "+
				"not evaluable as a probe residual (columns, literals, arithmetic and "+
				"comparisons are; function calls and subqueries are not)",
				node.JoinFilter, jt)
		}
	}

	// Build right side (small table) into hash table
	rightSource, rightOps, _, err := p.buildPipeline(ctx, node.Children[1])
	if err != nil {
		return nil, nil, nil, fmt.Errorf("building join right side: %w", err)
	}

	// Wrap right side source + ops into a single source for Build()
	buildSource := &pipelineSource{
		source: rightSource,
		ops:    rightOps,
	}

	// Launch hash table build in a goroutine while concurrently preparing
	// the left (probe) side. For multi-way joins, the left side recursively
	// calls buildJoin → each level overlaps its build with the next level's
	// preparation, so all independent hash table builds run concurrently.
	//
	// For reverse bloom, the build goroutine waits for a signal before
	// starting — the probe side's child pipeline must finish first so we
	// can inject a bloom filter into the build-side scan.
	var buildStart chan struct{}
	if useReverseBloom {
		buildStart = make(chan struct{})
	}
	buildDone := make(chan struct{})
	var buildErr error
	var rbBuildSource exec.Source = buildSource // may be wrapped with bloom
	go func() {
		defer close(buildDone)
		// The build runs here so it can overlap with the probe side's
		// preparation, which also means nothing above it recovers: this
		// goroutine's whole call stack — pipelineSource.runFrom, the
		// operators pushed onto the build side, every expression they
		// evaluate — had no boundary at all. A filter pushed onto a join's
		// build side that raised the DESIGNED query-error panic (an invalid
		// cast) therefore killed the SERVER, while the same condition on a
		// scan filter or the fast path returned a normal error to the client
		// (#508). Everything else it can raise did the same.
		//
		// buildErr is read by all three consumers after the barrier, so
		// setting it is the whole delivery. Registered after
		// close(buildDone), so it runs FIRST on the way out and the error is
		// in place before the barrier opens.
		defer exec.CatchQueryPanic(ctx, "hash join build", func(err error) {
			buildErr = fmt.Errorf("building hash table: %w", err)
		})
		if buildStart != nil {
			<-buildStart // wait for reverse bloom injection
		}
		if err := hj.Build(ctx, rbBuildSource); err != nil {
			buildErr = fmt.Errorf("building hash table: %w", err)
			return
		}
		if deferBuild || useReverseBloom {
			if hj.FixKeyAssignment() {
				slog.Warn("join key repair fired at runtime — plan-time side assignment missed a pair",
					"left_keys", hj.LeftKeys, "right_keys", hj.RightKeys)
			}
			if joinType == exec.SemiJoin || joinType == exec.AntiJoin {
				hj.PruneBuildColumns(keepCols)
			}
		}
	}()

	// Left side (probe) streams through — prepared concurrently with build.
	leftSource, leftOps, _, err := p.buildPipeline(ctx, node.Children[0])
	if err != nil {
		if buildStart != nil {
			close(buildStart)
		}
		<-buildDone // prevent goroutine leak
		// The build and the probe are prepared CONCURRENTLY, so when the build
		// fails the probe can fail too — on the shared scan claim the build's
		// own teardown just released. Which of the two returns first is a
		// scheduling race, and reporting the probe's would hand the client the
		// consequence ("shared scan of lat_item did not complete") in place of
		// the reason ("filter column \"amount\" does not exist"). A masked root
		// cause is a diagnosis defect, so when both failed and the probe's is
		// only an abandoned claim, the build's error is the answer.
		if buildErr != nil && isAbandonedClaim(err) {
			return nil, nil, nil, buildErr
		}
		return nil, nil, nil, fmt.Errorf("building join left side: %w", err)
	}

	if useReverseBloom {
		// Reverse bloom: run probe-side child pipeline first, build a bloom
		// from its join key values, inject as filter on build-side scan, then
		// signal the build goroutine to start with the filtered scan.
		probe := hj.Probe()
		p.applyLateMaterialization(probe)
		if f := joinProbeOutputFilter(node); f != nil {
			probe.OutputFilter = f
		}
		probe.OutputExcludeProbe, probe.OutputExcludeBuild = joinHiddenPositions(node)

		bridge := &reverseBloomBridge{
			childSource:   leftSource,
			childOps:      leftOps,
			rbBuildSource: &rbBuildSource,
			buildSource:   buildSource,
			buildStart:    buildStart,
			barrier:       buildDone,
			buildErr:      &buildErr,
			probeKey:      leftKeys[0],
			buildKey:      rightKeys[0],
			workers:       innerPipelineWorkers(leftSource),
			spill:         p.getSpillManager(),
		}
		return bridge, append([]exec.UnaryOperator{probe}, lateralEmptyDefaultOps(node)...), &exec.CollectSink{}, nil
	}

	if deferBuild {
		// Pipeline break: run early probes (scan → inner joins) as a child
		// pipeline that overlaps with the deferred build. The bridge collects
		// filtered batches, waits for the build barrier, then replays them
		// through the deferred probe operators.
		probe := hj.Probe()
		p.applyLateMaterialization(probe)
		if f := joinProbeOutputFilter(node); f != nil {
			probe.OutputFilter = f
		}
		probe.OutputExcludeProbe, probe.OutputExcludeBuild = joinHiddenPositions(node)

		bridge := &deferredJoinBridge{
			childSource: leftSource,
			childOps:    leftOps,
			barrier:     buildDone,
			buildErr:    &buildErr,
			workers:     innerPipelineWorkers(leftSource),
			spill:       p.getSpillManager(),
		}

		if joinType == exec.RightJoin || joinType == exec.FullOuterJoin {
			return &joinFlushSource{
				inner:    bridge,
				innerOps: []exec.UnaryOperator{probe},
				probe:    probe,
			}, nil, &exec.CollectSink{}, nil
		}
		return bridge, append([]exec.UnaryOperator{probe}, lateralEmptyDefaultOps(node)...), &exec.CollectSink{}, nil
	}

	// Immediate: wait for build to complete before accessing hash table state.
	<-buildDone
	if buildErr != nil {
		return nil, nil, nil, buildErr
	}

	// Fix key assignment: parseJoinKeys takes columns from the SQL literally
	// (left of "=" → leftKey, right → rightKey), but the SQL may put the
	// build-side column on the left (e.g., "JOIN t ON t.id = probe.id").
	// After building, we know the build schema; swap any misassigned pairs.
	// A repair firing here means plan-time assignJoinKeySides missed a pair.
	if hj.FixKeyAssignment() {
		slog.Warn("join key repair fired at runtime — plan-time side assignment missed a pair",
			"left_keys", hj.LeftKeys, "right_keys", hj.RightKeys)
	}

	// SemiAntiFilter already set above (pre-goroutine).

	// For SEMI/ANTI joins, prune build-side batches to only columns needed by
	// the SemiAntiFilter. The build side never appears in the output, so after
	// the hash index is built, only filter columns need to be retained.
	if joinType == exec.SemiJoin || joinType == exec.AntiJoin {
		hj.PruneBuildColumns(keepCols)
	}

	// Insert bloom filter pre-check before probe for early row elimination.
	// Rows whose join key is definitely not in the build side are filtered
	// out via selection vector before they reach the probe operator.
	if bf := hj.BloomPushdownOp(); bf != nil {
		leftOps = append(leftOps, bf)

		// Push bloom filter deeper into the scan layer for row-group-level
		// pruning. If the probe source is a catalog scan with integer join
		// keys, attach a BloomScanFilter so entire row groups whose key
		// range has no hits in the bloom filter are skipped before I/O.
		if bsf := bf.BloomScanFilter(); bsf != nil {
			attachBloomToScanSource(leftSource, bsf)
		}
	}

	// Push dynamic min/max range filter to the scan layer for row-group pruning.
	// Complements the bloom filter: works for all types (string, date, wide int
	// ranges) and is cheaper to evaluate (single range comparison per row group).
	if ranges := hj.BuildKeyRange(); len(ranges) > 0 {
		attachDynamicFilterToScanSource(leftSource, ranges)
	}
	probe := hj.Probe()
	p.applyLateMaterialization(probe)
	// Push output filter into the probe to avoid materializing intermediate
	// columns not needed by upstream operators. In multi-way joins, this
	// eliminates allocation and gather work for columns that would otherwise
	// be built then immediately dropped.
	if f := joinProbeOutputFilter(node); f != nil {
		probe.OutputFilter = f
	}
	probe.OutputExcludeProbe, probe.OutputExcludeBuild = joinHiddenPositions(node)
	leftOps = append(leftOps, probe)
	// The lateral's empty-input default rides directly above the probe: an
	// outer row the lateral matched nothing for exists only as this join's
	// pad, and the column's own value there is 0, not NULL. Above the join
	// rather than in the enclosing query's references, so a star sees it
	// (exec.LateralEmptyDefault, #977).
	leftOps = append(leftOps, lateralEmptyDefaultOps(node)...)

	// For RIGHT and FULL OUTER joins, unmatched build-side rows must be
	// flushed after all probe batches have been processed. Wrap the source
	// so that FlushUnmatched is called at the end.
	if joinType == exec.RightJoin || joinType == exec.FullOuterJoin {
		return &joinFlushSource{
			inner:    leftSource,
			innerOps: leftOps,
			probe:    probe,
		}, nil, &exec.CollectSink{}, nil
	}

	// For RightSemiJoin/RightAntiJoin, the probe phase marks matched build
	// entries but outputs nothing. After probing, emit matched (semi) or
	// unmatched (anti) build rows.
	if joinType == exec.RightSemiJoin || joinType == exec.RightAntiJoin {
		return &rightSemiFlushSource{
			inner:    leftSource,
			innerOps: leftOps,
			probe:    probe,
			joinType: joinType,
		}, nil, &exec.CollectSink{}, nil
	}

	return leftSource, leftOps, &exec.CollectSink{}, nil
}
