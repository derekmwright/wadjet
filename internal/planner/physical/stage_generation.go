// This file holds stage generation for the physical planner, governed by ADR-0010 and ADR-0026.
package physical

import (
	"fmt"
	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// ExpandFederatedScans checks if scan stages reference tables that exist on
// multiple clusters. If so, it splits the scan into per-cluster scan stages
// and rewrites downstream dependencies. Returns stages unchanged if only one
// cluster has the table (or if federation lookup fails).
func (p *Planner) ExpandFederatedScans(stages []Stage) []Stage {
	clusters, err := p.catalog.ListClusters()
	if err != nil || len(clusters) <= 1 {
		return stages // single cluster or error — no expansion needed
	}

	// Build cluster → table set
	clusterTables := make(map[string]map[string]bool, len(clusters))
	for _, c := range clusters {
		m := make(map[string]bool, len(c.Tables))
		for _, t := range c.Tables {
			m[t] = true
		}
		clusterTables[c.ClusterID] = m
	}

	localID := p.catalog.ClusterID()
	var expanded []Stage
	oldToNew := map[string][]string{} // original stageID → replacement stageIDs

	for _, stage := range stages {
		if stage.Type != "scan" || stage.TableName == "" {
			expanded = append(expanded, stage)
			continue
		}

		// Find all clusters that have this table
		var targetClusters []string
		for _, c := range clusters {
			if clusterTables[c.ClusterID][stage.TableName] {
				targetClusters = append(targetClusters, c.ClusterID)
			}
		}

		if len(targetClusters) <= 1 {
			// Single cluster — tag with local ID, no split
			stage.ClusterID = localID
			expanded = append(expanded, stage)
			continue
		}

		// Split into per-cluster scan stages
		var replacements []string
		for _, cid := range targetClusters {
			newStage := stage // copy value
			newStage.ID = fmt.Sprintf("%s-%s", stage.ID, cid)
			newStage.ClusterID = cid

			if cid != localID {
				// Get scan files from remote manifest
				manifest, err := p.catalog.GetRemoteManifest(cid, stage.TableName)
				if err != nil {
					continue // skip clusters we can't read
				}
				var files []string
				var fileSizes []int64
				for _, part := range manifest.Partitions {
					if len(stage.PartitionFilter) > 0 && len(part.Values) > 0 {
						if !matchesPartitionFilter(part.Values, stage.PartitionFilter) {
							continue
						}
					}
					for _, f := range part.Files {
						files = append(files, f.Path)
						fileSizes = append(fileSizes, f.SizeBytes)
					}
				}
				newStage.ScanFiles = files
				newStage.ScanFileSizes = fileSizes
				// The remote cluster's own delete state — the local one
				// names files this stage will never read.
				newStage.ScanDeletes = deleteMarkerMap(manifest.DeleteMarkers)
				if len(files) > 0 {
					newStage.Tasks = len(files)
				}
			}

			replacements = append(replacements, newStage.ID)
			expanded = append(expanded, newStage)
		}
		oldToNew[stage.ID] = replacements
	}

	if len(oldToNew) == 0 {
		return expanded // nothing was split
	}

	// Rewrite dependencies: any stage depending on a split scan stage
	// now depends on all its replacement stages
	for i := range expanded {
		var newDeps []string
		for _, dep := range expanded[i].Dependencies {
			if replacements, ok := oldToNew[dep]; ok {
				newDeps = append(newDeps, replacements...)
			} else {
				newDeps = append(newDeps, dep)
			}
		}
		expanded[i].Dependencies = newDeps
	}

	return expanded
}

func (p *Planner) generateStages(node *logical.Node) []Stage {
	var stages []Stage
	// Fresh CTE dedup cache per query — walkStages populates it as it
	// encounters CTE-named subtrees and consults it on subsequent walks
	// (typically when emitScalarProducerStages re-walks a CTE-referencing
	// scalar subquery, which would otherwise double-compute the CTE).
	p.ctePlannedTerminal = make(map[string]string)
	p.cteTerminals = make(map[string]bool)
	p.cteRefCounts = countCTEReferences(node)
	p.starReadBlocks = starReadBlockProjections(node)
	p.publishedBlocks = map[*logical.Node]bool{}
	p.scanDeletes = nil
	p.limitStageRoot = node
	p.setOpErr = nil
	p.joinCondErr = nil
	p.correlatedErr = nil
	p.scalarRowsErr = nil
	p.authzErr = nil
	p.inSubqueryErr = nil
	p.aggStageRenames = nil
	p.attachedFilterExprs = nil
	p.attachedProjectionOutputs = nil
	p.aggProjectionRenames = nil
	p.walkStages(node, &stages, nil)
	for i := range stages {
		p.attachedProjectionOutputs = append(p.attachedProjectionOutputs,
			stageProjectionOutputs(&stages[i])...)
	}
	// A project stage filterCarrierIndex reserved for a predicate that then
	// turned out to have no text is a full materialization round-trip for
	// nothing. Drop it before anything else reads the graph.
	stages = pruneEmptyProjectStages(stages)
	// Resolve cte-alias phantoms emitted by walkStages dedup. Must happen
	// before fuseJoinStages so fusion sees real stage IDs everywhere.
	stages = flattenCTEAliases(stages)
	// Broadcast-join fusion absorbs a leaf broadcast_join into its consumer
	// join's FusedJoins list, so a chain of N broadcast joins runs as ONE
	// task that builds N hash tables and pipelines probes batch-by-batch
	// through them — instead of N separate stages with an S3 round-trip
	// between each, single-tasked all the way down because the chain root
	// probe is a small dimension table.
	//
	// Previously gated to only the legacy single-pipeline executor — the
	// native-DAG validator rejected fused shapes because executeStageHashJoin
	// only handled 2 deps and didn't read task.FusedJoins. Both are fixed:
	//   - native_dag_rewrite.go's validator now allows 2+N deps when N
	//     FusedJoins are present.
	//   - executor_stage.go:executeStageHashJoin builds a hash table per
	//     FusedJoin entry and chains their Probe operators in the pipeline.
	//   - dispatchComputeStage translates planner-side FusedJoinSpec
	//     (BuildDepStage) into wire-format FusedJoinSpec (BuildFiles).
	if p.WorkerCount > 1 {
		stages = fuseJoinStages(stages)
	}
	markCoPathingSelfJoinBuilds(stages)
	return stages
}

// markCoPathingSelfJoinBuilds finds joins whose build-side scans target
// the same source table AND whose forward-reachable stage sets intersect
// (one join transitively depends on the other), then sets
// QualifyAllBuildCols on both. This is the planner half of the Q07
// self-join column-disambiguation fix.
//
// The narrower "co-pathing" check is required because counting same-table
// scans across the WHOLE plan also catches scalar-subquery scans (Q02, Q17)
// and CTE-producer scans (Q15) that are independent of the outer join chain
// — qualifying their unrelated joins broke those queries in the first
// attempt.
func markCoPathingSelfJoinBuilds(stages []Stage) {
	stageByID := make(map[string]*Stage, len(stages))
	for i := range stages {
		stageByID[stages[i].ID] = &stages[i]
	}

	// For each join stage, walk its build-side dep chain (transitively
	// through any Exchange wrappers) to the underlying scan and record
	// (joinIdx, scanTableName).
	//
	// The walk deliberately does NOT branch into both sides of a join stage
	// encountered mid-chain (bushy builds). An attempted extension that did
	// (2026-07-09) surfaced same-table scans from Q02/Q20's decorrelated
	// subquery chains and force-qualified joins whose downstream consumers
	// reference bare names — 0 rows. Bushy self-join collisions are instead
	// qualified at the join where the collision occurs, via isDup +
	// BuildColOrigins in joinOutputSchemaWithMapping. If a bushy shape ever
	// needs Q07-style cross-chain force-qualification, decide it from the
	// LOGICAL tree's exact output visibility (subtreeNaming), not from a
	// stage-DAG walk.
	type joinScan struct {
		joinIdx   int
		joinID    string
		tableName string
	}
	var joinScans []joinScan
	for i := range stages {
		s := &stages[i]
		if s.Type != StageHashJoin && s.Type != StageBroadcastJoin && s.Type != StageSortMergeJoin {
			continue
		}
		buildDep := s.RightDepStage
		if buildDep == "" {
			continue
		}
		// Walk through Exchange wrappers (one or two levels) to the scan.
		//
		// A stage that does not publish its INPUT's columns ends the walk,
		// and the join it builds is not a repeated scan of that table (#981).
		// An aggregate's output is its group keys and its aggregates; a stage
		// carrying a materialized block projection publishes the block's own
		// names. Two laterals over ONE table are exactly that shape — `(SELECT
		// MAX(amount) AS mx FROM lat_item …) s` beside `(SELECT MIN(amount) AS
		// mn FROM lat_item …) s2` — and reading the table through them marked
		// both joins as co-pathing self-joins, so every build column was
		// force-qualified and the client was handed `s.mx`, `s2.mn` where
		// PostgreSQL and the single-process path publish `mx`, `mn`. Names
		// that really DO collide are still qualified, by the `isDup` +
		// BuildColOrigins rule in joinOutputSchemaWithMapping, which is the
		// rule for a collision the plan cannot see coming.
		cur := stageByID[buildDep]
		for hop := 0; cur != nil && cur.Type != StageScan && hop < 3; hop++ {
			if len(cur.Dependencies) == 0 || stageCollapsesItsInput(cur) ||
				len(cur.ProjectExprs) > 0 {
				cur = nil
				break
			}
			cur = stageByID[cur.Dependencies[0]]
		}
		if cur == nil || cur.Type != StageScan || cur.TableName == "" {
			continue
		}
		joinScans = append(joinScans, joinScan{joinIdx: i, joinID: s.ID, tableName: cur.TableName})
	}

	if len(joinScans) < 2 {
		return
	}

	// reachable[X] = set of stage IDs reachable forward from X (X's transitive
	// consumers). Computed by running BFS from each join via reverse-deps.
	consumers := make(map[string][]string, len(stages))
	for i := range stages {
		for _, dep := range stages[i].Dependencies {
			consumers[dep] = append(consumers[dep], stages[i].ID)
		}
	}
	reachable := make(map[string]map[string]bool, len(joinScans))
	for _, js := range joinScans {
		reach := map[string]bool{js.joinID: true}
		stack := []string{js.joinID}
		for len(stack) > 0 {
			n := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			for _, c := range consumers[n] {
				if !reach[c] {
					reach[c] = true
					stack = append(stack, c)
				}
			}
		}
		reachable[js.joinID] = reach
	}

	// Mark a join when another join over the SAME table is in its forward
	// reachable set OR vice versa — i.e. the two are in the same join
	// chain.
	//
	// NOTE (bushy, 2026-07-09): generalizing this to "reachable sets
	// intersect" (parallel branches meeting downstream) was tried and
	// REVERTED — it force-qualified Q02's outer/scalar-subquery partsupp
	// pair (parallel branches meeting at the scalar join) whose consumers
	// reference bare names, breaking flag-OFF Q02. Bushy self-join
	// disambiguation is handled where the copies actually collide, via
	// isDup + BuildColOrigins in the join executor.
	for i := range joinScans {
		for j := range joinScans {
			if i == j {
				continue
			}
			a, b := joinScans[i], joinScans[j]
			if a.tableName != b.tableName {
				continue
			}
			if reachable[a.joinID][b.joinID] || reachable[b.joinID][a.joinID] {
				stages[a.joinIdx].QualifyAllBuildCols = true
				stages[b.joinIdx].QualifyAllBuildCols = true
			}
		}
	}
}
