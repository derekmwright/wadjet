// This file holds stage emission for the physical planner, governed by ADR-0010 and ADR-0026.
package physical

import (
	"context"
	"fmt"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"log/slog"
	"strings"
	"time"
)

func (p *Planner) walkStages(node *logical.Node, stages *[]Stage, parentID *string) {
	// CTE deduplication: when this subtree's root is a CTE reference and
	// a structurally-identical clone has already been planned, link the
	// parent's deps to the cached terminal stage and skip re-walking.
	// Eliminates the dual-chain float drift that fails Q15 under multi-
	// file scans: the JOIN's right side and the MAX-subquery producer
	// chain previously emitted independent stages computing the same
	// CTE, producing 1-ULP drift between their float SUMs.
	//
	// The structural hash guards correctness — a CTE clone with different
	// pushed-down filters or column projections has a different hash and
	// is NOT deduped, falling back to the historical compute-twice path.
	if node.CTEName != "" {
		hash := cteSubtreeHash(node)
		cacheKey := node.CTEName + "|" + hash
		if termID, ok := p.ctePlannedTerminal[cacheKey]; ok {
			// Emit a phantom "cte-alias" stage that points at the cached
			// terminal. Parent walkStages cases compute their dependencies
			// via leafStages over [preCount:], which naturally picks up
			// this alias as a leaf — so the parent's deps reference the
			// alias's ID. A post-pass (flattenCTEAliases) rewrites every
			// dep that points to an alias into the alias's target and
			// drops the alias stages, leaving the parent reading directly
			// from the cached CTE terminal. Surgical: avoids modifying
			// every parent case to consult a Planner-level "deduped child"
			// list; the bookkeeping lives entirely in the alias stage and
			// the post-pass.
			// SHARING IS A FACT AT THIS MOMENT, whatever the reference
			// count over the outer plan said. p.cteRefCounts is computed
			// from the statement's OWN logical plan, and a CTE referenced
			// from a scalar subquery's TEXT appears in it ZERO times — each
			// producer is planned by its own emitScalarProducerStagesTyped
			// walk over the SHARED cache. So the flag is set here, where a
			// second reference has just been pointed at the stage, rather
			// than only where the first one was recorded (#876).
			p.cteTerminals[termID] = true
			// A CTE BODY IS PLANNED ONCE, so this reference's block was
			// published (or not) by the walk that planned it — this clone's
			// Project nodes are different pointers and never reach the publish
			// hook below. Carrying the verdict here is what keeps the refusal
			// honest: without it a twice-referenced CTE whose projection WAS
			// materialized still looked unpublished, and the query was routed
			// off the DAG onto a pipeline that is not answer-preserving
			// (round-1 B1). The cached terminal's own ProjectExprs is the
			// observable fact, not a guess.
			if idx, ok := p.stageIndexByID(*stages, termID); ok &&
				len((*stages)[idx].ProjectExprs) > 0 {
				markStarReadBlocks(node, p.starReadBlocks, p.publishedBlocks)
			}
			aliasID := fmt.Sprintf("cte-alias-%d", len(*stages))
			*stages = append(*stages, Stage{
				ID:           aliasID,
				Type:         stageTypeCTEAlias,
				Dependencies: []string{termID},
			})
			if parentID != nil {
				for i := range *stages {
					if (*stages)[i].ID == *parentID {
						(*stages)[i].Dependencies = append((*stages)[i].Dependencies, aliasID)
						break
					}
				}
			}
			return
		}
		// Defer recording: after walkStages returns, the last stage
		// in *stages is the terminal of this CTE's subtree (walkStages
		// emits children first, then the node's own stage last for
		// every node type except Filter — and Filter at a CTE root is
		// degenerate because Filter doesn't emit its own stage).
		before := len(*stages)
		defer func() {
			if len(*stages) > before {
				termID := (*stages)[len(*stages)-1].ID
				p.ctePlannedTerminal[cacheKey] = termID
				// Referenced more than once: every reference reads this
				// stage's output, so nothing consumer-specific may be
				// attached to it (#656 follow-up).
				p.cteTerminals[termID] = p.cteRefCounts[strings.ToLower(node.CTEName)] > 1
			}
		}()
	}

	switch node.Type {
	case logical.NodeScan:
		stageID := fmt.Sprintf("scan-%d", len(*stages))
		tasks := 1
		var scanFiles []string
		var scanFileSizes []int64
		var estBytes, estRows int64
		partFilter := node.PartitionFilter
		var scanDeletes map[string][]int64
		if meta, err := p.getManifest(context.Background(), node.TableName); err == nil {
			for _, part := range meta.Partitions {
				if len(partFilter) > 0 && len(part.Values) > 0 {
					if !matchesPartitionFilter(part.Values, partFilter) {
						continue
					}
				}
				for _, f := range part.Files {
					scanFiles = append(scanFiles, f.Path)
					scanFileSizes = append(scanFileSizes, f.SizeBytes)
					estBytes += f.SizeBytes
					estRows += f.NumRows
				}
			}
			if len(scanFiles) > 0 {
				tasks = len(scanFiles)
			}
			// Merge-on-read deletes, from THIS manifest object — the same
			// snapshot the file list came from (see Stage.ScanDeletes).
			scanDeletes = deleteMarkerMap(meta.DeleteMarkers)
			p.rememberScanDeletes(node.TableName, scanDeletes)
		}
		// Build unique ScanAlias: "table" for first scan, "table:1", "table:2"
		// for duplicates. This disambiguates multiple scans of the same table
		// (e.g., self-joins) in scan-split pipeline mode.
		scanAlias := node.TableName
		dupCount := 0
		for _, s := range *stages {
			if s.Type == "scan" && s.TableName == node.TableName {
				dupCount++
			}
		}
		if dupCount > 0 {
			scanAlias = fmt.Sprintf("%s:%d", node.TableName, dupCount)
		}

		stage := Stage{
			ID:              stageID,
			Type:            "scan",
			Tasks:           tasks,
			TableName:       node.TableName,
			ScanAlias:       scanAlias,
			Columns:         node.RequiredColumns,
			PartitionFilter: partFilter,
			ScanFiles:       scanFiles,
			ScanFileSizes:   scanFileSizes,
			ScanDeletes:     scanDeletes,
			EstimatedBytes:  estBytes,
			EstimatedRows:   estRows,
		}
		*stages = append(*stages, stage)
		if parentID != nil {
			for i := range *stages {
				if (*stages)[i].ID == *parentID {
					(*stages)[i].Dependencies = append((*stages)[i].Dependencies, stageID)
				}
			}
		}

	case logical.NodeAggregate:
		preCount := len(*stages)
		for _, child := range node.Children {
			p.walkStages(child, stages, nil)
		}
		// A Project below an aggregate emits no stage (walkStages treats it
		// as a passthrough), so its SELECT-list renames never happen on the
		// DAG. Every name this aggregate reads is therefore resolved back to
		// what the stage below it emits, or the aggregate asks for a column
		// the batch does not have and HashAggregate answers NULL (#355).
		var aggChild *logical.Node
		if len(node.Children) > 0 {
			aggChild = node.Children[0]
		}
		var aggSpecs []AggSpec
		hasDistinctAgg := false
		for _, agg := range node.AggExprs {
			// Resolved before the spec is built so aggSpecOutputType and the
			// derived-expression branch below both see the real column: the
			// type lookup misses on an alias too, and an undeclared type
			// makes MAX come back float64 where the column is INT64.
			// A DELIMITED argument arrives with its quotes: the parser records
			// `MAX("g + 1")` as the six characters plus two quote bytes, because
			// `ColRef.String()` re-delimits anything that needs it. The
			// single-process path strips them (`NormalizeIdentRef`, below at the
			// aggCols loop) and the DAG carried the spelling verbatim — so the
			// alias lookup missed, the argument was never re-spelled to its
			// source column, and the worker's projection asked for a column
			// literally named `"g + 1"`: `column "\"g + 1\"" does not exist in
			// the input schema`, three attempts, a hard query failure for a
			// query PostgreSQL answers (#736).
			//
			// A delimited identifier's quotes are not part of its NAME (#725),
			// and the same rule applies to an aggregate's argument as to a GROUP
			// BY key. The table qualifier is preserved for the reason the
			// single-process loop gives: `cleanExpr` would drop it and a bare
			// name binds to the FIRST column of that name (#622).
			agg.InputCol = plansql.NormalizeIdentRef(strings.TrimSpace(agg.InputCol))
			agg.InputCol2 = plansql.NormalizeIdentRef(strings.TrimSpace(agg.InputCol2))
			agg.InputCol3 = plansql.NormalizeIdentRef(strings.TrimSpace(agg.InputCol3))
			inputExpr := agg.InputExpr
			exprCols := aggChild
			if resolved, expr, exprInput, renamed := resolveAggInputName(agg.InputCol, aggChild); renamed {
				if expr != nil {
					// The alias named an EXPRESSION, not a column.
					//
					// Where nothing between here and the scan MATERIALIZES it,
					// there is nothing to read: hand the worker the expression
					// and let it project the value under the alias before
					// aggregating — the same route a derived aggregate argument
					// (`SUM(a * (1 - b))`) already takes, and it names its
					// projected column InputCol, so the alias has to stay. Its
					// column references are written against the Project's
					// INPUT, which is where the type has to be resolved.
					//
					// Where a producer DOES materialize it, computing it again
					// is the defect: `SUM(v)` over
					// `(SELECT DISTINCT a * 2 AS v FROM t)` shipped
					// InputCol="v" with InputExpr="a * 2", and the worker
					// recomputed `a * 2` over a distinct stage's output — which
					// emits the group key under that TEXT and carries no `a` at
					// all. Every row read NULL and the SUM came back NULL,
					// silently, where PostgreSQL answers 29.48. The producer's
					// emitted name IS the expression's text, so the argument is
					// a bare NAME there, exactly as aggStageGroupKey spells a
					// computed GROUP BY key.
					emitted, _ := aggInputAliasIsAggregateGroupKey(aggChild, expr.String())
					switch {
					case aggInputRespellable(aggChild):
						inputExpr, exprCols = expr, exprInput
					case emitted != "":
						// The aggregate below emits this expression as a GROUP
						// BY key, under the key's own TEXT (which is what the
						// DISTINCT rewrite produces), so the argument is a bare
						// NAME spelled that way — the same convention
						// aggStageGroupKey applies to a computed group key.
						agg.InputCol = emitted
						inputExpr = &plansql.ColRef{Column: emitted}
					case aggInputAliasIsMaterializedUnderItsName(aggChild):
						// A JOIN, a sort, a LIMIT, a window: the alias-naming
						// OpProject on the producing fragment materializes it
						// under the ALIAS, so the alias is what to read.
						inputExpr = &plansql.ColRef{Column: agg.InputCol}
					default:
						// Anything else — arithmetic over an aggregate's
						// OUTPUT is the shape that lands here — keeps the
						// pre-#702 behaviour: hand the worker the expression
						// and let its pre-projection compute the value. The
						// references inside it are written against the
						// Project's INPUT, which is where the type resolves.
						inputExpr, exprCols = expr, exprInput
					}
				} else {
					agg.InputCol = resolved
					if _, bare := inputExpr.(*plansql.ColRef); bare || inputExpr == nil {
						inputExpr = &plansql.ColRef{Column: resolved}
					}
				}
			}
			if resolved, expr, _, renamed := resolveAggInputName(agg.InputCol2, aggChild); renamed && expr == nil {
				agg.InputCol2 = resolved
			}
			if resolved, expr, _, renamed := resolveAggInputName(agg.InputCol3, aggChild); renamed && expr == nil {
				agg.InputCol3 = resolved
			}
			outType, outTypeKnown := aggSpecOutputType(node, agg)
			spec := AggSpec{
				Func:            agg.Func,
				InputCol:        agg.InputCol,
				OutputCol:       agg.OutputCol,
				OutputType:      outType,
				OutputTypeKnown: outTypeKnown,
				InputCol2:       agg.InputCol2,
				InputCol3:       agg.InputCol3,
				Separator:       agg.Separator,
				Percentile:      agg.Percentile,
			}
			if fields, ok := aggOhlcvOutputFields(node, agg); ok {
				spec.OutputFields = fields
			}
			// The (p,s) that goes with a DECIMAL OutputType. See the field's
			// comment: without it a partial task whose filter matched nothing
			// writes a .wshf header declaring DECIMAL(0,0) (#685).
			if m, known := aggSpecOutputDecimal(node, agg); known {
				spec.OutputPrecision, spec.OutputScale = m.Precision, m.Scale
			}
			// And the INPUT's (p,s) for a bare DECIMAL column argument. The
			// derived-expression branch below overwrites both when there is an
			// expression to type instead. decomposeAvg needs this one: AVG
			// travels as a (SUM, COUNT) pair, and the SUM leg's declaration is
			// the INPUT's scale, which AVG's own declared scale cannot be
			// inverted back to once the +4 increment saturates at the carrier's
			// 38 digits (#685).
			if m, known := aggSpecInputDecimal(node, agg); known {
				spec.InputPrecision, spec.InputScale = m.Precision, m.Scale
			}
			agg.InputExpr = inputExpr
			// DISTINCT rides the canonical Func string the worker already
			// maps to exec.AggCountDistinct (#291: the flag used to be
			// dropped here, so distributed COUNT(DISTINCT x) degenerated
			// to COUNT(x) on every path).
			if agg.Distinct && strings.EqualFold(agg.Func, "count") {
				spec.Func = "count_distinct"
			} else if agg.Distinct {
				// Every OTHER aggregate carries the flag itself (#703). The
				// worker maps it onto exec.AggColumn.Distinct; before this it
				// was dropped here and a distributed SUM(DISTINCT x) was a
				// plain SUM, silently.
				spec.Distinct = true
			}
			if agg.Distinct {
				hasDistinctAgg = true
			}
			// Capture derived expression text when the aggregate argument
			// is not a bare column reference (e.g.
			// SUM(l_extendedprice * (1 - l_discount))). Downstream
			// native-DAG workers need this to project the derived column
			// before running HashAggregate.
			if agg.InputExpr != nil {
				// A ROW FIELD PATH is a bare reference in shape only: the
				// worker's HashAggregate resolves inputs by NAME through
				// columnIndexFallback, which has no ROW arm, so it has to be
				// pre-projected exactly like a computed argument (#568).
				_, bare := agg.InputExpr.(*plansql.ColRef)
				if !bare || astIsFieldPath(agg.InputExpr, inputColDecls(exprCols)) {
					// Every reference INSIDE the expression gets the same
					// resolution resolveAggInputName gave the argument as a
					// whole. Without it the worker compiles the text against a
					// batch carrying the SCAN's columns and a derived name
					// reads NULL on every row — TPC-H Q08's shape, silently 0
					// (#702). DAG-only: this rewrites the spec's TEXT, not the
					// logical node the single-process pipeline runs, where the
					// Project below is a real operator and the alias is real.
					stageExpr := agg.InputExpr
					if respelled, ok := respellAggInputExpr(stageExpr, exprCols); ok {
						stageExpr = respelled
					}
					// And the one resolution that rewrite declines by
					// construction: a reference to a derived table's or CTE's
					// alias for a WINDOW OUTPUT SLOT. respellAggInputExpr
					// respells only where the walk reaches a SCAN through
					// Project and Filter alone, and a Window stops it — so
					// `SUM(w * 2)` over `SELECT SUM(a) OVER () AS w` shipped
					// the text `w * 2`, which the window stage's stream cannot
					// resolve (it publishes `__win_0`), and every row read NULL
					// (#877; #878 is the same through a CTE joined to itself).
					// The slot family is RESERVED, so a name that resolves into
					// it is the planner's own and nothing else.
					if respelled, ok := respellWindowSlotAliasRefs(stageExpr, exprCols); ok {
						stageExpr = respelled
					}
					spec.InputExpr = stageExpr.String()
					// And the type that expression evaluates into, since
					// the worker builds the pre-aggregate projection from
					// the text alone and has no catalog to consult.
					// emittedColDecls, not inputColDecls, and for the reason
					// the single-process pre-projection uses it too: the walk
					// has to cross a DERIVED TABLE. TPC-H Q08's CASE branch is
					// a bare reference to a column a subquery computes, and
					// inputColTypes stops at that subquery's Project — so the
					// expression declared FLOAT64, the worker built a float
					// vector from the text alone, and the branch's DECIMAL box
					// was DROPPED: the DAG answered 0 where the single-process
					// path refused the store outright. Two paths, one walk.
					spec.InputType, spec.InputPrecision, spec.InputScale = declTypeParts(
						inferProjectionDeclType(agg.InputExpr, parquet.TypeFloat64, nil, emittedColDecls(exprCols)))
					// And the OUTPUT declaration from that same triple. The
					// worker materializes this projection from it, so the
					// vector every partial that sees a row observes IS this
					// declaration — reading the output off it is what makes the
					// identity row of a partial whose filter matched nothing
					// declare what its siblings will write, instead of
					// aggSpecOutputType's float64 default for anything that is
					// not a bare column (#685: SUM(a * (1 - b)) and its whole
					// class). See aggOutputFromInputDecl.
					//
					// It reads the triple the line above produced, so the two
					// changes compose: #695 made that triple resolve through a
					// DERIVED TABLE, which is where TPC-H Q08's `volume` lives,
					// and #685 turns it into the output every partial declares.
					// Before #695 that triple was FLOAT64 for every derived
					// aggregate input, so #685's output declaration inherited
					// the same wrong carrier.
					if t, p, sc, known := aggOutputFromInputDecl(
						agg.Func, agg.Distinct, spec.InputType, spec.InputPrecision, spec.InputScale,
						aggInputIsWideInteger(agg.InputExpr, emittedColDecls(exprCols))); known {
						spec.OutputType, spec.OutputTypeKnown = t, true
						spec.OutputPrecision, spec.OutputScale = p, sc
					}
				}
			}
			// The candidate spellings of every reference in the shipped
			// argument that names a derived table's alias. Recorded from the
			// text the spec really carries — a reference the passes above
			// already re-spelled to its source is no longer an alias and
			// records nothing — and settled at the end of planning, where the
			// producing fragment's real output is known (#770).
			spec.InputRefs = aggInputAliasCandidates(spec, aggChild)
			aggSpecs = append(aggSpecs, spec)
		}
		// The key's TWO names: what the aggregate publishes it as, and what
		// the fragment computing it resolves it by. Before they were two, an
		// unresolvable key serialized as NULL rather than failing, so `GROUP
		// BY k` over `SELECT o_orderstatus AS k` collapsed 3 groups into one
		// NULL group of every row — and the key an aggregate BELOW already
		// published was recomputed against a schema without its leaves, which
		// is the same collapse one shape over (ADR-0026 §2, #736, #794).
		groupBy, groupByResolve := stageGroupKeyNames(node, aggChild)
		// The gather's output renames read the LOGICAL name and need the name
		// the stage's fragment actually EMITS for it. That used to be the
		// dispatch re-spelling, because the dispatch spelling was also the
		// published one; now it is exec's own output rule over the published
		// list, which is what the single-process aggregate emits for the same
		// query (#355, #467, ADR-0026 §2b).
		emitted := stageEmittedKeyNames(groupBy, groupByResolve)
		haveGBExprs := len(node.GroupByExprs) == len(node.GroupBy)
		for i, key := range node.GroupBy {
			var keyExpr plansql.Node
			if haveGBExprs {
				keyExpr = node.GroupByExprs[i]
			}
			if _, renamed := aggStageDispatchKey(key, keyExpr, aggChild); !renamed {
				continue
			}
			if p.aggStageRenames == nil {
				p.aggStageRenames = make(map[string]string)
			}
			p.aggStageRenames[strings.ToLower(key)] = emitted[i]
			// The gather's rename reads this map by the name the outer
			// SELECT list uses, which for a key written through the
			// derived table's alias (`GROUP BY u.k`) is the BARE one
			// (`SELECT k`). Record both spellings or the lookup misses
			// and the result comes back at full upstream width (#467).
			if bare := derivedScopeBareName(key, aggChild); bare != "" {
				if _, taken := p.aggStageRenames[strings.ToLower(bare)]; !taken {
					p.aggStageRenames[strings.ToLower(bare)] = emitted[i]
				}
			}
		}
		// Plan-time types for the derived keys, computed here where the
		// aggregate's input schema is still known (#379); every stage
		// shape below carries the same map. Keyed by the PUBLISHED name,
		// which is index-aligned with the resolution list and survives
		// resolveStageGroupKeys re-spelling one.
		groupByTypes, groupByDecimal := stageGroupKeyDecls(groupBy, groupByResolve, aggChild)

		// Optimization: fuse aggregation into scan when the only child
		// stages are scans (no joins or sorts in between). This eliminates
		// the scan→aggregate S3 round-trip by doing partial aggregation at
		// the scan level. Each scan task produces partial aggregate results
		// instead of raw rows, massively reducing data volume.
		childStages := (*stages)[preCount:]
		// Distinct aggregates cannot ride the two-phase partial/merge
		// shape: a per-task partial COUNT(DISTINCT) merged by the final's
		// COUNT→SUM rewrite double-counts values that appear in more than
		// one task (#291 — observed as COUNT(DISTINCT)=COUNT(*)). They
		// dispatch instead as one RawInputAggregate final over raw rows:
		// exact by construction (grouped finals declare clustering on the
		// group keys, so the distribution pass hash-partitions raw input
		// into disjoint groups; ungrouped finals collapse to Singleton).
		//
		// MEDIAN, PERCENTILE_*, MODE, MIN_BY/MAX_BY and STRING_AGG take the
		// same route for the same reason and at the same cost — none of
		// them is a valid input to itself, and none has a bounded summary
		// that merges (agg_whole_input.go, #353).
		if hasDistinctAgg || anyAggNeedsWholeInput(aggSpecs) {
			finalStageID := fmt.Sprintf("final_aggregate-%d", len(*stages))
			*stages = append(*stages, Stage{
				ID:          finalStageID,
				Type:        "final_aggregate",
				Tasks:       1,
				GroupByCols: groupBy,
				// A RawInputAggregate final reads RAW rows, not a partial's
				// output, so it is the one final that COMPUTES its keys and
				// therefore the one that carries the resolution list.
				GroupByResolve:    groupByResolve,
				GroupByTypes:      groupByTypes,
				GroupByDecimal:    groupByDecimal,
				AggSpecs:          aggSpecs,
				RawInputAggregate: true,
				Dependencies:      leafStages(childStages),
			})
		} else if canFuseScanAggregate(childStages) && !p.fusesIntoACTETerminal(childStages) {
			for i := range *stages {
				if i < preCount {
					continue
				}
				if (*stages)[i].Type == "scan" {
					(*stages)[i].FusedAggGroupBy = groupBy
					(*stages)[i].GroupByResolve = groupByResolve
					(*stages)[i].GroupByTypes = groupByTypes
					(*stages)[i].GroupByDecimal = groupByDecimal
					(*stages)[i].FusedAggSpecs = aggSpecs
					// The scan's RequiredColumns carry the aggregate OUTPUT
					// names (e.g. __having_0) because ancestors reference
					// them. On a fused scan-aggregate those are produced by
					// the fragment's HashAggregate, not read from parquet —
					// but the worker's all-or-nothing projection guard can't
					// know that: one unknown name silently reverts the whole
					// scan to full width (Q18's fused lineitem leg measured
					// 143 B/row vs the ~25 B/row its 2-column read set
					// needs). Strip pure outputs from the read set; an
					// output that aliases a real input (SUM(x) AS x) stays.
					// Both names: the read set must keep every column the
					// fragment READS, and after the two names separated that
					// is the RESOLUTION spelling — the published name is what
					// the aggregate emits, which is exactly what may be
					// pruned.
					(*stages)[i].Columns = pruneFusedAggOutputCols(
						(*stages)[i].Columns, append(append([]string(nil), groupBy...),
							resolveExprs(groupByResolve)...), aggSpecs, (*stages)[i].FilterExprs)
				}
			}
			// Skip the separate aggregate stage — scans produce partial aggs.
			// Final aggregate merges partial results from all scan tasks.
			leafIDs := leafStages(childStages)
			emitMergeAggregateTree(stages, leafIDs, groupBy, groupByTypes, groupByDecimal, aggSpecs, childStages)
		} else {
			// Standard two-phase distributed aggregation
			stageID := fmt.Sprintf("aggregate-%d", len(*stages))
			stage := Stage{
				ID:          stageID,
				Type:        "aggregate",
				Tasks:       1,
				GroupByCols: groupBy,
				// The PARTIAL computes the keys from raw upstream rows, so it
				// is the stage that carries the resolution list. The final
				// below reads this stage's OUTPUT, where every key is already
				// a column under its published name (#794).
				GroupByResolve: groupByResolve,
				GroupByTypes:   groupByTypes,
				GroupByDecimal: groupByDecimal,
				AggSpecs:       aggSpecs,
			}
			stage.Dependencies = leafStages(childStages)
			*stages = append(*stages, stage)

			finalStageID := fmt.Sprintf("final_aggregate-%d", len(*stages))
			*stages = append(*stages, Stage{
				ID:             finalStageID,
				Type:           "final_aggregate",
				Tasks:          1,
				GroupByCols:    groupBy,
				GroupByTypes:   groupByTypes,
				GroupByDecimal: groupByDecimal,
				AggSpecs:       aggSpecs,
				Dependencies:   []string{stageID},
			})
		}

	case logical.NodeSort:
		preCount := len(*stages)
		for _, child := range node.Children {
			p.walkStages(child, stages, nil)
		}
		// Same materialization for a sort over a computed subquery column
		// (#383): the sort keys on the alias, which otherwise names no
		// column anywhere on the DAG and the ORDER BY is silently lost.
		if len(node.Children) == 1 {
			absorbComputedSubqueryProjection(node.Children[0], (*stages)[preCount:], true)
		}
		sortStageID := fmt.Sprintf("sort-%d", len(*stages))
		var sortKeys []SortKeySpec
		var sortChild *logical.Node
		if len(node.Children) == 1 {
			sortChild = node.Children[0]
		}
		for _, ob := range node.OrderBy {
			key := SortKeySpec{
				Column:               resolveSortKeyColumn(ob.Column, sortChild),
				Desc:                 ob.Desc,
				NullsLast:            resolveNullsLast(ob),
				SlotPos:              sortKeySlotPosStage(ob, node, (*stages)[preCount:]),
				WrittenTerm:          strings.TrimSpace(ob.Column),
				NamesAggregateOutput: sortTermNamesAggregateItem(ob.Column, sortChild),
			}
			// A projection absorbed onto the producer while this subtree was
			// walked (absorbAggregateOutputProjection) MAKES the alias real
			// and narrows the stream to it, so the name the resolver chased
			// the key to — a group key's expression text — no longer reaches
			// this sort. Key on the alias the producer emits.
			if !strings.EqualFold(key.Column, ob.Column) &&
				!producerEmitsName((*stages)[preCount:], key.Column) &&
				producerMaterializesName((*stages)[preCount:], ob.Column) {
				key.Column = cleanExpr(ob.Column)
			}
			// A key still spelled __sortkey_N names a column the logical
			// Project materializes and no stage does. Record what defines
			// it; resolveHiddenSortKeys settles it at the end of planning,
			// once it can see whether some other pass already put the name
			// on the producing stage (#424).
			annotateHiddenSortSource(&key, sortChild)
			// And the non-synthetic sibling: a key naming a DERIVED table's
			// SELECT-list alias, which resolveSortKeyColumn above leaves
			// alone over a scan/join producer because it cannot yet know
			// whether attachScanSelectProjections will materialize the name
			// (#467, #468). Record the source column;
			// resolveDerivedAliasSortKeys decides once that is settled.
			annotateDerivedAliasSortKey(&key, sortChild)
			sortKeys = append(sortKeys, key)
		}

		// Phase 1: partial sort (coordinator splits into parallel tasks at runtime)
		sortStage := Stage{
			ID:       sortStageID,
			Type:     "sort",
			Tasks:    1,
			SortKeys: sortKeys,
		}
		// Only depend on leaf stages from subtree (not transitive deps like scan).
		sortStage.Dependencies = leafStages((*stages)[preCount:])
		*stages = append(*stages, sortStage)

		// Phase 2: merge sort — multi-level tree when many partial sort tasks.
		emitMergeSortTree(stages, sortStageID, sortKeys, (*stages)[preCount:])

	case logical.NodeLimit:
		// Pass limit info down to sort stage if child is sort
		preLimitCount := len(*stages)
		for _, child := range node.Children {
			p.walkStages(child, stages, parentID)
		}
		// The bound a stage may truncate to is limit+offset, not limit: the
		// OFFSET is applied once, at the coordinator, after the merge, so the
		// rows it skips have to survive the stage that produced them. A
		// stage told to keep 3 rows for `LIMIT 3 OFFSET 5` kept the first
		// three and the answer was the first page again (#337). No bound
		// when there is no LIMIT — an OFFSET alone bounds nothing. NoLimit
		// (-1), not 0: `node.LimitVal` is itself 0 for a real `LIMIT 0`, and
		// treating that the same as "no LIMIT" silently dropped the bound
		// end-to-end for `ORDER BY ... LIMIT 0` on the DAG path (#481).
		stageBound := logical.NoLimit
		hasStageBound := false
		if node.LimitVal != logical.NoLimit {
			stageBound = node.LimitVal + node.OffsetVal
			hasStageBound = true
		}
		// Propagate limit to both merge_sort and sort stages — but only to a
		// sort NO LOWER LIMIT ALREADY OWNS.
		//
		// The scan is backwards over the whole stage list, so for a nested
		// LIMIT it reaches the INNER one's sort: `(SELECT n FROM nation
		// ORDER BY n LIMIT 3) i LIMIT 5` wrote 5 over the inner's 3 and then
		// suppressed its own stage on the strength of the sort it had just
		// mis-claimed, answering 5 where PostgreSQL answers 3 (#525).
		// Restricting the range to (*stages)[preLimitCount:] does not help —
		// the inner sort is inside this LIMIT's own child walk.
		//
		// Two things say a sort is spoken for. A sort/merge_sort that already
		// carries HasLimit was bounded by a LIMIT below (nothing else writes
		// it during walkStages), and a StageLimit anywhere between here and
		// the sort means a lower LIMIT is applied above that sort — this
		// bound has to compose ON TOP of it, not underneath. Either way the
		// scan stops and reports sorted=false, so needsLimitStage gives this
		// LIMIT a stage of its own, which is the correct composition and the
		// one the all-bare nesting already produced.
		sorted := false
		for i := len(*stages) - 1; i >= 0; i-- {
			st := &(*stages)[i]
			if st.Type == StageLimit {
				break
			}
			if st.Type != "merge_sort" && st.Type != "sort" {
				continue
			}
			if st.HasLimit {
				break
			}
			if hasStageBound {
				st.Limit = stageBound
				st.HasLimit = true
			}
			// else: leave both fields at their zero value — an unbounded
			// stage is (0,false), and the shared-subplan fingerprint hashes
			// Limit unconditionally, so writing NoLimit here would split an
			// OFFSET-only sort from its no-LIMIT twin.
			sorted = true
			if st.Type == "sort" {
				break
			}
		}
		// No sort to carry it: a bare LIMIT. The coordinator bounds the
		// gathered result either way (correctness), but without a per-task
		// bound every task still reads its whole input first — DataGrip
		// opening a 15M-row table read all of it for the 501 rows it wanted.
		// Push the bound into this subtree's own stages so each task stops
		// early. Only when nothing between here and the scan can change
		// cardinality; see limitPushdownSafe.
		if !sorted && stageBound > 0 && limitPushdownSafe(node) {
			for i := preLimitCount; i < len(*stages); i++ {
				// "scan" is what walkStages emits for a leaf read; "pipeline"
				// is the type those stages carry once fragments are built.
				// Exchange stages are left alone — they move rows between
				// stages rather than producing them.
				if t := (*stages)[i].Type; t == "scan" || t == "pipeline" {
					(*stages)[i].RowLimit = stageBound
				}
			}
		}
		// …and the bound itself, for every LIMIT the two existing appliers
		// cannot reach. See needsLimitStage: the coordinator's post-gather
		// pass reads the ROOT node only, and the sort top-N above needs an
		// ORDER BY below the LIMIT and cannot skip an OFFSET. A LIMIT that
		// is neither reached NOTHING (#478) — RowLimit above is a per-task
		// truncation, and k tasks each keeping n rows is not the first n
		// rows of their union.
		if p.needsLimitStage(node, sorted) {
			limitStage := Stage{
				ID:           fmt.Sprintf("limit-%d", len(*stages)),
				Type:         StageLimit,
				Tasks:        1,
				Offset:       node.OffsetVal,
				Dependencies: leafStages((*stages)[preLimitCount:]),
			}
			if node.LimitVal != logical.NoLimit {
				limitStage.Limit = node.LimitVal
				limitStage.HasLimit = true
			}
			*stages = append(*stages, limitStage)
		}

	case logical.NodeJoin:
		// Track leaf stages from each child separately so we get the
		// correct left (probe) and right (build) dependencies — even
		// when a child is itself a multi-stage subtree (e.g., nested join).
		var childLeaves [][]string
		// armMaterialized[i] is true when the arm's own SELECT list was
		// materialized onto the stage that terminates it, so the stream the
		// join receives IS the arm's published output rather than its raw
		// inner columns (#780). It decides which of the two answers below
		// names the build arm.
		armMaterialized := make([]bool, len(node.Children))
		for ci, child := range node.Children {
			childStart := len(*stages)
			p.walkStages(child, stages, nil)
			// A join input that is a subquery with a COMPUTED projection
			// must materialize the computed column into its producing scan
			// fragment, or the build/probe files never carry it and every
			// downstream read — the ON residual, the projected output —
			// sees NULL (#383).
			armMaterialized[ci] = absorbComputedSubqueryProjection(child, (*stages)[childStart:], false)
			// …and the same materialization for a join input that is a
			// SELECT list over an AGGREGATE: `(SELECT g, COUNT(*)+1 AS k …
			// GROUP BY g) b` joined ON b.k names a column the aggregate
			// stage does not emit, and the shuffle refused the plan (#681).
			if idx, ok := aggregateProjectionTarget(child, *stages, childStart); ok &&
				!p.cteTerminals[(*stages)[idx].ID] {
				p.recordAggProjectionRenames((*stages)[idx].ID,
					absorbAggregateOutputProjection(child, &(*stages)[idx]))
			}
			childLeaves = append(childLeaves, leafStages((*stages)[childStart:]))
		}
		// Map logical join type to canonical short form. Needed before the
		// broadcast decision: a join that preserves its BUILD side cannot
		// replicate it.
		jt := mapJoinType(node.JoinType)
		// An inner join with no condition at all IS a cross join (#376) —
		// same normalization as buildJoin, or the stage below would carry a
		// keyless hash_join the worker rejects.
		if jt == "inner" && strings.TrimSpace(node.JoinCond) == "" && node.JoinFilter == "" {
			jt = "cross"
		}

		// Broadcast replicates the build side to every task and splits the
		// probe across them. A RIGHT or FULL join emits its UNMATCHED build
		// rows, and no task can tell whether another task matched a given
		// build row — every task would emit all of them, so a 25-row answer
		// came back 75 rows on a 3-worker cluster. Those join types take the
		// hash-shuffle path instead, where both sides are co-partitioned and
		// each task owns a disjoint slice of the build. Same rule
		// planSkewSplitTasks already applies for the same reason.
		isBroadcast := !preservesBuildSide(jt) && p.isBroadcastCandidate(node)
		// A null-aware anti join reads ONE fact off its whole build side —
		// did any row have a NULL key — and answers with no rows at all when
		// the answer is yes (#507). Hash-partitioning the build splits that
		// fact: the task holding the NULL partition emits nothing while every
		// other task emits its probe rows, so `NOT IN` over a NULL-carrying
		// list came back with the rows a two-valued anti join would keep.
		// Replicating the build is what makes the fact whole per task; it is
		// a correctness requirement here, not a size heuristic.
		//
		// It overrides the SIZE decision, including an explicit
		// BroadcastBytesThreshold < 0 ("broadcast disabled"), so it is
		// counted and logged rather than silent: a null-aware anti join whose
		// build the threshold would have refused is replicating N× across the
		// cluster, and #539 is where the shape that removes the trade is
		// tracked.
		if node.NullAwareAnti && !preservesBuildSide(jt) && !isBroadcast && !nullAwareAntiForcingDisabled {
			isBroadcast = true
			NullAwareAntiForcedBroadcasts.Add(1)
			bytes, known := p.estimateSubtreeBytes(node.Children[1])
			slog.Warn("null-aware anti join: build side FORCED to replicate past the broadcast decision",
				"reason", "NOT IN's three-valued rule reads one fact off the WHOLE build (#507); "+
					"a hash-partitioned build splits it",
				"build_bytes", bytes, "build_bytes_known", known,
				"broadcast_threshold", p.BroadcastBytesThreshold,
				"tracked_in", "#539")
		}
		joinType := "hash_join"
		if isBroadcast {
			joinType = "broadcast_join"
		}

		// Identify left (probe) and right (build) dependency stages
		var leftDep, rightDep string
		if len(childLeaves) >= 1 && len(childLeaves[0]) > 0 {
			leftDep = childLeaves[0][len(childLeaves[0])-1]
		}
		if len(childLeaves) >= 2 && len(childLeaves[1]) > 0 {
			rightDep = childLeaves[1][len(childLeaves[1])-1]
		}

		// Extract join keys from condition (cross joins have no ON clause)
		var leftKeys, rightKeys []string
		var buildNaming *subtreeNaming
		if len(node.Children) >= 2 {
			buildNaming = subtreeNamingOf(node.Children[1])
		}
		if jt != "cross" {
			var residual []string
			leftKeys, rightKeys, residual = parseJoinKeys(node.JoinCond)
			if len(residual) > 0 {
				// walkStages has no error return; park the refusal the way
				// a set-operation refusal is parked (#346) and let
				// PlanDistributed raise it. Emitting a join keyed on a name
				// that is not a column is what made this silent.
				p.refuseJoin(refuseJoinCond(jt, node.JoinCond, residual))
			}
			// An outer join's ON residual (#358) rides stage.JoinFilter to the
			// worker, which compiles it there. Compile-check it NOW so an
			// unsupported expression refuses the plan instead of failing every
			// task at run time.
			if node.JoinFilter != "" && (jt == "left" || jt == "right" || jt == "full") {
				alias := ""
				if len(node.Children) >= 2 {
					alias = joinArmAlias(node.Children[1])
				}
				if BuildJoinResidualFilter(node.JoinFilter, alias) == nil {
					p.refuseJoin(fmt.Errorf("join ON residual %q on a %s join: "+
						"not evaluable as a probe residual (columns, literals, arithmetic and "+
						"comparisons are; function calls and subqueries are not)",
						node.JoinFilter, jt))
				}
			}
			// parseJoinKeys assigns left/right based on position in the "="
			// expression, not based on which child subtree owns the column.
			// Fix the assignment so leftKeys are from the probe (left) child
			// and rightKeys are from the build (right) child.
			if buildNaming != nil {
				assignJoinKeySides(leftKeys, rightKeys,
					subtreeNamingOf(node.Children[0]), buildNaming)
			}
		}

		// Resolve join keys through CTE/Project aliases so shuffle keys
		// match the actual column names in the data (e.g., supplier_no → l_suppkey).
		if len(node.Children) >= 2 {
			for i, key := range leftKeys {
				leftKeys[i] = resolveShuffleKey(key, node.Children[0], p.publishedBlocks)
			}
			for i, key := range rightKeys {
				rightKeys[i] = resolveShuffleKey(key, node.Children[1], p.publishedBlocks)
			}
		}

		// Big-vs-big inner equi-joins upgrade the shuffled hash join to a
		// sort-merge join when the SortMergeJoinBytes gate passes (same gate
		// as the local buildJoin path). The exchange children below are
		// IDENTICAL — co-partitioning is all SMJ needs — so only the stage
		// type changes. Broadcast candidates keep the strictly-better
		// broadcast path.
		if joinType == StageHashJoin && jt == "inner" && node.JoinFilter == "" &&
			len(leftKeys) > 0 && p.shouldSortMergeJoin(node, leftKeys, rightKeys) {
			joinType = StageSortMergeJoin
			SortMergeJoinsPlanned.Add(1)
		}

		// The key pair's resolved common type (#615), shared by the join
		// stage below and by the two exchange-repartition stages that feed
		// it — the SAME list, so the partition hash and the join key cannot
		// be built at two different types.
		stageKeyTypes := resolveJoinKeyTypes(node, leftKeys, rightKeys)

		// Insert shuffle stages for non-broadcast joins when distributed
		numPartitions := 0
		if !isBroadcast && jt != "cross" && len(leftKeys) > 0 && p.WorkerCount > 1 {
			// Use 8x workers as partition count to reduce per-task join memory.
			// Each partition receives 1/numPartitions of the shuffled data, so
			// higher counts reduce peak hash table memory on each worker.
			// At SF100 with 3 workers, 24 partitions halves per-partition
			// memory compared to the previous 12. HashPartitionCount is the
			// same rule EnsureDistribution applies to count-unpinned exchanges
			// (grouped finals, windows) — one width for all hash shuffles.
			numPartitions = HashPartitionCount(p.WorkerCount)

			// Compute columns the shuffle must preserve: join keys + all
			// columns needed downstream (from the join's NeededColumns).
			// Both sides get the full set — the Parquet reader ignores
			// columns that don't exist in the file.
			//
			// resolveJoinNeededColumns, not NeededColumns: the shuffle carries
			// what the STREAMS carry, which is source names, and the join
			// stage's own Columns have been resolved that way since #385. The
			// two disagreeing is a column DROPPED in the shuffle — a derived
			// table's `SUM(a) OVER () AS w` reaches the exchange as `w` while
			// the window stage emits `__win_0`, so the payload preserved a
			// name nothing carries, the join's input lost the real one, and
			// the gather's `OutputRename{__win_0 -> w}` fell back to the
			// producer's raw columns: `[id, y.id]` for a query that asked for
			// `[id, w]`. It bit only the SHUFFLED lowering, because the
			// broadcast one has no payload list to get wrong (#694 round 2).
			needed := resolveJoinNeededColumns(node, p.publishedBlocks)
			var shuffleCols []string
			if len(needed) > 0 {
				seen := make(map[string]bool, len(needed)+len(leftKeys)+len(rightKeys))
				for _, col := range needed {
					if !seen[col] {
						shuffleCols = append(shuffleCols, col)
						seen[col] = true
					}
				}
				for _, col := range leftKeys {
					if !seen[col] {
						shuffleCols = append(shuffleCols, col)
						seen[col] = true
					}
				}
				for _, col := range rightKeys {
					if !seen[col] {
						shuffleCols = append(shuffleCols, col)
						seen[col] = true
					}
				}
			}

			// Left (probe) side shuffle
			leftShuffleID := fmt.Sprintf("exchange-repartition-%d", len(*stages))
			*stages = append(*stages, Stage{
				ID:      leftShuffleID,
				Type:    StageExchangeRepartition,
				Tasks:   1,
				Columns: shuffleCols,
				Exchange: &ExchangeStage{
					Keys: append([]string(nil), leftKeys...),
					// Hash at the PAIR's resolved type, so the two sides'
					// equal values land in one partition (#615).
					KeyTypes: append([]parquet.TypeID(nil), stageKeyTypes...),
					Count:    numPartitions,
				},
				Dependencies: []string{leftDep},
			})

			// Right (build) side shuffle
			rightShuffleID := fmt.Sprintf("exchange-repartition-%d", len(*stages))
			*stages = append(*stages, Stage{
				ID:      rightShuffleID,
				Type:    StageExchangeRepartition,
				Tasks:   1,
				Columns: shuffleCols,
				Exchange: &ExchangeStage{
					Keys:     append([]string(nil), rightKeys...),
					KeyTypes: append([]parquet.TypeID(nil), stageKeyTypes...),
					Count:    numPartitions,
				},
				Dependencies: []string{rightDep},
			})

			leftDep = leftShuffleID
			rightDep = rightShuffleID
		}

		joinTasks := 1
		if numPartitions > 0 {
			joinTasks = numPartitions
		} else if isBroadcast && p.WorkerCount > 1 {
			// Parallel broadcast: split probe side across workers
			joinTasks = p.WorkerCount
		}
		stageID := fmt.Sprintf("join-%d", len(*stages))
		probeSchema, buildSchema := joinSideSchemas(node, leftKeys, rightKeys,
			p.publishedBlocks, p.subqueryOutputColumn)
		stage := Stage{
			ID:                 stageID,
			Type:               joinType,
			Tasks:              joinTasks,
			Columns:            resolveJoinNeededColumns(node, p.publishedBlocks),
			JoinType:           jt,
			JoinLeftKeys:       leftKeys,
			JoinRightKeys:      rightKeys,
			JoinKeyTypes:       stageKeyTypes,
			LeftDepStage:       leftDep,
			RightDepStage:      rightDep,
			JoinPartitionCount: numPartitions,
			JoinProbeSchema:    probeSchema,
			JoinBuildSchema:    buildSchema,
		}
		// Propagate build-side table alias for column disambiguation in self-joins
		// (e.g., nation n1 JOIN nation n2 — prevents duplicate columns from being dropped).
		if len(node.Children) >= 2 {
			// stageBuildTableAlias, not joinArmAlias: the DAG's build stream is
			// the arm's RAW columns, because a Project emits no stage. See
			// joinArmAlias' comment for the two answers and why they differ.
			//
			// UNLESS the arm's own SELECT list was just materialized onto the
			// stage that terminates it (#780). Then the stream is not raw: it
			// is exactly what the arm publishes, one column per SELECT item,
			// and the one name the enclosing query writes describes all of
			// them — which is the MATERIALIZED answer, `joinArmAlias`, and
			// with it the materialized per-column origins. Qualifying that
			// stream by an inner scan's alias instead named columns the arm
			// does not publish, and the enclosing `m.a` then bound the PROBE
			// arm's `a` by the qualifier strip: a silent wrong value.
			if armMaterialized[1] {
				if alias := joinArmAlias(node.Children[1]); alias != "" {
					stage.BuildTableAlias = alias
				}
				stage.BuildColOrigins = buildNaming.materializedBuildColOrigins()
			} else {
				if alias := stageBuildTableAlias(node.Children[1]); alias != "" {
					stage.BuildTableAlias = alias
				}
				// Multi-table build subtrees additionally carry per-column origin
				// aliases so the executor qualifies each duplicate with its OWNING
				// scan, not the (arbitrary) first one. Nil for single-scan builds.
				stage.BuildColOrigins = buildNaming.buildColOrigins()
			}
		}
		// The join's own materialized columns travel with the stage: the
		// worker drops them from the probe's output exactly as the
		// single-process planner does, so the two paths publish one column
		// set (ADR-0026 3c).
		stage.HiddenJoinCols = stageHiddenPositions(node, p.publishedBlocks)
		if marker, _, drop := lateralEmptySpec(node); marker != "" {
			stage.LateralPadMarker = marker
			stage.LateralDropMarker = drop
			for _, d := range node.LateralEmptyDefaults {
				if d.ExprSQL == "" {
					continue
				}
				stage.LateralEmptyDefaults = append(stage.LateralEmptyDefaults,
					LateralEmptyDefaultSpec{Column: d.Column, ExprSQL: d.ExprSQL})
			}
		}
		// Propagate semi/anti join inequality filters
		if node.JoinFilter != "" {
			stage.JoinFilter = node.JoinFilter
		}
		// …and NOT IN's three-valued rule, which is a property of the
		// PREDICATE this anti join came from and unknowable from the stage
		// alone (#507).
		stage.NullAwareAnti = node.NullAwareAnti
		if leftDep != "" {
			stage.Dependencies = append(stage.Dependencies, leftDep)
		}
		if rightDep != "" {
			stage.Dependencies = append(stage.Dependencies, rightDep)
		}
		*stages = append(*stages, stage)

	case logical.NodeUnion, logical.NodeIntersect, logical.NodeExcept:
		p.emitSetOpStages(node, stages)

	case logical.NodeDual:
		// Table-less SELECT: single-row source, runs locally on coordinator.
		stageID := fmt.Sprintf("dual-%d", len(*stages))
		*stages = append(*stages, Stage{
			ID:    stageID,
			Type:  "dual",
			Tasks: 1,
		})

	case logical.NodeFilter:
		// Walk children first. Try to push filter expressions down to the
		// appropriate stage: scan stages get predicate pushdown, join/aggregate
		// stages evaluate filters post-execution.
		preFilterCount := len(*stages)
		for _, child := range node.Children {
			p.walkStages(child, stages, parentID)
		}
		// A SELECT list between this predicate and an aggregate names the
		// aggregate's outputs by the query's aliases, which no stage emits.
		// Carry it onto the aggregate stage so the predicate has an alias to
		// be evaluated against — and so it is evaluated ABOVE the
		// projection, which is what filterCarrierIndex arranges once the
		// stage carries one (#656 shape f).
		if len(node.Children) == 1 {
			if idx, ok := aggregateProjectionTarget(node.Children[0], *stages, preFilterCount); ok &&
				!p.cteTerminals[(*stages)[idx].ID] {
				// Never onto a CTE body's terminal: an OpProject NARROWS,
				// and every other reference of that CTE reads the same
				// stage (#656 follow-up).
				p.recordAggProjectionRenames((*stages)[idx].ID,
					absorbAggregateOutputProjection(node.Children[0], &(*stages)[idx]))
			}
		}
		if len(node.Predicates) > 0 && len(*stages) > 0 {
			// Capture the filter-carrying stage by INDEX (not pointer) because
			// subsequent producer-stage emissions may append to *stages and
			// invalidate any held pointer.
			// The stage that will RUN the predicate — the last one emitted
			// when it qualifies, and otherwise a StageProject inserted above
			// it. Attaching to `len(*stages)-1` unconditionally is what let a
			// predicate land on a merge_sort a later pass deletes, on a
			// deduped cte-alias that never dispatches, or on a stage whose
			// projection runs above the filter slot (#656).
			filterIdx := filterCarrierIndex(stages, p.cteTerminals)
			if filterIdx < 0 {
				break
			}
			for _, pred := range node.Predicates {
				var exprStr, aliasStr string
				var aliasNames []string
				// A Project emits no stage here, so a predicate naming one of
				// its RENAMED or computed outputs would reach the producing
				// fragment as a column that fragment's schema does not carry
				// — nil from expr.ColRef.Eval, UNKNOWN on every row, zero
				// rows in silence (#653). The predicate does not move; only
				// its spelling changes. pred is the loop's own copy, so this
				// never writes to the logical plan the single-process path
				// shares.
				if len(node.Children) == 1 {
					if ast, names, ok := logical.ResolveFilterThroughProjects(pred, node.Children[0]); ok {
						exprStr, aliasNames = ast.String(), names
					}
				}
				// The spelling the query wrote, kept alongside the resolved
				// one: which of the two the carrying stage can evaluate is
				// decided at the end of planning (Stage.FilterAliases).
				switch {
				case pred.Raw != "":
					aliasStr = pred.Raw
				case pred.ASTExpr != nil:
					aliasStr = pred.ASTExpr.String()
				}
				if exprStr == "" {
					exprStr = aliasStr
				}
				if exprStr == "" {
					continue
				}
				if strings.EqualFold(aliasStr, exprStr) || len(aliasNames) == 0 {
					aliasStr, aliasNames = "", nil
				}
				// Resolve scalar subqueries. Under native-DAG, CTE-referencing
				// subqueries are deferred to the coordinator — this call returns
				// placeholders and the SQL for each producer we must emit.
				// The FILTER's input column types, so an IN-subquery can be
				// declined for a probe whose width the literal-list rule
				// would change (#615 F2).
				filterDecls := colDecls{}
				if len(node.Children) == 1 {
					filterDecls = inputColDecls(node.Children[0])
				}
				resolvedExpr, deferred := p.resolveFilterSubqueries(exprStr, filterDecls)
				for _, d := range deferred {
					producerID, err := p.emitScalarProducerStages(stages, d.SubquerySQL)
					if err != nil {
						// An AUTHORIZATION refusal is not a shape this
						// planner could not express here: it is the query's
						// answer, and there is no other path that answers it
						// differently. Park it — PlanDistributed returns it —
						// rather than run the same subquery on the
						// coordinator and then splice its TEXT back into a
						// worker's filter, which is what turned this refusal
						// into `subqueries require a SubqueryRunner` after
						// three task attempts (#945).
						if p.parkAuthorizationRefusal(err) {
							continue
						}
						// Fall back: evaluate the subquery eagerly and splice
						// a literal in place of the placeholder. Loses
						// correctness for CTE-drift cases but keeps the query
						// running rather than failing outright.
						start := time.Now()
						rows, schema, sErr := p.executeSubquerySchema(p.planCtx, d.SubquerySQL)
						slog.Warn("scalar producer emission failed; executed subquery on coordinator",
							"duration", time.Since(start).Round(time.Millisecond),
							"emit_error", err, "exec_error", sErr)
						spliced := false
						if sErr == nil && len(rows) > 0 {
							// Same one-row rule as the eager path above.
							v, cardErr := expr.ScalarSubqueryValue(d.SubquerySQL, rows)
							if cardErr != nil {
								p.refuseScalarRows(cardErr)
							} else {
								typ, typed := scalarColType(schema)
								lit := scalarToLiteral(v, typ, typed).String()
								resolvedExpr = strings.ReplaceAll(resolvedExpr, ":"+d.Placeholder, lit)
								spliced = true
							}
						}
						if !spliced {
							// Both paths failed: restore the original subquery
							// text so downstream sees what it saw before
							// deferral existed, not a dangling :scalar_N.
							resolvedExpr = strings.ReplaceAll(resolvedExpr,
								":"+d.Placeholder, "("+d.SubquerySQL+")")
						}
						continue
					}
					fs := &(*stages)[filterIdx]
					if fs.ScalarDependencies == nil {
						fs.ScalarDependencies = make(map[string]string)
					}
					fs.ScalarDependencies[d.Placeholder] = producerID
					// NOTE: producer IDs are deliberately NOT appended to
					// Dependencies because Dependencies models data that
					// flows into the stage as record batches; scalar
					// producers feed into FilterExprs via late-bound
					// string substitution instead. The coordinator's stage
					// goroutine awaits ScalarDependencies separately.
				}
				// Re-index after any appends.
				fs := &(*stages)[filterIdx]
				// A USER predicate on a stage that carries a security
				// projection runs ABOVE that projection, never in the slot
				// beside the policy's own row filter — otherwise it reads the
				// STORED column and its row set is arithmetic on the value the
				// policy hides (#859 round 2). The barrier is absorbed by the
				// time this runs: walkStages recurses children first, so the
				// Project below this Filter has already set the field.
				//
				// A predicate that pushdown moved BELOW the barrier reaches
				// this attach point earlier, while the field is still empty,
				// and lands in FilterExprs — which is right: substitution
				// replaced its policed references with the mask, so it names
				// no policed column at all.
				if !node.PolicyFilter && len(fs.SecurityProjectExprs) > 0 {
					fs.PostSecurityFilterExprs = append(fs.PostSecurityFilterExprs, resolvedExpr)
					p.attachedFilterExprs = append(p.attachedFilterExprs, resolvedExpr)
					continue
				}
				if node.PolicyFilter {
					// The one predicate that MAY read the stored column.
					fs.PolicyFilterExprs = append(fs.PolicyFilterExprs, resolvedExpr)
				}
				fs.FilterExprs = append(fs.FilterExprs, resolvedExpr)
				// Counted so a gate can assert CONSERVATION: every predicate
				// stage emission attached has to survive every rewriting
				// pass as a slot some stage still carries. A pass that
				// deletes the carrier without migrating the field is exactly
				// how shapes a–e of #656 answered without their WHERE.
				p.attachedFilterExprs = append(p.attachedFilterExprs, resolvedExpr)
				if aliasStr != "" {
					// Either spelling may be the one that survives
					// resolveFilterAliasSpelling; the gate accepts whichever.
					p.attachedFilterExprs[len(p.attachedFilterExprs)-1] =
						resolvedExpr + "\x00" + aliasStr
				}
				// Index-aligned: every FilterExprs entry gets an alias slot,
				// zero when there is no second spelling to choose from.
				for len(fs.FilterAliases) < len(fs.FilterExprs)-1 {
					fs.FilterAliases = append(fs.FilterAliases, FilterAliasSpec{})
				}
				fs.FilterAliases = append(fs.FilterAliases,
					FilterAliasSpec{Expr: aliasStr, Names: aliasNames})
			}
		}

	case logical.NodeWindow:
		preCount := len(*stages)
		for _, child := range node.Children {
			p.walkStages(child, stages, nil)
		}
		stageID := fmt.Sprintf("window-%d", len(*stages))
		winKeys := resolveWindowKeys(node)
		var winChild *logical.Node
		if len(node.Children) == 1 {
			winChild = node.Children[0]
		}
		var winCols []WindowColSpec
		for _, we := range node.WindowExprs {
			// Resolved by the same helper buildWindow uses, so the stage
			// spec and the single-process operator describe one computation
			// — including the output type, which nothing downstream of the
			// worker can correct (#345).
			ec := windowExecColumn(node, we, winKeys)
			var orderBy []SortKeySpec
			for i, ob := range we.OrderBy {
				// ec.OrderBy carries the RESOLVED spelling; ob.Column is
				// what the query wrote. A stage keyed on the latter would
				// send the worker the name #585 could not resolve.
				// SlotPos rides along: `windowExecColumn` decided it from the
				// aggregate's emitted output, and the worker rebuilds an
				// `exec.SortKey` from this spec — so without it the DAG's
				// window binds the key by NAME where the single-process one
				// binds it by position (#968).
				orderBy = append(orderBy, SortKeySpec{Column: ec.OrderBy[i].Column, Desc: ob.Desc,
					NullsLast: resolveNullsLast(ob), SlotPos: ec.OrderBy[i].SlotPos})
			}
			// A key naming a derived table's or CTE's SELECT-list alias
			// (`PARTITION BY gk` over `SELECT g AS gk`) is bound by neither
			// of resolveWindowKeys' two arms: it is not a qualified
			// reference and not an expression to materialize. The
			// single-process pipeline never notices — the Project below the
			// window is a real operator there — but on the DAG that Project
			// emits no stage, so the window's input carries the SOURCE
			// column and the worker refused the key outright (#658).
			// Resolved here rather than in a late pass because a PARTITION
			// BY key is also the stage's distribution: the exchange that
			// clusters the window's input is keyed on it, and rewriting the
			// key after EnsureDistribution would leave the two disagreeing.
			partitionBy := append([]string(nil), ec.PartitionBy...)
			// A COMPUTED alias (`PARTITION BY gk` over `SELECT g*2 AS gk`)
			// has no source column, so the walk above declines it and the
			// key named nothing the window's input carries:
			// `window: PARTITION BY "gk" is not a column of its input` on
			// both DAG arms for a query the single-process path answers
			// (#658). The value is MADE instead — the definition is
			// materialized onto the producing fragment under the alias's own
			// name — which is the same two names ADR-0026 §2 gives a GROUP BY
			// key and #807 gives a sort key, at the third caller of one
			// function.
			// …and a QUALIFIED key resolves inside the ARM its qualifier
			// names, for the reason the ARGUMENT does below (#742 round 4):
			// `derivedAliasSourceColumn` stops at a Join because it has no way
			// to choose an arm, so asked of one it answers nothing and the key
			// travelled as written. On the DAG the arm's own Project emits no
			// stage, so the join's stream carries x's SOURCE column and not
			// its alias, and `PARTITION BY x.w` over two arms that both
			// publish `w` bound the other arm's column — every row its own
			// partition, the window's own value where PostgreSQL answers the
			// partition's total (#975). Scoping it is what makes the key name
			// a column of the stream AND the stage's distribution key name the
			// same thing.
			var winAliases []aliasColumn
			for i, pb := range partitionBy {
				// Scoped with a SOURCE column: that arm's column is the key.
				// Scoped with NONE is a COMPUTED alias, and the walk FALLS
				// THROUGH to the un-scoped passes below — they are what
				// materializes a computed derived alias (#658), and skipping
				// them because the scoping ran left `PARTITION BY z.gk` over a
				// single derived relation refusing its own plan.
				if src, scoped := windowArgSourceInScope(pb, winChild); scoped && src != "" {
					partitionBy[i] = cleanExpr(src)
					continue
				}
				if src := derivedAliasSourceColumn(pb, winChild); src != "" {
					partitionBy[i] = cleanExpr(src)
					continue
				}
				if c := derivedAliasColumnFor(pb, winChild); c.Expr != "" {
					winAliases = append(winAliases, c)
					partitionBy[i] = c.Name
				}
			}
			for i := range orderBy {
				if src, scoped := windowArgSourceInScope(orderBy[i].Column, winChild); scoped && src != "" {
					orderBy[i].Column = cleanExpr(src)
					continue
				}
				if src := derivedAliasSourceColumn(orderBy[i].Column, winChild); src != "" {
					orderBy[i].Column = cleanExpr(src)
					continue
				}
				if c := derivedAliasColumnFor(orderBy[i].Column, winChild); c.Expr != "" {
					winAliases = append(winAliases, c)
					orderBy[i].Column = c.Name
				}
			}
			if len(winAliases) > 0 {
				materializeWindowAliasKeys((*stages)[preCount:], winAliases)
			}
			// …and the ARGUMENT, which exec.Window also reads by name off
			// the input batch: `SUM(v) OVER ()` over `SELECT c_i64 AS v`
			// found no vector called `v` and wrote NULL in every row, the
			// silent half of the same defect. A materialized argument is
			// already a __winkey_N the fragment computes, and
			// derivedAliasSourceColumn leaves those (and `*`) alone.
			// …and a QUALIFIED argument resolves inside the arm its
			// qualifier names, because derivedAliasSourceColumn stops at a
			// Join and answered nothing for it (round 4 of #742). Without
			// the scoping `SUM(x.w) OVER ()` over two arms both publishing
			// `w` reached the worker as the bare `w` and summed the OTHER
			// arm's column.
			inputCol := ec.InputCol
			if src, scoped := windowArgSourceInScope(inputCol, winChild); scoped {
				if src != "" {
					inputCol = cleanExpr(src)
				}
			} else if src := derivedAliasSourceColumn(inputCol, winChild); src != "" {
				inputCol = cleanExpr(src)
			}
			winCols = append(winCols, WindowColSpec{
				Func:     we.Func,
				InputCol: inputCol,
				// …and the candidates for the case the scoping above cannot
				// settle: an argument naming a derived arm's COMPUTED alias
				// has no source column to rewrite to, so it travels as the
				// alias and the window's input may publish it under another
				// name or not at all (#770).
				InputRefs:      aliasCandidatesForText(inputCol, winChild),
				OutputCol:      ec.OutputCol,
				OutputType:     ec.OutputType,
				PartitionBy:    partitionBy,
				OrderBy:        orderBy,
				Frame:          we.Frame,
				LagLeadOffset:  ec.LagLeadOffset,
				LagLeadDefault: ec.LagLeadDefault,
				NtileBuckets:   ec.NtileBuckets,
				NthValueN:      ec.NthValueN,
			})
		}
		stage := Stage{
			ID:    stageID,
			Type:  StageWindow,
			Tasks: 1,
			// The keys the window fragment has to COMPUTE before it can
			// partition on them (#585). The worker prepends one projection
			// for the whole stage: the keys are shared across its OVER
			// clauses, and computing a shared key twice would put two
			// columns of one name on the batch.
			WindowKeyExprs: respellWindowKeyExprs(windowKeySpecs(winKeys), winChild),
			WindowCols:     winCols,
		}
		// Only depend on leaf stages from subtree (not transitive deps like scan).
		stage.Dependencies = leafStages((*stages)[preCount:])
		*stages = append(*stages, stage)

	default:
		// Passthrough nodes (Project, Distinct) — walk children.
		//
		// NOTE (#163/#466): Distinct still emits no stage here. It no longer
		// silently drops the DISTINCT, because nothing that carries semantics
		// reaches this branch: logical.rewriteDistinctAsGroupBy turns every
		// user Distinct(Project) in the tree into a GroupBy aggregate, which
		// stages as an aggregate and solves by construction the problem that
		// blocked a GroupByAll dedup stage (the projection below the Distinct
		// becomes the group keys, so the dedup runs on the output columns
		// rather than over-distinguishing on the scan's full width). What can
		// still arrive is a Distinct the rewrite declined: on the root path
		// the coordinator's post-gather dedup applies it (MergeInfo.
		// HasDistinct); anywhere else refuseUnstageableDistinct has already
		// refused the query. A planner-inserted BuildSideDedup Distinct
		// carries no user-visible semantics and passes through as before.
		preDefaultCount := len(*stages)
		for _, child := range node.Children {
			p.walkStages(child, stages, parentID)
		}
		// A SELECT list directly above an AGGREGATE names outputs the stage
		// emits under the GROUP BY expression's own TEXT, which no consumer
		// can spell. Carry it here — not only where a Filter or a JOIN
		// forced it — because a SORT, an outer projection or the gather
		// needs the alias just as much: `SELECT k * 2 AS d FROM (SELECT
		// g + 1 AS k … GROUP BY g + 1) s ORDER BY d` failed loud with
		// `sort: key column "d" does not exist`, and a window over the same
		// producer the same way (#656 F2).
		//
		// absorbAggregateOutputProjection declines every projection that
		// already names what the stage emits, so this is a no-op for the
		// ordinary shapes — and it renames ONLY what has no usable name, so
		// the resolvers that map an alias back to its source column are
		// untouched.
		if node.Type == logical.NodeProject {
			if idx, ok := aggregateProjectionTarget(node, *stages, preDefaultCount); ok &&
				!p.cteTerminals[(*stages)[idx].ID] {
				p.recordAggProjectionRenames((*stages)[idx].ID,
					absorbAggregateOutputProjection(node, &(*stages)[idx]))
			}
		}
		// A DERIVED BLOCK A STAR READS PUBLISHES ITS OWN PROJECTION (#984).
		//
		// The absorb above renames what the aggregate emits and is additive;
		// this publishes the block's whole list, by position, because the
		// consumer is a STAR and reads by position. Only for a block the
		// pre-pass marked — no Project between it and the root, a JOIN in
		// between, and a projection that is not already the stage's own
		// column list — so a named SELECT list, which resolves each column
		// through its own consumer, is untouched.
		//
		// A CTE TERMINAL IS PUBLISHED TOO, unlike the absorb above. The
		// cteTerminals guard exists for anything CONSUMER-SPECIFIC — a filter
		// belonging to one reference of a CTE read by two — and a block's own
		// SELECT list is the opposite of that: it is the relation the CTE
		// DEFINES, identical for every reference. Withholding it there left a
		// twice-referenced CTE marked and unpublishable, so the query was
		// refused and routed instead of executing (round-1 B1).
		if node.Type == logical.NodeProject && p.starReadBlocks[node] != blockAgrees &&
			len(*stages) > preDefaultCount {
			// Only a block that was REALLY materialized is recorded. The
			// declaration for an empty side and the join's key binding both
			// read this set, and a block marked published that the pass then
			// declined would have them describing a relation no task writes —
			// which is the ADR-0010 disagreement this pass exists to end.
			if publishBlockProjection(node, stages, preDefaultCount, p.publishedBlocks,
				p.subqueryOutputColumn) {
				p.publishedBlocks[node] = true
			}
		}
		// ABAC security barrier (InjectColumnPolicies wraps the scan in a
		// Project of masked/visible columns). An ordinary Project can pass
		// through — the gather recovers aliases — but a DROPPED barrier
		// leaks raw values, so absorb it into the scan stage it wraps:
		// the scan fragment applies it as an OpProject before anything
		// else consumes rows (filters excepted; they run on pre-barrier
		// columns exactly as the single-process pipeline orders them).
		if node.Type == logical.NodeProject && node.SecurityBarrier && len(node.Children) == 1 {
			// Predicate pushdown may have moved Filters below the barrier
			// (barrier → Filter… → Scan); filters on raw columns below the
			// mask is exactly the single-process pipeline's order, so
			// absorbing across them preserves semantics.
			child := node.Children[0]
			for child != nil && child.Type == logical.NodeFilter && len(child.Children) == 1 {
				child = child.Children[0]
			}
			if child != nil && child.Type == logical.NodeScan {
				absorbSecurityBarrier(node, child, stages)
			}
		}
	}
}

// absorbSecurityBarrier attaches a security-barrier projection to the scan
// stage just emitted for its child. The barrier lists every visible column
// (bare passthrough) with masked columns as literal expressions and denied
// columns absent; output names are the original column names, so downstream
// stages (joins, aggregates, gather renames) resolve unchanged — they just
// see masked values and never see denied ones.
func absorbSecurityBarrier(node, scan *logical.Node, stages *[]Stage) {
	var target *Stage
	for i := len(*stages) - 1; i >= 0; i-- {
		s := &(*stages)[i]
		if s.Type == StageScan && s.TableName == scan.TableName {
			target = s
			break
		}
	}
	if target == nil {
		return
	}
	// Trim to the columns the scan actually reads (parquet pruning hint) —
	// passthroughs of unread columns would emit useless null columns.
	need := make(map[string]bool, len(target.Columns))
	for _, c := range target.Columns {
		need[c] = true
	}
	specs := make([]ProjectExprSpec, 0, len(node.Projections))
	for _, pr := range node.Projections {
		name := pr.Alias
		if name == "" {
			name = pr.Column
		}
		if name == "" {
			continue
		}
		expr := pr.Expr
		if expr == "" {
			expr = pr.Column
		}
		isExpr := pr.ASTExpr != nil && !isSimpleColRefForRename(pr.ASTExpr)
		// Trim passthroughs the scan doesn't read; masks are computed from
		// literals (the scan never reads the raw column), so they always stay.
		if !isExpr && len(need) > 0 && !need[name] {
			continue
		}
		var typ parquet.TypeID
		var prec, scale int
		if isExpr {
			// Same integer-preserving-arithmetic hint as
			// attachScanSelectProjections (#297, #445).
			typ, prec, scale = declTypeParts(inferProjectionDeclType(pr.ASTExpr, parquet.TypeString,
				strictIntArithCols(scan),
				colDecls{types: scan.ScanColTypes, fields: scan.ScanColFields, dec: scan.ScanColDecimal}))
		}
		specs = append(specs, ProjectExprSpec{Expr: expr, Name: name, Type: typ,
			TypeKnown: isExpr, Precision: prec, Scale: scale})
	}
	if len(specs) > 0 {
		target.SecurityProjectExprs = specs
	}
}
