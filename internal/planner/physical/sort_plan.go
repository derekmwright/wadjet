// This file holds sort plan for the physical planner, governed by ADR-0026.
package physical

import (
	"context"
	"fmt"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"strings"
)

// sortKeySlotPosStage is sortKeySlotPos for the DAG, which needs a stricter
// proof and gets one.
//
// The position addresses the SELECT LIST, so it may only be used where the
// operator's input IS the select list. On the single-process path a Project
// operator sits directly below the Sort and it is. On the DAG **no stage is
// emitted for a Project**, so the sort stage reads the materialized output of
// the PRODUCING stage — which is the select list only when that producer is a
// single relation, narrowed to exactly those columns by the scan-output
// pruning. Put a JOIN or a set operation under it and the stage emits both
// arms' whole schemas: `SELECT clt1.c2, clt2.c1 FROM clt1, clt2 ORDER BY 2`
// then sorted by column ONE on both DAG arms, right values in the wrong
// sequence (round-0 B4 — the author's own self-flag).
//
// The producer's final column list is NOT available here: Stage.OutputColumns
// is filled by pruneScanOutputColumns AFTER walkStages returns, so an exact
// check against it cannot be made at this point. The subtree's SHAPE can be,
// and it is the claim this bound rests on — asserted from both sides in
// benchmarks/tpch/duplicate_name_dag_test.go and
// internal/coordinator/collide_two_path_test.go: one relation uses the
// position, a join declines it and resolves by name, and both answer
// PostgreSQL's order.
func sortKeySlotPosStage(ob logical.OrderExpr, sortNode *logical.Node, produced []Stage) int {
	pos := sortKeySlotPos(ob, sortNode)
	if pos == 0 {
		return 0
	}
	if !subtreeJoinsRelations(sortNode) {
		return pos
	}
	// …unless the producer MATERIALIZED the select list, which is the one
	// case where the stream and the select list are the same list (#1003).
	if producerPublishesSelectList(produced, sortNode) {
		return pos
	}
	return 0
}

// producerPublishesSelectList reports whether the stage that produces this
// sort's input publishes the SELECT list as the ordered prefix of its own
// output.
//
// It is the measurement the bound above otherwise has to guess at, and
// declining to measure it is a silent wrong ORDER (#1003). Two output columns
// may legally carry one name — `SELECT DISTINCT a.order_id AS amount,
// b.amount … ORDER BY 1, 2 DESC` publishes `amount` twice — and once the
// position is dropped the key is resolved by that name, which
// `ColumnIndexFallback` answers with the FIRST match. BOTH keys then bound
// column one, so the two DAG arms returned the rows sorted by the leading key
// alone where PostgreSQL 17 and the single-process arms apply both. A total
// order is not one of ADR-0013's nondeterminism classes.
//
// What makes the position usable here is not the producer's KIND but what it
// PUBLISHES (ADR-0026 §8, K3's rule): the `final_aggregate` stage under that
// query materializes `[a.order_id→amount, b.amount→amount, a.order_id,
// b.amount]`, so the select list IS positions 1 and 2 of the stream. Where the
// projection is NOT materialized — `SELECT clt1.c2, clt2.c1 FROM clt1, clt2
// ORDER BY 2`, whose join stage carries no ProjectExprs — the check fails and
// the key resolves by name exactly as before.
//
// The whole visible list is compared, name AND source expression, so a
// producer that publishes the same names in another order, or narrows the
// list, does not qualify.
func producerPublishesSelectList(produced []Stage, sortNode *logical.Node) bool {
	if len(produced) == 0 || sortNode == nil || len(sortNode.Children) == 0 {
		return false
	}
	child := sortNode.Children[0]
	if child == nil || child.Type != logical.NodeProject || logical.HasStarProjection(child) {
		return false
	}
	visible := logical.VisibleProjections(child.Projections)
	if len(visible) == 0 {
		return false
	}
	specs := produced[len(produced)-1].ProjectExprs
	if len(specs) < len(visible) {
		return false
	}
	for i, pr := range visible {
		src := cleanExpr(pr.Expr)
		if src == "" {
			src = pr.Column
		}
		if src == "" || !sameProjectionSource(specs[i].Expr, src) {
			return false
		}
		// The SOURCE is the identity; the NAME is the check, and a
		// projection has more than one legitimately — the resolution
		// spelling every pass inside the planner binds by, and the
		// PublishedName the client is told (ADR-0026 §2). The stage
		// materializes whichever the output owes.
		if !projectionAnswersToName(pr, specs[i].Name) {
			return false
		}
	}
	return true
}

// sameProjectionSource reports whether a stage spec's source expression and a
// logical projection's name the SAME input column.
//
// They may differ by a QUALIFIER and by nothing else: an aggregate publishes a
// group key under its stripped name (ADR-0026 §2) so the logical projection
// reads `order_id`, while the stage spec keeps the written `a.order_id`. Where
// both sides carry a qualifier they must agree on it, so two arms of a
// self-join are never taken for one another.
func sameProjectionSource(specExpr, projExpr string) bool {
	a := plansql.NormalizeIdentRef(cleanExpr(specExpr))
	b := plansql.NormalizeIdentRef(cleanExpr(projExpr))
	if a == "" || b == "" {
		return false
	}
	if strings.EqualFold(a, b) {
		return true
	}
	ab, bb := blockBareName(a), blockBareName(b)
	if !strings.EqualFold(ab, bb) {
		return false
	}
	// Exactly one side was bare; two different qualifiers are two columns.
	return a == ab || b == bb
}

// projectionAnswersToName reports whether name is one of the names this
// projection legitimately publishes.
func projectionAnswersToName(pr logical.Projection, name string) bool {
	if name == "" {
		return false
	}
	name = plansql.NormalizeIdentRef(name)
	for _, cand := range []string{pr.PublishedName, pr.Alias, pr.Column, cleanExpr(pr.Expr)} {
		if cand != "" && strings.EqualFold(plansql.NormalizeIdentRef(cand), name) {
			return true
		}
	}
	return false
}

// subtreeJoinsRelations reports whether a node's subtree combines two
// relations — a join or a set operation — anywhere below it.
func subtreeJoinsRelations(n *logical.Node) bool {
	if n == nil {
		return false
	}
	switch n.Type {
	case logical.NodeJoin, logical.NodeUnion, logical.NodeIntersect, logical.NodeExcept:
		return true
	}
	for _, c := range n.Children {
		if subtreeJoinsRelations(c) {
			return true
		}
	}
	return false
}

// sortKeySlotPos is the input column index (1-based) a sort key addresses, or
// 0 to resolve it by name.
//
// It answers only when the Sort's input PROVABLY publishes the select list in
// order and by itself — the projection immediately below it, with no star left
// unexpanded and no hidden column ahead of the visible ones. Everywhere else
// the name is the address it always was; a position guessed against a schema
// this layer cannot enumerate would sort by the wrong column, which is the
// defect rather than the fix.
func sortKeySlotPos(ob logical.OrderExpr, sortNode *logical.Node) int {
	if ob.SlotPos <= 0 || sortNode == nil || len(sortNode.Children) == 0 {
		return 0
	}
	child := sortNode.Children[0]
	if child == nil || child.Type != logical.NodeProject || logical.HasStarProjection(child) {
		return 0
	}
	visible := logical.VisibleProjections(child.Projections)
	if ob.SlotPos > len(visible) {
		return 0
	}
	// The hidden columns a Sort's projection appends come AFTER the visible
	// ones (hiddenSortTrimOp trims the tail), so a visible position is the
	// same index in the batch — but only when the visible ones really are the
	// prefix.
	for i := 0; i < len(visible); i++ {
		if child.Projections[i] != visible[i] {
			return 0
		}
	}
	return ob.SlotPos
}

// sortKeyLocalSlotPos is the single-process pipeline's address for an ORDER BY
// key: sortKeySlotPos's ordinal answer first, and then the SELECT-list
// POSITION of the item the key NAMES.
//
// The name alone stopped being an address the moment two output columns could
// share one, which is #556/#557's position identity one consumer over.
// `WITH cte AS (...) SELECT a.id, b.id FROM cte a JOIN cte b ON a.a = b.a
// ORDER BY a.id, b.id` publishes two columns called `id`; the Sort's keys are
// built as `cleanExpr(ob.Column)`, which STRIPS the qualifier, so both keys
// became `id` and `columnIndexFallback` bound both of them to the FIRST one.
// The second key was never applied: PostgreSQL 17 answers
// `1,1 | 1,2 | 1,3 | 1,8` and the single-process path answered
// `1,8 | 1,3 | 1,2 | 1,1` — the right rows in the wrong sequence, which no
// multiset comparison can see (#905, the #629 family). The stage DAG is right
// on this shape already, because its sort keys keep the QUALIFIED spelling and
// its join stage publishes `a.id` and `b.id` under those names; the single
// path's Project output carries neither, so the position is the only address
// it has.
//
// The match is the one PostgreSQL makes: an ORDER BY term may name an output
// column, by its alias or by the spelling the SELECT list wrote. Exactly one
// visible item must match — two is ambiguous and keeps today's by-name
// resolution, which is also what the qualified-to-bare fallback is for.
//
// It is deliberately NOT wired into sortKeySlotPosStage. A position there
// addresses the PRODUCING STAGE's output, which is the select list only for a
// single narrowed relation (see that function), and the DAG does not need it.
func sortKeyLocalSlotPos(ob logical.OrderExpr, sortNode *logical.Node) int {
	if pos := sortKeySlotPos(ob, sortNode); pos > 0 {
		return pos
	}
	term := strings.TrimSpace(ob.Column)
	if term == "" || sortNode == nil || len(sortNode.Children) == 0 {
		return 0
	}
	child := sortNode.Children[0]
	if child == nil || child.Type != logical.NodeProject || logical.HasStarProjection(child) {
		return 0
	}
	visible := logical.VisibleProjections(child.Projections)
	// The same prefix proof sortKeySlotPos makes: a visible position is an
	// index into the batch only while the visible items really are the prefix
	// of the projection list.
	for i := 0; i < len(visible); i++ {
		if child.Projections[i] != visible[i] {
			return 0
		}
	}
	match := 0
	for i := range visible {
		if visible[i].Alias == "" || !strings.EqualFold(visible[i].Alias, term) {
			continue
		}
		if match > 0 {
			return 0 // two items answer to this name
		}
		match = i + 1
	}
	if match > 0 {
		return match
	}
	for i := range visible {
		if !strings.EqualFold(strings.TrimSpace(projSourceName(&visible[i])), term) {
			continue
		}
		if match > 0 {
			return 0
		}
		match = i + 1
	}
	return match
}

func (p *Planner) buildSort(ctx context.Context, node *logical.Node) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	if len(node.Children) == 0 {
		return nil, nil, nil, fmt.Errorf("sort has no child")
	}

	childSource, childOps, _, err := p.buildPipeline(ctx, node.Children[0])
	if err != nil {
		return nil, nil, nil, err
	}

	var keys []exec.SortKey
	for _, ob := range node.OrderBy {
		order := exec.Ascending
		if ob.Desc {
			order = exec.Descending
		}
		keys = append(keys, exec.SortKey{
			Column:    sortKeyLocalColumn(ob),
			Order:     order,
			NullsLast: resolveNullsLast(ob),
			// The select-list POSITION: the Project below a Sort narrows the
			// schema to exactly its visible outputs in order, so position i of
			// the select list is column i of this operator's input — and it is
			// the only address that survives two outputs sharing a name
			// (#557 for a term written as an ordinal, #905 for one written as
			// a name).
			SlotPos: sortKeyLocalSlotPos(ob, node),
		})
	}

	sortOp := exec.NewSort(keys)
	if sm := p.getSpillManager(); sm != nil {
		sortOp.Spill = sm
	}

	return &sortSourceAdapter{
		childSource: childSource,
		childOps:    childOps,
		sort:        sortOp,
	}, nil, &exec.CollectSink{}, nil
}

func (p *Planner) buildLimit(ctx context.Context, node *logical.Node) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	if len(node.Children) == 0 {
		return nil, nil, nil, fmt.Errorf("limit has no child")
	}

	child := node.Children[0]

	// Top-N late materialization: narrow scan + row-loc top-N + winner
	// refetch for wide-projection ORDER BY ... LIMIT over a plain scan.
	// Falls back to the ordinary top-N build when the shape doesn't
	// qualify (see topn_late_mat.go).
	if child.Type == logical.NodeSort && node.LimitVal > 0 {
		src, ok, err := p.tryBuildTopNLateMat(ctx, child, node.LimitVal+node.OffsetVal)
		if err != nil {
			return nil, nil, nil, err
		}
		if ok {
			var ops []exec.UnaryOperator
			if node.OffsetVal > 0 {
				ops = append(ops, exec.NewLimit(int64(node.LimitVal), int64(node.OffsetVal)))
			}
			return src, ops, &exec.CollectSink{}, nil
		}
	}

	// Optimization: Limit(Sort(...)) → TopN sort (heap-based, keeps only N rows)
	if child.Type == logical.NodeSort && node.OffsetVal == 0 && node.LimitVal != logical.NoLimit {
		return p.buildTopN(ctx, child, node.LimitVal)
	}
	// LIMIT n OFFSET m over a sort: same Top-K machinery with n+m kept
	// rows, plus the Limit operator above to skip the offset. Without
	// this, OFFSET queries (ClickBench Q40-43) fully materialized the
	// sort input.
	if child.Type == logical.NodeSort && node.OffsetVal > 0 && node.LimitVal > 0 {
		source, ops, sink, err := p.buildTopN(ctx, child, node.LimitVal+node.OffsetVal)
		if err != nil {
			return nil, nil, nil, err
		}
		ops = append(ops, exec.NewLimit(int64(node.LimitVal), int64(node.OffsetVal)))
		return source, ops, sink, nil
	}

	source, ops, sink, err := p.buildPipeline(ctx, child)
	if err != nil {
		return nil, nil, nil, err
	}

	// Push LIMIT hint to scan source: enables lazy file downloading instead
	// of the eager "download all files upfront" strategy. Safe when no pipeline
	// breaker (sort/aggregate/join) sits between LIMIT and SCAN — if there is
	// one, the source won't be a catalogScanSource and the assertion fails.
	// OFFSET without LIMIT bounds nothing: every row past the offset is in
	// the answer, so there is no row count at which the scan may stop.
	if cs, ok := source.(*catalogScanSource); ok && len(cs.rowPreds) == 0 && node.LimitVal > 0 {
		// Pushed scan filters need the row-group-parallel path; the lazy
		// LIMIT scan would silently skip them. Filters win — a filtered
		// LIMIT usually needs to scan broadly anyway.
		cs.rowLimit = int64(node.LimitVal) + int64(node.OffsetVal)
	}

	limit := exec.NewLimit(int64(node.LimitVal), int64(node.OffsetVal))
	ops = append(ops, limit)

	return source, ops, sink, nil
}

func (p *Planner) buildTopN(ctx context.Context, sortNode *logical.Node, n int) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	childSource, childOps, _, err := p.buildPipeline(ctx, sortNode.Children[0])
	if err != nil {
		return nil, nil, nil, err
	}

	var keys []exec.SortKey
	for _, ob := range sortNode.OrderBy {
		order := exec.Ascending
		if ob.Desc {
			order = exec.Descending
		}
		keys = append(keys, exec.SortKey{
			Column:    sortKeyLocalColumn(ob),
			Order:     order,
			NullsLast: resolveNullsLast(ob),
			SlotPos:   sortKeyLocalSlotPos(ob, sortNode),
		})
	}

	sortOp := exec.NewSort(keys)
	sortOp.Limit = n // Top-K: only materialize top N rows
	// Parity with buildSort: without the spill manager, the pre-sort input
	// of every ORDER BY ... LIMIT query buffered fully untracked — the
	// top-K heap only runs at finalize, so this was an unbounded,
	// pressure-invisible accumulation on the most common query shape.
	if sm := p.getSpillManager(); sm != nil {
		sortOp.Spill = sm
	}

	return &sortSourceAdapter{
		childSource: childSource,
		childOps:    childOps,
		sort:        sortOp,
	}, nil, &exec.CollectSink{}, nil
}

func (p *Planner) buildWindow(ctx context.Context, node *logical.Node) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	if len(node.Children) == 0 {
		return nil, nil, nil, fmt.Errorf("window has no child")
	}

	childSource, childOps, _, err := p.buildPipeline(ctx, node.Children[0])
	if err != nil {
		return nil, nil, nil, err
	}

	winKeys := resolveWindowKeys(node)
	keyProjections, keyMeta, err := p.windowKeyProjections(winKeys)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(keyProjections) > 0 {
		// The GROUP BY expression shape, one operator over: a pass-through
		// projection that keeps every input column and appends the computed
		// PARTITION BY / ORDER BY keys, so exec.Window resolves them by name
		// like any other column (#585).
		childOps = append(childOps, NewComputedColumnsOpWithMeta(keyProjections, keyMeta))
	}

	if err := refuseUnwindowable(node.WindowExprs); err != nil {
		return nil, nil, nil, err
	}
	var winCols []exec.WindowColumn
	for _, we := range node.WindowExprs {
		winCols = append(winCols, windowExecColumn(node, we, winKeys))
	}

	winOp := exec.NewWindow(winCols)
	if sm := p.getSpillManager(); sm != nil {
		winOp.Spill = sm
	}

	return &windowSourceAdapter{
		childSource: childSource,
		childOps:    childOps,
		win:         winOp,
	}, nil, &exec.CollectSink{}, nil
}

func (p *Planner) buildDistinct(ctx context.Context, node *logical.Node) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	if len(node.Children) == 0 {
		return nil, nil, nil, fmt.Errorf("distinct has no child")
	}

	childSource, childOps, _, err := p.buildPipeline(ctx, node.Children[0])
	if err != nil {
		return nil, nil, nil, err
	}

	// DISTINCT is a keys-only hash aggregate over all output columns (the
	// Trino/Spark shape). The previous streaming Distinct operator kept one
	// serialized key per distinct row in an untracked, unspillable map —
	// tens of GB at SF100 cardinalities, invisible to the memory budget.
	// As a HashAggregate it inherits spill, tracker accounting, cooperative
	// relief, and the typed group fast paths. GroupByAll resolves the key
	// set from the first batch's schema, so no plan-time schema knowledge
	// is needed (covers SELECT DISTINCT * and the semi/anti dedup rewrite).
	hashAgg := exec.NewHashAggregate(nil, nil)
	hashAgg.GroupByAll = true
	if est := findScanRowEstimate(node.Children[0]); est > 0 {
		hashAgg.InputRowHint = est
	}
	if sm := p.getSpillManager(); sm != nil {
		hashAgg.Spill = sm
	}

	return &aggSourceAdapter{
		childSource: childSource,
		childOps:    childOps,
		agg:         hashAgg,
	}, nil, &exec.CollectSink{}, nil
}
