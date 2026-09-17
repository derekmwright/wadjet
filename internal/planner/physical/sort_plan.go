// SPDX-License-Identifier: MIT

// This file holds sort plan for the physical planner, governed by ADR-0026.
package physical

import (
	"context"
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
)

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
	if width, ok := sortInputSetOpWidth(child); ok {
		if ob.SlotPos > width {
			return 0
		}
		return ob.SlotPos
	}
	if child == nil || child.Type != logical.NodeProject || logical.HasStarProjection(child) {
		return 0
	}
	visible := logical.VisibleProjections(child.Projections)
	if ob.SlotPos > len(visible) {
		return 0
	}
	// The hidden columns a Sort's projection appends come AFTER the visible
	// ones (HiddenSortTrimOp trims the tail), so a visible position is the
	// same index in the batch — but only when the visible ones really are the
	// prefix.
	for i := 0; i < len(visible); i++ {
		if child.Projections[i] != visible[i] {
			return 0
		}
	}
	return ob.SlotPos
}

// sortInputSetOpWidth reports the number of result columns when a Sort reads a
// SET OPERATION directly, and false otherwise.
//
// A set operation needs no proof that a position addresses its stream: its
// output IS its result column list, in order, on both engines. Every arm is
// projected onto that list — `SetOpArmProjection` builds the DAG union stage's
// per-arm projection from it and `AlignSetOpRows` re-keys the in-process rows
// onto it — precisely so the arms are one schema and the concatenation is well
// defined. Position i of the list is therefore column i of the sort's input by
// construction.
//
// The list is the operation's leftmost arm's names and two of them may be the
// SAME string: `SELECT order_id AS amount, amount FROM lat_item UNION SELECT
// id, total FROM lat_ord ORDER BY 1, 2 DESC` publishes `amount` twice, so with
// no position both keys resolved to the first column and key 2 was never
// applied — on every arm, 7 rows in PostgreSQL's key-1 order with key 2
// ignored (#1022).
func sortInputSetOpWidth(child *logical.Node) (int, bool) {
	if !isSetOpNode(setOpUnwrap(child)) {
		return 0, false
	}
	n := len(setOpOutputNames(child))
	return n, n > 0
}

// sortKeyLocalSlotPos tries an ordinal first, then the visible SELECT-list
// position named by alias or written expression (#556/#557; #905, #629).
// Exactly one visible item must match; ambiguous names keep existing by-name/
// qualified-to-bare fallback. Positions distinguish duplicate output names that
// the local Project no longer qualifies; multiset tests cannot detect wrong order.
// Do not wire this into sortKeySlotPosStage: DAG positions address producer output
// and require that function's separate SELECT-list proof.
// See docs/internals/local-sort-visible-item-positions.md for the design.
func sortKeyLocalSlotPos(ob logical.OrderExpr, sortNode *logical.Node) int {
	if pos := sortKeySlotPos(ob, sortNode); pos > 0 {
		return pos
	}
	return sortKeyWrittenSlotPos(ob, sortNode)
}

// sortKeyWrittenSlotPos is the visible SELECT-list position a WRITTEN sort term
// names — by its alias, else by the expression the item was written as — or 0
// where no position is provable.
//
// Exactly one visible item must answer to the term: two is the ambiguity
// PostgreSQL raises 42702 for and wadjet answers by binding the first
// (ADR-0012's divergence list), and taking a position there would change
// WHICH one, on one arm and not the others.
//
// Extracted from sortKeyLocalSlotPos so both engines resolve a written term's
// slot through ONE function: the single-process path had it and the DAG did
// not, which is #1014 — `ORDER BY b.amount` beside an output column also
// called `amount` sorted by the other one on the distributed arms alone. What
// the two callers do NOT share is the PROOF that the position addresses their
// stream; each still makes its own (sortKeySlotPosStage's is measured against
// the producing stage).
func sortKeyWrittenSlotPos(ob logical.OrderExpr, sortNode *logical.Node) int {
	term := strings.TrimSpace(ob.Column)
	if term == "" || sortNode == nil || len(sortNode.Children) == 0 {
		return 0
	}
	child := sortNode.Children[0]
	if child == nil || child.Type != logical.NodeProject || logical.HasStarProjection(child) {
		return 0
	}
	visible := logical.VisibleProjections(child.Projections)
	// The same prefix proof SortKeySlotPos makes: a visible position is an
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
		ChildSource: childSource,
		ChildOps:    childOps,
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
		return p.BuildTopN(ctx, child, node.LimitVal)
	}
	// LIMIT n OFFSET m over a sort: same Top-K machinery with n+m kept
	// rows, plus the Limit operator above to skip the offset. Without
	// this, OFFSET queries (ClickBench Q40-43) fully materialized the
	// sort input.
	if child.Type == logical.NodeSort && node.OffsetVal > 0 && node.LimitVal > 0 {
		source, ops, sink, err := p.BuildTopN(ctx, child, node.LimitVal+node.OffsetVal)
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

func (p *Planner) BuildTopN(ctx context.Context, sortNode *logical.Node, n int) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
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
		ChildSource: childSource,
		ChildOps:    childOps,
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
		childOps = append(childOps, newComputedColumnsOpWithMeta(keyProjections, keyMeta))
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
		ChildSource: childSource,
		ChildOps:    childOps,
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
		ChildSource: childSource,
		ChildOps:    childOps,
		agg:         hashAgg,
	}, nil, &exec.CollectSink{}, nil
}
