// This file holds join hidden slots for the physical planner, governed by ADR-0023 and ADR-0026.
package physical

import (
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"strings"
)

// joinProbeOutputFilter is the single-process join's OutputFilter, built from
// the join node's NeededColumns with ROW FIELD PATHS expanded to their
// CONTAINER.
//
// A field path names no column (ADR-0022): `c_row.b` is resolved OUT of the
// ROW column `c_row` by the expression compiler, so an OutputFilter carrying
// the dotted spelling and not the container drops the only thing the
// projection can read, and every field comes back NULL —
//
//	SELECT x.id, c_row.b FROM typemx_nested x JOIN typemx_dim d ON x.id = d.k
//	-- PostgreSQL 0, 11, NULL, NULL, 44 · single-process all-NULL
//
// — while the same query with no join answers correctly, because nothing
// narrows there. An OutputFilter can only NARROW, so adding a qualifier that
// names no column costs nothing; the expansion is unconditional for that
// reason rather than guessing which qualifiers are ROW columns.
// lateralEmptyDefaultOps is the operator that carries an ungrouped aggregate's
// empty-input value on the lateral's own output column, or nil when this join
// has none. See exec.LateralEmptyDefault and logical.Node.LateralCountDefaults.
func lateralEmptyDefaultOps(node *logical.Node) []exec.UnaryOperator {
	marker, cols, drop := lateralEmptySpec(node)
	op := exec.NewLateralEmptyDefault(marker, cols, drop)
	if op == nil {
		return nil
	}
	return []exec.UnaryOperator{op}
}

// compileLateralDefault compiles one column's empty-input rule — the CASE the
// logical layer rendered — through the engine's own expression compiler, the
// same route every other computed column takes. A rule that will not compile
// is dropped rather than approximated: the column then reads what the pad
// wrote, which is NULL.
func compileLateralDefault(sql string) (exec.Expression, error) {
	node, err := plansql.ParseExpression(sql)
	if err != nil {
		return nil, err
	}
	compiled, err := expr.Compile(node)
	if err != nil {
		return nil, err
	}
	return compiled.Eval, nil
}

// lateralEmptySpec is the empty-input default's three parts: the column whose
// NULL marks a padded row, one COMPILED rule per output column, and whether
// this operator is the one that drops the marker.
//
// It drops the marker when the marker IS a slot the lowering minted — the join
// would otherwise drop it BELOW this operator, and the marker is what this
// operator reads. Where the lateral publishes its key under a name the query
// wrote, the column is the user's and nobody drops it.
func lateralEmptySpec(node *logical.Node) (marker string, cols []exec.LateralDefault, drop bool) {
	if node == nil || len(node.LateralEmptyDefaults) == 0 || node.LateralPadMarker == "" {
		return "", nil, false
	}
	for _, d := range node.LateralEmptyDefaults {
		if d.ExprSQL == "" {
			// The item's empty-input value is NULL, which is what the pad
			// already writes — or this pass could not build the rule. Either
			// way the column is left exactly as the pad wrote it.
			continue
		}
		compiled, err := compileLateralDefault(d.ExprSQL)
		if err != nil {
			continue
		}
		cols = append(cols, exec.LateralDefault{Column: d.Column, Expr: compiled})
	}
	marker = node.LateralPadMarker
	for _, h := range node.HiddenJoinCols {
		if sameLateralMarker(h, marker) {
			drop = true
			break
		}
	}
	if len(cols) == 0 && !drop {
		return "", nil, false
	}
	return marker, cols, drop
}

// LateralEmptyDefaultSpec is one output column's empty-input RULE on a join
// stage — the CASE the logical layer rendered, which the worker compiles; see
// logical.LateralEmptyDefault and exec.LateralDefault.
type LateralEmptyDefaultSpec struct {
	Column  string
	ExprSQL string
}

// HiddenJoinCol is one column a join MINTED for itself: the ORDINAL it sits
// at in the side that carries it, the name the planner expects there (a
// safety check, never the identity), and which side that is.
type HiddenJoinCol struct {
	Ordinal int
	Name    string
	Probe   bool
}

// stageHiddenPositions is joinHiddenPositions against the model the
// DISTRIBUTED path runs under: what each side's STAGE emits, which is not what
// the logical subtree emits — a Project emits no stage, so a lateral whose
// SELECT list is a bare projection streams its SCAN's columns and the slot's
// alias never lands there at all. A slot the stream does not carry has no
// ordinal and is dropped by nobody, which is the honest answer for that shape.
//
// WHERE THE BLOCK IS MATERIALIZED THE TWO MODELS ARE ONE (#984). A block a
// star reads publishes its own projection onto the stage, so the stage's
// column list IS the subtree's emitted list and the ordinal is read from the
// projection — the single-process model, from the same function that computes
// it there. Reading the stream's ordinal for a materialized block would drop
// whatever column happens to sit at the slot's old position.
func stageHiddenPositions(node *logical.Node, published map[*logical.Node]bool) []HiddenJoinCol {
	if node == nil || len(node.HiddenJoinCols) == 0 || len(node.Children) < 2 {
		return nil
	}
	side := lateralSideOf(node)
	if side < 0 {
		return nil
	}
	var out []HiddenJoinCol
	if block := materializedBlockUnder(node.Children[side], published); block != nil {
		for _, hidden := range node.HiddenJoinCols {
			if lateralMarkerDroppedAbove(node, hidden) {
				continue
			}
			for i, name := range emittedColumnNames(block) {
				if !strings.EqualFold(name, hidden) {
					continue
				}
				out = append(out, HiddenJoinCol{Ordinal: i, Name: hidden, Probe: side == 0})
				break
			}
		}
		return out
	}
	declared := declaredJoinSchema(node.Children[side], nil, nil, nil)
	for _, hidden := range node.HiddenJoinCols {
		if lateralMarkerDroppedAbove(node, hidden) {
			continue
		}
		for i, col := range declared {
			if !strings.EqualFold(col.Name, hidden) {
				continue
			}
			out = append(out, HiddenJoinCol{Ordinal: i, Name: hidden, Probe: side == 0})
			break
		}
	}
	return out
}

// lateralMarkerDroppedAbove reports whether the empty-input default operator
// ABOVE this join is the one that removes name — the marker it reads. The join
// leaves it in place then, because the join's own drop runs below.
func lateralMarkerDroppedAbove(node *logical.Node, name string) bool {
	if node == nil || node.LateralPadMarker == "" || len(node.LateralEmptyDefaults) == 0 {
		return false
	}
	return sameLateralMarker(name, node.LateralPadMarker)
}

// sameLateralMarker compares a hidden column's name with the pad marker on
// their BARE halves: the marker is qualified by the lateral's alias so the
// OPERATOR can tell a colliding build column from a user's stored one, and
// `HiddenJoinCols` carries the slot as the lowering minted it.
func sameLateralMarker(a, b string) bool {
	bare := func(s string) string {
		if dot := strings.LastIndexByte(s, '.'); dot >= 0 && dot < len(s)-1 {
			return s[dot+1:]
		}
		return s
	}
	return strings.EqualFold(bare(a), bare(b))
}

// lateralSideOf is which child of this join the LATERAL lowering built, or -1
// when neither says so. Only that side can carry a column this join minted;
// the other one's `__key_0` is a USER's stored column (ADR-0012), and looking
// for the name on both sides dropped it.
func lateralSideOf(node *logical.Node) int {
	if node == nil || len(node.Children) < 2 {
		return -1
	}
	for i := 0; i < 2; i++ {
		if node.Children[i] != nil && node.Children[i].LateralSubtree {
			return i
		}
	}
	return -1
}

// joinHiddenPositions is where each column this join MINTED sits in the side
// that carries it — the identity `exec.HashJoinProbe.OutputExcludeProbe` /
// `OutputExcludeBuild` drop by.
//
// A NAME cannot be that identity. Reading is not minting, so a table may
// already store a column called `__key_0` (ADR-0012), and a query may
// CORRELATE ON it — which is exactly when a "is it a join key of its own
// side" test admits the user's column to the exclusion. Only the position
// says which column this lowering put there.
//
// The model is the SINGLE-PROCESS one: what each side's subtree EMITS, which
// for a Project is its projections in order. The distributed path computes its
// own against the STAGE's stream, because a Project emits no stage there
// (stageHiddenPositions).
func joinHiddenPositions(node *logical.Node) (probe, build map[int]string) {
	if node == nil || len(node.HiddenJoinCols) == 0 || len(node.Children) < 2 {
		return nil, nil
	}
	side := lateralSideOf(node)
	if side < 0 {
		return nil, nil
	}
	names := emittedColumnNames(node.Children[side])
	if len(names) == 0 {
		return nil, nil
	}
	found := map[int]string{}
	for _, hidden := range node.HiddenJoinCols {
		if lateralMarkerDroppedAbove(node, hidden) {
			continue
		}
		for i, n := range names {
			if !strings.EqualFold(n, hidden) {
				continue
			}
			found[i] = hidden
			break
		}
	}
	if len(found) == 0 {
		return nil, nil
	}
	if side == 0 {
		return found, nil
	}
	return nil, found
}

// emittedColumnNames is the ORDER a batch from this subtree arrives in, as
// far as the logical plan states it: a Project emits its projections, an
// Aggregate its published keys then its aggregates, a Scan its columns. A
// shape it cannot state returns nil, and a position nobody can compute drops
// nothing.
func emittedColumnNames(n *logical.Node) []string {
	for cur := n; cur != nil; {
		switch cur.Type {
		case logical.NodeProject:
			out := make([]string, 0, len(cur.Projections))
			for _, pr := range cur.Projections {
				name := pr.Alias
				if name == "" {
					name = pr.Column
				}
				if name == "" {
					name = strings.TrimSpace(pr.Expr)
				}
				out = append(out, name)
			}
			return out
		case logical.NodeAggregate:
			published, resolve := stageGroupKeyNames(cur, aggInput(cur))
			out := append([]string(nil), stageEmittedKeyNames(published, resolve)...)
			for _, agg := range cur.AggExprs {
				out = append(out, agg.OutputCol)
			}
			return out
		case logical.NodeScan:
			return cur.ScanColumns
		}
		if len(cur.Children) != 1 {
			return nil
		}
		cur = cur.Children[0]
	}
	return nil
}

// aggInput is the node an aggregate reads, or nil.
func aggInput(n *logical.Node) *logical.Node {
	if n == nil || len(n.Children) != 1 {
		return nil
	}
	return n.Children[0]
}

func joinProbeOutputFilter(node *logical.Node) map[string]bool {
	if len(node.NeededColumns) == 0 {
		return nil
	}
	filter := make(map[string]bool, len(node.NeededColumns)+2)
	for _, col := range node.NeededColumns {
		filter[col] = true
		if dot := strings.IndexByte(col, '.'); dot > 0 && dot < len(col)-1 {
			filter[col[:dot]] = true
		}
	}
	return filter
}
