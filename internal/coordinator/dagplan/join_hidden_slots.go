// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

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
	side := localPlanFacts.LateralSideOf(node)
	if side < 0 {
		return nil
	}
	var out []HiddenJoinCol
	if block := materializedBlockUnder(node.Children[side], published); block != nil {
		for _, hidden := range node.HiddenJoinCols {
			if localPlanFacts.LateralMarkerDroppedAbove(node, hidden) {
				continue
			}
			for i, name := range localPlanFacts.EmittedColumnNames(block) {
				if !strings.EqualFold(name, hidden) {
					continue
				}
				out = append(out, HiddenJoinCol{Ordinal: i, Name: hidden, Probe: side == 0})
				break
			}
		}
		return out
	}
	declared := localPlanFacts.DeclaredJoinSchema(node.Children[side], nil, nil, nil)
	for _, hidden := range node.HiddenJoinCols {
		if localPlanFacts.LateralMarkerDroppedAbove(node, hidden) {
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
