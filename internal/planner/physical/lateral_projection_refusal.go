package physical

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// ErrLateralProjectionDistributed refuses star-read blocks whose projection
// no stage publishes: starReadBlocks minus publishedBlocks, checked AFTER
// stage generation (#984). A plan-time name test cannot predict success.
// Decline computed items with undecidable types (empty and non-empty sides
// must describe one relation, ADR-0010), specs unresolvable against producer
// output, or StageProject placements hiding ordering read from a direct dep.
// The coordinator-local handoff disposition remains unchanged.
// See docs/internals/unpublished-star-block-refusal.md for the design.
var ErrLateralProjectionDistributed = errors.New(
	"a star reads a derived block whose projection no stage publishes")

// refuseUnpublishedStarBlock returns the refusal when a block a star reads was
// NOT materialized onto a stage.
func refuseUnpublishedStarBlock(candidates map[*logical.Node]blockDivergence,
	published map[*logical.Node]bool) error {
	var missing []string
	for block := range candidates {
		if published[block] {
			continue
		}
		names := emittedColumnNames(block)
		if len(names) == 0 {
			names = []string{"<unnamed>"}
		}
		missing = append(missing, strings.Join(names, ", "))
	}
	if len(missing) == 0 {
		return nil
	}
	// Deterministic: the set is walked from a map, and a refusal message that
	// changes between two runs of one query is not a message a user can act on.
	sort.Strings(missing)
	return fmt.Errorf("%w: the subquery publishes (%s), which no stage could be "+
		"made to emit — a Project emits no stage, so a star over this join would "+
		"publish the stage's stream instead of the columns the query wrote",
		ErrLateralProjectionDistributed, missing[0])
}
