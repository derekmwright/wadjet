package physical

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// ErrLateralProjectionDistributed marks a plan the stage DAG refuses because a
// STAR reads a derived block whose PROJECTION no stage could publish.
//
// The refusal's TRIGGER changed with arc K3 (#984) and its DISPOSITION did not.
// It used to fire on a name test — "is every column the block publishes a
// column of the stream, once" — and every shape that failed it was handed to
// the coordinator-local pipeline, because a Project emits no stage and the
// star would otherwise publish the stream. A stage carries the block's own
// projection now (starReadBlockProjections / publishBlockProjection), so
// almost every one of those shapes runs distributed; what is left is the set
// the pass DECLINED, and this refusal names exactly that set:
//
//	starReadBlocks  the blocks a star reads whose projection is not the
//	                stage's column list — the candidates
//	publishedBlocks the ones a stage really carries
//	the difference  a star reading a relation the plan cannot state
//
// The two ways a candidate is declined, and why each is a decline rather than
// a guess:
//
//   - a COMPUTED item whose type the plan cannot state. `ARRAY[COUNT(*)]`
//     inside a CASE with a NULL arm decides nothing (expr.Undecided), and a
//     projection materialized under a type the empty side of the same join
//     declares differently is ADR-0010's `one stage's files describe one
//     relation` — the loud failure this route exists to spare the client.
//   - a producer that cannot carry the projection at all: its specs do not
//     resolve against what it emits, or a StageProject above it would hide an
//     ordering its consumer reads off its direct dependency.
//
// Asking the question AFTER stage generation is what makes it exact. A
// plan-time name test cannot know whether the pass will succeed, and a refusal
// that fires where the pass would have worked takes an ordinary distributed
// query off the DAG for nothing.
var ErrLateralProjectionDistributed = errors.New(
	"a star reads a derived block whose projection no stage publishes")

// refuseUnpublishedStarBlock returns the refusal when a block a star reads was
// NOT materialized onto a stage.
func refuseUnpublishedStarBlock(candidates, published map[*logical.Node]bool) error {
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
