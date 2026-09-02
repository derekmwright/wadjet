package physical

import (
	"errors"
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// ErrGroupKeyDistributed marks a plan the stage DAG refuses because a GROUP BY
// key needs its RESOLUTION name and its PUBLISHED name to be two different
// things, and a stage has only one field for both.
//
// ADR-0026 §2 gives a derived key a hidden `__gb_expr_N` slot to be resolved by
// and its canonical text to be published under, and BOTH engines now carry the
// two separately: the single-process pre-aggregate projection with
// `exec.HashAggregate.GroupByOutNames`, and the stage DAG with
// `Stage.GroupByResolve` beside `Stage.GroupByCols`.
//
// One shape of #736's third mechanism is answered by that carrier and no
// longer refused — a key an aggregate DIRECTLY BELOW already publishes:
//
//	SELECT DISTINCT g + 1 AS k FROM typemx GROUP BY g + 1
//	  PG 8 rows; both DAG arms used to answer ONE row, NULL.
//
// It lowers to two aggregates keyed alike (`rewriteDistinctAsGroupBy`), and the
// outer one reads the inner one's OUTPUT, where the key is already a column
// under its own text and `g` is gone. Re-materializing it evaluated that `g`
// against a schema without one and collapsed the table into a single NULL
// group. The resolution list says `Computed=false` for such a key, so the
// fragment looks up the COLUMN instead of re-parsing the text as arithmetic.
//
// What is still refused here needs more than the carrier, and each shape's own
// mechanism is named at its condition below. A refusal is a HANDOFF rather than
// the query's outcome: `Coordinator.ExecuteSQL` routes it to the
// coordinator-local single-process pipeline, which is the
// `ErrDistinctDistributed` / `ErrGroupingSetsDistributed` route.
//
// Refusing narrowly is the whole point: an ordinary `GROUP BY g + 1` is
// unaffected and stays distributed, because its key needs no second name — the
// worker materializes it into a slot and publishes it under the same text, and
// nothing else in the batch answers to that text.
var ErrGroupKeyDistributed = errors.New(
	"this GROUP BY key needs a published name the stage cannot carry")

// refuseUnstageableGroupKey returns a typed refusal for the first Aggregate
// whose keys need the two names separated.
func refuseUnstageableGroupKey(n *logical.Node) error {
	if n == nil {
		return nil
	}
	if n.Type == logical.NodeAggregate {
		if err := unstageableGroupKey(n); err != nil {
			return err
		}
	}
	for _, child := range n.Children {
		if err := refuseUnstageableGroupKey(child); err != nil {
			return err
		}
	}
	return nil
}

func unstageableGroupKey(agg *logical.Node) error {
	keys := groupKeyOutputs(agg)
	if len(keys) == 0 {
		return nil
	}
	// The aggregate's OUTPUT names, lowercased. A key published under one of
	// them is two columns of one name in the batch the gather reads.
	outs := make(map[string]bool, len(agg.AggExprs))
	for _, a := range agg.AggExprs {
		if a.OutputCol != "" {
			outs[strings.ToLower(a.OutputCol)] = true
		}
	}
	for _, k := range keys {
		if k.Literal || k.Identity == "" {
			continue
		}
		// (1) A key an aggregate DIRECTLY BELOW already publishes was refused
		// here, and is not any more: `groupKeyOutputs` records it as
		// PublishedBelow, `stageGroupKeyNames` turns that into a resolution
		// with Computed=false, and the fragment looks the COLUMN up instead of
		// re-deriving arithmetic against a schema the aggregate below no
		// longer has (ADR-0026 §2).
		//
		// (2) A DERIVED key published under a name one of this aggregate's own
		// outputs also answers to. On the DAG the key's column comes first and
		// wins every by-name lookup above it.
		if k.Derived && outs[strings.ToLower(k.Name)] {
			return fmt.Errorf("%w: the key %q and an aggregate output share one published name,"+
				" and the batch resolves a name to its FIRST column — the key's",
				ErrGroupKeyDistributed, k.Name)
		}
	}
	// (3) A key that names a derived table's COMPUTED alias — window-wrapped
	// or not — was refused here. It is RESOLVED now, at the end of planning,
	// against a model of what the producing fragment really ships
	// (resolveStageGroupKeys, ADR-0026 §2/§4a). What that pass refuses is a
	// different statement: not "the answer is undecidable" but "no fragment in
	// this plan carries the value", with the stream's column list in the error.
	return nil
}
