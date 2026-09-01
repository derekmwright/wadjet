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
// and its canonical text to be published under, and the single-process
// pipeline carries the two separately (the pre-aggregate projection and
// `exec.HashAggregate.GroupByOutNames`). `Stage.GroupByCols` is both at once:
// the worker's `derivedGroupKeys` re-derives "is this key derived?" by PARSING
// the text, mints its own slot, and publishes under the same string. Two shapes
// need the separation and get a wrong answer without it — silently, on both DAG
// arms, where the single-process path answers PostgreSQL's rows:
//
//	SELECT DISTINCT g + 1 AS k FROM typemx GROUP BY g + 1
//	  PG 8 rows; both DAG arms ONE row, NULL.
//	SELECT g + 1 AS k, COUNT(*) AS "g + 1" FROM typemx GROUP BY g + 1
//	  PG the counts; both DAG arms the KEY's value under the aggregate's alias.
//
// The first lowers to two aggregates keyed alike (`rewriteDistinctAsGroupBy`);
// the outer one reads the inner one's OUTPUT, where the key is already a column
// under its own text and `g` is gone, so re-materializing it evaluates that `g`
// against a schema without one and collapses the table into a single NULL
// group. The second gives one name to two columns of one batch, and
// `batch.RecordBatch.ColumnIndex` answers with the FIRST — the key, which the
// aggregate publishes before its outputs.
//
// Both are #736's third mechanism, and the fix ADR-0026 sketches is a new
// `Stage` field carrying the source-spelled expression beside the published
// name — a wire change through `distributed.OpSpec` and the worker's aggregate
// builder. Until that exists, the refusal is what makes these shapes ANSWER:
// `Coordinator.ExecuteSQL` routes them to the coordinator-local single-process
// pipeline, which separates the two names already. This is the
// `ErrDistinctDistributed` / `ErrGroupingSetsDistributed` route, and it is a
// HANDOFF rather than the query's outcome.
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
		// (1) A key an aggregate DIRECTLY BELOW already publishes, and that is
		// the ONLY reason it needs no materializing — `groupKeyOutputs` records
		// it as PublishedBelow for exactly this question. Its published name is
		// a text no column REFERENCE can spell, so the worker re-derives it as
		// arithmetic against a schema the aggregate below no longer has.
		//
		// Asking `!k.Derived && !nameIsPlainColumn(k.Name)` instead was the
		// first cut and it was far too wide: `GROUP BY n1.n_name` is not
		// derived because it IS a column, and its qualified name is not a
		// "plain column" by that predicate — so TPC-H Q07 was refused and
		// routed local, which the stage-dump golden caught.
		if k.PublishedBelow {
			return fmt.Errorf("%w: the key %q is already published by the aggregate below it,"+
				" and a stage carries one name for both what the worker computes and what it"+
				" emits — recomputing it there reads columns that aggregate no longer has",
				ErrGroupKeyDistributed, k.Name)
		}
		// (2) A DERIVED key published under a name one of this aggregate's own
		// outputs also answers to. On the DAG the key's column comes first and
		// wins every by-name lookup above it.
		if k.Derived && outs[strings.ToLower(k.Name)] {
			return fmt.Errorf("%w: the key %q and an aggregate output share one published name,"+
				" and the batch resolves a name to its FIRST column — the key's",
				ErrGroupKeyDistributed, k.Name)
		}
	}
	return nil
}
