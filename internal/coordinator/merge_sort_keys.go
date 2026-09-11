package coordinator

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// Coordinator merges must apply every ORDER BY key or refuse (#1002, #163).
// SlotPos wins: duplicate output names are not addresses (#557).
// Otherwise use exec.ColumnIndexFallback: exact spelling, bare name for a
// qualified reference, then a unique .bare suffix; decline ambiguity (#989).
// mergeProbePartials and dedupGatherResult bind like local and DAG sorts.
// An unresolved key is a 0A000 refusal, never a skipped key or an unclassified
// error: a total order has no permitted nondeterminism (ADR-0013).
// This is a wadjet merge boundary, not a PostgreSQL rejection (#811, ADR-0012).
// See docs/internals/coordinator-merge-order-binding.md for the design.
const mergeOrderSQLState = "0A000"

func mergeSortKeyIndices(b *batch.RecordBatch, orderBy []logical.OrderExpr) ([]int, error) {
	if b == nil {
		return nil, sqlerr.New(mergeOrderSQLState, "ordering a merged result: no batch to order")
	}
	out := make([]int, len(orderBy))
	for i, ob := range orderBy {
		if ob.SlotPos > 0 && ob.SlotPos <= len(b.Schema) {
			out[i] = ob.SlotPos - 1
			continue
		}
		name := plansql.NormalizeIdentRef(strings.TrimSpace(ob.Column))
		idx := exec.ColumnIndexFallback(b, name)
		if idx < 0 {
			have := make([]string, len(b.Schema))
			for j, c := range b.Schema {
				have[j] = c.Name
			}
			return nil, sqlerr.New(mergeOrderSQLState,
				"ordering a merged result: ORDER BY key %q does not resolve in the merged columns [%s]",
				ob.Column, strings.Join(have, " "))
		}
		out[i] = idx
	}
	return out, nil
}

// orderableBatchesErr is what a merge says when `coalesceForOrdering` did not
// hand it ONE batch to order.
//
// It is nil for an EMPTY result — zero batches, or several carrying no active
// rows, which is what a merge over partials that all filtered everything out
// looks like. There is nothing to put in an order there and no answer to get
// wrong.
//
// Everything else is a REFUSAL: `coalesceForOrdering` declines when the
// partials do not share one schema, which means they do not describe one
// relation, and the alternative to saying so is the rows in their arrival
// order — an order the client did not ask for and cannot detect (#1002).
func orderableBatchesErr(batches []*batch.RecordBatch) error {
	total := 0
	for _, b := range batches {
		if b != nil {
			total += b.ActiveLen()
		}
	}
	if total == 0 {
		return nil
	}
	return sqlerr.New(mergeOrderSQLState,
		"ordering a merged result: %d partial batches carrying %d rows do not share one schema",
		len(batches), total)
}
