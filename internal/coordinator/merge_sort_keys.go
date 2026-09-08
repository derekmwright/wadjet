package coordinator

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// THE COORDINATOR'S MERGE APPLIES THE QUERY'S ORDERING OR SAYS IT CANNOT
// (#1002).
//
// Two merges in this package re-apply a top-level ORDER BY over rows the DAG
// has already produced: `mergeProbePartials`, after a probe-split re-aggregate
// or dedup, and `dedupGatherResult`, after the post-gather DISTINCT that
// `walkStages` emits no stage for (#163). Both used to bind a key by looking
// its written spelling up in `mergeColIdx`, an EXACT map, and to `continue`
// past a key that missed.
//
// A dropped key is a silent wrong ORDER. `SELECT DISTINCT * FROM lat_item a
// JOIN lat_item b ON b.order_id = a.order_id ORDER BY a.order_id, a.amount,
// b.amount` is a TOTAL order — ADR-0013 lists no nondeterminism class that
// covers one — and the join publishes `[id order_id product amount b.id
// b.order_id b.product b.amount]`: the probe's columns bare and every
// DUPLICATE build column qualified by its owning alias. Neither `a.order_id`
// nor `a.amount` is a key of that map, so both were dropped and both DAG arms
// answered the rows sorted by `b.amount` alone — the LEADING key not applied
// at all — while PostgreSQL 17 and the two single-process arms answered the
// written sequence.
//
// The binding is `exec.ColumnIndexFallback`, the engine's ONE resolver: the
// exact spelling, then the bare name for a qualified reference, then a single
// `.bare` suffix match, declining an ambiguity rather than guessing. It is
// what the single-process Sort binds through (`physical.sortKeyLocalColumn`,
// #989) and what the DAG's own sort stage binds through, so the three paths
// resolve one key one way. `SlotPos` wins where the planner recorded one: a
// name stops being an address the moment two output columns carry it (#557),
// and that is the address the SELECT list gives.
//
// A key that still does not resolve is an ERROR. The alternative is what this
// function replaces — rows in an order the client did not ask for and cannot
// detect — and `reAggregatePartials` already refuses an unresolvable GROUP BY
// name one screen up for the same reason.
// mergeOrderSQLState is the class both merge refusals carry: PostgreSQL
// ANSWERS these statements, and what wadjet is saying is that ITS OWN merge
// cannot apply the ordering — `0A000`, feature not supported, which is the code
// the rest of this family uses for a wadjet-side bound (#811 family C, ADR-0012).
// A refusal that reaches a client with no SQLSTATE is one the client cannot act
// on.
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
