// SPDX-License-Identifier: MIT

package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// AN ORDER BY TERM ADDRESSES AN OUTPUT SLOT, NOT A NAME (#968).
//
// `SELECT x.a AS b, SUM(x.b) AS a FROM decpair x GROUP BY x.a ORDER BY a
// LIMIT 3` puts TWO columns called `a` into the aggregate's one output schema
// — the group key, whose relation qualifier `exec.PublishedGroupKeyNames`
// strips, and the aggregate, whose output name is the user's alias. On the
// stage DAG the sort reads that schema directly (no stage is emitted for a
// Project), `exec.columnIndexFallback` answers with the FIRST match, and the
// query came back with PostgreSQL's rows in the order of the OTHER column:
// the three smallest group keys where PostgreSQL takes the three smallest
// sums.
//
// The slot is the address, and the two halves of it are recorded where each
// is known: the CLASS at emission, on `SortKeySpec.NamesAggregateOutput`,
// because only the SELECT list says whether the term names an aggregate call
// or a key reference; and the POSITION here, at the end of planning, because
// only then is the producing stage's output settled. That is ADR-0026 §6's
// pattern — a consumer records its candidates at emission and settles them
// against what its PRODUCER publishes — with `aggregateEmittedSlots` as the
// model, the same one `pinProjectSpecSlots` reads one consumer over.

// sortKeyLocalColumn keeps the ORDER BY spelling including its qualifier (#989).
// Join streams qualify duplicate build names; stripping qualifiers merges keys
// and can invert a total order, which ADR-0013 does not permit (#989).
// Bind the producer's published identity, never reinterpret names as structure
// (ADR-0026 §6, §2c). columnIndexFallback tries qualified first then bare, so no
// build-side model is needed and prior unqualified resolution remains available.
// A SELECT-list position from sortKeyLocalSlotPos still wins (#905); star-only
// queries have no Project position.
// See docs/internals/local-sort-key-qualified-identity.md for the design.
func sortKeyLocalColumn(ob logical.OrderExpr) string {
	return plansql.NormalizeIdentRef(strings.TrimSpace(ob.Column))
}

// windowOrderKeySlot is the 1-based position a WINDOW's ORDER BY key addresses
// in its input, or 0 to keep the name path.
//
// It answers only where the window reads an AGGREGATE directly and that
// aggregate emits the key's name TWICE — the one case a name cannot say which
// column is meant. `aggregateEmittedOutputNames` is the model, the same one
// `buildProject`'s slot pinning and `pinSortKeySlotsOverProducerOutput` read,
// and the class comes from the term itself.
//
// Everywhere else it answers 0 and nothing changes: a window whose producer is
// not an aggregate, a name only one column answers to, a term no re-spell gave
// a class.
func windowOrderKeySlot(win *logical.Node, name string, isAgg bool) int {
	if win == nil || len(win.Children) != 1 || strings.TrimSpace(name) == "" {
		return 0
	}
	names, ok := aggregateEmittedOutputNames(win.Children[0])
	if !ok {
		return 0
	}
	agg := findAggregateAncestor(win.Children[0])
	if agg == nil {
		return 0
	}
	nAgg := len(agg.AggExprs)
	if nAgg == 0 || nAgg > len(names) {
		return 0
	}
	want := strings.ToLower(strings.TrimSpace(name))
	hits := 0
	for _, n := range names {
		if strings.EqualFold(strings.TrimSpace(n), want) {
			hits++
		}
	}
	if hits < 2 {
		return 0 // one column answers to it; the name IS the address
	}
	// An aggregate emits `[group keys…, aggregate outputs…]`, so the class
	// splits the list at len(names)-nAgg — the same split
	// `aggregateEmittedSlots` makes for a Stage.
	lo, hi := 0, len(names)-nAgg
	if isAgg {
		lo, hi = len(names)-nAgg, len(names)
	}
	for i := lo; i < hi; i++ {
		if strings.EqualFold(strings.TrimSpace(names[i]), want) {
			return i + 1
		}
	}
	return 0
}
