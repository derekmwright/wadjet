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

// sortTermNamesAggregateItem reports whether the SELECT item an ORDER BY term
// names is an aggregate call.
//
// Exactly one visible item must answer to the term, by its alias or by the
// name it publishes: two is the ambiguity PostgreSQL raises 42702 for and
// wadjet answers by binding the first (ADR-0012), and a class guessed there
// would move that answer.
func sortTermNamesAggregateItem(term string, sortChild *logical.Node) bool {
	term = strings.TrimSpace(term)
	if term == "" || sortChild == nil || sortChild.Type != logical.NodeProject {
		return false
	}
	isAgg, hits := false, 0
	for _, proj := range logical.VisibleProjections(sortChild.Projections) {
		if !strings.EqualFold(proj.Alias, term) &&
			!strings.EqualFold(projectionOutputName(proj), term) {
			continue
		}
		isAgg, hits = proj.IsAgg, hits+1
	}
	return hits == 1 && isAgg
}

// pinSortKeySlotsOverProducerOutput gives every sort key whose name the
// PRODUCING aggregate publishes twice the slot its CLASS names.
//
// It fires only where the producer's output is provably `[group keys…,
// aggregate outputs…]` — `aggregateEmittedSlots` refuses any stage carrying a
// projection, a union arm or no aggregate at all — because a slot read off a
// model that does not hold is worse than the name it replaces. Everywhere
// else the key keeps the name path it has always had.
func pinSortKeySlotsOverProducerOutput(stages []Stage, idx map[string]int, i int) {
	s := &stages[i]
	if len(s.SortKeys) == 0 || len(s.ProjectExprs) > 0 {
		return
	}
	// WHICH relation the key addresses: a stage that AGGREGATES and carries an
	// ordering sorts its OWN output, so its own model is the answer; a plain
	// sort stage reads its producer's. Asking this stage first is what keeps
	// the two from being confused where both are aggregate-family stages.
	names, classes, ok := aggregateEmittedSlots(s)
	if !ok {
		d, dep := idx[firstDep(s)]
		if !dep {
			return
		}
		names, classes, ok = aggregateEmittedSlots(&stages[d])
	}
	if !ok {
		return
	}
	dup := map[string]int{}
	for _, n := range names {
		dup[strings.ToLower(strings.TrimSpace(n))]++
	}
	seen := map[string]int{}
	for k := range s.SortKeys {
		key := &s.SortKeys[k]
		if key.SlotPos > 0 || key.Column == "" {
			continue
		}
		name := strings.ToLower(strings.TrimSpace(plansql.NormalizeIdentRef(key.Column)))
		if dup[name] < 2 {
			continue // one column answers to it; the name IS the address
		}
		cursor := name + "\x00key"
		if key.NamesAggregateOutput {
			cursor = name + "\x00agg"
		}
		nth, want := seen[cursor], -1
		for j, n := range names {
			if classes[j] != key.NamesAggregateOutput ||
				!strings.EqualFold(strings.TrimSpace(n), name) {
				continue
			}
			if nth == 0 {
				want = j
				break
			}
			nth--
		}
		if want < 0 {
			continue
		}
		seen[cursor]++
		key.SlotPos = want + 1
	}
}
