package physical

import "strings"

// Addressing an aggregate's outputs by POSITION from the projection above it.
//
// ADR-0026 §3a: a GROUP BY key and an AGGREGATE OUTPUT may carry the same
// published name — `SELECT COUNT(*) AS g, g AS x FROM t GROUP BY g` emits two
// columns called `g` — and every consumer that resolves a name through
// `batch.RecordBatch.ColumnIndex` gets the FIRST of them. The single-process
// projection already pins each item to the slot its own CLASS names
// (`buildProject`, #575); the DAG's fragment projection did not, so the CTE
// spelling of the same query answered the KEY's values under the aggregate's
// alias on both DAG arms while the single-process path answered the counts.
//
// The slot is decided here, from the producer's own output order — an
// aggregate emits `[group keys…, aggregate outputs…]` — and travels as
// `ProjectExprSpec.SourceSlot`, which the worker and the coordinator both turn
// into `exec.ProjectColumn.SourceIdx`.

// pinProjectSpecSlots pins each spec that reads a DUPLICATED name of the
// producer's output to the slot its class names, and reports how many it
// pinned.
//
// classOf answers, per spec index, whether the item is an AGGREGATE OUTPUT
// (true) or a group-key reference (false). The gather's OutputRenames already
// carry exactly that answer for the same list, resolved through however many
// wrappers stand between (renameIsAggregateOutput), which is why the caller
// passes them rather than re-deriving the class here.
func pinProjectSpecSlots(producer *Stage, specs []ProjectExprSpec, classOf func(int) (bool, bool)) int {
	if producer == nil || len(specs) == 0 {
		return 0
	}
	names, classes, ok := aggregateEmittedSlots(producer)
	if !ok {
		return 0
	}
	dup := map[string]int{}
	for _, n := range names {
		dup[strings.ToLower(strings.TrimSpace(n))]++
	}
	// Per (name, class) cursor, so two aggregate outputs of one name take the
	// first and the second aggregate slot in SELECT-list order — the same
	// walk buildProject makes over keySlotByName / aggSlotByName.
	seen := map[string]int{}
	pinned := 0
	for j := range specs {
		key := strings.ToLower(strings.TrimSpace(specs[j].Expr))
		if key == "" || dup[key] < 2 {
			continue
		}
		isAgg, known := classOf(j)
		if !known {
			continue
		}
		cursor := key + "\x00agg"
		if !isAgg {
			cursor = key + "\x00key"
		}
		nth, want := seen[cursor], -1
		for i, n := range names {
			if classes[i] != isAgg || !strings.EqualFold(strings.TrimSpace(n), key) {
				continue
			}
			if nth == 0 {
				want = i
				break
			}
			nth--
		}
		if want < 0 {
			continue
		}
		seen[cursor]++
		specs[j].SourceSlot = want
		specs[j].SourceSlotSet = true
		pinned++
	}
	return pinned
}

// aggregateEmittedSlots is an aggregate-family producer's output columns IN
// ORDER, with the class of each: `[group keys…, aggregate outputs…]` is what
// exec.HashAggregate emits and what the worker's fragment rebuilds.
//
// ok is false for any producer whose output is not that shape — a projection
// already on the stage renames it, grouping sets reorder it, a union or a scan
// emits something else entirely — because a slot read off a model that does
// not hold is worse than the name path it replaces.
func aggregateEmittedSlots(s *Stage) (names []string, isAgg []bool, ok bool) {
	if len(s.ProjectExprs) > 0 || len(s.UnionArms) > 0 {
		return nil, nil, false
	}
	keys, aggs := s.GroupByCols, s.AggSpecs
	if len(keys) == 0 && len(s.FusedAggGroupBy) > 0 {
		keys, aggs = s.FusedAggGroupBy, s.FusedAggSpecs
	}
	if len(aggs) == 0 {
		return nil, nil, false
	}
	for _, k := range keys {
		names = append(names, k)
		isAgg = append(isAgg, false)
	}
	for _, a := range aggs {
		names = append(names, a.OutputCol)
		isAgg = append(isAgg, true)
	}
	return names, isAgg, true
}
