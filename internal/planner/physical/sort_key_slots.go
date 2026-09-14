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
	items := logical.VisibleProjections(sortChild.Projections)
	match, hits := -1, 0
	for i, proj := range items {
		if !strings.EqualFold(proj.Alias, term) &&
			!strings.EqualFold(projectionOutputName(proj), term) {
			continue
		}
		match, hits = i, hits+1
	}
	if hits == 0 {
		// A term written the way the ITEM wrote it — `ORDER BY x.product`
		// over `SELECT x.product` — answers to no alias and to no published
		// name, because the published name is the bare `product`. It still
		// names exactly one item, and the class of that item is the question.
		for i, proj := range items {
			if !strings.EqualFold(strings.TrimSpace(proj.Expr), term) {
				continue
			}
			match, hits = i, hits+1
		}
	}
	if hits != 1 || match < 0 {
		return false
	}
	if items[match].IsAgg {
		return true
	}
	// THE CLASS OF WHAT THE ITEM REFERS TO, not of the item itself — the same
	// question `extractOutputRenames` asks for the gather's pairing, asked the
	// same way (#785 round 2). One derived table makes an aggregate output a
	// plain column reference here (`SELECT x.product` over `SELECT COUNT(*) AS
	// product … GROUP BY product`), and the aggregate below publishes its KEY
	// under that same name: without the class the sort took the first column
	// of the name, which is the key, and the rows came back in the key's order
	// where PostgreSQL orders by the count (#1095).
	if len(sortChild.Children) != 1 {
		return false
	}
	src := items[match].Column
	if src == "" {
		src = strings.ToLower(strings.TrimSpace(items[match].Expr))
	}
	if src == "" {
		return false
	}
	return renameIsAggregateOutput(src, sortChild.Children[0])
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
		// A WINDOW between the sort and the aggregate APPENDS its outputs to
		// its input, so the input's slots are unchanged and the model one
		// stage further down still holds. Without this step
		// `SELECT x.a AS b, SUM(x.b) AS a, RANK() OVER (…) … ORDER BY a`
		// came back in the group KEY's order on both DAG arms — the rows were
		// right and the sequence was not (#968).
		if stages[d].Type == StageWindow && len(stages[d].ProjectExprs) == 0 {
			if w, hasDep := idx[firstDep(&stages[d])]; hasDep {
				d = w
			}
		}
		names, classes, ok = aggregateEmittedSlots(&stages[d])
	}
	if !ok {
		names, classes, ok = joinProbeAggregateSlots(stages, idx, s)
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
		if dup[name] == 0 {
			// THE SAME QUALIFIED→BARE FALLBACK the key is RESOLVED by. A key
			// written through the block's alias (`x.product`) reaches the
			// stream as a name no column carries exactly, and
			// `exec.ColumnIndexFallback` then binds the bare `product` — so
			// the slot has to be looked for under the name that will actually
			// bind, or a collision the producer really publishes is invisible
			// here (#1095). Only when the exact spelling matches NOTHING:
			// where it matches, it is the address.
			if dot := strings.LastIndexByte(name, '.'); dot > 0 && dot < len(name)-1 {
				name = name[dot+1:]
			}
		}
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

// joinProbeAggregateSlots is the producer model for an ordering FUSED ONTO A
// JOIN whose PROBE side is the aggregate that publishes one name twice.
//
// A join emits its PROBE's columns first and unchanged — `joinOutputSchema-
// WithMapping`'s own order, which `joinStreamColumns` models — so a slot in
// the probe's output is the same slot in the join's. That is the whole claim,
// and it is MEASURED rather than assumed: the probe's model must be the
// leading prefix of this stage's own output stream, name for name in order, or
// the key keeps the name path it has always had. A stage that drops a
// materialized join column (`HiddenJoinCols`) shifts those positions and is
// declined outright.
//
// Without it `SELECT x.product, o.id FROM (SELECT COUNT(*) AS product FROM
// lat_item GROUP BY product) x JOIN lat_ord o ON true ORDER BY x.product,
// o.id` sorted by the group KEY on the three DAG arms — the product names —
// while the projection read the count, so the rows were right and the sequence
// was PostgreSQL's ordered by a column the client never sees (#1095).
func joinProbeAggregateSlots(stages []Stage, idx map[string]int, s *Stage) ([]string, []bool, bool) {
	if s == nil || !isJoinStage(s.Type) || len(s.HiddenJoinCols) > 0 {
		return nil, nil, false
	}
	d, ok := idx[s.LeftDepStage]
	if !ok {
		return nil, nil, false
	}
	names, classes, ok := aggregateEmittedSlots(&stages[d])
	if !ok {
		return nil, nil, false
	}
	out := stageStreamColumns(stages, idx, s, passThroughDepth)
	if len(out) < len(names) {
		return nil, nil, false
	}
	for i, n := range names {
		if !strings.EqualFold(strings.TrimSpace(out[i].Name), strings.TrimSpace(n)) {
			return nil, nil, false
		}
	}
	return names, classes, true
}
