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

// sortKeyLocalColumn is the spelling the single-process Sort resolves an ORDER
// BY term by: THE ONE THE QUERY WROTE, qualifier and all (#989).
//
// The qualifier is the only thing that distinguishes one reference's column
// from another's when the two share a bare name, and a Sort over a join reads
// exactly that stream: `exec.joinOutputSchemaWithMapping` publishes the probe's
// columns bare and qualifies every DUPLICATE build column by its owning alias,
// so `SELECT * FROM q a JOIN q b …` publishes `[order_id amount b.order_id
// b.amount]`. Stripping the qualifier here — which is what `cleanExpr` does —
// made `a.amount` and `b.amount` into ONE key, `amount`, and
// `exec.columnIndexFallback` bound both of them to the first column carrying
// it. `ORDER BY a.order_id, a.amount, b.amount` is a TOTAL order, so exactly
// one sequence is legal (ADR-0013 lists no class this falls under), and the
// single-process and spilled arms answered the trailing key INVERTED inside
// every peer group while both DAG arms — whose sort keys keep the qualified
// spelling — answered PostgreSQL's order (#989).
//
// This is ADR-0026 §6 at the ORDER BY consumer: a consumer binds through the
// identity its PRODUCER published, and never by re-reading a name as
// structure (§2c). It needs no model of which side of the join built, because
// `columnIndexFallback` tries the qualified spelling FIRST and falls back to
// the bare one — so `b.amount` binds `b.amount` when the join qualified b, and
// binds the bare `amount` when the join qualified a instead. The name-based
// resolution the key had before is the second step of the same resolver, so
// every term that resolved before still resolves to the same column; the only
// answer that moves is the one where the stream really does carry the
// qualified column the term names.
//
// #905 gave the same family its POSITION where the Sort's child is a Project
// (`sortKeyLocalSlotPos`), and that address still wins: a star-only query has
// no Project to take a position from, which is why the qualified spelling is
// the address left.
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
