package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// What an aggregate publishes its GROUP BY keys under, and how a consumer
// finds the one it means.
//
// One aggregate, two engines: the single-process pipeline materializes a
// derived key into a pre-aggregate projection column, and the stage DAG ships
// the key text to a worker that does the same. Both then have to answer the
// same question for every SELECT item, HAVING term and sort key above them —
// "which key IS this?" — and both used to answer it by comparing rendered
// text, each with its own idea of what counts as the same rendering. That is
// #720 (a HAVING that never matched), #723 (a SELECT item rebuilt as
// arithmetic over a column the aggregate does not emit, NULL on every row)
// and #725 (a delimited identifier shipped with its quotes).
//
// groupKeyOutputs is the single answer both paths read: one published NAME
// per key, and one plansql.ExprIdentity that resolves to it.

// groupKeyOut describes one GROUP BY key as the aggregate publishes it.
type groupKeyOut struct {
	// Name is the column the key's value is PUBLISHED under. For a bare
	// column of the aggregate's input that is the column itself; for a
	// derived key it is the key's canonical text, which is what both engines
	// name it, so the two aggregate output schemas agree and one predicate
	// spelled against the key is evaluable on either.
	Name string
	// Slot is the column the key is RESOLVED by against the aggregate's
	// INPUT. For a bare column it is the column itself; for a derived key it
	// is a hidden `__gb_expr_N`, which is what the value is materialized
	// into and what HashAggregate groups on.
	//
	// It differs from Name on purpose. Materializing a derived key under its
	// own canonical TEXT puts a planner-made column into the user's
	// namespace, and the two collide whenever the input already carries a
	// column spelled the same way: the single-process pre-aggregate
	// projection APPENDS and batch.RecordBatch.ColumnIndex answers with the
	// first exact match, so the INPUT column won and the query grouped by it;
	// the worker's projection NARROWS and the derived key won instead, so the
	// two engines disagreed. `__gb_expr_N` is in the reserved namespace
	// (reserved_slots.go), so no query can spell it and the collision is
	// impossible rather than merely unlikely (ADR-0026).
	Slot string
	// Identity is plansql.ExprIdentity of the key expression: the key a
	// consumer's own expression is looked up by.
	Identity string
	// Derived marks a key the aggregate's input does not already carry, so
	// one of the two paths has to materialize it.
	Derived bool
	// PublishedBelow marks a key that WOULD be derived — its expression is not
	// a column of the aggregate's input — except that an aggregate DIRECTLY
	// BELOW already publishes it, under this same Name. It is therefore not
	// materialized, and it is published under a text no column reference can
	// spell.
	//
	// The distinction matters to a consumer that has to know WHY a key is not
	// derived: `GROUP BY n1.n_name` is not derived because it IS a column, and
	// `SELECT DISTINCT g + 1 … GROUP BY g + 1` is not derived because the
	// aggregate below computed it. Only the second needs a published name a
	// stage cannot carry (#736).
	PublishedBelow bool
	// Literal marks a constant key the single-process path elides from the
	// key set and re-attaches afterwards; its Name is the synthetic one.
	Literal bool
}

// groupKeyOutputs describes an Aggregate node's GROUP BY keys. The rules are
// buildAggregate's own, stated once so aggregateOutputNames, the projection
// above the aggregate and the pre-aggregate projection cannot drift apart.
func groupKeyOutputs(agg *logical.Node) []groupKeyOut {
	if agg == nil || agg.Type != logical.NodeAggregate {
		return nil
	}
	var decls, emitted colDecls
	var below map[string]string
	if len(agg.Children) == 1 {
		decls = inputColDecls(agg.Children[0])
		// The names already in scope, for MINTING: a slot is only hidden if
		// nothing else answers to it, and a stored column named `__gb_expr_0`
		// is a legal column that must keep working. The reservation refuses
		// user-minted names at the query and DDL doors; minting skips what
		// is in scope regardless, so the two do not have to agree for the
		// slot to be safe.
		emitted = emittedColDecls(agg.Children[0])
		// The keys an aggregate DIRECTLY BELOW this one already publishes,
		// by identity. `SELECT DISTINCT g + 1 AS k … GROUP BY g + 1` lowers
		// to two aggregates keyed alike, and the outer one reads the inner
		// one's OUTPUT: the value is already a column there, under the key's
		// own name, while the recorded expression still says `g + 1` over a
		// `g` that is no longer in scope. Materializing it would evaluate
		// that `g` against a schema without one and collapse the table into
		// a single NULL group.
		//
		// Only an aggregate below counts. A derived table that merely has a
		// column SPELLED like the key carries a DIFFERENT value under that
		// name, and re-using it would group by the wrong column — which is
		// the collision the slot exists for.
		below = groupKeysPublishedBelow(agg.Children[0])
	}
	haveExprs := len(agg.GroupByExprs) == len(agg.GroupBy)
	// A literal key is elided only when a non-literal key remains: GROUP BY
	// over literals alone must still emit zero rows on empty input, which
	// one retained key preserves.
	nonLit := 0
	if haveExprs {
		for _, e := range agg.GroupByExprs {
			if e == nil {
				nonLit++
				continue
			}
			if _, isLit := e.(*plansql.Lit); !isLit {
				nonLit++
			}
		}
	}
	// ONE allocator for the whole aggregate, from the shared reserved-slot
	// API. A per-key NAMER is what let two derived keys land in the same
	// slot: each started at its own index, stepped past the names in SCOPE,
	// and neither knew what the other had already taken (ADR-0026).
	//
	// Seeded from BOTH scope views, because they answer different questions
	// and a slot has to clear both. inputColDecls says what a bare reference
	// can name and STOPS at a rename Project; emittedColDecls says what the
	// input really publishes, which is where a derived table's renames live.
	// A name visible to only one of them is still a name the slot must not
	// take.
	alloc := plansql.NewSlotAllocator()
	alloc.Seed(declNames(decls)...)
	alloc.Seed(declNames(emitted)...)
	out := make([]groupKeyOut, len(agg.GroupBy))
	for i, gb := range agg.GroupBy {
		k := groupKeyOut{Name: plansql.NormalizeIdentRef(strings.TrimSpace(gb))}
		k.Slot = k.Name
		var e plansql.Node
		if haveExprs {
			e = agg.GroupByExprs[i]
		}
		if e == nil {
			// No AST for this key: parse the recorded text so the identity
			// is still the canonical one. A text that does not parse keeps
			// its own spelling as its identity, which is what every site
			// compared before identities existed.
			if parsed, err := plansql.ParseExpression(gb); err == nil {
				k.Identity = plansql.ExprIdentity(parsed)
			} else {
				k.Identity = strings.ToLower(strings.TrimSpace(gb))
			}
			out[i] = k
			continue
		}
		k.Identity = plansql.ExprIdentity(e)
		if _, isLit := e.(*plansql.Lit); isLit && nonLit > 0 {
			k.Literal = true
			// From the SAME allocator as a derived key. A literal's
			// re-attached column is a hidden slot too, and minting it
			// outside the allocator is how it would collide with one.
			k.Name = allocGroupKeySlot(alloc)
			k.Slot = k.Name
		} else if plain := isPlainGroupKey(e, decls); !plain && below[k.Identity] == k.Name {
			// Not materialized, and not because it is a column: the aggregate
			// DIRECTLY BELOW already computed it and publishes it under this
			// name. Recorded so a consumer can tell the two reasons apart —
			// the DAG's stage carries ONE name and cannot express this one
			// (#736).
			k.PublishedBelow = true
		} else if !plain {
			k.Derived = true
			// The key is MATERIALIZED, so it needs a column of its own in
			// the aggregate's input — and the input may already carry one
			// under the name the key publishes under. `GROUP BY g + 1` over
			// a relation that also has a column called "g + 1" is two
			// different values wanting one name, and which one wins is an
			// accident of the operator: the single-process pre-aggregate
			// projection APPENDS and batch.RecordBatch.ColumnIndex answers
			// with the FIRST exact match, so the input column won and the
			// query grouped by it; the worker's projection NARROWS, so the
			// key won and the two engines disagreed.
			//
			// The name is taken away from the collision entirely: the value
			// goes into a hidden `__gb_expr_N`, which is in the reserved
			// namespace (reserved_slots.go) and therefore a name NO query
			// can spell, and the key is published under its own text by a
			// rename at the aggregate's output (ADR-0026).
			//
			// ALWAYS, not only where a collision is visible from here: the
			// aggregate's input schema at planning time does not carry a
			// derived table's renames, so "is this name contested" cannot be
			// answered where the decision is made — and a slot used only
			// sometimes is a slot that protects only sometimes.
			k.Slot = allocGroupKeySlot(alloc)
		}
		out[i] = k
	}
	return out
}

// allocGroupKeySlot takes the next group-key slot from a scope's allocator.
//
// An exhausted family (a scope holding a million `__gb_expr_N` names, which
// has no known SQL) returns the family's zeroth name rather than an empty
// string: every caller here is naming a column it is about to create, and a
// nameless column is a worse failure than a collision that cannot occur.
func allocGroupKeySlot(a *plansql.SlotAllocator) string {
	if name, ok := a.Next(plansql.SlotGroupKey); ok {
		return name
	}
	return plansql.SlotName(plansql.SlotGroupKey, 0)
}

// declNames lists the column names a colDecls describes, for seeding an
// allocator with the scope it must not collide with.
func declNames(d colDecls) []string {
	if len(d.types) == 0 {
		return nil
	}
	out := make([]string, 0, len(d.types))
	for name := range d.types {
		out = append(out, name)
	}
	return out
}

// groupKeysByIdentity indexes a STAGE's published output names by the
// identity of the expression each one names.
//
// The stage has only the names — it is a serialized plan, not an AST — so
// each is parsed back to recover its identity. A name that does not parse
// as an expression is indexed under itself, which is what a text comparison
// gave before identities existed.
func groupKeysByIdentity(names map[string]string) map[string]string {
	if len(names) == 0 {
		return nil
	}
	m := make(map[string]string, len(names))
	for _, real := range names {
		id := strings.ToLower(real)
		if parsed, err := plansql.ParseExpression(real); err == nil {
			id = plansql.ExprIdentity(parsed)
		}
		if _, taken := m[id]; taken {
			continue
		}
		m[id] = real
	}
	return m
}

// groupKeyByIdentity indexes the DERIVED keys of an aggregate by identity, so
// a SELECT item, a HAVING term or a sort key spelled any way at all resolves
// to the one column the aggregate publishes it under.
//
// Bare-column keys are deliberately absent: their value is published under
// the input column's own name and every consumer already reads it there. A
// mapping for them would only re-route a resolution that works, and the one
// shape it would repair — a SELECT item that spells the column in a different
// CASE — is broken with no GROUP BY in sight (`SELECT G FROM t`), so it
// belongs to the identifier-folding defect and not to this one.
func groupKeyByIdentity(agg *logical.Node) map[string]string {
	keys := groupKeyOutputs(agg)
	if len(keys) == 0 {
		return nil
	}
	m := make(map[string]string, len(keys))
	for _, k := range keys {
		// A key is in the map when a consumer above cannot simply NAME it:
		// a derived key (published under its expression's text), an elided
		// literal (published under its slot), and — the DISTINCT rewrite's
		// case — a key that needs no materialization because the aggregate
		// BELOW already computed it, but is still published under a text no
		// column reference can spell. Leaving that last one out re-parsed
		// `g + 1` as arithmetic over a `g` the aggregate does not emit and
		// answered NULL for every row (ADR-0026).
		if !k.Derived && !k.Literal && nameIsPlainColumn(k.Name) {
			continue
		}
		if k.Identity == "" {
			continue
		}
		if _, taken := m[k.Identity]; taken {
			continue // first wins; two keys of one identity are one column
		}
		m[k.Identity] = k.Name
	}
	return m
}

// aggregateUnderOutput finds the Aggregate the output projection reads, or nil
// when the plan's top is not a grouped query. Only the nodes that leave the
// aggregate's own columns visible are walked through: a Project, and the
// wrappers aggScopePreservingWrapper names. A join or a set operation below
// the top means the SELECT list is written over something else, and the walk
// declines rather than guessing.
func aggregateUnderOutput(root *logical.Node) *logical.Node {
	for n := root; n != nil; {
		switch {
		case n.Type == logical.NodeAggregate:
			return n
		case n.Type == logical.NodeProject, aggScopePreservingWrapper(n.Type):
		default:
			return nil
		}
		if len(n.Children) != 1 {
			return nil
		}
		n = n.Children[0]
	}
	return nil
}

// aggScopePreservingWrapper reports whether a node standing between an
// aggregate and the SELECT list above it leaves the aggregate's OWN output
// columns visible, under their own names.
//
// It is the same question `scopePreservingWrapper` (output_rename_resolve.go)
// asks about a relation's scope, asked about an aggregate's output schema.
//
// The list itself is `logical.AggScopePreservingWrapper`, and this is a
// delegation rather than a copy because the FOURTH reader —
// `logical.aggregateOverGroupRows`, which decides whether a predicate above a
// Project may be substituted below it — is in that package, and `physical`
// imports `logical` and not the other way round. #774 is what a copy costs:
// that reader stopped at a WINDOW with its own Filter-only list while ADR-0026
// §4 named three walks reading one, and a `WHERE` on a computed key above a
// window admitted no row at all on every arm.
//
// A WINDOW is on the list, and its omission is #737: `exec.Window` APPENDS its
// output to its input, so every column the aggregate published is still there
// under its own name. With the window stopping both walks, a SELECT item that
// IS a computed group key was compiled as ARITHMETIC over an input column the
// aggregate does not emit, and `SELECT g + 1 AS k, ROW_NUMBER() OVER (…) FROM
// t GROUP BY g + 1` answered the right eight rows with a NULL key on all five
// arms.
func aggScopePreservingWrapper(t logical.NodeType) bool {
	return logical.AggScopePreservingWrapper(t)
}

// publishedGroupKeyNames is the name list exec.HashAggregate publishes its
// group keys under, aligned with the key set AFTER the literal-elided entries
// are compacted out. An empty entry means "keep exec's own rule"; only a
// DERIVED key names itself, because only a derived key resolves by a slot
// whose name is not the one the query means.
//
// derived reports whether any entry is set; the caller leaves GroupByOutNames
// nil otherwise, so a plan with no derived key is byte-identical to one made
// before slots existed.
func publishedGroupKeyNames(keys []groupKeyOut, elided map[int]bool) (names []string, derived bool) {
	names = make([]string, 0, len(keys))
	for i, k := range keys {
		if elided[i] {
			continue
		}
		if k.Derived {
			names = append(names, k.Name)
			derived = true
			continue
		}
		names = append(names, "")
	}
	return names, derived
}

// has reports whether the input declares a column of this exact name.
//
// Used to decide that a GROUP BY key needs no materialization: the value is
// already there, under the name the key is published under.
func (d colDecls) has(name string) bool {
	if name == "" || d.types == nil {
		return false
	}
	_, ok := d.types[strings.ToLower(strings.TrimSpace(name))]
	return ok
}

// groupKeysPublishedBelow indexes, by identity, the group keys an Aggregate
// directly below n already publishes and the name it publishes each under.
// Empty when there is no such aggregate — a scan, a join, a set operation and
// a derived table all answer "nothing is already computed for you here".
//
// The walk descends only through nodes that pass an aggregate's own output
// rows through unchanged.
func groupKeysPublishedBelow(n *logical.Node) map[string]string {
	for n != nil {
		switch n.Type {
		case logical.NodeAggregate:
			out := map[string]string{}
			for _, k := range groupKeyOutputs(n) {
				if k.Identity != "" && !k.Literal {
					out[k.Identity] = k.Name
				}
			}
			return out
		case logical.NodeProject:
			// Only a projection that PRESERVES the aggregate's outputs: any
			// other one renames or computes, and then the name below is not
			// the name here.
			if !n.PreservesAggOutputs {
				return nil
			}
		default:
			// The THIRD walk asking the same question, and it read its own
			// hardcoded list until 2026-09-01 while §4's invariant said "both
			// walks read one list, so they cannot disagree" — there were
			// three. A WINDOW was the entry it was missing, and the shape is
			// #737's under a DISTINCT: `SELECT DISTINCT g + 1 AS k, ROW_NUMBER()
			// OVER (…) FROM t GROUP BY g + 1` lowers to two aggregates keyed
			// alike with the window between them, the outer one did not see
			// that the inner already publishes the key, materialized it again
			// over a schema with no `g`, and collapsed the table into one NULL
			// group on the single-process path.
			if !aggScopePreservingWrapper(n.Type) {
				return nil
			}
		}
		if len(n.Children) != 1 {
			return nil
		}
		n = n.Children[0]
	}
	return nil
}
