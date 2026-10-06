// SPDX-License-Identifier: MIT

package physical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// THE BINDER'S STAMP (ADR-0047, stage 1).
//
// The binder is the one layer that knows which relation a column reference
// names. Until this file it recorded none of it: colScope.resolveRef answered
// only "refuse or not", and every later layer re-derived the answer from the
// reference's SPELLING — which is how `t.n` and `n` in a block over `t` came
// to be two group keys (#1524) and `zzj.d92` and `zzp.d92` could have been
// one.
//
// Here the scope keeps, beside its name sets, each FROM source as a relation
// INSTANCE with its ordered column list, and the enclosing query level it sits
// under. bindRef resolves a reference against those by PostgreSQL's rules —
// the qualifier names one instance of the innermost level that has it; a bare
// name is the one column of the innermost level that provides it, ambiguous
// when two of that level's instances do — and stampBlock records the result
// on the ColRef (plansql.ColRef.Bound).
//
// A block is stamped IN FULL OR NOT AT ALL. A reference the binder cannot bind
// with certainty — an unenumerable source, a field path, a USING-merged bare
// name, a name no level provides — leaves every reference of its block
// unbound, so a consumer comparing two terms of one block never compares a
// bound term with an unbound one (ADR-0047 §6; RISKS R1: a bound term against
// an unbound copy of the same key never matches, and the consumer's "not a
// key" path is #738's wrong value).

// relInst is one relation instance of a block's FROM: the spelling it is
// referenced by and the columns it publishes, in its own order. An open
// instance is a source whose columns this binder cannot enumerate (a table
// function reading an input it may not open, a table the catalog could not
// answer for): its qualifier is known, its columns are not.
type relInst struct {
	id   plansql.RelID
	qual string
	cols []string
	open bool
}

// addInst registers one closed FROM source as a relation instance.
func (s *colScope) addInst(b *binder, qual string, cols []string) {
	if s == nil || b == nil {
		return
	}
	b.nextRel++
	s.insts = append(s.insts, &relInst{id: b.nextRel, qual: qual, cols: append([]string(nil), cols...)})
}

// addOpenInst registers a FROM source whose columns cannot be enumerated.
func (s *colScope) addOpenInst(b *binder, qual string) {
	if s == nil || b == nil {
		return
	}
	s.instOpen = true
	b.nextRel++
	s.insts = append(s.insts, &relInst{id: b.nextRel, qual: qual, open: true})
}

// bindCategory is why a reference did or did not bind (RISKS M1).
type bindCategory int

const (
	bindCertain    bindCategory = iota // an instance column of this block
	bindOuter                          // an instance column of an enclosing level
	bindOutput                         // a SELECT output item (ORDER BY / alias rules)
	bindOpen                           // a star output may provide it: not certain
	bindSourceOpen                     // an unenumerable FROM source may provide it
	bindField                          // a field path through a ROW column
	bindAmbiguous                      // two instances (a USING merge, an ambiguous name)
	bindUnresolved                     // no level provides it (a niladic, a system column)
	bindDotted                         // a name holding a dot: the plan's text carriers cannot name it (P3)
	numBindCategories
)

var bindCategoryNames = [numBindCategories]string{
	"certain", "outer", "output", "open", "sourceOpen", "field", "ambiguous", "unresolved", "dotted",
}

func (c bindCategory) String() string { return bindCategoryNames[c] }

func (c bindCategory) bound() bool { return c <= bindOutput }

// bindRef resolves ref against s and the levels it sits under, by
// PostgreSQL's order: a QUALIFIED reference names the relation of the
// innermost level that has one under that name, and only when no level has
// one is the qualifier read as a ROW column (a field path); a BARE reference
// is the column of the innermost level that provides the name, ambiguous when
// two of that level's relations do.
func bindRef(s *colScope, ref *plansql.ColRef) (plansql.Binding, bindCategory) {
	if ref.Table == "" && pgSystemColumns[strings.ToLower(ref.Column)] {
		return plansql.Binding{}, bindUnresolved
	}
	if strings.Contains(ref.Table, ".") || strings.Contains(ref.Column, ".") {
		// `"a.b".n`, `t."x.y"`: the binder can resolve it, but the plan
		// carries a key and the term that matches it as TEXT past this
		// point, and a dotted name re-read from text is a qualified one —
		// the computed key over `"a.b".n` is wrong at base even when both
		// sides are spelled alike (GK review B2/P3). Binding the block would
		// route a mixed spelling the base refused into that defect, so the
		// block keeps the spelling rules until the carriers hold the name.
		return plansql.Binding{}, bindDotted
	}
	level := 0
	for sc := s; sc != nil; sc = sc.up {
		var b plansql.Binding
		var cat bindCategory
		var done bool
		if ref.Table != "" {
			b, cat, done = sc.bindQualified(ref)
		} else {
			b, cat, done = sc.bindBare(ref)
		}
		if done {
			if cat == bindCertain && level > 0 {
				cat = bindOuter
			}
			b.Level = level
			return b, cat
		}
		level++
	}
	if ref.Table != "" {
		// No relation of any level answers to the qualifier: a field path
		// when a level provides a column of that name.
		for sc := s; sc != nil; sc = sc.up {
			for _, in := range sc.insts {
				if !in.open && ordOf(in.cols, ref.Table) != -1 {
					return plansql.Binding{}, bindField
				}
			}
			if sc.instOpen {
				return plansql.Binding{}, bindSourceOpen
			}
		}
	}
	return plansql.Binding{}, bindUnresolved
}

// bindQualified resolves a qualified reference against this level's own
// relations. done is false when no relation of this level has the qualifier.
func (s *colScope) bindQualified(ref *plansql.ColRef) (plansql.Binding, bindCategory, bool) {
	var hits []*relInst
	for _, in := range s.insts {
		if strings.EqualFold(in.qual, ref.Table) {
			hits = append(hits, in)
		}
	}
	if len(hits) > 1 {
		// Two relations fold to one qualifier (`FROM a t, b "T"`): the
		// byte-exact spelling decides, as PostgreSQL's does.
		var exact []*relInst
		for _, in := range hits {
			if in.qual == ref.Table {
				exact = append(exact, in)
			}
		}
		hits = exact
	}
	switch len(hits) {
	case 0:
		return plansql.Binding{}, 0, false
	case 1:
		if hits[0].open {
			return plansql.Binding{}, bindSourceOpen, true
		}
		switch ord := ordOf(hits[0].cols, ref.Column); ord {
		case -1:
			return plansql.Binding{}, bindUnresolved, true
		case -2:
			return plansql.Binding{}, bindAmbiguous, true
		default:
			return plansql.Binding{Rel: hits[0].id, Ord: ord}, bindCertain, true
		}
	}
	return plansql.Binding{}, bindAmbiguous, true
}

// bindBare resolves a bare reference against this level's own relations.
// done is false when no relation of this level provides the name.
func (s *colScope) bindBare(ref *plansql.ColRef) (plansql.Binding, bindCategory, bool) {
	var hit *relInst
	ord := -1
	for _, in := range s.insts {
		if in.open {
			continue
		}
		o := ordOf(in.cols, ref.Column)
		if o == -1 {
			continue
		}
		if hit != nil || o == -2 {
			return plansql.Binding{}, bindAmbiguous, true
		}
		hit, ord = in, o
	}
	if s.instOpen {
		// An unenumerable relation of this level may provide the name —
		// alone, or beside the one found, which would make it ambiguous.
		return plansql.Binding{}, bindSourceOpen, true
	}
	if hit == nil {
		return plansql.Binding{}, 0, false
	}
	return plansql.Binding{Rel: hit.id, Ord: ord}, bindCertain, true
}

// ordOf is the position of name in cols: -1 when absent, -2 when two columns
// carry it. A byte-exact spelling wins over a fold-equal one; otherwise the
// comparison folds case, which is the concession the binder's name sets make
// (a derived table's or a CTE's published names reach the binder folded, so
// a delimited reference to `"Ab"` can only be matched that way — #731's
// exact check is colScope.refuseDelimitedMiss's, made before binding).
func ordOf(cols []string, name string) int {
	exact, folded := -1, -1
	nExact, nFolded := 0, 0
	for i, c := range cols {
		if c == name {
			exact = i
			nExact++
		}
		if strings.EqualFold(c, name) {
			folded = i
			nFolded++
		}
	}
	switch {
	case nExact == 1:
		return exact
	case nExact > 1:
		return -2
	case nFolded == 1:
		return folded
	case nFolded > 1:
		return -2
	}
	return -1
}

// BlockBindingReport is one block's binding census, for RISKS M1. It is only
// built when BindingProbe is set.
type BlockBindingReport struct {
	Counts  [numBindCategories]int // per category, in bindCategoryNames order
	Stamped bool                   // every participant bound: the block carries the stamp
	// Mixed: some participants bound and some the SCOPE cannot bind (an
	// unenumerable source, a star output, a field path, an ambiguous or
	// unresolved name) — the block a partial stamp would have refused; under
	// the every-or-none rule it is unstamped and matched by spelling.
	Mixed bool
	// Declined: the binder could bind the block, but a CARRIER after it
	// cannot hold the binding — a dotted name the plan's text names re-read
	// as a qualified one, a FROM-less subquery the parser unfolded — so the
	// block is left unbound on purpose and matched by spelling.
	Declined bool
	// Unbound lists the participants that did not bind, "category:spelling".
	Unbound []string
	// Block is a short rendering of the block, for reading the census.
	Block string
	// Instances are the block's own FROM relations as the binder registered
	// them: the qualifier and the ordered column list (nil for an open one),
	// for RISKS M5 (the binder's list against the producing node's).
	Instances []BindingInstance
}

// BindingInstance is one relation instance of a BlockBindingReport.
type BindingInstance struct {
	Qual string
	Cols []string
	Open bool
}

// CategoryNames is the column order of BlockBindingReport.Counts.
func CategoryNames() []string { return bindCategoryNames[:] }

// BindingProbe, when set, receives every validated block's binding census.
// A measurement hook (RISKS M1); nil in production.
var BindingProbe func(BlockBindingReport)

// stampBlock binds every column reference of a block's participants — the
// SELECT items, GROUP BY, HAVING, QUALIFY and ORDER BY (window terms ride
// inside the items) — and records the bindings when, and only when, every one
// of them binds. resolve is the block's input scope (its FROM and the levels
// it sits under).
func (b *binder) stampBlock(info *plansql.SelectInfo, resolve *colScope) bool {
	if b.blockRel == nil {
		b.blockRel = map[*plansql.SelectInfo]plansql.RelID{}
	}
	b.nextRel++
	self := b.nextRel
	b.blockRel[info] = self

	type pending struct {
		ref *plansql.ColRef
		b   plansql.Binding
		cat bindCategory
	}
	var all []pending
	seen := map[*plansql.ColRef]bool{}
	input := func(n plansql.Node) {
		var refs []*plansql.ColRef
		walkExpr(n, &refs, nil, nil)
		for _, r := range refs {
			if seen[r] {
				continue
			}
			seen[r] = true
			bd, cat := bindRef(resolve, r)
			all = append(all, pending{r, bd, cat})
		}
	}
	outputs, starOut := blockOutputs(info)
	outputOrd := func(name string) int {
		ord := -1
		for i, o := range outputs {
			if strings.EqualFold(o, name) {
				if ord >= 0 {
					return -2
				}
				ord = i
			}
		}
		return ord
	}
	// The input-rule positions first, so a node a clause shares with a
	// SELECT item (an ordinal ORDER BY carries the item's own tree) keeps the
	// item's binding.
	for i := range info.Columns {
		col := &info.Columns[i]
		if col.Star {
			continue
		}
		input(col.ASTExpr)
		input(col.AggArgExpr)
		for _, a := range col.AggArgs {
			input(a)
		}
	}
	for _, gb := range info.GroupByExprs {
		input(gb)
	}
	// HAVING / QUALIFY read the input; a bare name nothing in the input
	// provides is this engine's output-alias concession there.
	aliasFallback := func(n plansql.Node) {
		var refs []*plansql.ColRef
		walkExpr(n, &refs, nil, nil)
		for _, r := range refs {
			if seen[r] {
				continue
			}
			seen[r] = true
			bd, cat := bindRef(resolve, r)
			if cat == bindUnresolved && r.Table == "" && !starOut {
				if o := outputOrd(r.Column); o >= 0 {
					bd, cat = plansql.Binding{Rel: self, Ord: o, Output: true}, bindOutput
				}
			}
			all = append(all, pending{r, bd, cat})
		}
	}
	aliasFallback(info.HavingExpr)
	aliasFallback(info.QualifyExpr)
	// ORDER BY: a term that is a bare name naming an OUTPUT column binds that
	// output (PostgreSQL resolves it there first); anything else reads the
	// input.
	for i := range info.OrderBy {
		ob := &info.OrderBy[i]
		if ob.Expr == nil {
			continue
		}
		if ref, ok := unparen(ob.Expr).(*plansql.ColRef); ok && ref.Table == "" && !seen[ref] {
			if o := outputOrd(ref.Column); o >= 0 && !starOut {
				seen[ref] = true
				all = append(all, pending{ref, plansql.Binding{Rel: self, Ord: o, Output: true}, bindOutput})
				continue
			}
			if starOut {
				// A star's outputs are its input columns; a bare name is
				// one of them or an alias the list wrote. Not certain.
				bd, cat := bindRef(resolve, ref)
				if cat.bound() {
					cat = bindOpen
				}
				seen[ref] = true
				all = append(all, pending{ref, bd, cat})
				continue
			}
		}
		aliasFallback(ob.Expr)
	}

	var rep BlockBindingReport
	every := true
	if holdsUnfoldedSubquery(info) {
		rep.Declined = true
		rep.Unbound = append(rep.Unbound, "unfolded:FROM-less subquery")
	}
	any, scopeMiss := false, false
	for _, p := range all {
		rep.Counts[p.cat]++
		switch {
		case p.cat.bound():
			any = true
		case p.cat == bindDotted:
			rep.Declined = true
			every = false
			rep.Unbound = append(rep.Unbound, p.cat.String()+":"+p.ref.String())
		default:
			scopeMiss = true
			every = false
			rep.Unbound = append(rep.Unbound, p.cat.String()+":"+p.ref.String())
		}
	}
	rep.Stamped = every && !rep.Declined && b.stamp
	rep.Mixed = any && scopeMiss
	// A binder that does not stamp leaves the AST exactly as it found it.
	// The embedded door validates a statement TWICE when a policy applies —
	// once at the door, which stamps, and again under the policy's schema
	// after the plan is built (auth.EnforcePlanPolicies) — and clearing the
	// first pass's bindings there would hand the physical planner unbound
	// terms of a block the logical builder matched by binding: the mixture
	// RISKS R1 names. The second pass's own grouping check reads the
	// bindings the first pass recorded, so both passes judge alike.
	if b.stamp {
		for _, p := range all {
			if rep.Stamped {
				bd := p.b
				p.ref.Bound = &bd
			} else {
				p.ref.Bound = nil
			}
		}
	}
	if BindingProbe != nil {
		var items []string
		for _, c := range info.Columns {
			items = append(items, c.Expr)
		}
		rep.Block = "SELECT " + strings.Join(items, ", ")
		for _, in := range resolve.insts {
			rep.Instances = append(rep.Instances, BindingInstance{Qual: in.qual, Cols: in.cols, Open: in.open})
		}
		if len(info.GroupBy) > 0 {
			rep.Block += " GROUP BY " + strings.Join(info.GroupBy, ", ")
		}
		BindingProbe(rep)
	}
	if rep.Stamped {
		return true
	}
	// A policy recheck retains the first validation's stamps.
	for _, p := range all {
		if p.ref.Bound != nil {
			return true
		}
	}
	return false
}

// unstamped runs fn with stamping off: the blocks it validates are checked as
// before and record no binding. It is how a body the planner plans from a
// re-parse of its TEXT — an expression subquery, a LATERAL body — keeps the
// spelling rules its plan is matched by, whatever door the statement came
// through.
func (b *binder) unstamped(fn func() error) error {
	saved := b.stamp
	b.stamp = false
	defer func() { b.stamp = saved }()
	return fn()
}

// holdsUnfoldedSubquery reports whether the parser rewrote a FROM-less scalar
// subquery of this block into the expression it stands for (the
// SelectColumn.UnfoldedFrom family). PostgreSQL judges such a term AS WRITTEN —
// inside a sublink only a plain grouped column licenses an outer reference
// (`SELECT (SELECT i + 1) … GROUP BY i + 1` is 42803 there) — and the written
// form is TEXT here, so the block keeps the spelling rules that read it
// (canonical.go's FROM-less residual; ADR-0047 stage 3 parses the body once).
func holdsUnfoldedSubquery(info *plansql.SelectInfo) bool {
	for i := range info.Columns {
		if info.Columns[i].UnfoldedFrom != "" {
			return true
		}
	}
	for _, o := range info.GroupBySubqueryOrigin {
		if o != "" {
			return true
		}
	}
	if info.HavingUnfoldedFrom != "" {
		return true
	}
	for i := range info.OrderBy {
		if info.OrderBy[i].UnfoldedFrom != "" {
			return true
		}
	}
	return false
}
