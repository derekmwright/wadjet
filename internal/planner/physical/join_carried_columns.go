package physical

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// A join must carry every column it evaluates. After late projection/filter
// spelling passes, union references into its OutputFilter and input exchange
// manifests (ADR-0025, #700). Touch only stages carrying filters/projections;
// empty lists mean carry everything and must stay empty. Manifests apply per
// side and cannot invent absent columns. carrier_assert.go runs afterward
// and remains responsible for refusing unresolvable names.
// See docs/internals/join-evaluated-column-payloads.md for the design.
func ensureJoinCarriesEvaluatedColumns(stages []Stage) {
	idx := make(map[string]int, len(stages))
	for i := range stages {
		idx[stages[i].ID] = i
	}
	// What the plan really COMPUTES, movers and joins excluded — the same
	// set assertJoinFiltersAreBacked reads, for the same reason: a join's
	// Columns is an OutputFilter and an exchange's is a payload manifest, so
	// a dotted name appearing in one is no evidence that anything produces
	// it. Reading them made `c_row.b` look produced and the container
	// expansion below decline.
	computed := producedColumnsInPlan(stages)
	for i := range stages {
		s := &stages[i]
		if !isJoinStage(s.Type) {
			continue
		}
		// Own FilterExprs/ProjectExprs read stage INPUT; names already supplied there
		// need no widening. Chained residual filters run after the primary probe's
		// OutputFilter, so their columns must survive that list regardless of input.
		// Join CONDITIONS are in neither set: key machinery resolves them against
		// both sides, not against the narrowed probe stream.
		ownRefs := exprColumnRefs(s.FilterExprs, projectExprTexts(s.ProjectExprs))
		// Also carry references of GROUP BY keys computed above this join.
		// resolveStageGroupKeys uses the ARMS' supply; this pass must make the
		// OutputFilter carry it (ADR-0026 §2, #794). For ROW field paths, carry the
		// CONTAINER via rowContainersOf, not withRowContainers: the aggregate's
		// published dotted key falsely looks already produced to that helper, before
		// it has been evaluated. Missing containers can bind another arm's field-name
		// column or form one NULL group (#361, #769).
		ownRefs = append(ownRefs, rowContainersOf(groupKeyResolutionRefs(stages, idx, s), computed)...)
		// …and what an AGGREGATE ARGUMENT over this join reads. Same rule,
		// same reason: `MIN(c_row.b)` is materialized by a pre-aggregate
		// projection inside the fragment, so the CONTAINER has to survive the
		// join's OutputFilter. Only the field paths are added — an ordinary
		// argument names a column the payload already carries because the
		// aggregate reads it.
		ownRefs = append(ownRefs, rowContainerRefsOnly(aggregateInputRefs(stages, s), computed)...)
		chainRefs := probeSideChainRefs(stages, idx, s)
		// A ROW FIELD PATH names no column, so carrying `c_row.b` carries
		// nothing: what the fragment reads is the CONTAINER, and the
		// expression compiler resolves the field out of it (ADR-0022). The
		// join's OutputFilter is built from NeededColumns, which spells the
		// dotted form, so the container was narrowed away and every field
		// came back NULL. Expanded here, where the subtree's produced set is
		// already at hand, and only when the subtree really produces the
		// qualifier — an ordinary `x.id` has no column called `x` and never
		// reaches this.
		ownRefs = withRowContainers(ownRefs, computed)
		chainRefs = withRowContainers(chainRefs, computed)
		// A join stage's OWN residual filter and projection are evaluated
		// against its OUTPUT view, which this same Columns list narrows — so
		// they belong in it whatever the input carries. That is the broadcast
		// half of the shape below: `join-4 FILTER=[(a * 2) > 1]` over
		// `COLS=[dv id ...]` with no `a`, on a probe that plainly has one.
		if len(ownRefs) > 0 && len(s.Columns) > 0 {
			s.Columns = unionColumnNames(s.Columns, ownRefs)
		}
		if len(chainRefs) > 0 && len(s.Columns) > 0 {
			s.Columns = unionColumnNames(s.Columns, chainRefs)
		}
		// A chained link's Columns narrows the JOINED stream, so include both sides
		// of residuals evaluated at or after that link (#755, #762). Fragment order is
		// primary probe(s.Columns), link k(cj[k].Columns), its residual filter, later
		// links, then stage PostFilter. Excluding build-side refs is valid for the
		// primary s.Columns, where that build has not entered yet, but not for cj[k].
		// probeSideChainRefs deliberately omits those build refs; add them here.
		for k := range s.ChainedJoins {
			if len(s.ChainedJoins[k].Columns) == 0 {
				continue // empty already means "carry everything"
			}
			later := append([]string(nil), chainRefs...)
			for j := k; j < len(s.ChainedJoins); j++ {
				later = append(later, exprColumnRefs(s.ChainedJoins[j].FilterExprs)...)
			}
			// …and the stage's own PostFilter and projection, which run after
			// every link and read the stream each link's list has narrowed.
			later = append(later, ownRefs...)
			if len(later) > 0 {
				s.ChainedJoins[k].Columns = unionColumnNames(s.ChainedJoins[k].Columns, later)
			}
		}
		refs := append(append([]string(nil), ownRefs...), chainRefs...)
		if len(refs) == 0 {
			continue
		}
		// The column has to survive EVERY narrowing stage between this one
		// and whatever produces it, not just this one's own list. A join
		// below this one is an OutputFilter too, and it drops the column
		// just as effectively:
		//
		//	WITH c AS (SELECT id, a * 2 AS dv FROM t)
		//	SELECT COUNT(*) FROM t x JOIN c ON c.id = x.id JOIN t y ON c.id = y.id
		//	WHERE c.dv > 1
		//	-- PostgreSQL 5 · single 5 · DAG broadcast 5 · DAG SHUFFLED 0
		//
		// Here the re-spelled predicate `(a * 2) > 1` lands on the SECOND
		// join, and adding `a` to that stage and its own exchange is not
		// enough — the FIRST join, which is its probe, had already narrowed
		// `a` away. So the refs are pushed down the dependency graph through
		// every stage whose column list only NARROWS (joins and exchanges),
		// stopping at the producers, which is where the column comes from.
		widenNarrowingStagesBelow(stages, idx, i, refs)
	}
}

// groupKeyResolutionRefs lists the columns the GROUP BY keys computed over
// this join's output are RESOLVED by — the chain-terminal aggregate on the
// stage itself, and any aggregate stage that reads it.
//
// Only a COMPUTED resolution contributes references: a resolution that is a
// NAME is a column of the stream in its own right and is already in the
// payload for the reason the key is (§2c — a name is not re-read as structure,
// here as well as in the fragment).
func groupKeyResolutionRefs(stages []Stage, idx map[string]int, s *Stage) []string {
	var texts []string
	add := func(c *Stage) {
		if len(c.GroupByResolve) == 0 || !stageComputesGroupKeys(c) {
			return
		}
		for _, r := range c.GroupByResolve {
			if r.Computed && r.Expr != "" {
				texts = append(texts, r.Expr)
			}
		}
	}
	add(s)
	for i := range stages {
		c := &stages[i]
		if len(c.Dependencies) != 1 || c.Dependencies[0] != s.ID {
			continue
		}
		add(c)
	}
	if len(texts) == 0 {
		return nil
	}
	return exprColumnRefs(texts)
}

// widenNarrowingStagesBelow unions refs into root and reachable join/exchange
// OutputFilters/manifests, only where that subtree can supply the name.
// These lists narrow and cannot invent columns (ADR-0025); producer read sets
// stay unchanged and empty lists retain carry-everything semantics.
// Use the weak producing-stage test shared with dropUnbackedJoinColumns and
// assertJoinFiltersAreBacked, scoped per subtree, to avoid redundant payloads.
func widenNarrowingStagesBelow(stages []Stage, idx map[string]int, root int, refs []string) {
	produced := make(map[int]map[string]string, len(stages))
	seen := make(map[int]bool, 8)
	queue := []int{root}
	for len(queue) > 0 {
		i := queue[0]
		queue = queue[1:]
		if seen[i] {
			continue
		}
		seen[i] = true
		s := &stages[i]
		widens := isJoinStage(s.Type)
		switch s.Type {
		case StageExchangeRepartition, StageExchangeReplicate:
			widens = true
		}
		if !widens {
			// A scan's OutputColumns also narrows its shipped set before late alias
			// resolution can discover a source-column consumer (#766). Add back only
			// columns the scan ALREADY READS; never widen its read set. An empty
			// OutputColumns means ship everything and stays empty.
			if s.Type == StageScan && len(s.OutputColumns) > 0 {
				readable := make(map[string]string, len(s.Columns))
				for _, c := range s.Columns {
					readable[strings.ToLower(c)] = c
				}
				var back []string
				for _, r := range refs {
					if name, ok := readable[strings.ToLower(stripQualifier(r))]; ok {
						back = append(back, name)
					}
				}
				if len(back) > 0 {
					s.OutputColumns = unionColumnNames(s.OutputColumns, back)
				}
			}
			continue // a producer: its list is a read set, not a filter
		}
		keep := refsSubtreeCanSupply(stages, idx, i, refs, produced)
		if len(keep) == 0 {
			continue // nothing below here supplies any of them
		}
		if len(s.Columns) > 0 {
			s.Columns = unionColumnNames(s.Columns, keep)
		}
		// A chained join's OutputFilter is its own list, applied inside the
		// same fragment to the stream the chained probe then reads.
		for k := range s.ChainedJoins {
			if len(s.ChainedJoins[k].Columns) > 0 {
				s.ChainedJoins[k].Columns = unionColumnNames(s.ChainedJoins[k].Columns, keep)
			}
		}
		for _, dep := range s.Dependencies {
			if j, ok := idx[dep]; ok {
				queue = append(queue, j)
			}
		}
	}
}

// refsSubtreeCanSupply filters refs to the ones some stage at or below i
// really computes.
func refsSubtreeCanSupply(stages []Stage, idx map[string]int, i int, refs []string,
	memo map[int]map[string]string) []string {
	avail := subtreeProducedColumns(stages, idx, i, memo)
	if len(avail) == 0 {
		return nil
	}
	var keep []string
	for _, r := range refs {
		if columnResolves(&plansql.ColRef{Column: r}, avail) {
			keep = append(keep, r)
		}
	}
	return keep
}

// subtreeProducedColumns is every column name any stage at or below i emits,
// memoized per root. Movers and joins are included here — unlike in
// dropUnbackedJoinColumns, the question is not "is this name trustworthy" but
// "could a value by this name reach the top of this subtree at all".
func subtreeProducedColumns(stages []Stage, idx map[string]int, i int,
	memo map[int]map[string]string) map[string]string {
	if m, ok := memo[i]; ok {
		return m
	}
	out := map[string]string{}
	memo[i] = out // break cycles; a shared subplan can be reached twice
	var walk func(int, int)
	walk = func(j, depth int) {
		if depth > passThroughDepth*4 {
			return
		}
		s := &stages[j]
		// stageEmittedColumns and the computed fields only. A stage's
		// Columns is a FILTER or a payload manifest and can NEVER invent a
		// column (ADR-0025) — reading it as evidence of production is the
		// very mistake dropUnbackedJoinColumns exists to undo, and here it
		// let `s_nationkey` look available under a lineitem-only branch of
		// Q05, because the two sides of one shuffle share a single manifest.
		for k, v := range stageEmittedColumns(s) {
			out[k] = v
		}
		// A SCAN's own read set is the one column list that IS evidence of
		// production: it names columns the fragment really reads off the
		// table. `pruneScanOutputColumns` may have narrowed what it SHIPS
		// (OutputColumns), and this walk's caller can put such a column back
		// — so "can this subtree supply the name" has to be asked of what the
		// scan reads, not of what the prune left. Without it the pass declines
		// at the join above and then widens a scan nothing downstream carries.
		if s.Type == StageScan && len(s.OutputColumns) > 0 {
			for _, c := range s.Columns {
				if c == "" || strings.EqualFold(c, logical.RowCountOnlyColumn) {
					continue
				}
				out[strings.ToLower(c)] = c
			}
		}
		for _, w := range s.WindowCols {
			if w.OutputCol != "" {
				out[strings.ToLower(w.OutputCol)] = w.OutputCol
			}
		}
		for _, p := range s.ProjectExprs {
			if p.Name != "" {
				out[strings.ToLower(p.Name)] = p.Name
			}
		}
		for _, dep := range s.Dependencies {
			if k, ok := idx[dep]; ok {
				walk(k, depth+1)
			}
		}
	}
	walk(i, 0)
	memo[i] = out
	return out
}

// ensureJoinCarriesGatherOutputs restores gather OutputRename SOURCES to a
// join's OutputFilter and exchanges when no SELECT projection materializes
// them. NeededColumns may carry an alias instead of the window's emitted slot.
// dropUnbackedJoinColumns removes unproduced names before
// assertGatherOutputIsReachable, so unreachable SELECT lists still refuse
// and route local; widening rescues only values something actually computes.
func ensureJoinCarriesGatherOutputs(stages []Stage) {
	idx := make(map[string]int, len(stages))
	for i := range stages {
		idx[stages[i].ID] = i
	}
	for i := range stages {
		g := &stages[i]
		if g.Type != StageExchangeGather || len(g.OutputRenames) == 0 || len(g.Dependencies) != 1 {
			continue
		}
		var want []string
		for _, r := range g.OutputRenames {
			if r.From != "" {
				want = append(want, r.From)
			}
		}
		if len(want) == 0 {
			continue
		}
		// Walk down the pass-through chain to the producing join, exactly as
		// the reachability check does when it reads the emitted set.
		j, ok := idx[g.Dependencies[0]]
		if !ok {
			continue
		}
		s := &stages[j]
		for depth := 0; depth < passThroughDepth && !isJoinStage(s.Type); depth++ {
			if !forwardsInputColumns(s.Type) || len(s.Dependencies) != 1 {
				break
			}
			k, ok := idx[s.Dependencies[0]]
			if !ok {
				break
			}
			s = &stages[k]
		}
		if !isJoinStage(s.Type) || len(s.Columns) == 0 {
			continue
		}
		s.Columns = unionColumnNames(s.Columns, want)
		for _, dep := range s.Dependencies {
			k, ok := idx[dep]
			if !ok {
				continue
			}
			d := &stages[k]
			switch d.Type {
			case StageExchangeRepartition, StageExchangeReplicate:
			default:
				continue
			}
			if len(d.Columns) == 0 {
				continue
			}
			d.Columns = unionColumnNames(d.Columns, want)
		}
	}
}

// probeSideChainRefs is the columns a chained join's residual filter reads
// FROM THE PROBE STREAM — the ones this stage's own OutputFilter can drop
// before the chained probe ever runs.
//
// A chained link joins a new build side in, and its filter usually reads
// columns from BOTH. The build side's arrive with that link's own input and
// are unaffected by anything this stage narrows; only the probe side's have to
// survive `s.Columns`. Treating all of them as probe-side put `s_nationkey`
// on Q05's join and `n1.n_name` on Q07's, on queries that were already right.
func probeSideChainRefs(stages []Stage, idx map[string]int, s *Stage) []string {
	var out []string
	seen := map[string]bool{}
	for _, cj := range s.ChainedJoins {
		refs := exprColumnRefs(cj.FilterExprs)
		if len(refs) == 0 {
			continue
		}
		// The build dep is usually an EXCHANGE, whose own emitted set is not
		// modelled — so the question has to be asked of its subtree, or a
		// mover reads as supplying nothing and every build-side reference
		// looks probe-side. That is what kept Q07's two qualified nation
		// names on join-12.
		// What the build STREAM carries, which is its declared manifest when
		// it has one — not what its TABLE has. A self-join makes the two
		// disagree completely: the chained build of `c JOIN t x JOIN t y` is
		// the same relation as the probe, so every column resolves in its
		// subtree while its payload list carries only the join key. An empty
		// manifest is the one case that really does mean "everything", and
		// only then is the subtree the right question (a replicated build,
		// which is how Q07 reaches its nation names).
		build := map[string]string{}
		if j, ok := idx[cj.BuildDepStage]; ok {
			d := &stages[j]
			if len(d.Columns) > 0 {
				for _, c := range d.Columns {
					build[strings.ToLower(c)] = c
				}
			} else {
				memo := map[int]map[string]string{}
				build = subtreeProducedColumns(stages, idx, j, memo)
			}
		}
		for _, r := range refs {
			if len(build) > 0 && columnResolves(&plansql.ColRef{Column: r}, build) {
				continue // arrives with the chained link's own build input
			}
			if lc := strings.ToLower(r); !seen[lc] {
				seen[lc] = true
				out = append(out, r)
			}
		}
	}
	return out
}

// exprColumnRefs lists the column names a set of expression TEXTS reads.
//
// A name the FRAGMENT computes for itself is not one the payload owes: a
// materialized window key is written by the window stage below, and a scalar
// placeholder is substituted at dispatch. Both are skipped for the same
// reason columnResolves skips them.
func exprColumnRefs(groups ...[]string) []string {
	var out []string
	seen := map[string]bool{}
	for _, g := range groups {
		for _, text := range g {
			if strings.TrimSpace(text) == "" {
				continue
			}
			ast, err := plansql.ParseExpression(text)
			if err != nil {
				continue
			}
			for _, ref := range collectColRefs(ast) {
				if strings.HasPrefix(ref.Column, windowKeyColPrefix) ||
					strings.HasPrefix(ref.Column, ":") {
					continue
				}
				name := ref.String()
				if lc := strings.ToLower(name); !seen[lc] {
					seen[lc] = true
					out = append(out, name)
				}
			}
		}
	}
	return out
}

// projectExprTexts is the expression half of a stage's ProjectExprs.
func projectExprTexts(specs []ProjectExprSpec) []string {
	out := make([]string, 0, len(specs))
	for _, p := range specs {
		out = append(out, p.Expr)
	}
	return out
}

// chainedFilterTexts is every RESIDUAL filter a chained or fused join runs
// inside this stage's fragment. Join CONDITIONS are excluded — see the note in
// ensureJoinCarriesEvaluatedColumns.
func chainedFilterTexts(s *Stage) []string {
	var out []string
	for _, cj := range s.ChainedJoins {
		out = append(out, cj.FilterExprs...)
		// NOT BuildFilterExprs: those filter the chained join's BUILD input
		// before its hash table is built, so their columns come from the
		// build dependency and never from this stage's narrowed probe
		// output. Treating them as probe payload put `__subsume_f0` on
		// Q21's join and `s_nationkey` on Q05's.
	}
	for _, fj := range s.FusedJoins {
		out = append(out, fj.FilterExprs...)
	}
	return out
}

// withRowContainers appends, for every dotted reference the produced set does
// NOT have under its dotted spelling but DOES have under its qualifier, that
// qualifier — the ROW column a field path actually reads. See ADR-0022 and
// the scan sanitizer's own arm ("what the scan must read is the BASE
// column"), which makes the same substitution one level down.
// aggregateInputRefs lists the column references the AGGREGATE ARGUMENTS
// computed over this join's output read — the specs on the stage itself and on
// any stage that reads it, the same two places groupKeyResolutionRefs looks.
func aggregateInputRefs(stages []Stage, s *Stage) []string {
	var texts []string
	add := func(c *Stage) {
		specs := append(append([]AggSpec(nil), c.AggSpecs...), c.FusedAggSpecs...)
		specs = append(specs, c.ChainedAggSpecs...)
		for _, a := range specs {
			read := a.InputExpr
			if read == "" {
				read = a.InputCol
			}
			if read != "" && read != "*" {
				texts = append(texts, read)
			}
		}
	}
	add(s)
	for i := range stages {
		c := &stages[i]
		if len(c.Dependencies) == 1 && c.Dependencies[0] == s.ID {
			add(c)
		}
	}
	if len(texts) == 0 {
		return nil
	}
	return exprColumnRefs(texts)
}

// rowContainerRefsOnly is rowContainersOf keeping ONLY the containers it adds.
// The references themselves are already in the payload wherever they name a
// column; adding them back would widen a manifest with dotted names no file
// carries, which is what ADR-0025 calls a phantom.
func rowContainerRefsOnly(refs []string, avail map[string]string) []string {
	expanded := rowContainersOf(refs, avail)
	if len(expanded) == len(refs) {
		return nil
	}
	return expanded[len(refs):]
}

// rowContainersOf returns the CONTAINERS of the field paths among refs, plus
// refs themselves. It is withRowContainers without the "already produced"
// escape: a group-key resolution is a spelling the fragment will EVALUATE, so
// a stage publishing that same name is the computation's OUTPUT and says
// nothing about what its INPUT must carry.
func rowContainersOf(refs []string, avail map[string]string) []string {
	if len(refs) == 0 || len(avail) == 0 {
		return refs
	}
	out := refs
	seen := make(map[string]bool, len(refs))
	for _, r := range refs {
		seen[strings.ToLower(r)] = true
	}
	for _, r := range refs {
		dot := strings.IndexByte(r, '.')
		if dot <= 0 || dot == len(r)-1 {
			continue
		}
		qual := strings.ToLower(r[:dot])
		name, ok := avail[qual]
		if !ok || seen[qual] {
			continue
		}
		seen[qual] = true
		out = append(append([]string(nil), out...), name)
	}
	return out
}

func withRowContainers(refs []string, avail map[string]string) []string {
	if len(refs) == 0 || len(avail) == 0 {
		return refs
	}
	out := refs
	seen := make(map[string]bool, len(refs))
	for _, r := range refs {
		seen[strings.ToLower(r)] = true
	}
	for _, r := range refs {
		dot := strings.IndexByte(r, '.')
		if dot <= 0 || dot == len(r)-1 {
			continue
		}
		if _, ok := avail[strings.ToLower(r)]; ok {
			continue
		}
		qual := r[:dot]
		name, ok := avail[strings.ToLower(qual)]
		if !ok || seen[strings.ToLower(qual)] {
			continue
		}
		seen[strings.ToLower(qual)] = true
		out = append(append([]string(nil), out...), name)
	}
	return out
}

// unionColumnNames appends the entries of add that base does not already
// carry, case-insensitively, preserving base's order so an untouched plan
// keeps its column order exactly.
func unionColumnNames(base, add []string) []string {
	seen := make(map[string]bool, len(base)+len(add))
	for _, c := range base {
		seen[strings.ToLower(c)] = true
	}
	out := base
	for _, c := range add {
		lc := strings.ToLower(c)
		if seen[lc] {
			continue
		}
		seen[lc] = true
		out = append(append([]string(nil), out...), c)
	}
	return out
}

// producedColumnsInPlan records names computed by producing stages; movers and
// joins are excluded because payload manifests and OutputFilters cannot invent columns.
// assertJoinFiltersAreBacked refuses JOIN predicates naming no producer anywhere,
// wrapping ErrUnreachableGatherOutput so the coordinator answers locally.
// This weaker check cannot detect a reference bound to the wrong column;
// assertCarrierSchemaResolves excludes joins because only the executor resolves
// per-column origins in their qualified input union. DECIMAL projections must
// not invent (p,s) when AggSpec carries only OutputType (ADR-0024 item 2).
// See docs/internals/join-filter-production-check.md for the design.
func producedColumnsInPlan(stages []Stage) map[string]string {
	produced := map[string]string{}
	for i := range stages {
		switch stages[i].Type {
		case StageExchangeRepartition, StageExchangeReplicate, StageExchangeGather,
			StageHashJoin, StageBroadcastJoin, StageSortMergeJoin:
			continue
		}
		for k, v := range stageEmittedColumns(&stages[i]) {
			produced[k] = v
		}
		// A scan's READ SET is production too: pruneScanOutputColumns may
		// have narrowed what it SHIPS, and widenNarrowingStagesBelow can put
		// such a column back.
		if stages[i].Type == StageScan && len(stages[i].OutputColumns) > 0 {
			for _, c := range stages[i].Columns {
				if c == "" || strings.EqualFold(c, logical.RowCountOnlyColumn) {
					continue
				}
				produced[strings.ToLower(c)] = c
			}
		}
		for _, w := range stages[i].WindowCols {
			if w.OutputCol != "" {
				produced[strings.ToLower(w.OutputCol)] = w.OutputCol
			}
		}
	}
	return produced
}

func assertJoinFiltersAreBacked(stages []Stage) error {
	produced := producedColumnsInPlan(stages)
	if len(produced) == 0 {
		return nil
	}
	for i := range stages {
		s := &stages[i]
		if !isJoinStage(s.Type) {
			continue
		}
		exprs := append([]string(nil), s.FilterExprs...)
		for _, cj := range s.ChainedJoins {
			exprs = append(exprs, cj.FilterExprs...)
		}
		for _, fj := range s.FusedJoins {
			exprs = append(exprs, fj.FilterExprs...)
		}
		kind := "filters on"
		// A join's PROJECTION is the same question one field over, and the
		// one #766 is: `attachScanSelectProjections` puts the outer SELECT
		// list on the join, spelled with the arm's alias, and no fragment
		// computes that alias when the arm is an AGGREGATE two Projects down.
		// The BROADCAST lowering of the identical query is already refused
		// here — its gather still renames from the alias, so
		// assertGatherOutputIsReachable sees it — and the coordinator answers
		// it locally. The shuffled lowering attaches the projection instead,
		// which satisfies that check and then fails inside the fragment:
		// `column "c.dv" does not exist in the input schema`, at DISPATCH, on
		// a query PostgreSQL answers. Asking the weak question of the
		// projection too makes the two lowerings agree, and agree on the
		// disposition that ANSWERS.
		projStart := len(exprs)
		for _, pe := range s.ProjectExprs {
			exprs = append(exprs, pe.Expr)
		}
		for k, e := range exprs {
			if k == projStart {
				kind = "projects"
			}
			if strings.TrimSpace(e) == "" {
				continue
			}
			ast, err := plansql.ParseExpression(e)
			if err != nil {
				continue
			}
			for _, ref := range collectColRefs(ast) {
				if strings.HasPrefix(ref.Column, windowKeyColPrefix) ||
					strings.HasPrefix(ref.Column, ":") {
					continue
				}
				if columnResolves(ref, produced) {
					continue
				}
				return fmt.Errorf("%w: stage %s (%s) %s %q and NO stage in the plan "+
					"computes %q — the predicate would be UNKNOWN on every row and the query "+
					"would answer WITHOUT it (#700)",
					ErrUnreachableGatherOutput, s.ID, s.Type, kind, e, ref.String())
			}
		}
	}
	return nil
}
