package physical

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// A join's exchange carries every column the join stage will EVALUATE.
//
// A join stage's `Columns` is an OutputFilter and its input exchanges'
// `Columns` are payload manifests: both NARROW what arrives (ADR-0025, "A
// stage's Columns list is a FILTER, not a promise"). They are computed from
// the join node's `NeededColumns` at stage emission — before
// `attachScanSelectProjections` decides that this join is where the outer
// SELECT list gets computed, and before `resolveFilterAliasSpelling` decides
// how a WHERE above the join is spelled. So a name that only those late
// passes introduce is absent from every list the payload is built from, and
// the shuffle drops the column the fragment is about to read:
//
//	SELECT p.w AS pw, q.w AS qw, r.w AS rw
//	  FROM (SELECT id, SUM(b) OVER () AS w FROM t) p
//	  JOIN (SELECT id, SUM(a) OVER () AS w FROM t) q ON p.id = q.id
//	  JOIN (SELECT id, MIN(a) OVER () AS w FROM t) r ON p.id = r.id
//
// The three window slots are what the join's projection reads (`pw=__win_0`,
// `qw=__win_1`, `rw=__win_2`) and the exchanges carried `[id r.id q.id p.id]`
// — `column "__win_0" does not exist in the input schema`, on a query
// PostgreSQL answers. The same gap is silent rather than loud whenever the
// missing name resolves to SOMETHING ELSE on the stream, which is the shape
// #700 was filed for: the exchange carried the CTE's alias while the
// predicate had been re-spelled to the base column, so the filter was UNKNOWN
// on every row and the query answered zero.
//
// The repair is to close the loop rather than to widen the payload
// everywhere: after the late passes have settled what each join stage
// evaluates, union those column references back into the join's own
// OutputFilter and into its input exchanges' manifests. Only a stage that
// really carries a filter or a projection is touched, so a plan with neither
// is byte-identical, and an already-empty list is left empty — for both kinds
// of list, empty means "carry everything" and narrowing it here would be the
// defect in the other direction.
//
// Widening cannot invent a column: a payload naming something an arm does not
// have is ignored (the manifest is applied per side and both sides already
// receive the union of the two, which is why the two-arm spelling of the
// shape above happened to work). The name-resolvability question — does
// anything at all produce this? — stays with the checks in carrier_assert.go,
// which run after this pass and refuse the plan when the answer is no.
func ensureJoinCarriesEvaluatedColumns(stages []Stage) {
	idx := make(map[string]int, len(stages))
	for i := range stages {
		idx[stages[i].ID] = i
	}
	for i := range stages {
		s := &stages[i]
		if !isJoinStage(s.Type) {
			continue
		}
		// Two classes, evaluated in two different places.
		//
		// A stage's OWN FilterExprs/ProjectExprs run against its INPUT, so a
		// name the input already supplies needs nothing done.
		//
		// A CHAINED join's residual filter runs INSIDE this fragment, after
		// the primary probe, against a stream this stage's own OutputFilter
		// has already narrowed. So its columns have to be in that list
		// whatever the input carries — which is the whole of the shape that
		// answered zero:
		//
		//	WITH c AS (SELECT id, a * 2 AS dv FROM t)
		//	SELECT COUNT(*) FROM c JOIN t x ON c.id = x.id JOIN t y ON c.id = y.id
		//	WHERE c.dv > 1
		//	-- PG 5 · single 5 · DAG broadcast 5 · DAG SHUFFLED 0
		//
		// A join CONDITION is deliberately NOT in either set: it is resolved
		// by the join's key machinery from both sides, never read off the
		// narrowed probe stream, and treating it as payload is what added
		// `s_nationkey` to Q05's customer/orders exchange and `n1.n_name`
		// to three Q07 manifests.
		ownRefs := exprColumnRefs(s.FilterExprs, projectExprTexts(s.ProjectExprs))
		chainRefs := probeSideChainRefs(stages, idx, s)
		// A join stage's OWN residual filter and projection are evaluated
		// against its OUTPUT view, which this same Columns list narrows — so
		// they belong in it whatever the input carries. That is the broadcast
		// half of the shape below: `join-4 FILTER=[(a * 2) > 1]` over
		// `COLS=[dv id ...]` with no `a`, on a probe that plainly has one.
		if len(ownRefs) > 0 && len(s.Columns) > 0 {
			s.Columns = unionColumnNames(s.Columns, ownRefs)
		}
		if len(chainRefs) > 0 {
			if len(s.Columns) > 0 {
				s.Columns = unionColumnNames(s.Columns, chainRefs)
			}
			for k := range s.ChainedJoins {
				if len(s.ChainedJoins[k].Columns) > 0 {
					s.ChainedJoins[k].Columns =
						unionColumnNames(s.ChainedJoins[k].Columns, chainRefs)
				}
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

// widenNarrowingStagesBelow unions refs into the OutputFilter of the stage at
// root and of every join or exchange reachable from it, so a name the root
// will evaluate is not dropped by a narrowing stage underneath.
//
// Only joins and exchanges are widened: their Columns is a FILTER or a payload
// manifest, and neither can invent a column (ADR-0025). A producer's list is
// its read set and is left alone — widening THAT would change what is scanned.
// An empty list already means "carry everything" and stays empty.
//
// And a ref is pushed into a subtree ONLY IF THAT SUBTREE CAN SUPPLY IT.
// Without that test the walk adds every referenced name to every intermediate
// stage, which is a real cost paid on every row of every task: the first cut
// of this pass widened 21 TPC-H stage lines across six queries — `s_nationkey`
// onto Q05's customer/orders branch, which has no supplier scan under it;
// `n1.n_name` and `n2.n_name` onto three Q07 exchanges, two STRING columns
// crossing the network twice more; `__scalar_0` onto four Q02 stages, a
// scalar-subquery placeholder no stage produces at all. None of those queries
// was ever wrong — the consumer already had a path to the value — so every one
// of those columns was a second carry of something already carried.
//
// Asking whether the subtree PRODUCES the name is the narrow question that
// keeps the chain shapes working and leaves TPC-H alone: the CTE's `a` really
// is produced by the scan under that branch, and `s_nationkey` really is not
// produced under Q05's join-4. It is the same weak "does anything here compute
// this" test dropUnbackedJoinColumns and assertJoinFiltersAreBacked already
// use, asked per subtree instead of per plan.
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
			// A scan's SHIPPED set is a narrowing too, and it is the one
			// narrowing below a join that this walk used to stop at.
			// `pruneScanOutputColumns` sets OutputColumns to the columns a
			// consumer DECLARED it wanted, and it runs before
			// attachScanSelectProjections resolves the outer SELECT list
			// back to a SOURCE column — so a column the scan reads for its
			// own pushed filter and does not ship is exactly what the
			// projection above the join then asks for:
			//
			//	SELECT x.w AS xw FROM (SELECT id, a AS w FROM t) x
			//	JOIN (SELECT id, b AS w FROM t) z ON x.id = z.id
			//	JOIN t u ON x.id = u.id WHERE x.w > 1
			//	-- scan-1 reads [a id] for `a > 1` and ships OUT=[id];
			//	--   join-8 projects `a AS xw`
			//	-- PostgreSQL 5 rows · single 5 · DAG broadcast 5
			//	-- DAG shuffled  ERROR column "a" does not exist (#766)
			//
			// The READ set is untouched — widening THAT would change what is
			// scanned — so only a name the scan already reads is added back
			// to what it ships. An empty OutputColumns already means "ship
			// everything" and stays empty.
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

// ensureJoinCarriesGatherOutputs is the same rule for the one consumer that
// reads a join's output without evaluating anything: the GATHER.
//
// `attachScanSelectProjections` declines a SELECT list that is nothing but
// renamed column references with no ordering that needs them materialized —
// correctly, because the gather's own `OutputRename` does that job. But the
// gather renames FROM a name, and the join's OutputFilter was narrowed to the
// join node's NeededColumns, which spells a derived window column as the
// alias the arms publish rather than as the slot the window stage emits. The
// name the gather asks for is then absent, `assertGatherOutputIsReachable`
// refuses the plan, and the query is answered by the local engine instead:
//
//	SELECT p.w AS pw, q.w AS qw, r.w AS rw, p.id FROM …three sibling window blocks…
//	-- the stage DAG computed no SELECT list for this shape … emitted:
//	--   [id p.id q.id r.id]
//
// Adding the gather's sources back to the join and to its exchanges is safe
// in both directions. A name no stage produces is removed again by
// `dropUnbackedJoinColumns` before the assert reads the set, so a genuinely
// unreachable SELECT list is still refused and still routed local — this pass
// can only rescue a name something really computes.
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

// assertJoinFiltersAreBacked refuses a plan whose JOIN stage carries a
// predicate naming a column NOTHING in the plan computes.
//
// `assertCarrierSchemaResolves` deliberately excludes join stages: a join's
// input is the qualified union of two sides with per-column origin rules only
// the executor resolves, and asserting over it produces false refusals. That
// exclusion is right, and it is also why one silent zero survives every gate
// in this file — the identical query without the join REFUSES, loudly and
// correctly:
//
//	WITH c AS (SELECT id, SUM(a) * 2 AS dv FROM t GROUP BY id)
//	SELECT COUNT(*) FROM c WHERE c.dv > 1
//	-- native-DAG: stage final_aggregate-1 filters on "c.dv > 1" and its
//	--   input carries no [c.dv]; input: [__agg_0 id]   -> routed local, ANSWERS 5
//
//	… the same CTE with `JOIN t x ON c.id = x.id` added
//	-- PostgreSQL 5 · single 5 · both DAG arms 0, in silence
//
// The cause is upstream and is not this check's to repair: `SUM(a) * 2 AS dv`
// over a DECIMAL aggregate is DECLINED by absorbAggregateOutputProjection,
// because AggSpec carries an OutputType but no (p,s) and a wrong DECIMAL
// declaration is worse than no projection (ADR-0024 item 2). The decline is
// correct; what is not correct is that nothing then computes `dv` and the
// query answers WITHOUT the predicate. The same shape over a FLOAT or BIGINT
// aggregate is not declined and answers correctly on every arm, which is what
// says the type is the trigger and the join is only what hides it.
//
// So this asks the WEAKER question — does ANY producing stage in the plan
// compute this name — which is the one `dropUnbackedJoinColumns` already asks
// and which is known not to refuse TPC-H Q02. It cannot see a name that
// resolves to the WRONG column, only one that resolves to nothing, and that is
// exactly the class that answers zero in silence. Movers and joins are
// excluded from the producing set for the same reason they are there: their
// column lists are the thing under suspicion.
//
// The refusal wraps ErrUnreachableGatherOutput, so the coordinator routes the
// query to its local engine and ANSWERS it — the same disposition its
// join-free spelling already had.
func assertJoinFiltersAreBacked(stages []Stage) error {
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
		for _, w := range stages[i].WindowCols {
			if w.OutputCol != "" {
				produced[strings.ToLower(w.OutputCol)] = w.OutputCol
			}
		}
	}
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
