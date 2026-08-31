package physical

import (
	"fmt"
	"strings"

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
		refs := joinEvaluatedColumnRefs(s)
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
func widenNarrowingStagesBelow(stages []Stage, idx map[string]int, root int, refs []string) {
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
			continue // a producer: its list is a read set, not a filter
		}
		if len(s.Columns) > 0 {
			s.Columns = unionColumnNames(s.Columns, refs)
		}
		// A chained join's OutputFilter is its own list, applied inside the
		// same fragment to the stream the chained probe then reads.
		for k := range s.ChainedJoins {
			if len(s.ChainedJoins[k].Columns) > 0 {
				s.ChainedJoins[k].Columns = unionColumnNames(s.ChainedJoins[k].Columns, refs)
			}
		}
		for _, dep := range s.Dependencies {
			if j, ok := idx[dep]; ok {
				queue = append(queue, j)
			}
		}
	}
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

// joinEvaluatedColumnRefs lists the column names a join stage's own
// FilterExprs and ProjectExprs read.
//
// A name the FRAGMENT computes for itself is not one the payload owes: a
// materialized window key is written by the window stage below, and a scalar
// placeholder is substituted at dispatch. Both are skipped for the same
// reason columnResolves skips them.
func joinEvaluatedColumnRefs(s *Stage) []string {
	var out []string
	seen := map[string]bool{}
	add := func(text string) {
		if strings.TrimSpace(text) == "" {
			return
		}
		ast, err := plansql.ParseExpression(text)
		if err != nil {
			return
		}
		for _, ref := range collectColRefs(ast) {
			if strings.HasPrefix(ref.Column, windowKeyColPrefix) || strings.HasPrefix(ref.Column, ":") {
				continue
			}
			name := ref.String()
			if lc := strings.ToLower(name); !seen[lc] {
				seen[lc] = true
				out = append(out, name)
			}
		}
	}
	for _, f := range s.FilterExprs {
		add(f)
	}
	for _, p := range s.ProjectExprs {
		add(p.Expr)
	}
	// A join absorbed into this stage evaluates its own residual filter
	// inside this fragment, so its references are this stage's payload too.
	// `fuseStageChains` moves a downstream join's FilterExprs onto the
	// ChainedJoinSpec rather than onto the stage, so reading only
	// s.FilterExprs misses them entirely — and the absorbed filter is often
	// the RE-SPELLED one, naming a source column (`(a * 2) > 1`) that the
	// OutputFilter narrowed away two links earlier:
	//
	//	WITH c AS (SELECT id, a * 2 AS dv FROM t)
	//	SELECT COUNT(*) FROM c JOIN t x ON c.id = x.id JOIN t y ON c.id = y.id
	//	WHERE c.dv > 1
	//	-- PostgreSQL 5 · single 5 · DAG broadcast 5 · DAG SHUFFLED 0
	//
	// The shuffled lowering is the one that chains, which is why only that
	// arm answered zero, and it is type-independent — the same shape over a
	// BIGINT column answers 0 the same way.
	for _, cj := range s.ChainedJoins {
		for _, f := range cj.FilterExprs {
			add(f)
		}
		for _, f := range cj.BuildFilterExprs {
			add(f)
		}
		add(cj.JoinFilter)
	}
	for _, fj := range s.FusedJoins {
		for _, f := range fj.FilterExprs {
			add(f)
		}
		add(fj.JoinFilter)
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
		for _, e := range exprs {
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
				return fmt.Errorf("%w: stage %s (%s) filters on %q and NO stage in the plan "+
					"computes %q — the predicate would be UNKNOWN on every row and the query "+
					"would answer WITHOUT it (#700)",
					ErrUnreachableGatherOutput, s.ID, s.Type, e, ref.String())
			}
		}
	}
	return nil
}
