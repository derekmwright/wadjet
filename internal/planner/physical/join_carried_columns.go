package physical

import (
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
		if len(s.FilterExprs) == 0 && len(s.ProjectExprs) == 0 {
			continue
		}
		refs := joinEvaluatedColumnRefs(s)
		if len(refs) == 0 {
			continue
		}
		if len(s.Columns) > 0 {
			s.Columns = unionColumnNames(s.Columns, refs)
		}
		for _, dep := range s.Dependencies {
			j, ok := idx[dep]
			if !ok {
				continue
			}
			d := &stages[j]
			switch d.Type {
			case StageExchangeRepartition, StageExchangeReplicate:
			default:
				continue
			}
			if len(d.Columns) == 0 {
				continue // already carries everything
			}
			d.Columns = unionColumnNames(d.Columns, refs)
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
