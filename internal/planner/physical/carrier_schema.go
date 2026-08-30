package physical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// Does the stage carrying a predicate or a projection have the COLUMNS to
// evaluate it?
//
// stageRunsFilterExprs answers a weaker question — does the fragment read the
// field at all — and a stage can pass that and still answer nothing, because
// the expression names a column its input does not carry. That is the whole
// #653/#656 failure mode: `expr.ColRef.Eval` returns nil for a name it cannot
// resolve, the predicate is UNKNOWN on every row, and a WHERE admits only
// TRUE. Every silent shape in the family looks identical at the stage-type
// level and different here.
//
// The check is deliberately partial. A JOIN's input is the QUALIFIED union of
// two sides, with per-column origin rules (BuildColOrigins, QualifyAllBuildCols)
// that only the executor resolves; asserting over it would produce false
// refusals, which are worse than a narrower gate. Join stages are therefore
// excluded and named as excluded, rather than silently passing.

// carrierInputColumns returns the column names the stage at i evaluates its
// FilterExprs and ProjectExprs against, and ok=false for a stage whose input
// schema this check does not model.
func carrierInputColumns(stages []Stage, idx map[string]int, i int) (map[string]string, bool) {
	s := &stages[i]
	switch s.Type {
	case StageScan:
		// The filter runs against the READ SET, before any projection.
		if len(s.Columns) == 0 {
			return nil, false
		}
		out := map[string]string{}
		for _, c := range s.Columns {
			out[strings.ToLower(c)] = c
		}
		// A fused scan-aggregate's own outputs are readable above it.
		for _, k := range s.FusedAggGroupBy {
			out[strings.ToLower(k)] = k
		}
		for _, a := range s.FusedAggSpecs {
			if a.OutputCol != "" {
				out[strings.ToLower(a.OutputCol)] = a.OutputCol
			}
		}
		return out, true
	case StageAggregate, StageFinalAggregate, StageMergeAggregate:
		groupKeys, aggOuts := aggregateStageOutputs(s)
		out := make(map[string]string, len(groupKeys)+len(aggOuts))
		for k, v := range groupKeys {
			out[k] = v
		}
		for k, v := range aggOuts {
			out[k] = v
		}
		if len(out) == 0 {
			return nil, false
		}
		return out, true
	case StageUnion:
		// A union emits exactly what its ARMS project, under the result
		// names every arm was normalized onto. Modelling it is what lets a
		// sort ABOVE a union be checked at all: without it the ordering over
		// `a UNION ALL b` was unmodelled, and a key the arms disagreed about
		// reached the fragment and panicked (#656 R4).
		if len(s.UnionArms) == 0 {
			return nil, false
		}
		out := map[string]string{}
		for _, sp := range s.UnionArms[0].Projections {
			if sp.Name != "" {
				out[strings.ToLower(sp.Name)] = sp.Name
			}
		}
		if len(out) == 0 {
			return nil, false
		}
		return out, true
	case StageSort, StageMergeSort, StageLimit, StageWindow, StageProject:
		if len(s.Dependencies) != 1 {
			return nil, false
		}
		depIdx, ok := idx[s.Dependencies[0]]
		if !ok {
			return nil, false
		}
		emitted := emittedThroughPassThrough(stages, idx, &stages[depIdx])
		if len(emitted) == 0 {
			return nil, false
		}
		// A window's own outputs are readable by anything it carries above
		// the operator.
		for _, w := range s.WindowCols {
			if w.OutputCol != "" {
				emitted[strings.ToLower(w.OutputCol)] = w.OutputCol
			}
		}
		return emitted, true
	}
	// Joins, unions, exchanges, dual: not modelled — see the file comment.
	return nil, false
}

// unresolvableColumnRefs returns the column references in exprText that the
// carrier's input does not carry, or nil when every one resolves (or when the
// text cannot be parsed, which is a different defect and a different check).
//
// Resolution mirrors the RUNTIME lookup's tolerance: the exact spelling, the
// bare name after dropping a qualifier, and — for a ROW field path — the
// qualifier read as the column. A synthetic name the fragment computes for
// itself (a materialized window key, a scalar-subquery placeholder) is not a
// column reference at all and is skipped.
func unresolvableColumnRefs(exprText string, emitted map[string]string) []string {
	ast, err := plansql.ParseExpression(exprText)
	if err != nil {
		return nil
	}
	var missing []string
	seen := map[string]bool{}
	emittedName := func(n plansql.Node) bool {
		if _, ok := n.(*plansql.ColRef); ok {
			return false // a bare reference is columnResolves's question
		}
		_, ok := emitted[strings.ToLower(n.String())]
		return ok
	}
	for _, ref := range collectColRefsBelow(ast, emittedName) {
		if columnResolves(ref, emitted) {
			continue
		}
		name := ref.String()
		if seen[name] {
			continue
		}
		seen[name] = true
		missing = append(missing, name)
	}
	return missing
}

func columnResolves(ref *plansql.ColRef, emitted map[string]string) bool {
	if strings.HasPrefix(ref.Column, windowKeyColPrefix) || strings.HasPrefix(ref.Column, ":") {
		return true // computed by the fragment itself, or a scalar placeholder
	}
	if _, ok := emitted[strings.ToLower(ref.String())]; ok {
		return true
	}
	if _, ok := emitted[strings.ToLower(ref.Column)]; ok {
		return true
	}
	if ref.Table != "" {
		// A ROW field path: the QUALIFIER is the column.
		if _, ok := emitted[strings.ToLower(ref.Table)]; ok {
			return true
		}
		// A qualified reference whose bare name the stream carries.
		if _, ok := emitted[strings.ToLower(stripQualifier(ref.Column))]; ok {
			return true
		}
	}
	// A join qualifies a colliding build column, so the stream may spell it
	// the other way round.
	bare := strings.ToLower(stripQualifier(ref.Column))
	for lower := range emitted {
		if strings.ToLower(stripQualifier(lower)) == bare {
			return true
		}
	}
	return false
}

// collectColRefs lists every column reference in an expression.
func collectColRefs(n plansql.Node) []*plansql.ColRef {
	return collectColRefsBelow(n, nil)
}

// collectColRefsBelow is collectColRefs with a STOP predicate: a node the
// predicate accepts is a column in its own right, and its children are not
// references at all.
//
// That distinction is the difference between a defect and a false refusal on
// a computed GROUP BY key. An aggregate stage emits its key under the key's
// own EXPRESSION TEXT, so `g + 1` is the column NAME and a HAVING spelled
// `g + 1 > 2` resolves against it exactly — while a walk that descends into
// the term sees a reference to `g`, which the aggregate's OUTPUT genuinely
// does not carry. Reading that as unresolvable refused
// `WITH a AS (SELECT g+1 AS gk, COUNT(*) AS n FROM t GROUP BY g+1 HAVING
// g+1 > 2) SELECT gk, n FROM a WHERE gk > 3` outright, on a plan whose
// fragment computes it correctly.
func collectColRefsBelow(n plansql.Node, stop func(plansql.Node) bool) []*plansql.ColRef {
	var out []*plansql.ColRef
	var walk func(plansql.Node)
	walk = func(n plansql.Node) {
		if n != nil && stop != nil && stop(n) {
			return
		}
		switch e := n.(type) {
		case nil:
			return
		case *plansql.ColRef:
			out = append(out, e)
		case *plansql.BinaryOp:
			walk(e.Left)
			walk(e.Right)
		case *plansql.CmpExpr:
			walk(e.Left)
			walk(e.Right)
		case *plansql.AndNode:
			walk(e.Left)
			walk(e.Right)
		case *plansql.OrNode:
			walk(e.Left)
			walk(e.Right)
		case *plansql.NotNode:
			walk(e.Inner)
		case *plansql.UnaryOp:
			walk(e.Inner)
		case *plansql.ParenNode:
			walk(e.Inner)
		case *plansql.CastNode:
			walk(e.Inner)
		case *plansql.IsExpr:
			walk(e.Left)
		case *plansql.LikeExpr:
			walk(e.Left)
			walk(e.Pattern)
		case *plansql.BetweenExpr:
			walk(e.Left)
			walk(e.Low)
			walk(e.High)
		case *plansql.InExpr:
			walk(e.Left)
			for _, v := range e.Values {
				walk(v)
			}
		case *plansql.FuncCallNode:
			for _, a := range e.Args {
				walk(a)
			}
		case *plansql.CaseNode:
			walk(e.Subject)
			walk(e.Else)
			for _, w := range e.Whens {
				walk(w.Cond)
				walk(w.Result)
			}
		}
	}
	walk(n)
	return out
}

// gatherOutputSources returns the gather stage and the columns its producer
// chain emits, so a caller can ask whether every OutputRename source really
// exists. ok=false when the plan has no gather or its producer is one this
// check does not model.
//
// This is the half a conservation check cannot supply. Conservation sees a
// projection that was ATTACHED and then deleted; placement sees one that was
// attached to the wrong stage. A projection that was NEVER ATTACHED — the
// SELECT list above a sort or a LIMIT that attachScanSelectProjections simply
// declined — is invisible to both, and the client gets the producer's raw
// columns under their source names (`SELECT id*2 AS d FROM (… ORDER BY id
// LIMIT 5)` came back as `id` = 0..4). The gather's own rename list is where
// that shows: its From names a column nothing emits.
func gatherOutputSources(stages []Stage) (*Stage, map[string]string, bool) {
	idx := make(map[string]int, len(stages))
	for i := range stages {
		idx[stages[i].ID] = i
	}
	for i := range stages {
		if stages[i].Type != StageExchangeGather || len(stages[i].Dependencies) != 1 {
			continue
		}
		depIdx, ok := idx[stages[i].Dependencies[0]]
		if !ok {
			return nil, nil, false
		}
		emitted := emittedThroughPassThrough(stages, idx, &stages[depIdx])
		if len(emitted) == 0 {
			return nil, nil, false
		}
		dropUnbackedJoinColumns(stages, idx, &stages[depIdx], emitted)
		if len(emitted) == 0 {
			return nil, nil, false
		}
		return &stages[i], emitted, true
	}
	return nil, nil, false
}

// dropUnbackedJoinColumns removes from emitted the names that NO stage in the
// plan produces.
//
// A join's Stage.Columns is an OutputFilter and an exchange's is a payload
// manifest: both NARROW what arrives and neither can invent a column. Reading
// them as "emitted" made the reachability check believe a name that nothing
// computes. `SELECT x.id, x.w FROM (SELECT id, SUM(a) OVER () + 0 AS w FROM t)
// x JOIN t y ON …` puts `w` in the filter because the SELECT list needs it,
// while the window stage below emits `__win_0` and the derived table's
// `__win_0 + 0 AS w` was attached to no fragment at all. The check passed, the
// gather's rename found nothing at run time, and the client got the producer's
// raw columns — the exact failure assertGatherOutputIsReachable exists to
// refuse (#656 F2, reached through a join).
//
// The test is against EVERY PRODUCING stage in the plan, not against the
// join's own inputs. Intersecting with the inputs was the first attempt and it
// refused TPC-H Q02: a producer's emitted set is modelled per stage type and a
// subtree the walk narrows differently makes a real column look absent, which
// turns a working query into a refusal. Asking "does anything here compute
// this name" cannot make that mistake — the phantom is a name nothing in the
// plan produces, which is a much weaker and much safer question.
//
// Movers and filters are excluded from the producing set for the same reason
// they are the problem: their column lists are what is under suspicion.
func dropUnbackedJoinColumns(stages []Stage, idx map[string]int, s *Stage, emitted map[string]string) {
	for depth := 0; s != nil && depth < passThroughDepth; depth++ {
		if !forwardsInputColumns(s.Type) {
			break
		}
		if len(s.Dependencies) != 1 {
			return
		}
		depIdx, ok := idx[s.Dependencies[0]]
		if !ok {
			return
		}
		s = &stages[depIdx]
	}
	if s == nil || !isJoinStage(s.Type) {
		return
	}
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
		return
	}
	for lower := range emitted {
		if !columnResolves(&plansql.ColRef{Column: emitted[lower]}, produced) {
			delete(emitted, lower)
		}
	}
}

// isJoinStage reports whether typ is one of the join lowerings.
func isJoinStage(typ string) bool {
	switch typ {
	case StageHashJoin, StageBroadcastJoin, StageSortMergeJoin:
		return true
	}
	return false
}
