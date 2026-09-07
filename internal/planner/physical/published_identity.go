package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// A CONSUMER BINDS THROUGH THE IDENTITY ITS PRODUCER PUBLISHED (#770).
//
// ADR-0026 §2 gave a GROUP BY key two names — the PUBLISHED name every
// consumer above the aggregate reads it under, and the RESOLUTION spelling the
// computing fragment looks up in its own input — and carried both on the
// Stage. The two names stop at the aggregate. A JOIN publishes names of its
// own: `joinOutputSchemaWithMapping` emits the probe's columns and then the
// build's with every DUPLICATE bare name QUALIFIED by its owning alias, and a
// join's `Columns` (an OutputFilter) plus its exchanges' payload manifests are
// built from `NeededColumns`, which spells the name the QUERY wrote. So a
// consumer that resolves a column by a SECOND spelling — the resolution
// spelling of a group key, an aggregate's argument, a window's argument — is
// handed a name and left to hope the payload carries it under exactly that
// text.
//
// Two things go wrong, and #770 is both at once:
//
//   - the value IS on the stream, under the spelling the join published for
//     it. `SELECT DISTINCT x.w, y.w, z.w` over three derived arms resolves
//     x's key to the source column `a`, which the join publishes as `x.a`
//     because z's arm carries an `a` too. The runtime's own fallback then
//     finds TWO columns ending `.a` and declines, which is right — a stream
//     with two `.a` is not one the engine may guess at — and the task fails
//     on a query PostgreSQL answers.
//   - the value is on NO stream at all, because a narrowing stage below
//     dropped it. y's key resolves to `w`, which the y arm's fragment
//     computes and the join UNDER the consumer filtered away.
//
// The first is answered by RESPELLING to what the producer publishes; it costs
// no bytes. The second is answered by CARRYING the value, which does — so it
// is asked SECOND and only of a reference the first could not place. The
// TPC-H stage-dump golden is the measurement: every group key there already
// binds, so no query gains a column.
//
// The two questions are asked of the stream the fragment will really see —
// `aggregateInputStreamColumns` with the narrowing lists APPLIED — which is a
// different question from the one `resolveStageGroupKeys` asks. That pass runs
// before the payload is settled and asks what the arms can SUPPLY; this one
// runs after and asks what they will SHIP. Both are needed: the first picks
// the value, the second picks its name.
//
// The pass runs in two phases, and the order is the whole of the argument that
// it costs nothing: phase 1 CARRIES only what no spelling on the stream
// reaches, phase 2 then RESPELLS every consumer against the stream those
// carries produced. Doing them in one loop would respell against a stream a
// later stage's carry is about to change.
func bindConsumersToPublishedIdentity(stages []Stage) {
	idx := make(map[string]int, len(stages))
	for i := range stages {
		idx[stages[i].ID] = i
	}
	for i := range stages {
		carryUnreachableConsumerValues(stages, idx, i)
		respellUnionArmsOverProducerOutput(stages, idx, i, true)
	}
	for i := range stages {
		respellConsumersOverProducerOutput(stages, idx, i)
		respellUnionArmsOverProducerOutput(stages, idx, i, false)
	}
}

// carryUnreachableConsumerValues widens the payload with the values stage i's
// fragment will evaluate and NO spelling on its input reaches.
//
// Only a value the producing subtree really computes is carried
// (`widenNarrowingStagesBelow`'s own subtree test), and only after every
// cheaper answer has been tried: the ALIAS the arm's fragment may publish
// first, then the SOURCE column a plain rename reads. A definition's columns
// are never carried — recomputing a value the arm already computed is a
// second carry AND a second evaluation.
func carryUnreachableConsumerValues(stages []Stage, idx map[string]int, i int) {
	s := &stages[i]
	keys := stageGroupKeyList(s)
	groups := stageComputesGroupKeys(s) && len(s.GroupByResolve) == len(keys)
	specs := stageComputedAggSpecs(s)
	wins := stageComputedWindowCols(s)
	if !groups && len(specs) == 0 && len(wins) == 0 {
		return
	}
	in, arms := aggregateInputStreamColumnsShipped(stages, idx, s)
	if len(in) == 0 {
		return
	}
	root := i
	if !isJoinStage(s.Type) {
		j, ok := idx[firstDep(s)]
		if !ok {
			return
		}
		root = j
	}
	carry := func(candidates ...string) {
		for _, c := range candidates {
			if c == "" {
				continue
			}
			if keep := refsSubtreeCanSupply(stages, idx, root, []string{c},
				map[int]map[string]string{}); len(keep) == 1 {
				widenNarrowingStagesBelow(stages, idx, root, keep)
				return
			}
		}
	}
	if groups {
		for k := range s.GroupByResolve {
			r := s.GroupByResolve[k]
			// A COMPUTED resolution is an expression the fragment evaluates,
			// and its own references are already carried by
			// `groupKeyResolutionRefs`. Only a NAME is looked up.
			if r.Computed || r.Expr == "" {
				continue
			}
			if _, ok := bindStreamColumn(r.Expr, in); ok {
				continue
			}
			if _, ok := publishedSpellingFor(r.Expr, keys[k], arms, in); ok {
				continue
			}
			carry(r.Expr)
		}
	}
	for _, spec := range specs {
		for _, ref := range spec.InputRefs {
			if _, _, ok := producerSpellingForRef(ref, arms, in); ok {
				continue
			}
			carry(stripQualifier(ref.Written), ref.Source)
		}
	}
	for _, wc := range wins {
		for _, ref := range wc.InputRefs {
			if _, _, ok := producerSpellingForRef(ref, arms, in); ok {
				continue
			}
			carry(stripQualifier(ref.Written), ref.Source)
		}
	}
}

// respellConsumersOverProducerOutput rewrites every consumer reference on
// stage i to the spelling its producer PUBLISHES.
func respellConsumersOverProducerOutput(stages []Stage, idx map[string]int, i int) {
	s := &stages[i]
	keys := stageGroupKeyList(s)
	groups := stageComputesGroupKeys(s) && len(s.GroupByResolve) == len(keys)
	specs := stageComputedAggSpecs(s)
	wins := stageComputedWindowCols(s)
	if !groups && len(specs) == 0 && len(wins) == 0 {
		return
	}
	in, arms := aggregateInputStreamColumnsShipped(stages, idx, s)
	if len(in) == 0 {
		return
	}
	if groups {
		for k := range s.GroupByResolve {
			r := &s.GroupByResolve[k]
			if r.Computed || r.Expr == "" {
				continue
			}
			if _, ok := bindStreamColumn(r.Expr, in); ok {
				continue
			}
			if name, ok := publishedSpellingFor(r.Expr, keys[k], arms, in); ok {
				r.Expr = name
			}
		}
	}
	for _, spec := range specs {
		respellAggSpecOverProducerOutput(spec, arms, in)
	}
	for _, wc := range wins {
		// A window's argument is a NAME by construction — an expression
		// argument is materialized into `__winkey_N` by an earlier pass — so
		// there is one reference and no splice to parenthesize.
		for _, ref := range wc.InputRefs {
			if !strings.EqualFold(ref.Written, wc.InputCol) {
				continue
			}
			if _, ok := bindStreamColumnFromArm(ref.Written, arms, in); ok {
				continue
			}
			if name, isExpr, ok := producerSpellingForRef(ref, arms, in); ok && !isExpr {
				wc.InputCol = name
			}
		}
	}
}

// stageComputedWindowCols is every window column whose ARGUMENT this stage's
// fragment resolves against its raw input.
func stageComputedWindowCols(s *Stage) []*WindowColSpec {
	if s.Type != StageWindow {
		return nil
	}
	var out []*WindowColSpec
	for k := range s.WindowCols {
		if len(s.WindowCols[k].InputRefs) > 0 {
			out = append(out, &s.WindowCols[k])
		}
	}
	return out
}

// stageComputedAggSpecs is every aggregate spec whose ARGUMENT this stage's
// fragment resolves against a RAW input, in the three places a stage can carry
// one. It is stageComputesGroupKeys' twin, and it excludes a merge for the
// same reason: a final or merge aggregate over a partial's output reads that
// partial's OutputCol, never the argument's own spelling.
func stageComputedAggSpecs(s *Stage) []*AggSpec {
	var lists [][]AggSpec
	switch s.Type {
	case StageScan:
		lists = append(lists, s.FusedAggSpecs)
	case StageAggregate:
		lists = append(lists, s.AggSpecs)
	case StageFinalAggregate, StageMergeAggregate:
		if s.RawInputAggregate {
			lists = append(lists, s.AggSpecs)
		}
	case StageHashJoin, StageBroadcastJoin, StageSortMergeJoin:
		lists = append(lists, s.ChainedAggSpecs, s.FusedAggSpecs)
	}
	var out []*AggSpec
	for _, l := range lists {
		for k := range l {
			if len(l[k].InputRefs) > 0 {
				out = append(out, &l[k])
			}
		}
	}
	return out
}

// producerSpellingForRef answers, for ONE reference an argument makes to a
// derived table's alias, what the producing fragment calls that value.
//
// The rules are `resolveDerivedAliasKey`'s, asked of an argument instead of a
// key and against the stream the fragment will really see. Each asks WHICH ARM
// first: the reference names a derived table, that table is one arm of the
// join, and a column of the same name on another arm is a different value.
//
//  1. the stream spells the reference EXACTLY, because the arm's fragment
//     materialized the alias and the join qualified this arm's copy;
//  2. exactly one column of the alias's bare name FROM THE REFERENCE'S ARM —
//     the arm materialized it and nothing else spells it that way;
//  3. the SOURCE column a plain rename reads, under the one spelling the
//     stream gives it on that arm. This is the answer
//     aggInputAliasIsMaterializedUnderItsName gets wrong: it says a JOIN
//     materializes the alias, and attachScanSelectProjections puts no
//     projection on an arm whose SELECT list is a bare rename, so the join
//     publishes the SOURCE and reading the alias reads nothing;
//  4. the DEFINITION, re-spelled into the arm's own spellings, which the
//     fragment's pre-aggregate projection then evaluates.
//
// The second return says the replacement is an EXPRESSION rather than a name,
// so the caller parenthesizes it before splicing it into a larger one.
func producerSpellingForRef(ref AggInputRef, arms map[string]bool,
	in []streamCol) (string, bool, bool) {
	arm, constrained := keyArmConstraint(ref.Written, arms)
	fromArm := func(c streamCol) bool {
		return !constrained || strings.EqualFold(c.Arm, arm)
	}
	for _, c := range in {
		if !c.Dropped && fromArm(c) && strings.EqualFold(c.Name, ref.Written) {
			return c.Name, false, true
		}
	}
	if name, ok := publishedSpellingFor(ref.Written, ref.Written, arms, in); ok {
		return name, false, true
	}
	if ref.Source != "" {
		if name, ok := publishedSpellingFor(ref.Source, ref.Written, arms, in); ok {
			return name, false, true
		}
	}
	if ref.Def != "" {
		if respelled, ok := respellDefOverArm(ref.Def, in, arm, constrained); ok {
			return respelled, true, true
		}
	}
	return "", false, false
}

// respellAggSpecOverProducerOutput rewrites the spec's argument text so every
// reference names what its producer publishes.
//
// InputCol is rewritten only where it IS the whole argument: with an InputExpr
// present it is the NAME the worker's pre-aggregate projection writes the
// value under, not a name it reads (see AggSpec.InputExpr), and rewriting it
// would break that pairing.
func respellAggSpecOverProducerOutput(spec *AggSpec, arms map[string]bool, in []streamCol) {
	byWritten := make(map[string]AggInputRef, len(spec.InputRefs))
	for _, r := range spec.InputRefs {
		byWritten[strings.ToLower(r.Written)] = r
	}
	rewrite := func(text string) (string, bool) {
		node, err := plansql.ParseExpression(text)
		if err != nil {
			return "", false
		}
		out, changed, complete := rewriteColRefs(node, func(c *plansql.ColRef) (plansql.Node, bool) {
			ref, ok := byWritten[strings.ToLower(c.String())]
			if !ok {
				return nil, false
			}
			if _, ok := bindStreamColumnFromArm(ref.Written, arms, in); ok {
				return nil, false
			}
			name, isExpr, ok := producerSpellingForRef(ref, arms, in)
			if !ok {
				return nil, false
			}
			if isExpr {
				inner, err := plansql.ParseExpression(name)
				if err != nil {
					return nil, false
				}
				// PARENTHESIZED, because the definition is substituted into a
				// larger expression and `b * 100` spliced bare into `x + …`
				// would re-associate (respellAggInputExpr's own rule).
				return &plansql.ParenNode{Inner: inner}, true
			}
			if strings.EqualFold(name, c.String()) {
				return nil, false
			}
			if dot := strings.IndexByte(name, '.'); dot > 0 {
				return &plansql.ColRef{Table: name[:dot], Column: name[dot+1:]}, true
			}
			return &plansql.ColRef{Column: name}, true
		})
		// A walk that met a node kind it does not rewrite has NOT considered
		// every reference, and a PARTIAL respell looks resolved without being
		// it — respellAggInputExpr's rule, for its reason.
		if !complete || !changed {
			return "", false
		}
		return out.String(), true
	}
	if spec.InputExpr != "" {
		if text, ok := rewrite(spec.InputExpr); ok {
			spec.InputExpr = text
		}
		return
	}
	if spec.InputCol == "" || spec.InputCol == "*" {
		return
	}
	if text, ok := rewrite(spec.InputCol); ok {
		spec.InputCol = text
	}
}

// bindStreamColumnFromArm is bindStreamColumn with the ARM constraint the
// reference's own qualifier states.
//
// The arm is what makes a bind that SUCCEEDS still wrong. `SUM(y.w + x.w)`
// over two arms that both publish `w` binds both references to the probe's
// `w` through the runtime's strip-the-qualifier step and answers 2 x SUM(y.w)
// — 9650.0000 where PostgreSQL answers 4865.2500, silently. Asking whether the
// bind lands on the arm the reference NAMES is what separates "the payload has
// this value" from "the payload has a value of this name".
func bindStreamColumnFromArm(name string, arms map[string]bool, in []streamCol) (string, bool) {
	arm, constrained := keyArmConstraint(name, arms)
	if !constrained {
		return bindStreamColumn(name, in)
	}
	armCols := make([]streamCol, 0, len(in))
	for _, c := range in {
		if strings.EqualFold(c.Arm, arm) {
			armCols = append(armCols, c)
		}
	}
	return bindStreamColumn(name, armCols)
}

// aggInputAliasCandidates records, for every reference in the spec's shipped
// argument that names a derived table's SELECT-list alias, the two candidate
// spellings only the finished stage graph can settle.
//
// It reads the text the SPEC carries rather than the query's, so a reference
// the emission-time passes already re-spelled to its source records nothing:
// `resolveAggInputName` reports a source column as no rename at all.
func aggInputAliasCandidates(spec AggSpec, child *logical.Node) []AggInputRef {
	text := spec.InputExpr
	if text == "" {
		text = spec.InputCol
	}
	return aliasCandidatesForText(text, child)
}

// aliasCandidatesForText is aggInputAliasCandidates over one expression text —
// an aggregate's argument, or a window's.
func aliasCandidatesForText(text string, child *logical.Node) []AggInputRef {
	if text == "" || text == "*" || child == nil {
		return nil
	}
	node, err := plansql.ParseExpression(text)
	if err != nil {
		return nil
	}
	var out []AggInputRef
	seen := map[string]bool{}
	for _, c := range collectColRefs(node) {
		written := c.String()
		if seen[strings.ToLower(written)] {
			continue
		}
		resolved, expr, _, renamed := resolveAggInputName(written, child)
		if !renamed {
			continue
		}
		seen[strings.ToLower(written)] = true
		r := AggInputRef{Written: written}
		if expr != nil {
			r.Def = expr.String()
		} else {
			r.Source = resolved
		}
		out = append(out, r)
	}
	return out
}

// aggregateInputStreamColumnsShipped is aggregateInputStreamColumns asking
// what the fragment's input really SHIPS rather than what its arms could
// supply: every OutputFilter, payload manifest and scan projection applied.
//
// The unfiltered view is the right one for `resolveStageGroupKeys`, which runs
// before the payload is settled and would otherwise refuse a key the carry
// pass is about to make reachable. It is the wrong one HERE, where the
// question is which NAME the value will arrive under.
func aggregateInputStreamColumnsShipped(stages []Stage, idx map[string]int,
	s *Stage) ([]streamCol, map[string]bool) {
	switch {
	case s.Type == StageScan:
		raw := *s
		raw.FusedAggGroupBy, raw.FusedAggSpecs = nil, nil
		return scanStreamColumns(&raw), nil
	case isJoinStage(s.Type):
		bare := *s
		bare.ChainedAggGroupBy, bare.ChainedAggSpecs = nil, nil
		return joinStreamColumnsArms(stages, idx, &bare, passThroughDepth, true)
	case s.Type == StageAggregate, s.Type == StageFinalAggregate, s.Type == StageMergeAggregate,
		s.Type == StageWindow:
	default:
		return nil, nil
	}
	i, ok := idx[firstDep(s)]
	if !ok {
		return nil, nil
	}
	dep := &stages[i]
	if isJoinStage(dep.Type) {
		return joinStreamColumnsArms(stages, idx, dep, passThroughDepth, true)
	}
	return stageStreamColumnsFiltered(stages, idx, dep, passThroughDepth, true), nil
}

// bindStreamColumn is the plan-time mirror of the runtime resolver
// (`exec.columnIndexFallback`, which every group key, aggregate input, sort
// key and join key comes through): the exact spelling, then the BARE part of a
// qualified reference, then a UNIQUE `.bare` suffix.
//
// Ambiguity DECLINES, which is the half `columnResolves` does not have and the
// half that matters here. That checker answers "could anything here be this
// name" and accepts two arms spelling `.w`; the engine refuses to guess an arm
// (#742), so a plan-time test that accepts what the runtime rejects reports a
// reachable value where the task will fail.
func bindStreamColumn(name string, in []streamCol) (string, bool) {
	if name == "" {
		return "", false
	}
	for _, c := range in {
		if !c.Dropped && strings.EqualFold(c.Name, name) {
			return c.Name, true
		}
	}
	bare := stripQualifier(name)
	if !strings.EqualFold(bare, name) {
		for _, c := range in {
			if !c.Dropped && strings.EqualFold(c.Name, bare) {
				return c.Name, true
			}
		}
	}
	suffix := "." + strings.ToLower(bare)
	match, hits := "", 0
	for _, c := range in {
		if c.Dropped {
			continue
		}
		if strings.HasSuffix(strings.ToLower(c.Name), suffix) {
			match, hits = c.Name, hits+1
		}
	}
	if hits == 1 {
		return match, true
	}
	return "", false
}

// publishedSpellingFor is the producer's own name for the value a consumer
// wrote as `written` and resolves by `name`.
//
// The ARM is what makes the answer unambiguous where the bare name is not:
// `written` names a derived table, that table is one arm of the join, and a
// column of the same name on ANOTHER arm is a different value.
// `keyArmConstraint` reads the arm off the written spelling the same way
// `resolveDerivedAliasKey` does, and a reference written BARE constrains
// nothing — SQL already resolved its ambiguity.
//
// A DROPPED column is not a candidate: the executor could not emit it at all,
// and binding to the surviving column of that name is the wrong value rather
// than a missing one.
func publishedSpellingFor(name, written string, arms map[string]bool, in []streamCol) (string, bool) {
	arm, constrained := keyArmConstraint(written, arms)
	bare := strings.ToLower(stripQualifier(name))
	if bare == "" {
		return "", false
	}
	match, hits := "", 0
	for _, c := range in {
		if c.Dropped {
			continue
		}
		if constrained && !strings.EqualFold(c.Arm, arm) {
			continue
		}
		if strings.ToLower(stripQualifier(c.Name)) != bare {
			continue
		}
		match, hits = c.Name, hits+1
	}
	if hits != 1 {
		return "", false
	}
	return match, true
}

// A UNION ARM's projection is the fourth consumer, and it is the one that
// arrives as an EXPRESSION rather than a name.
//
// An arm forwarding a derived table's COMPUTED column is rewritten into the
// expression that DEFINES it (#554), so `SELECT y.w` over
// `(SELECT id, b*100 AS w FROM t) y` projects `b * 100 AS yw`. The producing
// fragment computed that value already and publishes it as `w`; the join above
// carries `w` and never carries `b`, so the arm recomputed the definition over
// a stream with no `b` and every row of that column came back NULL — and the
// UNION's dedup then collapsed five distinct pairs into two, which is a wrong
// ROW COUNT rather than a visible NULL.
//
// `respellSpecsOverProducerOutput` already asks whether the spec resolves and
// DECLINES when it does not, which leaves the arm shipping the text that
// answers NULL. Asking the producer what it CALLS the value is the answer it
// is missing: a fragment that materializes an expression publishes it under a
// name, and that name is the arm's projection.
func respellUnionArmsOverProducerOutput(stages []Stage, idx map[string]int, i int, carry bool) {
	s := &stages[i]
	if s.Type != StageUnion {
		return
	}
	for a := range s.UnionArms {
		j, ok := idx[s.UnionArmDep(a)]
		if !ok {
			continue
		}
		in := stageStreamColumnsFiltered(stages, idx, &stages[j], passThroughDepth, true)
		if len(in) == 0 {
			continue
		}
		for k := range s.UnionArms[a].Projections {
			sp := &s.UnionArms[a].Projections[k]
			if sp.Expr == "" || specResolvesOverStream(sp.Expr, in) {
				continue
			}
			name := publishedNameForExpr(stages, idx, j, sp.Expr)
			if name == "" {
				continue
			}
			if _, bound := bindStreamColumn(name, in); bound {
				if !carry {
					sp.Expr = name
				}
				continue
			}
			if carry {
				if keep := refsSubtreeCanSupply(stages, idx, j, []string{name},
					map[int]map[string]string{}); len(keep) == 1 {
					widenNarrowingStagesBelow(stages, idx, j, keep)
				}
			}
		}
	}
}

// specResolvesOverStream reports whether every column reference in a
// projection's text binds on the stream, with the runtime resolver's own
// rules including its refusal to guess between two `.bare` matches.
func specResolvesOverStream(text string, in []streamCol) bool {
	ast, err := plansql.ParseExpression(text)
	if err != nil {
		return true // not ours to judge; leave the spec alone
	}
	for _, ref := range collectColRefs(ast) {
		if strings.HasPrefix(ref.Column, windowKeyColPrefix) || strings.HasPrefix(ref.Column, ":") {
			continue // the fragment computes it, or dispatch substitutes it
		}
		if _, ok := bindStreamColumn(ref.String(), in); !ok {
			return false
		}
	}
	return true
}

// publishedNameForExpr is the name some fragment at or below root publishes
// for the value this expression computes.
//
// The comparison is `plansql.ExprIdentity`, which is what ADR-0026 §1 makes
// the identity of an expression: parentheses, identifier case and whitespace
// erased and nothing else. A projection whose Name IS its Expr publishes no
// second name and is skipped — that is a column passing through, not a value
// being computed.
func publishedNameForExpr(stages []Stage, idx map[string]int, root int, text string) string {
	ast, err := plansql.ParseExpression(text)
	if err != nil {
		return ""
	}
	want := plansql.ExprIdentity(ast)
	if want == "" {
		return ""
	}
	seen := make(map[int]bool, 8)
	queue := []int{root}
	for depth := 0; len(queue) > 0 && depth < passThroughDepth*4; depth++ {
		next := queue
		queue = nil
		for _, k := range next {
			if seen[k] {
				continue
			}
			seen[k] = true
			s := &stages[k]
			for _, p := range s.ProjectExprs {
				if p.Name == "" || p.Expr == "" || strings.EqualFold(p.Name, p.Expr) {
					continue
				}
				node, err := plansql.ParseExpression(p.Expr)
				if err != nil {
					continue
				}
				if plansql.ExprIdentity(node) == want {
					return p.Name
				}
			}
			for _, dep := range s.Dependencies {
				if d, ok := idx[dep]; ok {
					queue = append(queue, d)
				}
			}
		}
	}
	return ""
}
