package physical

import (
	"strings"
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
func bindConsumersToPublishedIdentity(stages []Stage) {
	idx := make(map[string]int, len(stages))
	for i := range stages {
		idx[stages[i].ID] = i
	}
	for i := range stages {
		s := &stages[i]
		if !stageComputesGroupKeys(s) {
			continue
		}
		keys := stageGroupKeyList(s)
		if len(s.GroupByResolve) != len(keys) {
			continue
		}
		in, arms := aggregateInputStreamColumnsShipped(stages, idx, s)
		if len(in) == 0 {
			continue
		}
		var carry []string
		for k := range s.GroupByResolve {
			r := &s.GroupByResolve[k]
			// A COMPUTED resolution is an expression the fragment evaluates,
			// and its own references are already carried by
			// `groupKeyResolutionRefs`. Only a NAME is looked up.
			if r.Computed || r.Expr == "" {
				continue
			}
			if _, ok := bindStreamColumn(r.Expr, in); ok {
				continue
			}
			if name, ok := publishedSpellingFor(r.Expr, keys[k], arms, in); ok {
				r.Expr = name
				continue
			}
			carry = append(carry, r.Expr)
		}
		if len(carry) == 0 {
			continue
		}
		// The value reaches this fragment under no spelling. Carry it, the
		// same way `ensureJoinCarriesEvaluatedColumns` carries a computed
		// resolution's references: into this stage's own OutputFilter and
		// into every narrowing stage below that can supply it, never into a
		// producer's read set.
		root := i
		if !isJoinStage(s.Type) {
			j, ok := idx[firstDep(s)]
			if !ok {
				continue
			}
			root = j
		}
		widenNarrowingStagesBelow(stages, idx, root, carry)
	}
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
	case s.Type == StageAggregate, s.Type == StageFinalAggregate, s.Type == StageMergeAggregate:
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
