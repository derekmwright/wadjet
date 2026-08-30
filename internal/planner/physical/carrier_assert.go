package physical

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// ErrUnreachableGatherOutput marks a plan whose gather renames a source no
// stage emits — a SELECT list that became nobody's job.
//
// It is refused at PLAN time, not at dispatch, so the coordinator can route
// the query onto its local single-process engine and ANSWER it, the way it
// already does for a correlated subquery (#359), an unstageable DISTINCT
// (#466), an unmaterializable IN set (#524) and a SELECT-list subquery
// (#659). The alternative is what the DAG did before: hand the client the
// producer's raw columns under their source names — `[__win_0, n_nationkey]`
// for a query that asked for one column `x`.
var ErrUnreachableGatherOutput = errors.New(
	"the stage DAG computed no SELECT list for this shape")

// The two checks a stage-TYPE check cannot make, promoted to run on every
// distributed plan.
//
// stageRunsFilterExprs asks whether the fragment READS the field.
// assertCarrierSchemaResolves asks whether it can EVALUATE what it reads, and
// assertGatherOutputIsReachable asks whether anything computed the SELECT
// list at all. Both were gates only, which closed the class for the corpus
// and left it open for every query outside it; they cost microseconds on a
// plan of tens of stages, so ValidateNativeDAGShape runs them before dispatch
// and REFUSES rather than answers (#656 F2).

// assertCarrierSchemaResolves refuses a plan where a stage carries an
// expression its INPUT cannot resolve.
//
// This is the silent half by construction: expr.ColRef.Eval returns nil for a
// name it cannot resolve, so the predicate is UNKNOWN on every row and a
// WHERE admits only TRUE.
func assertCarrierSchemaResolves(stages []Stage) error {
	idx := make(map[string]int, len(stages))
	for i := range stages {
		idx[stages[i].ID] = i
	}
	for i := range stages {
		emitted, modelled := carrierInputColumns(stages, idx, i)
		if !modelled {
			continue
		}
		for _, e := range stages[i].FilterExprs {
			if missing := unresolvableColumnRefs(e, emitted); len(missing) > 0 {
				return fmt.Errorf("native-DAG: stage %s (%s) filters on %q and its input carries "+
					"no %v — the predicate would be UNKNOWN on every row and the query would "+
					"answer WITHOUT it (#656); input: %v",
					stages[i].ID, stages[i].Type, e, missing, sortedEmittedNames(emitted))
			}
		}
		for _, pe := range stages[i].ProjectExprs {
			if missing := unresolvableColumnRefs(pe.Expr, emitted); len(missing) > 0 {
				return fmt.Errorf("native-DAG: stage %s (%s) projects %q AS %q and its input "+
					"carries no %v — the column would come back NULL (#656); input: %v",
					stages[i].ID, stages[i].Type, pe.Expr, pe.Name, missing,
					sortedEmittedNames(emitted))
			}
		}
	}
	return nil
}

// assertGatherOutputIsReachable refuses a plan whose gather renames a source
// no stage emits.
//
// It is the only check that sees a projection that was NEVER ATTACHED:
// nothing was deleted and nothing is on the wrong stage, the SELECT list
// simply became nobody's job, and the client gets the producer's raw columns
// under their source names.
func assertGatherOutputIsReachable(stages []Stage) error {
	gather, emitted, modelled := gatherOutputSources(stages)
	if !modelled {
		return nil
	}
	for _, r := range gather.OutputRenames {
		if r.Expr != nil || r.From == "" {
			continue // the gather computes this one itself
		}
		if _, ok := lookupEmittedColumn(emitted, r.From); !ok {
			return fmt.Errorf("%w: the gather renames %q to %q and no stage emits a column of "+
				"that name, so the client would get the producer's raw columns (#656); "+
				"emitted: %v", ErrUnreachableGatherOutput, r.From, r.To,
				sortedEmittedNames(emitted))
		}
	}
	return nil
}

// sortedEmittedNames renders an emitted-column set for a refusal message.
func sortedEmittedNames(emitted map[string]string) []string {
	out := make([]string, 0, len(emitted))
	for _, v := range emitted {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// assertSortKeysResolve refuses a plan whose sort keys its input cannot
// supply.
//
// The same question as assertCarrierSchemaResolves, asked of the one field it
// does not cover. A sort key is not silent — `sort: key column "d" does not
// exist in the input schema` is a loud dispatch failure — but it is a loud
// failure for a query PostgreSQL ANSWERS, and the shape reaches it by exactly
// the route this ADR is about: the projection that would have computed the
// key was never attached, so the sort keys on a name nothing emits.
//
// Refusing at PLAN time routes the query local, which answers it.
//
// A key with a SourceExpr, a SourceColumn or an AliasSource is left alone: a
// later pass (resolveHiddenSortKeys, resolveDerivedAliasSortKeys) materializes
// or renames it, and it is that pass's own gate that owns the question.
func assertSortKeysResolve(stages []Stage) error {
	idx := make(map[string]int, len(stages))
	for i := range stages {
		idx[stages[i].ID] = i
	}
	for i := range stages {
		s := &stages[i]
		if len(s.SortKeys) == 0 {
			continue
		}
		emitted, modelled := carrierInputColumns(stages, idx, i)
		if !modelled {
			continue
		}
		for _, pe := range s.ProjectExprs {
			if pe.Name != "" {
				emitted[strings.ToLower(pe.Name)] = pe.Name
			}
		}
		for _, k := range s.SortKeys {
			if k.Column == "" || k.SourceExpr != "" || k.SourceColumn != "" || k.AliasSource != "" {
				continue
			}
			if _, ok := lookupEmittedColumn(emitted, k.Column); ok {
				continue
			}
			return fmt.Errorf("%w: stage %s (%s) orders on %q and its input carries no such "+
				"column, so the fragment would fail at dispatch for a query the engine can "+
				"answer (#656); input: %v",
				ErrUnreachableGatherOutput, s.ID, s.Type, k.Column, sortedEmittedNames(emitted))
		}
	}
	return nil
}
