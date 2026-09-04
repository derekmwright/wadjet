package physical

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
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
//
// It refuses with ErrUnreachableGatherOutput, like every other check in this
// file, so the coordinator ROUTES the query to its local engine and answers
// it. It did not, and the sentinel alone was not enough: the checks here run
// from ValidateNativeDAGShape at DISPATCH, while the routing block reads the
// PLANNING error, so a query PostgreSQL answers reached the client as a hard
// error while its CTE twin — refused inside PlanDistributed by
// assertJoinFiltersAreBacked — was answered (#763). ExecuteSQL now asks this
// same question once more right after planning, where routing is still
// possible; the wrap is what that ask reads.
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
				return fmt.Errorf("%w: stage %s (%s) filters on %q and its input carries "+
					"no %v — the predicate would be UNKNOWN on every row and the query would "+
					"answer WITHOUT it (#656); input: %v",
					ErrUnreachableGatherOutput,
					stages[i].ID, stages[i].Type, e, missing, sortedEmittedNames(emitted))
			}
		}
		for _, pe := range stages[i].ProjectExprs {
			if missing := unresolvableColumnRefs(pe.Expr, emitted); len(missing) > 0 {
				return fmt.Errorf("%w: stage %s (%s) projects %q AS %q and its input "+
					"carries no %v — the column would come back NULL (#656); input: %v",
					ErrUnreachableGatherOutput,
					stages[i].ID, stages[i].Type, pe.Expr, pe.Name, missing,
					sortedEmittedNames(emitted))
			}
		}
	}
	return nil
}

// assertAggregateInputsResolve refuses a plan whose aggregate reads an
// ARGUMENT expression its own input cannot supply.
//
// The same question assertCarrierSchemaResolves asks of a carried Filter or
// Project, asked of the third field that travels as TEXT and is compiled at the
// worker: AggSpec.InputExpr, which buildAggInputProjection materializes ahead
// of HashAggregate. It is silent for exactly the same reason — `expr.ColRef`
// answers nil for a name it cannot resolve, so the pre-projection writes NULL
// into every row and the aggregate returns a number that is wrong rather than
// missing. `SUM(CASE WHEN s = 'x' THEN twice ELSE 0 END)` over a derived table
// computing `twice` came back as the total of its ELSE branch (#702).
//
// respellAggInputExpr is what makes those names resolve; this is the backstop
// for the shapes it does not reach — a reference inside a node kind the
// rewriter does not descend into, or a rename below a producer the alias walk
// stops at. Refusing loses the DAG's parallelism for such a query; answering it
// with the wrong number loses the query.
//
// Its INPUT is the aggregate's dependency, not the aggregate's own output, so
// it needs a different schema from carrierInputColumns'. Only the PARTIAL
// aggregate is checked: a final or merge aggregate reads its partials'
// OUTPUTS, where an InputExpr is already materialized under InputCol and
// re-resolving the text would be asking the wrong question. Join, union and
// exchange-fed inputs are skipped by emittedThroughPassThrough / the
// modelled check, the same exclusions the ADR names.
func assertAggregateInputsResolve(stages []Stage) error {
	idx := make(map[string]int, len(stages))
	for i := range stages {
		idx[stages[i].ID] = i
	}
	for i := range stages {
		s := &stages[i]
		specs, emitted, ok := aggregateStageInputs(stages, idx, s)
		if !ok {
			continue
		}
		for _, a := range specs {
			// InputExpr for an argument that is an EXPRESSION, InputCol for one
			// that is a bare COLUMN — a bare reference travels as the NAME and
			// leaves InputExpr empty, so checking only the expression left the
			// commoner half unguarded: `SUM(v)` over
			// `(SELECT DISTINCT a * 2 AS v FROM t)` answered NULL on both DAG
			// arms, silently, where PostgreSQL answers 29.48. Same question,
			// both spellings.
			read := a.InputExpr
			if read == "" {
				read = a.InputCol
			}
			if read == "" || read == "*" {
				continue // COUNT(*) reads no column
			}
			if missing := unresolvableColumnRefs(read, emitted); len(missing) > 0 {
				return fmt.Errorf("%w: stage %s (%s) aggregates %s(%s) and its input "+
					"carries no %v — the pre-projection would write NULL into every row and "+
					"the aggregate would answer a WRONG NUMBER rather than fail (#702); "+
					"input: %v", ErrUnreachableGatherOutput, s.ID, s.Type, a.Func,
					read, missing, sortedEmittedNames(emitted))
			}
		}
	}
	return nil
}

// aggregateStageInputs pairs a stage's PARTIAL aggregate specs with the
// columns their pre-projection is compiled against, for the two stage shapes
// that run one: a standalone aggregate stage, whose input is its dependency's
// stream, and a FUSED scan-aggregate, whose input is the scan's own read set
// in the same fragment.
//
// ok=false for everything else, which includes the shapes a name check cannot
// judge: a final or merge aggregate (its InputExpr is already materialized
// upstream) and an aggregate over a join or a union (its input is the
// qualified union of two sides).
func aggregateStageInputs(stages []Stage, idx map[string]int, s *Stage) ([]AggSpec, map[string]string, bool) {
	if s.Type == StageScan && len(s.FusedAggSpecs) > 0 {
		emitted := stageScanReadSet(s)
		if len(emitted) == 0 {
			return nil, nil, false
		}
		return s.FusedAggSpecs, emitted, true
	}
	if s.Type != StageAggregate || len(s.AggSpecs) == 0 || len(s.Dependencies) != 1 {
		return nil, nil, false
	}
	depIdx, ok := idx[s.Dependencies[0]]
	if !ok {
		return nil, nil, false
	}
	dep := &stages[depIdx]
	if !aggregateInputIsModelled(dep.Type) {
		return nil, nil, false
	}
	emitted := emittedThroughPassThrough(stages, idx, dep)
	if len(emitted) == 0 {
		return nil, nil, false
	}
	return s.AggSpecs, emitted, true
}

// stageScanReadSet is the column set a fused scan-aggregate's pre-projection
// sees: the SCAN's read set, before its own aggregate narrows anything. It is
// deliberately not stageEmittedColumns, which answers the fused stage's
// OUTPUT — the group keys and aggregate outputs, which is what a consumer
// above reads and not what the pre-projection is compiled against.
func stageScanReadSet(s *Stage) map[string]string {
	out := make(map[string]string, len(s.Columns))
	for _, c := range s.Columns {
		if c == "" {
			continue
		}
		out[strings.ToLower(c)] = c
	}
	return out
}

// aggregateInputIsModelled reports whether a stage's output columns can be
// enumerated well enough to judge what an aggregate above it can read.
//
// Every kind stageEmittedColumns can answer for is here, and the list is the
// check's REACH: a kind left out is a feeding stage the backstop is blind to,
// which is how a DISTINCT-fed aggregate answered a silent 0 while the check
// waved it through — the DISTINCT rewrite lowers to an AGGREGATE stage, and
// the aggregate kinds were the ones missing.
//
// Only JOIN is deliberately excluded, and named as excluded for the reason
// ADR-0025 gives: a join's output is the qualified union of two sides, and
// asserting over it produces false refusals.
func aggregateInputIsModelled(typ string) bool {
	switch typ {
	case StageScan, StageSort, StageMergeSort, StageLimit, StageWindow, StageProject,
		StageExchangeRepartition, StageExchangeReplicate, StageExchangeGather,
		StageAggregate, StageFinalAggregate, StageMergeAggregate, StageUnion:
		return true
	}
	return false
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
			if k.Column == "" {
				continue
			}
			// A key still spelled `__sortkey_N` here has NO later pass. That
			// slot is the planner's own materialization of an ORDER BY term,
			// and both passes that settle one — resolveHiddenSortKeys and
			// resolveDerivedAliasSortKeys — have already run: either the term
			// is on some producer's OpProject (and is in `emitted`), or the
			// key was renamed onto a real column (and is no longer spelled
			// `__sortkey_N`), or the passes DECLINED. So the exemption below
			// does not apply to it, and a hidden key nothing emits is
			// unreachable however its Source fields are filled in.
			//
			// resolveHiddenSortKeys declines whenever the producer's fragment
			// runs no OpProject — an aggregate-family stage, a union — which
			// is exactly where fuseSortIntoPredecessor folds an ORDER BY over
			// a derived table's aggregate:
			//
			//	SELECT d.g, d.s FROM (SELECT g, SUM(id) AS s FROM t GROUP BY g) d
			//	ORDER BY d.s * 2
			//	SELECT u.k FROM (SELECT DISTINCT id AS k, g AS v FROM t) u
			//	ORDER BY u.k * 2
			//	-- both: `sort: key column "__sortkey_0" does not exist in the
			//	--   input schema`, three dispatch attempts in, on queries the
			//	--   single-process pipeline answers (#787)
			//
			// Refusing here routes them to that pipeline instead of failing.
			if logical.IsHiddenSortColumn(k.Column) {
				if _, ok := lookupEmittedColumn(emitted, k.Column); ok {
					continue
				}
				return fmt.Errorf("%w: stage %s (%s) orders on the materialized term %q and no "+
					"fragment computes it, so the task would fail at dispatch for a query the "+
					"engine can answer (#787); input: %v",
					ErrUnreachableGatherOutput, s.ID, s.Type, k.Column, sortedEmittedNames(emitted))
			}
			if k.SourceExpr != "" || k.SourceColumn != "" || k.AliasSource != "" {
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

// assertUnionArmsAgreeOnTypes refuses a plan whose set-operation arms declare
// DIFFERENT types for the same output column.
//
// A union writes one .wshf stream per arm and every consumer reads them as one
// relation, so the arms' declarations are not advice — they decide how the
// bytes are read back. Two arms that disagree hand the consumer a column whose
// declared type does not match its data: the sort above reads a FLOAT64 key
// off an INT64 vector, gets an empty typed slice, and indexes it. That is a
// runtime PANIC inside the fragment (`index out of range [0] with length 0`),
// recovered by the query boundary and reported as an internal error, for a
// query PostgreSQL answers (#656 R4).
//
// reconcileSetOpArmTypes is what makes the arms agree, and it can only do that
// for arms it can TYPE; an arm it cannot type leaves the disagreement in the
// plan. Refusing here is the backstop, and it uses the sentinel so the
// coordinator routes the query local and ANSWERS it rather than panicking.
//
// An arm that declares nothing is not a disagreement: the worker copies the
// source column and the type is whatever the producer emits — which is also
// why a bare REFERENCE that declares a type is checked against its producer
// rather than against the other arms. The worker DirectCopies such a spec and
// ignores the declaration, so two arms can agree on paper and still write
// different bytes.
func assertUnionArmsAgreeOnTypes(stages []Stage) error {
	idx := make(map[string]int, len(stages))
	for i := range stages {
		idx[stages[i].ID] = i
	}
	for i := range stages {
		s := &stages[i]
		if s.Type != StageUnion || len(s.UnionArms) == 0 {
			continue
		}
		if err := armDeclarationsMatchTheirProducers(stages, idx, s); err != nil {
			return err
		}
		if len(s.UnionArms) < 2 {
			continue
		}
		type decl struct {
			t          parquet.TypeID
			prec, scal int
			arm        int
		}
		// A column any arm COERCES is reconciled after its vector exists, not
		// by the declaration: the DECIMAL rung deliberately leaves the
		// integer arm declaring INT32 and MOVES its carrier, because
		// declaring the target would make the projection build a DECIMAL
		// vector and the checked writer refuse the int box before
		// DecimalCoerce could convert it (ADR-0012 item 12). Those columns
		// are outside this check.
		coerced := map[string]bool{}
		for a := range s.UnionArms {
			for _, c := range s.UnionArms[a].DecimalCoercions {
				coerced[strings.ToLower(c.Name)] = true
			}
		}
		seen := map[string]decl{}
		for a := range s.UnionArms {
			for _, sp := range s.UnionArms[a].Projections {
				if sp.Name == "" || !sp.TypeKnown || coerced[strings.ToLower(sp.Name)] {
					continue
				}
				key := strings.ToLower(sp.Name)
				have, ok := seen[key]
				if !ok {
					seen[key] = decl{sp.Type, sp.Precision, sp.Scale, a}
					continue
				}
				if have.t == sp.Type && have.prec == sp.Precision && have.scal == sp.Scale {
					continue
				}
				return fmt.Errorf("%w: union stage %s declares %q as %s(%d,%d) in arm %d and "+
					"%s(%d,%d) in arm %d — the consumer would read one arm's bytes at the "+
					"other arm's type (#656)", ErrUnreachableGatherOutput, s.ID, sp.Name,
					have.t, have.prec, have.scal, have.arm, sp.Type, sp.Precision, sp.Scale, a)
			}
		}
	}
	return nil
}

// armDeclarationsMatchTheirProducers checks the half two arms agreeing with
// each other cannot see.
//
// A spec whose expression is a bare column REFERENCE is copied by the worker
// straight off the producer's stream — DirectCopy, the declared type unread —
// so a declaration that disagrees with the producer is a lie the plan tells
// its consumers. Both arms declaring FLOAT64 while one of them copies an INT64
// column is exactly that, and the sort above read the key as float over int
// bytes and indexed an empty slice (#656 R4).
func armDeclarationsMatchTheirProducers(stages []Stage, idx map[string]int, s *Stage) error {
	for a := range s.UnionArms {
		dep := s.UnionArms[a].DepStage
		if _, ok := idx[dep]; !ok && a < len(s.Dependencies) {
			dep = s.Dependencies[a]
		}
		j, ok := idx[dep]
		if !ok {
			continue
		}
		decls := stageDeclaredOutputTypes(&stages[j])
		if len(decls) == 0 {
			continue
		}
		for _, sp := range s.UnionArms[a].Projections {
			if sp.Name == "" || !sp.TypeKnown || sp.Expr == "" {
				continue
			}
			src, ok := bareColumnRefName(sp.Expr)
			if !ok {
				continue // a computed arm builds its own vector from the declaration
			}
			have, ok := decls[strings.ToLower(src)]
			if !ok || have == sp.Type {
				continue
			}
			return fmt.Errorf("%w: union stage %s arm %d copies %q from %s, which emits it as "+
				"%s, and declares it %s — the worker copies the column and the consumer reads "+
				"the bytes at the declared type (#656)", ErrUnreachableGatherOutput,
				s.ID, a, src, stages[j].ID, have, sp.Type)
		}
	}
	return nil
}

// bareColumnRefName returns the column a spec expression names, when the
// expression is nothing but a reference (delimited or not).
func bareColumnRefName(exprText string) (string, bool) {
	ast, err := plansql.ParseExpression(exprText)
	if err != nil {
		return "", false
	}
	ref, ok := ast.(*plansql.ColRef)
	if !ok || ref.Table != "" {
		return "", false
	}
	return ref.Column, true
}

// stageDeclaredOutputTypes is what a stage says its own output columns are.
func stageDeclaredOutputTypes(s *Stage) map[string]parquet.TypeID {
	out := map[string]parquet.TypeID{}
	for _, sp := range s.ProjectExprs {
		if sp.Name != "" && sp.TypeKnown {
			out[strings.ToLower(sp.Name)] = sp.Type
		}
	}
	if len(s.ProjectExprs) > 0 {
		return out // a projection NARROWS to exactly its outputs
	}
	for k, t := range s.GroupByTypes {
		out[strings.ToLower(k)] = t
	}
	for _, ag := range s.AggSpecs {
		if ag.OutputCol != "" && ag.OutputTypeKnown {
			out[strings.ToLower(ag.OutputCol)] = ag.OutputType
		}
	}
	return out
}
