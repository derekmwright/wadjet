// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// SetOpLeftCountCol / SetOpRightCountCol are the per-arm tag columns an
// INTERSECT/EXCEPT lowering appends to each arm's projection: arm 0 tags
// every row (1, 0), arm 1 tags (0, 1). SUMming them under a GROUP BY over
// the full result row yields (rows in arm A, rows in arm B) per distinct
// row — the entire state the operation's count rule needs. Exported because
// the coordinator's fragment builder names the same columns in the emit
// operator's OpSpec.
const (
	SetOpLeftCountCol  = "__setop_lcnt"
	SetOpRightCountCol = "__setop_rcnt"
)

// emitSetOpStages lowers all arms onto one DAG result (#346). UNION ALL uses
// StageUnion: task i reads arm i's whole output and projects result names/types.
// UNION adds Singleton GroupByAll dedup; one task holds the whole distinct set.
// INTERSECT/EXCEPT append (1,0)/(0,1) arm tags and group by the full result row,
// summing tags. Repartition on all result columns co-locates equal rows, including
// NULLs, so each partition answers independently. The SetOp emit operator applies
// membership/multiplicity rules to counts and drops tags.
// See docs/internals/set-operation-stage-lowering.md for the design.
func (p *StagePlanner) emitSetOpStages(node *logical.Node, stages *[]Stage) {
	if len(node.Children) < 2 {
		p.refuseSetOp(fmt.Errorf("distributed planning: %s has %d arms, expected at least 2",
			p.PlanContext.SetOpName(node), len(node.Children)))
		return
	}
	counting := node.Type != logical.NodeUnion
	if counting && len(node.Children) != 2 {
		// INTERSECT/EXCEPT are built binary (left-deep chains nest as
		// arms); anything else is a malformed plan, not a shape to guess at.
		p.refuseSetOp(fmt.Errorf("distributed planning: %s has %d arms, expected exactly 2. See issue #346",
			p.PlanContext.SetOpName(node), len(node.Children)))
		return
	}

	// The NO-COMMON-TYPE refusal first, over EVERY result column, before any
	// other disposition this lowering can reach. reconcileSetOpArmTypes below
	// walks the columns in order and returns on the first one it cannot
	// reconcile, and several of those refusals are about this engine's own
	// carriers rather than about the query's meaning — so a query that is
	// 42804 in PostgreSQL took whichever message the leftmost unreconcilable
	// column happened to produce, and the single-process path (which calls
	// this same walk from buildSetOp) took a different one. One query, one
	// answer (#648).
	if err := p.PlanContext.SetOpArmTypeConflict(node); err != nil {
		p.refuseSetOp(err)
		return
	}

	// SQL takes the result column names from the FIRST arm; every arm is
	// projected onto them so the arms' outputs are one schema and the
	// concatenation is well defined.
	outNames := p.PlanContext.SetOpOutputNames(node.Children[0])
	if len(outNames) == 0 {
		p.refuseSetOp(fmt.Errorf(
			"%s is not supported by distributed (stage-DAG) execution for this shape: the first "+
				"arm has no resolvable output column list, so the arms cannot be projected onto a "+
				"common schema. See issue #346", p.PlanContext.SetOpName(node)))
		return
	}
	if counting {
		for _, n := range outNames {
			if n == SetOpLeftCountCol || n == SetOpRightCountCol {
				p.refuseSetOp(fmt.Errorf(
					"%s: result column %q collides with the operation's internal count column. See issue #346",
					p.PlanContext.SetOpName(node), n))
				return
			}
		}
	}

	plans := make([]physical.SetOpArmPlan, 0, len(node.Children))
	deps := make([]string, 0, len(node.Children))
	for i, child := range node.Children {
		start := len(*stages)
		p.walkStages(child, stages, nil)
		leaves := leafStages((*stages)[start:])
		if len(leaves) != 1 {
			p.refuseSetOp(fmt.Errorf(
				"%s is not supported by distributed (stage-DAG) execution for this shape: arm %d "+
					"lowered to %d terminal stages, expected exactly 1. See issue #346",
				p.PlanContext.SetOpName(node), i+1, len(leaves)))
			return
		}
		plan, err := p.PlanContext.SetOpArmProjection(child, outNames)
		if err != nil {
			p.refuseSetOp(fmt.Errorf("%s: arm %d: %w. See issue #346", p.PlanContext.SetOpName(node), i+1, err))
			return
		}
		plans = append(plans, plan)
		deps = append(deps, leaves[0])
	}
	unknownLits := make([][]bool, 0, len(node.Children))
	for _, child := range node.Children {
		unknownLits = append(unknownLits, p.PlanContext.SetOpUnknownLiteralArms(child, len(outNames)))
	}
	if err := reconcileSetOpArmTypes(plans, outNames, p.PlanContext.SetOpBaseName(node), unknownLits); err != nil {
		if sqlerr.StateOf(err) != "" {
			// A refusal that already carries PostgreSQL's SQLSTATE and wording
			// is the client's answer as written, with no distributed-planning
			// preamble in front of it (#648).
			p.refuseSetOp(err)
			return
		}
		p.refuseSetOp(fmt.Errorf("%s: %w. See issue #346", p.PlanContext.SetOpName(node), err))
		return
	}

	arms := make([]UnionArm, len(plans))
	for i := range plans {
		arms[i] = UnionArm{
			Projections:      plans[i].Specs,
			DecimalCoercions: plans[i].Coerce,
		}
	}
	if counting {
		// Tag columns ride AFTER the reconciled result columns so
		// reconcileSetOpArmTypes' per-index bookkeeping above stays
		// aligned. Complementary constants: SUM(left tag) per group is the
		// row's multiplicity in arm A, SUM(right tag) in arm B.
		for i := range arms {
			l, r := "1", "0"
			if i == 1 {
				l, r = "0", "1"
			}
			arms[i].Projections = append(arms[i].Projections,
				physical.ProjectExprSpec{Expr: l, Name: SetOpLeftCountCol, Type: parquet.TypeInt64, TypeKnown: true},
				physical.ProjectExprSpec{Expr: r, Name: SetOpRightCountCol, Type: parquet.TypeInt64, TypeKnown: true})
		}
	}
	unionID := fmt.Sprintf("union-%d", len(*stages))
	*stages = append(*stages, Stage{
		ID:           unionID,
		Type:         StageUnion,
		Tasks:        len(arms),
		Dependencies: deps,
		UnionArms:    arms,
	})

	switch {
	case counting:
		p.emitSetOpCountingStage(stages, unionID, node, outNames)
	case !node.UnionAll:
		p.emitSetOpDedup(stages, unionID)
	}
}

// emitSetOpCountingStage groups tagged INTERSECT/EXCEPT concatenation by the
// full result row and SUMs both tags. RawInputAggregate forbids merge-mode spec
// rewriting: the exchange repartitions RAW rows, not partial aggregates.
// Keep SortKeys/Limit empty to require ClusteredOn(GroupByCols); EnsureDistribution
// then repartitions and dispatches one task per partition. A later fused sort
// may correctly collapse this to Singleton. Deterministic NULL hash markers and
// HashAggregate's NULL equality preserve set membership semantics.
func (p *StagePlanner) emitSetOpCountingStage(stages *[]Stage, unionID string, node *logical.Node, outNames []string) {
	op := "intersect"
	if node.Type == logical.NodeExcept {
		op = "except"
	}
	*stages = append(*stages, Stage{
		ID:          fmt.Sprintf("final_aggregate-%d", len(*stages)),
		Type:        "final_aggregate",
		Tasks:       1,
		GroupByCols: append([]string(nil), outNames...),
		// The result columns are positions 0..n-1 of the union stage's output
		// by construction — physical.PlanContext.SetOpArmProjection emits exactly them, in order,
		// with the two tag columns appended AFTER. Two of those names may be
		// the same string, and then the name is not an address: both keys
		// resolved to column one and `EXCEPT` answered 0 rows where
		// PostgreSQL answers 4 (#1022, ADR-0026 §3a).
		GroupByColIdx: setOpKeyPositions(len(outNames)),
		// A RawInputAggregate reads the union's RAW rows, so it computes its
		// keys and carries a resolution list. Here the two names are the same
		// string — the set operation's result columns are what every arm's
		// projection publishes — and saying so explicitly is what keeps the
		// worker off the text-parsing recovery (ADR-0026 §2).
		GroupByResolve: identityGroupKeyResolutions(outNames),
		AggSpecs: []AggSpec{
			{Func: "SUM", InputCol: SetOpLeftCountCol, OutputCol: SetOpLeftCountCol,
				OutputType: parquet.TypeInt64, OutputTypeKnown: true},
			{Func: "SUM", InputCol: SetOpRightCountCol, OutputCol: SetOpRightCountCol,
				OutputType: parquet.TypeInt64, OutputTypeKnown: true},
		},
		RawInputAggregate: true,
		SetOp:             op,
		SetOpAll:          node.UnionAll,
		Dependencies:      []string{unionID},
	})
}

// emitSetOpDedup appends the DISTINCT half of a bare UNION: a keys-only hash
// aggregate over every column of the concatenation (Stage.GroupByAll, the
// shape exec.HashAggregate and the worker's fragment builder already speak).
//
// Singleton by construction — one task sees every row of both arms. That is a
// scalability bound, not a correctness one, and it is the same bound the
// coordinator's existing DISTINCT fallback carries (dedupGatherResult). The
// sharded alternative is a hash exchange on all output columns feeding N
// per-partition dedups; the exchange's row hash already has the property that
// makes it sound (identical rows hash identically, so equal rows always land
// in the same partition), so this can become sharded without touching
// anything emitted here.
func (p *StagePlanner) emitSetOpDedup(stages *[]Stage, unionID string) {
	*stages = append(*stages, Stage{
		ID:           fmt.Sprintf("final_aggregate-%d", len(*stages)),
		Type:         "final_aggregate",
		Tasks:        1,
		GroupByAll:   true,
		Dependencies: []string{unionID},
	})
}

// refuseSetOp parks the first refusal; PlanDistributed returns it. First one
// wins so a nested set operation's specific message is not overwritten by an
// outer one's.
func (p *StagePlanner) refuseSetOp(err error) {
	if p.setOpErr == nil {
		p.setOpErr = err
	}
}

// setOpKeyPositions is the identity position list for a set operation's n
// result columns, the group-key twin of identityGroupKeyResolutions above.
func setOpKeyPositions(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}

// reconcileSetOpArmTypes makes every arm's file emit the same column types.
// Numeric widening uses casts or value-moving DECIMAL coercion; refuse unsupported
// disagreements rather than invent a number-to-text conversion. Reconcile DECIMAL
// (p,s) even when TypeIDs already match, or file headers reinterpret scale (#533).
// unknown marks per-arm/per-column UNKNOWN literals: they take other arms' types
// without casts, with SetValueChecked parsing text into the reconciled vector
// (#648).
func reconcileSetOpArmTypes(plans []physical.SetOpArmPlan, outNames []string, op string, unknown [][]bool) error {
	if len(plans) < 2 {
		return nil
	}
	for col := range outNames {
		want, allKnown, err := localPlanFacts.SetOpTargetType(plans, col, outNames[col], op, unknown)
		if err != nil {
			return err
		}
		if !want.Known {
			continue // nothing typed this column; leave every arm as written
		}
		// Cast only when every arm's type is known — an untyped arm cannot
		// be cast to match, and forcing the typed ones alone would just move
		// the mismatch.
		if !allKnown {
			// Except when a known arm is DECIMAL. Then "leave it alone" is
			// not neutral: the untyped arm writes its own .wshf at whatever
			// scale it happens to carry, the stage that reads both takes the
			// first header's, and the values come back a power of ten out
			// with nothing able to see it. That is #551's channel reached
			// through allKnown rather than through decKnown, and it is the
			// one the SQL shapes actually take — a join arm with a DERIVED
			// side resolves to no type at all, not to a DECIMAL with no
			// (p,s). Refuse, naming the column.
			if setOpAnyDecimalArm(plans, col) {
				return fmt.Errorf("result column %q is DECIMAL in one arm and its type cannot be "+
					"resolved in %s — a set operation moves every arm into one DECIMAL(precision, "+
					"scale) and an arm with no resolved type cannot be moved; give the arm an "+
					"explicit CAST to a DECIMAL(p,s), or select the column directly",
					outNames[col], setOpUntypedArmsDesc(plans, col))
			}
			continue
		}
		if want.Typ == parquet.TypeDecimal {
			if !want.DecKnown {
				// An arm whose (p,s) nothing resolved. Leaving every arm as
				// written was the pre-#533 behaviour, and it is a SILENT
				// WRONG ANSWER: each arm's task writes its own .wshf file at
				// its own scale, and the downstream stage that reads several
				// of them takes the FIRST header's — so the wider arm's
				// unscaled integer comes back a power of ten out, with
				// nothing upstream of the reader able to see it (#551, and
				// ADR-0012 item 12's "the answer is WRONG — not refused").
				//
				// So it is refused, naming the column. A guessed scale moves
				// values; a refusal is a loud failure where this was a quiet
				// wrong number, and ADR-0012 item 12 already calls that "the
				// honest interim".
				return fmt.Errorf("result column %q is DECIMAL in %s, and its precision and scale "+
					"cannot be resolved from the query — a set operation moves every arm into one "+
					"DECIMAL(precision, scale) and there is no scale to move them to; give the arm "+
					"an explicit CAST to a DECIMAL(p,s), or select the column directly",
					outNames[col], setOpUnresolvedArmsDesc(plans, col))
			}
			for i := range plans {
				if localPlanFacts.SetOpArmIsUnknownLit(unknown, i, col) {
					// An UNKNOWN literal takes the resolved type, and the ARM'S
					// OWN STAGE has to say so: the union arm's projection is
					// what the worker builds the .wshf column from, and a
					// literal left declared STRING wrote a STRING column into a
					// file the next stage reads beside a DECIMAL one.
					plans[i].Specs[col].Type = want.Typ
					plans[i].Specs[col].Fields = want.Fields
					plans[i].Specs[col].ElementType = want.ElementType
					plans[i].Specs[col].TypeKnown = true
					plans[i].Specs[col].Precision = want.Dec.Precision
					plans[i].Specs[col].Scale = want.Dec.Scale
					continue
				}
				ct := plans[i].Types[col]
				// Declare each spec at its arm's OWN type and (p,s): worker vectors exist
				// BEFORE DecimalCoerce rewrites their unscaled carriers, including integers at
				// scale 0. Stamping the target would reject computed integer boxes before
				// coercion (#551; ADR-0018 §4, ADR-0024 item 4). Bare DirectCopy ignores the
				// spec and types from input, so it does not test that seam. Do not leave the
				// spec zero-valued: that declares BOOL without (p,s), and DECIMAL would be
				// read at scale 0 (ADR-0024 item 2).
				if ct.Known {
					plans[i].Specs[col].Type = ct.Typ
					plans[i].Specs[col].TypeKnown = true
					plans[i].Specs[col].Precision = ct.Dec.Precision
					plans[i].Specs[col].Scale = ct.Dec.Scale
				}
				if ct.Typ == want.Typ && ct.DecKnown && ct.Dec == want.Dec {
					continue
				}
				plans[i].Coerce = append(plans[i].Coerce, physical.DecimalCoercion{
					Name:      outNames[col],
					Precision: want.Dec.Precision,
					Scale:     want.Dec.Scale,
				})
				plans[i].Types[col] = want
			}
			continue
		}
		for i := range plans {
			if localPlanFacts.SetOpArmIsUnknownLit(unknown, i, col) {
				// The resolved type, DECLARED on the arm's own projection, and
				// no CAST: SetValueChecked parses the literal's text into
				// whatever vector the spec names, which is what PostgreSQL
				// means by an unknown literal taking the other arm's type. Left
				// at STRING, the arm's .wshf file carried a STRING column and
				// the consumer refused it — `column "v" is STRING … but IPV4 in
				// an earlier file of the same stage input` (ADR-0010) — after
				// doing the work, where the single-process path answered.
				plans[i].Specs[col].Type = want.Typ
				plans[i].Specs[col].Fields = want.Fields
				plans[i].Specs[col].ElementType = want.ElementType
				plans[i].Specs[col].TypeKnown = true
				plans[i].Types[col] = want
				continue
			}
			if plans[i].Types[col].Typ == want.Typ {
				// Two ARRAY arms can still disagree about the ELEMENT
				// (`int[] ∪ bigint[]`); the narrower arm converts its
				// elements, or the stage writes two element types into one
				// column (arc CW).
				// Two DECIMAL elements of different (p,s) meet at the common
				// one (setOpElementTarget): the arm casts to that
				// `DECIMAL(p,s)[]`, whose declared element the cast writes
				// at the target scale (round 4, B3).
				if el, ae := want.ElementType, plans[i].Types[col].ElementType; el != nil && ae != nil &&
					el.Type == parquet.TypeDecimal && ae.Type == parquet.TypeDecimal &&
					(ae.Precision != el.Precision || ae.Scale != el.Scale) {
					plans[i].Specs[col].Expr = fmt.Sprintf("CAST(%s AS DECIMAL(%d,%d)[])",
						plans[i].Specs[col].Expr, el.Precision, el.Scale)
					plans[i].Specs[col].Type = want.Typ
					plans[i].Specs[col].ElementType = el
					plans[i].Specs[col].TypeKnown = true
					plans[i].Types[col] = physical.SetOpColType{Typ: want.Typ, Known: true, ElementType: el}
					continue
				}
				if el, ae := want.ElementType, plans[i].Types[col].ElementType; el != nil && ae != nil && ae.Type != el.Type {
					cast, ok := setOpCastExpr("x", ae.Type, el.Type)
					if !ok {
						return fmt.Errorf("result column %q must be an array of %s to match the other arms, "+
							"and arm %d's elements cannot be cast to it", outNames[col], el.Type, i+1)
					}
					elemName := strings.TrimSuffix(cast[strings.LastIndex(cast, " AS ")+4:], ")")
					plans[i].Specs[col].Expr = "CAST(" + plans[i].Specs[col].Expr + " AS " + elemName + "[])"
					plans[i].Specs[col].Type = want.Typ
					plans[i].Specs[col].ElementType = el
					plans[i].Specs[col].TypeKnown = true
					plans[i].Types[col] = physical.SetOpColType{Typ: want.Typ, Known: true, ElementType: el}
				}
				continue
			}
			cast, ok := setOpCastExpr(plans[i].Specs[col].Expr, plans[i].Types[col].Typ, want.Typ)
			if !ok {
				return fmt.Errorf("result column %q must be %s to match the other arms, and arm %d's "+
					"value cannot be cast to it", outNames[col], want.Typ, i+1)
			}
			plans[i].Specs[col].Expr = cast
			plans[i].Specs[col].Type = want.Typ
			plans[i].Specs[col].Fields = want.Fields
			plans[i].Specs[col].ElementType = want.ElementType
			plans[i].Specs[col].TypeKnown = true
			plans[i].Types[col] = physical.SetOpColType{Typ: want.Typ, Known: true, ElementType: want.ElementType}
		}
	}
	return nil
}

func setOpAnyDecimalArm(plans []physical.SetOpArmPlan, col int) bool {
	for i := range plans {
		if ct := plans[i].Types[col]; ct.Known && ct.Typ == parquet.TypeDecimal {
			return true
		}
	}
	return false
}

// setOpUntypedArmsDesc names the arms the walk could not type at all, with the
// expression each one selects.
func setOpUntypedArmsDesc(plans []physical.SetOpArmPlan, col int) string {
	var arms []string
	for i := range plans {
		if !plans[i].Types[col].Known {
			arms = append(arms, fmt.Sprintf("arm %d (%s)", i+1, plans[i].Specs[col].Expr))
		}
	}
	if len(arms) == 0 {
		return "one of its arms"
	}
	return strings.Join(arms, " and ")
}

// setOpUnresolvedArmsDesc names the arms whose DECIMAL (p,s) the walk could
// not resolve, with the expression each one selects — the localization the
// refusal above owes its reader, since the column NAME is the same in every
// arm by construction.
func setOpUnresolvedArmsDesc(plans []physical.SetOpArmPlan, col int) string {
	var arms []string
	for i := range plans {
		ct := plans[i].Types[col]
		if ct.Typ == parquet.TypeDecimal && !ct.DecKnown {
			arms = append(arms, fmt.Sprintf("arm %d (%s)", i+1, plans[i].Specs[col].Expr))
		}
	}
	if len(arms) == 0 {
		return "one of its arms"
	}
	return strings.Join(arms, " and ")
}

// setOpCastExpr wraps an arm's expression so it produces the reconciled type.
// The destination spellings are the ones expr.Cast understands.
//
// `from` is the arm's OWN declared type, and it is not decoration: a widening
// that LOSES the narrower type's rounding has to narrow FIRST. `real ∪ float8`
// is the one such rung in physical.setOpWiden's ladder, and it is a VALUE:
// `w_f32 + CAST(1.0 AS REAL)` over 2^24 is 16777216 as a real and 16777217 as
// a double, and this engine computes every float expression on the float64
// carrier. The single-process path narrows because the arm's projection
// materializes into the FLOAT32 vector its own declaration names; the stage
// arms declare the RECONCILED type on that same projection, so nothing
// narrowed and one expression answered two values depending on the arm
// (#1117, found by arc ND's review). An inner `CAST(… AS REAL)` is what the
// single path's store does, spelled where both paths can see it.
func setOpCastExpr(e string, from, to parquet.TypeID) (string, bool) {
	if from == parquet.TypeFloat32 && to != parquet.TypeFloat32 {
		e = "CAST(" + e + " AS REAL)"
	}
	switch to {
	case parquet.TypeInt64:
		return "CAST(" + e + " AS BIGINT)", true
	case parquet.TypeFloat32:
		// The evaluator's REAL arm produces a float64 box; the FLOAT32 the
		// projection declares is what narrows it at the store
		// (Vector.SetValue's TypeFloat32 arm). Both halves are needed: without
		// the cast an integer or DECIMAL arm keeps its own box, and without
		// the declaration the column would be float8 and render a real's 0.1
		// as 0.10000000149011612.
		return "CAST(" + e + " AS REAL)", true
	case parquet.TypeFloat64:
		return "CAST(" + e + " AS DOUBLE)", true
	}
	return "", false
}

// respellUnionArmProjections rewrites every union arm's projection against the
// columns its producer really emits.
//
// An arm's projection is written in the QUERY's spelling, and a producer may
// name a column something else: above an aggregate a computed group key is
// emitted under the TEXT of its GROUP BY expression, so an arm projecting
// `n_regionkey + 1 AS gk` rebuilds ARITHMETIC over `n_regionkey`, which that
// stage does not emit, and every row of the union answers NULL.
// `WITH a AS (SELECT g+1 AS gk, COUNT(*) AS n FROM t GROUP BY g+1) SELECT gk
// FROM a UNION ALL SELECT gk FROM a ORDER BY gk` returned sixteen NULLs where
// PostgreSQL returns 1..7 and one NULL.
//
// It runs LATE in PlanDistributed, after flattenCTEAliases: a deduped CTE
// reference names a `cte-alias` phantom until that pass repoints it at the
// body's terminal, so an arm respelled at emission time would see no producer
// at all — which is why the SECOND arm of the query above stayed NULL when
// this ran beside the arm construction.
//
// An arm whose term has no spelling over the producer's output is left exactly
// as written; assertCarrierSchemaResolves is what reports that.
func respellUnionArmProjections(stages []Stage) {
	idx := make(map[string]int, len(stages))
	for i := range stages {
		idx[stages[i].ID] = i
	}
	for i := range stages {
		s := &stages[i]
		if s.Type != StageUnion {
			continue
		}
		for a := range s.UnionArms {
			j, ok := idx[s.UnionArmDep(a)]
			if !ok {
				continue
			}
			if re, ok := respellSpecsOverProducerOutput(stages, j, s.UnionArms[a].Projections); ok {
				s.UnionArms[a].Projections = re
			}
		}
	}
}
