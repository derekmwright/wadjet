package physical

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
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

// emitSetOpStages lowers a set-operation node onto the stage DAG.
//
// walkStages used to walk both arms and emit nothing else, on the comment
// "each side runs independently; merge results at the end" — and nothing
// merged. The terminal gather then attached to whichever arm happened to be
// emitted last, so `SELECT r_regionkey FROM region UNION ALL SELECT
// r_regionkey FROM region` answered with five rows carrying r_regionkey,
// r_name and r_comment: one arm, unprojected, at half the row count (#346).
//
// What is emitted here:
//
//	UNION ALL  → one StageUnion. Arm i is dispatched as task i, reads its
//	             arm's whole output, and projects it onto the result column
//	             names and types; the stage's files are therefore the
//	             concatenation.
//	UNION      → the same StageUnion plus a GroupByAll final_aggregate that
//	             dedups the concatenation. The dedup is Singleton: correct,
//	             but one task holds the whole distinct set (see the note on
//	             emitSetOpDedup).
//	INTERSECT  → the same StageUnion with per-arm TAG columns appended
//	EXCEPT       (arm 0 rows carry (1,0), arm 1 rows (0,1)), then a grouped
//	             counting final_aggregate: GROUP BY the full result row,
//	             SUM the tags. The distribution pass inserts an
//	             exchange-repartition on the full row between the two
//	             (StageUnion is RoundRobin, a grouped final requires
//	             ClusteredOn its group keys), so equal rows from both arms
//	             — NULLs included, the shuffle hash marks them
//	             deterministically — meet in one partition and each
//	             partition is independently answerable. The stage's SetOp
//	             marker makes its fragment append an emit operator that
//	             turns each group's (countA, countB) into rows per the
//	             operation's rule and drops the tags. See
//	             emitSetOpCountingStage.
func (p *Planner) emitSetOpStages(node *logical.Node, stages *[]Stage) {
	if len(node.Children) < 2 {
		p.refuseSetOp(fmt.Errorf("distributed planning: %s has %d arms, expected at least 2",
			setOpName(node), len(node.Children)))
		return
	}
	counting := node.Type != logical.NodeUnion
	if counting && len(node.Children) != 2 {
		// INTERSECT/EXCEPT are built binary (left-deep chains nest as
		// arms); anything else is a malformed plan, not a shape to guess at.
		p.refuseSetOp(fmt.Errorf("distributed planning: %s has %d arms, expected exactly 2. See issue #346",
			setOpName(node), len(node.Children)))
		return
	}

	// SQL takes the result column names from the FIRST arm; every arm is
	// projected onto them so the arms' outputs are one schema and the
	// concatenation is well defined.
	outNames := setOpOutputNames(node.Children[0])
	if len(outNames) == 0 {
		p.refuseSetOp(fmt.Errorf(
			"%s is not supported by distributed (stage-DAG) execution for this shape: the first "+
				"arm has no resolvable output column list, so the arms cannot be projected onto a "+
				"common schema. See issue #346", setOpName(node)))
		return
	}
	if counting {
		for _, n := range outNames {
			if n == SetOpLeftCountCol || n == SetOpRightCountCol {
				p.refuseSetOp(fmt.Errorf(
					"%s: result column %q collides with the operation's internal count column. See issue #346",
					setOpName(node), n))
				return
			}
		}
	}

	plans := make([]setOpArmPlan, 0, len(node.Children))
	deps := make([]string, 0, len(node.Children))
	for i, child := range node.Children {
		start := len(*stages)
		p.walkStages(child, stages, nil)
		leaves := leafStages((*stages)[start:])
		if len(leaves) != 1 {
			p.refuseSetOp(fmt.Errorf(
				"%s is not supported by distributed (stage-DAG) execution for this shape: arm %d "+
					"lowered to %d terminal stages, expected exactly 1. See issue #346",
				setOpName(node), i+1, len(leaves)))
			return
		}
		plan, err := setOpArmProjection(child, outNames)
		if err != nil {
			p.refuseSetOp(fmt.Errorf("%s: arm %d: %w. See issue #346", setOpName(node), i+1, err))
			return
		}
		plans = append(plans, plan)
		deps = append(deps, leaves[0])
	}
	if err := reconcileSetOpArmTypes(plans, outNames); err != nil {
		p.refuseSetOp(fmt.Errorf("%s: %w. See issue #346", setOpName(node), err))
		return
	}

	arms := make([]UnionArm, len(plans))
	for i := range plans {
		arms[i] = UnionArm{
			DepStage:         deps[i],
			Projections:      plans[i].specs,
			DecimalCoercions: plans[i].coerce,
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
				ProjectExprSpec{Expr: l, Name: SetOpLeftCountCol, Type: parquet.TypeInt64, TypeKnown: true},
				ProjectExprSpec{Expr: r, Name: SetOpRightCountCol, Type: parquet.TypeInt64, TypeKnown: true})
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

// emitSetOpCountingStage appends the counting half of an INTERSECT/EXCEPT: a
// grouped final_aggregate over the tagged concatenation, GROUP BY the full
// result row, SUMming the two tag columns. RawInputAggregate because the
// input is raw tagged rows (the exchange re-partitions the concatenation, it
// does not pre-aggregate), so the dispatcher must not run the merge-mode
// spec rewrite.
//
// The stage deliberately carries no SortKeys/Limit, which is what makes
// RequiredChildDistribution demand ClusteredOn(GroupByCols) — the full
// result row — and EnsureDistribution insert the co-partitioning
// exchange-repartition over the union. OutputDistribution then mirrors the
// exchange's partitioning, so dispatchComputeStage fans the counting out one
// task per partition: the sharded path, not a Singleton bottleneck. (A sort
// later folded in by fuseSortIntoPredecessor collapses the stage to
// Singleton via the same rules that govern every grouped final — correct,
// serial.)
//
// NULL semantics ride existing machinery end to end: the shuffle hash marks
// NULL key cells with a deterministic byte (equal rows co-locate) and
// HashAggregate groups NULLs as equal, which is exactly the membership rule
// SQL gives set operations.
func (p *Planner) emitSetOpCountingStage(stages *[]Stage, unionID string, node *logical.Node, outNames []string) {
	op := "intersect"
	if node.Type == logical.NodeExcept {
		op = "except"
	}
	*stages = append(*stages, Stage{
		ID:          fmt.Sprintf("final_aggregate-%d", len(*stages)),
		Type:        "final_aggregate",
		Tasks:       1,
		GroupByCols: append([]string(nil), outNames...),
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
func (p *Planner) emitSetOpDedup(stages *[]Stage, unionID string) {
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
func (p *Planner) refuseSetOp(err error) {
	if p.setOpErr == nil {
		p.setOpErr = err
	}
}

func setOpName(node *logical.Node) string {
	base := "UNION"
	switch node.Type {
	case logical.NodeIntersect:
		base = "INTERSECT"
	case logical.NodeExcept:
		base = "EXCEPT"
	}
	if node.UnionAll {
		return base + " ALL"
	}
	return base
}

func isSetOpNode(n *logical.Node) bool {
	return n != nil &&
		(n.Type == logical.NodeUnion || n.Type == logical.NodeIntersect || n.Type == logical.NodeExcept)
}

// setOpUnwrap descends the wrappers findOutputProjectionNode descends
// (ORDER BY / LIMIT / WHERE / DISTINCT above an arm) and returns the first
// node that produces rows. Used to recognise an arm that is ITSELF a set
// operation — `a UNION ALL b UNION ALL c` parses left-deep, so the outer
// union's first arm is another union node with no projection of its own.
func setOpUnwrap(n *logical.Node) *logical.Node {
	for n != nil {
		switch n.Type {
		case logical.NodeSort, logical.NodeLimit, logical.NodeFilter, logical.NodeDistinct:
			if len(n.Children) == 1 {
				n = n.Children[0]
				continue
			}
		}
		return n
	}
	return nil
}

// setOpOutputNames is the set operation's result column list, taken from the
// first arm's SELECT list as SQL requires — through any nesting, since a
// chain of unions takes its names from the leftmost arm of the whole chain.
// Names are lowercased to match the convention the rest of the DAG's
// projection plumbing uses.
func setOpOutputNames(arm *logical.Node) []string {
	inner := setOpUnwrap(arm)
	if isSetOpNode(inner) && len(inner.Children) > 0 {
		return setOpOutputNames(inner.Children[0])
	}
	// `SELECT * FROM t` builds no Project at all — the arm IS the scan, and
	// its output columns are the table's, in catalog order.
	if inner != nil && inner.Type == logical.NodeScan && len(inner.ScanColumns) > 0 {
		names := make([]string, len(inner.ScanColumns))
		for i, c := range inner.ScanColumns {
			names[i] = strings.ToLower(c)
		}
		return names
	}
	proj := findOutputProjectionNode(arm)
	if proj == nil || len(proj.Projections) == 0 {
		return nil
	}
	names := make([]string, 0, len(proj.Projections))
	for _, pr := range proj.Projections {
		name := pr.Alias
		if name == "" {
			name = pr.Expr
		}
		if name == "" {
			name = pr.Column
		}
		if name == "" {
			return nil
		}
		names = append(names, strings.ToLower(name))
	}
	return names
}

// setOpArmPlan is one arm's contribution to the union stage: the projection
// that renames/computes its columns, plus the plan-time output type of each,
// which reconcileSetOpArmTypes needs to make the arms concatenable.
type setOpArmPlan struct {
	specs []ProjectExprSpec
	types []setOpColType
	// coerce names the columns whose VALUES this arm must move before they
	// enter the union stream — a DECIMAL carrier at the wrong scale, or an
	// integer that has to become one (#533). A CAST cannot do this job: the
	// cast evaluator produces a float64 for a DECIMAL destination, which is
	// exactly the precision loss the exact carrier exists to avoid.
	coerce []DecimalCoercion
}

// setOpColType is a plan-time output type, or the absence of one. There is no
// spare TypeID to mean "unknown" — TypeBool is the zero value — so the flag
// carries it.
type setOpColType struct {
	typ   parquet.TypeID
	known bool
	// dec is a DECIMAL column's declared precision and scale: the two facts
	// a bare TypeID cannot express, and the ones two DECIMAL arms can
	// DISAGREE on while looking identical to a TypeID comparison. That is
	// #533 — reconcileSetOpArmTypes saw one TypeID on both arms, reconciled
	// nothing, and the wider arm's unscaled Int128 was then read at the
	// narrower arm's scale, 100x too large.
	//
	// decKnown is false for a DECIMAL whose (p,s) the arm walk could not
	// resolve — a computed expression carries none, the same case
	// declaredProjectionDecimal declines (#458).
	dec      logical.DecimalMeta
	decKnown bool
}

// setOpArmProjection builds the OpProject spec list that puts one arm's
// output under the set operation's result column names, plus each column's
// plan-time type.
//
// The projection runs in the union stage's own fragment, over the arm's
// materialized output, so it works the same whether the arm ended in a scan,
// a filter-scan, a join or a sort. Aggregate outputs are the exception: they
// exist under names the aggregate machinery chose, not under the SELECT
// list's expression text, so those arms are refused rather than guessed at.
func setOpArmProjection(arm *logical.Node, outNames []string) (setOpArmPlan, error) {
	inner := setOpUnwrap(arm)
	// A nested set operation already projected ITS arms onto ITS OWN result
	// names; read the arm through those, not through a projection it does
	// not have.
	if isSetOpNode(inner) {
		innerNames := setOpOutputNames(inner)
		if len(innerNames) != len(outNames) {
			return setOpArmPlan{}, fmt.Errorf("nested %s emits %d columns, the enclosing set operation has %d",
				setOpName(inner), len(innerNames), len(outNames))
		}
		plan := setOpArmPlan{
			specs: make([]ProjectExprSpec, len(outNames)),
			types: make([]setOpColType, len(outNames)),
		}
		for i, n := range innerNames {
			plan.specs[i] = ProjectExprSpec{Expr: n, Name: outNames[i]}
		}
		// The nested operation's OWN reconciliation decides what this arm
		// actually emits, so ask for it rather than reporting "unknown".
		// Reporting unknown is what made `a UNION ALL b UNION ALL c` — which
		// parses left-deep, so arm 1 of the outer union IS a union — skip
		// reconciliation entirely: the enclosing operation saw one typed arm
		// and one untyped one, declined to cast either, and the three files
		// then disagreed about the column. For a DECIMAL that is #533 again
		// one level up; for the INT32/INT64/FLOAT64 ladder it dropped a whole
		// arm's rows on the floor.
		if inferred := setOpNodeResultTypes(inner); len(inferred) == len(outNames) {
			plan.types = inferred
		}
		return plan, nil
	}
	// A bare scan arm (`SELECT * FROM t`): the columns correspond by catalog
	// order, which is the order the star expands in.
	if inner != nil && inner.Type == logical.NodeScan && len(inner.ScanColumns) > 0 {
		if len(inner.ScanColumns) != len(outNames) {
			return setOpArmPlan{}, fmt.Errorf("selects %d columns, the first arm selects %d",
				len(inner.ScanColumns), len(outNames))
		}
		plan := setOpArmPlan{
			specs: make([]ProjectExprSpec, len(outNames)),
			types: make([]setOpColType, len(outNames)),
		}
		for i, c := range inner.ScanColumns {
			lc := strings.ToLower(c)
			plan.specs[i] = ProjectExprSpec{Expr: lc, Name: outNames[i]}
			if t, ok := inner.ScanColTypes[lc]; ok {
				plan.types[i] = setOpColType{typ: t, known: true}
				if t == parquet.TypeDecimal {
					plan.types[i].dec, plan.types[i].decKnown = setOpColDecimalMeta(inner.ScanColDecimal, lc)
				}
			}
		}
		return plan, nil
	}

	projNode := findOutputProjectionNode(arm)
	if projNode == nil {
		return setOpArmPlan{}, fmt.Errorf("no resolvable SELECT list to project onto the result columns %v", outNames)
	}
	if len(projNode.Projections) != len(outNames) {
		return setOpArmPlan{}, fmt.Errorf("selects %d columns, the first arm selects %d",
			len(projNode.Projections), len(outNames))
	}
	// Types for computed outputs have to be decided here: the output column
	// does not exist in the arm's schema, so the worker cannot resolve it
	// (same reason attachScanSelectProjections carries Type — #333).
	//
	// setOpArmDecls rather than inputColDecls: this is the set operation's own
	// view of the arm, with a JOIN's two sides kept apart under their
	// qualified names (#551) and a derived table's Project descended into
	// (#554). Its DECIMAL (p,s) rides in the same colDecls as the TypeID, so
	// the type and the scale are read out of ONE resolved key and cannot come
	// to describe different columns.
	var colTypes colDecls
	var strictInt map[string]bool
	var below *logical.Node
	if len(projNode.Children) == 1 {
		below = projNode.Children[0]
		colTypes = setOpArmDecls(below)
		// Same integer-preserving-arithmetic hint as
		// attachScanSelectProjections (#297, #445).
		strictInt = strictIntArithCols(below)
	}
	plan := setOpArmPlan{
		specs: make([]ProjectExprSpec, 0, len(outNames)),
		types: make([]setOpColType, 0, len(outNames)),
	}
	for i, pr := range projNode.Projections {
		if pr.IsAgg {
			return setOpArmPlan{}, fmt.Errorf(
				"selects the aggregate %q, whose output the arm's aggregate stage names for itself — "+
					"the union stage cannot project the SELECT list over it", pr.Expr)
		}
		e := pr.Expr
		if e == "" {
			e = pr.Column
		}
		if e == "" {
			return setOpArmPlan{}, fmt.Errorf("select item %d has neither an expression nor a column", i+1)
		}
		// The arm's SELECT list is written against the arm's OUTPUT schema,
		// and the arm's stream carries SOURCE names — a Project inside the
		// arm emits no stage, the convention every consumer compensates for.
		// The union stage is a consumer like the rest: without this,
		// `SELECT k FROM (SELECT s_suppkey AS k FROM supplier) x UNION ALL
		// …` projected a column named `k` over a stream that carries
		// s_suppkey and the task failed loud with `column "k" does not exist
		// in the input schema` (#490).
		ast := pr.ASTExpr
		// forwardedComputed marks a bare reference that turned OUT to name a
		// derived table's COMPUTED column: the spec now carries an
		// expression, so the worker builds the output vector from the
		// declared type instead of copying a column (#554).
		forwardedComputed := false
		if below != nil {
			if pr.ASTExpr != nil && !isSimpleColRefForRename(pr.ASTExpr) {
				if sub, ok := substituteNestedRenameRefs(pr.ASTExpr, below); ok && sub != nil {
					ast = sub
					e = sub.String()
				}
			} else {
				// A reference that forwards a derived table's COMPUTED column
				// names nothing the arm's stream carries, so it has to become
				// the expression that builds it — but ONLY when the arm walk
				// can also TYPE that column. The union stage EVALUATES the
				// rewritten expression and builds the output vector from the
				// declared type, and a wrong declaration there is a silently
				// wrong column, where the un-rewritten name is a loud task
				// failure (#554).
				sub, rewritable := setOpArmComputedSource(e, below)
				_, typed := setOpRefDecl(colTypes, e, pr)
				if rewritable && typed && sub != nil {
					ast = sub
					e = sub.String()
					forwardedComputed = true
				} else if src := resolveOutputRenameSource(strings.ToLower(e), below); src != "" {
					e = src
				}
			}
		}
		spec := ProjectExprSpec{Expr: e, Name: outNames[i]}
		ct := setOpColType{}
		if pr.ASTExpr != nil && !isSimpleColRefForRename(pr.ASTExpr) {
			if referencesSyntheticAgg(pr.ASTExpr) {
				return setOpArmPlan{}, fmt.Errorf("select item %d references an aggregate the gather evaluates", i+1)
			}
			// A computed column's declared type IS its runtime type: the
			// worker builds the output vector from it.
			decl := inferProjectionDeclType(ast, parquet.TypeString, strictInt, colTypes)
			// A numeric LITERAL arm carries the (p,s) of its SPELLING, which
			// PostgreSQL reads as numeric and this walk otherwise read as
			// float8 — so `SELECT d FROM t UNION ALL SELECT 1.23456`
			// resolved double precision where PostgreSQL resolves numeric
			// (#665). setOpLitArm is scoped to this site on purpose; see its
			// own comment.
			//
			// The expression is REWRITTEN to the literal's plain text as a
			// quoted string, because the evaluator folds a numeric literal
			// into a float64 and `1234567890123456.78` is not one: declaring
			// DECIMAL over that box would put an exact type on a number that
			// is already rounded. SetValueChecked parses the text at the
			// column's scale with no float in between.
			if d, ok := setOpLitArm(ast); ok {
				decl = d.decl
				e = "'" + d.text + "'"
				spec.Expr = e
			}
			spec.Type, spec.Precision, spec.Scale = declTypeParts(decl)
			spec.TypeKnown = true
			ct = setOpColType{typ: spec.Type, known: true}
			if decl.ID == parquet.TypeDecimal && decl.DecKnown {
				// A computed DECIMAL arm now knows its own (p,s), so the
				// arms reconcile through the ordinary rule instead of
				// leaving every arm as written — the #551 silent channel
				// (ADR-0024 item 2).
				ct.dec = logical.DecimalMeta{Precision: decl.Precision, Scale: decl.Scale}
				ct.decKnown = true
			}
		} else if c, ok := setOpRefDecl(colTypes, e, pr); ok {
			// A bare reference copies its source column, so the source's
			// type is what the arm emits. spec.Type stays unset: the worker
			// resolves a plain ColRef by DirectCopy and ignores it — unless
			// the reference was rewritten into the derived table's computed
			// EXPRESSION above, which the worker has to evaluate and
			// therefore has to be told the type of.
			ct = c
			if forwardedComputed {
				spec.Type = ct.typ
				spec.TypeKnown = true
				if ct.decKnown {
					spec.Precision, spec.Scale = ct.dec.Precision, ct.dec.Scale
				}
			}
		} else if cr, isRef := bareColRefOf(pr.ASTExpr); isRef && colTypes.isFieldPath(cr) {
			// A ROW FIELD PATH is not the bare reference it looks like: `rd.d`
			// names no column of anything, so the lookups above all miss and
			// the arm came back untyped — which for a DECIMAL field beside a
			// DECIMAL column is #551's channel with the disagreement one level
			// in. The FIELD's declaration answers, on exactly the terms
			// colDecls.colDecl resolves it (ADR-0022), and the spec carries the
			// type because nothing downstream resolves a field path by name:
			// it is MATERIALIZED the way a computed expression is.
			if fc, ok := colTypes.field(cr); ok {
				ct = setOpColType{typ: fc.Type, known: true}
				spec.Type = fc.Type
				spec.TypeKnown = true
				if fc.Type == parquet.TypeDecimal && fc.Precision > 0 {
					ct.dec = logical.DecimalMeta{Precision: fc.Precision, Scale: fc.Scale}
					ct.decKnown = true
					spec.Precision, spec.Scale = fc.Precision, fc.Scale
				}
			}
		}
		plan.specs = append(plan.specs, spec)
		plan.types = append(plan.types, ct)
	}
	return plan, nil
}

// setOpRefDecl types a bare column reference an arm forwards, under either
// spelling: the OUTPUT name the SELECT list wrote, or the SOURCE name the
// stream carries (what the projection was just resolved to). A miss on both
// leaves the column untyped, which is what it was before any spelling was
// tried.
//
// The SELECT list's own spelling is tried FIRST, because setOpArmDecls now
// answers for a derived table's EMITTED names and those are the ones the
// SELECT list wrote (#554). Trying the resolved source name first would let a
// derived table that binds one source name to another output name
// (`SELECT e4 AS e2, e2 AS e4 …`) answer about the wrong column of the two.
//
// Each candidate goes through the qualifier-stripping lookup, so a QUALIFIED
// spelling resolves too: an arm that ends in a join names its columns "a.u4"
// (#533), and after #551 the qualified key is the one that says WHICH side's
// column that is.
//
// The TypeID and the DECIMAL (p,s) come out of the SAME resolved key. Reading
// them from two lookups is how a declaration comes to describe two different
// columns — the mistake ADR-0024 removed from declaredProjectionDecl, and the
// one this function used to make by resolving the type through any of four
// spellings while reading the scale through one.
func setOpRefDecl(decls colDecls, resolved string, pr logical.Projection) (setOpColType, bool) {
	for _, cand := range []string{pr.Expr, pr.Column, resolved, pr.Alias} {
		if cand == "" {
			continue
		}
		key, ok := lookupColKey(decls.types, cand)
		if !ok {
			continue
		}
		d := declFromKey(decls, key)
		ct := setOpColType{typ: d.ID, known: true}
		if d.ID == parquet.TypeDecimal && d.DecKnown && d.Precision > 0 {
			ct.dec = logical.DecimalMeta{Precision: d.Precision, Scale: d.Scale}
			ct.decKnown = true
		}
		return ct, true
	}
	return setOpColType{}, false
}

// reconcileSetOpArmTypes makes every arm emit the same TYPE per column, not
// only the same name. It has to: the arms' outputs are separate .wshf files
// read as one stream, and a column declared FLOAT64 in one file and INT32 in
// another is not a union, it is a decoding error — `SELECT r_regionkey + 100
// AS k FROM region UNION ALL SELECT n_nationkey AS k FROM nation` panicked
// the gather task writing the second arm's chunk.
//
// Only numeric widening is performed (the ladder INT32 → INT64 → DECIMAL →
// FLOAT64, applied with a CAST on the narrower arms, or with a value-moving
// coercion where the destination is DECIMAL). Any other disagreement is
// refused: coercing, say, a number to text to make the files line up would
// answer a question the user did not ask.
//
// TWO DECIMAL arms need reconciling as much as two different TypeIDs do, and
// this is the part that was missing (#533). A TypeID comparison calls
// DECIMAL(9,2) and DECIMAL(18,4) equal — they are the same TypeID — so
// nothing was rewritten, each arm's file kept its own scale in its WSHF
// header, and the reader of both files took the first one's. The unscaled
// integer 127501 then rendered as 1275.01 instead of 12.7501.
func reconcileSetOpArmTypes(plans []setOpArmPlan, outNames []string) error {
	if len(plans) < 2 {
		return nil
	}
	for col := range outNames {
		want, allKnown, err := setOpTargetType(plans, col, outNames[col])
		if err != nil {
			return err
		}
		if !want.known {
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
		if want.typ == parquet.TypeDecimal {
			if !want.decKnown {
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
				ct := plans[i].types[col]
				// The arm's DECLARED spec is the arm's OWN type, not the
				// reconciled one, because it is what the worker builds this
				// arm's output vector from and the coercion below runs AFTER
				// that vector exists. DecimalCoerce rewrites an unscaled
				// carrier — an INT32/INT64 arm included, since an integer box
				// is a value at scale 0 — so the value has to ARRIVE as what
				// the arm produces.
				//
				// Declaring the TARGET here instead is what broke the seam
				// with #551's landing: a COMPUTED integer arm
				// (`n_regionkey + 100`) built a DECIMAL vector and the checked
				// writer refused the int box before the coercion could touch
				// it — "integer value 100 reached a DECIMAL(scale 1) column as
				// a raw unscaled carrier" (ADR-0018 §4, ADR-0024 item 4). A
				// BARE column arm never showed it: that one is a DirectCopy,
				// which types itself from the input and ignores the spec.
				//
				// Stamping the arm's own type is still needed, and is what
				// this clause is for: a spec left at the ZERO value declares
				// TypeBool with no (p,s), and a DECIMAL vector built from that
				// comes out at scale 0 with every value read back a
				// hundredfold out (ADR-0024 item 2).
				if ct.known {
					plans[i].specs[col].Type = ct.typ
					plans[i].specs[col].TypeKnown = true
					plans[i].specs[col].Precision = ct.dec.Precision
					plans[i].specs[col].Scale = ct.dec.Scale
				}
				if ct.typ == want.typ && ct.decKnown && ct.dec == want.dec {
					continue
				}
				plans[i].coerce = append(plans[i].coerce, DecimalCoercion{
					Name:      outNames[col],
					Precision: want.dec.Precision,
					Scale:     want.dec.Scale,
				})
				plans[i].types[col] = want
			}
			continue
		}
		for i := range plans {
			if plans[i].types[col].typ == want.typ {
				continue
			}
			cast, ok := setOpCastExpr(plans[i].specs[col].Expr, want.typ)
			if !ok {
				return fmt.Errorf("result column %q must be %s to match the other arms, and arm %d's "+
					"value cannot be cast to it", outNames[col], want.typ, i+1)
			}
			plans[i].specs[col].Expr = cast
			plans[i].specs[col].Type = want.typ
			plans[i].specs[col].TypeKnown = true
			plans[i].types[col] = setOpColType{typ: want.typ, known: true}
		}
	}
	return nil
}

// setOpAnyDecimalArm reports whether any arm of one result column resolved to
// DECIMAL. It is what makes an UNTYPED sibling arm a refusal rather than a
// shrug: two arms nothing typed are the pre-existing "leave it alone" case and
// carry no scale to disagree about, while a typed DECIMAL beside an untyped
// arm is the reinterpretation #551 is about.
func setOpAnyDecimalArm(plans []setOpArmPlan, col int) bool {
	for i := range plans {
		if ct := plans[i].types[col]; ct.known && ct.typ == parquet.TypeDecimal {
			return true
		}
	}
	return false
}

// setOpUntypedArmsDesc names the arms the walk could not type at all, with the
// expression each one selects.
func setOpUntypedArmsDesc(plans []setOpArmPlan, col int) string {
	var arms []string
	for i := range plans {
		if !plans[i].types[col].known {
			arms = append(arms, fmt.Sprintf("arm %d (%s)", i+1, plans[i].specs[col].Expr))
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
func setOpUnresolvedArmsDesc(plans []setOpArmPlan, col int) string {
	var arms []string
	for i := range plans {
		ct := plans[i].types[col]
		if ct.typ == parquet.TypeDecimal && !ct.decKnown {
			arms = append(arms, fmt.Sprintf("arm %d (%s)", i+1, plans[i].specs[col].Expr))
		}
	}
	if len(arms) == 0 {
		return "one of its arms"
	}
	return strings.Join(arms, " and ")
}

// setOpTargetType folds one result column's arms into the type they must all
// emit. allKnown is false when some arm carries no type at all, which is the
// caller's signal to leave the column alone.
func setOpTargetType(plans []setOpArmPlan, col int, name string) (setOpColType, bool, error) {
	var want setOpColType
	allKnown := true
	for _, plan := range plans {
		ct := plan.types[col]
		if !ct.known {
			allKnown = false
			continue
		}
		if !want.known {
			want = ct
			continue
		}
		widened, ok := setOpWiden(want.typ, ct.typ)
		if !ok {
			return setOpColType{}, false, fmt.Errorf(
				"the arms disagree on the type of result column %q (%s vs %s) and "+
					"neither widens into the other; make the types match in the query",
				name, want.typ, ct.typ)
		}
		want = setOpColType{typ: widened, known: true}
	}
	if want.known && want.typ == parquet.TypeDecimal && allKnown {
		arms := make([]setOpColType, 0, len(plans))
		for _, plan := range plans {
			arms = append(arms, plan.types[col])
		}
		want.dec, want.decKnown = setOpDecimalTarget(arms)
	}
	return want, allKnown, nil
}

// setOpNodeResultTypes is the per-column type a nested set-operation node
// emits: exactly what reconcileSetOpArmTypes will make ITS arms agree on,
// computed without emitting anything. nil when the shape is one this walk
// cannot type, which the caller reads as "unknown", the answer it had before.
func setOpNodeResultTypes(n *logical.Node) []setOpColType {
	names := setOpOutputNames(n)
	if len(names) == 0 || len(n.Children) < 2 {
		return nil
	}
	plans := make([]setOpArmPlan, 0, len(n.Children))
	for _, child := range n.Children {
		plan, err := setOpArmProjection(child, names)
		if err != nil {
			return nil
		}
		plans = append(plans, plan)
	}
	out := make([]setOpColType, len(names))
	for col := range names {
		want, allKnown, err := setOpTargetType(plans, col, names[col])
		if err != nil || !allKnown {
			continue
		}
		out[col] = want
	}
	return out
}

// setOpWiden is the numeric ladder: INT32 → INT64 → DECIMAL → FLOAT32 →
// FLOAT64.
//
// Every rung is PostgreSQL's, verified against postgres:17-alpine with
// pg_typeof over the union itself:
//
//	`numeric UNION ALL bigint`          → numeric
//	`numeric UNION ALL double precision`→ double precision
//	`real    UNION ALL integer/bigint`  → real
//	`real    UNION ALL numeric`         → real
//	`real    UNION ALL double precision`→ double precision
//
// Arm ORDER changes none of them, and changes none of them here.
//
// FLOAT32 gets its OWN rung rather than sharing FLOAT64's. Both are PREFERRED
// types of PostgreSQL's numeric category, so each beats the exact types
// (integer, numeric) it meets and only float8 beats float4 — and the
// difference is a VALUE, not just an OID: a real column holding 0.1 renders
// 0.1, and the same column widened to double precision renders
// 0.10000000149011612, which is the float32 value spelled to float64
// precision and is not what either engine holds. `CREATE TABLE t (x FLOAT)`
// declares a FLOAT32 column here, so this is reachable from plain DDL.
func setOpWiden(a, b parquet.TypeID) (parquet.TypeID, bool) {
	if a == b {
		return a, true
	}
	rank := func(t parquet.TypeID) int {
		switch t {
		case parquet.TypeInt32:
			return 1
		case parquet.TypeInt64:
			return 2
		case parquet.TypeDecimal:
			return 3
		case parquet.TypeFloat32:
			return 4
		case parquet.TypeFloat64:
			return 5
		}
		return 0
	}
	ra, rb := rank(a), rank(b)
	if ra == 0 || rb == 0 {
		return 0, false
	}
	switch {
	case ra == 5 || rb == 5:
		return parquet.TypeFloat64, true
	case ra == 4 || rb == 4:
		return parquet.TypeFloat32, true
	case ra == 3 || rb == 3:
		return parquet.TypeDecimal, true
	}
	return parquet.TypeInt64, true
}

// setOpCastExpr wraps an arm's expression so it produces the reconciled type.
// The destination spellings are the ones expr.Cast understands.
func setOpCastExpr(e string, to parquet.TypeID) (string, bool) {
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
