package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A SELECT list ABOVE an aggregate, carried onto the aggregate's own stage.
//
// An aggregate stage names its outputs the way the WORKER computes them: a
// group key by the exact text of the GROUP BY expression ("g + 1"), an
// aggregate by its AggSpec.OutputCol. The SELECT list above it names them by
// the query's aliases, and on the DAG a Project emits no stage — so nothing
// ever performs that rename, and nothing ever computes an expression written
// over the aggregate's outputs (`COUNT(*) + 1 AS k`).
//
// Two consumers notice, and both used to fail rather than answer:
//
//   - a WHERE above the alias. walkStages re-spells the predicate into the
//     Project's defining expression, which for a computed group key is
//     `(g + 1) > 3` — an expression over `g`, a column the aggregate's OUTPUT
//     does not carry. Every row answered UNKNOWN and the query returned
//     nothing (#656 shape f).
//   - a join key that is a computed alias. The shuffle key `b.k` named a
//     column no stage emitted: `partitioned shuffle: key "b.k" not in
//     schema` (#681).
//
// absorbAggregateOutputProjection puts what is MISSING onto the aggregate
// stage as Stage.ProjectExprs, spelled against the names the stage really
// emits — a computed group key becomes a DELIMITED identifier, because
// "g + 1" is a column NAME here and re-parsing it as arithmetic is exactly
// the defect. The aggregate fragment applies it after HAVING and before any
// fused sort.
//
// It carries a name only where the stage has none a consumer can use: a plain
// rename stays a pass-through, because every resolver on the DAG points the
// other way (see absorbAggregateOutputProjection's own comment). It declines
// outright — leaving the plan exactly as it was — for any projection it
// cannot map onto an output the stage emits. A projection carried wrong is
// worse than one not carried at all.

// aggregateProjectionTarget finds the aggregate-family stage a Project node
// sits directly above, among the stages its subtree emitted from index `from`.
// ok=false when the shape is not that.
//
// The LAST such stage is the right one: both aggregate shapes end in the
// final (fused scan-aggregate → final_aggregate, or partial aggregate →
// final_aggregate), and that final is what the Project reads.
func aggregateProjectionTarget(project *logical.Node, stages []Stage, from int) (stageIdx int, ok bool) {
	if project == nil || project.Type != logical.NodeProject || len(project.Children) != 1 {
		return 0, false
	}
	// Only a Project reading the aggregate's OUTPUT. A HAVING sits between
	// the two as a Filter and changes nothing about the columns, so the walk
	// descends through it (logical.AggregateBelowProject); anything else in
	// between — a Sort, another Project, a join — emits or renames on its own
	// terms and is a different question.
	if logical.AggregateBelowProject(project) == nil {
		return 0, false
	}
	for i := len(stages) - 1; i >= from; i-- {
		switch stages[i].Type {
		case StageAggregate, StageFinalAggregate, StageMergeAggregate:
			return i, true
		}
	}
	return 0, false
}

// absorbAggregateOutputProjection sets stage.ProjectExprs from the SELECT
// list of the Project directly above the aggregate, and reports whether it
// did.
//
// It carries a name ONLY where the stage has none a consumer can use, and
// this restraint is the whole of its safety. A PLAIN rename
// (`l_suppkey AS supplier_no`, `SUM(…) AS total_revenue`,
// `SELECT DISTINCT s_nationkey AS a`) must stay a pass-through: every
// consumer on the DAG resolves such an alias BACK to the source column
// (resolveShuffleKey, resolveAggInputName, resolveSortKeyColumn,
// resolveOutputRenameSource), so emitting it under the alias instead breaks
// the join key — Q15 answered 0 rows and a DISTINCT-alias join counted 27
// where PostgreSQL counts 25.
//
// What has no usable name is a group key the worker computes under the TEXT
// of its GROUP BY expression ("g + 1"), and an expression over the
// aggregate's outputs (`COUNT(*) + 1`) that nothing computes at all. Those
// get the alias; everything else keeps the name the stage already emits, and
// when nothing needed one the whole projection is declined.
// The returned map is the RENAMES it performed, lowercased old name to new.
// Every downstream reference to an old name has to travel through it: the
// stage stops emitting that name the moment the projection lands, and the
// gather, the sort keys and the filters above it were all written against the
// old spelling (#656 follow-up, F1).
func absorbAggregateOutputProjection(project *logical.Node, stage *Stage) map[string]string {
	if len(project.Projections) == 0 || len(stage.ProjectExprs) > 0 {
		return nil
	}
	groupKeys, aggOuts := aggregateStageOutputs(stage)
	decls := aggregateStageDecls(stage)
	specs := make([]ProjectExprSpec, 0, len(project.Projections)+len(groupKeys)+len(aggOuts))
	renamed := map[string]string{}
	needed := false
	for i := range project.Projections {
		p := &project.Projections[i]
		alias := projectionOutputName(*p)
		if alias == "" {
			return nil
		}
		src, computed, ok := aggregateProjectionSource(p, alias, groupKeys, aggOuts)
		if !ok {
			return nil
		}
		switch {
		case computed:
			// Nothing on the stage carried this value under any name, so
			// there is no old spelling to retarget.
			// Nothing on the stage carries this value. The expression is
			// written over the aggregate's OUTPUT columns (the logical
			// planner already replaced its nested aggregates with refs to
			// their synthetic OutputCol), so its declared type has to come
			// from those outputs — there is no catalog column to read it off.
			// AggSpec carries an OutputType but no (p,s) for a DECIMAL one,
			// so an expression over a DECIMAL aggregate cannot be DECLARED
			// here at all: the inference falls to the float rule, and with
			// exact DECIMAL arithmetic the evaluator then hands an exact
			// value to a FLOAT64 vector. Decline the whole projection and
			// leave `SUM(a) * 2` on the route it already had — a wrong
			// declaration is worse than no projection (ADR-0024 item 2).
			if referencesDecimalAggregate(p.ASTExpr, stage) {
				return nil
			}
			decl := inferProjectionDeclType(p.ASTExpr, parquet.TypeString, nil, decls)
			typ, prec, scale := declTypeParts(decl)
			specs = append(specs, ProjectExprSpec{
				Expr: src, Name: strings.ToLower(alias),
				Type: typ, TypeKnown: true, Precision: prec, Scale: scale,
			})
			needed = true
		case !nameIsPlainColumn(src):
			// The stage emits it under an expression TEXT, which no
			// consumer can name. The alias is the only usable spelling.
			specs = append(specs, ProjectExprSpec{
				Expr: plansql.QuoteIdent(src), Name: strings.ToLower(alias),
			})
			renamed[strings.ToLower(src)] = strings.ToLower(alias)
			needed = true
		default:
			specs = append(specs, ProjectExprSpec{Expr: src, Name: strings.ToLower(src)})
		}
	}
	if !needed {
		return nil
	}
	// An OpProject NARROWS the batch to exactly its outputs, so every other
	// column the stage emits has to ride along as a pass-through — a
	// consumer resolving one by source name would otherwise find it gone.
	have := make(map[string]bool, len(specs))
	for _, s := range specs {
		have[s.Name] = true
	}
	for _, m := range []map[string]string{groupKeys, aggOuts} {
		for low, real := range m {
			if have[low] {
				continue
			}
			have[low] = true
			// A computed group key rides along under its OWN spelling too,
			// delimited. The alias is what a consumer can NAME, but every
			// resolver that ran before this projection existed — the sort
			// key that chased `o_year` down to `substr(o_orderdate, 1, 4)`,
			// the gather rename written against the same text — points at
			// the old one, and narrowing it away would break them. Emitting
			// both costs one column and keeps this projection purely
			// ADDITIVE (#656 F1/F2).
			expr := real
			if !nameIsPlainColumn(real) {
				expr = plansql.QuoteIdent(real)
			}
			// The EXACT spelling, not the lowercased key: this is a
			// pass-through, and every consumer that already resolved to this
			// column named it as the stage spells it. `GROUP BY
			// ARRAY[n_regionkey]` is emitted under that text, and a fused
			// sort keyed on it stops finding it the moment the projection
			// renames it to `array[n_regionkey]`.
			specs = append(specs, ProjectExprSpec{Expr: expr, Name: real})
		}
	}
	stage.ProjectExprs = specs
	return renamed
}

// nameIsPlainColumn reports whether a stage output's name is one an ordinary
// column reference can spell — i.e. whether a consumer can name it at all.
// A group key computed from an expression is emitted under the expression's
// own TEXT ("g + 1"), which re-parses as arithmetic over a column the
// aggregate's output does not carry.
func nameIsPlainColumn(s string) bool {
	if s == "" {
		return false
	}
	n, err := plansql.ParseExpression(s)
	if err != nil {
		return false
	}
	ref, isCol := n.(*plansql.ColRef)
	return isCol && ref.Table == "" && strings.EqualFold(ref.Column, s)
}

// aggregateProjectionSource maps one SELECT-list item onto the aggregate
// stage's output columns. computed=false returns the stage's exact output
// NAME (unquoted — the caller decides whether it needs delimiting);
// computed=true returns an EXPRESSION over those outputs.
// The two maps are kept apart on purpose: a query may give a group key and an
// aggregate the SAME output name (`SELECT g, COUNT(*) AS g … GROUP BY g`), and
// looking an item up in the union would sometimes answer with the other one.
func aggregateProjectionSource(p *logical.Projection, name string, groupKeys, aggOuts map[string]string) (src string, computed bool, ok bool) {
	// An AGGREGATE item resolves against the aggregate outputs only. Its
	// AggSpec.OutputCol is normally the item's own output name.
	if p.IsAgg {
		for _, cand := range []string{p.Expr, name} {
			if cand == "" {
				continue
			}
			if real, hit := aggOuts[strings.ToLower(cand)]; hit {
				return real, false, true
			}
		}
		return "", false, false
	}
	// A plain reference: a group key spelled as a column, or the exact text
	// of a computed group key.
	for _, cand := range []string{p.Column, p.Expr} {
		if cand == "" {
			continue
		}
		if real, hit := groupKeys[strings.ToLower(cand)]; hit {
			return real, false, true
		}
		if real, hit := aggOuts[strings.ToLower(cand)]; hit {
			return real, false, true
		}
	}
	if p.ASTExpr == nil {
		return "", false, false
	}
	// An expression over the aggregate's outputs (`COUNT(*) + 1`): the
	// logical planner rewrote its aggregate calls into refs to their
	// synthetic OutputCol, so every leaf must now be a name the stage emits.
	emitted := make(map[string]string, len(groupKeys)+len(aggOuts))
	for k, v := range groupKeys {
		emitted[k] = v
	}
	for k, v := range aggOuts {
		emitted[k] = v
	}
	rewritten, ok := requoteAggOutputRefs(p.ASTExpr, emitted)
	if !ok {
		return "", false, false
	}
	return rewritten.String(), true, true
}

// requoteAggOutputRefs checks that every column reference in an expression
// names one of the aggregate stage's outputs, rewriting each to the exact
// spelling the stage emits. ok=false when any reference does not — the caller
// then declines the whole projection rather than shipping an expression the
// fragment cannot evaluate.
//
// It reuses substituteNestedRenameRefs' copy-on-write shape without its
// resolver: here the question is membership, not renaming.
func requoteAggOutputRefs(n plansql.Node, emitted map[string]string) (plansql.Node, bool) {
	switch e := n.(type) {
	case nil:
		return nil, true
	case *plansql.ColRef:
		if e.Table != "" {
			if real, hit := emitted[strings.ToLower(e.String())]; hit {
				return &plansql.ColRef{Column: real}, true
			}
		}
		real, hit := emitted[strings.ToLower(e.Column)]
		if !hit {
			return nil, false
		}
		return &plansql.ColRef{Column: real}, true
	case *plansql.Lit, *plansql.IntervalLit:
		return n, true
	case *plansql.BinaryOp:
		l, lok := requoteAggOutputRefs(e.Left, emitted)
		r, rok := requoteAggOutputRefs(e.Right, emitted)
		if !lok || !rok {
			return nil, false
		}
		return &plansql.BinaryOp{Left: l, Op: e.Op, Right: r}, true
	case *plansql.UnaryOp:
		in, ok := requoteAggOutputRefs(e.Inner, emitted)
		if !ok {
			return nil, false
		}
		return &plansql.UnaryOp{Op: e.Op, Inner: in}, true
	case *plansql.ParenNode:
		in, ok := requoteAggOutputRefs(e.Inner, emitted)
		if !ok {
			return nil, false
		}
		return &plansql.ParenNode{Inner: in}, true
	case *plansql.CastNode:
		in, ok := requoteAggOutputRefs(e.Inner, emitted)
		if !ok {
			return nil, false
		}
		return &plansql.CastNode{Inner: in, TypeName: e.TypeName}, true
	}
	// Every other node kind — a function call, a CASE, a subquery — is left
	// alone rather than guessed at, which keeps the plan exactly as it was.
	return nil, false
}

// aggregateStageOutputs lists the columns an aggregate stage's fragment emits,
// keyed by lowercased name and valued by the exact spelling, split into the
// GROUP BY keys and the aggregate outputs.
func aggregateStageOutputs(s *Stage) (groupKeys, aggOuts map[string]string) {
	groupKeys = make(map[string]string, len(s.GroupByCols))
	aggOuts = make(map[string]string, len(s.AggSpecs))
	for _, k := range s.GroupByCols {
		if k != "" {
			groupKeys[strings.ToLower(k)] = k
		}
	}
	for _, a := range s.AggSpecs {
		if a.OutputCol != "" {
			aggOuts[strings.ToLower(a.OutputCol)] = a.OutputCol
		}
	}
	return groupKeys, aggOuts
}

// aggregateStageDecls is the declared type of every aggregate stage output,
// for typing a projection written over them.
func aggregateStageDecls(s *Stage) colDecls {
	types := make(map[string]parquet.TypeID, len(s.GroupByCols)+len(s.AggSpecs))
	dec := map[string]logical.DecimalMeta{}
	for _, k := range s.GroupByCols {
		if t, ok := s.GroupByTypes[k]; ok {
			types[strings.ToLower(k)] = t
		}
		if d, ok := s.GroupByDecimal[k]; ok {
			dec[strings.ToLower(k)] = d
		}
	}
	for _, a := range s.AggSpecs {
		if a.OutputTypeKnown && a.OutputCol != "" {
			types[strings.ToLower(a.OutputCol)] = a.OutputType
		}
	}
	return colDecls{types: types, dec: dec}
}

// referencesDecimalAggregate reports whether an expression names an aggregate
// output this stage declares DECIMAL. Stage.AggSpec has no (p,s) for one, so
// such an expression has no declarable type here.
func referencesDecimalAggregate(n plansql.Node, stage *Stage) bool {
	dec := map[string]bool{}
	for _, a := range stage.AggSpecs {
		if a.OutputCol != "" && a.OutputTypeKnown && a.OutputType == parquet.TypeDecimal {
			dec[strings.ToLower(a.OutputCol)] = true
		}
	}
	if len(dec) == 0 {
		return false
	}
	for _, ref := range collectColRefs(n) {
		if dec[strings.ToLower(ref.Column)] {
			return true
		}
	}
	return false
}
