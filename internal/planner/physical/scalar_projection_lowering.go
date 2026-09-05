package physical

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The SELECT-list half of the scalar-subquery deferral (#659).
//
// A scalar subquery in a PREDICATE has had a distributed lowering since Q11:
// walkStages puts the filter text through resolveFilterSubqueries, which
// replaces the subquery with a `:scalar_N` placeholder and hands back the SQL
// to emit as a PRODUCER STAGE; the coordinator awaits that stage, reads its
// single row, and substitutes the literal into the filter text before
// dispatch. The same subquery in the SELECT LIST had none: the projection was
// attached verbatim, the worker's expression compiler has no SubqueryRunner,
// and every task failed with `subqueries require a SubqueryRunner`. Refusing
// the plan and routing the query to the coordinator-local pipeline made it
// RIGHT (ADR-0021, `ErrScalarSubqueryProjectionDistributed`) at the cost of
// running the whole query single-process.
//
// This is the lowering, and it is the SAME machinery rather than a second one.
// That matters for more than economy: the alternative — execute the subquery
// at plan time and splice a literal — is what `scalarDeferAll` moved AWAY
// from, because a value accumulated on the coordinator's single-process
// pipeline and one accumulated across stages differ in float order (the Q15
// SF0.1 zero-row root cause). A producer stage accumulates the way the outer
// query does.
//
// Two things the predicate path does not need and this one does:
//
//   - **The spec keeps the item's ORIGINAL name.** `ProjectExprSpec.Name` is
//     what `extractOutputRenames` maps to the user's alias, and that pass reads
//     the LOGICAL projection, which this lowering does not touch. So the spec's
//     Expr carries `:scalar_N` and its Name carries the item's own text.
//   - **The spec must declare a TYPE.** A projection's declared type IS the
//     output column's type, and a bare numeric literal is float8 in this
//     dialect (ADR-0024) — so a substituted `4999014997` would come back
//     float64 where the single path answers int64. The producer's own plan
//     knows the answer, so `emitScalarProducerStages` hands it back and the
//     spec declares it.
//
// The boundary is drawn at the shape the resolver can rewrite: an item whose
// subquery survives `resolveSubqueryAST` (a CASE arm, a function argument — the
// walker's `default:` returns the node unwalked) is DECLINED here and reaches
// the refusal exactly as it did before. A CORRELATED subquery is declined by
// `resolveSubqueryAST` itself (it parks `ErrCorrelatedSubqueryDistributed` on
// the planner) and routes to the local pipeline, which is the only engine in
// this process that can re-run one per outer row.

// projScalarProducer is one lowered SELECT-list subquery: the producer stage
// that computes it and the type that stage's single column declares.
type projScalarProducer struct {
	producerID string
	typ        parquet.TypeID
	typeKnown  bool
	prec       int
	scale      int
}

// lowerProjectionSubquery rewrites one SELECT-list item that carries a scalar
// subquery into `:scalar_N` placeholder text, emitting the producer stages the
// coordinator will await. It returns ok=false for an item it cannot rewrite,
// which keeps that item on the refusal path.
func (p *Planner) lowerProjectionSubquery(stages *[]Stage, item *logical.Projection,
	decls colDecls) (text string, decl expr.DeclType, declKnown, ok bool) {

	if item.ASTExpr == nil || p.planCtx == nil {
		return "", decl, false, false
	}
	var deferred []deferredScalar
	before := p.scalarPlaceholderSeq
	resolved := p.resolveSubqueryAST(p.planCtx, item.ASTExpr, &deferred, decls)
	if resolved == nil {
		p.scalarPlaceholderSeq = before
		return "", decl, false, false
	}
	// A subquery the walk did not descend into is still in the tree. Decline
	// rather than attach it: the worker cannot compile it, and the refusal
	// below routes the query where it can be answered.
	if exprCarriesSubquery(resolved) {
		p.scalarPlaceholderSeq = before
		return "", decl, false, false
	}
	// Every deferred subquery needs a producer stage. A failure here is a
	// decline, not a fallback to plan-time execution: the whole point of the
	// producer is that the value accumulates the way the outer query's does.
	for _, d := range deferred {
		producerID, typ, typeKnown, err := p.emitScalarProducerStagesTyped(stages, d.SubquerySQL)
		if err != nil {
			return "", decl, false, false
		}
		if p.projScalarProducers == nil {
			p.projScalarProducers = map[string]projScalarProducer{}
		}
		p.projScalarProducers[d.Placeholder] = projScalarProducer{
			producerID: producerID, typ: typ.ID, typeKnown: typeKnown,
			prec: typ.Precision, scale: typ.Scale,
		}
	}
	// The item's own type. When the item IS the subquery, that is the
	// producer's declared type; anything wrapping it goes through the
	// ordinary projection inference with the placeholder's type supplied, so
	// `(SELECT MAX(v) FROM c) + 1` is typed like any other arithmetic.
	decl, declKnown = p.placeholderDeclType(resolved, decls)
	return resolved.String(), decl, declKnown, true
}

// placeholderDeclType types an item whose subqueries are now placeholders.
// A bare placeholder declares its producer's type; anything else is inferred
// with the placeholders' types folded into the column declarations, which is
// how `inferProjectionDeclType` already learns a column's type.
func (p *Planner) placeholderDeclType(n plansql.Node, decls colDecls) (expr.DeclType, bool) {
	if ph, isPH := n.(*plansql.LiteralPlaceholder); isPH {
		if prod, seen := p.projScalarProducers[ph.Name]; seen && prod.typeKnown {
			return expr.DeclType{ID: prod.typ, Precision: prod.prec, Scale: prod.scale,
				DecKnown: prod.typ == parquet.TypeDecimal}, true
		}
		return expr.DeclType{}, false
	}
	// A wrapped placeholder: give the inference the placeholder's type under
	// its own spelling, which is how it reads a column reference.
	if len(p.projScalarProducers) > 0 {
		merged := colDecls{types: map[string]parquet.TypeID{}, fields: decls.fields, dec: map[string]logical.DecimalMeta{}}
		for k, v := range decls.types {
			merged.types[k] = v
		}
		for k, v := range decls.dec {
			merged.dec[k] = v
		}
		for name, prod := range p.projScalarProducers {
			if !prod.typeKnown {
				continue
			}
			merged.types[":"+name] = prod.typ
			if prod.typ == parquet.TypeDecimal {
				merged.dec[":"+name] = logical.DecimalMeta{Precision: prod.prec, Scale: prod.scale}
			}
		}
		decls = merged
	}
	return inferProjectionDeclType(n, parquet.TypeString, nil, decls), true
}

// exprCarriesSubquery reports whether any subquery construct survives in n.
func exprCarriesSubquery(n plansql.Node) bool {
	found := false
	visitExprSubqueries(n, func(string, string) { found = true })
	return found
}

// attachProjectionScalarDependencies wires each lowered SELECT-list
// placeholder to the stage whose ProjectExprs carry it, so the coordinator
// awaits that producer and substitutes its value before dispatch.
//
// A placeholder no stage carries is a REFUSAL rather than a dangling `:scalar_N`
// shipped to a worker: the passes between the attach and here may move a
// projection onto a stage this walk covers or drop it entirely, and a
// predicate the worker cannot compile is a failed task, not an answer. The
// refusal is the one the coordinator already routes on.
func (p *Planner) attachProjectionScalarDependencies(stages []Stage) error {
	if len(p.projScalarProducers) == 0 {
		return nil
	}
	// THE SHARED-CTE BOUNDARY, and it is a CYCLE rather than a preference.
	//
	// A producer over a CTE the OUTER query also reads takes that CTE body's
	// stage: `ctePlannedTerminal` hands back the one the outer walk already
	// emitted, which is what keeps a CTE from being computed twice and is what
	// Q15's dual-chain float drift is prevented by. But the stage that carries
	// the SELECT list is that same body's consumer — so the carrier would
	// await a producer that depends on the carrier, and the query hangs. It
	// did: `WITH c AS (…) SELECT id, (SELECT MAX(v) FROM c) FROM c` sat on the
	// coordinator's stage barrier until the test's twenty-minute deadline.
	//
	// So the check is reachability, not a shape guess: if the producer's
	// dependency closure contains the stage awaiting it, the lowering is
	// declined and the coordinator routes the query to its local pipeline,
	// which answers it. A CTE the outer query does NOT read has no shared
	// stage and lowers like any other relation.
	placed := make(map[string]bool, len(p.projScalarProducers))
	deps := stageDependencyClosure(stages)
	for i := range stages {
		s := &stages[i]
		for _, spec := range s.ProjectExprs {
			for name, prod := range p.projScalarProducers {
				if !strings.Contains(spec.Expr, ":"+name) {
					continue
				}
				if deps[prod.producerID][s.ID] {
					return fmt.Errorf("%w: the SELECT-list subquery's producer stage %s reads"+
						" stage %s, which is the stage that would await it — the subquery and"+
						" the outer query share a CTE body; the coordinator runs this query"+
						" single-process",
						ErrScalarSubqueryProjectionDistributed, prod.producerID, s.ID)
				}
				if s.ScalarDependencies == nil {
					s.ScalarDependencies = make(map[string]string)
				}
				s.ScalarDependencies[name] = prod.producerID
				placed[name] = true
			}
		}
	}
	for name := range p.projScalarProducers {
		if !placed[name] {
			return fmt.Errorf("%w: the SELECT-list subquery lowered to :%s reached no stage,"+
				" so no worker could be given its value; the coordinator runs this query"+
				" single-process", ErrScalarSubqueryProjectionDistributed, name)
		}
	}
	return nil
}

// stageDependencyClosure returns, per stage ID, every stage it transitively
// depends on. Used to keep a scalar producer from being awaited by a stage the
// producer itself reads — a cycle the coordinator's stage barrier cannot
// break, so the query hangs rather than failing.
func stageDependencyClosure(stages []Stage) map[string]map[string]bool {
	direct := make(map[string][]string, len(stages))
	for i := range stages {
		direct[stages[i].ID] = stages[i].Dependencies
	}
	out := make(map[string]map[string]bool, len(stages))
	var walk func(string, map[string]bool)
	walk = func(id string, seen map[string]bool) {
		for _, d := range direct[id] {
			if seen[d] {
				continue
			}
			seen[d] = true
			walk(d, seen)
		}
	}
	for i := range stages {
		seen := map[string]bool{}
		walk(stages[i].ID, seen)
		out[stages[i].ID] = seen
	}
	return out
}
