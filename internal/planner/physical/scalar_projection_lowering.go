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
//   - **The spec is typed the way the SINGLE PATH types the item, and
//     deliberately not from the producer.** The producer's plan knows the
//     value's type and the projection could declare it — measured, that makes
//     `SELECT (SELECT MAX(c_i64) FROM t)` a bigint on the DAG, which is
//     PostgreSQL's answer. The single-process pipeline answers a STRING for
//     the same query (`expr.ScalarSubquery` declares nothing, so the item
//     falls to the projection's string fallback), and a lowering that made
//     the two paths disagree about a column's TYPE would be trading a cost
//     for a divergence. So the item is typed exactly as it is typed today and
//     the box defect is recorded and filed instead. The one thing this must
//     NOT do is declare a type it does not know: an undeclared spec that
//     claims TypeKnown takes TypeID zero, which is BOOLEAN, and the first cut
//     of this returned `true` for that query.
//
// FOUR declines, and each one is a mechanism rather than a shape on a list.
// An item whose subquery survives `resolveSubqueryAST` — a CASE arm, a
// function argument, anywhere the walker's `default:` returns the node
// unwalked. A CORRELATED subquery, which `resolveSubqueryAST` itself parks as
// `ErrCorrelatedSubqueryDistributed`: only the local pipeline can re-run one
// per outer row. A producer whose value has no lossless literal spelling
// (scalarProducerValueIsLiteralSafe). And a producer that would be awaited by
// a stage it READS (attachProjectionScalarDependencies). All four keep today's
// disposition: refused, routed local, right.

// projScalarProducer is one lowered SELECT-list subquery: the producer stage
// that computes it.
type projScalarProducer struct {
	producerID string
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
	// EVERY subquery in the item must become a PRODUCER, and counting them
	// first is what makes that checkable. resolveSubqueryAST has a second
	// exit: a subquery that is not provably one row is not deferred at all —
	// it is EXECUTED on the coordinator at plan time and its value spliced in
	// as a literal (plan.go's `scalarToLiteral` arm). That literal never meets
	// scalarProducerValueIsLiteralSafe, so `(SELECT b FROM t ORDER BY id LIMIT
	// 1)` over a DECIMAL(18,4) reached a worker as `1` where PostgreSQL and
	// the single path answer 12.7500 — a wrong VALUE — and a TIMESTAMP, a
	// FLOAT64 and a DURATION came back int64-boxed where the single path
	// answers text. It is also the whole of `WADJET_SCALAR_DEFER=0`, under
	// which NOTHING defers and every one of those shapes took the splice.
	//
	// So the rule is counted, not inspected: as many producers as the item had
	// subqueries, or the item is not lowered at all.
	want := countExprSubqueries(item.ASTExpr)
	var deferred []deferredScalar
	before := p.scalarPlaceholderSeq
	stagesBefore := len(*stages)
	resolved := p.resolveSubqueryAST(p.planCtx, item.ASTExpr, &deferred, decls)
	if resolved == nil {
		p.scalarPlaceholderSeq = before
		return "", decl, false, false
	}
	if len(deferred) != want {
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
		producerID, valueType, typeKnown, err := p.emitScalarProducerStagesTyped(stages, d.SubquerySQL)
		if err != nil {
			return "", decl, false, false
		}
		if !typeKnown || !scalarProducerValueIsLiteralSafe(valueType) {
			return "", decl, false, false
		}
		// A producer that REUSED a CTE body somebody already planned emits a
		// `cte-alias` phantom, and the pass that resolves those
		// (flattenCTEAliases) runs inside generateStages — before this
		// lowering exists. The phantom therefore reaches dispatch, where it
		// carries no operators and the stage fails three times with no
		// SQLSTATE: `empty Operators on task … (StageType="cte-alias")`. Two
		// SELECT-list subqueries over one CTE, and a SELECT-list subquery
		// beside a WHERE one over the same CTE, both do it.
		//
		// Declining is the honest disposition and not a smaller fix: the
		// alternative is to re-run the alias flattening after every producer
		// is emitted, which renumbers stages the surrounding passes have
		// already bound references into. The shape keeps what it had at base —
		// refused, routed, right.
		for i := stagesBefore; i < len(*stages); i++ {
			if (*stages)[i].Type == stageTypeCTEAlias {
				return "", decl, false, false
			}
		}
		if p.projScalarProducers == nil {
			p.projScalarProducers = map[string]projScalarProducer{}
		}
		p.projScalarProducers[d.Placeholder] = projScalarProducer{producerID: producerID}
	}
	// The item's own type, taken from the ordinary projection inference over
	// a tree whose subqueries are now placeholders: a bare placeholder falls
	// to the string fallback, which is what the single path answers, and
	// `(SELECT …) + 1` folds float8, which is also what the single path
	// answers. Both are what PostgreSQL does NOT say (bigint in each case),
	// and that is a box defect this lowering neither introduces nor is
	// allowed to fix on one path only.
	decl = inferProjectionDeclType(resolved, parquet.TypeString, nil, decls)
	return resolved.String(), decl, true, true
}

// scalarProducerValueIsLiteralSafe reports whether a producer's value can
// travel to a worker AS A LITERAL and be read back as the same value at the
// same type.
//
// This is ADR-0021 §1e's rule — the one the correlated re-run applies to an
// outer row's values — asked at the other end of the same round trip. The
// producer's value is rendered to TEXT by the coordinator and re-parsed by the
// worker's expression compiler, and for a type whose literal does not carry
// its own scale or width that round trip is lossy: `(SELECT AVG(a) FROM t)`
// over a `DECIMAL(p,2)` column substitutes `7.570000`, which is the right
// digits, and comes back `7.57` — the enclosing projection is declared from
// the subquery's SOURCE column rather than from its aggregate, so the value is
// read at the column's scale and not the producer's.
//
// The integer family, BOOL and STRING have no such second parameter, so their
// literal is the value. Everything else DECLINES and keeps routing to the
// coordinator-local pipeline, which never renders the value at all. Lifting
// the decline needs a producer that declares its own (p,s) — or its own width,
// or its own instant — to the projection that reads it, which is a stage-model
// change rather than a rewrite.
func scalarProducerValueIsLiteralSafe(t parquet.TypeID) bool {
	switch t {
	case parquet.TypeBool, parquet.TypeInt32, parquet.TypeInt64, parquet.TypeString:
		return true
	}
	return false
}

// exprCarriesSubquery reports whether any subquery construct survives in n.
func exprCarriesSubquery(n plansql.Node) bool {
	return countExprSubqueries(n) > 0
}

// countExprSubqueries counts the subquery constructs in n. The LOWERING needs
// the count rather than the boolean: "no subquery left in the tree" is true
// both when every one became a producer and when one was executed at plan time
// and replaced by a literal, and those two are not the same disposition.
func countExprSubqueries(n plansql.Node) int {
	count := 0
	visitExprSubqueries(n, func(string, string) { count++ })
	return count
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
