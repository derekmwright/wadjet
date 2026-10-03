// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"fmt"
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// respellWindowKeyExprs rewrites each materialized window-key EXPRESSION so
// its column references name what the window stage's input really carries.
//
// physical.PlanContext.ResolveWindowKeys is shared by both paths, so a key expression written over
// a derived table's or CTE's SELECT-list alias (`SUM(v * 2) OVER ()` above
// `SELECT c_i64 AS v`) is correct for the single-process pipeline, where the
// Project below the window is a real operator. On the DAG that Project emits
// no stage, so `v` names nothing: the fragment's pre-window projection
// evaluated it to NULL and the window wrote NULL in every row — #672's other
// half, and the ARGUMENT sibling of #658's PARTITION BY key.
//
// Only a reference the alias walk RESOLVES is rewritten; a spec whose
// expression cannot be re-parsed, or that names nothing derived, comes back
// exactly as it was.
func respellWindowKeyExprs(specs []physical.ProjectExprSpec, child *logical.Node) []physical.ProjectExprSpec {
	if len(specs) == 0 || child == nil {
		return specs
	}
	for i := range specs {
		ast, err := plansql.ParseExpression(specs[i].Expr)
		if err != nil {
			continue
		}
		if rewritten, changed := localPlanFacts.RespellDerivedAliasRefs(ast, child); changed {
			specs[i].Expr = rewritten.String()
		}
	}
	return specs
}

// windowAliasSlots binds every computed derived alias one window stage reads —
// a PARTITION BY / ORDER BY key, the argument, a reference inside a key
// EXPRESSION — to a SYNTHETIC slot, and materializes it on the producer.
//
// A materialized alias never takes the name of a column the producer already
// forwards. A derived table may give an alias the name of a column it also
// reads (`SELECT b * 2 AS b, b AS ob`): materializing the alias under `b`
// REPLACED the forwarded `b`, so the sibling `ob` — a rename of the source `b`
// the gather reads off the same stream — read the doubled value on the DAG
// arms. Such an alias stays in its `__winkey_alias_N` slot, a name that
// collides with nothing a query can write (the `__winkey_` namespace is
// reserved, sql.RefuseReservedSlotName) and with nothing the stream forwards,
// so the window reads the alias's value and every other consumer reads what
// it read before. An alias whose name the producer does not forward is
// materialized under that name (materialize says why).
type windowAliasSlots struct {
	p      *StagePlanner
	byKey  map[string]string // lower(alias) + "\x00" + definition → slot
	cols   []aliasColumn     // the slots, each carrying its alias's definition
	toName map[string]string // slot → the alias's own name (the fallback)
}

func newWindowAliasSlots(p *StagePlanner) *windowAliasSlots {
	return &windowAliasSlots{p: p, byKey: map[string]string{}, toName: map[string]string{}}
}

// bind returns the slot column c is materialized under.
func (w *windowAliasSlots) bind(c aliasColumn) string {
	key := strings.ToLower(c.Name) + "\x00" + c.Expr
	if slot, ok := w.byKey[key]; ok {
		return slot
	}
	slot := fmt.Sprintf("%salias_%d", plansql.SlotWindowKey, w.p.windowAliasSeq)
	w.p.windowAliasSeq++
	w.byKey[key] = slot
	w.toName[strings.ToLower(slot)] = c.Name
	c.Name = slot
	w.cols = append(w.cols, c)
	return slot
}

// bindKeyExprs rewrites every reference inside the window's key expressions
// that names a computed derived alias to the alias's slot. It runs BEFORE
// respellWindowKeyExprs turns a rename into its source's name: over
// `SELECT b * 2 AS b, b AS ob`, `ob` respells to `b`, which is then spelled
// exactly like the alias. An alias whose origin is an aggregate's group rows
// (aliasOriginReadsAggregate) is never materialized below the aggregate: it is
// spelled in the names the aggregate stage publishes instead
// (aggregateAliasDefinition).
func (w *windowAliasSlots) bindKeyExprs(specs []physical.ProjectExprSpec, child *logical.Node) []physical.ProjectExprSpec {
	for i := range specs {
		ast, err := plansql.ParseExpression(specs[i].Expr)
		if err != nil {
			continue
		}
		out, changed, complete := localPlanFacts.RewriteColRefs(ast, func(ref *plansql.ColRef) (plansql.Node, bool) {
			written := ref.String()
			if aliasOriginReadsAggregate(written, child) {
				return aggregateAliasDefinition(written, child, 0)
			}
			c := derivedAliasColumnFor(written, child)
			if c.Expr == "" {
				return nil, false
			}
			return &plansql.ColRef{Column: w.bind(c)}, true
		})
		if changed && complete {
			specs[i].Expr = out.String()
		}
	}
	return specs
}

// materialize projects every alias onto the producer below the window.
//
// The slot is needed only where the alias's name is a column the producer
// already forwards — the overwrite above. Every other alias is materialized
// under its OWN name, once, as it was before slots existed: the stream then
// carries the alias for every consumer above the window (the SELECT list's
// own `z.gk`, which the DAG otherwise cannot reach and hands to the
// coordinator-local pipeline), and the key reads the very column the query
// projects, so an expression evaluated twice (a volatile one) cannot give the
// key one value and the projected column another. When the producer declines
// the projection, every alias is read under its own name.
func (w *windowAliasSlots) materialize(stages []Stage, stage *Stage, child *logical.Node) {
	if len(w.cols) == 0 {
		return
	}
	producer := windowAliasProducer(stages)
	own := map[string]string{} // slot → alias, for an alias the producer does not forward
	for i := range w.cols {
		slot := strings.ToLower(w.cols[i].Name)
		if alias := w.toName[slot]; !forwardsColumn(producer, alias) {
			own[slot] = alias
			w.cols[i].Name = alias
		}
	}
	if materializeAliasColumns(producer, w.cols) {
		w.rename(stage, child, own)
		return
	}
	w.rename(stage, child, w.toName)
}

// rename respells every slot in m (lower-cased slot → name) the stage's
// window columns and key expressions name.
func (w *windowAliasSlots) rename(stage *Stage, child *logical.Node, m map[string]string) {
	if len(m) == 0 {
		return
	}
	back := func(name string) string {
		if n, ok := m[strings.ToLower(name)]; ok {
			return n
		}
		return name
	}
	for i := range stage.WindowCols {
		wc := &stage.WindowCols[i]
		if in := back(wc.InputCol); in != wc.InputCol {
			wc.InputCol = in
			wc.InputRefs = aliasCandidatesForText(in, child)
		}
		for j := range wc.PartitionBy {
			wc.PartitionBy[j] = back(wc.PartitionBy[j])
		}
		for j := range wc.OrderBy {
			wc.OrderBy[j].Column = back(wc.OrderBy[j].Column)
		}
	}
	for i := range stage.WindowKeyExprs {
		ast, err := plansql.ParseExpression(stage.WindowKeyExprs[i].Expr)
		if err != nil {
			continue
		}
		out, changed, complete := localPlanFacts.RewriteColRefs(ast, func(ref *plansql.ColRef) (plansql.Node, bool) {
			if n, ok := m[strings.ToLower(ref.Column)]; ok && ref.Table == "" {
				return &plansql.ColRef{Column: n}, true
			}
			return nil, false
		})
		if changed && complete {
			stage.WindowKeyExprs[i].Expr = out.String()
		}
	}
}

// aliasOriginReadsAggregate reports whether a computed derived alias is
// defined over an AGGREGATE's group rows, at any depth: the walk that finds the
// alias's defining Project (derivedAliasDefinition) continues below it through
// every derived table's SELECT list and every node that keeps one row per group
// (a HAVING, a sort, a LIMIT, a window — logical.AggregateOverGroupRows' list),
// and answers true when it reaches an Aggregate.
//
// Such an alias cannot be materialized on the producer materializeWindowAliasKeys
// picks: that producer sits BELOW the aggregate, where `MAX(b) * 2` — or
// `a.mb * 2` over a derived `MAX(b) AS mb`, or `MAX(b) * 2 + ROW_NUMBER() OVER
// (…)` — cannot be evaluated, and the planner refuses the stage (`carries
// projections [b g v] that its fragment does not evaluate`). The key reads the
// alias's own name, as it did before key expressions were materialized. A JOIN
// ends the walk: a join stage is itself a producer above the aggregate, and it
// computes the alias.
func aliasOriginReadsAggregate(name string, child *logical.Node) bool {
	_, owner := derivedAliasDefinition(name, child)
	for depth := 0; owner != nil && depth < aggRespellDepth; depth++ {
		if logical.AggregateOverGroupRows(owner) != nil {
			return true
		}
		var next *logical.Node
		for n := owner; len(n.Children) == 1 && n.Children[0] != nil; {
			n = n.Children[0]
			if n.Type == logical.NodeProject {
				next = n
				break
			}
			if !logical.AggScopePreservingWrapper(n.Type) {
				break
			}
		}
		owner = next
	}
	return false
}

// aggregateAliasDefinition spells a computed derived alias defined over an
// aggregate's group rows in the names that relation's stage publishes, so the
// window fragment computes it from its own input. A Project directly over the
// group rows is absorbed by the aggregate stage (absorbAggregateOutputProjection)
// and publishes the alias itself, so the reference stays a name; an alias one
// derived table further up (`a.mb * 2` over `SELECT g, MAX(b) AS mb … GROUP BY
// g`) is computed by no stage, and its definition is substituted, each of its
// own references resolved the same way against the relation its Project reads
// — a computed alias below is substituted again, a rename is respelled to its
// source, and a qualifier is dropped, because the stage publishes the
// aggregate's outputs unqualified.
func aggregateAliasDefinition(name string, child *logical.Node, depth int) (plansql.Node, bool) {
	if depth >= aggRespellDepth {
		return nil, false
	}
	def, owner := derivedAliasDefinition(name, child)
	if def == nil || owner == nil || len(owner.Children) != 1 || logical.AggregateOverGroupRows(owner) != nil {
		return nil, false
	}
	below := owner.Children[0]
	out, _, complete := localPlanFacts.RewriteColRefs(def, func(ref *plansql.ColRef) (plansql.Node, bool) {
		if sub, ok := aggregateAliasDefinition(ref.String(), below, depth+1); ok {
			return sub, true
		}
		if src := localPlanFacts.DerivedAliasSourceColumn(ref.String(), below); src != "" {
			return &plansql.ColRef{Column: localPlanFacts.CleanExpr(src)}, true
		}
		if ref.Table != "" {
			return &plansql.ColRef{Column: ref.Column}, true
		}
		return nil, false
	})
	if !complete {
		return nil, false
	}
	return &plansql.ParenNode{Inner: out}, true
}

// respellAggInputExpr rewrites aggregate argument references to the columns
// emitted by the stage below, using physical.PlanContext.ResolveAggInputName per reference (#702).
// A rename becomes its source column; a computed alias becomes its defining
// expression, parenthesized to preserve association inside the larger AST.
// DAG-only: rewrite stage-spec text, never the logical node executed by the
// local pipeline, where the derived Project is a real operator (ADR-0025).
// See docs/internals/aggregate-argument-alias-substitution.md for the design.
func respellAggInputExpr(n plansql.Node, child *logical.Node) (plansql.Node, bool) {
	return respellAggInputExprAt(n, child, 0)
}

// aggRespellDepth bounds the recursion below. Derived-table chains are one or
// two deep in practice; the bound is there so a malformed plan cannot spin.
const aggRespellDepth = 8

func respellAggInputExprAt(n plansql.Node, child *logical.Node, depth int) (plansql.Node, bool) {
	if n == nil || child == nil || depth >= aggRespellDepth {
		return n, false
	}
	if !aggInputRespellable(child) {
		return n, false
	}
	out, changed, complete := localPlanFacts.RewriteColRefs(n, func(ref *plansql.ColRef) (plansql.Node, bool) {
		if resolved, expr, below, renamed := localPlanFacts.ResolveAggInputName(ref.String(), child); renamed {
			if expr != nil {
				// The definition may name aliases of its OWN input:
				// `SELECT twice * 3 AS t FROM (SELECT id * 2 AS twice …)`
				// substitutes `twice * 3`, which still names `twice`.
				// physical.PlanContext.ResolveAggInputName returns at the first computed alias it
				// meets and cannot continue past it, so the substituted
				// subtree is respelled against the node that Project reads.
				inner := expr
				if r, ok := respellAggInputExprAt(expr, below, depth+1); ok {
					inner = r
				}
				return &plansql.ParenNode{Inner: inner}, true
			}
			if !strings.EqualFold(resolved, ref.String()) {
				return &plansql.ColRef{Column: localPlanFacts.CleanExpr(resolved)}, true
			}
			return nil, false
		}
		// A ROW FIELD PATH whose CONTAINER is the rename: `rw.b` over
		// `SELECT c_row AS rw`. The whole spelling names no column, and the
		// field `b` is not a column either — only the QUALIFIER is a name to
		// resolve, and resolving it gives the path the stage's stream really
		// carries. Without this the reference reached the worker as `rw.b`,
		// which nothing there can look up.
		//
		// It runs only AFTER the whole spelling has failed, which is
		// ADR-0022 §1's order: a derived table that emits a column named
		// `rw.b`, or whose own alias is `rw`, is resolved above and never
		// reaches here. A qualifier that is a FROM alias rather than a
		// rename resolves to nothing and is left exactly as written.
		if ref.Table == "" {
			return nil, false
		}
		qual, qexpr, _, qrenamed := localPlanFacts.ResolveAggInputName(ref.Table, child)
		if !qrenamed || qexpr != nil || strings.EqualFold(qual, ref.Table) {
			return nil, false
		}
		return &plansql.ColRef{Table: localPlanFacts.CleanExpr(qual), Column: ref.Column}, true
	})
	if !complete {
		// Same rule as above, and here it has teeth: an argument carrying a
		// subquery or an EXISTS has references this walk never saw, so a
		// partial rewrite would ship a spelling that resolves for some of them
		// and not others. Declining leaves the original text, which
		// assertAggregateInputsResolve then judges — and refuses, with the
		// sentinel, if it names something no stage emits.
		return n, false
	}
	return out, changed
}

// aggInputRespellable permits source respelling only when a Scan is reached
// through Project and Filter alone (#702): no intervening stage materializes
// the derived names. Join/Distinct materialize aliases; Aggregate/SetOp/Window
// publish their own column sets. Sort/Limit may receive an OpProject in a
// later pass, so materialization cannot be decided here (ADR-0025).
// All other shapes retain their behavior; assertAggregateInputsResolve makes
// unresolved residual inputs loud rather than silently reading missing names.
// See docs/internals/aggregate-input-respelling-boundary.md for the design.
func aggInputRespellable(n *logical.Node) bool {
	for depth := 0; n != nil && depth < aggRespellDepth; depth++ {
		switch n.Type {
		case logical.NodeScan:
			return true
		case logical.NodeProject, logical.NodeFilter:
			if len(n.Children) != 1 {
				return false
			}
			n = n.Children[0]
		default:
			return false
		}
	}
	return false
}

// aggInputAliasIsAggregateGroupKey matches a derived alias's defining expression
// against the aggregate's GROUP BY list, where it is emitted under expression
// text and can be read as a bare name. The mere presence of an aggregate is
// insufficient: arithmetic over its output must be computed, not read by name.
// DISTINCT groups by the whole projected expression and therefore qualifies;
// join/ordering materialization uses the alias instead of expression text.
// See docs/internals/aggregate-input-group-key-materialization.md for the design.
func aggInputAliasIsAggregateGroupKey(n *logical.Node, exprText string) (string, bool) {
	exprText = strings.TrimSpace(exprText)
	if exprText == "" {
		return "", false
	}
	for depth := 0; n != nil && depth < aggRespellDepth; depth++ {
		switch n.Type {
		case logical.NodeAggregate:
			for _, g := range n.GroupBy {
				// The match is case-INSENSITIVE, because column resolution is,
				// but the name returned is the PRODUCER'S — the group key as
				// the aggregate emits it, not the SELECT item's case-preserved
				// text. `SELECT SUM(v) FROM (SELECT A * 2 AS v … GROUP BY
				// a * 2)` matched on `A * 2` and shipped that spelling, and the
				// operator looks its input up by name: `aggregate input
				// "A * 2" is not a column of its input (input has: v, a * 2)`.
				if strings.EqualFold(strings.TrimSpace(g), exprText) {
					return strings.TrimSpace(g), true
				}
			}
			return "", false
		case logical.NodeProject, logical.NodeFilter:
			if len(n.Children) != 1 {
				return "", false
			}
			n = n.Children[0]
		default:
			return "", false
		}
	}
	return "", false
}

// aggInputAliasIsMaterializedUnderItsName reports whether the producer between
// the aggregate and its source materializes the derived alias under the ALIAS
// — which is what attachScanSelectProjections' alias-naming OpProject does on a
// join arm's fragment, and on a sort, a LIMIT or a union.
//
// An AGGREGATE is deliberately not in the list. It may publish the alias
// (absorbAggregateOutputProjection puts the SELECT list on a collapsing
// producer) or it may not, and where it does not the alias names nothing;
// computing the expression works in both cases, because the aggregate's own
// outputs are what the expression reads. So an aggregate below falls through to
// the compute answer, which is what ff7c3f19 did for every one of these shapes.
//
// A WINDOW was in the list and does not belong there, which is the other half
// of #877/#878. The alias-naming OpProject a window fragment can carry comes
// from attachScanSelectProjections, and that pass attaches the OUTERMOST SELECT
// list and returns at its first aggregate item — so whenever an AGGREGATE is
// the consumer asking this question, the answer is provably no: nothing put the
// alias on the window's fragment, and reading it gave NULL on every row. It
// stops the walk rather than being deleted from the case list, because a
// producer below a window is not the aggregate's producer either.
func aggInputAliasIsMaterializedUnderItsName(n *logical.Node) bool {
	for depth := 0; n != nil && depth < aggRespellDepth; depth++ {
		switch n.Type {
		case logical.NodeWindow:
			return false
		case logical.NodeJoin, logical.NodeSort, logical.NodeLimit,
			logical.NodeDistinct:
			return true
		case logical.NodeProject, logical.NodeFilter:
			if len(n.Children) != 1 {
				return false
			}
			n = n.Children[0]
		default:
			return false
		}
	}
	return false
}

// respellWindowSlotAliasRefs rewrites derived/CTE SELECT aliases resolving to
// window output slots to those slots (#877, #878; ADR-0025).
// Only the reserved window-output family qualifies: users cannot store or
// alias a column there (plansql.RefuseReservedSlotName). Other resolutions
// belong to the preceding alias rules.
// DAG-only: rewrite stage-spec text, never the local engine's logical node.
// See docs/internals/window-output-slot-alias-resolution.md for the design.
func respellWindowSlotAliasRefs(n plansql.Node, child *logical.Node) (plansql.Node, bool) {
	if n == nil || child == nil {
		return n, false
	}
	out, changed, complete := localPlanFacts.RewriteColRefs(n, func(ref *plansql.ColRef) (plansql.Node, bool) {
		resolved, expr, _, renamed := localPlanFacts.ResolveAggInputName(ref.String(), child)
		if !renamed || expr != nil {
			return nil, false
		}
		if plansql.ReservedSlotFamily(resolved) != string(plansql.SlotWindowOutput) {
			return nil, false
		}
		return &plansql.ColRef{Column: localPlanFacts.CleanExpr(resolved)}, true
	})
	if !complete {
		// Same rule as physical.PlanContext.RespellDerivedAliasRefs: a walk that met a node kind it
		// does not rewrite has NOT considered every reference, and a partial
		// respell looks resolved without being it.
		return n, false
	}
	return out, changed
}
