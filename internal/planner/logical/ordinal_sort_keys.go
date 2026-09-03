package logical

import (
	"strconv"
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// `SELECT * ... ORDER BY 1` — a select-list POSITION over a list whose length
// is not known until the star expands.
//
// The ordinal is resolved in the parser (`resolvePositionalRefs`) for every
// list whose items are countable there, which is every list with no `*` before
// the position. A star is not countable there: it stands for however many
// columns its source has, and the source is a catalog question. So the parser
// leaves such an ordinal alone, `resolveOrderBy` carries it on the sort key as
// `OrderExpr.Position`, and this pass resolves it where the answer exists —
// immediately after `ExpandStarProjections`, against the projection list the
// star produced.
//
// Before this, the shape was REFUSED on every arm ("`SELECT *` is expanded too
// late for the planner to count its positions here"), while PostgreSQL answers
// it. `SELECT * FROM t ORDER BY 1` is what psql, DataGrip, Superset and every
// "preview this table" button emit (#810).
//
// The refusal it replaces was not wrong about the risk — materializing the
// numeric CONSTANT as the sort key would sort by a value that is the same in
// every row, which is a silent no-op and exactly what the ORDER BY pass exists
// to end. That is why an ordinal this pass cannot resolve stays unresolved and
// is refused loudly by the physical planner rather than quietly materialized.

// ResolveOrdinalSortKeys rewrites every deferred positional sort key against
// the projection below its Sort.
//
// It is safe to run more than once and on a plan with no deferred keys: the
// walk exits on the first node with nothing to do.
func ResolveOrdinalSortKeys(n *Node) {
	if n == nil {
		return
	}
	for _, child := range n.Children {
		ResolveOrdinalSortKeys(child)
	}
	if n.Type != NodeSort || !hasOrdinalKey(n.OrderBy) || len(n.Children) == 0 {
		return
	}
	names := projectOutputNamesBelow(n.Children[0])
	if len(names) == 0 {
		return
	}
	for i, k := range n.OrderBy {
		if k.Position <= 0 || k.Position > len(names) {
			// Out of range against the EXPANDED list is a real error, but it
			// is not this pass's to raise: Optimize returns no error, and a
			// key left unresolved is refused by name at plan time with the
			// position in the message. Leaving it is what keeps that path
			// reachable.
			continue
		}
		n.OrderBy[i].Column = names[k.Position-1]
		n.OrderBy[i].Position = 0
	}
}

// RefuseUnresolvedOrdinalSortKeys reports the first select-list ordinal
// ResolveOrdinalSortKeys could not answer, as the error the client sees.
//
// Called once from each plan entry point — Planner.Plan and
// Planner.PlanDistributed — so BOTH engines refuse the same shape with the
// same class and the same wording. Refusing inside the sort BUILDER would
// have covered the single-process path only, and the DAG would have gone on
// to spell a stage over a key named "1" and failed three task attempts later
// with a message about an input schema.
//
// Loud, never quiet: the alternative is to sort on the numeric constant, which
// is the same value in every row and returns the input untouched — right rows,
// arbitrary sequence, no error. That is the failure the whole hidden-sort-key
// pass exists to end (#320).
func RefuseUnresolvedOrdinalSortKeys(n *Node) error {
	if n == nil {
		return nil
	}
	if n.Type == NodeSort && len(n.Children) > 0 {
		for _, k := range n.OrderBy {
			if k.Position <= 0 {
				continue
			}
			if names := projectOutputNamesBelow(n.Children[0]); len(names) > 0 {
				// The list WAS countable, so this is PostgreSQL's own error
				// for the shape, verbatim in kind: `ORDER BY position 99 is
				// not in select list`, SQLSTATE 42P10.
				return sqlerr.New("42P10",
					"ORDER BY position %d is not in select list (it has %d columns)",
					k.Position, len(names))
			}
			return sqlerr.New("42P10",
				"ORDER BY position %d: `SELECT *` over this FROM clause expands to a "+
					"column list the planner cannot count — a star over a join, or over "+
					"a derived table whose own FROM is a join, is left unexpanded, "+
					"because guessing its column set would silently change which "+
					"columns the query returns. Name the columns, or ORDER BY the "+
					"column itself",
				k.Position)
		}
	}
	for _, child := range n.Children {
		if err := RefuseUnresolvedOrdinalSortKeys(child); err != nil {
			return err
		}
	}
	return nil
}

func hasOrdinalKey(keys []OrderExpr) bool {
	for _, k := range keys {
		if k.Position > 0 {
			return true
		}
	}
	return false
}

// projectOutputNamesBelow is the output column names of the nearest Project
// under a Sort, in order, excluding the planner's own hidden columns.
//
// A Distinct or a Filter between the Sort and its Project passes rows through
// without renaming them, so the walk descends through those; anything else
// stops it, because a node that DERIVES rows decides its own output names and
// guessing them is how a sort key silently matches nothing.
func projectOutputNamesBelow(n *Node) []string {
	for i := 0; n != nil && i < 8; i++ {
		switch n.Type {
		case NodeProject:
			if HasStarProjection(n) {
				// The star did not expand — over a join, or over a derived
				// table whose columns this layer cannot enumerate. Answering
				// from the unexpanded list would count the star as ONE
				// column, which is the wrong answer rather than none.
				return nil
			}
			visible := VisibleProjections(n.Projections)
			out := make([]string, 0, len(visible))
			for _, p := range visible {
				out = append(out, projectionOutputName(p))
			}
			return out
		case NodeScan:
			// `SELECT * FROM t ORDER BY 1` with no other select item builds
			// NO Project at all — the star IS the scan's output, so there is
			// nothing for ExpandStarProjections to rewrite and the names are
			// the scan's own. ScanColumns is physical.AnnotateScanColumns'
			// record of the table's real columns, in schema order, and it is
			// populated before Optimize runs on every path that reaches here;
			// RequiredColumns is deliberately NOT used, because pruning may
			// narrow it and a position counts the query's OUTPUT columns.
			return append([]string(nil), n.ScanColumns...)
		case NodeDistinct, NodeFilter, NodeLimit:
			if len(n.Children) == 0 {
				return nil
			}
			n = n.Children[0]
		default:
			return nil
		}
	}
	return nil
}

// projectionOutputName is the name a projection publishes, chosen the same way
// buildProject chooses it. Keeping the two in step is the whole point: a sort
// key resolved to a name the Project does not emit matches nothing, which is
// the failure mode this file's header describes.
func projectionOutputName(p Projection) string {
	switch {
	case p.Alias != "":
		return p.Alias
	case p.Column != "":
		return p.Column
	default:
		return p.Expr
	}
}

// deferredOrdinal reports the select-list position an ORDER BY term names when
// the parser could not count it — a numeric literal still standing in a list
// that carries a `*`.
//
// It answers false for every other shape, including a numeric literal in a
// list with no star: the parser already rewrote those, so one reaching here
// names no select item at all and must keep its existing refusal.
func deferredOrdinal(ob plansql.OrderByItem, cols []plansql.SelectColumn) (int, bool) {
	lit, ok := unwrapParens(ob.Expr).(*plansql.Lit)
	if !ok || lit.Kind != plansql.LitNumber {
		return 0, false
	}
	if !hasStarColumn(cols) {
		return 0, false
	}
	pos, err := strconv.Atoi(strings.TrimSpace(lit.Value))
	if err != nil || pos < 1 {
		return 0, false
	}
	return pos, true
}

func hasStarColumn(cols []plansql.SelectColumn) bool {
	for _, c := range cols {
		if c.Star {
			return true
		}
	}
	return false
}
