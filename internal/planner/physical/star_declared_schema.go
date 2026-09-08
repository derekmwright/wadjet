package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// starOnlyDeclaredOutputSchema is declaredOutputSchema for `SELECT *` — the
// one SELECT list that produces no Project node to read.
//
// logical.BuildFromSelect skips the projection entirely when the list is a
// bare star (builder.go's `if !isStarOnly(info.Columns)`), because the star
// selects the input unchanged and a projection would be the identity. The
// consequence is that findOutputProjectionNode finds nothing, declaredOutput
// Schema answers nil, and a `SELECT *` that returns ZERO rows reaches the
// client with no columns at all: psql prints nothing and JDBC's executeQuery
// throws "No results were returned by the query" (#846). Every other zero-row
// shape has been described from the plan since #416 — `SELECT c0 FROM t WHERE
// false` declares c0 — so this was the one hole, and it is the shape a BI
// tool opens a table with.
//
// The columns are the star's SOURCE columns, resolved exactly the way
// logical.ExpandStarProjections resolves them for a star that DOES share its
// SELECT list with another item: the lone scan below, its catalog-annotated
// ScanColumns in schema order, with the types AnnotateScanColumns left beside
// them. Same source, same order, so the declared answer and the executed one
// describe one result.
//
// It is NOT only a description. declaredOutputSchema also feeds
// subqueryOutputColumn (plan.go, #696), which picks the COMPARISON RULE for a
// scalar subquery on every row — so `d = (SELECT * FROM one_row)` over two
// DECIMALs of different scale answered ZERO rows without a declaration, where
// PostgreSQL 17 and the named spelling `(SELECT v FROM one_row)` both answer
// one. Declaring the star hands that call the same column the named spelling
// has always handed it, which is why an approximation here would not be free
// and why the walk DECLINES rather than guesses
// (wadjet.TestStarScalarSubqueryComparesLikeItsNamedSpelling, round-1 P2).
//
// What it declines, and why the boundary is exactly here:
//
//   - Anything with a Project below the pass-through nodes. Not this
//     function's case at all — findOutputProjectionNode answers it, and
//     `SELECT * FROM (SELECT c0 AS x FROM t) s` must publish `x`, not `c0`.
//   - A star over a JOIN, which is starJoinDeclaredOutputSchema's case below.
//     It is answered by CALLING the operator's own namer rather than by
//     copying it, which is why it can be answered at all: a name spelled two
//     ways by two namers is ADR-0026's defect, and a declaration that
//     disagreed with the non-empty answer would be worse than none.
//   - A star over an Aggregate, a Window, or a table function. The emitted
//     names there are the operator's, not the catalog's.
//
// ok=false means "not a bare star over a resolvable scan", and the ordinary
// projection walk answers (with its own nil, where there is no Project).
func starOnlyDeclaredOutputSchema(root *logical.Node) ([]parquet.Column, bool) {
	scan, names := starOnlySourceScan(root)
	if scan == nil || len(scan.ScanColumns) == 0 {
		return starJoinDeclaredOutputSchema(root)
	}
	if names == nil {
		names = scan.ScanColumns
	}
	out := make([]parquet.Column, 0, len(names))
	for _, spelled := range names {
		// The catalog's own spelling, never the reference's: a group key can
		// arrive qualified ("t.c0") or in another case, and the operator
		// publishes the column the way the table declares it.
		name, ok := scanColumnSpelling(scan, spelled)
		if !ok {
			return nil, false
		}
		col := parquet.Column{Name: name, Nullable: true}
		t, ok := lookupColType(scan.ScanColTypes, name)
		if !ok {
			// A column the annotation could not type would go out as text,
			// which is #416's failure mode. Declining the whole schema keeps
			// the honest "no declaration" answer instead of a wrong one.
			return nil, false
		}
		col.Type = t
		if t == parquet.TypeDecimal {
			// Same rule as the projection walk: a real (p, s) when the
			// catalog carries one, precision 0 — pgTypeMod's
			// "unconstrained" — rather than a fabricated pair (#458).
			if m, ok := lookupColDecimal(scan.ScanColDecimal, name); ok {
				col.Precision, col.Scale = m.Precision, m.Scale
			}
		}
		out = append(out, col)
	}
	return out, true
}

// scanColumnSpelling resolves a column reference to the scan's own spelling
// of that column, or ok=false when the scan carries no such column. A
// qualified reference names the same column as its bare form, which is the
// rule lookupColType already applies to the type beside it.
func scanColumnSpelling(scan *logical.Node, ref string) (string, bool) {
	lc := strings.ToLower(strings.TrimSpace(ref))
	if dot := strings.LastIndexByte(lc, '.'); dot >= 0 {
		lc = lc[dot+1:]
	}
	for _, col := range scan.ScanColumns {
		if strings.ToLower(col) == lc {
			return col, true
		}
	}
	return "", false
}

// starOnlySourceScan returns the scan a bare `SELECT *` reads, and the names
// it publishes when a grouping below the star narrows them (nil means "the
// scan's own columns, in schema order"). scan is nil when the plan is not
// that shape.
//
// The descent is findOutputProjectionNode's, minus the Project arm: a Filter,
// Sort, Limit or Distinct above the scan passes its input through unchanged,
// which is why `SELECT * FROM t WHERE false`, `... ORDER BY c0` and `...
// LIMIT 10` all publish the table's own columns. Reaching a Project means
// findOutputProjectionNode owns the answer; reaching anything else means the
// emitted columns are not the scan's.
//
// The one node that changes the answer without leaving the star's world is a
// pure GROUPING — an Aggregate with group keys and no aggregate functions,
// which is `SELECT * FROM t GROUP BY c0, c1` and, after
// logical.rewriteStarDistinct, `SELECT DISTINCT *` as well. It emits its
// group keys, in GroupBy order, which is the order the HashAggregate lays its
// output out in; the keys are read from the same scan annotation, so nothing
// new names anything.
func starOnlySourceScan(n *logical.Node) (*logical.Node, []string) {
	var names []string
	for n != nil {
		switch n.Type {
		case logical.NodeScan:
			if n.IsTableFunc {
				// AnnotateScanColumns leaves no catalog annotation on a
				// table function, so there is nothing to declare from.
				return nil, nil
			}
			return n, names
		case logical.NodeFilter, logical.NodeSort, logical.NodeLimit, logical.NodeDistinct:
			if len(n.Children) != 1 {
				return nil, nil
			}
			n = n.Children[0]
		case logical.NodeAggregate:
			// A second grouping below the first would mean the names the
			// upper one publishes are the lower one's output, not the
			// scan's; one is the whole of the shape this arm covers.
			if names != nil || len(n.Children) != 1 || len(n.AggExprs) != 0 || len(n.GroupBy) == 0 {
				return nil, nil
			}
			names = n.GroupBy
			n = n.Children[0]
		default:
			return nil, nil
		}
	}
	return nil, nil
}

// starJoinDeclaredOutputSchema is the declaration for `SELECT *` over a JOIN —
// the one zero-row shape that reached a client with NO COLUMNS AT ALL, on
// every arm (#978, #846's twin).
//
// `SELECT * FROM a JOIN b ON …` produces no Project node for the walk above to
// read and no single scan for it to describe, so a result WITH rows was
// described from the first batch and a result without rows was described by
// nothing: psql printed no header, pgJDBC's executeQuery had no column
// metadata, and the pgwire door sent an EMPTY RowDescription because that was
// the most honest thing it could say. `SELECT * FROM a WHERE false` has
// declared its columns since #416.
//
// THE NAMES ARE THE OPERATOR'S OWN. The join executor emits the probe's
// columns and then the build's, with every DUPLICATE bare name qualified by
// its owning alias, and that rule lives in `exec.joinOutputSchemaWithMapping`.
// This function does not reimplement it — it assembles the arguments from the
// plan and calls it (`exec.JoinOutputSchema`), which is why the declaration
// and the executed answer cannot disagree. A second copy of that rule is
// exactly what this file declined to write before, and it was right to.
//
// THE BOUNDARY IS ONE JOIN, and it is a claim rather than a convenience:
//
//   - Neither side may contain a join of its own. `declaredJoinSchema` walks a
//     nested join by CONCATENATING its sides and dropping duplicate names,
//     which is not the operator's rule, so a bushy shape would be described by
//     a list the engine never produces.
//   - `QualifyAllBuildCols` is a STAGE property set only where TWO joins in one
//     chain build from one table (markCoPathingSelfJoinBuilds), so with one
//     join in the plan it is false on every path — which is what lets this be
//     answered from the logical tree at all.
//   - A side whose columns the plan cannot type declines the whole schema, the
//     same rule the scan arm above applies: no declaration beats a wrong one.
func starJoinDeclaredOutputSchema(root *logical.Node) ([]parquet.Column, bool) {
	join := starOnlySourceJoin(root)
	if join == nil {
		return nil, false
	}
	// EACH SIDE IS DECLARED BY WHAT IT PUBLISHES, never by its stream. A
	// derived block is a real relation on both paths — a `Project` operator on
	// the single-process one, a materialized projection on the DAG — so a
	// declaration read from the scan below it describes columns the engine
	// does not emit: `(SELECT order_id, amount FROM kitem WHERE …)` declared
	// TEN fields where PostgreSQL and the non-empty twin describe seven, with
	// `s.id`, `product` and `qty` invented and a rename's alias missing
	// (round-1 B3). That is #984's own defect living inside #978's answer.
	published := sideBlockProjections(join)
	probe := declaredJoinSchema(join.Children[0], nil, published)
	build := declaredJoinSchema(join.Children[1], nil, published)
	if len(probe) == 0 || len(build) == 0 {
		return nil, false
	}
	excludeProbe, excludeBuild := joinHiddenPositions(join)
	out := exec.JoinOutputSchema(mapExecJoinType(strings.ToLower(join.JoinType)),
		probe, build, joinArmAlias(join.Children[1]),
		subtreeNamingOf(join.Children[1]).materializedBuildColOrigins(),
		false, joinProbeOutputFilter(join), excludeProbe, excludeBuild)
	if len(out) == 0 {
		return nil, false
	}
	// A RESERVED NAME IN THE ANSWER MEANS THIS WALK STOPPED TOO EARLY, and it
	// is the property rather than a list of shapes. A decorrelated LATERAL
	// whose empty-input default drops the pad MARKER leaves the slot on the
	// join's output for the operator ABOVE it to remove (ADR-0026 §3c), and
	// this walk models the join, not that operator — so the declaration
	// carried `__key_0` where the executed answer does not. Nothing the
	// planner minted for itself is ever in a client's relation, so seeing one
	// is proof the declaration is not the statement's output, and no
	// declaration beats a wrong one.
	for _, col := range out {
		if plansql.ReservedSlotFamily(col.Name) != "" ||
			plansql.ReservedSlotFamily(blockBareName(col.Name)) != "" {
			return nil, false
		}
	}
	return out, true
}

// starOnlySourceJoin is the single JOIN a bare `SELECT *` reads, or nil when
// the plan is not that shape.
//
// The descent is starOnlySourceScan's — a Filter, Sort, Limit or Distinct
// above the join passes its input through unchanged — and it stops at a
// Project for the same reason: reaching one means findOutputProjectionNode
// owns the answer.
func starOnlySourceJoin(n *logical.Node) *logical.Node {
	for n != nil {
		switch n.Type {
		case logical.NodeJoin:
			if len(n.Children) != 2 || containsJoin(n.Children[0]) || containsJoin(n.Children[1]) {
				return nil
			}
			switch strings.ToLower(n.JoinType) {
			case "semi", "anti":
				// A semi/anti join publishes its probe alone, and no star
				// spells one: the lowering makes them from IN and EXISTS.
				return nil
			}
			return n
		case logical.NodeFilter, logical.NodeSort, logical.NodeLimit, logical.NodeDistinct:
			if len(n.Children) != 1 {
				return nil
			}
			n = n.Children[0]
		default:
			return nil
		}
	}
	return nil
}

// containsJoin reports whether this subtree holds a join anywhere.
func containsJoin(n *logical.Node) bool {
	if n == nil {
		return false
	}
	if n.Type == logical.NodeJoin {
		return true
	}
	for _, c := range n.Children {
		if containsJoin(c) {
			return true
		}
	}
	return false
}

// sideBlockProjections marks the block Project on each side of this join, so
// declaredJoinSchema describes the side by the relation it PUBLISHES.
//
// It is not `Planner.publishedBlocks`: that set answers "did the DAG's stage
// materialize this projection", and the question here is the other one — what
// does this side EMIT — whose answer is the same on both paths, because a
// Project the DAG did not materialize is still a real operator on the
// single-process path and the DAG's star reads the block through the pruning
// that narrows to it.
func sideBlockProjections(join *logical.Node) map[*logical.Node]bool {
	out := map[*logical.Node]bool{}
	for _, side := range join.Children {
		for cur := side; cur != nil && len(cur.Children) == 1; cur = cur.Children[0] {
			if cur.Type == logical.NodeProject {
				if !cur.SecurityBarrier && !logical.HasStarProjection(cur) &&
					len(cur.Projections) > 0 {
					out[cur] = true
				}
				break
			}
			if cur.Type != logical.NodeFilter && cur.Type != logical.NodeLimit &&
				cur.Type != logical.NodeSort && cur.Type != logical.NodeDistinct {
				break
			}
		}
	}
	return out
}
