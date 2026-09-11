package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// starOnlyDeclaredOutputSchema declares bare SELECT * from its source scan,
// in schema order with annotated types, matching the executed result.
// The declaration also controls scalar-subquery comparison; decline rather
// than guess. Projects belong to findOutputProjectionNode; joins delegate to
// starJoinDeclaredOutputSchema and the executor's namer. Aggregate, Window
// and table-function outputs are not catalog columns. ok=false leaves the
// ordinary projection walk to answer. #846, #416, #696; ADR-0026.
// See docs/internals/bare-star-output-declaration.md for the design.
func starOnlyDeclaredOutputSchema(root *logical.Node,
	subqueryDecl func(string) (parquet.Column, bool)) ([]parquet.Column, bool) {
	scan, names := starOnlySourceScan(root)
	if scan == nil || len(scan.ScanColumns) == 0 {
		return starJoinDeclaredOutputSchema(root, subqueryDecl)
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

// starJoinDeclaredOutputSchema declares SELECT * over one join, including
// zero rows, using exec.JoinOutputSchema: probe then build, duplicate bare
// names qualified by their owning alias. #978, #846, #416.
// Neither side may contain a join: declaredJoinSchema's nested concatenation
// and deduplication do not match the executor. With one join,
// QualifyAllBuildCols is false; it is a stage property for co-pathing joins.
// Decline the whole schema if either side cannot be typed.
// See docs/internals/join-star-output-declaration.md for the design.
func starJoinDeclaredOutputSchema(root *logical.Node,
	subqueryDecl func(string) (parquet.Column, bool)) ([]parquet.Column, bool) {
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
	probe := declaredJoinSchema(join.Children[0], nil, published, subqueryDecl)
	build := declaredJoinSchema(join.Children[1], nil, published, subqueryDecl)
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
