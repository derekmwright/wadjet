package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
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
// What it declines, and why the boundary is exactly here:
//
//   - Anything with a Project below the pass-through nodes. Not this
//     function's case at all — findOutputProjectionNode answers it, and
//     `SELECT * FROM (SELECT c0 AS x FROM t) s` must publish `x`, not `c0`.
//   - A star over a JOIN. ExpandStarProjections declines the same shape for
//     the same reason (its column set is not knowable from one scan), and the
//     executed schema qualifies the right side's columns ("b.c0",
//     exec/join.go's qualCol) under a rule that lives in the operator. There
//     is no second copy of that rule here: a name spelled two ways by two
//     namers is ADR-0026's defect, and a declaration that disagreed with the
//     non-empty answer would be worse than none. Zero-row `SELECT *` over a
//     join is DEFERRED, tracked with the executed naming itself, which
//     diverges from PostgreSQL (c0, c1, c0, c1) whether or not there are rows.
//   - A star over an Aggregate, a Window, or a table function. The emitted
//     names there are the operator's, not the catalog's.
//
// ok=false means "not a bare star over a resolvable scan", and the ordinary
// projection walk answers (with its own nil, where there is no Project).
func starOnlyDeclaredOutputSchema(root *logical.Node) ([]parquet.Column, bool) {
	scan, names := starOnlySourceScan(root)
	if scan == nil || len(scan.ScanColumns) == 0 {
		return nil, false
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
