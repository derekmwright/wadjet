package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// declaredJoinSchema derives, at PLAN time, the columns one side of a join
// will produce — from the catalog annotation AnnotateScanColumns leaves on
// the scans beneath it (ScanColumns for the names and order, ScanColTypes for
// the types), narrowed to the columns the join actually carries.
//
// It exists for the one case the runtime cannot answer: a side that delivers
// no batch at all has no schema, and an outer join still owes rows shaped by
// it. A LEFT JOIN over an empty build must emit every probe row with the
// build's columns present and NULL — absent columns read as NULL through the
// projection's missing-name fallback but make COUNT(col) count them and
// `IS NULL` match none (#348) — and a RIGHT/FULL join over an empty probe
// partition must emit its build rows with the probe's columns present and
// NULL (#352).
//
// The result is advisory: exec.HashJoin consults it only when the side
// produced nothing, so an approximation for a subtree the walk cannot type
// exactly (an aggregate that renames its output, a table function) costs
// nothing on any non-empty join. want narrows the column set — pass the
// join's NeededColumns plus its keys, the same set the shuffle carries; an
// empty want keeps every scan column.
//
// Ordering mirrors buildReadSchema: table-schema order per scan, scans in
// walk order, which is the order a real batch from that side arrives in.
func declaredJoinSchema(n *logical.Node, want []string) []parquet.Column {
	if n == nil {
		return nil
	}
	wantSet := make(map[string]bool, len(want))
	for _, w := range want {
		w = strings.ToLower(strings.TrimSpace(w))
		if w == "" {
			continue
		}
		// A qualified reference ("o.o_orderstatus") names the same column as
		// its bare form in the scan's schema.
		if dot := strings.LastIndexByte(w, '.'); dot >= 0 {
			w = w[dot+1:]
		}
		wantSet[w] = true
	}

	var out []parquet.Column
	seen := make(map[string]bool)
	var walk func(*logical.Node)
	walk = func(cur *logical.Node) {
		if cur == nil {
			return
		}
		if cur.Type == logical.NodeProject {
			// A COMPUTED projection column exists only here — no scan
			// carries it. Declare it under its alias with the same type the
			// materializing projection emits it as
			// (absorbComputedSubqueryProjection, #383), so an empty side
			// still shapes it correctly. Bare columns and renames fall
			// through to the scans below, source-named as the DAG spells
			// them.
			var colTypes colDecls
			var strictInt map[string]bool
			haveTypes := false
			for _, pr := range cur.Projections {
				if pr.IsAgg || pr.Column != "" || pr.Alias == "" ||
					pr.ASTExpr == nil || isSimpleColRefForRename(pr.ASTExpr) {
					continue
				}
				lc := strings.ToLower(pr.Alias)
				if seen[lc] || (len(wantSet) > 0 && !wantSet[lc]) {
					continue
				}
				if !haveTypes && len(cur.Children) == 1 {
					colTypes = inputColDecls(cur.Children[0])
					// Same integer-preserving-arithmetic hint
					// absorbComputedSubqueryProjection passes when it
					// materializes this same computed column into the scan
					// fragment (#297, #445): without it, `id + 1` declares
					// FLOAT64 here but INT64 there, and a join over an empty
					// side disagrees with a join over a full one about the
					// type of its own column (#473).
					strictInt = strictIntArithCols(cur.Children[0])
					haveTypes = true
				}
				seen[lc] = true
				decl := inferProjectionDeclType(pr.ASTExpr, parquet.TypeString, strictInt, colTypes)
				out = append(out, parquet.Column{
					Name:      pr.Alias,
					Type:      decl.ID,
					Precision: decl.Precision,
					Scale:     decl.Scale,
					Nullable:  true,
				})
			}
		}
		if cur.Type == logical.NodeScan {
			for _, name := range cur.ScanColumns {
				lc := strings.ToLower(name)
				if seen[lc] {
					continue
				}
				if len(wantSet) > 0 && !wantSet[lc] {
					continue
				}
				t, ok := cur.ScanColTypes[lc]
				if !ok {
					continue
				}
				seen[lc] = true
				out = append(out, parquet.Column{Name: name, Type: t, Nullable: true})
			}
			return
		}
		if cur.Type == logical.NodeJoin && len(cur.Children) == 2 {
			walk(cur.Children[0])
			// Semi/anti joins expose only their probe side.
			if jt := strings.ToLower(cur.JoinType); jt == "semi" || jt == "anti" {
				return
			}
			walk(cur.Children[1])
			return
		}
		for _, child := range cur.Children {
			walk(child)
		}
	}
	walk(n)
	return out
}

// joinSideSchemas returns the declared probe- and build-side schemas for a
// join node: the columns downstream needs plus the join keys, which is
// exactly what the shuffle carries for each side.
func joinSideSchemas(node *logical.Node, leftKeys, rightKeys []string) (probe, build []parquet.Column) {
	if node == nil || len(node.Children) < 2 {
		return nil, nil
	}
	want := make([]string, 0, len(node.NeededColumns)+len(leftKeys)+len(rightKeys))
	want = append(want, node.NeededColumns...)
	want = append(want, leftKeys...)
	want = append(want, rightKeys...)
	return declaredJoinSchema(node.Children[0], want), declaredJoinSchema(node.Children[1], want)
}
