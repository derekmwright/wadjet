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
		// A PROJECTION THAT LISTS ITS COLUMNS *IS* THE SIDE'S OUTPUT, and
		// under a STAR that is the whole answer: with no `want` list to
		// narrow by, walking past it declares every column of the scans
		// BELOW it — the columns the projection dropped included. A task
		// whose build partition was empty then wrote a file seven columns
		// wide beside files carrying the five the projection publishes, and
		// the shuffle read refused the pair (ADR-0010).
		//
		// Only in the star case (`len(wantSet) == 0`). With a want list the
		// narrowing is already done by it, and the fall-through to the scans
		// is what names a renamed column the way the DAG spells it.
		if cur.Type == logical.NodeProject && len(wantSet) == 0 && len(cur.Children) == 1 &&
			projectionListsItsColumns(cur.Projections) {
			in := emittedColTypes(cur.Children[0])
			for _, pr := range cur.Projections {
				name := pr.Alias
				if name == "" {
					name = pr.Column
				}
				lc := strings.ToLower(name)
				if seen[lc] {
					continue
				}
				src := pr.Column
				if src == "" {
					src = pr.Alias
				}
				t, ok := lookupColType(in, src)
				if !ok {
					// No plan-time type for this column. Declaring one that
					// is merely plausible is what ADR-0010's guard exists to
					// catch, so it is left out and the runtime's own schema
					// stands wherever the side is not empty.
					continue
				}
				seen[lc] = true
				out = append(out, parquet.Column{Name: name, Type: t, Nullable: true})
			}
			return
		}
		if cur.Type == logical.NodeAggregate && len(cur.Children) == 1 {
			// AN AGGREGATE'S OUTPUT IS NOT ITS INPUT, so the walk stops here
			// rather than describing this side by the columns of the scan
			// underneath it.
			//
			// A decorrelated LATERAL is the shape that makes it matter: its
			// build side is `Project -> Aggregate -> Scan`, and the walk
			// declared the SCAN's `g` (INT32) for a side whose real output
			// carries `g` = MAX(id) (INT64). A task whose build partition was
			// empty then wrote a `.wshf` file declaring that column INT32
			// while every task with rows declared it INT64, and the consumer
			// types itself from whichever file it reads first — refused at the
			// read as `column "g" is INT32 ... but INT64 in an earlier file`
			// (ADR-0010), or, where the two shapes differed in WIDTH,
			// `declares 2 columns where an earlier file ... declared 5`.
			// #767's DAG half, and #956's after the correlation key moved
			// into a hidden slot.
			//
			// The names are the DAG's own: the published list `stageGroupKeyNames`
			// computes, put through `exec.PublishedGroupKeyNames`' output rule
			// by `stageEmittedKeyNames`, then one column per aggregate under
			// its OutputCol. Keys first, which is the order the operator emits
			// them in.
			in := emittedColTypes(cur.Children[0])
			published, resolve := stageGroupKeyNames(cur, cur.Children[0])
			emitted := stageEmittedKeyNames(published, resolve)
			keyTypes, _ := derivedGroupKeyTypes(cur.GroupBy, cur.Children[0])
			for i, name := range emitted {
				lc := strings.ToLower(name)
				if seen[lc] || (len(wantSet) > 0 && !wantSet[lc]) {
					continue
				}
				t, ok := lookupColType(in, cur.GroupBy[i])
				if !ok {
					t, ok = keyTypes[cur.GroupBy[i]]
				}
				if !ok {
					// No plan-time type for this key. Declaring one that is
					// merely plausible is what the ADR-0010 guard exists to
					// catch, so the column is left out and the runtime's own
					// schema stands wherever the side is not empty.
					continue
				}
				seen[lc] = true
				out = append(out, parquet.Column{Name: name, Type: t, Nullable: true})
			}
			for _, agg := range cur.AggExprs {
				lc := strings.ToLower(agg.OutputCol)
				if agg.OutputCol == "" || seen[lc] || (len(wantSet) > 0 && !wantSet[lc]) {
					continue
				}
				t, known := aggSpecOutputType(cur, agg)
				if !known {
					continue
				}
				col := parquet.Column{Name: agg.OutputCol, Type: t, Nullable: true}
				if t == parquet.TypeDecimal {
					// A DECIMAL carries half of every value in its HEADER
					// (ADR-0010): the chunk holds the unscaled integer and the
					// header holds the scale. Declaring one with no (p,s) is
					// exactly the disagreement the shuffle guard refuses, so a
					// DECIMAL aggregate whose (p,s) is not known at plan time
					// is left out rather than declared at scale 0.
					m, known := aggSpecOutputDecimal(cur, agg)
					if !known {
						continue
					}
					col.Precision, col.Scale = m.Precision, m.Scale
				}
				seen[lc] = true
				out = append(out, col)
			}
			return
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

// projectionListsItsColumns reports whether every projection names an output
// column this pass can declare — no star, no unnamed expression.
func projectionListsItsColumns(projs []logical.Projection) bool {
	if len(projs) == 0 {
		return false
	}
	for _, pr := range projs {
		name := pr.Alias
		if name == "" {
			name = pr.Column
		}
		if name == "" || name == "*" || strings.HasSuffix(name, ".*") {
			return false
		}
	}
	return true
}

// joinSideSchemas returns the declared probe- and build-side schemas for a
// join node: the columns downstream needs plus the join keys, which is
// exactly what the shuffle carries for each side.
func joinSideSchemas(node *logical.Node, leftKeys, rightKeys []string) (probe, build []parquet.Column) {
	if node == nil || len(node.Children) < 2 {
		return nil, nil
	}
	// NO NeededColumns is not "needs nothing" — it is a `SELECT *`, which
	// needs EVERY column. Narrowing the declaration to the join KEYS there
	// described a side by two columns while the side really produced five,
	// so a task whose build partition was empty wrote a file with the keys
	// alone beside files carrying the whole relation: `declares 3 columns
	// where an earlier file of the same stage input declared 4` (ADR-0010),
	// on every star over a decorrelated LATERAL. An empty want keeps every
	// column, which is what a star asks for.
	if len(node.NeededColumns) == 0 {
		return declaredJoinSchema(node.Children[0], nil), declaredJoinSchema(node.Children[1], nil)
	}
	want := make([]string, 0, len(node.NeededColumns)+len(leftKeys)+len(rightKeys))
	want = append(want, node.NeededColumns...)
	want = append(want, leftKeys...)
	want = append(want, rightKeys...)
	return declaredJoinSchema(node.Children[0], want), declaredJoinSchema(node.Children[1], want)
}
