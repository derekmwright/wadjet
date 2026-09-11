package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// declaredJoinSchema derives one join side's plan-time schema from annotated
// scan names/order/types, narrowed by want (NeededColumns plus join keys).
// Empty want retains every scan column. Match buildReadSchema order: table
// schema per scan, then scans in walk order. exec.HashJoin consults this
// advisory schema only when the side produces no batch, so approximations for
// untypable subtrees do not affect non-empty joins. Empty outer-join sides
// must have present NULL columns, not absent columns (#348, #352).
func declaredJoinSchema(n *logical.Node, want []string, published map[*logical.Node]bool,
	subqueryDecl func(string) (parquet.Column, bool)) []parquet.Column {
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
		// A SECURITY PROJECTION IS THE RELATION ITS SIDE PUBLISHES, for the
		// same reason a materialized block is: an ABAC column policy puts one
		// directly over the scan, and what the side hands the join is that
		// projection — DENIED columns gone, MASKED ones replaced. Declaring
		// the scan under it named `salary` in the RowDescription of
		// `SELECT * FROM policed JOIN other`, to the identity whose policy
		// denies it (#859, ADR-0033's 2026-09-08 amendment). Same rule and
		// same reader as the block arm; nothing else about this walk moves.
		if cur.Type == logical.NodeProject && (published[cur] || cur.SecurityBarrier) {
			// A MATERIALIZED BLOCK IS ITS PROJECTION (#984, #980). The stage
			// under this join runs the block's SELECT list as an OpProject,
			// so the relation it publishes is the projection and not the
			// stream below it. Declaring the stream here made an EMPTY side
			// write a file of a different WIDTH from its siblings' —
			// ADR-0010's `one stage's files describe one relation`.
			for _, col := range declaredBlockSchema(cur, wantSet, published, subqueryDecl) {
				lc := strings.ToLower(blockBareName(col.Name))
				if seen[lc] {
					continue
				}
				seen[lc] = true
				out = append(out, col)
			}
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
		if cur.Type == logical.NodeAggregate && len(cur.Children) == 1 {
			// Stop at an aggregate: its output differs from the scan underneath it.
			// Empty and non-empty partitions must agree on output width and types
			// (ADR-0010; #767, #956), including hidden correlation-key slots.
			// Declare keys first using stageGroupKeyNames and stageEmittedKeyNames
			// (exec.PublishedGroupKeyNames), then each aggregate under its OutputCol,
			// in the operator's emission order.
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
				col := parquet.Column{Name: name, Type: t, Nullable: true}
				if t == parquet.TypeDecimal {
					// A DECIMAL carries half of every value in its (p, s):
					// the chunk holds the unscaled integer and the header
					// holds the scale (ADR-0010). Leaving them at zero
					// declared `numeric` with no typmod where the same query
					// with rows declares NUMERIC(18,4), so a zero-row answer
					// and a non-empty one described the column differently on
					// the wire — and an empty side of a join wrote a header
					// its siblings disagree with.
					if m, ok := lookupColDecimal(cur.ScanColDecimal, name); ok {
						col.Precision, col.Scale = m.Precision, m.Scale
					}
				}
				out = append(out, col)
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
func joinSideSchemas(node *logical.Node, leftKeys, rightKeys []string,
	published map[*logical.Node]bool, subqueryDecl func(string) (parquet.Column, bool)) (probe, build []parquet.Column) {
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
		return declaredJoinSchema(node.Children[0], nil, published, subqueryDecl),
			declaredJoinSchema(node.Children[1], nil, published, subqueryDecl)
	}
	want := make([]string, 0, len(node.NeededColumns)+len(leftKeys)+len(rightKeys))
	want = append(want, node.NeededColumns...)
	want = append(want, leftKeys...)
	want = append(want, rightKeys...)
	return declaredJoinSchema(node.Children[0], want, published, subqueryDecl),
		declaredJoinSchema(node.Children[1], want, published, subqueryDecl)
}
