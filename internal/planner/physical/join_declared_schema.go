// SPDX-License-Identifier: MIT

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
		if w = wantBareName(w); w != "" {
			wantSet[w] = true
		}
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
			// A RENAME'S WANT ARRIVES IN THE CONSUMER'S SPELLING and the walk
			// below enumerates the PRODUCER'S. `(SELECT o2.customer AS c …) s`
			// is asked for `c` and offers `customer`, so narrowing by the want
			// alone dropped the column from the DECLARATION while the stream
			// still carried it — and on an OUTER join the task whose build
			// partition was EMPTY then wrote a file one column narrower than
			// its siblings': `declares 3 columns where an earlier file of the
			// same stage input declared 4` (ADR-0010, arc R2 round 2 B1).
			//
			// The want is WIDENED rather than replaced: this Project's own
			// items may also be wanted under their published names, and an
			// empty want already means "every column".
			if len(wantSet) > 0 {
				for _, pr := range cur.Projections {
					if pr.Column == "" {
						continue
					}
					pub := strings.ToLower(strings.TrimSpace(pr.Alias))
					if pub == "" {
						pub = wantBareName(pr.Column)
					}
					if !wantSet[pub] {
						continue
					}
					wantSet[wantBareName(pr.Column)] = true
					if pr.Expr != "" {
						wantSet[wantBareName(pr.Expr)] = true
					}
				}
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
			emitted := stageEmittedKeyNames(published, resolve, logicalAggOutNames(cur))
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
			alias := cur.TableAlias
			if alias == "" {
				alias = cur.TableName
			}
			for _, name := range cur.ScanColumns {
				lc := strings.ToLower(name)
				bare := lc
				emit := name
				if seen[lc] {
					// A SECOND RELATION INSIDE THIS SIDE ANSWERING TO ONE
					// NAME is not a column to drop: a JOIN under this side
					// emits it QUALIFIED by the relation that owns it
					// (`joinOutputSchemaWithMapping`'s duplicate rule), so
					// the stream is one column WIDER than the bare names
					// suggest. Dropping it here declared a narrower relation
					// than the side produces, and on an OUTER join the task
					// whose build partition was EMPTY wrote that narrower
					// file beside its siblings' — `declares 2 columns where
					// an earlier file of the same stage input declared 3`
					// (ADR-0010), on every outer join over a join-bodied
					// derived arm keyed on a name both of its relations
					// publish (arc R2 round 2, B1).
					//
					// With no alias the executor cannot qualify it either and
					// DROPS it (the `case isDup:` arm), so the declaration
					// drops it too: the two agree in both directions.
					if alias == "" {
						continue
					}
					emit = alias + "." + name
					lc = strings.ToLower(emit)
					if seen[lc] {
						continue
					}
				}
				if len(wantSet) > 0 && !wantSet[bare] {
					continue
				}
				t, ok := cur.ScanColTypes[bare]
				if !ok {
					continue
				}
				seen[lc] = true
				col := parquet.Column{Name: emit, Type: t, Nullable: true}
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

// wantSetBareName is the spelling declaredJoinSchema's want set is keyed by: a
// qualified reference ("o.o_orderstatus") names the same column as its bare
// form in the scan's schema.
func wantBareName(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	if dot := strings.LastIndexByte(name, '.'); dot >= 0 {
		name = name[dot+1:]
	}
	return name
}

// respellDeclaredJoinSideSchemas makes a join's advisory side schemas spell
// their columns the way the producing stage really does, WHERE A LATE PASS
// CHANGED THAT SPELLING AFTER THE DECLARATION WAS WRITTEN.
//
// `declaredJoinSchema` mirrors `joinOutputSchemaWithMapping`'s duplicate rule
// at emission: the first relation to publish a bare name keeps it and the next
// one is qualified by the alias that owns it. `markCoPathingSelfJoinBuilds`
// then runs over the FINISHED stage list and sets `QualifyAllBuildCols` on
// joins whose build scans co-path (the Q07 rule, ADR-0026 §7), which qualifies
// EVERY build column of those stages — including ones the declaration left
// bare because nothing contested them. A block whose body is such a join then
// declared `customer` where the stream writes `o2.customer`, and the task
// whose partition was EMPTY wrote that name beside its siblings':
// `names column 2 "customer" where an earlier file of the same stage input
// named it "o2.customer"` (ADR-0010, arc R2 round 2 B1).
//
// It fires ONLY for a side whose producing chain carries that flag, because
// that is the only spelling this layer knows changed. Elsewhere the
// declaration stands: the stream MODEL and the executor agree on every shape
// this arc measured except one, a FULL OUTER join over a CamelCase schema
// where the model qualifies a build column the executor emits bare
// (`camel_case_schema_invariance_test.go`, measured in round 2 and recorded as
// a filing candidate) — and respelling from the model there put the model's
// answer into a file the executor then disagreed with.
//
// Only the QUALIFIER moves, never the column's own spelling: the model carries
// whatever case the planner's lists hold and the executor keeps the catalog's.
func respellDeclaredJoinSideSchemas(stages []Stage) {
	idx := make(map[string]int, len(stages))
	for i := range stages {
		idx[stages[i].ID] = i
	}
	respell := func(decl []parquet.Column, dep string) {
		d, ok := idx[dep]
		if !ok || len(decl) == 0 || !chainQualifiesAllBuildCols(stages, idx, d, passThroughDepth) {
			return
		}
		in := stageStreamColumns(stages, idx, &stages[d], passThroughDepth)
		if len(in) == 0 {
			return
		}
		for j := range decl {
			if name, ok := streamSpellingFor(in, decl[j].Name); ok {
				decl[j].Name = name
			}
		}
	}
	for i := range stages {
		s := &stages[i]
		if !isJoinStage(s.Type) {
			continue
		}
		probeDep, buildDep := s.LeftDepStage, s.RightDepStage
		if probeDep == "" && len(s.Dependencies) > 0 {
			probeDep = s.Dependencies[0]
		}
		if buildDep == "" && len(s.Dependencies) > 1 {
			buildDep = s.Dependencies[1]
		}
		respell(s.JoinProbeSchema, probeDep)
		respell(s.JoinBuildSchema, buildDep)
	}
}

// chainQualifiesAllBuildCols reports whether the stage at i, or a stage it
// reads through, had `QualifyAllBuildCols` set on it.
func chainQualifiesAllBuildCols(stages []Stage, idx map[string]int, i, depth int) bool {
	if depth <= 0 || i < 0 || i >= len(stages) {
		return false
	}
	if stages[i].QualifyAllBuildCols {
		return true
	}
	for _, dep := range stages[i].Dependencies {
		if d, ok := idx[dep]; ok && chainQualifiesAllBuildCols(stages, idx, d, depth-1) {
			return true
		}
	}
	return false
}

// streamSpellingFor is the QUALIFIER a stream carries a declared column under,
// put back on the declared column's own spelling, when the two disagree about
// it and exactly one stream column answers to the declared bare name.
func streamSpellingFor(in []streamCol, declared string) (string, bool) {
	want := strings.TrimSpace(declared)
	if want == "" {
		return "", false
	}
	bare := wantBareName(want)
	written := want
	if dot := strings.LastIndexByte(written, '.'); dot >= 0 {
		written = written[dot+1:]
	}
	match, hits := "", 0
	for _, c := range in {
		if c.Dropped {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(c.Name), want) {
			return "", false // the stream already spells it this way
		}
		if wantBareName(c.Name) == bare {
			match, hits = strings.TrimSpace(c.Name), hits+1
		}
	}
	if hits != 1 {
		return "", false
	}
	if dot := strings.LastIndexByte(match, '.'); dot >= 0 {
		return match[:dot+1] + written, true
	}
	return written, true
}
