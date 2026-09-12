package physical

import (
	"sort"
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// absorbComputedSubqueryProjection adds computed subquery aliases to their
// producing fragment (#383); bare/renamed columns keep SOURCE names and no
// existing column is dropped or renamed, preserving aggregate InputExpr inputs.
// Scan-rooted Project → Filter* → Scan requires exactly one scan stage;
// unsupported producers bail. Window and join arms have dedicated branches below.
// requireEnclosing requires a computing Project under another Project: the sort
// hook passes true to leave output projections to attachScanSelectProjections;
// the join hook passes false. Stage.ProjectExprs uses #169's OpProject machinery.
// See docs/internals/computed-subquery-materialization.md for the design.
func absorbComputedSubqueryProjection(child *logical.Node, childStages []Stage, requireEnclosing bool) bool {
	// Find the COMPUTING Project: descend through stage-less Filters and
	// rename-only Projects (an outer `SELECT rk2 FROM (…) t` wraps the
	// computing subquery in a bare Project — both are passthroughs on the
	// DAG). The first Project that computes is the candidate; anything
	// else ends the walk.
	var proj *logical.Node
	enclosing := 0
	for n := child; n != nil; {
		if n.Type == logical.NodeFilter && len(n.Children) == 1 {
			n = n.Children[0]
			continue
		}
		if n.Type != logical.NodeProject || n.SecurityBarrier || len(n.Children) != 1 {
			return false
		}
		computes := false
		for _, pr := range n.Projections {
			if pr.IsAgg {
				return false
			}
			if pr.Column == "" && pr.Alias != "" && pr.ASTExpr != nil && !isSimpleColRefForRename(pr.ASTExpr) {
				computes = true
			}
		}
		if computes {
			if requireEnclosing && enclosing == 0 {
				return false
			}
			proj = n
			break
		}
		enclosing++
		n = n.Children[0]
	}
	if proj == nil {
		return false
	}
	// Below it: a chain of stage-less nodes ending at a SCAN or at a WINDOW.
	//
	// A WINDOW is the second producer this pass materializes onto, and it is
	// what #742 is: `(SELECT id, SUM(a) OVER () + 0 AS w FROM t)` publishes a
	// value the window stage does not compute — the window emits its slot
	// `__win_0` and nothing named `w` — so when the OTHER arm of the join
	// publishes a `w` of its own, the qualified reference `p.w` resolved to
	// IT. Silently, and with the arms' aliases spelled DIFFERENTLY the same
	// shape fails loudly instead (`column "p.w1" does not exist`). A window
	// stage forwards its input's columns and runs its OpProject ABOVE the
	// operator (ADR-0025, #656 shape g), so the alias can be computed there
	// over the slot.
	//
	// A rename-only Project below the computing one is walked through rather
	// than refused: `(SELECT id, v * 2 AS dv FROM (SELECT id, a AS v FROM t) z)`
	// names `v`, which no stage emits, and the substitution below rewrites it
	// to `a`. Whether the rewrite SUCCEEDED is checked against the target's
	// own stream before anything is attached, so a chain this walk cannot
	// resolve keeps today's behaviour rather than computing the wrong value.
	renameChild := proj.Children[0]
	below := proj.Children[0]
	for below != nil && len(below.Children) == 1 &&
		(below.Type == logical.NodeFilter ||
			(below.Type == logical.NodeProject && !below.SecurityBarrier)) {
		below = below.Children[0]
	}
	// A JOIN is the third producer this pass materializes onto, and it is
	// what #780 is: `(SELECT g.id AS id, g.a * 3 AS a FROM t g JOIN t h ON
	// g.id = h.id) m` publishes a COMPUTED column, the arm's Project emits
	// no stage (ADR-0025), and no stage in the plan computes it — so the
	// enclosing `m.a` resolved through the arm's RAW stream and answered
	// `g.a` unmultiplied, on both DAG arms and in silence. The scan branch
	// below cannot reach it (the arm has TWO scans, and the value is
	// computed over the joined stream), and neither can the window branch.
	// A join fragment's OpProject runs above the join, which is where the
	// arm's own SELECT list is evaluable and the only place it is.
	if below == nil || (below.Type != logical.NodeScan && below.Type != logical.NodeWindow &&
		below.Type != logical.NodeJoin) {
		return false
	}

	// Collect the COMPUTED projections. Renames and bare passthroughs are
	// none of this pass's business; an aggregate projection means the
	// SELECT list is not a row-wise computation at all.
	windowArm := below.Type == logical.NodeWindow
	joinArm := below.Type == logical.NodeJoin
	var computed []ProjectExprSpec
	needCols := map[string]bool{}
	var colTypes colDecls
	var strictInt map[string]bool
	haveTypes := false
	for _, pr := range proj.Projections {
		if pr.IsAgg {
			return false
		}
		if pr.Column != "" || pr.Alias == "" || pr.ASTExpr == nil || isSimpleColRefForRename(pr.ASTExpr) {
			continue
		}
		if referencesSyntheticAgg(pr.ASTExpr) {
			return false
		}
		if !haveTypes {
			colTypes = inputColDecls(proj.Children[0])
			if joinArm {
				// A JOIN arm's declarations have to be read the way the
				// EXECUTOR spells that stream: `inputColDecls` merges the two
				// sides and DROPS a name they declare differently, which is
				// the honest answer to a bare reference and no answer at all
				// to `h.d92 * 2` over two tables that both have a `d92`. The
				// undecided answer falls to the FLOAT rule, and the fragment
				// then tried to store a DECIMAL's rendering into a float
				// vector — the #361 guard, on a query PostgreSQL answers.
				// `emittedColDecls` publishes the per-arm QUALIFIED entries
				// beside the merged bare ones (withJoinArmQualifiers), which
				// is exactly the spelling the arm's own SELECT list wrote.
				colTypes = emittedColDecls(proj.Children[0])
			}
			// Same integer-preserving-arithmetic hint as
			// attachScanSelectProjections (#297, #445): without it, `id + 1`
			// over a strict-int column declares (and computes) FLOAT64 here.
			strictInt = strictIntArithCols(proj.Children[0])
			haveTypes = true
		}
		expr := pr.Expr
		if expr == "" {
			expr = pr.ASTExpr.String()
		}
		// A reference to a rename the walk above stepped through names a
		// column no stage emits, so it is rewritten to its SOURCE — the same
		// substitution attachScanSelectProjections applies for #387's shape
		// one level up. A rewrite the walker declines leaves the spec alone;
		// the resolvability check below is what refuses to attach it.
		ast := pr.ASTExpr
		respelled := false
		if rewritten, ok := substituteNestedRenameRefs(pr.ASTExpr, renameChild); ok && rewritten != nil {
			if rewritten != pr.ASTExpr {
				expr = rewritten.String()
				respelled = true
			}
			ast = rewritten
		}
		if windowArm {
			// The builder's nested-window rewrite leaves the SLOT reference
			// in ASTExpr and the ABBREVIATED source text in Expr —
			// `sum(a) OVER (...) + 0`, which no parser accepts (ADR-0025).
			// The AST is the spelling the fragment can compile.
			if !referencesSyntheticWindow(ast) {
				return false
			}
			expr = ast.String()
		}
		spec := ProjectExprSpec{Expr: expr, Name: strings.ToLower(pr.Alias)}
		// A computed column's declaration determines its runtime vector (#333),
		// including DECIMAL (p,s), which must not default to scale 0 (ADR-0024 item 2).
		// Infer against the schema the respelled expression NOW names: SOURCE columns,
		// with intervening stage-less Filters stripped, as in #387.
		// Window slots need windowSpecOutputType's complete declaration, including
		// (p,s): a missing type can project correctly yet feed NULL to an aggregate.
		// See docs/internals/computed-arm-declarations.md for the design.
		declTypes, declStrict := colTypes, strictInt
		switch {
		case windowArm:
			declTypes = windowArmColDecls(below)
			declStrict = strictIntArithCols(below.Children[0])
		case respelled:
			declTypes = armSourceDecls(renameChild)
			declStrict = strictIntArithColsThroughRenames(stripArmFilters(renameChild))
		}
		decl := inferProjectionDeclType(ast, parquet.TypeString, declStrict, declTypes)
		spec.Type, spec.TypeKnown = decl.ID, true
		spec.Precision, spec.Scale = decl.Precision, decl.Scale
		spec.Fields = declTypeParts(decl).Fields
		computed = append(computed, spec)
		collectASTCols(ast, needCols)
	}
	if len(computed) == 0 {
		return false
	}

	alias := make(map[string]bool, len(computed))
	for _, c := range computed {
		alias[c.Name] = true
	}

	if windowArm {
		absorbWindowArmProjection(childStages, computed, needCols, alias)
		return false
	}
	if joinArm {
		return absorbJoinArmProjection(childStages, computed, needCols, alias)
	}

	// Target: the single scan stage this subtree emitted. Scalar-producer
	// stages a Filter deferred may follow it; a second scan means the
	// subtree is not the shape this pass handles.
	var target *Stage
	for i := range childStages {
		s := &childStages[i]
		if s.Type != StageScan {
			continue
		}
		if target != nil {
			return false
		}
		target = s
	}
	if target == nil || !strings.EqualFold(target.TableName, below.TableName) {
		return false
	}
	if len(target.ProjectExprs) > 0 || len(target.FusedAggSpecs) > 0 || len(target.FusedAggGroupBy) > 0 {
		return false
	}
	// Every column the (possibly rewritten) expressions read has to be one
	// the SCAN can supply. A rename chain the substitution could not resolve
	// leaves a name the table does not have, and attaching it would compute
	// NULL where the query means a value — decline instead, which keeps the
	// behaviour this pass had before it walked through Projects at all.
	if !armExprColumnsAvailable(needCols, alias, scanReadableColumns(target)) {
		return false
	}

	// The read set carries the computed alias as a phantom column (needed-
	// column propagation lists it): strip it — the parquet projection
	// guard reverts to full width on any unknown name — and add the real
	// columns the expressions read, which downstream pruning may have
	// dropped when only the alias was referenced.
	cols := make([]string, 0, len(target.Columns)+len(needCols))
	seen := map[string]bool{}
	for _, c := range target.Columns {
		lc := strings.ToLower(c)
		if alias[lc] || seen[lc] {
			continue
		}
		seen[lc] = true
		cols = append(cols, c)
	}
	for c := range needCols {
		if !seen[c] && !alias[c] {
			seen[c] = true
			cols = append(cols, c)
		}
	}
	target.Columns = cols

	// Passthrough of the (post-strip) read set plus the computed aliases:
	// OpProject narrows the fragment's output to exactly its projections,
	// so every column a consumer resolves by source name must be listed.
	specs := make([]ProjectExprSpec, 0, len(cols)+len(computed))
	for _, c := range cols {
		specs = append(specs, ProjectExprSpec{Expr: c, Name: c})
	}
	specs = append(specs, computed...)
	target.ProjectExprs = specs
	return false
}

// scanReadableColumns is what a scan stage's fragment can put on its output:
// the columns it reads off the table. `Columns` is the read set — the alias
// this pass is about to compute rides in it as a phantom and is stripped by
// the caller — so it is the right question here, unlike a join's or an
// exchange's list.
func scanReadableColumns(target *Stage) map[string]string {
	out := make(map[string]string, len(target.Columns)+len(target.ScanSchema))
	for _, c := range target.Columns {
		out[strings.ToLower(c)] = c
	}
	for _, c := range target.ScanSchema {
		out[strings.ToLower(c.Name)] = c.Name
	}
	return out
}

// armExprColumnsAvailable reports whether every column the arm's computed
// expressions read is one the target's stream carries. The aliases this pass
// is about to MINT are excluded — they are the outputs, not the inputs.
func armExprColumnsAvailable(needCols map[string]bool, alias map[string]bool,
	available map[string]string) bool {
	for c := range needCols {
		if alias[c] {
			continue
		}
		if _, ok := available[strings.ToLower(c)]; ok {
			continue
		}
		if _, ok := available[strings.ToLower(stripQualifier(c))]; ok {
			continue
		}
		return false
	}
	return true
}

// absorbWindowArmProjection materializes a derived arm's computed SELECT list
// onto the WINDOW stage that produces its rows.
//
// The scan branch above cannot reach this shape: the value is computed OVER
// the window's output slot (`__win_0 + 0`), which the scan does not have, and
// the window stage emits the slot and nothing named by the user's alias. A
// window stage's OpProject runs ABOVE the operator (ADR-0025, #656 shape g),
// so the alias is computable exactly there, and only there.
//
// Same additive contract as the scan branch: the arm's own stream is passed
// through under its source names — every DAG resolver reads them — and only
// the computed aliases are appended. A stage that already carries a
// projection, a subtree with two window stages, or an expression naming
// something the window's stream does not carry, all decline and keep today's
// behaviour.
func absorbWindowArmProjection(childStages []Stage, computed []ProjectExprSpec,
	needCols map[string]bool, alias map[string]bool) {
	var target *Stage
	for i := range childStages {
		s := &childStages[i]
		if s.Type != StageWindow {
			continue
		}
		if target != nil {
			return // two windows: not the shape this pass models
		}
		target = s
	}
	if target == nil || len(target.ProjectExprs) > 0 {
		return
	}
	idx := make(map[string]int, len(childStages))
	for i := range childStages {
		idx[childStages[i].ID] = i
	}
	stream := emittedThroughPassThrough(childStages, idx, target)
	for _, w := range target.WindowCols {
		if w.OutputCol != "" {
			stream[strings.ToLower(w.OutputCol)] = w.OutputCol
		}
	}
	// The alias rides in the arm's needed-column propagation as a phantom;
	// it is an OUTPUT of the projection, never an input to it.
	for a := range alias {
		delete(stream, strings.ToLower(a))
	}
	if len(stream) == 0 || !armExprColumnsAvailable(needCols, alias, stream) {
		return
	}
	names := make([]string, 0, len(stream))
	for _, n := range stream {
		names = append(names, n)
	}
	sort.Strings(names)
	specs := make([]ProjectExprSpec, 0, len(names)+len(computed))
	for _, n := range names {
		specs = append(specs, ProjectExprSpec{Expr: n, Name: n})
	}
	specs = append(specs, computed...)
	target.ProjectExprs = specs
}

// absorbJoinArmProjection materializes a derived arm's computed SELECT list
// on its terminal JOIN stage (#780); a single scan cannot carry joined values.
// Passthrough uses stageStreamColumns, mirroring joinOutputSchemaWithMapping:
// duplicate build columns are qualified by their owner or dropped if unqualifiable.
// Exclude the arm's computed aliases from passthrough: a published computed
// value must win over a raw column of the same name (ADR-0025).
// See docs/internals/join-arm-projection-materialization.md for the design.
func absorbJoinArmProjection(childStages []Stage, computed []ProjectExprSpec,
	needCols map[string]bool, alias map[string]bool) bool {
	leaves := leafStages(childStages)
	if len(leaves) != 1 {
		return false // more than one stream: not the shape this pass models
	}
	target, ti := (*Stage)(nil), -1
	for i := range childStages {
		if childStages[i].ID == leaves[0] {
			target, ti = &childStages[i], i
			break
		}
	}
	if target == nil || !isJoinStage(target.Type) || len(target.ProjectExprs) > 0 {
		return false
	}
	idx := make(map[string]int, len(childStages))
	for i := range childStages {
		idx[childStages[i].ID] = i
	}
	stream := make(map[string]string, len(childStages))
	var names []string
	for _, c := range stageStreamColumns(childStages, idx, target, passThroughDepth) {
		if c.Dropped || c.Name == "" {
			continue
		}
		lc := strings.ToLower(c.Name)
		if alias[lc] {
			continue // an OUTPUT of this projection, never an input to it
		}
		if _, dup := stream[lc]; dup {
			continue
		}
		stream[lc] = c.Name
		names = append(names, c.Name)
	}
	if len(names) == 0 || !armExprColumnsAvailable(needCols, alias, stream) {
		return false
	}
	specs := make([]ProjectExprSpec, 0, len(names)+len(computed))
	for _, n := range names {
		specs = append(specs, ProjectExprSpec{Expr: n, Name: n})
	}
	specs = append(specs, computed...)
	// Nothing is attached that the join's own output cannot evaluate: an arm
	// whose expression names a spelling the joined stream does not carry
	// keeps today's behaviour rather than computing NULL under the right
	// name, which would trade a wrong value for a different wrong value.
	if !specsResolveAgainstStageOutput(childStages, ti, specs) {
		return false
	}
	target.ProjectExprs = specs
	return true
}

// stripArmFilters descends past the FILTER nodes an arm's rename chain may be
// interleaved with. walkStages emits no stage for a Filter and
// substituteNestedRenameRefs walks through one, so the declarations that type
// a respelled expression are the ones visible below them — reading them at
// the Filter answered the FLOAT fallback for `a * 2` over `a AS v`, and the
// fragment then tried to store a DECIMAL's rendering into a float vector.
func stripArmFilters(n *logical.Node) *logical.Node {
	for n != nil && n.Type == logical.NodeFilter && len(n.Children) == 1 {
		n = n.Children[0]
	}
	return n
}

// armSourceDecls is sourceColDeclsThroughRenames with those Filters stripped,
// at every level of the chain rather than only the first.
func armSourceDecls(n *logical.Node) colDecls {
	for {
		n = stripArmFilters(n)
		if n == nil || n.Type != logical.NodeProject || len(n.Children) != 1 {
			break
		}
		for _, pr := range n.Projections {
			if pr.IsAgg || pr.Column == "" {
				return colDecls{}
			}
		}
		n = n.Children[0]
	}
	return inputColDecls(n)
}

// windowArmColDecls declares the columns visible ABOVE a window node: its
// input's, plus each window OUTPUT SLOT typed the way the window stage types
// it. The slot is not a catalog column, so inputColDecls answers nothing for
// it and every expression over one fell to the float rule — which for a
// DECIMAL sum is a declaration that disagrees with the bytes.
func windowArmColDecls(win *logical.Node) colDecls {
	if win == nil || len(win.Children) != 1 {
		return colDecls{}
	}
	base := inputColDecls(win.Children[0])
	types := make(map[string]parquet.TypeID, len(base.types)+len(win.WindowExprs))
	for k, v := range base.types {
		types[k] = v
	}
	dec := make(map[string]logical.DecimalMeta, len(base.dec)+len(win.WindowExprs))
	for k, v := range base.dec {
		dec[k] = v
	}
	for _, we := range win.WindowExprs {
		if we.OutputCol == "" {
			continue
		}
		d := windowSpecOutputType(win, we)
		name := strings.ToLower(we.OutputCol)
		types[name] = d.ID
		delete(dec, name)
		if d.ID == parquet.TypeDecimal && d.DecKnown {
			dec[name] = logical.DecimalMeta{Precision: d.Precision, Scale: d.Scale}
		}
	}
	return colDecls{types: types, fields: base.fields, dec: dec}
}
