package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The synthetic ORDER BY key on the DAG, and the stage that has to emit it.
//
// `ORDER BY b` over `SELECT a` names a column the SELECT-list Project drops,
// so logical.resolveOrderBy MATERIALIZES the term as a hidden projection
// called __sortkey_N and points the Sort at it (see
// planner/logical/order_by_keys.go). On the single-process pipeline that
// Project is a real operator: it runs below the Sort, computes the column,
// and hiddenSortTrimOp drops it again before the rows reach the client.
//
// On the DAG an ordinary Project emits NO STAGE. The name therefore exists
// nowhere unless some pass writes it onto the fragment that produces the
// sort's input, and exactly one pass did: attachScanSelectProjections, which
// runs only for the OUTERMOST SELECT list — the one feeding the terminal
// gather. A sort anywhere else — an ORDER BY inside a derived table or a CTE,
// whose consumer is an aggregate or a join rather than the gather — got no
// such projection, and the task failed with
//
//	sort: key column "__sortkey_0" does not exist in the input schema
//
// while the single-process path answered the same query (#424). That is the
// loud half of the family #313/#316/#320 closed the silent half of: the same
// missing name, caught by the sort operator instead of matching nothing.
//
// resolveHiddenSortKeys settles every such key, and runs LAST — after
// attachScanSelectProjections — because the repair depends on what that pass
// did. Where it fired, the producing fragment already emits __sortkey_N and
// there is nothing to do; where it declined, this pass makes the producer
// carry the term:
//
//   - a plain column reference (`ORDER BY s_acctbal`) needs no computation.
//     The producer already ships that column under its own name — the DAG's
//     convention, which every other resolver compensates for — so the KEY is
//     renamed to it and no projection is added. Nothing downstream reads
//     __sortkey_N: the gather projects to the visible SELECT list, and a
//     consumer stage reads the producer's columns by their source names.
//
//   - a computed term (`ORDER BY LENGTH(s_name)`) has no source column, so it
//     is projected INTO the producing fragment under the hidden name — the
//     same materialize-at-source shape as absorbComputedSubqueryProjection
//     (#383) and the same Stage.ProjectExprs → OpProject machinery (#169).
//
// A shape it does not recognize is left exactly as it was, which keeps
// today's loud failure rather than inventing an order.

// resolveHiddenSortKeys points every synthetic ORDER BY key at a column its
// producing stage really emits.
func resolveHiddenSortKeys(stages []Stage) {
	idx := make(map[string]int, len(stages))
	for i := range stages {
		idx[stages[i].ID] = i
	}
	for i := range stages {
		if !hasHiddenSortKey(stages[i].SortKeys) {
			continue
		}
		producer := sortKeyProducer(stages, idx, i)
		if producer == nil {
			continue
		}
		emitted := stageEmittedColumns(producer)
		for k := range stages[i].SortKeys {
			key := &stages[i].SortKeys[k]
			if !logical.IsHiddenSortColumn(key.Column) || key.SourceExpr == "" {
				continue
			}
			if _, ok := emitted[strings.ToLower(key.Column)]; ok {
				continue // already materialized (attachScanSelectProjections)
			}
			if key.SourceColumn != "" {
				if name, ok := lookupEmittedColumn(emitted, key.SourceColumn); ok {
					key.Column = name
					continue
				}
			}
			if materializeSortKey(producer, *key) {
				emitted = stageEmittedColumns(producer)
			}
		}
	}
}

func hasHiddenSortKey(keys []SortKeySpec) bool {
	for _, k := range keys {
		if logical.IsHiddenSortColumn(k.Column) && k.SourceExpr != "" {
			return true
		}
	}
	return false
}

// sortKeyProducer returns the stage whose output the stage at i sorts, or nil
// when this pass does not recognize the shape.
//
// A standalone sort stage sorts its single dependency's output. Every other
// stage carrying SortKeys got them from fuseSortIntoPredecessor, which folds
// the ordering onto the stage that PRODUCES the rows — there the key has to
// exist in the stage's own output, so the stage is its own producer.
//
// Only the stage types whose fragments append an OpProject for
// Stage.ProjectExprs qualify (scan and the three join types): a producer this
// pass cannot make emit a column is one it must leave alone.
func sortKeyProducer(stages []Stage, idx map[string]int, i int) *Stage {
	s := &stages[i]
	if s.Type != StageSort && s.Type != "merge_sort" {
		if projectableProducer(s.Type) {
			return s
		}
		return nil
	}
	if len(s.Dependencies) != 1 {
		return nil
	}
	depIdx, ok := idx[s.Dependencies[0]]
	if !ok {
		return nil
	}
	dep := &stages[depIdx]
	if !projectableProducer(dep.Type) {
		return nil
	}
	return dep
}

func projectableProducer(typ string) bool {
	switch typ {
	case StageScan, StageHashJoin, StageBroadcastJoin, StageSortMergeJoin:
		return true
	}
	return false
}

// stageEmittedColumns lists the columns a stage's fragment ships, keyed by
// lowercased name and valued by the spelling the stream carries.
//
// A projection narrows the output to exactly its own outputs, so it wins over
// the column lists when present; pruneScanOutputColumns' OutputColumns is the
// next authority, and the read set is the fallback. The row-count sentinel is
// not a column — buildReadSchema drops it whenever a real column is present —
// so it never counts as one here.
func stageEmittedColumns(s *Stage) map[string]string {
	out := map[string]string{}
	add := func(names []string) {
		for _, n := range names {
			if n == "" || strings.EqualFold(n, logical.RowCountOnlyColumn) {
				continue
			}
			out[strings.ToLower(n)] = n
		}
	}
	switch {
	case len(s.ProjectExprs) > 0:
		for _, p := range s.ProjectExprs {
			add([]string{p.Name})
		}
	case len(s.OutputColumns) > 0:
		add(s.OutputColumns)
	default:
		add(s.Columns)
	}
	return out
}

// lookupEmittedColumn finds col among the producer's outputs, tolerating one
// side carrying a table qualifier the other omits — the same qualified↔bare
// fallback the engine's runtime column lookup applies (columnIndexFallback).
func lookupEmittedColumn(emitted map[string]string, col string) (string, bool) {
	if name, ok := emitted[strings.ToLower(col)]; ok {
		return name, true
	}
	bare := strings.ToLower(stripQualifier(col))
	if bare == "" {
		return "", false
	}
	if name, ok := emitted[bare]; ok {
		return name, true
	}
	// The producer may carry the QUALIFIED spelling (a join qualifies its
	// build side's colliding columns) where the ORDER BY named it bare.
	var match string
	for lower, name := range emitted {
		if strings.ToLower(stripQualifier(lower)) != bare {
			continue
		}
		if match != "" {
			// Two columns share the bare name — a self-join. Picking one
			// would sort by an arbitrary side; leave the key alone and let
			// the sort report the missing column.
			return "", false
		}
		match = name
	}
	return match, match != ""
}

// materializeSortKey projects a computed ORDER BY term into the producing
// fragment under its hidden name, and reports whether it did.
//
// OpProject narrows the fragment's output to exactly its projections, so the
// producer's existing output set is carried through as passthrough entries
// first — every column a consumer resolves by source name has to survive.
// A producer that already carries a projection is left alone: those specs were
// written by a pass that knows the query's output shape, and appending to them
// would widen a result the gather does not expect.
func materializeSortKey(producer *Stage, key SortKeySpec) bool {
	if len(producer.ProjectExprs) > 0 {
		return false
	}
	emitted := stageEmittedColumns(producer)
	if len(emitted) == 0 {
		return false
	}
	// Preserve the producer's own column order; map iteration is random and
	// the projected order becomes the stream's schema order.
	source := producer.Columns
	if len(producer.OutputColumns) > 0 {
		source = producer.OutputColumns
	}
	specs := make([]ProjectExprSpec, 0, len(source)+1)
	seen := make(map[string]bool, len(source))
	for _, c := range source {
		lower := strings.ToLower(c)
		if seen[lower] {
			continue
		}
		if _, ok := emitted[lower]; !ok {
			continue
		}
		seen[lower] = true
		specs = append(specs, ProjectExprSpec{Expr: c, Name: c})
	}
	if len(specs) == 0 {
		return false
	}
	producer.ProjectExprs = append(specs, ProjectExprSpec{
		Expr: key.SourceExpr,
		Name: strings.ToLower(key.Column),
		// The materialized column exists in no catalog, so its declared
		// type IS its runtime type — the worker builds the output vector
		// from it (#333). TypeKnown must ride along: Type's zero value is
		// TypeBool, so a genuinely BOOL sort key is indistinguishable from
		// "not set" without it, and projectOpFromSpecs drops it off the
		// wire (#445, #472).
		Type:      key.SourceType,
		TypeKnown: key.SourceTypeKnown,
	})
	return true
}

// annotateDerivedAliasSortKey records the column the DAG's streams carry for
// a sort key that names a DERIVED TABLE's SELECT-list alias. child is the
// node the Sort reads. See SortKeySpec.AliasSource for the defect.
//
// The walk is resolveSortKeyColumn's, with the opposite terminal: that
// resolver commits only at an AGGREGATE, whose outputs it can name exactly,
// and deliberately leaves a scan/join producer alone because
// attachScanSelectProjections may still put the alias onto its fragment.
// This annotation is how that decision gets DEFERRED instead of dropped —
// the name is recorded here and settled in resolveDerivedAliasSortKeys, which
// runs after that pass and can see what the fragment really emits.
//
// Only a PLAIN rename is recorded. A computed alias has no source column to
// point at; the #383/#169 machinery materializes it into the producing
// fragment under the alias itself, and pointing the key at the expression
// text would miss that projection. Chained renames resolve level by level
// (`j` → `k` → `s_nationkey`), each Project substituting at most once because
// a projection list is simultaneous.
func annotateDerivedAliasSortKey(key *SortKeySpec, child *logical.Node) {
	if key.Column == "" || logical.IsHiddenSortColumn(key.Column) {
		return // the synthetic-key mechanism above owns those
	}
	resolved := key.Column
	for n := child; n != nil; {
		switch n.Type {
		case logical.NodeProject:
			bare := derivedScopeBareName(resolved, n)
			proj := projectionForName(n.Projections, resolved, bare)
			if proj == nil {
				break
			}
			if proj.IsAgg || proj.Column == "" {
				return // aggregate output or computed alias — not ours
			}
			next := proj.Column
			if proj.Expr != "" {
				// The qualifier-preserving spelling, for the same reason
				// resolveSortKeyColumn prefers it: a self-joined table gives
				// both aliases the same bare column name, and only "n1.n_name"
				// says which. lookupEmittedColumn applies the qualified↔bare
				// fallback when the producer spells it the other way.
				next = proj.Expr
			}
			if strings.EqualFold(next, resolved) {
				return // self-rename, nothing to point at
			}
			resolved = next
		case logical.NodeFilter, logical.NodeLimit, logical.NodeSort, logical.NodeDistinct:
			// Order/cardinality-preserving passthroughs: keep descending.
		default:
			// A producer this walk cannot reason about (Aggregate, Join,
			// Scan, Window, a set operation): stop, and keep whatever the
			// Projects above it resolved.
			n = nil
			continue
		}
		if len(n.Children) != 1 {
			break
		}
		n = n.Children[0]
	}
	if !strings.EqualFold(resolved, key.Column) {
		key.AliasSource = resolved
	}
}

// resolveDerivedAliasSortKeys points every sort key that names a derived
// table's SELECT-list alias at the column its producing stage really emits.
//
// It runs after attachScanSelectProjections, because that pass is the one
// thing that can make the alias real: where it attached an alias-naming
// OpProject the fragment emits the alias and the key is already right, and
// where it declined the fragment emits the SOURCE column and the key has to
// be pointed there. The distinction cannot be drawn from the emitted column
// SET alone — a shadowing alias (`s_acctbal AS s_suppkey`) means the producer
// emits a column spelled like the key that is the WRONG one — so the test is
// whether the projection MATERIALIZES the name, not whether the name exists.
//
// A stage the producer walk does not recognize (a merge_sort over a sort, the
// gather's fused ordering) inherits the decision made for the key it was
// copied from: emitMergeSortTree hands out the same SortKeySpec values, so
// matching on (column, alias source) re-applies the rewrite exactly where the
// same key travelled.
func resolveDerivedAliasSortKeys(stages []Stage) {
	idx := make(map[string]int, len(stages))
	for i := range stages {
		idx[stages[i].ID] = i
	}
	rewrites := map[string]string{}
	rewriteKey := func(k SortKeySpec) string {
		return strings.ToLower(k.Column) + "\x00" + strings.ToLower(k.AliasSource)
	}
	for i := range stages {
		if !hasAliasSortKey(stages[i].SortKeys) {
			continue
		}
		producer := sortKeyProducer(stages, idx, i)
		if producer == nil {
			continue
		}
		emitted := stageEmittedColumns(producer)
		for k := range stages[i].SortKeys {
			key := &stages[i].SortKeys[k]
			if key.AliasSource == "" || projectionMaterializes(producer, key.Column) {
				continue
			}
			name, ok := lookupEmittedColumn(emitted, key.AliasSource)
			if !ok {
				continue // leave today's behavior rather than invent an order
			}
			rewrites[rewriteKey(*key)] = name
			key.Column = name
		}
	}
	if len(rewrites) == 0 {
		return
	}
	for i := range stages {
		for k := range stages[i].SortKeys {
			key := &stages[i].SortKeys[k]
			if key.AliasSource == "" {
				continue
			}
			if name, ok := rewrites[rewriteKey(*key)]; ok {
				key.Column = name
			}
		}
	}
}

func hasAliasSortKey(keys []SortKeySpec) bool {
	for _, k := range keys {
		if k.AliasSource != "" {
			return true
		}
	}
	return false
}

// projectionMaterializes reports whether the producer's fragment computes a
// column under this exact name — the only way a derived table's alias comes
// to exist on the DAG (attachScanSelectProjections' aliased branch, #316).
// The stage's plain column list does NOT count: a name that appears there
// belongs to the underlying relation, which for a shadowing alias is exactly
// the wrong column.
func projectionMaterializes(s *Stage, name string) bool {
	for _, p := range s.ProjectExprs {
		if strings.EqualFold(p.Name, name) {
			return true
		}
	}
	return false
}

// annotateHiddenSortSource records what materializes key, when key is a
// synthetic ORDER BY term. child is the node the Sort reads.
func annotateHiddenSortSource(key *SortKeySpec, child *logical.Node) {
	if !logical.IsHiddenSortColumn(key.Column) {
		return
	}
	proj, owner := findHiddenProjection(child, key.Column)
	if proj == nil {
		return
	}
	key.SourceExpr = proj.Expr
	if key.SourceExpr == "" && proj.ASTExpr != nil {
		key.SourceExpr = proj.ASTExpr.String()
	}
	if proj.ASTExpr != nil && isSimpleColRefForRename(proj.ASTExpr) {
		key.SourceColumn = proj.Column
		if key.SourceColumn == "" {
			key.SourceColumn = key.SourceExpr
		}
		return
	}
	if proj.ASTExpr != nil && len(owner.Children) == 1 {
		// Same integer-preserving-arithmetic hint the materializing
		// projection passes for every other computed-column site (#297,
		// #445): without it, `ORDER BY s_suppkey + 1` inside a derived
		// table declares FLOAT64 here where the same term at the query's
		// root gets INT64 through attachScanSelectProjections (#472).
		strictInt := strictIntArithCols(owner.Children[0])
		key.SourceType = inferProjectionTypeCols(proj.ASTExpr, parquet.TypeString,
			strictInt, inputColTypes(owner.Children[0]))
		key.SourceTypeKnown = true
	}
}

// findHiddenProjection locates the materialized ORDER BY projection named
// name, descending from the Sort's child through the nodes that pass a
// projection's output along unchanged.
func findHiddenProjection(child *logical.Node, name string) (*logical.Projection, *logical.Node) {
	for n := child; n != nil; {
		if n.Type == logical.NodeProject {
			for i := range n.Projections {
				p := &n.Projections[i]
				if p.Hidden && strings.EqualFold(p.Alias, name) {
					return p, n
				}
			}
		}
		switch n.Type {
		case logical.NodeProject, logical.NodeFilter, logical.NodeLimit,
			logical.NodeSort, logical.NodeDistinct:
			if len(n.Children) != 1 {
				return nil, nil
			}
			n = n.Children[0]
		default:
			return nil, nil
		}
	}
	return nil, nil
}
