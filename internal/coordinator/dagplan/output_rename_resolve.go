// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// resolveOutputRenameSourceForGather additionally resolves a computed alias
// over an AGGREGATE to the group key's expression TEXT, which is the name the
// aggregate stage emits when nothing renamed it.
func resolveOutputRenameSourceForGather(name string, child *logical.Node) string {
	return physical.ResolveRenameSource(name, child, true)
}

// renameIsAggregateOutput reports whether the SELECT item named `name` is,
// after every rename between here and the operator that computes it, an
// AGGREGATE OUTPUT rather than a group-key reference.
//
// `OutputRename.IsAgg` is a property of the item the OUTER block
// wrote, and the gather's duplicate-name pairing needs the class of what that
// item REFERS to. One derived table is enough to separate them:
//
//	SELECT u.g, u.x FROM (SELECT COUNT(*) AS g, g AS x FROM t
//	                      GROUP BY g HAVING COUNT(*) > 0) u ORDER BY u.x
//
// `u.g` is a plain column reference — IsAgg false — while the value it names
// is the COUNT, and the aggregate publishes its KEY under that same name. The
// key branch of `classScopedMatch` then took the first column of the name,
// which is the key, and both DAG arms answered 0,0 | 1,1 | 2,2 for
// PostgreSQL's 80,0 | 80,1 | 80,2 (#785 round 2).
//
// It walks the same way physical.ResolveRenameSource does and stops where that stops,
// so the two answers are about the same projection.
func renameIsAggregateOutput(name string, child *logical.Node) bool {
	resolved := name
	for n, hops := child, 0; n != nil && hops < 64; hops++ {
		switch {
		case n.Type == logical.NodeProject:
			bare := physical.DerivedScopeBareName(resolved, n)
			proj := projectionPublishingName(n.Projections, resolved, bare)
			if proj == nil {
				return false
			}
			if proj.IsAgg {
				return true
			}
			if proj.Column == "" {
				return false // a computed item is neither
			}
			// A Project whose INPUT is the aggregate's own output is where the
			// two classes are separated, so the answer is this projection's
			// own class and the walk stops. Descending past it re-asks by NAME
			// at the aggregate — where the key and the output answer to the
			// same name, which is the very collision being resolved — and
			// `g AS x` over `COUNT(*) AS g` came back "aggregate" because the
			// aggregate publishes a `g`.
			if logical.AggregateOverGroupRows(n) != nil {
				return false
			}
			next := proj.Column
			if proj.Expr != "" {
				next = strings.ToLower(proj.Expr)
			}
			if strings.EqualFold(next, resolved) {
				return false
			}
			resolved = next
		case n.Type == logical.NodeAggregate:
			// The AGGREGATE itself answers nothing. Its output schema is
			// exactly where a key and an output can share a name, so asking
			// it by name is the collision, not its resolution — and asking it
			// re-classified the block's OWN key reference as an aggregate
			// (`SELECT COUNT(*) AS g, g AS x … GROUP BY g`: `x`'s source is
			// `g`, which the aggregate does publish as an output). The class
			// of an item in THIS block is the item's own `IsAgg`; this walk
			// exists only to carry that class across a WRAPPER, and where
			// there is no wrapper there is nothing to carry.
			return false
		case n.Type == logical.NodeJoin && len(n.Children) == 2:
			if own := physical.OwnedJoinArm(n, resolved); own != nil {
				return renameIsAggregateOutput(resolved, own)
			}
			return renameIsAggregateOutput(resolved, n.Children[0]) ||
				renameIsAggregateOutput(resolved, n.Children[1])
		}
		if len(n.Children) == 1 {
			n = n.Children[0]
			continue
		}
		return false
	}
	return false
}

// resolveRenameSourceInScope resolves the BARE part of a qualified name
// inside the subtree its qualifier names, and reports whether that subtree
// was found at all.
//
// attachScanSelectProjections' fallback for a qualified spec — the nested
// Project's alias is bare, so `q.w` has to be looked up as `w` — dropped the
// qualifier and ran the ordinary walk, which takes the first arm that
// answers. With two arms publishing `w` that is the other arm's column
// (#742). Scoping the lookup asks the same question of the right relation.
func resolveRenameSourceInScope(name string, child *logical.Node) (string, bool) {
	dot := strings.LastIndexByte(name, '.')
	if dot <= 0 || dot == len(name)-1 || child == nil {
		return "", false
	}
	qual, bare := name[:dot], name[dot+1:]
	scope := physical.RelationScopeSubtree(child, qual)
	if scope == nil {
		return "", false
	}
	src := physical.ResolveOutputRenameSource(bare, scope)
	if strings.EqualFold(src, bare) {
		return "", true // the scope exists and renames nothing: not a miss
	}
	return src, true
}

// windowArgSourceInScope resolves a QUALIFIED window argument to the source
// column the DAG's streams carry, inside the arm its qualifier names.
//
// `physical.DerivedAliasSourceColumn` stops at a Join — it has no way to choose an arm
// — so asked of a join it answers nothing and the argument reached the worker
// under the derived ALIAS, which on the DAG no stream carries. Scoping it
// first is the same composition `resolveRenameSourceInScope` performs for a
// projection. ok=false means the qualifier names no relation here and the
// caller keeps the unscoped walk.
func windowArgSourceInScope(name string, child *logical.Node) (string, bool) {
	dot := strings.LastIndexByte(name, '.')
	if dot <= 0 || dot == len(name)-1 || child == nil {
		return "", false
	}
	qual, bare := name[:dot], name[dot+1:]
	scope := physical.RelationScopeSubtree(child, qual)
	if scope == nil {
		return "", false
	}
	return physical.DerivedAliasSourceColumn(bare, scope), true
}

// resolveJoinNeededColumns maps each entry of a join node's NeededColumns
// that names a subquery's rename back to its source column (#385). The join
// stage's Columns become the worker's OutputColumns filter
// (probe.OutputFilter), and the streams the join reads carry SOURCE names —
// an alias entry matches nothing, so the column the user asked for was
// silently dropped from the join output and the gather had nothing to rename
// (`SELECT n_name, k FROM nation JOIN (SELECT r_regionkey AS k FROM region) t
// ON n_regionkey = k` came back as [n_name n_regionkey]).
//
// Resolution reuses resolveShuffleKey — the join-key resolver for exactly
// this passthrough — which rewrites only plain renames (computed aliases are
// materialized under their own name by the #383 pass and stay). The original
// slice is returned untouched when nothing resolves, keeping unaffected
// plans byte-identical; when something does, duplicates introduced by the
// mapping (alias and its source both needed) collapse.
func resolveJoinNeededColumns(node *logical.Node, published map[*logical.Node]bool) []string {
	if len(node.NeededColumns) == 0 {
		return node.NeededColumns
	}
	changed := false
	resolved := make([]string, len(node.NeededColumns))
	for i, c := range node.NeededColumns {
		resolved[i] = resolveShuffleKey(c, node, published)
		if resolved[i] != c {
			changed = true
		}
	}
	if !changed {
		return node.NeededColumns
	}
	out := make([]string, 0, len(resolved))
	seen := make(map[string]bool, len(resolved))
	for _, c := range resolved {
		lc := strings.ToLower(c)
		if seen[lc] {
			continue
		}
		seen[lc] = true
		out = append(out, c)
	}
	return out
}
