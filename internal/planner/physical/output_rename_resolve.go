package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// resolveOutputRenameSource maps an OutputRename SOURCE that names a nested
// subquery's alias back to the column the DAG's streams actually carry (#385).
//
// walkStages treats an ordinary Project as a passthrough — it emits no stage
// — so a subquery's rename never happens anywhere on the DAG: every stream
// carries SOURCE column names, and each consumer compensates by resolving
// aliases back through the plan (resolveShuffleKey for join keys,
// resolveAggInputName for aggregate inputs, resolveSortKeyColumn for ORDER BY
// terms). The GATHER is the consumer this helper compensates for: when the
// outer SELECT merely forwards a subquery's alias (`SELECT k FROM (SELECT
// r_regionkey AS k FROM region) t`), extractOutputRenames reads the outermost
// Project and produces {From: k, To: k} — but no stage ever emitted a column
// named k, so applyOutputRenames could not resolve the source, degraded to
// its rename-only fallback, and the client saw the full upstream width under
// source names.
//
// The walk starts at the child of the outermost Project (whose list the
// renames came from) and substitutes at most once per Project — a projection
// list is simultaneous, so `b AS a, a AS b` must not chase itself — while
// descending through order/cardinality-preserving wrappers. Chained renames
// across NESTED Projects (`SELECT a FROM (SELECT b AS a FROM (SELECT c AS b
// ...))`) do resolve level by level.
//
// Three stop conditions mirror the sibling resolvers:
//   - a COMPUTED alias (Projection.Column == "") stops the walk: the value
//     has no source column to resolve to, and the #383/#169 machinery
//     materializes it into the producing fragment under the alias itself;
//   - an Aggregate stops the walk: its outputs are its own GroupBy /
//     OutputCol names, and descending past it would resolve against the
//     wrong schema (#355's aggStageRenames already handles group keys the
//     aggregate itself had to resolve);
//   - a Join recurses into both output-visible children (probe side only for
//     semi/anti), first substitution wins.
func resolveOutputRenameSource(name string, child *logical.Node) string {
	resolved := name
	if child == nil || name == "" {
		return resolved
	}
	for n := child; n != nil; {
		switch {
		case n.Type == logical.NodeProject:
			for _, proj := range n.Projections {
				if proj.Alias == "" || !strings.EqualFold(proj.Alias, resolved) {
					continue
				}
				if proj.IsAgg || proj.Column == "" {
					// Aggregate output or computed alias — the stage that
					// evaluates it emits it under this very name.
					return resolved
				}
				// Plain rename: prefer the qualifier-preserving Expr
				// spelling, mirroring extractOutputRenames — the gather's
				// resolveRenameSource applies the qualified↔bare fallback
				// either way.
				next := proj.Column
				if proj.Expr != "" {
					next = strings.ToLower(proj.Expr)
				}
				if strings.EqualFold(next, resolved) {
					return resolved // self-rename, nothing to chase
				}
				resolved = next
				break
			}
		case n.Type == logical.NodeAggregate:
			return resolved
		case n.Type == logical.NodeJoin && len(n.Children) == 2:
			if r := resolveOutputRenameSource(resolved, n.Children[0]); !strings.EqualFold(r, resolved) {
				return r
			}
			jt := strings.ToLower(n.JoinType)
			if jt == "semi" || jt == "anti" {
				return resolved
			}
			return resolveOutputRenameSource(resolved, n.Children[1])
		}
		if len(n.Children) == 1 {
			n = n.Children[0]
			continue
		}
		break
	}
	return resolved
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
func resolveJoinNeededColumns(node *logical.Node) []string {
	if len(node.NeededColumns) == 0 {
		return node.NeededColumns
	}
	changed := false
	resolved := make([]string, len(node.NeededColumns))
	for i, c := range node.NeededColumns {
		resolved[i] = resolveShuffleKey(c, node)
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
