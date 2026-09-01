package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// subtreeNaming captures structure-independent column-provenance facts about
// a logical subtree: which scan aliases it contains, which columns each alias
// provides, which projection/aggregate output names it exposes, and —
// mirroring the join executor's output-naming rule — which scan alias owns
// each bare column name in the subtree's OUTPUT schema.
//
// It replaces the flat scan-column union (collectPlanColumns) that join-key
// side assignment used to rely on. The union could not distinguish
// alias-qualified self-join keys (n1.x vs n2.x both collapsed to x) and
// broke down entirely when a join's build side spans multiple tables — the
// failure that blocked bushy join plans (see docs/design/bushy-join-cbo.md).
type subtreeNaming struct {
	// aliasCols maps a scan alias (lowercased; TableAlias falling back to
	// TableName) to that scan's column set (lowercased). Only scans visible
	// in the subtree's output are included: a semi/anti join's build side
	// contributes nothing.
	aliasCols map[string]map[string]bool
	// outputNames holds projection aliases and aggregate output columns
	// (lowercased) exposed by the subtree (e.g. supplier_no from
	// `l_suppkey AS supplier_no`, total_revenue from a CTE aggregate).
	// These are owned names without scan provenance.
	outputNames map[string]bool
	// origins maps each bare column name (lowercased) to the alias
	// (verbatim case) whose copy of the column stays UNQUALIFIED in the
	// subtree's output schema. Mirrors joinOutputSchemaWithMapping:
	// probe-side columns keep their names and duplicate build-side columns
	// get qualified, so the probe-most scan owns the bare name.
	origins map[string]string
	// root is the subtree itself, kept so ownsKey can ask the derived-scope
	// question the three maps cannot answer: whether a qualifier names a
	// DERIVED TABLE this subtree is, rather than a scan it contains.
	root *logical.Node
}

// subtreeNamingOf computes the naming facts for a logical subtree.
func subtreeNamingOf(n *logical.Node) *subtreeNaming {
	s := &subtreeNaming{
		aliasCols:   make(map[string]map[string]bool),
		outputNames: make(map[string]bool),
		origins:     make(map[string]string),
		root:        n,
	}
	s.collect(n)
	return s
}

func (s *subtreeNaming) collect(n *logical.Node) {
	if n == nil {
		return
	}
	switch n.Type {
	case logical.NodeScan:
		alias := n.TableAlias
		if alias == "" {
			alias = n.TableName
		}
		key := strings.ToLower(alias)
		cols := s.aliasCols[key]
		if cols == nil {
			cols = make(map[string]bool, len(n.ScanColumns))
			s.aliasCols[key] = cols
		}
		for _, col := range n.ScanColumns {
			lc := strings.ToLower(col)
			cols[lc] = true
			// First writer wins: children are visited probe-before-build,
			// so the probe-most scan owns the bare output name.
			if _, ok := s.origins[lc]; !ok {
				s.origins[lc] = alias
			}
		}
		return
	case logical.NodeJoin:
		s.collect(n.Children[0])
		// Semi/anti joins output only probe-side columns; the build side is
		// invisible above the join.
		jt := strings.ToLower(n.JoinType)
		if jt == "semi" || jt == "anti" {
			return
		}
		for _, child := range n.Children[1:] {
			s.collect(child)
		}
		return
	case logical.NodeProject:
		for _, proj := range n.Projections {
			if proj.Alias != "" {
				s.outputNames[strings.ToLower(proj.Alias)] = true
			}
		}
	case logical.NodeAggregate:
		for _, agg := range n.AggExprs {
			if agg.OutputCol != "" {
				s.outputNames[strings.ToLower(agg.OutputCol)] = true
			}
		}
	}
	for _, child := range n.Children {
		s.collect(child)
	}
}

// ownsKey reports whether a join-key reference resolves to a column this
// subtree exposes. Alias-qualified keys ("n2.n_nationkey") are owned only
// when the qualifier is one of the subtree's scan aliases and the column
// belongs to that scan — a self-join's other copy does NOT own them — or when
// the qualifier names a DERIVED TABLE this subtree is and the column is one of
// its outputs. Bare keys are owned when any scan in the subtree provides the
// column or the subtree exposes it as a projection/aggregate output name.
// Expression keys (anything that isn't a plain column reference) resolve to
// not-owned, matching the previous membership test's behavior.
//
// The derived-table half is what `y.b` over `(SELECT n_nationkey AS b FROM
// nation) y` needs: `b` is the derived table's OUTPUT name and appears in no
// scan's column set, so ownership came back false for BOTH sides of a join,
// assignJoinKeySides left the pair in its positional order, and each key was
// then resolved against the arm that does not own it. In a two-way join the
// mistake is invisible (the arms' keys are symmetric); in a three-way one it
// reached the worker verbatim and the shuffle failed loud with `partitioned
// shuffle: key "y.b" not in schema` (#490).
func (s *subtreeNaming) ownsKey(key string) bool {
	k := strings.ToLower(strings.TrimSpace(key))
	if dot := strings.IndexByte(k, '.'); dot >= 0 {
		if cols, ok := s.aliasCols[k[:dot]]; ok && cols[k[dot+1:]] {
			return true
		}
		// The qualifier may name a DERIVED TABLE rather than a scan, and then
		// the column after it is that table's OUTPUT name — resolved here the
		// way a bare key is, inside the scope that owns the qualifier and
		// nowhere else (derivedScopeBareName).
		if bare := derivedScopeBareName(k, s.root); bare != "" {
			return s.ownsBareName(bare)
		}
		return false
	}
	return s.ownsBareName(k)
}

// ownsBareName reports whether an UNQUALIFIED column name resolves inside this
// subtree: a projection or aggregate output it exposes, or a column any of its
// scans provides.
func (s *subtreeNaming) ownsBareName(k string) bool {
	if s.outputNames[k] {
		return true
	}
	for _, cols := range s.aliasCols {
		if cols[k] {
			return true
		}
	}
	return false
}

// buildColOrigins returns the bare-column → origin-alias map the join
// executor needs to qualify duplicate build columns with the OWNING scan's
// alias when the build side spans multiple tables. Single-alias subtrees
// return nil: the join's single BuildTableAlias is already exact, and nil
// keeps every current left-deep plan byte-identical.
func (s *subtreeNaming) buildColOrigins() map[string]string {
	if len(s.aliasCols) <= 1 {
		return nil
	}
	return s.origins
}

// materializedBuildColOrigins is buildColOrigins for a build stream that is the
// arm's OWN OUTPUT rather than its raw inner columns — the single-process
// pipeline, where the arm's Project is a real operator.
//
// The origins are the aliases of the scans INSIDE the subtree, and once the
// arm's Project has run those columns are not in the stream at all: what is
// there is what the arm publishes, under the ONE name the enclosing query
// writes. Qualifying them by an inner scan named them something no reference
// asks for and something that is not even true — `(SELECT p.id, j.d92 AS d92
// FROM zzp p JOIN zzj j ON …) m` published its `d92` as `p.d92` while the value
// was j's, so the declared schema (`m.d92`, numeric(18,4)) and the value the
// projection found (p's, numeric(9,2)) described one output two ways: 22003 on
// a query PostgreSQL answers, and a silent wrong number in the mirror.
//
// The DAG keeps the raw answer, because there the Project emits no stage and
// the inner names are exactly what the stream carries (joinArmAlias).
func (s *subtreeNaming) materializedBuildColOrigins() map[string]string {
	if namedArmScope(s.root) != "" {
		return nil
	}
	return s.buildColOrigins()
}

// assignJoinKeySides ensures leftKeys reference the probe (left) child and
// rightKeys the build (right) child, deciding by column OWNERSHIP in each
// child subtree rather than textual position in the join condition.
//
// Decision rule, per key pair: a key exclusively owned by the build side
// must sit in rightKeys. When ownership is symmetric or unresolvable
// (shared column names, expression keys), the pair keeps its positional
// order — exactly the previous fixJoinKeyOrder outcome — and the runtime
// safety net (FixKeyAssignment) remains the last resort.
func assignJoinKeySides(leftKeys, rightKeys []string, probe, build *subtreeNaming) {
	for i := range leftKeys {
		lProbe, lBuild := probe.ownsKey(leftKeys[i]), build.ownsKey(leftKeys[i])
		rProbe, rBuild := probe.ownsKey(rightKeys[i]), build.ownsKey(rightKeys[i])
		lExclBuild := lBuild && !lProbe
		rExclBuild := rBuild && !rProbe
		switch {
		case lExclBuild && !rExclBuild:
			leftKeys[i], rightKeys[i] = rightKeys[i], leftKeys[i]
		case rExclBuild && !lExclBuild:
			// Correctly assigned.
		case lBuild && !rBuild:
			// Partial information (e.g. right side is an expression):
			// mirror the previous membership rule.
			leftKeys[i], rightKeys[i] = rightKeys[i], leftKeys[i]
		}
	}
}
