// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// ErrLateralIdentityDistributed hands a plan with a correlated LATERAL the
// stage DAG cannot carry with its column identity intact back to the
// coordinator, which runs it on the single-process pipeline
// (Coordinator.runLateralIdentityLocal).
//
// THE PROPERTY (ADR-0021 §1s, arc JP round 3). The stage DAG does not run a
// decorrelated LATERAL body's Project as an operator: the join stage reads the
// body's SCAN stream, and every reference above it — the lifted key's outer
// side, `s.id`, `s.*`, a residual conjunct — is re-spelled onto that stream.
// Where the re-spell loses a qualifier it binds by BARE name, so a name the
// lateral arm carries is resolved correctly only when no other relation of the
// query carries it too. A slot the planner MINTED (`__key_0`, a pad marker) is
// unique by construction. So the DAG carries a correlated LATERAL only when
// (1) no non-minted name its arm carries ACROSS the join (crossingNames: what
// its SELECT list, aggregate or bare scan publishes and the columns those
// items are computed from) is carried by another relation of the query (the
// subtrees hanging off the path from the root to the arm), and (2) its join
// does not null-extend a grouped arm (or one publishing a window's output).
// Any other correlated LATERAL runs single-process — the distributed
// disposition until such laterals are evaluated as stages (#1323, the
// planner-phase follow-up); a routed answer is exactly the single-process
// pipeline's. The property is asked of every LATERAL arm, correlated or
// not (an uncorrelated body's colliding aggregate alias read the outer column
// on the DAG too — arc L1's UNCORRLAT/collidingName). The guard is asked just
// before stage generation, which rewrites names; the routed pipeline runs the
// same logical node.
//
// MEASURED (arc JP round 2 closure review, 1143 statements × five arms): every
// DAG wrong value in the lane was a LATERAL whose arm shares a name with the
// outer relation — the body's relation named by its TABLE or CTE name
// (28 rows for 9), `SELECT DISTINCT *` (s.id read o.id), `SELECT *` with a
// bare key, an unqualified body, a derived table or CTE inside the body, an
// aggregate aliased to an outer column's name, a third relation joined on
// `s.k`. Two rounds each taught the re-spell one more spelling and the next
// review found another; the property is asked of the plan instead.
var ErrLateralIdentityDistributed = errors.New(
	"a LATERAL whose arm shares a column name with another relation is not evaluable on the distributed path")

// refuseCollidingLateral refuses a logical plan in which some decorrelated
// LATERAL arm (a join child marked LateralSubtree) carries a non-minted name
// that another relation of the query also carries. The other relations are
// the subtrees hanging off the path from the root down to the arm: the nodes
// ON that path (the joins, filters and projections above the arm) republish
// the arm's own names and are not another relation.
func refuseCollidingLateral(root *logical.Node) error {
	var path []*logical.Node
	var walk func(n *logical.Node) error
	walk = func(n *logical.Node) error {
		if n == nil {
			return nil
		}
		path = append(path, n)
		defer func() { path = path[:len(path)-1] }()
		if n.LateralSubtree && len(path) > 1 && path[len(path)-2].Type == logical.NodeJoin {
			if err := lateralArmShares(n, path); err != nil {
				return err
			}
		}
		for _, c := range n.Children {
			if err := walk(c); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(root)
}

// lateralArmShares refuses when a name the arm at the end of path carries
// ACROSS the join (crossingNames) is carried by a subtree hanging off path.
func lateralArmShares(arm *logical.Node, path []*logical.Node) error {
	inside := map[string]bool{}
	crossingNames(arm, inside)
	outside := map[string]bool{}
	for i := 0; i+1 < len(path); i++ {
		for _, c := range path[i].Children {
			if c != path[i+1] {
				carriedNames(c, outside)
			}
		}
	}
	var shared []string
	for name := range inside {
		if outside[name] {
			shared = append(shared, name)
		}
	}
	if len(shared) > 0 {
		sort.Strings(shared)
		return fmt.Errorf("%w (shared: %s)", ErrLateralIdentityDistributed, strings.Join(shared, ", "))
	}
	if join := path[len(path)-2]; nullExtends(join) && groups(arm) {
		return fmt.Errorf("%w (a %s join pads a grouped arm)", ErrLateralIdentityDistributed, join.JoinType)
	}
	if join := path[len(path)-2]; nullExtends(join) && publishesWindow(arm) {
		return fmt.Errorf("%w (a %s join pads an arm publishing a window)", ErrLateralIdentityDistributed, join.JoinType)
	}
	return nil
}

// publishesWindow reports whether the arm's own list publishes a window's
// output. The pad file of a null-extending join over such an arm is written
// one column narrower than the arm's stream (`declares 4 columns where an
// earlier file of the same stage input declared 5`, the grouped arm's
// FC-JP-4 family) — measured once a correlated LATERAL body's window became
// evaluable per outer row (arc JP round 5, B2). A window read only by the
// body's own QUALIFY (arc LT's per-key bound) is not published and runs as
// stages as before.
func publishesWindow(arm *logical.Node) bool {
	outs := map[string]bool{}
	var collect func(n *logical.Node)
	collect = func(n *logical.Node) {
		if n == nil {
			return
		}
		if n.Type == logical.NodeWindow {
			for _, w := range n.WindowExprs {
				outs[strings.ToLower(w.OutputCol)] = true
			}
		}
		for _, c := range n.Children {
			collect(c)
		}
	}
	collect(arm)
	if len(outs) == 0 {
		return false
	}
	for n := arm; n != nil && n.Type == logical.NodeProject; {
		for _, p := range n.Projections {
			if outs[strings.ToLower(p.Column)] {
				return true
			}
			node := p.ASTExpr
			if node == nil && p.Expr != "" {
				node, _ = plansql.ParseExpression(p.Expr)
			}
			if node == nil {
				continue
			}
			if refs, err := plansql.ColumnRefs(node); err == nil {
				for _, r := range refs {
					if outs[strings.ToLower(r.Column)] {
						return true
					}
				}
			}
		}
		if len(n.Children) == 0 {
			break
		}
		n = n.Children[0]
	}
	return false
}

// nullExtends reports whether a join pads an unmatched row with NULLs.
func nullExtends(join *logical.Node) bool {
	switch strings.ToLower(strings.TrimSpace(join.JoinType)) {
	case "", "join", "inner", "inner join", "cross", "cross join", "semi", "anti":
		return false
	}
	return true
}

// groups reports whether a subtree holds an Aggregate or a Distinct.
func groups(n *logical.Node) bool {
	if n == nil {
		return false
	}
	if n.Type == logical.NodeAggregate || n.Type == logical.NodeDistinct {
		return true
	}
	for _, c := range n.Children {
		if groups(c) {
			return true
		}
	}
	return false
}

// carriedNames adds to out every bare, lower-cased, non-minted name the
// subtree at n carries — scan columns and the names Projects, Aggregates and
// Windows publish.
func carriedNames(n *logical.Node, out map[string]bool) {
	if n == nil {
		return
	}
	add := func(name string) {
		if i := strings.LastIndexByte(name, '.'); i >= 0 {
			name = name[i+1:]
		}
		name = strings.Trim(strings.TrimSpace(name), `"`)
		if name == "" || plansql.ReservedSlotFamily(name) != "" {
			return
		}
		out[foldKey(name)] = true
	}
	for _, c := range n.ScanColumns {
		add(c)
	}
	for _, p := range n.Projections {
		switch {
		case p.Alias != "":
			add(p.Alias)
		case p.Column != "":
			add(p.Column)
		}
	}
	for _, a := range n.DeferredColumnAliases {
		add(a)
	}
	for _, a := range n.AggExprs {
		add(a.OutputCol)
	}
	for _, g := range n.GroupBy {
		add(g)
	}
	for _, g := range n.GroupByPublish {
		add(g)
	}
	for _, w := range n.WindowExprs {
		add(w.OutputCol)
	}
	for _, c := range n.Children {
		carriedNames(c, out)
	}
}

// crossingNames adds to out the names an arm carries across its join: what
// its SELECT list publishes and every column those items (the minted key
// slot's source, a lifted predicate's column, an aggregate's input) are
// computed from — the names a reference above the join is re-spelled to on
// the stage DAG. A column the body only filters on stays inside the body's
// own stage and cannot be re-spelled onto, so it is not one. The walk goes
// down the arm's root chain to the first Project, Aggregate or Scan (a Scan
// reached with no Project above it publishes its every column).
func crossingNames(arm *logical.Node, out map[string]bool) {
	add := func(name string) {
		if i := strings.LastIndexByte(name, '.'); i >= 0 {
			name = name[i+1:]
		}
		name = strings.Trim(strings.TrimSpace(name), `"`)
		if name == "" || name == "*" || plansql.ReservedSlotFamily(name) != "" {
			return
		}
		out[foldKey(name)] = true
	}
	reads := func(n plansql.Node, text string) {
		if n == nil && text != "" {
			n, _ = plansql.ParseExpression(text)
		}
		if n == nil {
			return
		}
		refs, err := plansql.ColumnRefs(n)
		if err != nil {
			return
		}
		for _, r := range refs {
			add(r.Column)
		}
	}
	for n := arm; n != nil; {
		switch n.Type {
		case logical.NodeProject:
			for _, p := range n.Projections {
				switch {
				case p.Alias != "":
					add(p.Alias)
				case p.Column != "":
					add(p.Column)
				}
				if p.Column != "" {
					add(p.Column)
				}
				expr := p.Expr
				if p.ASTExpr == nil && p.Column != "" {
					expr = ""
				}
				reads(p.ASTExpr, expr)
			}
			for _, a := range n.DeferredColumnAliases {
				add(a)
			}
			// A Project over an Aggregate publishes what the aggregate
			// computes: its inputs are what the stage reads.
			if len(n.Children) > 0 && n.Children[0] != nil && n.Children[0].Type == logical.NodeAggregate {
				n = n.Children[0]
				continue
			}
			return
		case logical.NodeAggregate:
			for _, a := range n.AggExprs {
				add(a.OutputCol)
				add(a.InputCol)
				add(a.InputCol2)
				reads(a.InputExpr, "")
			}
			for _, g := range n.GroupBy {
				add(g)
			}
			for _, g := range n.GroupByPublish {
				add(g)
			}
			return
		case logical.NodeScan:
			for _, c := range n.ScanColumns {
				add(c)
			}
			return
		}
		if len(n.Children) == 0 {
			return
		}
		n = n.Children[0]
	}
}

// foldKey is a name under the folding the stage re-spell's resolvers compare
// by — strings.EqualFold, Unicode SIMPLE folding — as one comparable key: each
// rune becomes the lower case of the least rune of its folding orbit.
// Lower-casing alone is not that identity: `ſ` (U+017F) lower-cases to itself
// and EqualFold-matches `s`, so a body publishing `"ſ"` beside an outer `s`
// passed the guard and its star read the OUTER `s` on the DAG (arc JP round 4).
func foldKey(name string) string {
	var b strings.Builder
	for _, r := range name {
		least := r
		for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
			if f < least {
				least = f
			}
		}
		b.WriteRune(unicode.ToLower(least))
	}
	return b.String()
}
