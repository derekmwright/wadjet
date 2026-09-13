package logical

import (
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// A BARE `*` OVER A JOIN IS THE FROM CLAUSE'S ARMS, IN WRITTEN ORDER (#997,
// #1012). Each item is a QUALIFIED reference — so it binds its own relation's
// column whichever side the plan builds — published under the column's own
// name, duplicates kept by position, which is what PostgreSQL publishes.
func TestABareStarOverAJoinExpandsToTheFromClausesArms(t *testing.T) {
	tests := []struct {
		name  string
		plan  func() *Node
		items []string // "expr AS published"
	}{
		{
			name: "two arms, left then right",
			plan: func() *Node {
				return NewProject(joinOf(t, armScan("lat_ord", "o", "id", "customer"),
					armScan("lat_item", "i", "id", "order_id")),
					[]Projection{{Expr: "*"}})
			},
			items: []string{"o.id AS id", "o.customer AS customer",
				"i.id AS id", "i.order_id AS order_id"},
		},
		{
			name: "a self-join names each arm by its own alias",
			plan: func() *Node {
				return NewProject(joinOf(t, armScan("lat_item", "a", "id", "amount"),
					armScan("lat_item", "b", "id", "amount")),
					[]Projection{{Expr: "*"}})
			},
			items: []string{"a.id AS id", "a.amount AS amount",
				"b.id AS id", "b.amount AS amount"},
		},
		{
			name: "three arms keep the chain's written order",
			plan: func() *Node {
				inner := joinOf(t, armScan("lat_ord", "o", "id"),
					armScan("lat_item", "i", "id"))
				return NewProject(joinOf(t, inner, armScan("lat_ord", "o2", "id")),
					[]Projection{{Expr: "*"}})
			},
			items: []string{"o.id AS id", "i.id AS id", "o2.id AS id"},
		},
		{
			name: "an arm with no alias is named by its table",
			plan: func() *Node {
				return NewProject(joinOf(t, armScan("lat_ord", "", "id"),
					armScan("lat_item", "", "id")),
					[]Projection{{Expr: "*"}})
			},
			items: []string{"lat_ord.id AS id", "lat_item.id AS id"},
		},
		{
			name: "a star BESIDE an item expands in the star's position",
			plan: func() *Node {
				return NewProject(joinOf(t, armScan("lat_ord", "o", "id"),
					armScan("lat_item", "i", "id")),
					[]Projection{{Expr: "*"}, {Column: "o.id", Expr: "o.id", Alias: "id",
						ASTExpr: &plansql.ColRef{Table: "o", Column: "id"}}})
			},
			items: []string{"o.id AS id", "i.id AS id", "o.id AS id"},
		},
		{
			name: "a FILTER between the star and the join is a pass-through",
			plan: func() *Node {
				j := joinOf(t, armScan("lat_ord", "o", "id"), armScan("lat_item", "i", "id"))
				return NewProject(NewFilter(j, nil), []Projection{{Expr: "*"}})
			},
			items: []string{"o.id AS id", "i.id AS id"},
		},
		{
			name: "a derived block arm publishes its OWN projection",
			plan: func() *Node {
				block := NewProject(armScan("lat_item", "i", "id", "amount"),
					[]Projection{{Column: "i.amount", Expr: "i.amount", Alias: "amt"}})
				block.DerivedAlias = "s"
				return NewProject(joinOf(t, armScan("lat_ord", "o", "id"), block),
					[]Projection{{Expr: "*"}})
			},
			items: []string{"o.id AS id", "s.amt AS amt"},
		},
		{
			name: "a block with no projection of its own publishes its one relation",
			plan: func() *Node {
				block := armScan("lat_item", "lat_item", "id", "amount")
				block.CTEName = "c"
				return NewProject(joinOf(t, armScan("lat_ord", "o", "id"), block),
					[]Projection{{Expr: "*"}})
			},
			items: []string{"o.id AS id", "c.id AS id", "c.amount AS amount"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := tt.plan()
			ExpandStarProjections(plan)
			var got []string
			for _, p := range plan.Projections {
				got = append(got, p.Expr+" AS "+p.Alias)
			}
			if strings.Join(got, " | ") != strings.Join(tt.items, " | ") {
				t.Errorf("expanded to\n  %s\nwant\n  %s",
					strings.Join(got, " | "), strings.Join(tt.items, " | "))
			}
			for _, p := range plan.Projections {
				ref, ok := p.ASTExpr.(*plansql.ColRef)
				if !ok {
					t.Fatalf("item %q is not a column reference: %T", p.Expr, p.ASTExpr)
				}
				if ref.Table == "" {
					t.Errorf("item %q lost its qualifier: a bare reference binds the "+
						"FIRST column of that name, which is the other arm's whenever "+
						"the plan swaps the sides", p.Expr)
				}
			}
		})
	}
}

// …and the shapes it DECLINES, each keeping the answer it had rather than
// publishing a list this pass cannot state. A partial expansion is never
// returned: the star covers every arm or none of them.
func TestABareStarOverAJoinDeclinesWhatItCannotState(t *testing.T) {
	tests := []struct {
		name string
		plan func() *Node
	}{
		{
			name: "an arm that publishes one name TWICE",
			plan: func() *Node {
				// `s.id` would bind the first of the two and the second
				// column would carry the first's VALUES.
				block := NewProject(armScan("lat_item", "i", "id"), []Projection{
					{Column: "i.id", Expr: "i.id", Alias: "id"},
					{Column: "o2.id", Expr: "o2.id", Alias: "id"},
				})
				block.DerivedAlias = "s"
				return NewProject(joinOf(t, armScan("lat_ord", "o", "id"), block),
					[]Projection{{Expr: "*"}})
			},
		},
		{
			name: "two arms of ONE name",
			plan: func() *Node {
				return NewProject(joinOf(t, armScan("lat_item", "", "id"),
					armScan("lat_item", "", "id")),
					[]Projection{{Expr: "*"}})
			},
		},
		{
			name: "a table function arm carries no catalog list",
			plan: func() *Node {
				fn := NewScan("read_json", "r")
				fn.IsTableFunc = true
				return NewProject(joinOf(t, armScan("lat_ord", "o", "id"), fn),
					[]Projection{{Expr: "*"}})
			},
		},
		{
			name: "an unannotated arm",
			plan: func() *Node {
				return NewProject(joinOf(t, armScan("lat_ord", "o", "id"),
					NewScan("lat_item", "i")), []Projection{{Expr: "*"}})
			},
		},
		{
			name: "a LATERAL arm publishes slots the join drops",
			plan: func() *Node {
				lat := armScan("lat_item", "i", "id")
				lat.LateralSubtree = true
				return NewProject(joinOf(t, armScan("lat_ord", "o", "id"), lat),
					[]Projection{{Expr: "*"}})
			},
		},
		{
			name: "a SEMI join publishes its probe alone",
			plan: func() *Node {
				j := NewJoin(armScan("lat_ord", "o", "id"), armScan("lat_item", "i", "id"),
					"semi", "o.id = i.order_id")
				return NewProject(j, []Projection{{Expr: "*"}})
			},
		},
		{
			name: "an AGGREGATE between the star and the join",
			plan: func() *Node {
				j := joinOf(t, armScan("lat_ord", "o", "id"), armScan("lat_item", "i", "id"))
				agg := NewAggregate(j, []string{"o.id"}, nil)
				return NewProject(agg, []Projection{{Expr: "*"}})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := tt.plan()
			ExpandStarProjections(plan)
			if len(plan.Projections) != 1 || plan.Projections[0].Expr != "*" {
				var got []string
				for _, p := range plan.Projections {
					got = append(got, p.Expr)
				}
				t.Fatalf("the star was expanded to %s", strings.Join(got, ", "))
			}
		})
	}
}

// The minted Project is a HYPOTHESIS: the builder cannot ask what an arm
// publishes (no scan is annotated yet), so it mints the projection on SHAPE
// and ElideUnstatedJoinStar takes it back out when the expansion could not
// state the arms. What is left is the tree the builder would have built —
// including the naming the enclosing query stamped on the block's root, which
// belongs to the relation and not to the projection.
func TestAnUnstatedStarProjectionIsTakenBackOut(t *testing.T) {
	block := NewProject(joinOf(t, armScan("lat_ord", "o", "id"), NewScan("lat_item", "i")),
		[]Projection{{Expr: "*", Column: "*"}})
	block.StarJoinArms = true
	block.DerivedAlias = "s"
	block.CTEs = []plansql.CTEDef{{Name: "c"}}
	root := NewProject(block, []Projection{{Column: "s.id", Expr: "s.id", Alias: "id"}})

	ExpandStarProjections(root)
	got := ElideUnstatedJoinStar(root)

	if got != root || len(got.Children) != 1 {
		t.Fatalf("the root projection was removed: %+v", got)
	}
	kept := got.Children[0]
	if kept.Type != NodeJoin {
		t.Fatalf("the minted projection is still there: %v", kept.Type)
	}
	if kept.DerivedAlias != "s" {
		t.Errorf("the derived alias was dropped: %q — the enclosing query can no "+
			"longer name this relation", kept.DerivedAlias)
	}
	if len(kept.CTEs) != 1 {
		t.Errorf("the WITH list was dropped: %v", kept.CTEs)
	}
}

// A STATED star keeps its projection, alias and all.
func TestAStatedStarProjectionSurvivesTheElision(t *testing.T) {
	block := NewProject(joinOf(t, armScan("lat_ord", "o", "id"),
		armScan("lat_item", "i", "id")), []Projection{{Expr: "*", Column: "*"}})
	block.StarJoinArms = true
	block.DerivedAlias = "s"

	ExpandStarProjections(block)
	got := ElideUnstatedJoinStar(block)

	if got != block || got.Type != NodeProject || len(got.Projections) != 2 {
		t.Fatalf("the stated star's projection did not survive: %+v", got)
	}
	if got.DerivedAlias != "s" {
		t.Errorf("derived alias = %q, want s", got.DerivedAlias)
	}
}

// A POSITIONAL ORDER BY term under the minted projection is answered from the
// star's own expanded list, in the SOURCE spelling: the Sort reads the JOIN's
// stream, where `i.id` names one column and the published `id` names two.
func TestAPositionalSortKeyOverAStarJoinBindsItsItemsSource(t *testing.T) {
	j := joinOf(t, armScan("lat_ord", "o", "id", "customer"),
		armScan("lat_item", "i", "id", "amount"))
	sort := NewSort(j, []OrderExpr{{Position: 4, Desc: true}, {Position: 1}})
	proj := NewProject(sort, []Projection{{Expr: "*", Column: "*"}})
	proj.StarJoinArms = true

	ExpandStarProjections(proj)
	ResolveStarJoinOrdinalSortKeys(proj)

	if got := sort.OrderBy[0]; got.Column != "i.amount" || got.Position != 0 || !got.Desc {
		t.Errorf("key 1 = %+v, want i.amount DESC with the position answered", got)
	}
	if got := sort.OrderBy[1]; got.Column != "o.id" || got.Position != 0 {
		t.Errorf("key 2 = %+v, want o.id with the position answered", got)
	}
}

// An out-of-range position is left for RefuseUnresolvedOrdinalSortKeys, which
// is what raises PostgreSQL's 42P10 with the position in the message.
func TestAnOutOfRangePositionIsLeftForTheRefusal(t *testing.T) {
	j := joinOf(t, armScan("lat_ord", "o", "id"), armScan("lat_item", "i", "id"))
	sort := NewSort(j, []OrderExpr{{Position: 9}})
	proj := NewProject(sort, []Projection{{Expr: "*", Column: "*"}})
	proj.StarJoinArms = true

	ExpandStarProjections(proj)
	ResolveStarJoinOrdinalSortKeys(proj)

	if sort.OrderBy[0].Position != 9 {
		t.Fatalf("the out-of-range position was answered: %+v", sort.OrderBy[0])
	}
}

// armScan is one FROM arm: a catalog-annotated base-table scan.
func armScan(table, alias string, cols ...string) *Node {
	n := NewScan(table, alias)
	n.ScanColumns = cols
	return n
}

func joinOf(t *testing.T, left, right *Node) *Node {
	t.Helper()
	return NewJoin(left, right, "inner", "")
}
