package logical

import "testing"

// A star sharing its SELECT list with another item must be expanded before
// column pruning runs. The star names no columns of its own, so a pruner that
// sees it unexpanded narrows the scan to whatever ELSE the SELECT list
// mentions — and every column the star contributed then reads NULL (#315).
func TestStarExpansionFeedsColumnPruning(t *testing.T) {
	schema := []string{"id", "name", "note"}

	tests := []struct {
		name      string
		star      Projection
		alias     string
		wantProjs []string
		wantScan  []string
	}{
		{
			name:      "unqualified",
			star:      Projection{Expr: "*"},
			wantProjs: []string{"id", "name", "note", "name"},
			wantScan:  []string{"id", "name", "note"},
		},
		{
			name:      "qualified by table name",
			star:      Projection{Expr: "items.*"},
			wantProjs: []string{"id", "name", "note", "name"},
			wantScan:  []string{"id", "name", "note"},
		},
		{
			name:      "qualified by alias",
			star:      Projection{Expr: "t.*"},
			alias:     "t",
			wantProjs: []string{"id", "name", "note", "name"},
			wantScan:  []string{"id", "name", "note"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scan := NewScan("items", tt.alias)
			scan.ScanColumns = schema
			plan := NewProject(scan, []Projection{
				tt.star,
				{Column: "name", Expr: "name", Alias: "name"},
			})

			plan = Optimize(plan)

			var gotProjs []string
			for _, proj := range plan.Projections {
				gotProjs = append(gotProjs, proj.Alias)
			}
			if !equalStrings(gotProjs, tt.wantProjs) {
				t.Errorf("projections = %v, want %v", gotProjs, tt.wantProjs)
			}
			if !equalStrings(scan.RequiredColumns, tt.wantScan) {
				t.Errorf("scan reads %v, want %v — a pruned column reads NULL for every row",
					scan.RequiredColumns, tt.wantScan)
			}
		})
	}
}

// A star whose column set is not knowable here is left alone rather than
// guessed at: expanding it to one side of a join would silently change which
// columns the query returns.
func TestStarExpansionDeclinesUnknowableSources(t *testing.T) {
	t.Run("join", func(t *testing.T) {
		left := NewScan("items", "")
		left.ScanColumns = []string{"id", "name"}
		right := NewScan("owners", "")
		right.ScanColumns = []string{"id", "owner"}
		plan := NewProject(NewJoin(left, right, "inner", "items.id = owners.id"),
			[]Projection{{Expr: "*"}, {Column: "owner", Expr: "owner", Alias: "owner"}})

		ExpandStarProjections(plan)

		if len(plan.Projections) != 2 || plan.Projections[0].Expr != "*" {
			t.Fatalf("star over a join was expanded: %+v", plan.Projections)
		}
	})

	t.Run("unannotated scan", func(t *testing.T) {
		scan := NewScan("items", "") // no ScanColumns: no catalog at plan time
		plan := NewProject(scan, []Projection{{Expr: "*"}})

		ExpandStarProjections(plan)

		if len(plan.Projections) != 1 || plan.Projections[0].Expr != "*" {
			t.Fatalf("star over an unannotated scan was expanded: %+v", plan.Projections)
		}
	})
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
