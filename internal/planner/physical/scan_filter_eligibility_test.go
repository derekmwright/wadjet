// SPDX-License-Identifier: MIT

package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// lowerProbe is the probe of a built hash join of the given kind whose build
// side carries buildCols (all INT64), keyed on "id".
func lowerProbe(t *testing.T, jt exec.JoinType, buildCols ...string) *exec.HashJoinProbe {
	t.Helper()
	schema := make([]parquet.Column, len(buildCols))
	row := map[string]any{}
	for i, c := range buildCols {
		schema[i] = parquet.Column{Name: c, Type: parquet.TypeInt64}
		row[c] = int64(i + 1)
	}
	hj := exec.NewHashJoin(jt, []string{"id"}, []string{buildCols[0]})
	if err := hj.Build(t.Context(), exec.NewSliceSource(schema, []map[string]any{row})); err != nil {
		t.Fatalf("build: %v", err)
	}
	return hj.Probe()
}

// TestScanFilterTargetIsProvedOrNothing is #1677's guard at the choke point:
// a filter derived from a join's build keys reaches the probe scan only
// through hops that keep the key the stored column, and every other shape
// gets nothing. With the guard off (the walk it replaced) every unproved shape
// would have been handed the scan — the negative control.
func TestScanFilterTargetIsProvedOrNothing(t *testing.T) {
	scan := func() *catalogScanSource {
		return &catalogScanSource{tableName: "p", requiredCols: []string{"id", "x", "ssn"}}
	}
	pass := func(names ...string) exec.ProjectColumn {
		return exec.ProjectColumn{Name: names[0], DirectCopy: names[len(names)-1]}
	}
	cases := []struct {
		name string
		src  func() exec.Source
		ops  func() []exec.UnaryOperator
		col  string
		want bool
	}{
		{"plain scan", func() exec.Source { return scan() }, nil, "x", true},
		{"through a row filter and a bloom pre-check", func() exec.Source { return scan() },
			func() []exec.UnaryOperator {
				return []exec.UnaryOperator{&exec.MatchNothingFilter{}, &exec.NullCheckFilter{}}
			}, "x", true},
		{"through a lower inner join with no build column x", func() exec.Source { return scan() },
			func() []exec.UnaryOperator { return []exec.UnaryOperator{lowerProbe(t, exec.InnerJoin, "id", "v")} }, "x", true},
		{"through a lower semi join", func() exec.Source { return scan() },
			func() []exec.UnaryOperator { return []exec.UnaryOperator{lowerProbe(t, exec.SemiJoin, "id", "x")} }, "x", true},
		{"through a pass-through projection", func() exec.Source { return scan() },
			func() []exec.UnaryOperator {
				return []exec.UnaryOperator{exec.NewProject([]exec.ProjectColumn{pass("id"), pass("x")})}
			}, "x", true},
		{"through a pipelineSource", func() exec.Source {
			return &pipelineSource{source: scan(), ops: []exec.UnaryOperator{&exec.NullCheckFilter{}}}
		}, nil, "x", true},

		{"a lower inner join whose BUILD carries x", func() exec.Source { return scan() },
			func() []exec.UnaryOperator { return []exec.UnaryOperator{lowerProbe(t, exec.InnerJoin, "id", "r.x")} }, "x", false},
		{"a lower right join", func() exec.Source { return scan() },
			func() []exec.UnaryOperator { return []exec.UnaryOperator{lowerProbe(t, exec.RightJoin, "id", "v")} }, "x", false},
		{"a masked (computed) projection", func() exec.Source { return scan() },
			func() []exec.UnaryOperator {
				return []exec.UnaryOperator{exec.NewProject([]exec.ProjectColumn{pass("id"),
					{Name: "ssn", Computed: true}})}
			}, "ssn", false},
		{"a rename onto a stored column's name", func() exec.Source { return scan() },
			func() []exec.UnaryOperator {
				return []exec.UnaryOperator{exec.NewProject([]exec.ProjectColumn{pass("id"),
					{Name: "x", SourceCol: "y", DirectCopy: "y"}})}
			}, "x", false},
		{"a projection carrying x twice", func() exec.Source { return scan() },
			func() []exec.UnaryOperator {
				return []exec.UnaryOperator{exec.NewProject([]exec.ProjectColumn{pass("x"), pass("q.x", "x")})}
			}, "x", false},
		{"a positional source", func() exec.Source { return scan() },
			func() []exec.UnaryOperator {
				return []exec.UnaryOperator{exec.NewProject([]exec.ProjectColumn{
					{Name: "x", DirectCopy: "x", SourceIdx: 0, SourceIdxSet: true}})}
			}, "x", false},
		{"an operator the guard does not know", func() exec.Source { return scan() },
			func() []exec.UnaryOperator { return []exec.UnaryOperator{exec.NewLimit(1, 0)} }, "x", false},
		{"a column the scan does not store", func() exec.Source { return scan() }, nil, "q.x", false},
		{"a scan filling the multi-consumer cache", func() exec.Source {
			s := scan()
			s.cache = &scanCached{}
			return s
		}, nil, "x", false},
		{"a source that is not a base-table scan", func() exec.Source { return &exec.SliceSource{} }, nil, "x", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var ops []exec.UnaryOperator
			if tc.ops != nil {
				ops = tc.ops()
			}
			src := tc.src()
			got := scanFilterTarget(src, ops, tc.col) != nil
			if got != tc.want {
				t.Fatalf("scanFilterTarget(%s) = %v, want %v", tc.col, got, tc.want)
			}
			if tc.want {
				return
			}
			// The negative control: the walk this guard replaced handed the
			// scan to every shape that has one.
			scanFilterGuardOffForTest = true
			defer func() { scanFilterGuardOffForTest = false }()
			if _, isScan := tc.src().(*catalogScanSource); isScan && scanFilterTarget(tc.src(), ops, tc.col) == nil {
				t.Fatalf("with the guard off the old walk should have attached")
			}
		})
	}
}

// TestDynamicFilterKeepsOnlyProvedRanges: a multi-key join's ranges are
// installed column by column — the proved key keeps its range, the unproved
// one is dropped, never the whole set on the strength of one.
func TestDynamicFilterKeepsOnlyProvedRanges(t *testing.T) {
	s := &catalogScanSource{tableName: "p", requiredCols: []string{"id", "x", "ssn"}}
	ops := []exec.UnaryOperator{exec.NewProject([]exec.ProjectColumn{
		{Name: "id", DirectCopy: "id"}, {Name: "ssn", Computed: true}})}
	attachDynamicFilterToScanSource(s, ops, []exec.DynamicRange{
		{Column: "id", MinValue: int64(1), MaxValue: int64(2)},
		{Column: "ssn", MinValue: "***", MaxValue: "***"},
	})
	if len(s.dynamicFilter) != 1 || s.dynamicFilter[0].Column != "id" {
		t.Fatalf("installed %+v, want only the id range", s.dynamicFilter)
	}
}
