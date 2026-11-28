// SPDX-License-Identifier: MIT

package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// guardStored is the guard tests' table p as the catalog declares it.
var guardStored = []parquet.Column{
	{Name: "id", Type: parquet.TypeInt64}, {Name: "x", Type: parquet.TypeInt64}, {Name: "ssn", Type: parquet.TypeString},
}

// guardStoredType is col's declared type in guardStored: the shape tests
// below exercise line q's walk with the values in the stored type.
func guardStoredType(col string) batch.TypeID {
	for _, c := range guardStored {
		if c.Name == col {
			return c.Type
		}
	}
	return parquet.TypeInt64
}

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
		return &catalogScanSource{tableName: "p", requiredCols: []string{"id", "x", "ssn"}, storedCols: guardStored}
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
			got := scanFilterTarget(src, ops, tc.col, guardStoredType(tc.col)) != nil
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
			if _, isScan := tc.src().(*catalogScanSource); isScan && scanFilterTarget(tc.src(), ops, tc.col, guardStoredType(tc.col)) == nil {
				t.Fatalf("with the guard off the old walk should have attached")
			}
		})
	}
}

// TestDynamicFilterKeepsOnlyProvedRanges: a multi-key join's ranges are
// installed column by column — the proved key keeps its range, the unproved
// one is dropped, never the whole set on the strength of one.
func TestDynamicFilterKeepsOnlyProvedRanges(t *testing.T) {
	s := &catalogScanSource{tableName: "p", requiredCols: []string{"id", "x", "ssn"}, storedCols: guardStored}
	ops := []exec.UnaryOperator{exec.NewProject([]exec.ProjectColumn{
		{Name: "id", DirectCopy: "id"}, {Name: "ssn", Computed: true}})}
	attachDynamicFilterToScanSource(s, ops, []exec.DynamicRange{
		{Column: "id", MinValue: int64(1), MaxValue: int64(2)},
		{Column: "ssn", MinValue: "***", MaxValue: "***"},
	}, map[string]batch.TypeID{"id": parquet.TypeInt64, "ssn": parquet.TypeString})
	if len(s.dynamicFilter) != 1 || s.dynamicFilter[0].Column != "id" {
		t.Fatalf("installed %+v, want only the id range", s.dynamicFilter)
	}
}

// TestScanFilterTypeRuleIsProvedOrNothing is line r's half of the guard: a
// build-key filter on a proved stored column is installed only when its values
// are in exactly the column's declared type, and that type's representation is
// one the type ID fixes. Every promotion a join key accepts — DATE against
// TIMESTAMP, INTEGER against BIGINT, an integer or a float against NUMERIC,
// REAL against DOUBLE, a string against a network type — gets nothing. With
// the type rule off (line q's guard alone) each would have been installed —
// the negative control.
func TestScanFilterTypeRuleIsProvedOrNothing(t *testing.T) {
	stored := []parquet.Column{
		{Name: "i4", Type: parquet.TypeInt32}, {Name: "i8", Type: parquet.TypeInt64},
		{Name: "f4", Type: parquet.TypeFloat32}, {Name: "f8", Type: parquet.TypeFloat64},
		{Name: "m", Type: parquet.TypeDecimal, Precision: 10, Scale: 2},
		{Name: "d", Type: parquet.TypeDate}, {Name: "ts", Type: parquet.TypeTimestamp},
		{Name: "s", Type: parquet.TypeString}, {Name: "ip", Type: parquet.TypeIPv4},
		{Name: "u", Type: parquet.TypeUUID}, {Name: "c", Type: parquet.TypeCIDR},
		{Name: "pt", Type: parquet.TypePort}, {Name: "mc", Type: parquet.TypeMAC},
		{Name: "pr", Type: parquet.TypeProtocol}, {Name: "Mixed", Type: parquet.TypeInt64},
		{Name: "dup", Type: parquet.TypeInt64}, {Name: "DUP", Type: parquet.TypeInt64},
	}
	var cols []string
	for _, c := range stored {
		cols = append(cols, c.Name)
	}
	cols = append(cols, "mixed", "undeclared")
	scan := func() *catalogScanSource {
		return &catalogScanSource{tableName: "xp", requiredCols: cols, storedCols: stored}
	}
	cases := []struct {
		col  string
		val  batch.TypeID
		want bool
	}{
		{"i4", parquet.TypeInt32, true},
		{"i8", parquet.TypeInt64, true},
		{"f8", parquet.TypeFloat64, true},
		{"d", parquet.TypeDate, true},
		{"ts", parquet.TypeTimestamp, true},
		{"s", parquet.TypeString, true},
		{"ip", parquet.TypeIPv4, true},
		{"pt", parquet.TypePort, true},
		{"mc", parquet.TypeMAC, true},
		{"pr", parquet.TypeProtocol, true},
		{"mixed", parquet.TypeInt64, true}, // one case-insensitive declaration
		{"dup", parquet.TypeInt64, true},   // its own spelling decides

		{"d", parquet.TypeTimestamp, false},   // the DATE probe of TIMESTAMP build keys
		{"ts", parquet.TypeDate, false},       // and the reverse
		{"i4", parquet.TypeInt64, false},      // INTEGER against BIGINT
		{"i8", parquet.TypeInt32, false},      // and the reverse
		{"m", parquet.TypeInt32, false},       // NUMERIC against an integer
		{"m", parquet.TypeInt64, false},       //
		{"m", parquet.TypeFloat64, false},     // NUMERIC against DOUBLE
		{"m", parquet.TypeDecimal, false},     // NUMERIC at its own type: the type ID does not fix the scale
		{"f8", parquet.TypeInt64, false},      // DOUBLE against an integer
		{"i8", parquet.TypeFloat64, false},    // an integer against DOUBLE
		{"f4", parquet.TypeFloat64, false},    // REAL against DOUBLE
		{"f4", parquet.TypeFloat32, false},    // a type no build filter is recorded for
		{"ip", parquet.TypeString, false},     // a network column against a string
		{"u", parquet.TypeString, false},      // UUID against a string
		{"c", parquet.TypeCIDR, false},        // CIDR's confirmed bounds are their own domain
		{"d", parquet.TypeInt32, false},       // DATE against its int32 carrier
		{"pt", parquet.TypeInt32, false},      // PORT against its int32 carrier
		{"ip", parquet.TypeInt64, false},      // IPv4 against its int64 carrier
		{"i4", exec.KeyTypeUnresolved, false}, // the build's type is not proved
		{"undeclared", parquet.TypeInt64, false},
	}
	for _, tc := range cases {
		got := scanFilterTarget(scan(), nil, tc.col, tc.val) != nil
		if got != tc.want {
			t.Errorf("scanFilterTarget(%s, values %v) = %v, want %v", tc.col, tc.val, got, tc.want)
			continue
		}
		if tc.want {
			continue
		}
		// The negative control: line q's guard alone installs it.
		scanFilterTypeRuleOffForTest = true
		off := scanFilterTarget(scan(), nil, tc.col, tc.val) != nil
		scanFilterTypeRuleOffForTest = false
		if !off {
			t.Errorf("%s/%v: with the type rule off line q's guard should have attached", tc.col, tc.val)
		}
	}
}

// TestScanFilterValueTypesConservativePerColumn: a column two keys name keeps a
// type only when both keys carry it; a key past the answer's end has none.
func TestScanFilterValueTypesConservativePerColumn(t *testing.T) {
	got := scanFilterValueTypes([]string{"a", "b", "b", "c"},
		[]batch.TypeID{parquet.TypeInt64, parquet.TypeDate, parquet.TypeTimestamp})
	if got["a"] != parquet.TypeInt64 || got["b"] != exec.KeyTypeUnresolved || got["c"] != exec.KeyTypeUnresolved {
		t.Fatalf("scanFilterValueTypes = %v", got)
	}
}
