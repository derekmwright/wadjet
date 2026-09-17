// SPDX-License-Identifier: MIT

package physical

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// A physical.PhysicalPlan is a single-process pipeline and EXPLAIN VERBOSE says so.
//
// It used to print the distributed stage list, which the embedded engine
// never executes — `Plan` emitted a DAG only so this could print it. The
// stage list is the distributed planner's and `wadjetd` still prints it from
// there; the `wadjet` binary's EXPLAIN carries no "Stage " line at all, which
// TestTheEmbeddedExplainPrintsNoStageList gates on the built binary.
func TestPrettyPrintIsTheLocalPipeline(t *testing.T) {
	plan := &PhysicalPlan{}
	got := plan.PrettyPrint()
	if got != "Single-stage local execution" {
		t.Errorf("PrettyPrint() = %q, want 'Single-stage local execution'", got)
	}
	if strings.Contains(got, "Stage ") {
		t.Errorf("the local plan's EXPLAIN names a stage: %q", got)
	}
}

func TestHasFilterOrPartition_NilNode(t *testing.T) {
	if HasFilterOrPartition(nil) {
		t.Error("expected false for nil node")
	}
}

func TestHasFilterOrPartition_DeepFilter(t *testing.T) {
	scan := logical.NewScan("t", "")
	filter := logical.NewFilter(scan, nil)
	project := logical.NewProject(filter, nil)

	if !HasFilterOrPartition(project) {
		t.Error("expected true for deeply nested filter")
	}
}

func TestHasLimit_NilNode(t *testing.T) {
	if HasLimit(nil) {
		t.Error("expected false for nil node")
	}
}

func TestHasLimit_NoLimit(t *testing.T) {
	scan := logical.NewScan("t", "")
	if HasLimit(scan) {
		t.Error("expected false for scan without limit")
	}
}

func TestHasLimit_DeepLimit(t *testing.T) {
	scan := logical.NewScan("t", "")
	limit := logical.NewLimit(scan, 10, 0)
	sort := logical.NewSort(limit, nil)

	if !HasLimit(sort) {
		t.Error("expected true for deeply nested limit")
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{500, "500B"},
		{1 << 20, "1.0MB"},
		{5 * (1 << 20), "5.0MB"},
		{1 << 30, "1.0GB"},
		{3 * (1 << 30), "3.0GB"},
		{1 << 40, "1.0TB"},
		{2 * (1 << 40), "2.0TB"},
	}
	for _, tt := range tests {
		got := FormatBytes(tt.input)
		if got != tt.want {
			t.Errorf("formatBytes(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestMapJoinType(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"inner", "inner"},
		{"INNER JOIN", "inner"},
		{"left", "left"},
		{"LEFT OUTER JOIN", "left"},
		{"right", "right"},
		{"RIGHT JOIN", "right"},
		{"full", "full"},
		{"FULL OUTER JOIN", "full"},
		{"cross", "cross"},
		{"CROSS JOIN", "cross"},
		{"semi", "semi"},
		{"SEMI JOIN", "semi"},
		{"anti", "anti"},
		{"ANTI JOIN", "anti"},
		{"join", "inner"},
		{"", "inner"},
	}
	for _, tt := range tests {
		got := MapJoinType(tt.input)
		if got != tt.want {
			t.Errorf("MapJoinType(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestMapExecJoinType(t *testing.T) {
	tests := []struct {
		input string
		want  exec.JoinType
	}{
		{"left", exec.LeftJoin},
		{"right", exec.RightJoin},
		{"full", exec.FullOuterJoin},
		{"cross", exec.CrossJoin},
		{"semi", exec.SemiJoin},
		{"anti", exec.AntiJoin},
		{"inner", exec.InnerJoin},
		{"unknown", exec.InnerJoin},
	}
	for _, tt := range tests {
		got := MapExecJoinType(tt.input)
		if got != tt.want {
			t.Errorf("mapExecJoinType(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestParseJoinKeys(t *testing.T) {
	tests := []struct {
		cond     string
		leftN    int
		rightN   int
		residual int
	}{
		{cond: "e.user_id = u.user_id", leftN: 1, rightN: 1},
		{cond: "e.a = u.a AND e.b = u.b", leftN: 2, rightN: 2},
		{cond: "a = b", leftN: 1, rightN: 1},
		{cond: "no equals here", residual: 1},
		// The `1 = 1` sentinel extractJoinCondPredicates writes when every
		// ON conjunct has been pushed to a child: ON TRUE, not a refusal.
		{cond: "1 = 1", leftN: 1, rightN: 1},
	}
	for _, tt := range tests {
		left, right, residual := ParseJoinKeys(tt.cond)
		if len(left) != tt.leftN || len(right) != tt.rightN || len(residual) != tt.residual {
			t.Errorf("ParseJoinKeys(%q) = left=%v, right=%v, residual=%v, want %d left, %d right, %d residual",
				tt.cond, left, right, residual, tt.leftN, tt.rightN, tt.residual)
		}
	}
}

// TestParseJoinKeysRefusesUnrepresentable is #351: the key parser used to
// split the condition TEXT on "=" and hand whatever fell either side to the
// executor as a column name. An operand that is not a bare column resolves to
// nothing there, and an unresolvable key hashes as a constant — so the join
// matched no rows when one side was real and every row when neither was.
// Each of these must come back as a residual the caller refuses, never as a
// key.
func TestParseJoinKeysRefusesUnrepresentable(t *testing.T) {
	tests := []struct {
		name string
		cond string
	}{
		{"expression on the right", "n.n_regionkey = r.r_regionkey + 3"},
		{"expression on the left", "n.n_regionkey + 3 = r.r_regionkey"},
		{"expression on both sides", "n.n_regionkey + 1 = r.r_regionkey + 2"},
		{"function operand", "n.n_regionkey = ABS(r.r_regionkey)"},
		{"literal operand", "n.n_regionkey = 1"},
		// The lexical split found the "=" INSIDE these operators and
		// produced the column name "a.x <".
		{"less-or-equal", "a.x <= b.y"},
		{"greater-or-equal", "a.x >= b.y"},
		{"not-equal", "a.x != b.y"},
		{"disjunction", "a.x = b.y OR a.z = b.w"},
		// A key conjunct alongside an unrepresentable one: the good half
		// must not launder the bad half through.
		{"mixed with a real key", "a.k = b.k AND a.x = b.y + 1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, residual := ParseJoinKeys(tt.cond)
			if len(residual) == 0 {
				left, right, _ := ParseJoinKeys(tt.cond)
				t.Fatalf("ParseJoinKeys(%q) reported no residual; it produced keys left=%v right=%v, "+
					"which the executor would look up as column names", tt.cond, left, right)
			}
		})
	}
}

func TestMatchesPartitionFilter(t *testing.T) {
	tests := []struct {
		name     string
		partVals map[string]string
		filter   map[string]string
		want     bool
	}{
		{"match", map[string]string{"year": "2026"}, map[string]string{"year": "2026"}, true},
		{"no match", map[string]string{"year": "2025"}, map[string]string{"year": "2026"}, false},
		{"filter key missing from partition", map[string]string{}, map[string]string{"year": "2026"}, true},
		{"multi key match", map[string]string{"year": "2026", "month": "03"}, map[string]string{"year": "2026", "month": "03"}, true},
		{"partial mismatch", map[string]string{"year": "2026", "month": "04"}, map[string]string{"year": "2026", "month": "03"}, false},
		{"empty filter", map[string]string{"year": "2026"}, map[string]string{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MatchesPartitionFilter(tt.partVals, tt.filter)
			if got != tt.want {
				t.Errorf("MatchesPartitionFilter(%v, %v) = %v, want %v", tt.partVals, tt.filter, got, tt.want)
			}
		})
	}
}

func TestEvalFilterTyped(t *testing.T) {
	const (
		opNE = iota
		opGT
		opLT
		opGE
		opLE
		opEQ
	)

	// Test int32 comparisons
	pv := batch.NewVector(batch.TypeInt32, 2)
	bv := batch.NewVector(batch.TypeInt32, 2)
	pv.Int32Data[0] = 10
	pv.Int32Data[1] = 20
	bv.Int32Data[0] = 20
	bv.Int32Data[1] = 20
	if !EvalFilterTyped(pv, bv, 0, 0, opNE) {
		t.Error("10 != 20 should be true")
	}
	if EvalFilterTyped(pv, bv, 1, 1, opNE) {
		t.Error("20 != 20 should be false")
	}
	if !EvalFilterTyped(pv, bv, 0, 0, opLT) {
		t.Error("10 < 20 should be true")
	}
	if !EvalFilterTyped(pv, bv, 1, 1, opEQ) {
		t.Error("20 == 20 should be true")
	}

	// Test string comparisons via fallback
	sv := batch.NewVector(batch.TypeString, 2)
	sv.BytesData.Set(0, []byte("a"))
	sv.BytesData.Set(1, []byte("b"))
	tv := batch.NewVector(batch.TypeString, 2)
	tv.BytesData.Set(0, []byte("b"))
	tv.BytesData.Set(1, []byte("b"))
	if !EvalFilterTyped(sv, tv, 0, 0, opNE) {
		t.Error("a != b should be true")
	}
	if EvalFilterTyped(sv, tv, 1, 1, opNE) {
		t.Error("b != b should be false")
	}
	if !EvalFilterTyped(sv, tv, 0, 0, opLT) {
		t.Error("a < b should be true")
	}
}

func TestMapPredOp(t *testing.T) {
	tests := []struct {
		op   string
		want exec.CompareOp
	}{
		{"=", exec.OpEq},
		{"!=", exec.OpNe},
		{"<>", exec.OpNe},
		{"<", exec.OpLt},
		{"<=", exec.OpLe},
		{">", exec.OpGt},
		{">=", exec.OpGe},
		{"like", exec.CompareOp(-1)},
		{"in", exec.CompareOp(-1)},
	}
	for _, tt := range tests {
		got := MapPredOp(tt.op)
		if got != tt.want {
			t.Errorf("mapPredOp(%q) = %d, want %d", tt.op, got, tt.want)
		}
	}
}

func TestDecimalFromBytes(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		wantZ bool // true if we expect zero
	}{
		{"empty", nil, true},
		{"zero", []byte{0}, false},
		{"small positive", []byte{0x00, 0x0A}, false},                 // 10
		{"small negative", []byte{0xFF, 0xF6}, false},                 // -10 (sign-extended)
		{"large positive", []byte{0, 0, 0, 0, 0, 0, 0, 0, 42}, false}, // 42 in 9 bytes
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DecimalFromBytes(tt.input)
			if tt.wantZ && (got.Hi != 0 || got.Lo != 0) {
				t.Errorf("decimalFromBytes(%v) = {Hi:%d, Lo:%d}, want zero", tt.input, got.Hi, got.Lo)
			}
		})
	}
}

func TestIsURL(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"http://example.com", true},
		{"https://example.com/data.json", true},
		{"s3://bucket/key", false},
		{"/tmp/data.json", false},
		{"data.json", false},
	}
	for _, tt := range tests {
		got := IsURL(tt.input)
		if got != tt.want {
			t.Errorf("isURL(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestIsGlob(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"*.json", true},
		{"data?.csv", true},
		{"data[0-9].csv", true},
		{"data.json", false},
		{"/path/to/file.parquet", false},
	}
	for _, tt := range tests {
		got := IsGlob(tt.input)
		if got != tt.want {
			t.Errorf("isGlob(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestDBScanSource_Close_NilFields(t *testing.T) {
	s := &DbScanSource{}
	err := s.Close()
	if err != nil {
		t.Errorf("Close on nil fields should not error, got %v", err)
	}
}

func TestBuildTableFunctionSource_CSV_SepArg(t *testing.T) {
	source, err := BuildTableFunctionSource("read_csv", []string{"/tmp/test.csv"}, map[string]string{"sep": "|"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	csvSrc, ok := source.(*CsvTableFuncSource)
	if !ok {
		t.Fatalf("expected *physical.CsvTableFuncSource, got %T", source)
	}
	if csvSrc.NamedArgs["sep"] != "|" {
		t.Errorf("expected sep=|, got %v", csvSrc.NamedArgs)
	}
}

func TestBuildTableFunctionSource_ReadJSONAuto(t *testing.T) {
	source, err := BuildTableFunctionSource("read_json_auto", []string{"/tmp/data.json"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := source.(*JsonTableFuncSource); !ok {
		t.Errorf("expected *physical.JsonTableFuncSource for read_json_auto, got %T", source)
	}
}

func TestBuildTableFunctionSource_ReadCSVAuto(t *testing.T) {
	source, err := BuildTableFunctionSource("read_csv_auto", []string{"/tmp/data.csv"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := source.(*CsvTableFuncSource); !ok {
		t.Errorf("expected *physical.CsvTableFuncSource for read_csv_auto, got %T", source)
	}
}

func TestResolveNullsLast(t *testing.T) {
	yes, no := true, false
	cases := []struct {
		name string
		ob   logical.OrderExpr
		want bool
	}{
		// The engine default is PostgreSQL's rule: NULLS LAST for ASC,
		// NULLS FIRST for DESC. SQL leaves the default
		// implementation-defined; wadjet speaks the PostgreSQL wire
		// protocol, so a psql/DataGrip/Superset client writing ORDER BY x
		// DESC gets PostgreSQL's placement.
		//
		// This default was unreachable before #343 — the DESC comparator
		// negated the kernel's null handling along with its values, so a
		// nominal NULLS FIRST for DESC left the engine as NULLS LAST. With
		// that negation gone the declaration is what the engine emits.
		//
		// DuckDB defaults the other way (NULLS LAST in both directions), so
		// the differential gate runs the oracle with
		// default_null_order='nulls_last_on_asc_first_on_desc' rather than
		// exempting the ordering entries — a configured oracle still
		// compares every row, an exemption would not.
		{"ASC default", logical.OrderExpr{Column: "id"}, true},
		{"DESC default", logical.OrderExpr{Column: "id", Desc: true}, false},
		// An explicit clause wins in BOTH directions. DESC was the broken
		// pair — both spellings came out inverted (#343).
		{"ASC NULLS FIRST", logical.OrderExpr{Column: "id", NullsFirst: &yes}, false},
		{"ASC NULLS LAST", logical.OrderExpr{Column: "id", NullsFirst: &no}, true},
		{"DESC NULLS FIRST", logical.OrderExpr{Column: "id", Desc: true, NullsFirst: &yes}, false},
		{"DESC NULLS LAST", logical.OrderExpr{Column: "id", Desc: true, NullsFirst: &no}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveNullsLast(tc.ob); got != tc.want {
				t.Errorf("ResolveNullsLast = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestExtractFilterBuildColumns(t *testing.T) {
	// Empty
	cols := ExtractFilterBuildColumns("")
	if cols != nil {
		t.Errorf("expected nil for empty, got %v", cols)
	}

	// Simple equality
	cols = ExtractFilterBuildColumns("e.id = u.id")
	if len(cols) != 1 {
		t.Fatalf("expected 1 column, got %d: %v", len(cols), cols)
	}
	// The right side of "e.id = u.id" is the build column, kept QUALIFIED: a
	// join emits a build column under its relation's qualifier whenever the
	// bare name collides on the probe side, and stripping here read the
	// wrong relation's column (#527). exec resolves it with a bare-name
	// fallback for the single-relation builds that emit it that way.
	if cols[0] != "u.id" {
		t.Errorf("expected 'u.id', got %q", cols[0])
	}

	// Multiple conditions with AND
	cols = ExtractFilterBuildColumns("a.x = b.x AND a.y = b.y")
	if len(cols) != 2 {
		t.Fatalf("expected 2 columns, got %d: %v", len(cols), cols)
	}

	// No operator matches
	cols = ExtractFilterBuildColumns("unknown stuff")
	if len(cols) != 0 {
		t.Errorf("expected 0 columns for unrecognized filter, got %v", cols)
	}
}

func TestCleanExpr(t *testing.T) {
	if cleanExpr("t.col") != "col" {
		t.Error("expected 'col' for 't.col'")
	}
	if cleanExpr("col") != "col" {
		t.Error("expected 'col' for 'col'")
	}
	if cleanExpr("  t.col  ") != "col" {
		t.Error("expected 'col' for '  t.col  '")
	}
}

// TestCleanExprLeavesExpressionsAlone pins the #513 rule: only a COLUMN
// REFERENCE has a qualifier to strip. Splitting on the first dot of an
// expression returns a fragment of it — `upper(t0.c0)` became `c0)` — and that
// fragment became the output column name a client binds by.
func TestCleanExprLeavesExpressionsAlone(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"upper(t0.c0)", "upper(t0.c0)"},
		{"concat(t0.c0, t0.c1)", "concat(t0.c0, t0.c1)"},
		{"coalesce(t0.c0, 'q')", "coalesce(t0.c0, 'q')"},
		{"t0.c1 + 1", "t0.c1 + 1"},
		{"count(t0.c0)", "count(t0.c0)"},
		{"cast(t.c as bigint)", "cast(t.c as bigint)"},
		// Still a plain reference, still stripped.
		{"t.col", "col"},
		{"col", "col"},
		// A delimited identifier is ONE name, dot included (#304).
		{`"id.orig_h"`, "id.orig_h"},
	} {
		if got := cleanExpr(tc.in); got != tc.want {
			t.Errorf("CleanExpr(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
