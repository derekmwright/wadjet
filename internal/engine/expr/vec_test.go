package expr

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// makeStringBatch creates a batch with a string column for vectorized tests.
func makeStringBatch(vals []string) *batch.RecordBatch {
	schema := []parquet.Column{
		{Name: "s", Type: parquet.TypeString, Nullable: true},
	}
	b := batch.NewRecordBatch(schema, len(vals))
	for i, v := range vals {
		if v == "" {
			b.Columns[0].Nulls.SetNull(i)
			b.Columns[0].BytesData.Set(i, nil)
		} else {
			b.Columns[0].BytesData.Set(i, []byte(v))
		}
	}
	return b
}

// makeStringFloat64Batch creates a batch with string + float64 columns.
func makeStringFloat64Batch(strs []string, f1, f2 []float64) *batch.RecordBatch {
	schema := []parquet.Column{
		{Name: "s", Type: parquet.TypeString, Nullable: true},
		{Name: "start", Type: parquet.TypeFloat64},
		{Name: "length", Type: parquet.TypeFloat64},
	}
	n := len(strs)
	b := batch.NewRecordBatch(schema, n)
	for i, v := range strs {
		if v == "" {
			b.Columns[0].Nulls.SetNull(i)
			b.Columns[0].BytesData.Set(i, nil)
		} else {
			b.Columns[0].BytesData.Set(i, []byte(v))
		}
	}
	for i := 0; i < n; i++ {
		if i < len(f1) {
			b.Columns[1].Float64Data[i] = f1[i]
		}
		if i < len(f2) {
			b.Columns[2].Float64Data[i] = f2[i]
		}
	}
	return b
}

func TestVecUpper(t *testing.T) {
	vals := []string{"hello", "WORLD", "MiXeD", "123abc"}
	b := makeStringBatch(vals)
	out := batch.NewVector(batch.TypeString, len(vals))
	args := []*batch.Vector{b.Columns[0]}
	vecUpper(args, out, len(vals))

	want := []string{"HELLO", "WORLD", "MIXED", "123ABC"}
	for i, w := range want {
		got := out.BytesData.StringValue(i)
		if got != w {
			t.Errorf("row %d: got %q, want %q", i, got, w)
		}
	}
}

func TestVecLower(t *testing.T) {
	vals := []string{"HELLO", "world", "MiXeD", "123ABC"}
	b := makeStringBatch(vals)
	out := batch.NewVector(batch.TypeString, len(vals))
	args := []*batch.Vector{b.Columns[0]}
	vecLower(args, out, len(vals))

	want := []string{"hello", "world", "mixed", "123abc"}
	for i, w := range want {
		got := out.BytesData.StringValue(i)
		if got != w {
			t.Errorf("row %d: got %q, want %q", i, got, w)
		}
	}
}

func TestVecLength(t *testing.T) {
	vals := []string{"hello", "ab", "x"}
	b := makeStringBatch(vals)
	out := batch.NewVector(batch.TypeFloat64, len(vals))
	args := []*batch.Vector{b.Columns[0]}
	vecLength(args, out, len(vals))

	want := []float64{5, 2, 1}
	for i, w := range want {
		if out.Float64Data[i] != w {
			t.Errorf("row %d: got %f, want %f", i, out.Float64Data[i], w)
		}
	}
}

func TestVecTrim(t *testing.T) {
	vals := []string{"  hello  ", "\tworld\n", "notrim"}
	b := makeStringBatch(vals)
	out := batch.NewVector(batch.TypeString, len(vals))
	args := []*batch.Vector{b.Columns[0]}
	vecTrim(args, out, len(vals))

	want := []string{"hello", "world", "notrim"}
	for i, w := range want {
		got := out.BytesData.StringValue(i)
		if got != w {
			t.Errorf("row %d: got %q, want %q", i, got, w)
		}
	}
}

func TestVecSubstr(t *testing.T) {
	strs := []string{"hello world", "abcdef", "xy"}
	starts := []float64{1, 3, 2}
	lengths := []float64{5, 2, 10}
	b := makeStringFloat64Batch(strs, starts, lengths)

	out := batch.NewVector(batch.TypeString, len(strs))
	args := []*batch.Vector{b.Columns[0], b.Columns[1], b.Columns[2]}
	vecSubstr(args, out, len(strs))

	want := []string{"hello", "cd", "y"}
	for i, w := range want {
		got := out.BytesData.StringValue(i)
		if got != w {
			t.Errorf("row %d: got %q, want %q", i, got, w)
		}
	}
}

func TestVecSubstr_ConstArgs(t *testing.T) {
	// Simulate substr(col, 1, 2) with constant position/length
	strs := []string{"hello", "world", "abcdef"}
	b := makeStringBatch(strs)
	out := batch.NewVector(batch.TypeString, len(strs))

	constStart := makeConstVector(float64(1), len(strs))
	constLen := makeConstVector(float64(2), len(strs))
	args := []*batch.Vector{b.Columns[0], constStart, constLen}
	vecSubstr(args, out, len(strs))

	want := []string{"he", "wo", "ab"}
	for i, w := range want {
		got := out.BytesData.StringValue(i)
		if got != w {
			t.Errorf("row %d: got %q, want %q", i, got, w)
		}
	}
}

func TestVecUpper_WithNulls(t *testing.T) {
	// Row 1 is null (empty string signals null in makeStringBatch)
	vals := []string{"hello", "", "world"}
	b := makeStringBatch(vals)
	out := batch.NewVector(batch.TypeString, len(vals))
	args := []*batch.Vector{b.Columns[0]}
	vecUpper(args, out, len(vals))

	if out.BytesData.StringValue(0) != "HELLO" {
		t.Errorf("row 0: got %q, want HELLO", out.BytesData.StringValue(0))
	}
	if !out.Nulls.IsNullFast(1) {
		t.Error("row 1: expected null")
	}
	if out.BytesData.StringValue(2) != "WORLD" {
		t.Errorf("row 2: got %q, want WORLD", out.BytesData.StringValue(2))
	}
}

func TestVecReplace(t *testing.T) {
	strs := []string{"hello world", "foo bar baz"}
	b := makeStringBatch(strs)
	n := len(strs)

	old := batch.NewVector(batch.TypeString, n)
	old.BytesData.Set(0, []byte("world"))
	old.BytesData.Set(1, []byte("bar"))

	new := batch.NewVector(batch.TypeString, n)
	new.BytesData.Set(0, []byte("earth"))
	new.BytesData.Set(1, []byte("qux"))

	out := batch.NewVector(batch.TypeString, n)
	args := []*batch.Vector{b.Columns[0], old, new}
	vecReplace(args, out, n)

	want := []string{"hello earth", "foo qux baz"}
	for i, w := range want {
		got := out.BytesData.StringValue(i)
		if got != w {
			t.Errorf("row %d: got %q, want %q", i, got, w)
		}
	}
}

func TestVecAbs(t *testing.T) {
	n := 4
	src := batch.NewVector(batch.TypeFloat64, n)
	src.Float64Data[0] = -3.5
	src.Float64Data[1] = 2.0
	src.Float64Data[2] = 0.0
	src.Float64Data[3] = -1.0

	out := batch.NewVector(batch.TypeFloat64, n)
	vecAbs([]*batch.Vector{src}, out, n)

	want := []float64{3.5, 2.0, 0.0, 1.0}
	for i, w := range want {
		if out.Float64Data[i] != w {
			t.Errorf("row %d: got %f, want %f", i, out.Float64Data[i], w)
		}
	}
}

func TestVecRound(t *testing.T) {
	n := 3
	src := batch.NewVector(batch.TypeFloat64, n)
	src.Float64Data[0] = 3.14159
	src.Float64Data[1] = 2.5
	src.Float64Data[2] = -1.75

	prec := makeConstVector(float64(2), n)
	out := batch.NewVector(batch.TypeFloat64, n)
	vecRound([]*batch.Vector{src, prec}, out, n)

	want := []float64{3.14, 2.5, -1.75}
	for i, w := range want {
		if out.Float64Data[i] != w {
			t.Errorf("row %d: got %f, want %f", i, out.Float64Data[i], w)
		}
	}
}

// TestVecRoundHalfEven holds the vectorized DOUBLE PRECISION rounding
// kernel (#381) to the same half-to-even tie-break as its scalar
// counterpart, fnRoundHalfEven.
func TestVecRoundHalfEven(t *testing.T) {
	n := 5
	src := batch.NewVector(batch.TypeFloat64, n)
	src.Float64Data[0] = 0.5
	src.Float64Data[1] = 1.5
	src.Float64Data[2] = 2.5
	src.Float64Data[3] = -0.5
	src.Float64Data[4] = -1.5

	out := batch.NewVector(batch.TypeFloat64, n)
	vecRoundHalfEven([]*batch.Vector{src}, out, n)

	want := []float64{0, 2, 2, -0, -2}
	for i, w := range want {
		if out.Float64Data[i] != w {
			t.Errorf("row %d: got %f, want %f", i, out.Float64Data[i], w)
		}
	}
}

func TestFuncCall_EvalVec(t *testing.T) {
	// Test the full FuncCall.EvalVec path with upper(s)
	vals := []string{"hello", "WORLD", "test"}
	b := makeStringBatch(vals)

	fc := &FuncCall{
		Name: "upper",
		Args: []Expr{&ColRef{Name: "s"}},
	}

	out := batch.NewVector(batch.TypeString, len(vals))
	fc.EvalVec(b, out, len(vals))

	want := []string{"HELLO", "WORLD", "TEST"}
	for i, w := range want {
		got := out.BytesData.StringValue(i)
		if got != w {
			t.Errorf("row %d: got %q, want %q", i, got, w)
		}
	}
}

func TestFuncCall_EvalVec_WithLitArgs(t *testing.T) {
	// Test substr(s, 1, 2) via FuncCall.EvalVec with Lit args
	vals := []string{"hello", "world", "abcdef"}
	b := makeStringBatch(vals)

	fc := &FuncCall{
		Name: "substr",
		Args: []Expr{
			&ColRef{Name: "s"},
			&Lit{Val: float64(1)},
			&Lit{Val: float64(2)},
		},
	}

	out := batch.NewVector(batch.TypeString, len(vals))
	fc.EvalVec(b, out, len(vals))

	want := []string{"he", "wo", "ab"}
	for i, w := range want {
		got := out.BytesData.StringValue(i)
		if got != w {
			t.Errorf("row %d: got %q, want %q", i, got, w)
		}
	}
}

func TestFuncCall_EvalVec_Fallback(t *testing.T) {
	// Test that EvalVec falls back to per-row for unvectorized functions
	b := makeStringBatch([]string{"hello"})

	// coalesce has no vectorized implementation
	fc := &FuncCall{
		Name: "coalesce",
		Args: []Expr{&ColRef{Name: "s"}},
	}

	out := batch.NewVector(batch.TypeString, 1)
	fc.EvalVec(b, out, 1)

	got := out.BytesData.StringValue(0)
	if got != "hello" {
		t.Errorf("got %q, want hello", got)
	}
}

// --- Benchmarks: vectorized vs per-row ---

func benchStringBatch(n int) *batch.RecordBatch {
	schema := []parquet.Column{
		{Name: "s", Type: parquet.TypeString, Nullable: false},
	}
	b := batch.NewRecordBatch(schema, n)
	strs := []string{"hello world", "foo bar baz", "testing 123", "alpha beta gamma"}
	for i := 0; i < n; i++ {
		b.Columns[0].BytesData.Set(i, []byte(strs[i%len(strs)]))
	}
	return b
}

func BenchmarkUpper_PerRow(b *testing.B) {
	bb := benchStringBatch(2048)
	fc := &FuncCall{
		Name: "upper",
		Args: []Expr{&ColRef{Name: "s"}},
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for row := 0; row < 2048; row++ {
			_ = fc.Eval(bb, row)
		}
	}
}

func BenchmarkUpper_Vectorized(b *testing.B) {
	bb := benchStringBatch(2048)
	fc := &FuncCall{
		Name: "upper",
		Args: []Expr{&ColRef{Name: "s"}},
	}
	out := batch.NewVector(batch.TypeString, 2048)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out.BytesData.Reset()
		fc.EvalVec(bb, out, 2048)
	}
}

func BenchmarkSubstr_PerRow(b *testing.B) {
	bb := benchStringBatch(2048)
	fc := &FuncCall{
		Name: "substr",
		Args: []Expr{
			&ColRef{Name: "s"},
			&Lit{Val: float64(1)},
			&Lit{Val: float64(5)},
		},
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for row := 0; row < 2048; row++ {
			_ = fc.Eval(bb, row)
		}
	}
}

func BenchmarkSubstr_Vectorized(b *testing.B) {
	bb := benchStringBatch(2048)
	fc := &FuncCall{
		Name: "substr",
		Args: []Expr{
			&ColRef{Name: "s"},
			&Lit{Val: float64(1)},
			&Lit{Val: float64(5)},
		},
	}
	out := batch.NewVector(batch.TypeString, 2048)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out.BytesData.Reset()
		fc.EvalVec(bb, out, 2048)
	}
}

func BenchmarkLength_PerRow(b *testing.B) {
	bb := benchStringBatch(2048)
	fc := &FuncCall{
		Name: "length",
		Args: []Expr{&ColRef{Name: "s"}},
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for row := 0; row < 2048; row++ {
			_ = fc.Eval(bb, row)
		}
	}
}

func BenchmarkLength_Vectorized(b *testing.B) {
	bb := benchStringBatch(2048)
	fc := &FuncCall{
		Name: "length",
		Args: []Expr{&ColRef{Name: "s"}},
	}
	out := batch.NewVector(batch.TypeFloat64, 2048)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		fc.EvalVec(bb, out, 2048)
	}
}
