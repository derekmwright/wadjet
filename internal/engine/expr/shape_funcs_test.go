package expr

import (
	"fmt"
	"math/rand"
	"testing"
	"unicode/utf8"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// shapeCorpus is the randomized input the offsets paths are checked
// against: empty strings, NULLs, multibyte UTF-8 (where byte length and
// rune length diverge — the semantics that the offsets path must NOT
// silently change), and long ASCII values.
func shapeCorpus(t testing.TB, n int) (vals []string, nulls []bool) {
	t.Helper()
	rng := rand.New(rand.NewSource(0x5EED))
	alphabets := []string{
		"abcdefghijklmnopqrstuvwxyz",
		"héllo wörld ünïcode",        // 2-byte runes
		"日本語テキスト",                    // 3-byte runes
		"emoji 😀🎉 mixed",             // 4-byte runes
		"/path/to/some/url?q=1&x=yz", // URL-ish, the Q28 shape
	}
	vals = make([]string, n)
	nulls = make([]bool, n)
	for i := range vals {
		switch rng.Intn(10) {
		case 0:
			nulls[i] = true
		case 1:
			vals[i] = "" // empty, not null
		default:
			a := []rune(alphabets[rng.Intn(len(alphabets))])
			l := rng.Intn(len(a)) + 1
			start := rng.Intn(len(a)-l+1) + 0
			vals[i] = string(a[start : start+l])
		}
	}
	return vals, nulls
}

// shapeBatch builds a one-column batch with explicit null control (the
// vec_test helper conflates "" with NULL, which is exactly the distinction
// under test here).
func shapeBatch(t testing.TB, name string, typ parquet.TypeID, vals []string, nulls []bool) *batch.RecordBatch {
	t.Helper()
	schema := []parquet.Column{{Name: name, Type: typ, Nullable: true}}
	b := batch.NewRecordBatch(schema, len(vals))
	for i, v := range vals {
		if nulls[i] {
			b.Columns[0].Nulls.SetNull(i)
			b.Columns[0].BytesData.Set(i, nil)
			continue
		}
		b.Columns[0].BytesData.Set(i, []byte(v))
	}
	return b
}

// --- kernel parity: offsets path vs the boxed byte path ---

func TestVecShapeLenParity(t *testing.T) {
	const n = 512
	vals, nulls := shapeCorpus(t, n)
	for _, tc := range []struct {
		name string
		typ  parquet.TypeID
	}{
		{"string", parquet.TypeString},
		{"bytes", parquet.TypeBytes},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := shapeBatch(t, "s", tc.typ, vals, nulls)
			for _, k := range []struct {
				name string
				fn   VecScalarFunc
				want func(string) float64
			}{
				{"length", vecLength, func(s string) float64 { return float64(len(s)) }},
				{"octet_length", vecOctetLength, func(s string) float64 { return float64(len(s)) }},
				{"bit_length", vecBitLength, func(s string) float64 { return float64(8 * len(s)) }},
				{"char_length", vecCharLength, func(s string) float64 {
					return float64(utf8.RuneCountInString(s))
				}},
			} {
				t.Run(k.name, func(t *testing.T) {
					out := batch.NewVector(batch.TypeFloat64, n)
					k.fn([]*batch.Vector{b.Columns[0]}, out, n)
					for i := 0; i < n; i++ {
						if nulls[i] {
							if !out.Nulls.IsNull(i) {
								t.Fatalf("row %d: NULL input produced non-null output", i)
							}
							continue
						}
						if out.Nulls.IsNull(i) {
							t.Fatalf("row %d: non-null input produced NULL output", i)
						}
						if got, want := out.Float64Data[i], k.want(vals[i]); got != want {
							t.Fatalf("row %d (%q): got %v want %v", i, vals[i], got, want)
						}
					}
				})
			}
		})
	}
}

// TestVecShapeLenMultibyteSemantics pins the finding that length() here is a
// BYTE count, not PostgreSQL's character count, and that the offsets path
// did not change it.
func TestVecShapeLenMultibyteSemantics(t *testing.T) {
	vals := []string{"日本", "é", "😀", "abc"}
	nulls := make([]bool, len(vals))
	b := shapeBatch(t, "s", parquet.TypeString, vals, nulls)

	out := batch.NewVector(batch.TypeFloat64, len(vals))
	vecLength([]*batch.Vector{b.Columns[0]}, out, len(vals))
	wantBytes := []float64{6, 2, 4, 3}
	for i := range vals {
		if out.Float64Data[i] != wantBytes[i] {
			t.Fatalf("length(%q): got %v want %v (byte semantics)", vals[i], out.Float64Data[i], wantBytes[i])
		}
		// The boxed scalar definition must agree — this is the invariant the
		// offsets fast path is allowed to exist under.
		if got := fnLength([]any{vals[i]}); got != any(wantBytes[i]) {
			t.Fatalf("fnLength(%q): got %v want %v", vals[i], got, wantBytes[i])
		}
	}

	outRunes := batch.NewVector(batch.TypeFloat64, len(vals))
	vecCharLength([]*batch.Vector{b.Columns[0]}, outRunes, len(vals))
	wantRunes := []float64{2, 1, 1, 3}
	for i := range vals {
		if outRunes.Float64Data[i] != wantRunes[i] {
			t.Fatalf("char_length(%q): got %v want %v (rune semantics)", vals[i], outRunes.Float64Data[i], wantRunes[i])
		}
	}
}

// TestVecShapeLenNonByteArray covers the input shape that used to index a
// nil Offsets slice: length() of a numeric column.
func TestVecShapeLenNonByteArray(t *testing.T) {
	schema := []parquet.Column{{Name: "n", Type: parquet.TypeInt64, Nullable: true}}
	b := batch.NewRecordBatch(schema, 3)
	b.Columns[0].Int64Data[0] = 7
	b.Columns[0].Int64Data[1] = 12345
	b.Columns[0].Nulls.SetNull(2)

	out := batch.NewVector(batch.TypeFloat64, 3)
	vecLength([]*batch.Vector{b.Columns[0]}, out, 3)
	if out.Float64Data[0] != 1 || out.Float64Data[1] != 5 {
		t.Fatalf("length(int) got %v %v want 1 5", out.Float64Data[0], out.Float64Data[1])
	}
	if !out.Nulls.IsNull(2) {
		t.Error("NULL int should yield NULL length")
	}
}

// --- typed node parity: ColShapeLen vs the generic FuncCall it replaces ---

func TestColShapeLenParityAgainstGeneric(t *testing.T) {
	const n = 512
	vals, nulls := shapeCorpus(t, n)
	for _, typ := range []parquet.TypeID{parquet.TypeString, parquet.TypeBytes} {
		b := shapeBatch(t, "s", typ, vals, nulls)
		for name, mul := range map[string]int{"length": 1, "octet_length": 1, "bit_length": 8} {
			fast := &ColShapeLen{
				Col:      &ColRef{Name: "s"},
				Mul:      mul,
				Fallback: &FuncCall{Name: name, Args: []Expr{&ColRef{Name: "s"}}},
			}
			generic := &numericFuncCall{&FuncCall{Name: name, Args: []Expr{&ColRef{Name: "s"}}}}
			for i := 0; i < n; i++ {
				gf, gok := generic.EvalFloat64(b, i)
				ff, fok := fast.EvalFloat64(b, i)
				if gok != fok || (gok && gf != ff) {
					t.Fatalf("%s type=%v row %d (%q null=%v): fast=(%v,%v) generic=(%v,%v)",
						name, typ, i, vals[i], nulls[i], ff, fok, gf, gok)
				}
				if gv, fv := generic.Eval(b, i), fast.Eval(b, i); gv != fv {
					t.Fatalf("%s boxed Eval row %d: fast=%v generic=%v", name, i, fv, gv)
				}
			}
			// Vectorized fill must agree with the per-row values.
			out := batch.NewVector(batch.TypeFloat64, n)
			fast.EvalVec(b, out, n)
			dst := make([]float64, n)
			anyNull := fast.EvalFloat64Vec(b, dst, n)
			if anyNull != hasAnyNull(nulls) {
				t.Fatalf("%s: EvalFloat64Vec anyNull=%v want %v", name, anyNull, hasAnyNull(nulls))
			}
			for i := 0; i < n; i++ {
				want, ok := fast.EvalFloat64(b, i)
				if !ok {
					if !out.Nulls.IsNull(i) {
						t.Fatalf("%s: EvalVec row %d should be NULL", name, i)
					}
					continue
				}
				if out.Float64Data[i] != want || dst[i] != want {
					t.Fatalf("%s row %d: vec=%v float64vec=%v want %v", name, i, out.Float64Data[i], dst[i], want)
				}
			}
		}
	}
}

func hasAnyNull(nulls []bool) bool {
	for _, n := range nulls {
		if n {
			return true
		}
	}
	return false
}

// TestColShapeLenFallsBackForNonByteColumns pins that anything whose stored
// bytes are not the value ColRef.Eval boxes keeps the generic definition.
func TestColShapeLenFallsBackForNonByteColumns(t *testing.T) {
	schema := []parquet.Column{
		{Name: "d", Type: parquet.TypeDate, Nullable: true},
		{Name: "i", Type: parquet.TypeInt64, Nullable: true},
	}
	b := batch.NewRecordBatch(schema, 2)
	b.Columns[0].Int32Data[0] = 0 // 1970-01-01
	b.Columns[0].Int32Data[1] = 1
	b.Columns[1].Int64Data[0] = 1234
	b.Columns[1].Int64Data[1] = 5

	for _, col := range []string{"d", "i"} {
		fast := &ColShapeLen{
			Col:      &ColRef{Name: col},
			Mul:      1,
			Fallback: &FuncCall{Name: "length", Args: []Expr{&ColRef{Name: col}}},
		}
		generic := &numericFuncCall{&FuncCall{Name: "length", Args: []Expr{&ColRef{Name: col}}}}
		for i := 0; i < 2; i++ {
			gf, gok := generic.EvalFloat64(b, i)
			ff, fok := fast.EvalFloat64(b, i)
			if gok != fok || gf != ff {
				t.Fatalf("col %s row %d: fast=(%v,%v) generic=(%v,%v)", col, i, ff, fok, gf, gok)
			}
		}
	}
	// DATE specifically must still render ISO-8601 before measuring (the
	// stringInputFuncs contract from issue #273): length('1970-01-01') = 10.
	fast := &ColShapeLen{
		Col:      &ColRef{Name: "d"},
		Mul:      1,
		Fallback: &FuncCall{Name: "length", Args: []Expr{&ColRef{Name: "d"}}},
	}
	if v, ok := fast.EvalFloat64(b, 0); !ok || v != 10 {
		t.Fatalf("length(date) = (%v,%v), want (10,true)", v, ok)
	}
}

// TestColShapeLenMissingColumn covers the unresolved-column case: the
// generic FuncCall returns NULL, so the fast node must too.
func TestColShapeLenMissingColumn(t *testing.T) {
	b := shapeBatch(t, "s", parquet.TypeString, []string{"abc"}, []bool{false})
	fast := &ColShapeLen{
		Col:      &ColRef{Name: "nope"},
		Mul:      1,
		Fallback: &FuncCall{Name: "length", Args: []Expr{&ColRef{Name: "nope"}}},
	}
	if v, ok := fast.EvalFloat64(b, 0); ok {
		t.Fatalf("missing column should be NULL, got %v", v)
	}
	if v := fast.Eval(b, 0); v != nil {
		t.Fatalf("missing column Eval should be nil, got %v", v)
	}
}

// --- empty-string comparison ---

func TestColEmptyStrParity(t *testing.T) {
	const n = 512
	vals, nulls := shapeCorpus(t, n)
	b := shapeBatch(t, "s", parquet.TypeString, vals, nulls)

	for _, not := range []bool{false, true} {
		op := CmpEq
		if not {
			op = CmpNe
		}
		generic := &Cmp{Left: &ColRef{Name: "s"}, Right: &Lit{Val: ""}, Op: op}
		fast := &ColEmptyStr{
			Col:      &ColRef{Name: "s"},
			Not:      not,
			Fallback: &Cmp{Left: &ColRef{Name: "s"}, Right: &Lit{Val: ""}, Op: op},
		}
		for i := 0; i < n; i++ {
			if g, f := generic.EvalBool(b, i), fast.EvalBool(b, i); g != f {
				t.Fatalf("not=%v row %d (%q null=%v): fast=%v generic=%v", not, i, vals[i], nulls[i], f, g)
			}
		}
	}
}

// TestColEmptyStrNullIsFalseBothWays pins SQL's three-valued behavior as
// this engine implements it in Cmp: a NULL operand is false for = and <>.
func TestColEmptyStrNullIsFalseBothWays(t *testing.T) {
	b := shapeBatch(t, "s", parquet.TypeString, []string{"", "x"}, []bool{true, false})
	eq := &ColEmptyStr{Col: &ColRef{Name: "s"}, Fallback: &Cmp{Left: &ColRef{Name: "s"}, Right: &Lit{Val: ""}, Op: CmpEq}}
	ne := &ColEmptyStr{Col: &ColRef{Name: "s"}, Not: true, Fallback: &Cmp{Left: &ColRef{Name: "s"}, Right: &Lit{Val: ""}, Op: CmpNe}}
	if eq.EvalBool(b, 0) || ne.EvalBool(b, 0) {
		t.Error("NULL must compare false against the empty string for both = and <>")
	}
	if eq.EvalBool(b, 1) || !ne.EvalBool(b, 1) {
		t.Error("non-empty value comparison wrong")
	}
}

// TestColEmptyStrFallsBackForBytes: TypeBytes boxes []byte, which reaches a
// different branch of compare(); the fast node must defer.
func TestColEmptyStrFallsBackForBytes(t *testing.T) {
	vals := []string{"", "x"}
	b := shapeBatch(t, "s", parquet.TypeBytes, vals, []bool{false, false})
	for _, not := range []bool{false, true} {
		op := CmpEq
		if not {
			op = CmpNe
		}
		generic := &Cmp{Left: &ColRef{Name: "s"}, Right: &Lit{Val: ""}, Op: op}
		fast := &ColEmptyStr{
			Col:      &ColRef{Name: "s"},
			Not:      not,
			Fallback: &Cmp{Left: &ColRef{Name: "s"}, Right: &Lit{Val: ""}, Op: op},
		}
		for i := range vals {
			if g, f := generic.EvalBool(b, i), fast.EvalBool(b, i); g != f {
				t.Fatalf("bytes not=%v row %d: fast=%v generic=%v", not, i, f, g)
			}
		}
	}
}

// --- IS NULL ---

func TestColIsNullParity(t *testing.T) {
	const n = 256
	vals, nulls := shapeCorpus(t, n)
	for _, typ := range []parquet.TypeID{parquet.TypeString, parquet.TypeBytes} {
		b := shapeBatch(t, "s", typ, vals, nulls)
		for _, not := range []bool{false, true} {
			generic := &IsNull{Operand: &ColRef{Name: "s"}, Not: not}
			fast := &ColIsNull{
				Col:      &ColRef{Name: "s"},
				Not:      not,
				Fallback: &IsNull{Operand: &ColRef{Name: "s"}, Not: not},
			}
			for i := 0; i < n; i++ {
				if g, f := generic.EvalBool(b, i), fast.EvalBool(b, i); g != f {
					t.Fatalf("type=%v not=%v row %d (null=%v): fast=%v generic=%v", typ, not, i, nulls[i], f, g)
				}
			}
		}
	}
}

func TestColIsNullFallsBackForOtherTypes(t *testing.T) {
	schema := []parquet.Column{{Name: "i", Type: parquet.TypeInt64, Nullable: true}}
	b := batch.NewRecordBatch(schema, 2)
	b.Columns[0].Int64Data[0] = 5
	b.Columns[0].Nulls.SetNull(1)
	fast := &ColIsNull{Col: &ColRef{Name: "i"}, Fallback: &IsNull{Operand: &ColRef{Name: "i"}}}
	generic := &IsNull{Operand: &ColRef{Name: "i"}}
	for i := 0; i < 2; i++ {
		if g, f := generic.EvalBool(b, i), fast.EvalBool(b, i); g != f {
			t.Fatalf("row %d: fast=%v generic=%v", i, f, g)
		}
	}
}

// --- compile-time wiring ---

func TestCompileShapeSpecializations(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		want string
	}{
		{"SELECT length(s) FROM t", "*expr.ColShapeLen"},
		{"SELECT octet_length(s) FROM t", "*expr.ColShapeLen"},
		{"SELECT bit_length(s) FROM t", "*expr.ColShapeLen"},
		// Rune counting keeps the generic call.
		{"SELECT char_length(s) FROM t", "*expr.numericFuncCall"},
		// Non-column argument keeps the generic call.
		{"SELECT length(upper(s)) FROM t", "*expr.numericFuncCall"},
	} {
		e := compileSelectCol(t, tc.sql)
		if got := fmt.Sprintf("%T", e); got != tc.want {
			t.Errorf("%s compiled to %s, want %s", tc.sql, got, tc.want)
		}
	}
}

func TestCompileShapePredicateSpecializations(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		want string
	}{
		{"SELECT 1 FROM t WHERE s = ''", "*expr.ColEmptyStr"},
		{"SELECT 1 FROM t WHERE s <> ''", "*expr.ColEmptyStr"},
		{"SELECT 1 FROM t WHERE '' = s", "*expr.ColEmptyStr"},
		{"SELECT 1 FROM t WHERE s IS NULL", "*expr.ColIsNull"},
		{"SELECT 1 FROM t WHERE s IS NOT NULL", "*expr.ColIsNull"},
		// Non-empty literal keeps the generic comparison.
		{"SELECT 1 FROM t WHERE s = 'x'", "*expr.Cmp"},
		// Ordering comparisons against the empty string are not shape tests.
		{"SELECT 1 FROM t WHERE s > ''", "*expr.Cmp"},
		// IS NULL over an expression keeps the generic node.
		{"SELECT 1 FROM t WHERE upper(s) IS NULL", "*expr.IsNull"},
	} {
		e := compileWhere(t, tc.sql)
		if got := fmt.Sprintf("%T", e); got != tc.want {
			t.Errorf("%s compiled to %s, want %s", tc.sql, got, tc.want)
		}
	}
}
