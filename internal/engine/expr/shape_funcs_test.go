package expr

import (
	"fmt"
	"math/rand"
	"strconv"
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
				want func(string) int32
			}{
				// `length` is CHARACTER_LENGTH's kernel now (#856), so it
				// appears under char_length below rather than beside the two
				// byte-counting spellings. There is no vecLength any more.
				{"octet_length", vecOctetLength, func(s string) int32 { return int32(len(s)) }},
				{"bit_length", vecBitLength, func(s string) int32 { return int32(8 * len(s)) }},
				// CHARACTERS over a STRING column and BYTES over a BYTES one:
				// bytea has no characters, so `char_length` answers there
				// what `length` and `octet_length` answer, and answering a
				// rune count for one spelling and a byte count for the other
				// was the engine disagreeing with itself over one value
				// (#583 round 2, B4). PostgreSQL has neither function over
				// bytea — `char_length(bytea)` is 42883 — so this is the
				// text-only family's recorded superset, and a superset owes
				// consistency.
				{"char_length", vecCharLength, func(s string) int32 {
					if tc.typ == parquet.TypeBytes {
						return int32(len(s))
					}
					return int32(utf8.RuneCountInString(s))
				}},
			} {
				t.Run(k.name, func(t *testing.T) {
					// The length family is int4 (RetInt32), so its output
					// vector is Int32-backed (#530).
					out := batch.NewVector(batch.TypeInt32, n)
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
						if got, want := out.Int32Data[i], k.want(vals[i]); got != want {
							t.Fatalf("row %d (%q): got %v want %v", i, vals[i], got, want)
						}
					}
				})
			}
		})
	}
}

// TestVecShapeLenMultibyteSemantics used to PIN the finding that length() here
// was a BYTE count where PostgreSQL's is a character count. That was #856, and
// the pin is the assertion now: LENGTH and CHARACTER_LENGTH are synonyms on the
// server and are one kernel here, while OCTET_LENGTH keeps the byte count.
func TestVecShapeLenMultibyteSemantics(t *testing.T) {
	vals := []string{"日本", "é", "😀", "abc"}
	nulls := make([]bool, len(vals))
	b := shapeBatch(t, "s", parquet.TypeString, vals, nulls)

	// live PostgreSQL 17.11: length('日本') 2, length('é') 1, length('😀') 1.
	wantRunes := []int32{2, 1, 1, 3}
	out := batch.NewVector(batch.TypeInt32, len(vals))
	vecCharLength([]*batch.Vector{b.Columns[0]}, out, len(vals))
	for i := range vals {
		if out.Int32Data[i] != wantRunes[i] {
			t.Fatalf("length(%q): got %v want %v (character semantics)",
				vals[i], out.Int32Data[i], wantRunes[i])
		}
		// The boxed scalar definition must agree — this is the invariant the
		// vectorized kernel is allowed to exist under.
		if got := fnLength([]any{vals[i]}); got != any(wantRunes[i]) {
			t.Fatalf("fnLength(%q): got %v (%T) want %v", vals[i], got, got, wantRunes[i])
		}
	}

	// OCTET_LENGTH keeps the byte count, which is what makes the pair a
	// statement about LENGTH rather than about this fixture.
	outBytes := batch.NewVector(batch.TypeInt32, len(vals))
	vecOctetLength([]*batch.Vector{b.Columns[0]}, outBytes, len(vals))
	wantBytes := []int32{6, 2, 4, 3}
	for i := range vals {
		if outBytes.Int32Data[i] != wantBytes[i] {
			t.Fatalf("octet_length(%q): got %v want %v (byte semantics)",
				vals[i], outBytes.Int32Data[i], wantBytes[i])
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

	out := batch.NewVector(batch.TypeInt32, 3)
	vecCharLength([]*batch.Vector{b.Columns[0]}, out, 3)
	if out.Int32Data[0] != 1 || out.Int32Data[1] != 5 {
		t.Fatalf("length(int) got %v %v want 1 5", out.Int32Data[0], out.Int32Data[1])
	}
	outOctets := batch.NewVector(batch.TypeInt32, 3)
	vecOctetLength([]*batch.Vector{b.Columns[0]}, outOctets, 3)
	if outOctets.Int32Data[0] != 1 || outOctets.Int32Data[1] != 5 {
		t.Fatalf("octet_length(int) got %v %v want 1 5",
			outOctets.Int32Data[0], outOctets.Int32Data[1])
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
		// `length` is NOT here any more: it counts characters (#856) and so has
		// no offsets fast path. The two byte-counting spellings still do.
		for name, mul := range map[string]int{"octet_length": 1, "bit_length": 8} {
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
			// Vectorized fill must agree with the per-row values. The
			// projection materializes int4 (#530), so EvalVec writes an
			// Int32 vector; EvalFloat64Vec keeps the float64 contract its
			// arithmetic callers rely on.
			out := batch.NewVector(batch.TypeInt32, n)
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
				if int32(out.Int32Data[i]) != int32(want) || dst[i] != want {
					t.Fatalf("%s row %d: vec=%v float64vec=%v want %v", name, i, out.Int32Data[i], dst[i], want)
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
		{"SELECT octet_length(s) FROM t", "*expr.ColShapeLen"},
		{"SELECT bit_length(s) FROM t", "*expr.ColShapeLen"},
		// Rune counting keeps the generic call, and `length` is rune counting
		// now — the offsets fast path cannot serve a character count (#856).
		{"SELECT length(s) FROM t", "*expr.numericFuncCall"},
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

// A vec kernel must not die when the output vector cannot hold what it writes.
// `SELECT LENGTH(c) FROM t` typed its projection String, so the projection
// allocated a Bytes output vector, and the Float64Data-writing kernel indexed
// off the end of a zero-length slice — panicking the whole server process,
// every connection with it (#310).
func TestShapeLenKernelsSurviveMismatchedOutput(t *testing.T) {
	src := batch.NewVector(batch.TypeString, 3)
	for i, s := range []string{"abc", "de", ""} {
		src.SetValue(i, s)
	}
	// A Bytes output vector: no Int32Data at all (the type the length
	// family's kernels now write, #530).
	out := batch.NewVector(batch.TypeString, 3)
	if len(out.Int32Data) != 0 {
		t.Skip("string vectors carry Int32Data on this build")
	}

	vecOctetLength([]*batch.Vector{src}, out, 3)
	for i, want := range []int64{3, 2, 0} {
		got := out.GetValue(i)
		if !valueEqualsInt(got, want) {
			t.Fatalf("length row %d = %v (%T), want %d", i, got, got, want)
		}
	}

	out2 := batch.NewVector(batch.TypeString, 3)
	vecCharLength([]*batch.Vector{src}, out2, 3)
	if got := out2.GetValue(0); !valueEqualsInt(got, 3) {
		t.Fatalf("char_length row 0 = %v, want 3", got)
	}
}

func valueEqualsInt(v any, want int64) bool {
	switch n := v.(type) {
	case int64:
		return n == want
	case float64:
		return int64(n) == want
	case string:
		return n == strconv.FormatInt(want, 10)
	}
	return false
}
