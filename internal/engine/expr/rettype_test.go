package expr

import (
	"fmt"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// allOutputTypes is every type a projection can allocate an output vector as.
var allOutputTypes = []batch.TypeID{
	batch.TypeBool, batch.TypeInt32, batch.TypeInt64, batch.TypeFloat32,
	batch.TypeFloat64, batch.TypeString, batch.TypeBytes, batch.TypeTimestamp,
	batch.TypeIPv4, batch.TypeIPv6, batch.TypeCIDR, batch.TypeMAC,
	batch.TypePort, batch.TypeProtocol, batch.TypeDuration, batch.TypeUUID,
	batch.TypeDate, batch.TypeDecimal, batch.TypeArray, batch.TypeRow,
	batch.TypeMap, batch.TypeVector,
}

// probeArgs builds the argument vectors a vec kernel is swept with. Each shape
// is three columns wide because that is the widest kernel (substr); kernels
// that take fewer ignore the tail.
func probeArgs(t *testing.T, rows int) [][]*batch.Vector {
	t.Helper()
	str := func(vals ...string) *batch.Vector {
		v := batch.NewVector(batch.TypeString, rows)
		for i := 0; i < rows; i++ {
			v.SetValue(i, vals[i%len(vals)])
		}
		return v
	}
	i64 := func(vals ...int64) *batch.Vector {
		v := batch.NewVector(batch.TypeInt64, rows)
		for i := 0; i < rows; i++ {
			v.SetValue(i, vals[i%len(vals)])
		}
		return v
	}
	f64 := func(vals ...float64) *batch.Vector {
		v := batch.NewVector(batch.TypeFloat64, rows)
		for i := 0; i < rows; i++ {
			v.SetValue(i, vals[i%len(vals)])
		}
		return v
	}
	ts := func() *batch.Vector {
		v := batch.NewVector(batch.TypeTimestamp, rows)
		for i := 0; i < rows; i++ {
			v.SetValue(i, int64(1705314600000000)+int64(i))
		}
		return v
	}
	vec := func() *batch.Vector {
		v := batch.NewVectorVector(rows, 2)
		for i := 0; i < rows; i++ {
			v.SetValue(i, []float32{float32(i) + 1, 2})
		}
		return v
	}
	return [][]*batch.Vector{
		{str("hello", "world"), str("he", "wo"), str("l", "o")},
		{str("hello", "world"), i64(2), i64(3)},
		{str("2024-01-15 10:30:00"), str("year"), str("month")},
		{f64(2.5, -3.25), f64(2), f64(3)},
		{i64(42, -7), i64(2), i64(3)},
		{ts(), str("year"), i64(1)},
		// extract() takes its unit first, the instant second.
		{str("year"), ts(), i64(1)},
		{vec(), vec(), i64(1)},
	}
}

// Every registered vec function, evaluated into an output vector of every
// type, must not panic. A kernel writes a typed slice of its output vector
// directly (out.Float64Data[i], out.BoolData[i]) and the planner picks that
// vector's type from the declared return type — when the two disagreed the
// write ran off the end of a zero-length slice and killed the whole server
// process, for every connection, four times over (#310).
//
// The declaration removes the disagreement at the planner; this sweep removes
// the assumption that the planner is the only way one can arise. It goes
// through FuncCall.EvalVec, which is the only path the engine ever calls a
// kernel by (see plan.go's pc.VecEval and project.go).
func TestVecFuncsSurviveEveryOutputType(t *testing.T) {
	const rows = 4
	shapes := probeArgs(t, rows)

	evalPanic := func(name string, args []*batch.Vector, outType batch.TypeID) (msg string) {
		defer func() {
			if r := recover(); r != nil {
				msg = fmt.Sprint(r)
			}
		}()
		b, call := probeCall(name, args, rows)
		call.EvalVec(b, newOutVector(outType, rows), rows)
		return ""
	}

	var failures, argShape []string
	for _, name := range vecFuncNames() {
		declared, c := DefaultRegistry.ReturnType(name).Resolve(0, nil)
		if c == Undecided {
			continue // reported by TestVecKernelWritesDeclaredType
		}
		for si, args := range shapes {
			// Does this function accept these ARGUMENT vectors at all? Judge
			// that with the output vector it declares, so the two axes stay
			// separate: an input the kernel cannot read is a different
			// finding from an output vector it cannot write.
			if msg := evalPanic(name, args, declared); msg != "" {
				argShape = append(argShape, fmt.Sprintf("%s(shape %d): %s", name, si, msg))
				continue
			}
			for _, outType := range allOutputTypes {
				if msg := evalPanic(name, args, outType); msg != "" {
					failures = append(failures, fmt.Sprintf(
						"%s: out=%s shape=%d panicked: %s", name, outType, si, msg))
				}
			}
		}
	}
	if len(failures) > 0 {
		t.Errorf("%d vec function panics on a mismatched output vector:", len(failures))
		for _, f := range failures {
			t.Errorf("  %s", f)
		}
	}
	if len(argShape) > 0 {
		// The input-side twin of this bug class, and not what the declared
		// return type fixes: a kernel reading BytesData of an argument that
		// holds ints. FuncCall.EvalVec routes argument 0 of a string-input
		// function to the per-row path, which covers UPPER(int_col); an
		// argument PAST the first still reaches the kernel, because nothing
		// declares which positions are read as text (`contains(c, 2)`).
		// Closing it wants argument types declared the way return types now
		// are. Reported, not enforced.
		t.Logf("%d argument shapes a kernel cannot read (input-side, see comment):", len(argShape))
		for _, f := range argShape {
			t.Logf("  %s", f)
		}
	}
}

// The same sweep against the raw kernels, which are NOT individually
// defensive: the guard lives once, at the dispatch seam in FuncCall.EvalVec,
// rather than at the top of each of the kernels. This test does not fail on
// that — it reports which kernels rely on the seam, so the count is visible
// if someone ever calls a kernel directly.
func TestRawVecKernelsRelyOnTheDispatchGuard(t *testing.T) {
	const rows = 4
	shapes := probeArgs(t, rows)

	unguarded := map[string]int{}
	for _, name := range vecFuncNames() {
		fn := DefaultRegistry.LookupVec(name)
		for _, outType := range allOutputTypes {
			for _, args := range shapes {
				func() {
					defer func() {
						if recover() != nil {
							unguarded[name]++
						}
					}()
					fn(args, newOutVector(outType, rows), rows)
				}()
			}
		}
	}
	if len(unguarded) == 0 {
		return
	}
	names := make([]string, 0, len(unguarded))
	for n := range unguarded {
		names = append(names, n)
	}
	sort.Strings(names)
	t.Logf("%d/%d raw kernels write their typed slice unconditionally and are safe "+
		"only through FuncCall.EvalVec's guard: %v", len(names), len(vecFuncNames()), names)
}

// The declared return type must name the slice the vec kernel actually writes.
// This is the link the old hand-maintained isNumericFunc list did not have:
// there, a kernel writing Float64Data and a planner declaring String were
// separate facts that nothing compared.
func TestVecKernelWritesDeclaredType(t *testing.T) {
	const rows = 4
	shapes := probeArgs(t, rows)

	for _, name := range vecFuncNames() {
		ret := DefaultRegistry.ReturnType(name)
		declared, c := ret.Resolve(0, nil)
		if c == Undecided {
			t.Errorf("%s: has a vec kernel but a %s declaration — a kernel writes a "+
				"typed slice, so its function's return type cannot be undecided", name, ret)
			continue
		}
		produced := false
		for _, args := range shapes {
			func() {
				// An argument shape this kernel cannot read is a separate
				// finding; TestVecFuncsSurviveEveryOutputType reports those.
				defer func() { _ = recover() }()
				out := newOutVector(declared, rows)
				DefaultRegistry.LookupVec(name)(args, out, rows)
				for i := 0; i < rows; i++ {
					v := out.GetValue(i)
					if v == nil {
						continue
					}
					produced = true
					if !valueFitsType(v, declared) {
						t.Errorf("%s declares %s but its vec kernel produced %T (%v)",
							name, declared, v, v)
					}
				}
			}()
			if produced {
				break
			}
		}
		if !produced {
			// Not a failure: the probe matrix does not reach every kernel
			// (embed() needs a provider). Say so rather than pretend.
			t.Logf("%s: no probe input produced a value; only the panic sweep covers it", name)
		}
	}
}

// Every registered function declares a return type, and no declaration
// outlives its function. Register's signature already makes omission a
// compile error and the zero value a panic at init; this catches a registry
// mutated by any other route.
func TestEveryRegisteredFuncDeclaresAReturnType(t *testing.T) {
	names := DefaultRegistry.Names()
	for _, name := range names {
		if ret := DefaultRegistry.ReturnType(name); !ret.Declared() {
			t.Errorf("%s is registered with no declared return type", name)
		}
	}
	DefaultRegistry.mu.RLock()
	nfuncs, nrets := len(DefaultRegistry.funcs), len(DefaultRegistry.rets)
	DefaultRegistry.mu.RUnlock()
	if nfuncs != nrets {
		t.Errorf("%d functions but %d declarations — a declaration outlived its function",
			nfuncs, nrets)
	}
	// Every vec kernel accelerates a registered function.
	for _, name := range vecFuncNames() {
		if !DefaultRegistry.Has(name) {
			t.Errorf("vec kernel %q has no scalar function registered", name)
		}
	}
}

// Registering without a declaration is loud: the signature stops the common
// case at compile time, and a field-named literal that skips the type leaves
// the zero value, which panics here rather than reaching a kernel.
func TestRegisterRejectsUndeclaredReturnType(t *testing.T) {
	r := NewFuncRegistry()
	defer func() {
		if recover() == nil {
			t.Fatal("registering with an undeclared return type did not panic")
		}
	}()
	r.Register("undeclared_func", func(args []any) any { return nil }, Ret{})
}

// A vec kernel may not be registered for a function whose return type is not
// declared yet — the kernel would be writing a typed slice chosen by nothing.
func TestRegisterVecRequiresADeclaration(t *testing.T) {
	r := NewFuncRegistry()
	defer func() {
		if recover() == nil {
			t.Fatal("registering a vec kernel for an undeclared function did not panic")
		}
	}()
	r.RegisterVec("nosuchfunc", func(args []*batch.Vector, out *batch.Vector, n int) {})
}

func TestRetSameAsArgResolution(t *testing.T) {
	known := map[int]batch.TypeID{1: batch.TypeString}
	argType := func(i int) (batch.TypeID, Confidence) {
		t, ok := known[i]
		if !ok {
			return 0, Undecided
		}
		return t, Decided
	}
	tests := []struct {
		name  string
		ret   Ret
		nargs int
		want  batch.TypeID
		wantC Confidence
	}{
		{"fixed", RetFloat64, 0, batch.TypeFloat64, Decided},
		{"dynamic", RetDynamic, 2, batch.TypeString, Undecided},
		{"any arg, second decides", RetSameAsArg(batch.TypeFloat64), 3, batch.TypeString, Decided},
		// Nothing decided, so the fallback is the answer — and it is
		// reported as the guess it is.
		{"any arg, none decides", RetSameAsArg(batch.TypeFloat64), 1, batch.TypeFloat64, Guessed},
		{"named arg misses", RetSameAsArg(batch.TypeFloat64, 0), 3, batch.TypeFloat64, Guessed},
		{"named arg hits", RetSameAsArg(batch.TypeFloat64, 1, 2), 3, batch.TypeString, Decided},
		{"arg out of range", RetSameAsArg(batch.TypeFloat64, 1), 1, batch.TypeFloat64, Guessed},
	}
	for _, tc := range tests {
		got, c := tc.ret.Resolve(tc.nargs, argType)
		if got != tc.want || c != tc.wantC {
			t.Errorf("%s: Resolve = (%s, %s), want (%s, %s)", tc.name, got, c, tc.want, tc.wantC)
		}
	}
}

// A nested call that only GUESSED its own type must not stop its caller's
// search. This is #331 at the declaration layer: coalesce asked nullif,
// nullif had a bare column in the position it mirrors, and the numeric
// fallback it answered with was taken for a decision — so the string literal
// in the next argument, the one that knew, was never asked and every row of
// COALESCE(NULLIF(n_name,'ALGERIA'), 'fallback') came back as the integer 0.
func TestRetSameAsArgGuessDoesNotOutrankADecision(t *testing.T) {
	// The two declarations at play, and the argument shapes a caller sees:
	// undecided (a bare column reference), decided (a literal), or the
	// answer of a nested call, resolved recursively.
	nullif := RetSameAsArg(batch.TypeFloat64, 0)
	coalesce := RetSameAsArg(batch.TypeFloat64)
	ifnull := RetSameAsArg(batch.TypeString, 0, 1)

	undecided := func(batch.TypeID) func(int) (batch.TypeID, Confidence) {
		return func(int) (batch.TypeID, Confidence) { return 0, Undecided }
	}
	// nested resolves ret over the argument shapes given, exactly as the
	// planner's nodeDeclaredType does when an argument is itself a call.
	nested := func(ret Ret, args ...func(int) (batch.TypeID, Confidence)) func(int) (batch.TypeID, Confidence) {
		return func(int) (batch.TypeID, Confidence) {
			return ret.Resolve(len(args), func(i int) (batch.TypeID, Confidence) { return args[i](i) })
		}
	}
	lit := func(t batch.TypeID) func(int) (batch.TypeID, Confidence) {
		return func(int) (batch.TypeID, Confidence) { return t, Decided }
	}
	col := undecided(0)

	tests := []struct {
		name  string
		ret   Ret
		args  []func(int) (batch.TypeID, Confidence)
		want  batch.TypeID
		wantC Confidence
	}{
		{
			// The bug. COALESCE(NULLIF(<col>, 'lit'), 'lit')
			name:  "later argument decides past a nested guess",
			ret:   coalesce,
			args:  []func(int) (batch.TypeID, Confidence){nested(nullif, col, lit(batch.TypeString)), lit(batch.TypeString)},
			want:  batch.TypeString,
			wantC: Decided,
		},
		{
			// Two levels: COALESCE(NULLIF(NULLIF(<col>,'a'),'b'), 'lit').
			// A guess stays a guess however deep it is made.
			name: "two levels of nesting, outer argument decides",
			ret:  coalesce,
			args: []func(int) (batch.TypeID, Confidence){
				nested(nullif, nested(nullif, col, lit(batch.TypeString)), lit(batch.TypeString)),
				lit(batch.TypeString),
			},
			want:  batch.TypeString,
			wantC: Decided,
		},
		{
			// Nothing anywhere decides: the fallback still answers, so
			// today's behaviour at top level is unchanged.
			name:  "nested guess, nothing else to consult",
			ret:   coalesce,
			args:  []func(int) (batch.TypeID, Confidence){nested(nullif, col, col)},
			want:  batch.TypeFloat64,
			wantC: Guessed,
		},
		{
			// Only guesses among the candidates: the first one wins, in the
			// declaration's preference order.
			name: "first guess wins when no candidate decides",
			ret:  coalesce,
			args: []func(int) (batch.TypeID, Confidence){
				nested(RetSameAsArg(batch.TypeBool, 0), col),
				nested(nullif, col),
			},
			want:  batch.TypeBool,
			wantC: Guessed,
		},
		{
			// NULLIF(<int col>, 1) at top level: nothing decides, and the
			// numeric fallback is the right answer. Deleting it — the naive
			// "fix" for the case above — would type this projection String
			// and hand its kernel a vector with no Float64Data.
			name:  "numeric fallback survives at top level",
			ret:   nullif,
			args:  []func(int) (batch.TypeID, Confidence){col, lit(batch.TypeInt64)},
			want:  batch.TypeFloat64,
			wantC: Guessed,
		},
		{
			// The same nullif nested under a numeric caller: the caller's
			// own numeric literal decides, and the answer stays numeric.
			name:  "numeric stays numeric through a nested guess",
			ret:   coalesce,
			args:  []func(int) (batch.TypeID, Confidence){nested(nullif, col, lit(batch.TypeInt64)), lit(batch.TypeInt64)},
			want:  batch.TypeInt64,
			wantC: Decided,
		},
		{
			// ifnull mirrors arguments 0 and 1, so the guess in 0 must not
			// cost it the decision in 1 either.
			name:  "declared candidate list skips a guess too",
			ret:   ifnull,
			args:  []func(int) (batch.TypeID, Confidence){nested(nullif, col), lit(batch.TypeString)},
			want:  batch.TypeString,
			wantC: Decided,
		},
	}
	for _, tc := range tests {
		got, c := tc.ret.Resolve(len(tc.args), func(i int) (batch.TypeID, Confidence) { return tc.args[i](i) })
		if got != tc.want || c != tc.wantC {
			t.Errorf("%s: Resolve = (%s, %s), want (%s, %s)", tc.name, got, c, tc.want, tc.wantC)
		}
	}
}

// newOutVector builds an output vector of the given type, sized for rows.
func newOutVector(t batch.TypeID, rows int) *batch.Vector {
	if t == batch.TypeVector {
		return batch.NewVectorVector(rows, 2)
	}
	return batch.NewVector(t, rows)
}

// probeCall wraps argument vectors in a batch and a FuncCall over them, which
// is how the engine reaches a vec kernel.
func probeCall(name string, args []*batch.Vector, rows int) (*batch.RecordBatch, *FuncCall) {
	b := &batch.RecordBatch{Len: rows}
	call := &FuncCall{Name: name}
	for i, v := range args {
		col := fmt.Sprintf("a%d", i)
		b.Schema = append(b.Schema, parquet.Column{Name: col, Type: v.Type, Nullable: true})
		b.Columns = append(b.Columns, v)
		call.Args = append(call.Args, &ColRef{Name: col})
	}
	return b, call
}

func vecFuncNames() []string {
	DefaultRegistry.mu.RLock()
	names := make([]string, 0, len(DefaultRegistry.vecFuncs))
	for n := range DefaultRegistry.vecFuncs {
		names = append(names, n)
	}
	DefaultRegistry.mu.RUnlock()
	sort.Strings(names)
	return names
}

// valueFitsType reports whether a boxed value read back out of a vector of
// type t is the kind of value that vector stores.
func valueFitsType(v any, t batch.TypeID) bool {
	switch t {
	case batch.TypeBool:
		_, ok := v.(bool)
		return ok
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		switch v.(type) {
		case int32, int64:
			return true
		}
		return false
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeDuration:
		_, ok := v.(int64)
		return ok
	case batch.TypeFloat32:
		_, ok := v.(float32)
		return ok
	case batch.TypeFloat64:
		_, ok := v.(float64)
		return ok
	case batch.TypeString:
		switch v.(type) {
		case string, []byte:
			return true
		}
		return false
	case batch.TypeBytes:
		_, ok := v.([]byte)
		return ok
	case batch.TypeVector:
		_, ok := v.([]float32)
		return ok
	}
	// Types no vec kernel declares today: the sweep above still covers them.
	return true
}
