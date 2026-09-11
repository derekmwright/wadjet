package expr

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// THE TCP FLAG FAMILY AGREES WITH POSTGRESQL'S BIT ARITHMETIC (#966).
//
// Every expectation below is the answer PostgreSQL 17.11 printed for the
// EQUIVALENT bit spelling on the shared oracle server, over
// `a2_flows(f4 int, f8 bigint)` holding 0, 2, 18, 16, 4, 511, 24, NULL, 20, 256:
//
//	tcp_flags_has_all(f, 'SYN','ACK')   ==  (f & 18) = 18
//	tcp_flags_has_any(f, 'SYN','ACK')   ==  (f & 18) <> 0
//	tcp_flags_has_none(f, 'SYN','ACK')  ==  (f & 18) = 0
//
// The table is written as the PG transcript, not as a re-derivation: a test
// that recomputes `f & 18` in Go to check `f & 18` proves only that Go's & is
// associative with itself.
func TestTheTCPFlagPredicatesAreTheBitArithmetic(t *testing.T) {
	all := DefaultRegistry.Lookup("tcp_flags_has_all")
	any_ := DefaultRegistry.Lookup("tcp_flags_has_any")
	none := DefaultRegistry.Lookup("tcp_flags_has_none")

	// PostgreSQL 17.11, `SELECT id, f4, (f4&18)=18, (f4&18)<>0, (f4&18)=0`.
	for _, tc := range []struct {
		flags            any
		wantAll, wantAny bool
		wantNone         bool
		null             bool
	}{
		{flags: int64(0), wantAll: false, wantAny: false, wantNone: true},
		{flags: int64(2), wantAll: false, wantAny: true, wantNone: false},
		{flags: int64(18), wantAll: true, wantAny: true, wantNone: false},
		{flags: int64(16), wantAll: false, wantAny: true, wantNone: false},
		{flags: int64(4), wantAll: false, wantAny: false, wantNone: true},
		{flags: int64(511), wantAll: true, wantAny: true, wantNone: false},
		{flags: int64(24), wantAll: false, wantAny: true, wantNone: false},
		{flags: int64(20), wantAll: false, wantAny: true, wantNone: false},
		{flags: int64(256), wantAll: false, wantAny: false, wantNone: true},
		// PG: (-1) & 18 = 18. A negative value is a bit pattern.
		{flags: int64(-1), wantAll: true, wantAny: true, wantNone: false},
		// PG: NULL & 18 is NULL, so all three are NULL — not FALSE, and in
		// particular has_none does NOT match an absent value.
		{flags: nil, null: true},
		// An int32 box (an INT32 flags column) is the same pattern.
		{flags: int32(18), wantAll: true, wantAny: true, wantNone: false},
		// Past 2^53: 2^62|18. A float carrier answers 0 here.
		{flags: int64(1)<<62 | 18, wantAll: true, wantAny: true, wantNone: false},
	} {
		args := []any{tc.flags, "SYN", "ACK"}
		gotAll, gotAny, gotNone := all(args), any_(args), none(args)
		if tc.null {
			if gotAll != nil || gotAny != nil || gotNone != nil {
				t.Errorf("NULL flags answered (%v,%v,%v); PostgreSQL answers NULL for all three",
					gotAll, gotAny, gotNone)
			}
			continue
		}
		if gotAll != tc.wantAll {
			t.Errorf("tcp_flags_has_all(%v,'SYN','ACK') = %v, PostgreSQL's (f&18)=18 is %v",
				tc.flags, gotAll, tc.wantAll)
		}
		if gotAny != tc.wantAny {
			t.Errorf("tcp_flags_has_any(%v,'SYN','ACK') = %v, PostgreSQL's (f&18)<>0 is %v",
				tc.flags, gotAny, tc.wantAny)
		}
		if gotNone != tc.wantNone {
			t.Errorf("tcp_flags_has_none(%v,'SYN','ACK') = %v, PostgreSQL's (f&18)=0 is %v",
				tc.flags, gotNone, tc.wantNone)
		}
	}
}

func TestTheTCPFlagMaskIsTheRFC9293Table(t *testing.T) {
	mask := DefaultRegistry.Lookup("tcp_flag_mask")
	for _, tc := range []struct {
		names []any
		want  int32
	}{
		{[]any{"FIN"}, 1},
		{[]any{"SYN"}, 2},
		{[]any{"RST"}, 4},
		{[]any{"PSH"}, 8},
		{[]any{"ACK"}, 16},
		{[]any{"URG"}, 32},
		{[]any{"ECE"}, 64},
		{[]any{"CWR"}, 128},
		{[]any{"AE"}, 256},
		{[]any{"NS"}, 256}, // RFC 3540's name for the same bit
		{[]any{"SYN", "ACK"}, 18},
		{[]any{"syn", "AcK"}, 18}, // names are case-insensitive
		{[]any{"SYN", "SYN"}, 2},  // a repeat is the same bit
		{[]any{"FIN", "SYN", "RST", "PSH", "ACK", "URG", "ECE", "CWR", "AE"}, 511},
	} {
		if got := mask(tc.names); got != tc.want {
			t.Errorf("tcp_flag_mask(%v) = %#v, want int32 %d", tc.names, got, tc.want)
		}
	}
}

func TestTheTCPFlagRenderersNameTheBitsInHeaderOrder(t *testing.T) {
	names := DefaultRegistry.Lookup("tcp_flags")
	text := DefaultRegistry.Lookup("tcp_flags_text")
	for _, tc := range []struct {
		flags any
		want  []string
	}{
		{int64(0), []string{}},
		{int64(2), []string{"SYN"}},
		{int64(18), []string{"SYN", "ACK"}}, // ascending bit order, never ACK|SYN
		{int64(24), []string{"PSH", "ACK"}},
		{int64(256), []string{"AE"}}, // canonical spelling, never NS
		{int64(511), []string{"FIN", "SYN", "RST", "PSH", "ACK", "URG", "ECE", "CWR", "AE"}},
		// A bit outside the nine is not a TCP flag and is not named. The
		// documented rendering contract; has_all over the same value is
		// unaffected because it never consults this table.
		{int64(1024 | 2), []string{"SYN"}},
	} {
		got, ok := names([]any{tc.flags}).([]any)
		if !ok {
			t.Fatalf("tcp_flags(%v) did not answer an ARRAY: %#v", tc.flags, names([]any{tc.flags}))
		}
		if len(got) != len(tc.want) {
			t.Errorf("tcp_flags(%v) = %v, want %v", tc.flags, got, tc.want)
			continue
		}
		joined := ""
		for i, w := range tc.want {
			if got[i] != w {
				t.Errorf("tcp_flags(%v)[%d] = %v, want %s", tc.flags, i, got[i], w)
			}
			if i > 0 {
				joined += "|"
			}
			joined += w
		}
		if gotText := text([]any{tc.flags}); gotText != joined {
			t.Errorf("tcp_flags_text(%v) = %#v, want %q", tc.flags, gotText, joined)
		}
	}
	if got := names([]any{nil}); got != nil {
		t.Errorf("tcp_flags(NULL) = %#v, want NULL", got)
	}
	if got := text([]any{nil}); got != nil {
		t.Errorf("tcp_flags_text(NULL) = %#v, want NULL", got)
	}
}

// An unknown name is LOUD. Dropping the bit would turn has_all('SYN','ACKK')
// into has_all('SYN') — a larger row set nothing can tell from the intended
// one.
func TestAnUnknownTCPFlagNameIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func()
		msg  string
	}{
		{"mask_unknown_name", func() {
			DefaultRegistry.Lookup("tcp_flag_mask")([]any{"SYNN"})
		}, `TCP flag name "SYNN" not recognized`},
		{"has_all_unknown_name", func() {
			DefaultRegistry.Lookup("tcp_flags_has_all")([]any{int64(2), "SYN", "ACKK"})
		}, `TCP flag name "ACKK" not recognized`},
		{"has_any_numeric_name", func() {
			DefaultRegistry.Lookup("tcp_flags_has_any")([]any{int64(2), int64(2)})
		}, `TCP flag name "2" not recognized`},
		{"mask_empty_list", func() {
			DefaultRegistry.Lookup("tcp_flag_mask")(nil)
		}, "tcp_flag_mask requires at least one TCP flag name"},
		{"has_none_no_names", func() {
			DefaultRegistry.Lookup("tcp_flags_has_none")([]any{int64(2)})
		}, "tcp_flags_has_none requires at least one TCP flag name"},
		{"flags_argument_is_text", func() {
			DefaultRegistry.Lookup("tcp_flags_has_all")([]any{"nope", "SYN"})
		}, "the flags argument must be an integer, got text"},
		{"flags_argument_is_fractional", func() {
			DefaultRegistry.Lookup("tcp_flags")([]any{2.5})
		}, "the flags argument must be an integer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatalf("answered where a refusal was due (%s)", tc.msg)
				}
				fe, ok := r.(fatalEval)
				if !ok {
					t.Fatalf("panicked with %#v, not a fatalEval refusal", r)
				}
				if state := sqlerr.StateOf(fe.err); state != "22023" {
					t.Errorf("SQLSTATE %s, want 22023 (invalid_parameter_value)", state)
				}
				if msg := fe.err.Error(); !strings.Contains(msg, tc.msg) {
					t.Errorf("message %q does not carry %q", msg, tc.msg)
				}
			}()
			tc.call()
		})
	}
}

// A LIST OF NO NAMES IS ZERO; A LIST WITH A HOLE IN IT IS A REFUSAL.
//
// PostgreSQL has no equivalent function, so the bit arithmetic decides what it
// can: the mask of no names is 0, which is what `tcp_flags_from_string(”)`
// answered before this arc and what a telemetry column spelling "no flags" as
// the empty string needs. An empty ELEMENT — `'SYN,'`, `'SYN,,ACK'` — is a
// list that names something and then names nothing, which is a slip, and the
// refusal carries the POSITION because quoting the name would quote nothing
// (#966 round 2, P2).
func TestAnEmptyTCPFlagListIsZeroAndAHoleInOneIsRefused(t *testing.T) {
	fn := DefaultRegistry.Lookup("tcp_flags_from_string")
	for _, in := range []string{"", "   "} {
		if got := fn([]any{in}); got != int64(0) {
			t.Errorf("tcp_flags_from_string(%q) = %#v, want int64 0", in, got)
		}
	}
	for _, tc := range []struct{ in, msg string }{
		{"SYN,", `empty TCP flag name at position 2 of "SYN,"`},
		{",SYN", `empty TCP flag name at position 1 of ",SYN"`},
		{"SYN,,ACK", `empty TCP flag name at position 2 of "SYN,,ACK"`},
		{"SYN, ,ACK", `empty TCP flag name at position 2`},
	} {
		t.Run(tc.in, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatalf("tcp_flags_from_string(%q) answered where 22023 is due", tc.in)
				}
				fe, ok := r.(fatalEval)
				if !ok {
					t.Fatalf("panicked with %#v, not a fatalEval refusal", r)
				}
				if state := sqlerr.StateOf(fe.err); state != "22023" {
					t.Errorf("SQLSTATE %s, want 22023", state)
				}
				if !strings.Contains(fe.err.Error(), tc.msg) {
					t.Errorf("message %q does not carry %q", fe.err, tc.msg)
				}
			}()
			fn([]any{tc.in})
		})
	}
	// And the ordinary list, unchanged.
	if got := fn([]any{"SYN,ACK"}); got != int64(18) {
		t.Errorf("tcp_flags_from_string('SYN,ACK') = %#v, want 18", got)
	}
}

// A float flags argument outside int64's range is REFUSED, not saturated.
// Converting an out-of-range float to int64 in Go is implementation-defined;
// the float64 arm always guarded the range and the float32 arm did not
// (#966 round 2, N4).
func TestAnOutOfRangeFloatFlagsArgumentIsRefused(t *testing.T) {
	fn := DefaultRegistry.Lookup("tcp_flags_has_any")
	for _, arg := range []any{float32(1e30), float64(1e30), float32(-1e30), float64(2.5), float32(2.5)} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("tcp_flags_has_any(%T %v, 'SYN') answered where 22023 is due", arg, arg)
				}
			}()
			fn([]any{arg, "SYN"})
		}()
	}
	// An exact, in-range float still answers.
	if got := fn([]any{float32(18), "SYN"}); got != true {
		t.Errorf("tcp_flags_has_any(18.0, 'SYN') = %#v, want true", got)
	}
}

// A NULL flag NAME makes the call NULL, the way every strict function here
// behaves — it is not an unknown name.
func TestANullTCPFlagNameIsNull(t *testing.T) {
	for _, name := range []string{"tcp_flags_has_all", "tcp_flags_has_any", "tcp_flags_has_none"} {
		if got := DefaultRegistry.Lookup(name)([]any{int64(18), nil}); got != nil {
			t.Errorf("%s(18, NULL) = %#v, want NULL", name, got)
		}
	}
	if got := DefaultRegistry.Lookup("tcp_flag_mask")([]any{nil}); got != nil {
		t.Errorf("tcp_flag_mask(NULL) = %#v, want NULL", got)
	}
}

// THE TYPED ROW KERNEL ANSWERS WHAT THE GENERIC CALL ANSWERS.
//
// flagsTest reads Int32Data/Int64Data directly; its Fallback is the boxed
// FuncCall. Two implementations of one predicate is exactly the shape that
// drifts, so the kernel is checked against its own fallback, per row, over
// both integer widths and over the shapes it declines (a FLOAT column, a
// computed argument) where the fallback must be what answers.
func TestTheTypedFlagKernelAgreesWithTheGenericCall(t *testing.T) {
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "f4", Type: parquet.TypeInt32, Nullable: true},
		{Name: "f8", Type: parquet.TypeInt64, Nullable: true},
		{Name: "fx", Type: parquet.TypeFloat64, Nullable: true},
	}, 6)
	vals := []int64{0, 2, 18, 16, 511, 24}
	for i, v := range vals {
		b.Columns[0].Int32Data[i] = int32(v)
		b.Columns[1].Int64Data[i] = v
		b.Columns[2].Float64Data[i] = float64(v)
	}
	b.Columns[0].Nulls.SetNull(4)
	b.Columns[1].Nulls.SetNull(4)
	b.Len = 6

	for _, fn := range []string{"tcp_flags_has_all", "tcp_flags_has_any", "tcp_flags_has_none"} {
		for _, col := range []string{"f4", "f8", "fx"} {
			generic := &FuncCall{Name: fn, Args: []Expr{
				&ColRef{Name: col}, &Lit{Val: "SYN"}, &Lit{Val: "ACK"}}}
			kernel := newFlagsTest(&FuncCall{Name: fn, Args: []Expr{
				&ColRef{Name: col}, &Lit{Val: "SYN"}, &Lit{Val: "ACK"}}})
			if kernel == nil {
				t.Fatalf("%s(%s): no typed kernel was built", fn, col)
			}
			for row := 0; row < b.Len; row++ {
				want := generic.Eval(b, row)
				gotVal, gotNull := kernel.EvalBoolNull(b, row)
				var got any
				if !gotNull {
					got = gotVal
				}
				if got != want {
					t.Errorf("%s(%s) row %d: kernel %#v, generic %#v", fn, col, row, got, want)
				}
			}
		}
	}

	// A computed flags argument and a non-constant name keep the generic
	// path: newFlagsTest declines, and the call still answers.
	if newFlagsTest(&FuncCall{Name: "tcp_flags_has_all", Args: []Expr{
		&FuncCall{Name: "abs", Args: []Expr{&ColRef{Name: "f8"}}}, &Lit{Val: "SYN"}}}) != nil {
		t.Error("a computed flags argument specialized; it must keep the generic call")
	}
	if newFlagsTest(&FuncCall{Name: "tcp_flags_has_all", Args: []Expr{
		&ColRef{Name: "f8"}, &ColRef{Name: "f4"}}}) != nil {
		t.Error("a column-valued flag name specialized; it must keep the generic call")
	}
	// An unknown name must NOT specialize — the generic path is what raises.
	if newFlagsTest(&FuncCall{Name: "tcp_flags_has_all", Args: []Expr{
		&ColRef{Name: "f8"}, &Lit{Val: "NOPE"}}}) != nil {
		t.Error("an unknown flag name specialized; the refusal would be lost")
	}
}
