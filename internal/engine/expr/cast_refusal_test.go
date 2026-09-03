package expr

import (
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #839 and its census siblings: a CAST whose destination cannot read its TEXT
// used to answer the text back, or the number ZERO.
//
// The zero is the worse half. `CAST('abc' AS DOUBLE PRECISION)` handed a
// client a MEASUREMENT — and nothing downstream can tell a computed zero from
// a refused parse. Every expectation below is live postgres:17.11.

func castRefusalBatch(t *testing.T) *batch.RecordBatch {
	t.Helper()
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "s", Type: parquet.TypeString},
		{Name: "u", Type: parquet.TypeUUID},
	}, 1)
	b.Len = 1
	b.Columns[0].SetValue(0, "not-a-uuid")
	b.Columns[1].SetValue(0, "123e4567-e89b-12d3-a456-426614174000")
	return b
}

func TestCastToUUIDReadsOrRefuses(t *testing.T) {
	b := castRefusalBatch(t)
	// PostgreSQL 17.11 takes three input spellings and prints one.
	for _, c := range []struct{ in, want string }{
		{"123e4567-e89b-12d3-a456-426614174000", "123e4567-e89b-12d3-a456-426614174000"},
		{"123E4567-E89B-12D3-A456-426614174000", "123e4567-e89b-12d3-a456-426614174000"},
		{"123e4567e89b12d3a456426614174000", "123e4567-e89b-12d3-a456-426614174000"},
		{"{123e4567-e89b-12d3-a456-426614174000}", "123e4567-e89b-12d3-a456-426614174000"},
	} {
		got := (&Cast{Operand: &Lit{Val: c.in}, DestType: "uuid"}).Eval(b, 0)
		if got != c.want {
			t.Errorf("CAST(%q AS UUID) = %v, want %q", c.in, got, c.want)
		}
	}
	// A UUID COLUMN is already canonical text, so the cast is a no-op that
	// says so rather than one that says nothing.
	if got := (&Cast{Operand: &ColRef{Name: "u"}, DestType: "uuid"}).Eval(b, 0); got !=
		"123e4567-e89b-12d3-a456-426614174000" {
		t.Errorf("CAST(uuid_col AS UUID) = %v", got)
	}
	// The refusals. `' …'` is the one that proves the accept-set is
	// PostgreSQL's and not "whatever Go's UUID parsers take": the server
	// rejects surrounding whitespace.
	for _, in := range []string{
		"not-a-uuid", "", "123e4567-e89b-12d3-a456-42661417400",
		"123e4567-e89b-12d3-a456-4266141740000", "1-2-3-4-5",
		" 123e4567-e89b-12d3-a456-426614174000 ",
		"123e4567-e89b-12d3-a456-42661417400g",
	} {
		state, msg := recoverFatalEvalForTest(t, func() {
			(&Cast{Operand: &Lit{Val: in}, DestType: "uuid"}).Eval(b, 0)
		})
		want := `invalid input syntax for type uuid: "` + in + `"`
		if state != "22P02" || msg != want {
			t.Errorf("CAST(%q AS UUID) raised [%s] %s, want [22P02] %s", in, state, msg, want)
		}
	}
}

// TestCastTextToFloatReadsOrRefuses is the ZERO half of the same census: three
// destinations read non-numeric text as the number 0.
func TestCastTextToFloatReadsOrRefuses(t *testing.T) {
	b := castRefusalBatch(t)
	for _, c := range []struct {
		dest       string
		in         string
		state, msg string
	}{
		{"double precision", "abc", "22P02", `invalid input syntax for type double precision: "abc"`},
		{"float8", "", "22P02", `invalid input syntax for type double precision: ""`},
		{"real", "abc", "22P02", `invalid input syntax for type real: "abc"`},
		{"numeric", "abc", "22P02", `invalid input syntax for type numeric: "abc"`},
		{"decimal", "abc", "22P02", `invalid input syntax for type numeric: "abc"`},
		// A well-formed number the type cannot carry is a RANGE condition, and
		// the two codes are different answers — 22P02 sends a reader hunting a
		// typo in a number that was read correctly.
		{"double precision", "1e400", "22003", `"1e400" is out of range for type double precision`},
	} {
		state, msg := recoverFatalEvalForTest(t, func() {
			(&Cast{Operand: &Lit{Val: c.in}, DestType: c.dest}).Eval(b, 0)
		})
		if state != c.state || msg != c.msg {
			t.Errorf("CAST(%q AS %s) raised [%s] %s, want [%s] %s (live PostgreSQL 17.11)",
				c.in, c.dest, state, msg, c.state, c.msg)
		}
	}
	// The accept-set is PostgreSQL's: whitespace is trimmed, and inf/nan are
	// values in any case spelling. These are the boundary, from the outside —
	// a refusal that took them too would be a new divergence.
	for _, c := range []struct {
		in   string
		want float64
	}{{"  1.5  ", 1.5}, {"1.5e10", 1.5e10}, {"-2", -2}} {
		if got := (&Cast{Operand: &Lit{Val: c.in},
			DestType: "double precision"}).Eval(b, 0); got != c.want {
			t.Errorf("CAST(%q AS DOUBLE PRECISION) = %v, want %v", c.in, got, c.want)
		}
	}
	for _, in := range []string{"inf", "Infinity", "-inf"} {
		got, ok := (&Cast{Operand: &Lit{Val: in},
			DestType: "double precision"}).Eval(b, 0).(float64)
		if !ok || !math.IsInf(got, 0) {
			t.Errorf("CAST(%q AS DOUBLE PRECISION) = %v, want an infinity", in, got)
		}
	}
	got, ok := (&Cast{Operand: &Lit{Val: "nan"}, DestType: "double precision"}).Eval(b, 0).(float64)
	if !ok || !math.IsNaN(got) {
		t.Errorf(`CAST('nan' AS DOUBLE PRECISION) = %v, want NaN`, got)
	}
}

// TestCastToANetworkTypeStillPassesThrough is a DEFERRAL, pinned.
//
// `CAST('abc' AS IPV4|IPV6|CIDR|MACADDR)` returns the text under a STRING
// declaration; PostgreSQL raises 22P02 for its inet/cidr/macaddr equivalents.
// The fix is NOT a validator written inside Cast.Eval: the engine has no
// single network-text accept-set to validate against — the ingest boundary
// type-checks the Go box and not the text, and #627 records that the literal
// accept-set already diverges from PostgreSQL's abbreviated forms. Minting a
// second accept-set here would give one engine two answers to "is this an
// address", which is the failure `parquet.ParseDateDays` exists to prevent for
// dates.
//
// TODO(#839): delete this when the network types have ONE text accept-set,
// shared by ingest, the comparison kernels and this cast. This pin fails the
// day the cast starts refusing, which is the signal to record the accept-set
// rather than discover it.
// TestArrayLengthIsDimensionAware is #637: `array_length` was registered as an
// alias of the one-argument `cardinality`, so it answered 0 for an empty array
// where PostgreSQL answers NULL and IGNORED its dimension argument entirely.
//
// The two functions disagree on an empty array ON PURPOSE — cardinality counts
// elements and is 0, array_length asks "how long is dimension 1" and there is
// no dimension 1 — and the alias made them agree. Every expectation is live
// postgres:17.11.
func TestArrayLengthIsDimensionAware(t *testing.T) {
	three := []any{int64(1), int64(2), int64(3)}
	empty := []any{}
	for _, c := range []struct {
		name string
		args []any
		want any
	}{
		{"dimension 1 of a 3-element array", []any{three, int64(1)}, int32(3)},
		{"dimension 2 of a 1-D array", []any{three, int64(2)}, nil},
		{"dimension 0", []any{three, int64(0)}, nil},
		{"a negative dimension", []any{three, int64(-1)}, nil},
		{"a NULL dimension", []any{three, nil}, nil},
		{"dimension 1 of an EMPTY array", []any{empty, int64(1)}, nil},
		{"a NULL array", []any{nil, int64(1)}, nil},
		// The one-argument spelling PostgreSQL does not have keeps
		// cardinality's answer, so nothing that called it changes.
		{"the one-argument wadjet spelling", []any{three}, int32(3)},
		{"the one-argument spelling over an empty array", []any{empty}, int32(0)},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := fnArrayLength(c.args); got != c.want {
				t.Errorf("array_length%v = %#v, want %#v (live PostgreSQL 17.11)",
					c.args, got, c.want)
			}
		})
	}
	// CARDINALITY is the control and must NOT move: 0 for an empty array is
	// what PostgreSQL answers for it, and it is the answer array_length no
	// longer shares.
	if got := fnCardinality([]any{empty}); got != int32(0) {
		t.Errorf("cardinality(ARRAY[]) = %#v, want int32(0) — PostgreSQL answers 0 here and "+
			"NULL for array_length, which is the whole distinction", got)
	}
}

// TestInt32CountRaisesInsteadOfWrapping is #637's second half. #530 made the
// length family answer int4, which is PostgreSQL's declaration, through a bare
// `int32(len(...))`: past the boundary that produces a NEGATIVE number under a
// right type. PostgreSQL raises `integer out of range`.
//
// The function is tested directly because the inputs that reach the boundary
// cannot be built in a test — BIT_LENGTH needs a string past 256 MB and
// CARDINALITY an array of 2.1 billion elements. That unreachability is exactly
// why the wrap was silent, and it is why the check is asserted at the
// conversion rather than through a query nobody can write.
func TestInt32CountRaisesInsteadOfWrapping(t *testing.T) {
	if got := int32Count(math.MaxInt32); got != math.MaxInt32 {
		t.Errorf("int32Count(MaxInt32) = %d, want %d", got, math.MaxInt32)
	}
	if got := int32Count(0); got != 0 {
		t.Errorf("int32Count(0) = %d, want 0", got)
	}
	for _, n := range []int{math.MaxInt32 + 1, math.MinInt32 - 1} {
		state, msg := recoverFatalEvalForTest(t, func() { int32Count(n) })
		if state != "22003" || msg != "integer out of range" {
			t.Errorf("int32Count(%d) raised [%s] %s, want [22003] integer out of range "+
				"(live PostgreSQL 17.11)", n, state, msg)
		}
	}
}

func TestCastToANetworkTypeStillPassesThrough(t *testing.T) {
	b := castRefusalBatch(t)
	for _, dest := range []string{"ipv4", "ipv6", "cidr", "macaddr", "mac"} {
		got := func() (v any) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("CAST('abc' AS %s) now raises %v — #839's network half has moved. "+
						"Record the accept-set it validates against and check it is the SAME one "+
						"ingest and the comparison kernels use, then delete this pin.", dest, r)
				}
			}()
			return (&Cast{Operand: &Lit{Val: "abc"}, DestType: dest}).Eval(b, 0)
		}()
		if got != "abc" {
			t.Errorf("CAST('abc' AS %s) = %v, this pin records %q", dest, got, "abc")
		}
	}
}
