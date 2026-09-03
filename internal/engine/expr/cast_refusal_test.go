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
