// SPDX-License-Identifier: MIT

package expr

import (
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// Every cast destination this engine HAS gets its own domain, from every
// source box, and a value with no place in it is 22003 (#901).
//
// Before this, `int32`, `float32`, `port` and `protocol` matched no label in
// Cast.Eval's switch and fell to `default: return v` — the operand handed back
// unchanged — while inferCastType declared STRING for the same four names. The
// two layers agreed with each other about a text column, so
// `SELECT 3000000000::INT32` answered 3000000000 where PostgreSQL raises
// `integer out of range` for `3000000000::int4`, and `3000000000::PORT`
// answered it under a type whose whole carrier is a signed 32-bit field.
//
// PORT and PROTOCOL have LEFT this table (2026-09-15, Derek's decision on the
// round-2 review's FC-2). Their bound is the TYPE's — 0..65535 and 0..255 —
// not the int4 carrier's, and it is checked wherever a value ENTERS the type.
// TestAValueEnteringPortOrProtocolIsHeldToTheTypesRange below is their table;
// what stays here is int4's, which INT32 and DATE still take.
func TestAnInt32DomainCastRefusesPastItsOwnRange(t *testing.T) {
	b := batch.NewRecordBatch(nil, 1)

	// Each source box is a separate row of the matrix: the integer arm reads
	// an int64 through toInt64Safe, a float through castFloatToInt64Even, a
	// numeric literal through castFloatToInt64 and text through
	// kernel.IntLitText, and only ONE of those four used to be checked.
	for _, dest := range []string{"int32", "INT32"} {
		for _, c := range []struct {
			name    string
			operand Expr
			want    int64 // when refused == false
			refused bool
		}{
			{"max", &Lit{Val: int64(2147483647)}, 2147483647, false},
			{"min", &Lit{Val: int64(-2147483648)}, -2147483648, false},
			{"zero", &Lit{Val: int64(0)}, 0, false},
			{"max_plus_one", &Lit{Val: int64(2147483648)}, 0, true},
			{"min_minus_one", &Lit{Val: int64(-2147483649)}, 0, true},
			{"three_billion", &Lit{Val: int64(3000000000)}, 0, true},
			{"int64_max", &Lit{Val: int64(9223372036854775807)}, 0, true},
			{"int64_min", &Lit{Val: int64(-9223372036854775808)}, 0, true},
			// A FLOAT source reaches the same bound through a different arm.
			{"float_in_range", &Lit{Val: float64(1000)}, 1000, false},
			{"float_out_of_range", &Lit{Val: float64(3e9)}, 0, true},
			// So does TEXT, which used to be handed straight back.
			{"text_in_range", &Lit{Val: "443"}, 443, false},
			{"text_out_of_range", &Lit{Val: "3000000000"}, 0, true},
		} {
			t.Run(dest+"/"+c.name, func(t *testing.T) {
				got, err := evalCastForTest(&Cast{Operand: c.operand, DestType: dest}, b)
				if c.refused {
					if err == nil {
						t.Fatalf("CAST(%v AS %s) answered %v; no int32 holds it",
							c.operand, dest, got)
					}
					if st := sqlerr.StateOf(err); st != "22003" {
						t.Errorf("SQLSTATE %q, want 22003 (%v)", st, err)
					}
					if !strings.Contains(err.Error(), "integer out of range") {
						t.Errorf("refusal %q does not carry PostgreSQL's sentence "+
							"for the same magnitude reaching an int4", err)
					}
					return
				}
				if err != nil {
					t.Fatalf("CAST(%v AS %s) refused a value an int32 holds: %v",
						c.operand, dest, err)
				}
				n, ok := got.(int64)
				if !ok {
					t.Fatalf("CAST(%v AS %s) answered %#v, not an int64 — the cast is "+
						"handing its operand back unchanged", c.operand, dest, got)
				}
				if n != c.want {
					t.Errorf("= %d, want %d", n, c.want)
				}
			})
		}
	}

	// FLOAT32 is the same gap one family over: `CAST(1e40 AS FLOAT32)` handed
	// back the double where `CAST(1e40 AS REAL)` raises. The two spellings
	// name one type and must answer one thing.
	for _, dest := range []string{"float32", "FLOAT32"} {
		t.Run(dest+"/narrows_like_real", func(t *testing.T) {
			got, err := evalCastForTest(
				&Cast{Operand: &Lit{Val: float64(1.0) / 3.0}, DestType: dest}, b)
			if err != nil {
				t.Fatalf("%v", err)
			}
			f, ok := got.(float32)
			if !ok {
				t.Fatalf("answered %#v, not a float32 — the cast is a no-op", got)
			}
			if f != float32(1.0/3.0) {
				t.Errorf("= %v, want the float4 rounding %v", f, float32(1.0/3.0))
			}
		})
		t.Run(dest+"/refuses_past_float4", func(t *testing.T) {
			_, err := evalCastForTest(
				&Cast{Operand: &Lit{Val: 1e40}, DestType: dest}, b)
			if err == nil {
				t.Fatalf("CAST(1e40 AS %s) answered; a float4 cannot hold it", dest)
			}
			if st := sqlerr.StateOf(err); st != "22003" {
				t.Errorf("SQLSTATE %q, want 22003 (%v)", st, err)
			}
		})
	}
}

// IsIntegerCastDest is documented as "Cast.Eval's own switch, [...] so the two
// cannot drift". The contract is ONE-directional — everything it says yes to
// answers an int64 — and PORT and PROTOCOL are the deliberate exception in the
// other direction, so both halves are asserted rather than one.
func TestIsIntegerCastDestMatchesTheKernel(t *testing.T) {
	b := batch.NewRecordBatch(nil, 1)
	for _, dest := range []string{
		"int", "integer", "int4", "int32", "bigint", "int8", "signed", "smallint", "int2",
	} {
		if !IsIntegerCastDest(dest) {
			t.Errorf("IsIntegerCastDest(%q) = false", dest)
		}
		got, err := evalCastForTest(&Cast{Operand: &Lit{Val: int64(7)}, DestType: dest}, b)
		if err != nil {
			t.Fatalf("CAST(7 AS %s): %v", dest, err)
		}
		if _, ok := got.(int64); !ok {
			t.Errorf("IsIntegerCastDest(%q) is true but the kernel answers %#v, not an int64",
				dest, got)
		}
	}
	// PORT and PROTOCOL answer an int64 and are deliberately NOT here: the
	// predicate's one caller is the DAG gather's materialization, and telling
	// it "int64" would build an INT64 vector for a column the plan declares
	// PORT. If this starts returning true, the gather stops reaching the
	// PORT vector's own int4 guard.
	for _, dest := range []string{"port", "protocol", "date", "real", "float32"} {
		if IsIntegerCastDest(dest) {
			t.Errorf("IsIntegerCastDest(%q) = true; the gather would materialize an "+
				"INT64 vector for a column the plan declares otherwise", dest)
		}
	}
	// The four names are still accepted names; the declaration half (which
	// parquet.TypeID each one gets) is asserted in internal/planner/physical,
	// because inferCastType lives there.
	for _, name := range []string{"INT32", "FLOAT32", "PORT", "PROTOCOL"} {
		if !KnownCastDest(name) {
			t.Errorf("KnownCastDest(%q) = false", name)
		}
	}
}

// evalCastForTest runs one Cast and converts its FatalEvalPanic into an error,
// the way exec.Project's recovery seam does.
func evalCastForTest(c *Cast, b *batch.RecordBatch) (v any, err error) {
	defer func() {
		if r := recover(); r != nil {
			if fe, ok := r.(interface{ FatalEvalError() error }); ok {
				err = fe.FatalEvalError()
				return
			}
			panic(r)
		}
	}()
	return c.Eval(b, 0), nil
}

// TestAValueEnteringPortOrProtocolIsHeldToTheTypesRange is the rule Derek
// settled on 2026-09-15: a PORT is 0..65535 and a PROTOCOL 0..255, the range
// is checked when a value ENTERS the type — by CAST or by WRITE — and nowhere
// else, and a value outside it is 22003 naming the value and the type.
//
// Before it the CAST held the int4 CARRIER's range, so `CAST(70000 AS PORT)`
// answered 70000 and `CREATE TABLE p AS SELECT CAST(70000 AS PORT)` PERSISTED
// it into a column the catalog declares PORT — a value `INSERT` refuses with
// 22003 at the same table (review NT N2). One rule, one refusal, one message:
// parquet.NetworkIntRangeError, which is the writer's own.
//
// ARITHMETIC is deliberately NOT here: `port + 70000` is plain int4 arithmetic
// and may leave the range without error, which is PostgreSQL's `smallint + 1`
// rule and #901's position, both intact.
func TestAValueEnteringPortOrProtocolIsHeldToTheTypesRange(t *testing.T) {
	b := batch.NewRecordBatch(nil, 1)
	for _, d := range []struct {
		dest   string
		lo, hi int64
	}{{"port", 0, 65535}, {"PORT", 0, 65535}, {"protocol", 0, 255}, {"PROTOCOL", 0, 255}} {
		for _, c := range []struct {
			name    string
			operand Expr
			want    int64
			refused bool
		}{
			{"low edge", &Lit{Val: d.lo}, d.lo, false},
			{"high edge", &Lit{Val: d.hi}, d.hi, false},
			{"inside", &Lit{Val: d.hi / 2}, d.hi / 2, false},
			{"one past the high edge", &Lit{Val: d.hi + 1}, 0, true},
			{"one below the low edge", &Lit{Val: d.lo - 1}, 0, true},
			{"negative", &Lit{Val: int64(-5)}, 0, true},
			// From TEXT, from a WIDER int, and from a float: every source
			// reaches the same bound, which is what "when a value enters the
			// type" means.
			{"from text, inside", &Lit{Val: strconv.FormatInt(d.hi, 10)}, d.hi, false},
			{"from text, past", &Lit{Val: strconv.FormatInt(d.hi+1, 10)}, 0, true},
			{"from a wider int", &Lit{Val: int64(3000000000)}, 0, true},
			{"from int64's maximum", &Lit{Val: int64(math.MaxInt64)}, 0, true},
			{"from a float, inside", &Lit{Val: float64(d.hi)}, d.hi, false},
			{"from a float, past", &Lit{Val: float64(d.hi) + 1}, 0, true},
		} {
			t.Run(d.dest+"/"+c.name, func(t *testing.T) {
				got, err := evalCastForTest(&Cast{Operand: c.operand, DestType: d.dest}, b)
				if c.refused {
					if err == nil {
						t.Fatalf("CAST(%v AS %s) answered %v; the type's range is [%d, %d]",
							c.operand, d.dest, got, d.lo, d.hi)
					}
					if st := sqlerr.StateOf(err); st != "22003" {
						t.Errorf("SQLSTATE %q, want 22003 (%v)", st, err)
					}
					if !strings.Contains(err.Error(), "out of range") ||
						!strings.Contains(strings.ToUpper(err.Error()), strings.ToUpper(d.dest)) {
						t.Errorf("refusal %q names neither the type nor the bound", err)
					}
					return
				}
				if err != nil {
					t.Fatalf("CAST(%v AS %s) refused a value the type holds: %v",
						c.operand, d.dest, err)
				}
				if n, ok := got.(int64); !ok || n != c.want {
					t.Errorf("= %#v, want %d", got, c.want)
				}
			})
		}
	}
}
