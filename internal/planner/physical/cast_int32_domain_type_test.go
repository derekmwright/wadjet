// SPDX-License-Identifier: MIT

package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The planner half of expr.TestAnInt32DomainCastRefusesPastItsOwnRange (#901).
//
// `INT32`, `FLOAT32`, `PORT` and `PROTOCOL` are names this engine HAS, and
// this function declared STRING for all four because they matched no case
// label — while Cast.Eval's switch matched none of them either and handed the
// operand back unchanged. The two layers agreeing with each other about a text
// column is exactly what #652 closed for names that answer to nothing at all,
// and it is why the value never reached a vector whose int4 guard would have
// refused it: `SELECT 3000000000::PORT` answered 3000000000 under OID 25.
//
// A PORT COLUMN declares integer (OID 23) on the wire since #834, so a CAST to
// PORT must declare the same thing — a right value under a text OID is what a
// driver reads as a string.
func TestInferCastTypeInt32DomainSpellings(t *testing.T) {
	// Every integer spelling lands on INT64. #1070 narrowed arithmetic and
	// literals to int4 and did NOT narrow the CAST: the DAG declares a cast
	// OVER A WINDOW from the window's own output, so an int4 cast declaration
	// made the two paths disagree about one expression (ADR-0012's list).
	for _, spelling := range []string{"INT32", "int32", " Int32 ",
		"BIGINT", "int8", " Int64 ", "SIGNED"} {
		if got := inferCastType(spelling); got != parquet.TypeInt64 {
			t.Errorf("inferCastType(%q) = %v, want INT64", spelling, got)
		}
	}
	for _, spelling := range []string{"FLOAT32", "float32", " Float32 "} {
		if got := inferCastType(spelling); got != parquet.TypeFloat32 {
			t.Errorf("inferCastType(%q) = %v, want FLOAT32", spelling, got)
		}
	}
	for _, spelling := range []string{"PORT", "port", " Port "} {
		if got := inferCastType(spelling); got != parquet.TypePort {
			t.Errorf("inferCastType(%q) = %v, want PORT", spelling, got)
		}
	}
	for _, spelling := range []string{"PROTOCOL", "protocol", " Protocol "} {
		if got := inferCastType(spelling); got != parquet.TypeProtocol {
			t.Errorf("inferCastType(%q) = %v, want PROTOCOL", spelling, got)
		}
	}
	// The four ADDRESS types moved out of the pass-through list below when
	// #1092 gave Cast.Eval an arm for them: the evaluator parses the operand
	// with the type's own grammar, so the declaration has to name the type
	// that can HOLD what it produces. A CTAS over `CAST(col AS IPV4)` minted a
	// STRING column while the two layers agreed on STRING.
	for _, c := range []struct {
		spelling string
		want     parquet.TypeID
	}{
		{"IPV4", parquet.TypeIPv4}, {"ipv4", parquet.TypeIPv4}, {" Ip ", parquet.TypeIPv4},
		{"IPV6", parquet.TypeIPv6}, {"CIDR", parquet.TypeCIDR},
		{"MAC", parquet.TypeMAC}, {"MACADDR", parquet.TypeMAC},
	} {
		if got := inferCastType(c.spelling); got != c.want {
			t.Errorf("inferCastType(%q) = %v, want %v", c.spelling, got, c.want)
		}
	}
	// The BOUNDARY of this pass, from the other side: the destinations
	// Cast.Eval still passes through keep their STRING declaration, because
	// the two layers must agree about which names this engine converts. A
	// declaration change without a kernel change is the #310/#443 shape in
	// the other direction.
	for _, spelling := range []string{"DURATION", "BYTES", "VECTOR"} {
		if got := inferCastType(spelling); got != parquet.TypeString {
			t.Errorf("inferCastType(%q) = %v, want STRING — Cast.Eval has no arm for it, "+
				"so declaring a type would publish the operand under an OID nothing produces",
				spelling, got)
		}
	}
}
