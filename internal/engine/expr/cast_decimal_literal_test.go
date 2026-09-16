// SPDX-License-Identifier: MIT

package expr

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #1037: a wide DECIMAL literal under a CAST lost its fraction and rounded its
// integer digits before anything downstream saw it, because the cast read the
// float64 box compileLit built rather than the literal's own source text.
// 9007199254740993.25 is 2^53+1 plus a quarter: past a double's reach in both
// halves. Every expectation is psql on PostgreSQL 17.11.
func TestAWideNumericLiteralCastsFromItsOwnText(t *testing.T) {
	b := batch.NewRecordBatch([]parquet.Column{{Name: "id", Type: parquet.TypeInt64}}, 1)
	b.Len = 1
	b.Columns[0].SetValue(0, int64(1))
	for _, c := range []struct{ name, sql, want string }{
		{"parameterized_destination",
			"CAST(9007199254740993.25 AS DECIMAL(30,2))", "9007199254740993.25"},
		{"a_wider_scale_keeps_the_same_digits",
			"CAST(9007199254740993.25 AS DECIMAL(38,10))", "9007199254740993.2500000000"},
		{"a_narrower_scale_ROUNDS, half away from zero",
			"CAST(9007199254740993.25 AS DECIMAL(30,1))", "9007199254740993.3"},
		{"the_bare_destination_keeps_the_literal's_own_scale",
			"CAST(9007199254740993.25 AS DECIMAL)", "9007199254740993.25"},
		{"a_negated_literal_is_folded_with_its_text",
			"CAST(-9007199254740993.25 AS DECIMAL(30,2))", "-9007199254740993.25"},
		{"the_integer_half_alone",
			"CAST(9007199254740993 AS DECIMAL(30,2))", "9007199254740993.00"},
		{"thirty_eight_digits, the carrier's edge",
			"CAST(99999999999999999999999999999999999999 AS DECIMAL(38,0))",
			"99999999999999999999999999999999999999"},
		// The controls: a literal a double CAN hold answered correctly before
		// and still does, so the fix is about the carrier and not the cast.
		{"an_ordinary_literal", "CAST(12.75 AS DECIMAL(9,2))", "12.75"},
		{"an_ordinary_literal_rounds", "CAST(12.755 AS DECIMAL(9,2))", "12.76"},
		{"zero", "CAST(0 AS DECIMAL(9,2))", "0.00"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := intDomainCompile(t, c.sql).Eval(b, 0)
			if got != c.want {
				t.Errorf("%s = %#v, want %q (PostgreSQL 17.11)", c.sql, got, c.want)
			}
		})
	}
}

// A value the destination cannot hold is still a 22003, and reading the
// literal's text rather than its box must not turn that into an answer.
func TestAWideLiteralPastTheDestinationIsStillRefused(t *testing.T) {
	b := batch.NewRecordBatch([]parquet.Column{{Name: "id", Type: parquet.TypeInt64}}, 1)
	b.Len = 1
	b.Columns[0].SetValue(0, int64(1))
	for _, sql := range []string{
		"CAST(9007199254740993.25 AS DECIMAL(9,2))",
		"CAST(999999999999999999999999999999999999999 AS DECIMAL(38,0))",
	} {
		state, _ := recoverFatalEvalForTest(t, func() { intDomainCompile(t, sql).Eval(b, 0) })
		if state != "22003" {
			t.Errorf("%s raised [%s], want [22003]", sql, state)
		}
	}
}
