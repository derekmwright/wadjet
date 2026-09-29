// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A TIMESTAMP SCALAR INLINES AS THE TYPED INSTANT IT IS (#1378): the stage
// DAG substitutes a scalar subquery's value into the filter text a worker
// compiles, and a TIMESTAMP value is its epoch milliseconds. Spelled as the
// bare number, a DATE operand beside it read a day count: `d = (SELECT ts
// …)` matched nothing and `d < (SELECT ts …)` every row on the DAG arms,
// where PostgreSQL's `date = timestamp` (and the single-process arms)
// promote the DATE. Every other box keeps its spelling.
func TestArcDTTimestampScalarInlinesTyped(t *testing.T) {
	cases := []struct {
		v    any
		typ  parquet.TypeID
		want string
	}{
		{int64(1_704_153_600_000), parquet.TypeTimestamp, "cast('2024-01-02 00:00:00' as TIMESTAMP)"},
		{int64(1_709_553_600_500), parquet.TypeTimestamp, "cast('2024-03-04 12:00:00.5' as TIMESTAMP)"},
		{int64(1_704_153_600_000), parquet.TypeInt64, "1704153600000"},
		{"2024-01-02", parquet.TypeDate, "'2024-01-02'"},
	}
	for _, c := range cases {
		if got := scalarToLiteral(c.v, c.typ, true).String(); got != c.want {
			t.Errorf("scalarToLiteral(%v, %v) = %s, want %s", c.v, c.typ, got, c.want)
		}
	}
}
