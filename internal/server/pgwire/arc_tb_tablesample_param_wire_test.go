// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"os"
	"testing"
)

// A TABLESAMPLE argument bound as a PARAMETER is the argument (#1411): the
// server describes it as real (700, PostgreSQL's FLOAT4 tsm parameter: PW's
// rule, paramtypes.go), Bind renders it, and the planner reads the rendering
// as it reads the literal — coerced to real once (22003 for a value real
// cannot hold), the range checked when the scan begins (2202H for NULL, NaN,
// below 0 or above 100). At 6184761c a float8 parameter of 0 answered every
// row, 101 and 1e20 answered every row, and -1 / NaN / NULL were 42601.
//
// Every want is PostgreSQL 17.11 through the same Prepare / Bind over pw's
// fixture (p has six rows), re-measured when WADJET_PG_DSN names a server: a
// cell whose want differs from the live answer fails, and a pinned cell that
// starts agreeing fails.
func TestArcTBTablesampleParameterMatchesPostgres(t *testing.T) {
	ctx := context.Background()
	engine := pwEngine(t, ctx)
	var pg *pwSide
	if dsn := os.Getenv("WADJET_PG_DSN"); dsn != "" {
		side := pwPostgres(t, ctx, dsn)
		pg = &side
	}
	const (
		bern   = "SELECT count(*) FROM p TABLESAMPLE BERNOULLI ($1)"
		system = "SELECT count(*) FROM p TABLESAMPLE SYSTEM ($1)"
		oidI2  = 21
		oidI4  = 23
		oidI8  = 20
		oidF4  = 700
		oidF8  = 701
		oidNum = 1700
		oidTxt = 25
		oidB   = 16
	)
	all := func(described string) string { return "params=[" + described + "] fields=20 rows=[6]" }
	none := func(described string) string { return "params=[" + described + "] fields=20 rows=[0]" }
	cells := []struct {
		name, sql string
		p         pwParam
		want      string
		pin       string // this engine's answer where it is a kept divergence
	}{
		// unspecified: PostgreSQL types the position real
		{"unspecified/zero", bern, pwT("0"), none("700"), ""},
		{"unspecified/hundred", bern, pwT("100"), all("700"), ""},
		{"unspecified/hundred_e6", bern, pwT("100.000001"), all("700"), ""},
		{"unspecified/101", bern, pwT("101"), "ERR 2202H", ""},
		{"unspecified/minus_one", bern, pwT("-1"), "ERR 2202H", ""},
		{"unspecified/nan", bern, pwT("NaN"), "ERR 2202H", ""},
		{"unspecified/infinity", bern, pwT("Infinity"), "ERR 2202H", ""},
		{"unspecified/e400", bern, pwT("1e400"), "ERR 22003", ""},
		{"unspecified/abc", bern, pwT("abc"), "ERR 22P02", ""},
		{"unspecified/null", bern, pwParam{}, "ERR 2202H", ""},
		// float8 (JDBC setDouble, a Python float)
		{"float8/zero", bern, pwO(oidF8, "0"), none("701"), ""},
		{"float8/hundred", bern, pwO(oidF8, "100"), all("701"), ""},
		{"float8/101", bern, pwO(oidF8, "101"), "ERR 2202H", ""},
		{"float8/e20", bern, pwO(oidF8, "1e20"), "ERR 2202H", ""},
		{"float8/minus_one", bern, pwO(oidF8, "-1"), "ERR 2202H", ""},
		{"float8/nan", bern, pwO(oidF8, "NaN"), "ERR 2202H", ""},
		{"float8/infinity", bern, pwO(oidF8, "Infinity"), "ERR 2202H", ""},
		{"float8/e39", bern, pwO(oidF8, "1e39"), "ERR 22003", ""},
		{"float8/e_minus46", bern, pwO(oidF8, "1e-46"), "ERR 22003", ""},
		{"float8/null", bern, pwParam{oid: oidF8}, "ERR 2202H", ""},
		{"float8/binary_zero", bern, pwB(oidF8, float64(0)), none("701"), ""},
		{"float8/binary_101", bern, pwB(oidF8, float64(101)), "ERR 2202H", ""},
		{"float4/zero", bern, pwO(oidF4, "0"), none("700"), ""},
		{"float4/101", bern, pwO(oidF4, "101"), "ERR 2202H", ""},
		// integers and numeric
		{"int4/zero", bern, pwO(oidI4, "0"), none("23"), ""},
		{"int4/hundred", bern, pwO(oidI4, "100"), all("23"), ""},
		{"int4/101", bern, pwO(oidI4, "101"), "ERR 2202H", ""},
		{"int4/minus_one", bern, pwO(oidI4, "-1"), "ERR 2202H", ""},
		{"int4/binary_101", bern, pwB(oidI4, int32(101)), "ERR 2202H", ""},
		{"int2/hundred", bern, pwO(oidI2, "100"), all("21"), ""},
		{"int8/101", bern, pwO(oidI8, "101"), "ERR 2202H", ""},
		{"numeric/hundred", bern, pwO(oidNum, "100"), all("1700"), ""},
		{"numeric/minus_one", bern, pwO(oidNum, "-1"), "ERR 2202H", ""},
		{"numeric/e400", bern, pwO(oidNum, "1e400"), "ERR 22003", ""},
		// not a real
		// Pinned: Bind renders a text parameter in this position as the
		// untyped literal '100', which the argument reads through real's
		// input (catalog row parameters-pgwire.md, #1411).
		{"text/hundred", bern, pwO(oidTxt, "100"), "ERR 42804", all("25")},
		{"bool/true", bern, pwO(oidB, "true"), "ERR 42804", ""},
		// SYSTEM, and an expression over the parameter
		{"system/float8_101", system, pwO(oidF8, "101"), "ERR 2202H", ""},
		{"system/int4_zero", system, pwO(oidI4, "0"), none("23"), ""},
		{"expression/int4_times_two", "SELECT count(*) FROM p TABLESAMPLE BERNOULLI ($1 * 2)", pwO(oidI4, "50"), all("23"), ""},
		{"expression/cast_text_101", "SELECT count(*) FROM p TABLESAMPLE BERNOULLI (CAST($1 AS DOUBLE PRECISION))", pwO(oidTxt, "101"), "ERR 2202H", ""},
	}
	for k, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			cell := pwCell{name: c.name, sql: c.sql, ps: []pwParam{c.p}}
			if pg != nil {
				if got := pwRun(ctx, *pg, cell, k); got != c.want {
					t.Errorf("PostgreSQL answers %s; the cell's want is %s: re-measure", got, c.want)
				}
			}
			want := c.want
			if c.pin != "" {
				want = c.pin
			}
			if got := pwRun(ctx, engine, cell, k); got != want {
				t.Errorf("%s\n  got  %s\n  want %s", c.sql, got, want)
			}
		})
	}
}
