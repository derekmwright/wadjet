// SPDX-License-Identifier: MIT

package sql

import "testing"

// pgwire's Bind renders a float4/float8 parameter as
// CAST('<text>' AS DOUBLE PRECISION | REAL). In a LIMIT, OFFSET or FETCH count
// that spelling reads as the number its text lexes to — the bare number Bind rendered before it kept the parameter's type — and
// a text that is not one number token stays the syntax error its bare spelling
// is: `LIMIT CAST('-1' …)` must not become a count of -1, which the plan reads
// as "no limit" (PostgreSQL raises 2201W; the bare `LIMIT -1` is 42601 here).
// A TABLESAMPLE argument is an expression, not a count, since #1411: its
// cells are tablesample_argument_test.go's and the coordinator's
// TestArcTBTablesampleArgumentOnEveryArm.
func TestCountPositionReadsAFloatParameterAsItsNumber(t *testing.T) {
	for _, tc := range []struct {
		name, sql, limit, offset, pct string
	}{
		{"limit float8", "SELECT id FROM p ORDER BY id LIMIT CAST('1' AS DOUBLE PRECISION)", "1", "", ""},
		{"limit float4", "SELECT id FROM p ORDER BY id LIMIT CAST('1' AS REAL)", "1", "", ""},
		{"limit lower case", "SELECT id FROM p ORDER BY id LIMIT cast('1' as double precision)", "1", "", ""},
		{"limit fraction", "SELECT id FROM p ORDER BY id LIMIT CAST('1.5' AS DOUBLE PRECISION)", "1.5", "", ""},
		{"offset float8", "SELECT id FROM p ORDER BY id OFFSET CAST('1' AS DOUBLE PRECISION)", "", "1", ""},
		{"offset float4 then limit", "SELECT id FROM p ORDER BY id OFFSET CAST('1' AS REAL) LIMIT CAST('1' AS REAL)", "1", "1", ""},
		{"fetch first float8", "SELECT id FROM p ORDER BY id FETCH FIRST CAST('1' AS DOUBLE PRECISION) ROWS ONLY", "1", "", ""},
		{"fetch next float4", "SELECT id FROM p ORDER BY id FETCH NEXT CAST('1' AS REAL) ROWS ONLY", "1", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := Parse(tc.sql)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			info, err := ExtractSelect(parsed)
			if err != nil {
				t.Fatalf("extract: %v", err)
			}
			pct := ""
			if len(info.Tables) > 0 {
				pct = info.Tables[0].SamplePercent
			}
			if info.Limit != tc.limit || info.Offset != tc.offset || pct != tc.pct {
				t.Fatalf("limit %q offset %q pct %q, want %q %q %q", info.Limit, info.Offset, pct, tc.limit, tc.offset, tc.pct)
			}
		})
	}
	for _, sql := range []string{
		"SELECT id FROM p LIMIT CAST('-1' AS DOUBLE PRECISION)",
		"SELECT id FROM p LIMIT CAST('NaN' AS DOUBLE PRECISION)",
		"SELECT id FROM p LIMIT CAST('Infinity' AS REAL)",
		"SELECT id FROM p LIMIT CAST('1 2' AS DOUBLE PRECISION)",
		"SELECT id FROM p LIMIT CAST('1' AS INTEGER)",
		"SELECT id FROM p LIMIT CAST('1' AS DOUBLE)",
		"SELECT id FROM p OFFSET CAST('-1' AS REAL)",
	} {
		parsed, err := Parse(sql)
		if err != nil {
			continue
		}
		if info, err := ExtractSelect(parsed); err == nil {
			t.Errorf("%s: parsed (limit %q offset %q); the bare spelling is a syntax error and so is this", sql, info.Limit, info.Offset)
		}
	}
}
