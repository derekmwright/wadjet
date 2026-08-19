package sql

import (
	"strings"
	"testing"
)

// TestParseLimitOffsetEitherOrder pins that LIMIT and OFFSET are read in
// whichever order they were written (#337).
//
// They used to be read in a fixed LIMIT-then-OFFSET sequence, so
// `OFFSET 5 LIMIT 3` left the LIMIT unconsumed — and because nothing checked
// for leftovers, the statement ran as `OFFSET 5` alone, which the builder
// then dropped too. `OFFSET n LIMIT m` is legal in PostgreSQL and DuckDB.
func TestParseLimitOffsetEitherOrder(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantLimit  string
		wantOffset string
	}{
		{"limit only", "SELECT a FROM t LIMIT 3", "3", ""},
		{"offset only", "SELECT a FROM t OFFSET 5", "", "5"},
		{"offset zero", "SELECT a FROM t OFFSET 0", "", "0"},
		{"limit then offset", "SELECT a FROM t LIMIT 3 OFFSET 5", "3", "5"},
		{"offset then limit", "SELECT a FROM t OFFSET 5 LIMIT 3", "3", "5"},
		{"offset rows then limit", "SELECT a FROM t OFFSET 5 ROWS LIMIT 3", "3", "5"},
		{"order by, offset only", "SELECT a FROM t ORDER BY 1 OFFSET 5", "", "5"},
		{"order by, offset then limit", "SELECT a FROM t ORDER BY 1 OFFSET 5 LIMIT 3", "3", "5"},
		{"offset then fetch first", "SELECT a FROM t OFFSET 5 ROWS FETCH NEXT 3 ROWS ONLY", "3", "5"},
		{"set operation, offset then limit",
			"SELECT a FROM t UNION ALL SELECT a FROM u ORDER BY 1 OFFSET 5 LIMIT 3", "3", "5"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pq, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse(%q): %v", tt.sql, err)
			}
			if got := pq.SelectInfo.Limit; got != tt.wantLimit {
				t.Errorf("Limit = %q, want %q", got, tt.wantLimit)
			}
			if got := pq.SelectInfo.Offset; got != tt.wantOffset {
				t.Errorf("Offset = %q, want %q", got, tt.wantOffset)
			}
		})
	}
}

// TestParseRejectsRepeatedLimitOffset: reading the pair as a loop must not
// make a repeat legal. Each keyword binds once; a second one is stray input.
func TestParseRejectsRepeatedLimitOffset(t *testing.T) {
	for _, sql := range []string{
		"SELECT a FROM t LIMIT 3 LIMIT 4",
		"SELECT a FROM t OFFSET 5 OFFSET 6",
		"SELECT a FROM t LIMIT 3 OFFSET 5 LIMIT 4",
	} {
		if _, err := Parse(sql); err == nil {
			t.Errorf("Parse(%q) accepted a repeated clause", sql)
		}
	}
}

// TestParseSetOpOrderByOrdinal pins that `ORDER BY <n>` over a set operation
// resolves against the leftmost arm's SELECT list, which is where the result's
// column names come from in PostgreSQL and DuckDB alike (#337).
//
// Positional refs used to be skipped outright for set operations, so the sort
// key stayed the literal "1", matched no column, and the rows came back in
// arrival order — a UNION ALL of a table with itself returned 0,1,2,3,4,
// 0,1,2,3,4 with no error.
func TestParseSetOpOrderByOrdinal(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string // resolved ORDER BY column names, in order
	}{
		{
			name: "union all, single ordinal",
			sql:  "SELECT r_regionkey FROM region UNION ALL SELECT r_regionkey FROM region ORDER BY 1",
			want: []string{"r_regionkey"},
		},
		{
			name: "union, ordinal picks the leftmost arm's alias",
			sql:  "SELECT r_regionkey AS k FROM region UNION SELECT r_regionkey FROM region ORDER BY 1",
			want: []string{"k"},
		},
		{
			name: "second ordinal",
			sql:  "SELECT r_regionkey, r_name FROM region UNION ALL SELECT r_regionkey, r_name FROM region ORDER BY 2",
			want: []string{"r_name"},
		},
		{
			name: "chained set operation resolves against the leftmost arm",
			sql: "SELECT r_regionkey FROM region UNION ALL SELECT r_regionkey FROM region " +
				"UNION ALL SELECT r_regionkey FROM region ORDER BY 1",
			want: []string{"r_regionkey"},
		},
		{
			name: "except",
			sql:  "SELECT r_regionkey FROM region EXCEPT SELECT r_regionkey FROM region ORDER BY 1",
			want: []string{"r_regionkey"},
		},
		{
			name: "a named term is left as written",
			sql:  "SELECT r_regionkey FROM region UNION ALL SELECT r_regionkey FROM region ORDER BY r_regionkey",
			want: []string{"r_regionkey"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pq, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse(%q): %v", tt.sql, err)
			}
			info := pq.SelectInfo
			if info.Union == nil {
				t.Fatalf("Parse(%q) produced no set operation", tt.sql)
			}
			if len(info.OrderBy) != len(tt.want) {
				t.Fatalf("got %d ORDER BY terms, want %d: %+v", len(info.OrderBy), len(tt.want), info.OrderBy)
			}
			for i, want := range tt.want {
				if got := info.OrderBy[i].Column; got != want {
					t.Errorf("ORDER BY term %d = %q, want %q", i, got, want)
				}
			}
		})
	}
}

// TestParseSetOpOrderByOrdinalOutOfRange: an ordinal past the arm's column
// count is an error, matching the non-set-operation path.
func TestParseSetOpOrderByOrdinalOutOfRange(t *testing.T) {
	sql := "SELECT r_regionkey FROM region UNION ALL SELECT r_regionkey FROM region ORDER BY 3"
	_, err := Parse(sql)
	if err == nil {
		t.Fatalf("Parse(%q) accepted an out-of-range ordinal", sql)
	}
	if !strings.Contains(err.Error(), "out of range") {
		t.Errorf("error does not say the position is out of range: %v", err)
	}
}

// TestParseNaturalJoinNamesItself: NATURAL JOIN's keys are whichever columns
// the two sides share, which the parser cannot know. It is rejected by name
// rather than left for the end-of-statement guard to report as stray input —
// and rather than dropped, which is what used to happen and answered
// `FROM nation NATURAL JOIN region` as plain `FROM nation` (#337).
func TestParseNaturalJoinNamesItself(t *testing.T) {
	sql := "SELECT COUNT(*) FROM nation NATURAL JOIN region"
	_, err := Parse(sql)
	if err == nil {
		t.Fatalf("Parse(%q) accepted NATURAL JOIN and dropped the join", sql)
	}
	if !strings.Contains(err.Error(), "NATURAL JOIN is not supported") {
		t.Errorf("error does not name NATURAL JOIN: %v", err)
	}
}
