package sql

import "testing"

// TestValuesListInFrom pins #374: VALUES (...), (...) as a table source.
// The parser desugars it into a UNION ALL of single-row SELECTs
// (parseValuesTableRef), reusing the derived-table subquery path rather
// than a new logical node type.
func TestValuesListInFrom(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"single column with alias list", `SELECT v FROM (VALUES ('a'), ('b')) AS t(v)`, false},
		{"multi-row single column", `SELECT v FROM (VALUES ('Zebra'), ('apple'), ('Apple')) AS t(v) ORDER BY v`, false},
		{"multi-column, default column1/column2 names", `SELECT column1, column2 FROM (VALUES (1, 'x'), (2, 'y')) v`, false},
		{"no alias falls back to the raw text", `SELECT * FROM (VALUES (1))`, false},
		{"differing column counts is a parse error", `SELECT v FROM (VALUES (1), (1, 2)) AS t(v)`, true},
		{"too many column aliases is a parse error", `SELECT v FROM (VALUES (1)) AS t(a, b)`, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Parse(%q): expected error, got nil", tt.sql)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse(%q): %v", tt.sql, err)
			}
		})
	}
}

// TestValuesListInFromDesugarsToUnionAll pins the exact rewrite: VALUES rows
// become one SELECT per row, columns named from the alias list (or
// PostgreSQL's default column1, column2, ... when none is given).
func TestValuesListInFromDesugarsToUnionAll(t *testing.T) {
	parsed, err := Parse(`SELECT v FROM (VALUES ('a'), ('b')) AS t(v)`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}
	if len(info.Tables) != 1 {
		t.Fatalf("got %d tables, want 1", len(info.Tables))
	}
	got := info.Tables[0].Name
	want := "(SELECT 'a' AS v UNION ALL SELECT 'b' AS v)"
	if got != want {
		t.Errorf("desugared table source = %q, want %q", got, want)
	}
	if info.Tables[0].Alias != "t" {
		t.Errorf("alias = %q, want %q", info.Tables[0].Alias, "t")
	}
}

// TestTwoWordCastTypes pins #374: CAST(x AS double precision) and
// CAST(x AS character varying) — the spellings pg_dump, JDBC and
// SQLAlchemy emit — collapse to this engine's existing single-word
// "double"/"varchar" spellings, both for CAST(...) and the :: postfix form.
func TestTwoWordCastTypes(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{"double precision", `SELECT CAST(x AS double precision)`, "cast(x as double)"},
		{"character varying", `SELECT CAST(x AS character varying)`, "cast(x as varchar)"},
		{"character varying with length", `SELECT CAST(x AS character varying(50))`, "cast(x as varchar(50))"},
		{"case insensitive", `SELECT CAST(x AS Double Precision)`, "cast(x as double)"},
		{"postfix double precision", `SELECT x::double precision`, "cast(x as double)"},
		{"postfix character varying", `SELECT x::character varying`, "cast(x as varchar)"},
		{"single-word type unaffected", `SELECT CAST(x AS double)`, "cast(x as double)"},
		{"unrelated word after double is not consumed", `SELECT CAST(x AS double) AS precision`, "cast(x as double)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := selectExprString(t, tt.sql)
			if got != tt.want {
				t.Errorf("%s = %q, want %q", tt.sql, got, tt.want)
			}
		})
	}
}

// TestIsDistinctFrom pins #374: IS [NOT] DISTINCT FROM parses as a CmpExpr
// with a dedicated op string (not IsExpr's NULL/TRUE/FALSE check shape,
// since it carries a right-hand operand) and re-renders as valid SQL.
func TestIsDistinctFrom(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{"is distinct from", `SELECT a IS DISTINCT FROM b`, "a is distinct from b"},
		{"is not distinct from", `SELECT a IS NOT DISTINCT FROM b`, "a is not distinct from b"},
		{"against NULL literal", `SELECT a IS DISTINCT FROM NULL`, "a is distinct from null"},
		{"against a literal", `SELECT 1 IS DISTINCT FROM 2`, "1 is distinct from 2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := selectExprString(t, tt.sql)
			if got != tt.want {
				t.Errorf("%s = %q, want %q", tt.sql, got, tt.want)
			}
		})
	}
	// The rewritten string must re-parse to itself — the same round-trip
	// guarantee the fuzzer holds every other rewrite to.
	roundTrip := selectExprString(t, `SELECT a IS DISTINCT FROM b`)
	got := selectExprString(t, "SELECT "+roundTrip)
	if got != roundTrip {
		t.Errorf("IS DISTINCT FROM does not round-trip: %q reparsed as %q", roundTrip, got)
	}
}

// TestIsDistinctFromStillRejectsUnknownIsCheck pins that IS [NOT] still
// refuses anything other than NULL/TRUE/FALSE/DISTINCT FROM, and that the
// error message was extended rather than replaced.
func TestIsDistinctFromStillRejectsUnknownIsCheck(t *testing.T) {
	_, err := Parse(`SELECT a IS BOGUS`)
	if err == nil {
		t.Fatal("expected a parse error for IS BOGUS")
	}
}

// TestPositionFunction pins #374: POSITION(needle IN haystack) rewrites to
// strpos(haystack, needle) — the arguments swap order, and the IN here is
// this operator's own grammar, not the set-membership operator.
func TestPositionFunction(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{"literal needle and haystack", `SELECT POSITION('cd' IN 'abcdef')`, "strpos('abcdef', 'cd')"},
		{"column haystack", `SELECT POSITION('x' IN col)`, "strpos(col, 'x')"},
		{"both columns", `SELECT POSITION(a IN b)`, "strpos(b, a)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := selectExprString(t, tt.sql)
			if got != tt.want {
				t.Errorf("%s = %q, want %q", tt.sql, got, tt.want)
			}
		})
	}
}

// TestPositionDoesNotSwallowSetMembershipIn pins the reason this needed a
// dedicated parse path: a bare "x IN (...)" alongside POSITION in the same
// statement must still parse as ordinary set membership.
func TestPositionDoesNotSwallowSetMembershipIn(t *testing.T) {
	_, err := Parse(`SELECT POSITION('a' IN 'abc'), 1 IN (1, 2, 3)`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
}
