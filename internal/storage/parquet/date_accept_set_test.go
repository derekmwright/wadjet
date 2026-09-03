package parquet

import "testing"

// #641: ParseDateDays over-accepted two classes PostgreSQL refuses, on the
// WRITE path — so wadjet STORED a date no PostgreSQL client could have
// written. The #560 fuzz found ~432 of them across 12,743 strings.
//
// Every row below is a live postgres:17-alpine transcript, taken before the
// fix. The two engines' accept-sets are compared on the SQLSTATE as well as
// the value, because the wire carries the code a client branches on (#673).
//
// The rows that AGREE are as load-bearing as the rows that moved: a
// four-digit month and a three-digit day are both accepted by PostgreSQL, and
// a fix that refused "more than two digits" would have broken them. See
// threeDigitMonthKind for why the boundary is exactly three.
func TestParseDateDaysMatchesPostgresAcceptSet(t *testing.T) {
	const (
		accept = ""
		syntax = "22007" // invalid_datetime_format
		field  = "22008" // datetime_field_overflow
	)
	cases := []struct {
		in    string
		state string
		want  int32 // days since epoch, when accepted
		note  string
	}{
		// YEAR ZERO — accepted before the fix, at day -719528 / -719163.
		{in: "0000-01-01", state: field, note: "PostgreSQL has no year zero"},
		{in: "0000-1-1", state: field},
		{in: "0000/01/01", state: field},
		{in: "0000.01.01", state: field},
		{in: "0000-12-31", state: field},
		{in: "00000101", state: field, note: "compact form, year zero"},
		{in: "00001231", state: field},
		// Year one is fine, which is the boundary from the other side.
		{in: "0001-01-01", state: accept, want: -719162},
		{in: "0002-01-01", state: accept, want: -718797},

		// THREE-DIGIT MONTH — accepted before the fix, as 2026-03-12.
		// PostgreSQL reads a three-digit second field of 1..366 as a DAY OF
		// YEAR, which leaves the third field nowhere to go: 22007.
		{in: "2024-001-01", state: syntax},
		{in: "2026-003-12", state: syntax},
		{in: "2026/003/12", state: syntax},
		{in: "2026.003.12", state: syntax},
		{in: "2026-012-12", state: syntax, note: "12 is in 1..366, so it is a DOY"},
		{in: "2026-366-12", state: syntax, note: "the last DOY value"},
		// Outside 1..366 it is not a day of year: PostgreSQL reads it as a
		// month and answers 22008. wadjet already agreed on these.
		{in: "2026-000-12", state: field},
		{in: "2026-367-12", state: field, note: "one past the last DOY value"},
		{in: "2026-400-12", state: field},
		{in: "2026-999-12", state: field},

		// FOUR OR MORE digits in the month is a year-shaped token PostgreSQL
		// ACCEPTS as the month. Refusing "more than two digits" would have
		// broken these, which is why the check tests for exactly three.
		{in: "2026-0003-12", state: accept, want: 20524},
		{in: "2026-00003-12", state: accept, want: 20524},
		{in: "2026-000003-12", state: accept, want: 20524},
		// A three-digit DAY is accepted by BOTH: year and month are already
		// decided by then, so PostgreSQL's day-of-year branch cannot fire.
		{in: "2026-01-003", state: accept, want: 20456},
		{in: "2026-01-0003", state: accept, want: 20456},
		{in: "2026/01/003", state: accept, want: 20456},

		// Unmoved by this change, and here so a future edit to splitDateFields
		// has to keep them.
		{in: "2024-1-1", state: accept, want: 19723},
		{in: "2026.03.12", state: accept, want: 20524},
		{in: "02026-01-02", state: accept, want: 20455},
		{in: "20260102", state: accept, want: 20455},
		{in: "2024-13-01", state: field},
		{in: "2024-02-30", state: field},
		{in: "2026-01-02 03:04:05", state: accept, want: 20455},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseDateDays(tc.in)
			if tc.state == accept {
				if err != nil {
					t.Fatalf("%q refused (%v), but PostgreSQL 17.11 accepts it as day %d%s",
						tc.in, err, tc.want, noteOf(tc.note))
				}
				if got != tc.want {
					t.Errorf("%q = day %d, want %d (live PostgreSQL 17.11)", tc.in, got, tc.want)
				}
				return
			}
			if err == nil {
				t.Fatalf("%q accepted as day %d, but PostgreSQL 17.11 raises %s%s — this is "+
					"the WRITE path, so an over-accept STORES a date no PostgreSQL client "+
					"could have written (#641)", tc.in, got, tc.state, noteOf(tc.note))
			}
			c, ok := err.(interface{ SQLState() string })
			if !ok {
				t.Fatalf("%q: %v carries no SQLSTATE", tc.in, err)
			}
			if c.SQLState() != tc.state {
				t.Errorf("%q: SQLSTATE %s, want %s (live PostgreSQL 17.11)%s",
					tc.in, c.SQLState(), tc.state, noteOf(tc.note))
			}
		})
	}
}

func noteOf(s string) string {
	if s == "" {
		return ""
	}
	return " — " + s
}

// TestYearZeroIsRefusedAtEveryDoor: ParseDateDays is the ONE string→date
// conversion (its doc comment lists the five callers), so refusing year zero
// there refuses it at the ingest boundary, in the writer and in the filter
// kernel at once. This holds the container walk to it too, since a nested
// DATE reaches storage through a different function.
func TestYearZeroIsRefusedAtEveryDoor(t *testing.T) {
	col := Column{Name: "d", Type: TypeDate, Nullable: true}
	if err := ValidateNestedLeaves(col, "0000-01-01"); err == nil {
		t.Error("the ingest boundary admitted year zero at a top-level DATE leaf")
	}
	arr := Column{Name: "a", Type: TypeArray, ElementType: &col}
	if err := ValidateNestedLeaves(arr, []any{"2026-01-01", "0000-01-01"}); err == nil {
		t.Error("the ingest boundary admitted year zero inside an ARRAY")
	}
	row := Column{Name: "r", Type: TypeRow, Fields: []Column{col}}
	if err := ValidateNestedLeaves(row, map[string]any{"d": "2026-003-12"}); err == nil {
		t.Error("the ingest boundary admitted a three-digit month inside a ROW")
	}
	// The boundary from the other side: an ordinary date still passes every
	// one of those doors.
	if err := ValidateNestedLeaves(arr, []any{"2026-01-01", "0001-01-01"}); err != nil {
		t.Errorf("an ordinary date was refused inside an ARRAY: %v", err)
	}
}
