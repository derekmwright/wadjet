package sql

import (
	"strings"
	"testing"
)

// selectExprString parses a SELECT and returns the printed AST of its first
// projection.
func selectExprString(t *testing.T, sql string) string {
	t.Helper()
	parsed, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract %q: %v", sql, err)
	}
	if len(info.Columns) == 0 {
		t.Fatalf("no projection columns for %q", sql)
	}
	if info.Columns[0].ASTExpr == nil {
		t.Fatalf("projection 0 of %q has no expression", sql)
	}
	return info.Columns[0].ASTExpr.String()
}

// TestAtTimeZoneRewrite pins the shape AT TIME ZONE parses to: PostgreSQL's
// own canonical form timezone(zone, timestamp), zone first.
func TestAtTimeZoneRewrite(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			// The statement DataGrip sends at connection time. Before AT TIME
			// ZONE parsed, it failed with "expected ) after EXTRACT".
			name: "datagrip startup_time",
			sql:  "select round(extract(epoch from pg_postmaster_start_time() at time zone 'UTC')) as startup_time",
			want: "round(epoch(timezone('UTC', pg_postmaster_start_time())))",
		},
		{
			name: "bare column",
			sql:  "SELECT ts AT TIME ZONE 'UTC' FROM t",
			want: "timezone('UTC', ts)",
		},
		{
			name: "lowercase operator",
			sql:  "SELECT ts at time zone 'UTC' FROM t",
			want: "timezone('UTC', ts)",
		},
		{
			name: "mixed case operator",
			sql:  "SELECT ts At TiMe ZoNe 'UTC' FROM t",
			want: "timezone('UTC', ts)",
		},
		{
			name: "inside EXTRACT",
			sql:  "SELECT EXTRACT(EPOCH FROM ts AT TIME ZONE 'UTC') FROM t",
			want: "epoch(timezone('UTC', ts))",
		},
		{
			// %left AT — a chain groups left to right, so the zone operand
			// never swallows the next AT TIME ZONE.
			name: "chained is left associative",
			sql:  "SELECT ts AT TIME ZONE 'UTC' AT TIME ZONE 'GMT' FROM t",
			want: "timezone('GMT', timezone('UTC', ts))",
		},
		{
			// PostgreSQL puts AT above the multiplicative operators.
			name: "binds tighter than multiplication",
			sql:  "SELECT a * ts AT TIME ZONE 'UTC' FROM t",
			want: "a * timezone('UTC', ts)",
		},
		{
			name: "binds tighter than addition",
			sql:  "SELECT ts AT TIME ZONE 'UTC' + 1 FROM t",
			want: "timezone('UTC', ts) + 1",
		},
		{
			name: "binds tighter than subtraction on the left",
			sql:  "SELECT 1 - ts AT TIME ZONE 'UTC' FROM t",
			want: "1 - timezone('UTC', ts)",
		},
		{
			// ...and below unary minus, which PostgreSQL declares above AT.
			name: "binds looser than unary minus",
			sql:  "SELECT -ts AT TIME ZONE 'UTC' FROM t",
			want: "timezone('UTC', -ts)",
		},
		{
			// TYPECAST is above AT, so the cast belongs to the zone operand.
			name: "cast binds to the zone operand",
			sql:  "SELECT ts AT TIME ZONE 'UTC'::text FROM t",
			want: "timezone(cast('UTC' as text), ts)",
		},
		{
			name: "function call operand",
			sql:  "SELECT now() AT TIME ZONE 'UTC' FROM t",
			want: "timezone('UTC', now())",
		},
		{
			name: "nested inside a function argument",
			sql:  "SELECT date_trunc('day', ts AT TIME ZONE 'UTC') FROM t",
			want: "date_trunc('day', timezone('UTC', ts))",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := selectExprString(t, tt.sql); got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// TestAtTimeZoneInPredicate covers the comparison-precedence half: AT binds
// tighter than every comparison operator, so the operator's result is the
// comparison's operand rather than the other way round.
func TestAtTimeZoneInPredicate(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "equality",
			sql:  "SELECT 1 FROM t WHERE ts AT TIME ZONE 'UTC' = '2026-01-01T00:00:00Z'",
			want: "timezone('UTC', ts) = '2026-01-01T00:00:00Z'",
		},
		{
			name: "greater than on the right",
			sql:  "SELECT 1 FROM t WHERE '2026-01-01T00:00:00Z' > ts AT TIME ZONE 'UTC'",
			want: "'2026-01-01T00:00:00Z' > timezone('UTC', ts)",
		},
		{
			name: "between",
			sql:  "SELECT 1 FROM t WHERE ts AT TIME ZONE 'UTC' BETWEEN 'a' AND 'b'",
			want: "timezone('UTC', ts) between 'a' and 'b'",
		},
		{
			name: "is not null",
			sql:  "SELECT 1 FROM t WHERE ts AT TIME ZONE 'UTC' IS NOT NULL",
			want: "timezone('UTC', ts) is not null",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			info, err := ExtractSelect(parsed)
			if err != nil {
				t.Fatalf("extract: %v", err)
			}
			if info.WhereExpr == nil {
				t.Fatal("no WHERE expression")
			}
			if got := info.WhereExpr.String(); got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// TestAtTimeZoneUTCSpellings accepts every spelling of UTC; each is zero
// offset with no DST rule, which is the whole reason the conversion is
// well-defined here (see fnTimezone in internal/engine/expr).
func TestAtTimeZoneUTCSpellings(t *testing.T) {
	for _, zone := range []string{"UTC", "utc", "Utc", "GMT", "gmt", "Z", "Etc/UTC", "etc/gmt", " UTC "} {
		sql := "SELECT ts AT TIME ZONE '" + zone + "' FROM t"
		if _, err := Parse(sql); err != nil {
			t.Errorf("zone %q rejected: %v", zone, err)
		}
	}
}

// TestAtTimeZoneRejectsNonUTC is the guard against a wrong-signed conversion:
// a zone this type system cannot honor is named in a parse error rather than
// silently converted or silently nulled.
func TestAtTimeZoneRejectsNonUTC(t *testing.T) {
	for _, zone := range []string{"America/New_York", "Europe/London", "EST", "PST8PDT", "+05:00", ""} {
		sql := "SELECT ts AT TIME ZONE '" + zone + "' FROM t"
		_, err := Parse(sql)
		if err == nil {
			t.Errorf("zone %q accepted, expected rejection", zone)
			continue
		}
		if !strings.Contains(err.Error(), "AT TIME ZONE: only UTC is supported") {
			t.Errorf("zone %q: unhelpful error %v", zone, err)
		}
		if !strings.Contains(err.Error(), zone) {
			t.Errorf("zone %q: error does not name the zone: %v", zone, err)
		}
	}
}

// TestAtTimeZonePartialMatchConsumesNothing covers the reason the three words
// are matched as a unit: none of AT, TIME, or ZONE is reserved in this lexer,
// so `SELECT x at` has to keep meaning "x aliased at".
func TestAtTimeZonePartialMatchConsumesNothing(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantAlias string
	}{
		{"at alone is an alias", "SELECT x at FROM t", "at"},
		{"at as alias with AS", "SELECT x AS at FROM t", "at"},
		{"time alone is an alias", "SELECT x time FROM t", "time"},
		{"zone alone is an alias", "SELECT x zone FROM t", "zone"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			info, err := ExtractSelect(parsed)
			if err != nil {
				t.Fatalf("extract: %v", err)
			}
			if got := info.Columns[0].Alias; got != tt.wantAlias {
				t.Fatalf("alias = %q, want %q", got, tt.wantAlias)
			}
		})
	}

	// A column actually named `at`, `time`, or `zone` still resolves.
	for _, sql := range []string{"SELECT at FROM t", "SELECT time FROM t", "SELECT zone FROM t"} {
		if got := selectExprString(t, sql); got == "" {
			t.Fatalf("%q lost its projection", sql)
		}
	}

	// The full triple with no zone after it is a syntax error naming the
	// operator, not a half-applied rewrite.
	_, err := Parse("SELECT x at time zone FROM t")
	if err == nil {
		t.Fatal("expected an error for AT TIME ZONE with no zone")
	}
	if !strings.Contains(err.Error(), "expected time zone after AT TIME ZONE") {
		t.Fatalf("unhelpful error for a missing zone: %v", err)
	}
}

// TestAtTimeZoneRoundTrips holds the printer/parser fixed point the parser
// fuzz target asserts: the rewrite prints as a plain function call, so
// reparsing the printed form reaches the same AST.
func TestAtTimeZoneRoundTrips(t *testing.T) {
	for _, sql := range []string{
		"SELECT ts AT TIME ZONE 'UTC' FROM t",
		"SELECT ts AT TIME ZONE 'UTC' AT TIME ZONE 'GMT' FROM t",
		"SELECT EXTRACT(EPOCH FROM ts AT TIME ZONE 'UTC') FROM t",
	} {
		printed := selectExprString(t, sql)
		reprinted := selectExprString(t, "SELECT "+printed+" FROM t")
		if reprinted != printed {
			t.Fatalf("%q: printed %q, reprinted %q", sql, printed, reprinted)
		}
	}
}
