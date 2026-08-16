package expr

import "testing"

// ClickBench-surfaced scalar gaps: date_trunc minute/second/week/quarter
// units (Q43) and SQL-style \N backreferences in regexp_replace (Q29).
func TestDateTruncSubHourUnits(t *testing.T) {
	// 2013-07-15 10:23:45 UTC = 1373883825 (a Monday).
	const ts = int64(1373883825)
	cases := []struct {
		unit, want string
	}{
		{"minute", "2013-07-15T10:23:00Z"},
		{"second", "2013-07-15T10:23:45Z"},
		{"hour", "2013-07-15T10:00:00Z"},
		{"week", "2013-07-15T00:00:00Z"},
		{"quarter", "2013-07-01T00:00:00Z"},
	}
	for _, tc := range cases {
		got := fnDateTrunc([]any{tc.unit, ts})
		if got != tc.want {
			t.Errorf("date_trunc(%q): got %v, want %q", tc.unit, got, tc.want)
		}
	}
}

func TestRegexpReplaceBackrefs(t *testing.T) {
	cases := []struct {
		in, pat, repl, want string
	}{
		{"http://www.abc.com/x/y", `^https?://(?:www\.)?([^/]+)/.*$`, `\1`, "abc.com"},
		{"a-b", `(a)-(b)`, `\2\1`, "ba"},
		{"x", `x`, `$`, "$"},
		{"x", `x`, `\\1`, `\1`},
		{"noop", `zzz`, `\1`, "noop"},
	}
	for _, tc := range cases {
		got := fnRegexpReplace([]any{tc.in, tc.pat, tc.repl})
		if got != tc.want {
			t.Errorf("regexp_replace(%q, %q, %q): got %v, want %q", tc.in, tc.pat, tc.repl, got, tc.want)
		}
	}
}
