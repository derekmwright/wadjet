// SPDX-License-Identifier: MIT

package parquet

import (
	"errors"
	"testing"
)

// A numeric zone offset is read by PostgreSQL 17.11's DecodeTimezone digit
// rule: the digits after the sign are one integer, the hour, and with no `:`
// and more than two digits its last two digits are the minute — there is no
// run-together seconds form. The grammar used to split an un-coloned offset by
// its length (six digits as HH MM SS), so `+000130` read 00:01:30 where
// PostgreSQL reads 01:30, `+001500` read 00:15:00 (PostgreSQL 15:00),
// `+053000` read 05:30:00 (PostgreSQL 22009), and `+00130`, `+0000130`,
// `+00000000130` and `+05:` were 22007 (PostgreSQL reads them). Every cell is
// `TIMESTAMPTZ '2024-03-04 12:00:00<zone>'` at TimeZone UTC and `DATE` of the
// same text on postgres:17-alpine: the offset east of UTC in seconds, or the
// SQLSTATE both raised. `Z+05` is PostgreSQL's POSIX zone NAME (five hours
// WEST, 17:00 UTC), outside this grammar: 22007 here, temporal r25.
func TestTheZoneOffsetReadsPostgresDigitRule(t *testing.T) {
	cells := []struct {
		zone  string
		east  int    // PostgreSQL's offset east of UTC, seconds
		state string // PostgreSQL's SQLSTATE when it raises
	}{
		{"+5", 18000, ""},
		{"+05", 18000, ""},
		{"+530", 19800, ""},
		{"+0530", 19800, ""},
		{"+00130", 5400, ""},
		{"+0000130", 5400, ""},
		{"+00000000130", 5400, ""},
		{"+000130", 5400, ""},
		{"+001500", 54000, ""},
		{"+001", 60, ""},
		{"-00130", -5400, ""},
		{"+05:", 18000, ""},
		{"+5:3", 18180, ""},
		{"+05:3", 18180, ""},
		{"+1:30", 5400, ""},
		{"+05:030", 19800, ""},
		{"+005:30", 19800, ""},
		{"+05:-0", 18000, ""},
		{"-05:00:00", -18000, ""},
		{"-15:59:59", -57599, ""},
		{"-1559", -57540, ""},
		{"+15:59:59", 57599, ""},
		{"+053000", 0, "22009"},
		{"+0530:00", 0, "22009"},
		{"+16", 0, "22009"},
		{"+16.5", 0, "22009"},
		{"+16abc", 0, "22009"},
		{"+05:-30", 0, "22009"},
		{"+99999999999", 0, "22009"},
		{"+05:30:60", 0, "22009"},
		{"+05.5", 0, "22007"},
		{"+05:30:15:00", 0, "22007"},
		{"+", 0, "22007"},
		{"++05", 0, "22007"},
		{"+abc", 0, "22007"},
		{"Z", 0, ""},
		{"z", 0, ""},
		{"ZZ", 0, "22007"},
		{"Z+05", 0, "22007"}, // PostgreSQL: the POSIX zone Z+05 (−5 h); refused here (temporal r25)
		{"-24", 0, "22009"},
		{"+05:60", 0, "22009"},
		{"+05:30:-1", 0, "22009"},
		{"+05-30", 0, "22007"},
		{"+05:+30", 0, "22009"},
		{"+05+16", 0, "22009"},
		{"+05 -16", 0, "22009"},
		{"+05+05", 0, "22007"},
		{"+05 +05", 0, "22007"},
		{"+05:30-16", 0, "22007"},
		{"+05 16", 0, "22007"},
	}
	for _, c := range cells {
		text := "2024-03-04 12:00:00" + c.zone
		_, east, _, ok := ParseTimestampZone(text)
		_, terr := ParseTimestampMillis(text)
		_, derr := ParseDateDays(text)
		if c.state == "" {
			if !ok || east != c.east || terr != nil || derr != nil {
				t.Errorf("%q: offset %d ok=%v timestamp err %v date err %v; want offset %d (PostgreSQL 17.11)", text, east, ok, terr, derr, c.east)
			}
			continue
		}
		if ok {
			t.Errorf("%q: read offset %d; want SQLSTATE %s (PostgreSQL 17.11)", text, east, c.state)
		}
		for _, err := range []error{terr, derr} {
			var st interface{ SQLState() string }
			if !errors.As(err, &st) || st.SQLState() != c.state {
				t.Errorf("%q: %v; want SQLSTATE %s (PostgreSQL 17.11)", text, err, c.state)
			}
		}
	}
}
