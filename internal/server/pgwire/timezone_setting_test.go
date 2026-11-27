// SPDX-License-Identifier: MIT

package pgwire

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// A SESSION OF THIS ENGINE RUNS AT UTC, SO A SET NAMING ANY OTHER ZONE IS
// REFUSED (#1680 line d). `SET TimeZone = 'America/New_York'` was stored and
// echoed by SHOW TimeZone while every answer stayed UTC's —
// `TIMESTAMPTZ '2024-03-10 02:30:00'` answered 02:30 where PostgreSQL 17.11
// under that zone answers 2024-03-10 03:30:00-04 — and `SET TIME ZONE
// 'America/New_York'` (no `=` / TO) was accepted without being parsed at all.
//
// The accepted spellings are the ones PostgreSQL 17.11 runs at offset zero in
// every season, measured on postgres:17-alpine (initdb --locale=C
// --encoding=UTF8): each SET, then `TIMESTAMPTZ '2024-07-01 12:00:00+00'` and
// `TIMESTAMPTZ '2024-01-01 12:00:00+00'` printed `+00`. DEFAULT and LOCAL
// reset PostgreSQL to its default zone (UTC there and here). A zone
// PostgreSQL knows but this engine cannot run in is the named 0A000 refusal;
// a value PostgreSQL does not accept as a TimeZone at all (`Not/AZone`, `Z`)
// is PostgreSQL's own 22023 (invalid value for parameter "TimeZone"), and
// nothing is stored for either.
func TestSetTimeZoneRefusesEveryZoneButUTC(t *testing.T) {
	accepted := []string{
		"SET TimeZone = 'UTC'", "SET TimeZone = 'utc'", "SET TimeZone TO 'UTC'", "SET timezone = 'Etc/UTC'",
		"SET TimeZone = 'etc/utc'", "SET TimeZone = 'UCT'", "SET TimeZone = 'Etc/UCT'", "SET TimeZone = 'Universal'",
		"SET TimeZone = 'Etc/Universal'", "SET TimeZone = 'Zulu'", "SET TimeZone = 'Etc/Zulu'", "SET TimeZone = 'GMT'",
		"SET TimeZone = 'Etc/GMT'", "SET TimeZone = 'GMT0'", "SET TimeZone = 'Etc/GMT0'", "SET TimeZone = 'GMT+0'",
		"SET TimeZone = 'GMT-0'", "SET TimeZone = 'Etc/GMT+0'", "SET TimeZone = 'Etc/GMT-0'", "SET TimeZone = 'Greenwich'",
		"SET TimeZone = 'Etc/Greenwich'", "SET TimeZone = 'UTC0'", "SET TimeZone = '+00'", "SET TimeZone = '+00:00'",
		"SET TimeZone = '0'", "SET TimeZone = 0", "SET TimeZone TO DEFAULT", "SET TimeZone = DEFAULT",
		"SET TIMEZONE = 'UTC'", `SET "TimeZone" = 'UTC'`, "SET SESSION TimeZone = 'UTC'",
		"SET TIME ZONE 'UTC'", "SET TIME ZONE LOCAL", "SET TIME ZONE DEFAULT", "SET TIME ZONE 0",
		"SET TIME ZONE INTERVAL '+00:00' HOUR TO MINUTE", "set time zone 'Etc/UTC'",
	}
	refused := []string{
		"SET TimeZone = 'America/New_York'", "SET TimeZone = 'Asia/Kolkata'", "SET TimeZone = 'Europe/London'",
		"SET TimeZone = '+05:30'", "SET timezone = 'Asia/Kolkata'", "SET TimeZone = 'AMERICA/NEW_YORK'",
		"SET TimeZone = 'EST5EDT'", "SET TimeZone = 'UTC+3'", "SET TimeZone = '<+05>-5'", "SET TimeZone = '-5'",
		`SET "TimeZone" = 'Asia/Kolkata'`, "SET SESSION TimeZone = 'Asia/Kolkata'", "SET LOCAL TimeZone = 'Asia/Kolkata'",
		"SET TimeZone TO 'America/New_York'", "SET TIME ZONE 'America/New_York'", "SET TIME ZONE -5",
		"SET TIME ZONE 5.5", "SET TIME ZONE INTERVAL '-08:00' HOUR TO MINUTE", "set time zone 'asia/kolkata'",
	}
	for _, sql := range accepted {
		c := &pgConn{sessionVars: map[string]string{}}
		if err := c.handleSet(sql); err != nil {
			t.Errorf("%s: refused (%v); PostgreSQL 17.11 runs it at offset zero", sql, err)
		}
	}
	for _, sql := range refused {
		c := &pgConn{sessionVars: map[string]string{}}
		err := c.handleSet(sql)
		if err == nil || sqlerr.StateOf(err) != "0A000" || sqlerr.ReasonOf(err) != "timezone" {
			t.Errorf("%s: %v (state %q, reason %q), want the 0A000 timezone refusal", sql, err, sqlerr.StateOf(err), sqlerr.ReasonOf(err))
		}
		if v, ok := c.sessionVars["timezone"]; ok {
			t.Errorf("%s: refused but stored %q (SHOW TimeZone would echo it)", sql, v)
		}
	}
	// PostgreSQL 17.11's 22023, with its sentence: each of these is
	// `ERROR: invalid value for parameter "TimeZone": "<value>"` there.
	invalid := map[string]string{
		"SET TimeZone = 'Not/AZone'":   "Not/AZone",
		"SET TimeZone = 'Z'":           "Z",
		"SET TimeZone = 'PST'":         "PST",
		"SET TimeZone = 'abc'":         "abc",
		"SET TimeZone = '1000'":        "1000",
		"SET TimeZone TO 'Europe/Lon'": "Europe/Lon",
		"SET LOCAL TimeZone = 'IST'":   "IST",
		"SET TIME ZONE 'Not/AZone'":    "Not/AZone",
		"SET TIME ZONE 'Z'":            "Z",
	}
	for sql, val := range invalid {
		c := &pgConn{sessionVars: map[string]string{}}
		err := c.handleSet(sql)
		want := `invalid value for parameter "TimeZone": "` + val + `"`
		if sqlerr.StateOf(err) != "22023" || err.Error() != want {
			t.Errorf("%s: %v (state %q), want 22023 %s", sql, err, sqlerr.StateOf(err), want)
		}
		if v, ok := c.sessionVars["timezone"]; ok {
			t.Errorf("%s: refused but stored %q", sql, v)
		}
	}
	// Neighbours keep their own rules: IntervalStyle's refusal, and a SET
	// of an unrelated variable is still stored.
	c := &pgConn{sessionVars: map[string]string{}}
	if err := c.handleSet("SET IntervalStyle = 'iso_8601'"); sqlerr.ReasonOf(err) != "intervalstyle" {
		t.Errorf("SET IntervalStyle = 'iso_8601': %v, want its own refusal", err)
	}
	if err := c.handleSet("SET search_path = public"); err != nil || c.sessionVars["search_path"] != "public" {
		t.Errorf("SET search_path = public: %v, stored %q", err, c.sessionVars["search_path"])
	}
}

// validTimeZone is PostgreSQL 17.11's accept-set for a TimeZone value, so an
// unknown zone is its 22023 and only a zone it knows is the 0A000 refusal
// (#1680 line d, review of round 1). Every value below was SET on
// postgres:17-alpine (initdb --locale=C --encoding=UTF8, TimeZone UTC,
// DateStyle ISO, MDY): `accepted` answered SET, `rejected` answered 22023
// (`invalid value for parameter "TimeZone"`, or for an offset with seconds
// `time zone … appears to use leap seconds`).
func TestValidTimeZoneIsPostgreSQLsAcceptSet(t *testing.T) {
	accepted := []string{
		"America/New_York", "AMERICA/NEW_YORK", "America/new_york", "us/eastern", "US/Eastern", "Asia/Calcutta",
		"Etc/Gmt+5", "Etc/GMT+5", "EST", "EST5EDT", "PST8PDT", "posixrules", "Factory", "+05:30", "15:59", "5:30",
		"+05:30:00", "UTC+3", "GMT+5", "<+05>-5", "<ab>5", "<>5", "XYZ5", "ab5", "XY5", "XYZ+25", "XYZ167", "XYZ5:5",
		"+5ABC", "XYZ5ABC", "XYZ5ABC,M3.2.0,M11.1.0", "XYZ5ABC4,M3.2.0/2,M11.1.0/2", "25", "-5", "5.5", "+99",
		"-15", "16", "-16", "167", "-167.9", "1e1", "0x10", "+5", "  5", "5 ",
	}
	rejected := []string{
		"Not/AZone", "Z", "z", "PST", "IST", "foo", "abc", "XYZ", "localtime", "", "UTC ", " UTC", "Europe/London ",
		"America", "Europe/Lon", "+05:30:15", "XYZ5:30:15", "1000", "168", "XYZ168", "XYZ-5:60", "A-B5",
		"XYZ5ABC,garbage", "inf", "nan",
	}
	for _, v := range accepted {
		if !validTimeZone(v) {
			t.Errorf("validTimeZone(%q) = false; PostgreSQL 17.11 accepts it", v)
		}
	}
	for _, v := range rejected {
		if validTimeZone(v) {
			t.Errorf("validTimeZone(%q) = true; PostgreSQL 17.11 answers 22023", v)
		}
	}
}
