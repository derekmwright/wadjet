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
// reset PostgreSQL to its default zone (UTC there and here). `Z` and an
// unknown name are PostgreSQL's 22023 (invalid value for parameter
// "TimeZone"); here they are the same refusal as any other zone (0A000).
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
		"SET TimeZone = '+05:30'", "SET TimeZone = 'Not/AZone'", "SET TimeZone = 'Z'", "SET timezone = 'Asia/Kolkata'",
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
