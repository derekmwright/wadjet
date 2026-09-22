// SPDX-License-Identifier: MIT

package pgwire

import (
	"math"
	"strings"
	"testing"
)

// TestArcTSABinaryTimestampParameterIsTheInstantItNames: a binary
// timestamp / timestamptz parameter (int64 microseconds since 2000-01-01, the
// format pgx sends by default) binds as the SAME value as the text parameter
// and the TIMESTAMP literal spelling that instant (#1266's producer census).
//
// At 260fc569 the decode was `pgEpoch.Add(time.Duration(micros) *
// time.Microsecond)` rendered with a whole-second layout: the fraction was
// dropped (`12:30:45.5` bound as `12:30:45`), and time.Duration's nanosecond
// range wrapped any instant more than ~292 years from 2000 (1600 and 9999 bound
// as other years). PostgreSQL's ±infinity (the int64 extremes) decoded to a
// wrapped instant; the engine's TIMESTAMP has no infinity, so it is refused.
func TestArcTSABinaryTimestampParameterIsTheInstantItNames(t *testing.T) {
	_, srv := setupRealDB(t)
	client := newPGClient(t, srv.Addr())
	client.startup("wadjet", "wadjet")
	defer client.terminate()

	for _, tc := range []struct {
		name   string
		micros int64  // since 2000-01-01T00:00:00Z
		text   string // the same instant as PostgreSQL's text
		want   string // what the engine renders (millisecond carrier)
	}{
		{"fraction", 771769845500000, "2024-06-15 12:30:45.5", "2024-06-15 12:30:45.5"},
		{"whole_second", 771769845000000, "2024-06-15 12:30:45", "2024-06-15 12:30:45"},
		{"before_1708", -12617661599750000, "1600-02-29 06:00:00.25", "1600-02-29 06:00:00.25"},
		{"after_2292", 252455615999999000, "9999-12-31 23:59:59.999", "9999-12-31 23:59:59.999"},
		// Sub-millisecond: floored to the carrier, as the text literal is.
		{"pre_1970_sub_ms", -960867739876544, "1969-07-20 20:17:40.123456", "1969-07-20 20:17:40.123"},
	} {
		for _, oid := range []uint32{oidTimestamp, oidTimestampTZ} {
			for _, p := range []struct {
				form  string
				param boundParam
			}{
				{"binary", binaryParam(oid, be64b(tc.micros))},
				{"text", textParam(oid, tc.text)},
			} {
				t.Run(tc.name+"/"+p.form, func(t *testing.T) {
					_, _, rows, tag := client.paramQuery(
						"SELECT CAST(CAST($1 AS TIMESTAMP) AS VARCHAR) AS s, CAST($1 AS TIMESTAMP) = TIMESTAMP '"+
							tc.text+"' AS eq", []boundParam{p.param})
					if strings.HasPrefix(tag, "ERROR") {
						t.Fatalf("oid %d: %s", oid, tag)
					}
					if len(rows) != 1 || rows[0][0] != tc.want || rows[0][1] != "t" {
						t.Fatalf("oid %d: got %v, want [[%s t]]", oid, rows, tc.want)
					}
				})
			}
		}
	}

	// The refusal is the extended protocol's error state: nothing past the
	// Bind runs. At 260fc569 a refused Bind left the previous portal's SQL in
	// place, so the Execute that followed answered THAT statement's rows —
	// the prior query here, run again, with its CommandComplete after the
	// error. The previous statement below is chosen so its rows are
	// recognizable.
	for _, bad := range []struct {
		name  string
		param boundParam
	}{
		{"timestamp +infinity", binaryParam(oidTimestamp, be64b(math.MaxInt64))},
		{"timestamptz -infinity", binaryParam(oidTimestampTZ, be64b(math.MinInt64))},
		// Not a timestamp: the error-state half on its own, through a
		// refusal that already existed at 260fc569.
		{"int4 of the wrong width", binaryParam(oidInt4, []byte{0, 2})},
	} {
		if _, _, _, tag := client.paramQuery("SELECT 'previous portal' AS p", nil); strings.HasPrefix(tag, "ERROR") {
			t.Fatal(tag)
		}
		_, _, rows, tag := client.paramQuery("SELECT $1 AS v", []boundParam{bad.param})
		if !strings.HasPrefix(tag, "ERROR") || len(rows) != 0 {
			t.Errorf("%s: tag %q rows %v, want a refusal and no rows", bad.name, tag, rows)
		}
		// The connection is usable after the refusal's Sync.
		if _, _, rows, tag := client.paramQuery("SELECT 1 AS one", nil); strings.HasPrefix(tag, "ERROR") || len(rows) != 1 || rows[0][0] != "1" {
			t.Errorf("after the refusal: tag %q rows %v", tag, rows)
		}
	}
}
