package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #544: a TIMESTAMP coerced to text renders the INSTANT, at every site, and
// renders it the way the wire does.
//
// `CAST(c_ts AS STRING)` and `LIKE` over a bare TIMESTAMP column closed with
// #521. What was left, measured in the arc's ROUND0 against live PostgreSQL
// 17.11 and against wadjet's own pgwire door:
//
//	c_ts || ''                       1700000000000        the epoch box
//	CONCAT(c_ts, 'x')                1700000000000x
//	UPPER(c_ts)                      1700000000000
//	FORMAT('%s', c_ts)               %!s(int64=17000…)    a Go verb's complaint
//	CAST(CAST(… AS TIMESTAMP) AS TEXT)  826727136500      a NON-COLUMN operand
//	timestamp with .5 on the wire     1996-03-13 14:25:36.500   PG says .5
//
// Two mechanisms, both closed here. FuncCall.formatTemporalArgs had a DATE arm
// and no TIMESTAMP one, so every string function read the raw box; and
// expr.boxedTextOperand resolved only a bare *ColRef, so a timestamp produced
// by any other node reached CAST AS TEXT unrendered. The fraction is
// batch.FormatTimestamp, which pgwire's send path calls too — one formatter,
// so fixing it fixes both doors at once.
//
// typemx.c_ts at id 0 is epoch ms 1700000000000. Live PostgreSQL 17.11:
// `(to_timestamp(1700000000000/1000.0) AT TIME ZONE 'UTC')::text` is
// "2023-11-14 22:13:20".
func TestTimestampHasOneRenderingAtEverySite(t *testing.T) {
	ctx := context.Background()
	db := tmOpen(t)
	tbl := typematrix.Table
	const want = "2023-11-14 22:13:20"
	for _, c := range []struct{ name, sql string }{
		// The two that closed with #521, kept as the controls that say which
		// half moved.
		{"cast_as_string", `SELECT CAST(c_ts AS STRING) AS v FROM ` + tbl + ` WHERE id = 0`},
		{"cast_as_text", `SELECT CAST(c_ts AS TEXT) AS v FROM ` + tbl + ` WHERE id = 0`},
		// The concatenation operator and its function spelling.
		{"concat_operator", `SELECT c_ts || '' AS v FROM ` + tbl + ` WHERE id = 0`},
		{"concat_function", `SELECT CONCAT(c_ts, '') AS v FROM ` + tbl + ` WHERE id = 0`},
		// Every other string function that reads its argument as text.
		{"upper", `SELECT UPPER(c_ts) AS v FROM ` + tbl + ` WHERE id = 0`},
		{"lower", `SELECT LOWER(c_ts) AS v FROM ` + tbl + ` WHERE id = 0`},
		{"trim", `SELECT TRIM(c_ts) AS v FROM ` + tbl + ` WHERE id = 0`},
		{"replace", `SELECT REPLACE(c_ts, 'zzz', '') AS v FROM ` + tbl + ` WHERE id = 0`},
		{"format", `SELECT FORMAT('%s', c_ts) AS v FROM ` + tbl + ` WHERE id = 0`},
	} {
		t.Run(c.name, func(t *testing.T) {
			res, err := db.Query(ctx, c.sql)
			if err != nil {
				t.Fatalf("%v\n  SQL: %s", err, c.sql)
			}
			if len(res.Rows) != 1 || res.Rows[0]["v"] != want {
				t.Errorf("= %#v, want %q — a TIMESTAMP coerced to text renders the INSTANT, "+
					"and the epoch box is what the same column answered here while pgwire "+
					"answered the instant for it (#544)\n  SQL: %s", res.Rows, want, c.sql)
			}
		})
	}
	// The functions that measure or slice the rendered text: they must see the
	// instant's characters, not the epoch's digits. `LENGTH` of
	// "2023-11-14 22:13:20" is 19; of "1700000000000" it is 13.
	for _, c := range []struct {
		name, sql string
		want      any
	}{
		{"length", `SELECT LENGTH(c_ts) AS v FROM ` + tbl + ` WHERE id = 0`, int32(19)},
		{"substr", `SELECT SUBSTR(c_ts, 1, 4) AS v FROM ` + tbl + ` WHERE id = 0`, "2023"},
		{"strpos", `SELECT STRPOS(c_ts, '-') AS v FROM ` + tbl + ` WHERE id = 0`, int32(5)},
		{"starts_with", `SELECT STARTS_WITH(c_ts, '2023-11') AS v FROM ` + tbl + ` WHERE id = 0`, true},
		{"like", `SELECT COUNT(*) AS v FROM ` + tbl + ` WHERE c_ts LIKE '2023-11-14%'`, int64(104)},
	} {
		t.Run(c.name, func(t *testing.T) {
			res, err := db.Query(ctx, c.sql)
			if err != nil {
				t.Fatalf("%v\n  SQL: %s", err, c.sql)
			}
			if len(res.Rows) != 1 || res.Rows[0]["v"] != c.want {
				t.Errorf("= %#v, want %#v (#544)\n  SQL: %s", res.Rows, c.want, c.sql)
			}
		})
	}
	// The operand that is NOT a bare column: a nested CAST. boxedTextOperand
	// resolved only a *ColRef, so this answered the epoch on both doors while
	// the same cast over a COLUMN answered the instant — one expression, two
	// answers, decided by what produced the value.
	for _, c := range []struct{ name, sql, want string }{
		{"nested_cast_whole_second",
			`SELECT CAST(CAST('1996-03-13 14:25:36' AS TIMESTAMP) AS TEXT) AS v FROM ` + tbl +
				` WHERE id = 0`, "1996-03-13 14:25:36"},
		// PostgreSQL TRIMS the fraction: `.5`, not `.500`. Both measured live.
		{"nested_cast_half_second",
			`SELECT CAST(CAST('1996-03-13 14:25:36.5' AS TIMESTAMP) AS TEXT) AS v FROM ` + tbl +
				` WHERE id = 0`, "1996-03-13 14:25:36.5"},
		{"nested_cast_two_digit_fraction",
			`SELECT CAST(CAST('1996-03-13 14:25:36.12' AS TIMESTAMP) AS TEXT) AS v FROM ` + tbl +
				` WHERE id = 0`, "1996-03-13 14:25:36.12"},
		{"nested_cast_three_digit_fraction",
			`SELECT CAST(CAST('1996-03-13 14:25:36.123' AS TIMESTAMP) AS TEXT) AS v FROM ` + tbl +
				` WHERE id = 0`, "1996-03-13 14:25:36.123"},
		// A year below 1000 keeps four digits on the server: `0999-03-13`.
		{"nested_cast_short_year",
			`SELECT CAST(CAST('0999-03-13 14:25:36' AS TIMESTAMP) AS TEXT) AS v FROM ` + tbl +
				` WHERE id = 0`, "0999-03-13 14:25:36"},
		{"nested_cast_through_a_string_function",
			`SELECT UPPER(CAST('1996-03-13 14:25:36' AS TIMESTAMP)) AS v FROM ` + tbl +
				` WHERE id = 0`, "1996-03-13 14:25:36"},
		{"nested_cast_date",
			`SELECT CAST(CAST('1996-03-13' AS DATE) AS TEXT) AS v FROM ` + tbl +
				` WHERE id = 0`, "1996-03-13"},
	} {
		t.Run(c.name, func(t *testing.T) {
			res, err := db.Query(ctx, c.sql)
			if err != nil {
				t.Fatalf("%v\n  SQL: %s", err, c.sql)
			}
			if len(res.Rows) != 1 || res.Rows[0]["v"] != c.want {
				t.Errorf("= %#v, want %q (live PostgreSQL 17.11)\n  SQL: %s",
					res.Rows, c.want, c.sql)
			}
		})
	}
	// The BOUNDARY (protocol rule 11). DURATION is NOT a timestamp: #834 made
	// PORT/PROTOCOL/DURATION declare int4/int4/int8-nanoseconds on the wire,
	// so a DURATION rendered as its nanosecond integer AGREES with its own
	// declaration and with the projection. PostgreSQL has no such type, so
	// there is no server answer to follow — and a pass that rendered "every
	// integer-boxed temporal type" would have moved this cell away from the
	// engine's own wire.
	for _, c := range []struct {
		name, sql string
		want      any
	}{
		{"ctl_duration_cast", `SELECT CAST(c_dur AS STRING) AS v FROM ` + tbl + ` WHERE id = 1`, "1000000"},
		{"ctl_duration_concat", `SELECT c_dur || '' AS v FROM ` + tbl + ` WHERE id = 1`, "1000000"},
		// And a plain integer column, which must not acquire a rendering.
		{"ctl_int_concat", `SELECT c_i64 || '' AS v FROM ` + tbl + ` WHERE id = 1`, "1000003"},
	} {
		t.Run(c.name, func(t *testing.T) {
			res, err := db.Query(ctx, c.sql)
			if err != nil {
				t.Fatalf("%v\n  SQL: %s", err, c.sql)
			}
			if len(res.Rows) != 1 || res.Rows[0]["v"] != c.want {
				t.Errorf("= %#v, want %#v", res.Rows, c.want)
			}
		})
	}
	// The FUNCTION RESULTS. This block was a single pin recording that a
	// timestamp-VALUED scalar function renders RFC3339 — `2023-11-14T00:00:00Z`
	// where the very column it read renders `2023-11-14 00:00:00` and where
	// PostgreSQL 17.11 renders the same. "One rendering at every site" was
	// true of the cast, the concatenation and the wire and false of the
	// twenty-odd expressions that PRODUCE an instant, which is the larger half
	// of the surface a client meets.
	//
	// Every one of them now ends at expr.formatInstant, which is
	// batch.FormatTimestamp — the renderer the cast, the sort key and pgwire's
	// send path already share. Each cell below was measured on live PostgreSQL
	// 17.11.
	//
	// Since #868 a timestamp-valued FUNCTION declares TIMESTAMP, so the
	// embedded door hands back the epoch milliseconds a TIMESTAMP COLUMN hands
	// back — which is what "one rendering" means here: the value is the
	// instant and batch.FormatTimestamp renders it at the OUTPUT, exactly as
	// it does for `SELECT c_ts`. tsRendered below renders whichever of the two
	// boxes arrives, so these cells assert the TEXT a client reads on either
	// side of that change.
	for _, c := range []struct{ name, sql, want string }{
		{"date_trunc_day", `SELECT DATE_TRUNC('day', c_ts) AS v FROM ` + tbl +
			` WHERE id = 0`, "2023-11-14 00:00:00"},
		{"date_trunc_hour", `SELECT DATE_TRUNC('hour', c_ts) AS v FROM ` + tbl +
			` WHERE id = 0`, "2023-11-14 22:00:00"},
		{"date_trunc_month", `SELECT DATE_TRUNC('month', c_ts) AS v FROM ` + tbl +
			` WHERE id = 0`, "2023-11-01 00:00:00"},
		// The unit that does nothing still renders the one way — the arm that
		// returns its argument had its own Format call too.
		{"date_trunc_milliseconds", `SELECT DATE_TRUNC('milliseconds', c_ts) AS v FROM ` + tbl +
			` WHERE id = 0`, "2023-11-14 22:13:20"},
		// And the same instant reached three other ways: an epoch, a zone
		// conversion, and interval arithmetic over text carrying a clock.
		{"from_unixtime", `SELECT FROM_UNIXTIME(1699999999) AS v FROM ` + tbl +
			` WHERE id = 0`, "2023-11-14 22:13:19"},
		{"timezone_utc", `SELECT TIMEZONE('UTC', c_ts) AS v FROM ` + tbl +
			` WHERE id = 0`, "2023-11-14 22:13:20"},
		{"text_instant_plus_interval",
			`SELECT '2023-11-14 22:13:20' + INTERVAL '1' HOUR AS v FROM ` + tbl +
				` WHERE id = 0`, "2023-11-14 23:13:20"},
	} {
		t.Run(c.name, func(t *testing.T) {
			res, err := db.Query(ctx, c.sql)
			if err != nil {
				t.Fatalf("%v\n  SQL: %s", err, c.sql)
			}
			if len(res.Rows) != 1 || tsRendered(res.Rows[0]["v"]) != c.want {
				t.Errorf("= %#v, want %q (live PostgreSQL 17.11). Every instant-valued "+
					"expression renders through expr.formatInstant; a `T` and a `Z` here "+
					"mean a second renderer came back (#544)\n  SQL: %s",
					res.Rows, c.want, c.sql)
			}
		})
	}
	// The DECLARATION, which was this test's pinned residual until #868. A
	// client that asks what the column IS gets `timestamp` now, the way the
	// server answers: `date_trunc` RETURNS timestamp there (OID 1114) and this
	// engine declared its result STRING, because the scalar registry carried
	// one static Ret per entry and no TIMESTAMP-valued function result
	// existed. expr.RetTimestamp was declared and unused.
	//
	// The VALUE half is unchanged and is the cells above: every instant-valued
	// function still produces expr.formatInstant's text, and the projection
	// materializes that text into the TIMESTAMP vector its declaration names
	// (batch.Vector.SetValue's TypeTimestamp string arm). One renderer, one
	// reading, and the wire now carries the OID a pgJDBC or psycopg client
	// reads to pick a column class.
	for _, c := range []struct {
		name, sql string
		want      parquet.TypeID
	}{
		{"date_trunc", `SELECT DATE_TRUNC('day', c_ts) AS v FROM ` + tbl + ` WHERE id = 0`,
			parquet.TypeTimestamp},
		{"from_unixtime", `SELECT FROM_UNIXTIME(1699999999) AS v FROM ` + tbl + ` WHERE id = 0`,
			parquet.TypeTimestamp},
		{"timezone", `SELECT TIMEZONE('UTC', c_ts) AS v FROM ` + tbl + ` WHERE id = 0`,
			parquet.TypeTimestamp},
		{"now", `SELECT NOW() AS v FROM ` + tbl + ` WHERE id = 0`, parquet.TypeTimestamp},
		{"current_timestamp", `SELECT CURRENT_TIMESTAMP() AS v FROM ` + tbl + ` WHERE id = 0`,
			parquet.TypeTimestamp},
		{"pg_postmaster_start_time",
			`SELECT PG_POSTMASTER_START_TIME() AS v FROM ` + tbl + ` WHERE id = 0`,
			parquet.TypeTimestamp},
		// The column itself, as the control: the function and the column
		// declare the same thing now.
		{"column", `SELECT c_ts AS v FROM ` + tbl + ` WHERE id = 0`, parquet.TypeTimestamp},
		// The boundary from the other side. These three PRODUCE text on
		// purpose and must keep declaring it: date_format's format string is
		// the caller's, to_iso8601's NAME is its contract, and at_timezone's
		// result is a wall clock in another zone whose offset is load-bearing.
		{"date_format", `SELECT DATE_FORMAT(c_ts, '%Y') AS v FROM ` + tbl + ` WHERE id = 0`,
			parquet.TypeString},
		{"to_iso8601", `SELECT TO_ISO8601(c_ts) AS v FROM ` + tbl + ` WHERE id = 0`,
			parquet.TypeString},
		{"at_timezone", `SELECT AT_TIMEZONE(c_ts, 'UTC') AS v FROM ` + tbl + ` WHERE id = 0`,
			parquet.TypeString},
	} {
		t.Run("declares_"+c.name, func(t *testing.T) {
			res, err := db.Query(ctx, c.sql)
			if err != nil {
				t.Fatalf("%v\n  SQL: %s", err, c.sql)
			}
			if len(res.ColumnMetas) != 1 {
				t.Fatalf("want one column meta, got %d", len(res.ColumnMetas))
			}
			if got := res.ColumnMetas[0].TypeID; got != c.want {
				t.Errorf("declares %v, want %v (PostgreSQL 17.11)\n  SQL: %s", got, c.want, c.sql)
			}
		})
	}

	// The CLOCK functions, pinned as a SHAPE because their value moves.
	//
	// They render through the same one renderer as everything above, and
	// PostgreSQL does NOT: it types all three `timestamptz`, whose text
	// carries a zone offset and six fractional digits —
	// `2026-09-04 21:21:01.708284+00`, measured — where this engine says
	// `2026-09-04 21:21:01.708`. Two structural gaps, not a formatting choice:
	// there is no zone-aware timestamp type to print an offset from, and the
	// instant is epoch MILLISECONDS, so three digits is the most any rendering
	// here can carry.
	//
	// Before #544's second pass these answered RFC3339, which named the zone
	// and still was not PostgreSQL's text; routing them through the one
	// renderer is what that pass requires, and the offset it costs is recorded
	// in ADR-0012's divergence list rather than traded for a second formatter.
	//
	// TODO(#544): delete this pin when the engine has a zone-aware timestamp
	// and a microsecond carrier.
	for _, fn := range []string{"NOW()", "CURRENT_TIMESTAMP()", "PG_POSTMASTER_START_TIME()"} {
		t.Run("residual_clock_is_zoneless_at_ms/"+fn, func(t *testing.T) {
			res, err := db.Query(ctx, `SELECT `+fn+` AS v FROM `+tbl+` WHERE id = 0`)
			if err != nil {
				t.Fatalf("%v", err)
			}
			// Rendered from the box the door hands back, which is the
			// instant since #868 — the same one a TIMESTAMP column hands
			// back. What this pin is about is the TEXT a client reads, and
			// that text is unchanged.
			got := tsRendered(res.Rows[0]["v"])
			if got == "" {
				t.Fatalf("%s = %#v, want an instant", fn, res.Rows[0]["v"])
			}
			// The engine's one rendering: `2006-01-02 15:04:05[.fff]`, a
			// space and no `T`, no trailing zone, at most three fraction
			// digits.
			if _, err := time.Parse("2006-01-02 15:04:05.999", got); err != nil {
				t.Errorf("%s = %q, which is not the engine's one instant rendering (%v). "+
					"If a zone or a `T` came back, a second dialect did too (#544)", fn, got, err)
			}
			if strings.ContainsAny(got, "TZ+") {
				t.Errorf("%s = %q carries a zone; this pin records that it does NOT, and "+
					"PostgreSQL's timestamptz text does (`…+00`). If the engine grew a "+
					"zone-aware timestamp, delete this pin and update ADR-0012", fn, got)
			}
			if i := strings.IndexByte(got, '.'); i >= 0 && len(got)-i-1 > 3 {
				t.Errorf("%s = %q carries more than three fraction digits; the instant is "+
					"epoch MILLISECONDS. PostgreSQL carries six", fn, got)
			}
		})
	}
}

// tsRendered renders whatever box a timestamp-valued expression handed back as
// the text a client reads: epoch milliseconds through batch.FormatTimestamp —
// which is what a TIMESTAMP-declared column produces through the embedded door
// — or the text itself for the expressions that are text on purpose.
func tsRendered(v any) string {
	switch tv := v.(type) {
	case int64:
		return batch.FormatTimestamp(tv)
	case string:
		return tv
	}
	return fmt.Sprint(v)
}
