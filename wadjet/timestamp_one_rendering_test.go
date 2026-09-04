package wadjet

import (
	"context"
	"testing"

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
			if len(res.Rows) != 1 || res.Rows[0]["v"] != c.want {
				t.Errorf("= %#v, want %q (live PostgreSQL 17.11). Every instant-valued "+
					"expression renders through expr.formatInstant; a `T` and a `Z` here "+
					"mean a second renderer came back (#544)\n  SQL: %s",
					res.Rows, c.want, c.sql)
			}
		})
	}
	// The RESIDUAL that survives, pinned rather than described. The VALUE is
	// PostgreSQL's now; the DECLARED TYPE is not. `date_trunc` RETURNS
	// `timestamp` on the server (OID 1114) and this engine declares its result
	// STRING, because the scalar registry is `func([]any) any` with a single
	// static Ret per entry and no TIMESTAMP-valued function result exists.
	//
	// That is a different mechanism from the rendering — a client that asks
	// what the column IS still gets `text`, and no amount of formatting fixes
	// it — so it is recorded, not bandaged (correctness-fix protocol rule 11).
	// The structural fix is for ~14 registry entries to return the engine's
	// TIMESTAMP box and carry parquet.TypeTimestamp in their declaration,
	// which is a change to the registry's type channel, not to these
	// functions.
	//
	// TODO(#544): delete this pin when a timestamp-valued function declares
	// TIMESTAMP.
	t.Run("residual_date_trunc_declares_string_not_timestamp", func(t *testing.T) {
		res, err := db.Query(ctx,
			`SELECT DATE_TRUNC('day', c_ts) AS v FROM `+tbl+` WHERE id = 0`)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if len(res.ColumnMetas) != 1 {
			t.Fatalf("want one column meta, got %d", len(res.ColumnMetas))
		}
		if got := res.ColumnMetas[0].TypeID; got != parquet.TypeString {
			t.Errorf("DATE_TRUNC declares %v; this pin records STRING, and PostgreSQL "+
				"declares timestamp (OID 1114). If it has moved, #544's declaration "+
				"residual is closed: delete this pin and re-measure the wire OID of "+
				"every timestamp-valued function", got)
		}
	})
}
