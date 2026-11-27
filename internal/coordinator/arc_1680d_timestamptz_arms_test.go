// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/optswitch"
)

// The #1680 line d matrix: a timestamptz value against PostgreSQL 17.11, each
// cell measured under TimeZone UTC, America/New_York and Asia/Kolkata, with
// instants in a DST gap (2024-03-10 02:30 in New York), a DST fold
// (2024-11-03 01:30) and ordinary time. testdata/arc_1680d_timestamptz_pg17.tsv
// holds PostgreSQL's typed answers (TestArc1680dMeasure writes it);
// TestArc1680dTimestamptzEveryArm holds this engine's answer on every arm to
// PostgreSQL's under UTC — the only zone a session of this engine can be in:
// pgwire refuses a SET TimeZone naming any other zone (pgwire
// TestSetTimeZoneRefusesEveryZoneButUTC; internal/server
// TestTimestamptzOnEveryDoor), and the other doors have no SET.

type tzCell struct{ name, sql string }

// tzCells: a cell whose statement is `SELECT <expr> AS v` runs on the engine
// as `SELECT CAST(<expr> AS TEXT) AS v FROM ej_t WHERE id = 1` (one row
// through a scan, so the DAG arms run it; the text is compared, so the
// TIMESTAMP carrier's box is read as the engine prints it). Others run as
// written (tzEngineSQL).
var tzCells = []tzCell{
	{"in/lit/gap", "SELECT TIMESTAMPTZ '2024-03-10 02:30:00' AS v"},
	{"in/lit/fold", "SELECT TIMESTAMPTZ '2024-11-03 01:30:00' AS v"},
	{"in/lit/ordinary", "SELECT TIMESTAMPTZ '2024-07-01 12:00:00' AS v"},
	{"in/lit/ofsIST", "SELECT TIMESTAMPTZ '2024-07-01 12:00:00+05:30' AS v"},
	{"in/lit/ofsM4", "SELECT TIMESTAMPTZ '2024-07-01 12:00:00-04' AS v"},
	{"in/lit/ofsZ", "SELECT TIMESTAMPTZ '2024-07-01 12:00:00Z' AS v"},
	{"in/lit/ofsP00", "SELECT TIMESTAMPTZ '2024-07-01 12:00:00+00' AS v"},
	{"in/lit/ofsGapInstant", "SELECT TIMESTAMPTZ '2024-03-10 02:30:00-05' AS v"},
	{"in/lit/ofsFrac", "SELECT TIMESTAMPTZ '2024-07-01 12:00:00.123+05:30' AS v"},
	{"in/lit/ofsDayCross", "SELECT TIMESTAMPTZ '2024-07-01 02:00:00+05:30' AS v"},
	{"in/lit/ofsCompact", "SELECT TIMESTAMPTZ '2024-07-01 12:00:00+0530' AS v"},
	{"in/lit/ofsSpace", "SELECT TIMESTAMPTZ '2024-07-01 12:00:00 -04:00' AS v"},
	{"in/lit/ofsIsoT", "SELECT TIMESTAMPTZ '2024-07-01T12:00:00+05:30' AS v"},
	{"in/lit/zoneName", "SELECT TIMESTAMPTZ '2024-07-01 12:00:00 America/New_York' AS v"},
	{"in/lit/zoneUTCName", "SELECT TIMESTAMPTZ '2024-07-01 12:00:00 UTC' AS v"},
	{"in/lit/withTZ", "SELECT TIMESTAMP WITH TIME ZONE '2024-07-01 12:00:00+05:30' AS v"},
	{"in/lit/infinity", "SELECT TIMESTAMPTZ 'infinity' AS v"},
	{"in/lit/invalid", "SELECT TIMESTAMPTZ '2024-13-01 12:00:00+05:30' AS v"},
	{"in/lit/ofsRange", "SELECT TIMESTAMPTZ '294276-12-31 23:00:00-15' AS v"},
	{"in/ctl/tsOfs", "SELECT TIMESTAMP '2024-07-01 12:00:00+05:30' AS v"},
	{"in/cast/textNoOfs", "SELECT CAST('2024-07-01 12:00:00' AS TIMESTAMPTZ) AS v"},
	{"in/cast/textOfs", "SELECT CAST('2024-07-01 12:00:00+05:30' AS TIMESTAMPTZ) AS v"},
	{"in/cast/colonOfs", "SELECT '2024-07-01 12:00:00+05:30'::timestamptz AS v"},
	{"in/cast/textGap", "SELECT CAST('2024-03-10 02:30:00' AS TIMESTAMPTZ) AS v"},
	{"in/cast/tsToTstz", "SELECT CAST(TIMESTAMP '2024-07-01 12:00:00' AS TIMESTAMPTZ) AS v"},
	{"in/cast/tsGapToTstz", "SELECT CAST(TIMESTAMP '2024-03-10 02:30:00' AS TIMESTAMPTZ) AS v"},
	{"in/cast/tstzToTs", "SELECT CAST(TIMESTAMPTZ '2024-07-01 12:00:00+05:30' AS TIMESTAMP) AS v"},
	{"in/cast/dateToTstz", "SELECT CAST(DATE '2024-07-01' AS TIMESTAMPTZ) AS v"},
	{"in/cast/dateGapToTstz", "SELECT CAST(DATE '2024-03-10' AS TIMESTAMPTZ) AS v"},
	{"in/fn/toTimestamp", "SELECT to_timestamp(1720000000) AS v"},
	{"out/text", "SELECT CAST(TIMESTAMPTZ '2024-07-01 12:00:00+05:30' AS TEXT) AS v"},
	{"out/textOrd", "SELECT CAST(TIMESTAMPTZ '2024-07-01 12:00:00' AS TEXT) AS v"},
	{"out/toChar", "SELECT to_char(TIMESTAMPTZ '2024-07-01 12:00:00+05:30', 'YYYY-MM-DD HH24:MI') AS v"},
	{"atz/tstzUTC", "SELECT TIMESTAMPTZ '2024-07-01 12:00:00+05:30' AT TIME ZONE 'UTC' AS v"},
	{"atz/tstzNY", "SELECT TIMESTAMPTZ '2024-07-01 12:00:00+00' AT TIME ZONE 'America/New_York' AS v"},
	{"atz/tsUTC", "SELECT TIMESTAMP '2024-07-01 12:00:00' AT TIME ZONE 'UTC' AS v"},
	{"atz/tsIST", "SELECT TIMESTAMP '2024-07-01 12:00:00' AT TIME ZONE 'Asia/Kolkata' AS v"},
	{"atz/fnUTC", "SELECT timezone('UTC', TIMESTAMP '2024-07-01 12:00:00') AS v"},
	{"atz/fnTstzUTC", "SELECT timezone('UTC', TIMESTAMPTZ '2024-07-01 12:00:00+05:30') AS v"},
	{"cmp/tstzTstz", "SELECT TIMESTAMPTZ '2024-07-01 12:00:00+00' = TIMESTAMPTZ '2024-07-01 17:30:00+05:30' AS v"},
	{"cmp/tstzTs", "SELECT TIMESTAMPTZ '2024-07-01 12:00:00+00' = TIMESTAMP '2024-07-01 12:00:00' AS v"},
	{"cmp/tstzLitOfs", "SELECT TIMESTAMPTZ '2024-07-01 12:00:00+00' = '2024-07-01 17:30:00+05:30' AS v"},
	{"cmp/tstzLitOfsZero", "SELECT TIMESTAMPTZ '2024-07-01 12:00:00+05:30' = '2024-07-01 06:30:00+00' AS v"},
	{"cmp/tstzLitNoOfs", "SELECT TIMESTAMPTZ '2024-07-01 12:00:00+00' = '2024-07-01 12:00:00' AS v"},
	{"cmp/tstzLitLt", "SELECT TIMESTAMPTZ '2024-07-01 12:00:00+00' < '2024-07-01 12:30:00' AS v"},
	{"cmp/betweenOfs", "SELECT TIMESTAMPTZ '2024-07-01 12:00:00+00' BETWEEN '2024-07-01 17:00:00+05:30' AND '2024-07-01 18:00:00+05:30' AS v"},
	{"cmp/inOfs", "SELECT TIMESTAMPTZ '2024-07-01 12:00:00+00' IN ('2024-07-01 17:30:00+05:30') AS v"},
	{"cmp/nullifOfs", "SELECT NULLIF(TIMESTAMPTZ '2024-07-01 12:00:00+00', '2024-07-01 17:30:00+05:30') IS NULL AS v"},
	{"cmp/tsLitOfs", "SELECT TIMESTAMP '2024-07-01 12:00:00' = '2024-07-01 12:00:00+05:30' AS v"},
	{"key/distinctOffsets", "SELECT count(DISTINCT CASE id WHEN 1 THEN TIMESTAMPTZ '2024-07-01 12:00:00+00' WHEN 2 THEN TIMESTAMPTZ '2024-07-01 17:30:00+05:30' ELSE TIMESTAMPTZ '2024-07-01 08:00:00-04' END) AS n FROM " + tzRows + " WHERE id <= 3"},
	{"key/groupBy", "SELECT x AS v, count(*) AS n FROM (SELECT CASE id WHEN 1 THEN TIMESTAMPTZ '2024-07-01 12:00:00+00' WHEN 2 THEN TIMESTAMPTZ '2024-07-01 17:30:00+05:30' ELSE TIMESTAMPTZ '2024-07-01 13:00:00+00' END AS x FROM " + tzRows + " WHERE id <= 3) s GROUP BY x"},
	{"key/orderBy", "SELECT x AS v, row_number() OVER (ORDER BY x) AS r FROM (SELECT CASE id WHEN 1 THEN TIMESTAMPTZ '2024-07-01 12:00:00+05:30' WHEN 2 THEN TIMESTAMPTZ '2024-07-01 08:00:00+00' ELSE TIMESTAMPTZ '2024-07-01 03:00:00-04' END AS x FROM " + tzRows + " WHERE id <= 3) s"},
	{"key/join", "SELECT count(*) AS n FROM (SELECT id, CASE WHEN id = 1 THEN TIMESTAMPTZ '2024-07-01 12:00:00+00' END AS x FROM " + tzRows + ") a JOIN (SELECT id, CASE WHEN id = 2 THEN TIMESTAMPTZ '2024-07-01 17:30:00+05:30' END AS y FROM " + tzRows + ") b ON a.x = b.y"},
	{"ext/hour", "SELECT extract(hour FROM TIMESTAMPTZ '2024-07-01 12:00:00+05:30') AS v"},
	{"ext/epoch", "SELECT extract(epoch FROM TIMESTAMPTZ '2024-07-01 12:00:00+05:30') AS v"},
	{"ext/tzHour", "SELECT extract(timezone_hour FROM TIMESTAMPTZ '2024-07-01 12:00:00+05:30') AS v"},
	{"ext/tzMinute", "SELECT extract(timezone_minute FROM TIMESTAMPTZ '2024-07-01 12:00:00+05:30') AS v"},
	{"ext/tz", "SELECT extract(timezone FROM TIMESTAMPTZ '2024-07-01 12:00:00+05:30') AS v"},
	{"ext/tzHourTs", "SELECT extract(timezone_hour FROM TIMESTAMP '2024-07-01 12:00:00') AS v"},
	{"ext/datePartTzHour", "SELECT date_part('timezone_hour', TIMESTAMPTZ '2024-07-01 12:00:00+05:30') AS v"},
	{"ext/datePartTz", "SELECT date_part('timezone', TIMESTAMPTZ '2024-07-01 12:00:00+05:30') AS v"},
	{"ext/truncDay", "SELECT date_trunc('day', TIMESTAMPTZ '2024-07-01 02:00:00+05:30') AS v"},
	{"ext/truncDayZone", "SELECT date_trunc('day', TIMESTAMPTZ '2024-07-01 02:00:00+00', 'Asia/Kolkata') AS v"},
	{"ext/toDate", "SELECT CAST(TIMESTAMPTZ '2024-07-01 02:00:00+05:30' AS DATE) AS v"},
	{"ext/dateBin", "SELECT date_bin(INTERVAL '1 hour', TIMESTAMPTZ '2024-07-01 12:34:00+05:30', TIMESTAMPTZ '2000-01-01 00:00:00+00') AS v"},
	{"ext/timeBucket", "SELECT time_bucket(INTERVAL '1 hour', TIMESTAMPTZ '2024-07-01 12:34:00+05:30') AS v"},
	{"ar/diff", "SELECT TIMESTAMPTZ '2024-07-01 12:00:00+00' - TIMESTAMPTZ '2024-07-01 10:00:00+00' AS v"},
	{"ar/plusInt", "SELECT TIMESTAMPTZ '2024-07-01 12:00:00+00' + 1 AS v"},
	{"ar/plusInterval", "SELECT TIMESTAMPTZ '2024-07-01 12:00:00+00' + INTERVAL '1 day' AS v"},
}

var tzZones = []string{"UTC", "America/New_York", "Asia/Kolkata"}

const tzPGFile = "testdata/arc_1680d_timestamptz_pg17.tsv"

var tzSingleExpr = regexp.MustCompile(`^SELECT (.*) AS v$`)

// tzRows is the key cells' relation as PostgreSQL measures it: ej_t's ids
// (1..8), the only column they read. The arms read ej_t itself (a scan, so
// the DAG arms and the asynchronous door run the statement; the asynchronous
// door refuses a table-less VALUES).
const tzRows = "(SELECT * FROM (VALUES (1), (2), (3), (4), (5), (6), (7), (8)) q(id)) ej_t"

// tzEngineSQL is the statement the arms run for a cell (tzCells' comment).
func tzEngineSQL(c tzCell) string {
	if strings.HasPrefix(c.name, "key/") {
		s := strings.ReplaceAll(c.sql, tzRows, "ej_t")
		return strings.Replace(s, "SELECT x AS v,", "SELECT CAST(x AS TEXT) AS v,", 1)
	}
	if m := tzSingleExpr.FindStringSubmatch(c.sql); m != nil {
		return "SELECT CAST(" + m[1] + " AS TEXT) AS v FROM ej_t WHERE id = 1"
	}
	return c.sql
}

// TestArc1680dMeasure writes tzPGFile from a PostgreSQL 17.11 at
// TZ_PG_DSN (postgres:17-alpine, initdb --locale=C --encoding=UTF8).
func TestArc1680dMeasure(t *testing.T) {
	dsn := os.Getenv("TZ_PG_DSN")
	if dsn == "" {
		t.Skip("TZ_PG_DSN unset")
	}
	ctx := context.Background()
	var b strings.Builder
	b.WriteString("# #1680 line d: PostgreSQL 17.11 answers for the timestamptz matrix (TestArc1680dMeasure, arc_1680d_timestamptz_arms_test.go).\n")
	for i, zone := range tzZones {
		cfg, err := pgconn.ParseConfig(dsn)
		if err != nil {
			t.Fatal(err)
		}
		cfg.RuntimeParams["TimeZone"] = zone
		conn, err := pgconn.ConnectConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		one := func(sql string) string {
			r := iv80Read(conn.ExecParams(ctx, sql, nil, nil, nil, nil))
			if r.Err != nil || len(r.Rows) != 1 {
				t.Fatalf("%s: %v", sql, r.Err)
			}
			return string(r.Rows[0][0])
		}
		if i == 0 {
			fmt.Fprintf(&b, "# %s; datcollate=%s datctype=%s server_encoding=%s; DateStyle=%s; measured %s (postgres:17-alpine, initdb --locale=C --encoding=UTF8, --memory=4g).\n",
				one("SELECT version()"), one("SELECT datcollate FROM pg_database WHERE datname = current_database()"),
				one("SELECT datctype FROM pg_database WHERE datname = current_database()"), one("SHOW server_encoding"),
				one("SHOW DateStyle"), time.Now().UTC().Format("2006-01-02"))
			b.WriteString("# Each zone is the connection's TimeZone startup parameter (SHOW TimeZone confirms it per zone below).\n")
			b.WriteString("# name<TAB>zone<TAB>sql<TAB>answer: type(oid),... | row; row, or ERR <SQLSTATE>: message\n")
		}
		if got := one("SHOW TimeZone"); got != zone {
			t.Fatalf("SHOW TimeZone = %q, want %q", got, zone)
		}
		for _, c := range tzCells {
			r := iv80Read(conn.ExecParams(ctx, c.sql, nil, nil, nil, nil))
			var ans string
			if r.Err != nil {
				pe, ok := r.Err.(*pgconn.PgError)
				if !ok {
					t.Fatalf("%s: %v", c.name, r.Err)
				}
				ans = "ERR " + pe.Code + ": " + pe.Message
			} else {
				h := make([]string, len(r.FieldDescriptions))
				for i, fd := range r.FieldDescriptions {
					h[i] = fmt.Sprintf("%s(%d)", iv80PGTypeName(fd.DataTypeOID), fd.DataTypeOID)
				}
				rows := make([]string, len(r.Rows))
				for i, row := range r.Rows {
					f := make([]string, len(row))
					for j, v := range row {
						if v == nil {
							f[j] = "NULL"
						} else {
							f[j] = string(v)
						}
					}
					rows[i] = strings.Join(f, ",")
				}
				ans = strings.Join(h, ",") + " | " + strings.Join(rows, "; ")
			}
			b.WriteString(c.name + "\t" + zone + "\t" + c.sql + "\t" + ans + "\n")
		}
		conn.Close(ctx)
	}
	if err := os.WriteFile(tzPGFile, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// tzPGAnswers reads tzPGFile: name → zone → answer, checking every cell and
// zone is there and each statement is the cell's.
func tzPGAnswers(t *testing.T) map[string]map[string]string {
	t.Helper()
	f, err := os.Open(tzPGFile)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string]map[string]string{}
	sqlOf := map[string]string{}
	for _, c := range tzCells {
		sqlOf[c.name] = c.sql
	}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		p := strings.SplitN(line, "\t", 4)
		if len(p) != 4 {
			t.Fatalf("malformed line %q", line)
		}
		if sqlOf[p[0]] != p[2] {
			t.Fatalf("%s: the file's statement %q is not the cell's %q (re-measure)", p[0], p[2], sqlOf[p[0]])
		}
		if out[p[0]] == nil {
			out[p[0]] = map[string]string{}
		}
		out[p[0]][p[1]] = p[3]
	}
	for _, c := range tzCells {
		for _, z := range tzZones {
			if _, ok := out[c.name][z]; !ok {
				t.Fatalf("%s under %s is not in %s (re-measure)", c.name, z, tzPGFile)
			}
		}
	}
	return out
}

var tzNumericZeros = regexp.MustCompile(`^(-?[0-9]+)\.0+$`)

// tzExpected is PostgreSQL's answer in the form an arm renders (ejRender):
// rows sorted, cells joined by ','; an error is "ERR <SQLSTATE>". Two
// renderings are normalized, each a fact about this engine's carriers and not
// a value: a timestamptz value prints without PostgreSQL's `+00` (this engine
// declares it TIMESTAMP, the documented "no timestamptz" divergence; the
// instant is what is compared), and a numeric's trailing zero fraction
// (`1719815400.000000`) prints as the engine's float text does. A boolean is
// `true` / `false`, as the engine's text cast spells it.
func tzExpected(pg string) string {
	if strings.HasPrefix(pg, "ERR ") {
		code, _, _ := strings.Cut(strings.TrimPrefix(pg, "ERR "), ":")
		return "ERR " + code
	}
	head, body, _ := strings.Cut(pg, " | ")
	types := strings.Split(head, ",")
	rows := strings.Split(body, "; ")
	for i, r := range rows {
		f := strings.Split(r, ",")
		for j := range f {
			if j >= len(types) {
				continue
			}
			switch {
			case strings.HasPrefix(types[j], "timestamp with time zone"):
				f[j] = strings.TrimSuffix(f[j], "+00")
			case strings.HasPrefix(types[j], "numeric"):
				if m := tzNumericZeros.FindStringSubmatch(f[j]); m != nil {
					f[j] = m[1]
				}
			case strings.HasPrefix(types[j], "boolean"):
				f[j] = map[string]string{"t": "true", "f": "false"}[f[j]]
			}
		}
		rows[i] = strings.Join(f, ",")
	}
	sort.Strings(rows)
	return fmt.Sprintf("rows=%d %s", len(rows), strings.Join(rows, " | "))
}

var tzAllArms = []string{"single", "spilled512k", "dag", "dag-shuffled", "dag-morsel4", "dag-skew",
	"fastpath", "async", "async-probesplit"}

// tzKnown are the cells whose answer under UTC is not PostgreSQL's, each
// pinned on every arm (a pin fails when the answer moves):
//
//   - this line's refusal: a comparison of a timestamptz with a quoted literal
//     carrying a nonzero offset (0A000 timestamptz-zoned-literal), which read
//     the literal as a timestamp and discarded its offset (false at the base);
//   - stage A's named refusals of timestamptz arithmetic and the existing
//     AT TIME ZONE refusal of a zone other than UTC (0A000, unchanged here);
//   - loud gaps, the same at the base: TIMESTAMP WITH TIME ZONE is not parsed
//     (42601), a zone NAME inside the text is not in the timestamp grammar
//     (22007), date_bin over an interval is refused (0A000), and
//     to_timestamp, to_char, transaction_timestamp, extract(timezone …) (the
//     parser's timezone(x) rewrite) and the three-argument date_trunc do not
//     exist here (42883);
//   - two cells PostgreSQL refuses that this engine answers: time_bucket is
//     this engine's own function, and timezone_hour of a TIMESTAMP is 0A000
//     there — this engine holds a timestamptz in its TIMESTAMP carrier and
//     cannot tell the two apart, so it answers the session offset, 0, for both
//     (the process zone's offset at the base).
var tzKnown = map[string]string{
	"cmp/tstzLitOfs":     "ERR 0A000",
	"cmp/betweenOfs":     "ERR 0A000",
	"cmp/inOfs":          "ERR 0A000",
	"cmp/nullifOfs":      "ERR 0A000",
	"ar/diff":            "ERR 0A000",
	"ar/plusInterval":    "ERR 0A000",
	"atz/tstzNY":         "ERR 0A000",
	"atz/tsIST":          "ERR 0A000",
	"in/lit/withTZ":      "ERR 42601",
	"in/lit/zoneName":    "ERR 22007",
	"in/lit/zoneUTCName": "ERR 22007",
	"ext/dateBin":        "ERR 0A000",
	"in/fn/toTimestamp":  "ERR 42883",
	"out/toChar":         "ERR 42883",
	"ext/tz":             "ERR 42883",
	"ext/truncDayZone":   "ERR 42883",
	// The "no timestamptz" divergence where it reaches a VALUE: a timestamptz
	// cast to text is the TIMESTAMP text, with no `+00` (the right instant).
	"out/text":       "rows=1 2024-07-01 06:30:00",
	"out/textOrd":    "rows=1 2024-07-01 12:00:00",
	"ext/timeBucket": "rows=1 2024-07-01 07:00:00",
	"ext/tzHourTs":   "rows=1 0",
}

// A TIMESTAMPTZ VALUE IS POSTGRESQL'S INSTANT ON EVERY ARM (#1680 line d).
// `TIMESTAMPTZ '2024-07-01 12:00:00+05:30'` answered 12:00 on every arm and
// every door — the offset discarded, as TIMESTAMP input discards it — where
// PostgreSQL answers the instant 06:30 UTC; `CAST(text AS TIMESTAMPTZ)` and
// `'…'::timestamptz` likewise, and every consumer read the wrong instant
// (date_trunc, extract, CAST to DATE, GROUP BY / DISTINCT / ORDER BY / join
// over equal instants written with different offsets). extract(timezone_hour)
// answered the server PROCESS's local offset (-4 on a host in New York in
// July) and date_part('timezone_hour' / 'timezone') answered NULL, where
// PostgreSQL under UTC answers 0. Each cell is checked against PostgreSQL
// 17.11 under TimeZone=UTC; tzKnown pins the rest.
func TestArc1680dTimestamptzEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: nine arms over the timestamptz matrix")
	}
	pg := tzPGAnswers(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	arms := ejArms(t, ctx)
	for _, c := range tzCells {
		want := tzExpected(pg[c.name]["UTC"])
		pin, pinned := tzKnown[c.name]
		sql := tzEngineSQL(c)
		t.Run(c.name, func(t *testing.T) {
			for _, a := range arms {
				got := a.run(sql)
				expect := want
				if pinned {
					expect = pin
				}
				// The asynchronous door reports an error raised while the
				// query RUNS as a failed query with no SQLSTATE (its existing
				// envelope, the same at the base): an error cell passes there
				// on that failure.
				if strings.HasPrefix(a.name, "async") && strings.HasPrefix(expect, "ERR ") &&
					got == "ERR (uncoded) query state is failed, not completed" {
					continue
				}
				// The asynchronous door's plan-time gather check refuses a
				// computed SELECT item over a derived table's GROUP BY
				// (#656's 'computed no SELECT list'), whatever the item's
				// type: the same at the base, not this line's.
				if strings.HasPrefix(a.name, "async") && c.name == "key/groupBy" {
					if !strings.Contains(got, "computed no SELECT list") {
						t.Errorf("%s\n  arm  %s\n  got  %s\n  pin  ERR containing 'computed no SELECT list'", sql, a.name, got)
					}
					continue
				}
				switch {
				case pinned && got != pin:
					t.Errorf("%s\n  arm  %s\n  got  %s\n  pin  %s (a known answer moved: re-measure against PostgreSQL 17.11's %s)",
						sql, a.name, got, pin, pg[c.name]["UTC"])
				case !pinned && got != want:
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (PostgreSQL 17.11 under UTC: %s)", sql, a.name, got, want, pg[c.name]["UTC"])
				}
			}
		})
	}
}

// TestArc1680dUnderEveryKillSwitch: the line-d answers and refusals are
// decided before any optimization, so turning any one kill switch off moves
// no cell's answer on the single-process or the DAG arm — the refusal
// (timestamptz-zoned-literal) keeps its SQLSTATE, and every value cell its
// value (docs/testing/GATES.md, the kill-switch invariance of a refusal).
func TestArc1680dUnderEveryKillSwitch(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: every kill switch over the timestamptz matrix")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	var picked []ejArm
	for _, a := range ejArms(t, ctx) {
		if a.name == "single" || a.name == "dag" {
			picked = append(picked, a)
		}
	}
	base := map[string]string{}
	for _, a := range picked {
		for _, c := range tzCells {
			base[a.name+"|"+c.name] = a.run(tzEngineSQL(c))
		}
	}
	if got := base["single|cmp/tstzLitOfs"]; got != "ERR 0A000" {
		t.Fatalf("cmp/tstzLitOfs on single = %s, want the refusal", got)
	}
	moved := 0
	for _, tog := range optswitch.All() {
		prev := tog.Set(false)
		for _, a := range picked {
			for _, c := range tzCells {
				got, want := a.run(tzEngineSQL(c)), base[a.name+"|"+c.name]
				// With scan-col-sanitize off the stage DAG cannot read a
				// derived table's computed column ("column \"x\" does not
				// exist in the input schema"), whatever its type: the same
				// with an integer CASE in place of the timestamptz one, so
				// not this line's. It is a loud failure, never an answer.
				if tog.Name == "scan-col-sanitize" && a.name == "dag" && (c.name == "key/orderBy" || c.name == "key/join") &&
					strings.Contains(got, "does not exist in the input schema") {
					continue
				}
				if got != want {
					moved++
					t.Errorf("%s off, %s on %s:\n  with it  %s\n  without  %s", tog.Name, c.name, a.name, want, got)
				}
			}
		}
		tog.Set(prev)
	}
	t.Logf("%d cells x %d kill switches x %d arms; %d moved", len(tzCells), len(optswitch.All()), len(picked), moved)
}
