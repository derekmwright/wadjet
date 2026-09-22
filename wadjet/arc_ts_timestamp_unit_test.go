// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// tsInstant is one instant of the #1266 census: the text each producer is
// handed, and what PostgreSQL 17.11 answers for `timestamp '<pg>'` — its text
// and its EXTRACT(EPOCH) (measured on 17.11, --locale=C, UTC).
type tsInstant struct {
	label     string
	csv, json string  // the spelling the file carries
	pg        string  // PostgreSQL's ::text of the value
	epoch     float64 // PostgreSQL's EXTRACT(EPOCH)
	// parquet names the row of internal/storage/parquet/testdata/
	// timestamp_precision.parquet (PyArrow, one column per unit) carrying the
	// same instant, or "" when the file has none.
	parquet string
}

// The instants discriminate on purpose. Each is a different value in the
// wrong unit (x1000: `2024-06-15 12:30:45` read as 56425-08-29), fractions sit
// at .5 / .123 / .25 / .999 because a unit error moves the fraction into the
// seconds, pre-1970 and year-1600 values carry a negative carrier, and
// 9999-12-31 is the last day PostgreSQL's text form and this engine's share a
// four-digit year. The two offset spellings are PostgreSQL's
// `timestamp without time zone` input: the offset is discarded, so the wall
// clock is the value (as for the TIMESTAMP literal, parquet.WallClockMillis).
var tsInstants = []tsInstant{
	{"i01", "2024-06-15 12:30:45", "2024-06-15T12:30:45Z", "2024-06-15 12:30:45", 1718454645, ""},
	{"i02", "2024-06-15 12:30:45.5", "2024-06-15T12:30:45.5Z", "2024-06-15 12:30:45.5", 1718454645.5, ""},
	{"i03", "1969-07-20 20:17:40.123", "1969-07-20T20:17:40.123Z", "1969-07-20 20:17:40.123", -14182939.877, ""},
	{"i04", "1996-03-13 14:25:36", "1996-03-13 14:25:36", "1996-03-13 14:25:36", 826727136, "issue_321"},
	{"i05", "2025-08-12T12:00:00.123", "2025-08-12 12:00:00.123", "2025-08-12 12:00:00.123", 1755000000.123, "modern_with_millis"},
	{"i06", "1969-12-31 23:59:59.999", "1969-12-31T23:59:59.999", "1969-12-31 23:59:59.999", -0.001, "one_ms_before_epoch"},
	{"i07", "9999-12-31 23:59:59.999", "9999-12-31T23:59:59.999Z", "9999-12-31 23:59:59.999", 253402300799.999, ""},
	{"i08", "1600-02-29 06:00:00.25", "1600-02-29 06:00:00.25", "1600-02-29 06:00:00.25", -11670976799.75, ""},
	{"i09", "2001-02-03T04:05:06-07:00", "2001-02-03T04:05:06+05:30", "2001-02-03 04:05:06", 981173106, ""},
	{"i10", "1969-07-20 20:17:40", "1969-07-20T20:17:40Z", "1969-07-20 20:17:40", -14182940, "apollo_11"},
	{"i11", "1970-01-01 00:00:00", "1970-01-01T00:00:00Z", "1970-01-01 00:00:00", 0, "epoch"},
}

// TestArcTSEveryTimestampProducerWritesTheEngineUnit is #1266's cross-producer
// gate: the SAME instant, produced by every door a TIMESTAMP enters through,
// is one value — it renders PostgreSQL's text, compares `=` to the TIMESTAMP
// literal, meets the literal as a join key, sorts in the same place, lands in
// the literal's GROUP BY group, and answers the same EXTRACT(EPOCH).
//
// At 260fc569 read_csv and read_json stored t.UnixMicro() into a carrier the
// rest of the engine reads as epoch MILLISECONDS, so every cell of the csv,
// json and persisted (INSERT … SELECT / CTAS) producers fails there; the
// parquet producers are the control that already agreed (their reader divides
// by TimestampDivisorFromSchemaNode).
func TestArcTSEveryTimestampProducerWritesTheEngineUnit(t *testing.T) {
	dir := t.TempDir()
	var csvBody, jsonBody strings.Builder
	csvBody.WriteString("label,ts\n")
	for _, in := range tsInstants {
		fmt.Fprintf(&csvBody, "%s,%s\n", in.label, in.csv)
		fmt.Fprintf(&jsonBody, "{\"label\":%q,\"ts\":%q}\n", in.label, in.json)
	}
	csvPath := filepath.Join(dir, "instants.csv")
	jsonPath := filepath.Join(dir, "instants.json")
	for p, body := range map[string]string{csvPath: csvBody.String(), jsonPath: jsonBody.String()} {
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	pqPath, err := filepath.Abs("../internal/storage/parquet/testdata/timestamp_precision.parquet")
	if err != nil {
		t.Fatal(err)
	}

	byLabel := map[string]tsInstant{}
	for _, in := range tsInstants {
		byLabel[in.label] = in
	}
	var lit, cast []string
	var pqCase []string
	var pqLabels []string
	for _, in := range tsInstants {
		lit = append(lit, fmt.Sprintf("('%s', TIMESTAMP '%s')", in.label, in.pg))
		cast = append(cast, fmt.Sprintf("('%s', '%s')", in.label, in.csv))
		if in.parquet != "" {
			pqCase = append(pqCase, fmt.Sprintf("WHEN '%s' THEN '%s'", in.parquet, in.label))
			pqLabels = append(pqLabels, "'"+in.parquet+"'")
		}
	}
	litSQL := `SELECT label, ts FROM (VALUES ` + strings.Join(lit, ", ") + `) v(label, ts)`
	parquetSQL := func(col string) string {
		return `SELECT CASE label ` + strings.Join(pqCase, " ") + ` END AS label, ` + col +
			` AS ts FROM read_parquet('` + pqPath + `') WHERE label IN (` + strings.Join(pqLabels, ", ") + `)`
	}
	producers := []struct {
		name, sql string
		parquet   bool // carries only the instants the parquet file has
	}{
		{"literal", litSQL, false},
		{"cast_text", `SELECT label, CAST(s AS TIMESTAMP) AS ts FROM (VALUES ` + strings.Join(cast, ", ") + `) v(label, s)`, false},
		{"read_csv", `SELECT label, ts FROM read_csv('` + csvPath + `')`, false},
		{"read_json", `SELECT label, ts FROM read_json('` + jsonPath + `')`, false},
		{"insert_select_read_csv", `SELECT label, ts FROM ts_from_csv`, false},
		{"ctas_read_json", `SELECT label, ts FROM ts_from_json`, false},
		{"parquet_millis", parquetSQL("ts_millis"), true},
		{"parquet_micros", parquetSQL("ts_micros"), true},
		{"parquet_nanos", parquetSQL("ts_nanos"), true},
	}
	// postgres_scan (internal/storage/dbscan) is the door whose values come
	// from a driver's time.Time. It needs a server, so it joins the census
	// only when WADJET_PG_DSN names one (`task pg-oracle:up`); the unit
	// census of the same write is dbscan.TestArcTSScannerStoresEpochMillis.
	if dsn := os.Getenv("WADJET_PG_DSN"); dsn != "" && !testing.Short() {
		table := fmt.Sprintf("wadjet_arc_ts_%d", os.Getpid())
		pg, err := sql.Open("postgres", dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer pg.Close()
		var rows []string
		for _, in := range tsInstants {
			rows = append(rows, fmt.Sprintf("('%s', TIMESTAMP '%s')", in.label, in.pg))
		}
		for _, stmt := range []string{
			`SET statement_timeout = '30s'`,
			`DROP TABLE IF EXISTS ` + table,
			`CREATE TABLE ` + table + ` (label text, ts timestamp)`,
			`INSERT INTO ` + table + ` VALUES ` + strings.Join(rows, ", "),
		} {
			if _, err := pg.Exec(stmt); err != nil {
				t.Fatalf("%s: %v", stmt, err)
			}
		}
		defer pg.Exec(`DROP TABLE IF EXISTS ` + table)
		producers = append(producers, struct {
			name, sql string
			parquet   bool
		}{"postgres_scan", `SELECT label, ts FROM postgres_scan('` + dsn + `', '` + table + `')`, false})
	}

	// What each producer carries, in PostgreSQL's order of the instants.
	carriedBy := func(parquetOnly bool) []string {
		var out []string
		for _, in := range tsInstants {
			if !parquetOnly || in.parquet != "" {
				out = append(out, in.label)
			}
		}
		sort.SliceStable(out, func(i, j int) bool { return byLabel[out[i]].epoch < byLabel[out[j]].epoch })
		return out
	}
	carried := map[string]int{} // label -> producers carrying it
	for _, p := range producers {
		for _, l := range carriedBy(p.parquet) {
			carried[l]++
		}
	}

	for _, arm := range []struct {
		name   string
		budget int64
	}{{"single", 0}, {"budget_512KiB", 512 << 10}} {
		t.Run(arm.name, func(t *testing.T) {
			ctx := context.Background()
			cfg := Config{Store: objstore.NewMemStore(), Bucket: "test", MemoryBudget: arm.budget}
			if arm.budget > 0 {
				cfg.SpillDir = t.TempDir()
				defer exec.ForceSmallSpillRuns(4096)()
			}
			db, err := Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			query := func(t testing.TB, sql string) [][]any {
				t.Helper()
				res, err := db.Query(ctx, sql)
				if err != nil {
					t.Fatalf("%v\n  SQL: %s", err, sql)
				}
				out := make([][]any, len(res.Rows))
				for i, row := range res.Rows {
					for _, c := range res.Columns {
						out[i] = append(out[i], row[c])
					}
				}
				return out
			}
			// The persisted producers: the defect's worst consequence is that a
			// file's wrong instant is WRITTEN into a table, where it outlives
			// the reader.
			query(t, `CREATE TABLE ts_from_csv (label VARCHAR, ts TIMESTAMP)`)
			query(t, `INSERT INTO ts_from_csv SELECT label, ts FROM read_csv('`+csvPath+`')`)
			query(t, `CREATE TABLE ts_from_json AS SELECT label, ts FROM read_json('`+jsonPath+`')`)

			litEpoch := map[string]string{}
			for _, row := range query(t, `SELECT label, EXTRACT(EPOCH FROM ts) AS e FROM (`+litSQL+`) p`) {
				litEpoch[fmt.Sprint(row[0])] = fmt.Sprint(row[1])
			}

			for _, p := range producers {
				t.Run(p.name, func(t *testing.T) {
					// Text, and ORDER BY: the rows come back in PostgreSQL's
					// order of the instants, each rendered as PostgreSQL does.
					rows := query(t, `SELECT label, CAST(ts AS VARCHAR) AS s FROM (`+p.sql+`) p ORDER BY ts, label`)
					labels := carriedBy(p.parquet)
					var got, want []string
					for _, row := range rows {
						got = append(got, fmt.Sprintf("%v=%v", row[0], row[1]))
					}
					for _, l := range labels {
						want = append(want, l+"="+byLabel[l].pg)
					}
					if strings.Join(got, "; ") != strings.Join(want, "; ") {
						t.Errorf("text/order:\n  got  %s\n  want %s (PostgreSQL 17.11)", strings.Join(got, "; "), strings.Join(want, "; "))
					}

					// EXTRACT(EPOCH): the literal's answer, and PostgreSQL's
					// where the instant is a whole second (the engine's EXTRACT
					// fraction is a recorded divergence, not this arc's).
					for _, row := range query(t, `SELECT label, EXTRACT(EPOCH FROM ts) AS e FROM (`+p.sql+`) p`) {
						l, e := fmt.Sprint(row[0]), fmt.Sprint(row[1])
						if e != litEpoch[l] {
							t.Errorf("%s EXTRACT(EPOCH) = %s, the literal's is %s", l, e, litEpoch[l])
						}
						if pg := byLabel[l].epoch; pg == math.Trunc(pg) {
							var f float64
							if _, err := fmt.Sscan(e, &f); err != nil || f != pg {
								t.Errorf("%s EXTRACT(EPOCH) = %s, PostgreSQL 17.11 answers %v", l, e, pg)
							}
						}
					}

					// `=` against the literal, one instant at a time.
					for _, l := range labels {
						n := query(t, `SELECT COUNT(*) AS n FROM (`+p.sql+`) p WHERE ts = TIMESTAMP '`+
							byLabel[l].pg+`' AND label = '`+l+`'`)
						if fmt.Sprint(n[0][0]) != "1" {
							t.Errorf("%s: ts = TIMESTAMP '%s' matched %v rows, want 1", l, byLabel[l].pg, n[0][0])
						}
					}

					// Join key: every row meets exactly its own instant.
					jrows := query(t, `SELECT p.label AS pl, l.label AS ll FROM (`+p.sql+`) p JOIN (`+
						litSQL+`) l ON p.ts = l.ts ORDER BY pl`)
					var pairs []string
					for _, row := range jrows {
						pairs = append(pairs, fmt.Sprintf("%v/%v", row[0], row[1]))
					}
					sorted := append([]string(nil), labels...)
					sort.Strings(sorted)
					var wantPairs []string
					for _, l := range sorted {
						wantPairs = append(wantPairs, l+"/"+l)
					}
					if strings.Join(pairs, " ") != strings.Join(wantPairs, " ") {
						t.Errorf("join on ts:\n  got  %s\n  want %s", strings.Join(pairs, " "), strings.Join(wantPairs, " "))
					}
				})
			}

			// GROUP BY over every producer at once: one group per instant,
			// holding exactly the producers that carry it. On the budgeted
			// arm the aggregate is forced to drain, so the key crosses the
			// spill codec too.
			var union []string
			for _, p := range producers {
				union = append(union, `SELECT label, ts FROM (`+p.sql+`) `+p.name)
			}
			before := exec.ForcedAggDrains.Load()
			if arm.budget > 0 {
				prev := exec.ForceAggDrainEvery(1)
				defer exec.ForceAggDrainEvery(prev)
			}
			// GROUP BY (ts, label) with COUNT(*) only, because that is a
			// shape whose partial state takes the external-merge drain
			// (canUseExternalMerge; MIN over a string does not). An instant a
			// producer stored differently is a SECOND group for its label.
			groups := query(t, `SELECT label, COUNT(*) AS n, CAST(ts AS VARCHAR) AS s FROM (`+
				strings.Join(union, " UNION ALL ")+`) u GROUP BY ts, label ORDER BY label, s`)
			if arm.budget > 0 {
				exec.ForceAggDrainEvery(0)
				if exec.ForcedAggDrains.Load() == before {
					t.Errorf("the forced aggregate drain never engaged: the GROUP BY cell compared two in-memory runs")
				}
			}
			var got, want []string
			for _, g := range groups {
				got = append(got, fmt.Sprintf("%v×%v=%v", g[0], g[1], g[2]))
			}
			for _, in := range tsInstants {
				want = append(want, fmt.Sprintf("%s×%d=%s", in.label, carried[in.label], in.pg))
			}
			if strings.Join(got, "; ") != strings.Join(want, "; ") {
				t.Errorf("GROUP BY ts over every producer:\n  got  %s\n  want %s", strings.Join(got, "; "), strings.Join(want, "; "))
			}
		})
	}
}
