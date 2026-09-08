package coordinator

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// THE BAR ANSWERS THE SAME THING ON EVERY ARM, AND IT IS PostgreSQL'S (#965).
//
// `ohlcv(ts, price, volume)` is ONE mergeable state (ADR-0035), so its answer
// has to survive being computed in pieces and combined: per parallel clone,
// per spilled run, per DAG task. This is the arm census for that — single
// process, stage DAG, and the DAG with every build side pushed through a
// shuffle — over a fixture split into four files so the DAG really does
// combine partials rather than compute one.
//
// PostgreSQL has no `ohlcv`. Its oracle is the bar SPELLED OUT, and the
// spelling is exact rather than approximate on two points that decide whether
// the numbers below mean anything:
//
//  1. A multi-argument aggregate SKIPS a row where ANY argument is NULL —
//     measured on 17.11, `regr_count(y,x)` over (1,1),(2,NULL),(NULL,3),(4,4)
//     is 2 — so every field's spelling carries the SAME row filter:
//
//     (array_agg(px ORDER BY ts, px) FILTER (WHERE ok))[1]        AS open
//     MAX(px) FILTER (WHERE ok)                                   AS high
//     MIN(px) FILTER (WHERE ok)                                   AS low
//     (array_agg(px ORDER BY ts DESC, px DESC) FILTER (WHERE ok))[1] AS close
//     SUM(vol) FILTER (WHERE ok)                                  AS volume
//     (SUM(px*vol) FILTER (WHERE ok))/(SUM(vol) FILTER (WHERE ok)) AS vwap
//
//     with `ok` = `ts IS NOT NULL AND px IS NOT NULL AND vol IS NOT NULL`.
//     The GROUP BY key still comes from every row, so a bucket whose rows are
//     all skipped is a row of NULLs there and a NULL BAR here — a
//     composite-returning aggregate over no rows is NULL, and `(NULL::rec).f`
//     is NULL on the server too (measured).
//
//  2. The TIEBREAK is `(ts, price)`, which is why the ORDER BY in that
//     spelling has two terms. Rows 5 and 6 share an instant inside bucket
//     12:01 and rows 7 and 8 share the next one; without the second term
//     PostgreSQL itself would not have a defined answer, and neither would
//     an engine that read files in whatever order tasks finished.
//
// Measured on PostgreSQL 17.11, 2026-09-08, over exactly these eleven rows.
type ohlcvCell struct {
	name, sql string
	want      []string
	// wantErrLikeAll is a refusal EVERY arm must give.
	wantErrLikeAll string
	pgSays         string
}

const ohlcvTable = "a1bar"

// bkt renders a bucket the way na2Run boxes a TIMESTAMP: the epoch
// millisecond, with its Go type, because a right instant under the wrong box
// is exactly what the type prefix exists to catch.
func ohlcvBkt(text string) string {
	return fmt.Sprintf("bkt=int64:%d", ohlcvMillis(text))
}

func ohlcvCells() []ohlcvCell {
	// The SELECT-list order, which is the order na2Run renders a row in.
	nullBar := "|o=NULL|h=NULL|l=NULL|c=NULL|v=NULL|w=NULL"
	return []ohlcvCell{
		// The whole-table bar, one group, over a fixture the DAG splits four
		// ways: this is the cell that fails if the merge law is wrong.
		//
		// A FLOAT field renders to six significant digits (ADR-0013's
		// nondeterminism class 9), so this cell can tell a wrong bar from a
		// right one but not the last bits of a float sum. The two EXACT cells
		// below carry the digits.
		{name: "whole_table_float",
			sql: `SELECT (b).open AS o, (b).high AS h, (b).low AS l, (b).close AS c,
			             (b).volume AS v, (b).vwap AS w
			      FROM (SELECT ohlcv(ts, px_f64, vol_i64) AS b FROM ` + ohlcvTable + `) t`,
			want:   []string{"o=float:10|h=float:21|l=float:7|c=float:21|v=24|w=float:14.5833"},
			pgSays: "(10, 21, 7, 21, 24, 14.583333333333334)"},
		// Bucketed, which is the shape the function exists for. Two real
		// bars, one bucket whose every row is skipped, and the NULL-ts group.
		{name: "bucketed_float",
			sql: `SELECT bkt, (b).open AS o, (b).high AS h, (b).low AS l, (b).close AS c,
			             (b).volume AS v, (b).vwap AS w
			      FROM (SELECT time_bucket(INTERVAL '1' MINUTE, ts) AS bkt,
			                   ohlcv(ts, px_f64, vol_i64) AS b
			            FROM ` + ohlcvTable + ` GROUP BY 1) t ORDER BY bkt`,
			want: []string{
				ohlcvBkt("2020-09-13 12:00:00") + "|o=float:10|h=float:14|l=float:7|c=float:11|v=11|w=float:9.18182",
				ohlcvBkt("2020-09-13 12:01:00") + "|o=float:18|h=float:21|l=float:18|c=float:21|v=13|w=float:19.1538",
				ohlcvBkt("2020-09-13 12:02:00") + nullBar,
				"bkt=NULL" + nullBar,
			},
			pgSays: "12:00 (10,14,7,11,11,9.181818181818182); 12:01 (18,21,18,21,13,19.153846153846153); " +
				"12:02 and the NULL bucket keep no rows, so every field is NULL"},
		// The EXACT domain: an integer price and an integer volume.
		//
		// Every box here is asserted beside its value. The four prices keep
		// INT64 — the column's own type, which is what PostgreSQL's min/max
		// of an integer column declares. `volume` is SUM(int8), which is
		// NUMERIC (exec.IntegerAccOutputType) and therefore arrives as its own
		// exact TEXT. `vwap` is AVG(price)'s type, NUMERIC(38,4), so its
		// digits are compared exactly rather than to six of them.
		{name: "bucketed_int_exact",
			sql: `SELECT bkt, (b).open AS o, (b).high AS h, (b).low AS l, (b).close AS c,
			             (b).volume AS v, (b).vwap AS w
			      FROM (SELECT time_bucket(INTERVAL '1' MINUTE, ts) AS bkt,
			                   ohlcv(ts, px_i64, vol_i64) AS b
			            FROM ` + ohlcvTable + ` GROUP BY 1) t ORDER BY bkt`,
			want: []string{
				ohlcvBkt("2020-09-13 12:00:00") + "|o=int64:10|h=int64:14|l=int64:7|c=int64:11|v=11|w=9.1818",
				ohlcvBkt("2020-09-13 12:01:00") + "|o=int64:18|h=int64:21|l=int64:18|c=int64:21|v=13|w=19.1538",
				ohlcvBkt("2020-09-13 12:02:00") + nullBar,
				"bkt=NULL" + nullBar,
			},
			pgSays: "round(SUM(px::numeric*vol)/SUM(vol),4) = 9.1818 and 19.1538; " +
				"min/max of an integer column is integer, sum(int8) is numeric"},
		// DECIMAL(9,2) price with an INT32 volume: the price fields keep
		// (9,2) digit for digit, `volume` is SUM(int4) = BIGINT, and `vwap`
		// takes AVG's +4 on the input's scale, so DECIMAL(38,6).
		{name: "bucketed_decimal",
			sql: `SELECT bkt, (b).open AS o, (b).high AS h, (b).low AS l, (b).close AS c,
			             (b).volume AS v, (b).vwap AS w
			      FROM (SELECT time_bucket(INTERVAL '1' MINUTE, ts) AS bkt,
			                   ohlcv(ts, px_d92, vol_i32) AS b
			            FROM ` + ohlcvTable + ` GROUP BY 1) t ORDER BY bkt`,
			want: []string{
				ohlcvBkt("2020-09-13 12:00:00") + "|o=10.00|h=14.00|l=7.00|c=11.00|v=int64:11|w=9.181818",
				ohlcvBkt("2020-09-13 12:01:00") + "|o=18.00|h=21.00|l=18.00|c=21.00|v=int64:13|w=19.153846",
				ohlcvBkt("2020-09-13 12:02:00") + nullBar,
				"bkt=NULL" + nullBar,
			},
			pgSays: "round(SUM(px::numeric(9,2)*vol)/SUM(vol),6) = 9.181818 and 19.153846; " +
				"min/max of numeric(9,2) keeps (9,2), sum(int4) is bigint"},
		// THE INTERNAL IDENTITY. Every field of the bar equals the aggregate
		// it is spelled as, in the SAME query, so a bar and the five
		// aggregates beside it cannot drift apart. vwap is the sharp one: it
		// must be `SUM(price*volume)/SUM(volume)` digit for digit, not
		// approximately, which is what makes reusing the engine's own decimal
		// division rather than a float quotient load-bearing.
		{name: "the_bar_equals_its_spelled_out_aggregates",
			sql: `SELECT bkt,
			             CASE WHEN (b).high = hi THEN 'same' ELSE 'DIFFERS' END AS high_is,
			             CASE WHEN (b).low  = lo THEN 'same' ELSE 'DIFFERS' END AS low_is,
			             CASE WHEN (b).volume = vol THEN 'same' ELSE 'DIFFERS' END AS vol_is,
			             CASE WHEN (b).vwap = vwap THEN 'same' ELSE 'DIFFERS' END AS vwap_is
			      FROM (SELECT time_bucket(INTERVAL '1' MINUTE, ts) AS bkt,
			                   ohlcv(ts, px_f64, vol_i64) AS b,
			                   MAX(px_f64) AS hi, MIN(px_f64) AS lo,
			                   SUM(vol_i64) AS vol,
			                   SUM(px_f64*vol_i64)/SUM(vol_i64) AS vwap
			            FROM ` + ohlcvTable + `
			            WHERE ts IS NOT NULL AND px_f64 IS NOT NULL AND vol_i64 IS NOT NULL
			            GROUP BY 1) t ORDER BY bkt`,
			want: []string{
				ohlcvBkt("2020-09-13 12:00:00") + "|high_is=same|low_is=same|vol_is=same|vwap_is=same",
				ohlcvBkt("2020-09-13 12:01:00") + "|high_is=same|low_is=same|vol_is=same|vwap_is=same",
			},
			pgSays: "the same identity holds on the server for the same spellings"},
		// The empty group: an aggregate over no rows is NULL, and a field of
		// a NULL composite is NULL (both measured).
		{name: "empty_input_is_a_null_bar",
			sql: `SELECT (b).open AS o, (b).vwap AS w
			      FROM (SELECT ohlcv(ts, px_f64, vol_i64) AS b FROM ` + ohlcvTable + ` WHERE id < 0) t`,
			want:   []string{"o=NULL|w=NULL"},
			pgSays: "every field NULL; (NULL::record).f is NULL"},
		// A zero total volume has no weighted average, and the bar still has
		// its four prices. Its own table, so no other cell's numbers move.
		{name: "zero_volume_has_no_vwap",
			sql: `SELECT (b).open AS o, (b).close AS c, (b).volume AS v, (b).vwap AS w
			      FROM (SELECT ohlcv(ts, px_f64, vol_i64) AS b FROM ` + ohlcvZeroTable + `) t`,
			want:   []string{"o=float:5|c=float:6|v=0|w=NULL"},
			pgSays: "SUM(px*vol)/SUM(vol) with SUM(vol) = 0 is a division by zero; the bar answers NULL"},
		// THE BOUNDARY, from both sides. An argument type that has no bar is
		// refused on every arm with the same sentence, never answered as an
		// empty bar.
		{name: "a_text_ordering_key_is_loud_on_every_arm",
			sql:            `SELECT ohlcv(c_str, px_f64, vol_i64) AS b FROM ` + ohlcvTable,
			wantErrLikeAll: "the first argument must be a TIMESTAMP or a DATE",
			pgSays:         "no such function signature; 42883"},
		{name: "a_text_price_is_loud_on_every_arm",
			sql:            `SELECT ohlcv(ts, c_str, vol_i64) AS b FROM ` + ohlcvTable,
			wantErrLikeAll: "no bar over price STRING",
			pgSays:         "no such function signature; 42883"},
		{name: "a_text_volume_is_loud_on_every_arm",
			sql:            `SELECT ohlcv(ts, px_f64, c_str) AS b FROM ` + ohlcvTable,
			wantErrLikeAll: "and volume STRING",
			pgSays:         "no such function signature; 42883"},
		{name: "distinct_is_loud_on_every_arm",
			sql:            `SELECT ohlcv(DISTINCT ts, px_f64, vol_i64) AS b FROM ` + ohlcvTable,
			wantErrLikeAll: "ohlcv(DISTINCT ...) is not supported",
			pgSays:         "PostgreSQL dedupes on the whole argument tuple and answers"},
		{name: "the_window_form_is_loud_on_every_arm",
			sql:            `SELECT ohlcv(ts, px_f64, vol_i64) OVER (ORDER BY ts) AS b FROM ` + ohlcvTable,
			wantErrLikeAll: "is not supported as a window function",
			pgSays:         "PostgreSQL answers it; this engine has sixteen window functions"},
	}
}

func TestTheBarIsTheSameOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })

	for _, tc := range ohlcvCells() {
		t.Run(tc.name, func(t *testing.T) {
			want := append([]string(nil), tc.want...)
			sort.Strings(want)
			for _, arm := range []struct {
				name string
				run  func() ([]string, error)
			}{
				{"single", func() ([]string, error) { return na2Run(tmdRunSingle(ctx, single, tc.sql)) }},
				{"dag", func() ([]string, error) { return na2Run(tmdRunDAG(ctx, coord, tc.sql)) }},
				{"dag-shuffled", func() ([]string, error) { return na2Run(tmdRunDAG(ctx, coordB, tc.sql)) }},
			} {
				got, err := arm.run()
				if tc.wantErrLikeAll != "" {
					if err == nil {
						t.Errorf("%s arm: ANSWERED %v — this shape is refused on the other arms. "+
							"If it is now supported, lift the boundary on ALL arms in one change "+
							"and assert the VALUE against PostgreSQL; one engine answering while "+
							"another refuses is the divergence this cell exists to hold.\n  SQL: %s",
							arm.name, got, tc.sql)
					} else if !strings.Contains(err.Error(), tc.wantErrLikeAll) {
						t.Errorf("%s arm: %v\n  want one containing %q\n  SQL: %s",
							arm.name, err, tc.wantErrLikeAll, tc.sql)
					}
					continue
				}
				if err != nil {
					t.Errorf("%s arm: %v\n  PostgreSQL 17: %s\n  SQL: %s",
						arm.name, err, tc.pgSays, tc.sql)
					continue
				}
				if strings.Join(got, "\n") != strings.Join(want, "\n") {
					t.Errorf("%s arm\n  got  %v\n  want %v (live PostgreSQL 17: %s)\n  SQL: %s",
						arm.name, got, want, tc.pgSays, tc.sql)
				}
			}
		})
	}
}

// --- the fixture ------------------------------------------------------------

func ohlcvMillis(s string) int64 {
	v, err := time.Parse("2006-01-02 15:04:05", s)
	if err != nil {
		panic(fmt.Sprintf("ohlcv fixture: %v", err))
	}
	return v.UTC().UnixMilli()
}

// ohlcvSchema is the bar fixture: one instant column, four price columns
// covering the exact and the approximate carrier, two volume widths, and a
// text column that is the refusal boundary's other side.
func ohlcvSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
		{Name: "px_f64", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "px_i64", Type: parquet.TypeInt64, Nullable: true},
		{Name: "px_d92", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
		{Name: "vol_i64", Type: parquet.TypeInt64, Nullable: true},
		{Name: "vol_i32", Type: parquet.TypeInt32, Nullable: true},
		{Name: "c_str", Type: parquet.TypeString, Nullable: true},
	}}
}

// ohlcvData is exactly the eleven rows PostgreSQL was measured over.
//
// Rows 5/6 share an instant and 7/8 share the next one: those are the ties the
// (ts, price) tiebreak decides, and without them the fixture could not tell a
// value-decided open from an arrival-ordered one. Row 9 has no price, row 10
// no volume, row 11 no instant — one row per argument, which is the
// multi-argument NULL rule from three sides.
func ohlcvData() []map[string]any {
	return ohlcvRows([]ohlcvRow{
		{1, ohlcvMillis("2020-09-13 12:00:05"), 10.0, int64(3)},
		{2, ohlcvMillis("2020-09-13 12:00:15"), 14.0, int64(1)},
		{3, ohlcvMillis("2020-09-13 12:00:25"), 7.0, int64(5)},
		{4, ohlcvMillis("2020-09-13 12:00:35"), 11.0, int64(2)},
		{5, ohlcvMillis("2020-09-13 12:01:05"), 20.0, int64(4)},
		{6, ohlcvMillis("2020-09-13 12:01:05"), 18.0, int64(6)},
		{7, ohlcvMillis("2020-09-13 12:01:45"), 19.0, int64(1)},
		{8, ohlcvMillis("2020-09-13 12:01:45"), 21.0, int64(2)},
		{9, ohlcvMillis("2020-09-13 12:02:00"), nil, int64(7)},
		{10, ohlcvMillis("2020-09-13 12:02:10"), 30.0, nil},
		{11, nil, 40.0, int64(9)},
	})
}

// ohlcvZeroTable is the zero-total-volume fixture. It is a SEPARATE table so
// the eleven rows above stay exactly the eleven PostgreSQL was measured over —
// a division by zero folded into the main fixture would move every other
// cell's numbers.
const ohlcvZeroTable = "a1barz"

func ohlcvZeroData() []map[string]any {
	return ohlcvRows([]ohlcvRow{
		{1, ohlcvMillis("2021-01-01 00:00:00"), 5.0, int64(4)},
		{2, ohlcvMillis("2021-01-01 00:00:10"), 6.0, int64(-4)},
	})
}

type ohlcvRow struct {
	id  int32
	ts  any
	px  any
	vol any
}

func ohlcvRows(rows []ohlcvRow) []map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for _, v := range rows {
		m := map[string]any{
			"id": v.id, "ts": v.ts, "px_f64": v.px, "vol_i64": v.vol,
			"c_str": "x",
		}
		if v.px != nil {
			m["px_i64"] = int64(v.px.(float64))
			m["px_d92"] = fmt.Sprintf("%.2f", v.px.(float64))
		} else {
			m["px_i64"], m["px_d92"] = nil, nil
		}
		if v.vol != nil {
			m["vol_i32"] = int32(v.vol.(int64))
		} else {
			m["vol_i32"] = nil
		}
		out = append(out, m)
	}
	return out
}
