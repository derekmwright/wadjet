// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/server/pgwire"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// The fixture of arc TI: TIMESTAMP and DATE columns holding PostgreSQL's two
// infinite values beside finite ones. ti_i is the main relation (both
// infinities, a duplicate infinity, NULL, the year 9999 and the millisecond
// before the epoch); ti_j a join partner; ti_f the finite rows alone; ti_p
// the pruning relation (four files on the DAG doors — finite-low,
// finite-high, one holding infinity, one holding -infinity — and two rows per
// row group on the embedded doors); ti_big 30000 rows (a tenth infinity, a
// tenth -infinity, a tenth NULL) for the 512 KiB budget door and the
// shuffled DAG door's exchange.
var tiSpec = [][3]string{
	{"1", "1970-01-01 00:00:00", "1970-01-01"},
	{"2", "2024-01-15 10:30:00", "2024-01-15"},
	{"3", "infinity", "infinity"},
	{"4", "-infinity", "-infinity"},
	{"5", "", ""},
	{"6", "9999-12-31 23:59:59", "9999-12-31"},
	{"7", "1969-12-31 23:59:59.999", "1969-12-31"},
	{"8", "infinity", "infinity"},
}
var tjSpec = [][3]string{
	{"1", "infinity", "infinity"},
	{"2", "-infinity", "-infinity"},
	{"3", "2024-01-15 10:30:00", "2024-01-15"},
	{"4", "", ""},
}

var tpSpec = [][3]string{
	{"1", "2000-01-01 00:00:00", "2000-01-01"},
	{"2", "2000-06-01 00:00:00", "2000-06-01"},
	{"3", "2000-12-31 00:00:00", "2000-12-31"},
	{"4", "2030-01-01 00:00:00", "2030-01-01"},
	{"5", "2030-06-01 00:00:00", "2030-06-01"},
	{"6", "2030-12-31 00:00:00", "2030-12-31"},
	{"7", "2050-01-01 00:00:00", "2050-01-01"},
	{"8", "infinity", "infinity"},
	{"9", "2050-06-01 00:00:00", "2050-06-01"},
	{"10", "-infinity", "-infinity"},
	{"11", "1900-01-01 00:00:00", "1900-01-01"},
	{"12", "1900-06-01 00:00:00", "1900-06-01"},
}

const tiBigRows = 30000

// tiBigCell is ti_big's row id: infinity at id%10 == 0, -infinity at 1,
// NULL at 2, else 2000-01-01 plus id seconds (ts) and plus id%1000 days (d).
func tiBigCell(id int64) (ts, d any) {
	switch id % 10 {
	case 0:
		return int64(math.MaxInt64), int32(math.MaxInt32)
	case 1:
		return int64(math.MinInt64), int32(math.MinInt32)
	case 2:
		return nil, nil
	}
	return tcMillis("2000-01-01 00:00:00") + id*1000, int32(10957 + id%1000)
}

func tiBigRowsOf(finiteOnly bool) []map[string]any {
	rows := make([]map[string]any, 0, tiBigRows)
	for id := int64(1); id <= tiBigRows; id++ {
		ts, d := tiBigCell(id)
		if finiteOnly && id%10 < 2 {
			continue
		}
		rows = append(rows, map[string]any{"id": id, "ts": ts, "d": d})
	}
	return rows
}

func tiSchema(key string) parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: key, Type: parquet.TypeInt64},
		{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
		{Name: "d", Type: parquet.TypeDate, Nullable: true},
	}}
}

func tiRowsOf(key string, spec [][3]string, finiteOnly bool) []map[string]any {
	var rows []map[string]any
	for _, r := range spec {
		if finiteOnly && strings.Contains(r[1], "infinity") {
			continue
		}
		var id int64
		fmt.Sscan(r[0], &id)
		m := map[string]any{key: id, "ts": nil, "d": nil}
		switch r[1] {
		case "":
		case "infinity":
			m["ts"] = int64(math.MaxInt64)
		case "-infinity":
			m["ts"] = int64(math.MinInt64)
		default:
			m["ts"] = tcMillis(r[1])
		}
		switch r[2] {
		case "":
		case "infinity":
			m["d"] = int32(math.MaxInt32)
		case "-infinity":
			m["d"] = int32(math.MinInt32)
		default:
			m["d"] = r[2]
		}
		rows = append(rows, m)
	}
	return rows
}

func tiFixtureOK() bool {
	var buf bytes.Buffer
	pw, err := parquet.NewWriter(&buf, tiSchema("id"), parquet.DefaultWriterConfig())
	if err != nil {
		return false
	}
	if err := pw.WriteRows(tiRowsOf("id", tiSpec, false)); err != nil {
		return false
	}
	return pw.Close() == nil
}

func tiTables() []tmdTable {
	ok := tiFixtureOK()
	return []tmdTable{
		{name: "ti_i", schema: tiSchema("id"), rows: tiRowsOf("id", tiSpec, !ok)},
		{name: "ti_j", schema: tiSchema("k"), rows: tiRowsOf("k", tjSpec, !ok)},
		{name: "ti_f", schema: tiSchema("id"), rows: tiRowsOf("id", tiSpec, true)},
		{name: "ti_p", schema: tiSchema("id"), rows: tiRowsOf("id", tpSpec, !ok)},
		{name: "ti_big", schema: tiSchema("id"), rows: tiBigRowsOf(!ok)},
	}
}

func tiPGFixture() []string {
	q := func(s string) string {
		if s == "" {
			return "NULL"
		}
		return "'" + s + "'"
	}
	vals := func(spec [][3]string, finite bool) string {
		var v []string
		for _, r := range spec {
			if finite && strings.Contains(r[1], "infinity") {
				continue
			}
			v = append(v, fmt.Sprintf("(%s,%s,%s)", r[0], q(r[1]), q(r[2])))
		}
		return strings.Join(v, ",")
	}
	return []string{
		"CREATE TABLE ti_i (id bigint, ts timestamp, d date)",
		"INSERT INTO ti_i VALUES " + vals(tiSpec, false),
		"CREATE TABLE ti_j (k bigint, ts timestamp, d date)",
		"INSERT INTO ti_j VALUES " + vals(tjSpec, false),
		"CREATE TABLE ti_f (id bigint, ts timestamp, d date)",
		"INSERT INTO ti_f VALUES " + vals(tiSpec, true),
		"CREATE TABLE ti_p (id bigint, ts timestamp, d date)",
		"INSERT INTO ti_p VALUES " + vals(tpSpec, false),
		"CREATE TABLE ti_big (id bigint, ts timestamp, d date)",
		fmt.Sprintf("INSERT INTO ti_big SELECT id, CASE id %% 10 WHEN 0 THEN 'infinity'::timestamp WHEN 1 THEN '-infinity' WHEN 2 THEN NULL "+
			"ELSE '2000-01-01'::timestamp + id * interval '1 second' END, CASE id %% 10 WHEN 0 THEN 'infinity'::date WHEN 1 THEN '-infinity' "+
			"WHEN 2 THEN NULL ELSE '2000-01-01'::date + (id %% 1000)::int END FROM generate_series(1, %d) id", tiBigRows),
	}
}

func tiStandalone(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
	t.Helper()
	cfg := wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"}
	if budget > 0 {
		cfg.MemoryBudget = budget
		cfg.SpillDir = t.TempDir()
	}
	db, err := wadjet.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	for _, tbl := range tiTables() {
		if err := db.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
			t.Fatalf("create %s: %v", tbl.name, err)
		}
		if len(tbl.rows) == 0 {
			continue
		}
		rg := 2 // a scan crosses several row groups, each with its own statistics
		if len(tbl.rows) > 100 {
			rg = 4096
		}
		ing := db.NewIngester(tbl.name, tbl.schema, nil, ingest.Config{MaxBufferRows: len(tbl.rows) + 1, RowGroupSize: rg})
		if err := ing.Ingest(ctx, tbl.rows); err != nil {
			t.Fatalf("ingest %s: %v", tbl.name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", tbl.name, err)
		}
	}
	return db
}

func tiCells() []tcCell {
	var cells []tcCell
	add := func(name, sql string) { cells = append(cells, tcCell{name: name, sql: sql}) }
	// (1) INPUT grammar.
	spell := []string{"infinity", "-infinity", "+infinity", "Infinity", "INFINITY", "-INFINITY", " infinity ",
		"  -Infinity  ", "inf", "-inf", "+inf", "- infinity", "+ infinity", "infinity x", "infinity 10:00",
		"--infinity", "++infinity", "+-infinity", "infinityy", "infinit", "infinity BC", "infinity+05",
		"epoch", "2024-01-15"}
	for _, T := range []string{"TIMESTAMP", "DATE"} {
		for _, s := range spell {
			add(fmt.Sprintf("in/cast/%s/%q", T, s), fmt.Sprintf("SELECT CAST(%s AS %s)", tcQuote(s), T))
		}
		add("in/typed_lit/"+T+"/inf", fmt.Sprintf("SELECT %s 'infinity', %s '-infinity'", T, T))
		add("in/coloncolon/"+T+"/inf", fmt.Sprintf("SELECT 'infinity'::%s, '-infinity'::%s", T, T))
		add("in/tab_nl/"+T, fmt.Sprintf("SELECT CAST(chr(9) || 'infinity' || chr(10) AS %s)", T))
		add("in/text_col/"+T, fmt.Sprintf("SELECT CAST(x AS %s) FROM (VALUES ('infinity'), ('-infinity'), ('2024-01-15')) v(x) ORDER BY 1", T))
		add("in/values/"+T, fmt.Sprintf("SELECT x FROM (VALUES (%s 'infinity'), (%s '2024-01-15'), (%s '-infinity')) v(x) ORDER BY 1", T, T, T))
	}
	// (2) OUTPUT.
	add("out/select", "SELECT id, ts, d FROM ti_i ORDER BY id")
	add("out/cast_text", "SELECT id, CAST(ts AS TEXT), CAST(d AS TEXT) FROM ti_i ORDER BY id")
	add("out/concat", "SELECT id, ts || '|' || d FROM ti_i ORDER BY id")
	add("out/json", "SELECT id, json_build_object('t', ts, 'd', d) FROM ti_i ORDER BY id")
	add("out/array", "SELECT ARRAY[ts] FROM ti_i WHERE id IN (3, 4) ORDER BY id")
	add("out/darray", "SELECT ARRAY[d] FROM ti_i WHERE id IN (3, 4) ORDER BY id")
	add("out/upper", "SELECT upper(CAST(ts AS TEXT)) FROM ti_i WHERE id = 3")
	add("out/length", "SELECT length(CAST(d AS TEXT)) FROM ti_i WHERE id = 4")
	// (3) ORDER.
	for _, C := range []string{"ts", "d"} {
		ord := map[string]string{
			"eq_inf":         "SELECT id FROM ti_i WHERE {C} = 'infinity' ORDER BY id",
			"eq_ninf":        "SELECT id FROM ti_i WHERE {C} = '-infinity' ORDER BY id",
			"ne_inf":         "SELECT id FROM ti_i WHERE {C} <> 'infinity' ORDER BY id",
			"lt_inf":         "SELECT id FROM ti_i WHERE {C} < 'infinity' ORDER BY id",
			"le_inf":         "SELECT id FROM ti_i WHERE {C} <= 'infinity' ORDER BY id",
			"gt_ninf":        "SELECT id FROM ti_i WHERE {C} > '-infinity' ORDER BY id",
			"ge_ninf":        "SELECT id FROM ti_i WHERE {C} >= '-infinity' ORDER BY id",
			"gt_inf":         "SELECT id FROM ti_i WHERE {C} > 'infinity' ORDER BY id",
			"lt_ninf":        "SELECT id FROM ti_i WHERE {C} < '-infinity' ORDER BY id",
			"lt_finite":      "SELECT id FROM ti_i WHERE {C} < '2024-01-01' ORDER BY id",
			"gt_finite":      "SELECT id FROM ti_i WHERE {C} > '2000-01-01' ORDER BY id",
			"between":        "SELECT id FROM ti_i WHERE {C} BETWEEN '-infinity' AND 'infinity' ORDER BY id",
			"between_fin":    "SELECT id FROM ti_i WHERE {C} BETWEEN '2000-01-01' AND 'infinity' ORDER BY id",
			"in":             "SELECT id FROM ti_i WHERE {C} IN ('infinity', '-infinity') ORDER BY id",
			"not_in":         "SELECT id FROM ti_i WHERE {C} NOT IN ('infinity', '2024-01-15') ORDER BY id",
			"distinct":       "SELECT id FROM ti_i WHERE {C} IS DISTINCT FROM 'infinity' ORDER BY id",
			"not_distinct":   "SELECT id FROM ti_i WHERE {C} IS NOT DISTINCT FROM '-infinity' ORDER BY id",
			"order_asc":      "SELECT id FROM ti_i ORDER BY {C}, id",
			"order_desc":     "SELECT id FROM ti_i ORDER BY {C} DESC, id",
			"order_nf":       "SELECT id FROM ti_i ORDER BY {C} NULLS FIRST, id",
			"topn":           "SELECT id FROM ti_i ORDER BY {C} DESC NULLS LAST, id LIMIT 3",
			"topn_asc":       "SELECT id FROM ti_i ORDER BY {C}, id LIMIT 2",
			"minmax":         "SELECT min({C}), max({C}) FROM ti_i",
			"minmax_grp":     "SELECT id % 2, min({C}), max({C}) FROM ti_i GROUP BY 1 ORDER BY 1",
			"group":          "SELECT {C}, count(*) FROM ti_i GROUP BY {C} ORDER BY {C}",
			"distinct_sel":   "SELECT DISTINCT {C} FROM ti_i ORDER BY 1",
			"count_distinct": "SELECT count(DISTINCT {C}) FROM ti_i",
			"join":           "SELECT a.id, b.k FROM ti_i a JOIN ti_j b ON a.{C} = b.{C} ORDER BY 1, 2",
			"left_join":      "SELECT b.k, a.id FROM ti_j b LEFT JOIN ti_i a ON a.{C} = b.{C} ORDER BY 1, 2",
			"rank":           "SELECT id, rank() OVER (ORDER BY {C}) FROM ti_i ORDER BY id",
			"rank_desc":      "SELECT id, rank() OVER (ORDER BY {C} DESC) FROM ti_i ORDER BY id",
			"part":           "SELECT id, count(*) OVER (PARTITION BY {C}) FROM ti_i ORDER BY id",
			"lag":            "SELECT id, lag({C}) OVER (ORDER BY id) FROM ti_i ORDER BY id",
			"win_minmax":     "SELECT id, max({C}) OVER (ORDER BY id) FROM ti_i ORDER BY id",
			"union":          "SELECT {C} FROM ti_i UNION SELECT {C} FROM ti_j ORDER BY 1",
			"intersect":      "SELECT {C} FROM ti_i INTERSECT SELECT {C} FROM ti_j ORDER BY 1",
			"except":         "SELECT {C} FROM ti_i EXCEPT SELECT {C} FROM ti_j ORDER BY 1",
			"in_sub":         "SELECT id FROM ti_i WHERE {C} IN (SELECT {C} FROM ti_j) ORDER BY id",
			"not_in_sub":     "SELECT id FROM ti_i WHERE {C} NOT IN (SELECT {C} FROM ti_j WHERE {C} IS NOT NULL) ORDER BY id",
			"scalar_sub":     "SELECT id FROM ti_i WHERE {C} = (SELECT max({C}) FROM ti_j) ORDER BY id",
			"case":           "SELECT id, CASE WHEN {C} = 'infinity' THEN 'pos' WHEN {C} = '-infinity' THEN 'neg' ELSE 'fin' END FROM ti_i ORDER BY id",
			"coalesce":       "SELECT id, COALESCE({C}, 'infinity') FROM ti_i ORDER BY id",
			"greatest":       "SELECT id, GREATEST({C}, '2024-01-01') FROM ti_i ORDER BY id",
			"least":          "SELECT id, LEAST({C}, '2024-01-01') FROM ti_i ORDER BY id",
			"nullif":         "SELECT id, NULLIF({C}, 'infinity') FROM ti_i ORDER BY id",
			"having":         "SELECT id % 2, count(*) FROM ti_i GROUP BY 1 HAVING max({C}) = 'infinity' ORDER BY 1",
			"order_by_cmp":   "SELECT id FROM ti_i ORDER BY {C} < 'infinity', id",
			"group_by_cmp":   "SELECT {C} < 'infinity', count(*) FROM ti_i GROUP BY {C} < 'infinity' ORDER BY 1",
			"lt_all":         "SELECT id FROM ti_i WHERE {C} < ALL ('{\"infinity\"}') ORDER BY id",
			"eq_any":         "SELECT id FROM ti_i WHERE {C} = ANY ('{\"infinity\",\"2024-01-15\"}') ORDER BY id",
			"param_like_cmp": "SELECT id FROM ti_i WHERE {C} = CAST('infinity' AS {T}) ORDER BY id",
			"lt_expr_inf":    "SELECT id FROM ti_f WHERE {C} < CAST('infinity' AS {T}) ORDER BY id",
			"cte":            "WITH x AS (SELECT id, {C} FROM ti_i) SELECT id FROM x WHERE {C} > '9000-01-01' ORDER BY id",
			"derived":        "SELECT id FROM (SELECT id, {C} AS c FROM ti_i) x WHERE c = 'infinity' ORDER BY id",
			"count":          "SELECT count({C}), count(*) FROM ti_i WHERE {C} > '2000-01-01'",
			"finite_ctrl":    "SELECT id FROM ti_f WHERE {C} < 'infinity' ORDER BY id",
		}
		T := map[string]string{"ts": "TIMESTAMP", "d": "DATE"}[C]
		for k, v := range ord {
			add(fmt.Sprintf("ord/%s/%s", k, C), strings.NewReplacer("{C}", C, "{T}", T).Replace(v))
		}
	}
	add("ord/cross_eq", "SELECT id FROM ti_i WHERE d = ts ORDER BY id")
	add("ord/cross_lt", "SELECT id FROM ti_i WHERE d < ts ORDER BY id")
	add("ord/cross_join", "SELECT a.id, b.k FROM ti_i a JOIN ti_j b ON a.d = b.ts ORDER BY 1, 2")
	add("ord/cross_lit", "SELECT DATE 'infinity' = TIMESTAMP 'infinity', DATE '-infinity' < TIMESTAMP '-infinity', DATE 'infinity' > TIMESTAMP '9999-12-31'")
	add("ord/cross_union", "SELECT ts FROM ti_i WHERE id IN (3, 4) UNION SELECT d FROM ti_j ORDER BY 1")
	add("ord/cross_in", "SELECT id FROM ti_i WHERE d IN (TIMESTAMP 'infinity', TIMESTAMP '1970-01-01') ORDER BY id")
	// (4) ARITHMETIC and FUNCTIONS over ti_i rows 3, 4 (sentinels) and 2 (control).
	ar := map[string]string{
		"ts_plus_iv":      "SELECT id, ts + INTERVAL '1 day' FROM ti_i ORDER BY id",
		"ts_minus_iv":     "SELECT id, ts - INTERVAL '1 day' FROM ti_i ORDER BY id",
		"iv_plus_ts":      "SELECT id, INTERVAL '1 day' + ts FROM ti_i ORDER BY id",
		"ts_minus_lit":    "SELECT id, ts - TIMESTAMP '2024-01-01' FROM ti_i ORDER BY id",
		"lit_minus_ts":    "SELECT id, TIMESTAMP '2024-01-01' - ts FROM ti_i ORDER BY id",
		"ts_minus_ts":     "SELECT id, ts - ts FROM ti_i ORDER BY id",
		"d_plus_1":        "SELECT id, d + 1 FROM ti_i ORDER BY id",
		"d_minus_1":       "SELECT id, d - 1 FROM ti_i ORDER BY id",
		"one_plus_d":      "SELECT id, 1 + d FROM ti_i ORDER BY id",
		"d_minus_lit":     "SELECT id, d - DATE '2024-01-01' FROM ti_i ORDER BY id",
		"d_minus_d":       "SELECT id, d - d FROM ti_i ORDER BY id",
		"d_plus_iv":       "SELECT id, d + INTERVAL '1 day' FROM ti_i ORDER BY id",
		"d_minus_iv":      "SELECT id, d - INTERVAL '1 day' FROM ti_i ORDER BY id",
		"ts_to_d":         "SELECT id, CAST(ts AS DATE) FROM ti_i ORDER BY id",
		"d_to_ts":         "SELECT id, CAST(d AS TIMESTAMP) FROM ti_i ORDER BY id",
		"ts_to_ts":        "SELECT id, CAST(ts AS TIMESTAMP) FROM ti_i ORDER BY id",
		"ts_to_bigint":    "SELECT id, CAST(ts AS BIGINT) FROM ti_i WHERE id IN (2, 3) ORDER BY id",
		"d_to_int":        "SELECT id, CAST(d AS INTEGER) FROM ti_i WHERE id IN (2, 3) ORDER BY id",
		"ts_to_double":    "SELECT id, CAST(ts AS DOUBLE PRECISION) FROM ti_i WHERE id IN (2, 3) ORDER BY id",
		"date_trunc_day":  "SELECT id, date_trunc('day', ts) FROM ti_i ORDER BY id",
		"date_trunc_mon":  "SELECT id, date_trunc('month', ts) FROM ti_i ORDER BY id",
		"date_trunc_d":    "SELECT id, date_trunc('year', d) FROM ti_i ORDER BY id",
		"date_part_year":  "SELECT id, date_part('year', ts) FROM ti_i ORDER BY id",
		"date_part_dyear": "SELECT id, date_part('year', d) FROM ti_i ORDER BY id",
		"in_where_arith":  "SELECT id FROM ti_i WHERE ts + INTERVAL '1 day' = 'infinity' ORDER BY id",
		"in_where_darith": "SELECT id FROM ti_i WHERE d + 1 > '9999-12-31' ORDER BY id",
		"sort_by_arith":   "SELECT id FROM ti_i ORDER BY ts + INTERVAL '1 day', id",
		"group_trunc":     "SELECT date_trunc('day', ts), count(*) FROM ti_i GROUP BY 1 ORDER BY 1",
		"lit_ts_iv":       "SELECT TIMESTAMP 'infinity' + INTERVAL '1 day', TIMESTAMP '-infinity' - INTERVAL '1 year'",
		"lit_d_int":       "SELECT DATE 'infinity' + 1, DATE '-infinity' - 1",
		"lit_ts_minus":    "SELECT TIMESTAMP 'infinity' - TIMESTAMP 'infinity'",
		"lit_d_minus":     "SELECT DATE 'infinity' - DATE '2024-01-01'",
		"lit_cast_rt":     "SELECT CAST(CAST(DATE 'infinity' AS TIMESTAMP) AS DATE), CAST(TIMESTAMP '-infinity' AS DATE)",
		"of_ts_iv":        "SELECT TIMESTAMP '294276-12-31 23:59:59' + INTERVAL '1 day'",
		"of_ts_years":     "SELECT TIMESTAMP '2024-01-01' + INTERVAL '300000 years'",
		"of_ts_neg":       "SELECT TIMESTAMP '1970-01-01' - INTERVAL '6000 years'",
		"of_d_plus1":      "SELECT DATE '5874897-12-31' + 1",
		"of_d_to_max":     "SELECT DATE '5874897-12-31' + 2440742",
		"of_d_to_min":     "SELECT DATE '1969-12-31' - 2147483647",
		"of_d_to_max2":    "SELECT DATE '1970-01-02' + 2147483646",
		"of_d_big_ts":     "SELECT CAST(DATE '5874897-12-31' AS TIMESTAMP)",
		"of_int_to_d":     "SELECT CAST(2147483647 AS DATE)",
		"of_int_to_d_min": "SELECT CAST(-2147483648 AS DATE)",
		"of_ts_max_ms":    "SELECT TIMESTAMP '294276-12-31 23:59:59.999'",
		"of_ts_first_out": "SELECT TIMESTAMP '294277-01-01 00:00:00'",
		"of_d_first_out":  "SELECT DATE '5874898-01-01'",
		"finite_iv_ctrl":  "SELECT id, ts + INTERVAL '1 day', d + 1 FROM ti_f ORDER BY id",
	}
	for _, f := range []string{"year", "month", "day", "hour", "minute", "second", "dow", "doy", "epoch",
		"quarter", "week", "century", "decade", "millennium", "isoyear", "julian", "milliseconds", "microseconds", "isodow"} {
		ar["extract_ts_"+f] = fmt.Sprintf("SELECT id, extract(%s FROM ts) FROM ti_i WHERE id IN (2, 3, 4) ORDER BY id", f)
		ar["extract_d_"+f] = fmt.Sprintf("SELECT id, extract(%s FROM d) FROM ti_i WHERE id IN (2, 3, 4) ORDER BY id", f)
	}
	// engine-only names (no PostgreSQL spelling): the specification decides.
	for _, f := range []string{"year(ts)", "month(ts)", "day(d)", "hour(ts)", "minute(ts)", "second(ts)", "quarter(ts)",
		"week(ts)", "day_of_week(ts)", "day_of_year(d)", "last_day_of_month(d)", "epoch(ts)", "to_unixtime(ts)",
		"date_format(ts, '%Y')", "time_bucket(INTERVAL '1 day', ts)", "date_add('day', 1, ts)", "date_sub('day', 1, d)",
		"date_diff('day', ts, TIMESTAMP '2024-01-01')", "to_date(ts)", "at_timezone(ts, 'UTC')", "timezone('UTC', ts)"} {
		ar["eng_"+f] = fmt.Sprintf("SELECT id, %s FROM ti_i WHERE id IN (2, 3, 4) ORDER BY id", f)
	}
	for k, v := range ar {
		add("ar/"+k, v)
	}
	// (5) STORAGE: pruning over row-group / file statistics holding an
	// infinite bound, and the 512 KiB budget door / the shuffled exchange over
	// 30000 rows.
	for _, C := range []string{"ts", "d"} {
		for k, v := range map[string]string{
			"eq_inf":   "SELECT id FROM ti_p WHERE {C} = 'infinity' ORDER BY id",
			"eq_ninf":  "SELECT id FROM ti_p WHERE {C} = '-infinity' ORDER BY id",
			"gt_far":   "SELECT id FROM ti_p WHERE {C} > '9000-01-01' ORDER BY id",
			"lt_far":   "SELECT id FROM ti_p WHERE {C} < '1000-01-01' ORDER BY id",
			"ge_2030":  "SELECT id FROM ti_p WHERE {C} >= '2030-01-01' ORDER BY id",
			"between":  "SELECT id FROM ti_p WHERE {C} BETWEEN '2000-01-01' AND '2001-01-01' ORDER BY id",
			"lt_inf":   "SELECT count(*) FROM ti_p WHERE {C} < 'infinity'",
			"gt_ninf":  "SELECT count(*) FROM ti_p WHERE {C} > '-infinity'",
			"minmax":   "SELECT min({C}), max({C}), count({C}) FROM ti_p",
			"in_inf":   "SELECT id FROM ti_p WHERE {C} IN ('infinity', '1900-01-01') ORDER BY id",
			"ne_inf":   "SELECT count(*) FROM ti_p WHERE {C} <> 'infinity'",
			"le_ninf":  "SELECT id FROM ti_p WHERE {C} <= '-infinity' ORDER BY id",
			"ge_inf":   "SELECT id FROM ti_p WHERE {C} >= 'infinity' ORDER BY id",
			"fin_2050": "SELECT id FROM ti_p WHERE {C} > '2040-01-01' AND {C} < 'infinity' ORDER BY id",
		} {
			add(fmt.Sprintf("prune/%s/%s", k, C), strings.ReplaceAll(v, "{C}", C))
		}
		for k, v := range map[string]string{
			"group_lo":   "SELECT {C}, count(*) FROM ti_big GROUP BY {C} ORDER BY {C} NULLS FIRST LIMIT 3",
			"group_hi":   "SELECT {C}, count(*) FROM ti_big GROUP BY {C} ORDER BY {C} DESC NULLS LAST LIMIT 3",
			"sort_desc":  "SELECT id FROM ti_big ORDER BY {C} DESC NULLS LAST, id LIMIT 4",
			"sort_asc":   "SELECT id FROM ti_big ORDER BY {C}, id LIMIT 4",
			"sort_tail":  "SELECT id FROM ti_big WHERE {C} IS NOT NULL ORDER BY {C} DESC, id DESC LIMIT 2",
			"distinct":   "SELECT count(DISTINCT {C}), count({C}), count(*) FROM ti_big",
			"minmax":     "SELECT min({C}), max({C}) FROM ti_big",
			"rank_first": "SELECT count(*) FROM (SELECT rank() OVER (ORDER BY {C}) r FROM ti_big) x WHERE r = 1",
			"rank_last":  "SELECT max(r) FROM (SELECT rank() OVER (ORDER BY {C} NULLS FIRST) r FROM ti_big) x",
			"join_j":     "SELECT b.k, count(*) FROM ti_big a JOIN ti_j b ON a.{C} = b.{C} GROUP BY b.k ORDER BY b.k",
			"self_join":  "SELECT count(*) FROM ti_big a JOIN ti_big b ON a.id = b.id AND a.{C} = b.{C}",
			"lt_inf":     "SELECT count(*) FROM ti_big WHERE {C} < 'infinity'",
			"eq_ninf":    "SELECT count(*), min(id), max(id) FROM ti_big WHERE {C} = '-infinity'",
			"part_count": "SELECT {C}, max(c) FROM (SELECT {C}, count(*) OVER (PARTITION BY {C}) c FROM ti_big) x WHERE {C} IN ('infinity', '-infinity') GROUP BY {C} ORDER BY {C}",
		} {
			add(fmt.Sprintf("big/%s/%s", k, C), strings.ReplaceAll(v, "{C}", C))
		}
	}
	// (1) a bound parameter: unknown-typed (OID 0), text (25) and the column's
	// own type (1114 / 1082), text format, against the stored infinities.
	ownOID := map[string]uint32{"ts": 1114, "d": 1082}
	for _, C := range []string{"ts", "d"} {
		for _, oid := range []uint32{0, 25, ownOID[C]} {
			for _, x := range []string{"infinity", "-infinity", "+infinity", " Infinity ", "inf"} {
				cells = append(cells, tcCell{
					name:   fmt.Sprintf("param%d/%s/%q", oid, C, x),
					sql:    fmt.Sprintf("SELECT id FROM ti_i WHERE %s = $1 ORDER BY id", C),
					params: []string{x}, oids: []uint32{oid},
				})
			}
			cells = append(cells, tcCell{
				name:   fmt.Sprintf("param%d/%s/lt_inf", oid, C),
				sql:    fmt.Sprintf("SELECT id FROM ti_i WHERE %s < $1 ORDER BY id", C),
				params: []string{"infinity"}, oids: []uint32{oid},
			})
			cells = append(cells, tcCell{
				name:   fmt.Sprintf("param%d/%s/select", oid, C),
				sql:    "SELECT $1",
				params: []string{"-infinity"}, oids: []uint32{oid},
			})
		}
	}
	return cells
}

type tiPin struct{ pg, kept, row string }

// tiPins reads testdata/arc_ti_temporal_infinity_pg17.tsv: cell, PostgreSQL
// 17.11's answer, and for a kept divergence the engine's answer and its
// catalog row.
func tiPins(t *testing.T) map[string]tiPin {
	t.Helper()
	b, err := os.ReadFile("testdata/arc_ti_temporal_infinity_pg17.tsv")
	if err != nil {
		if os.Getenv("TI_DUMP") != "" {
			return map[string]tiPin{}
		}
		t.Fatal(err)
	}
	pins := map[string]tiPin{}
	for _, line := range strings.Split(string(b), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		cols := strings.Split(line, "\t")
		for len(cols) < 4 {
			cols = append(cols, "")
		}
		pins[cols[0]] = tiPin{pg: cols[1], kept: cols[2], row: cols[3]}
	}
	return pins
}

// TestArcTIInfinityEveryArm: PostgreSQL's two infinite values of TIMESTAMP
// and DATE — read from text (the one temporal grammar), stored, printed,
// ordered, grouped, joined, pruned, spilled and computed on — on five doors
// over pgwire (the embedded engine, the embedded engine under a 512 KiB
// budget, and the coordinator's router over the DAG, the shuffled DAG and the
// DAG with four morsel workers) against PostgreSQL 17.11's answer, re-measured
// when WADJET_PG_DSN names a server. The carrier's extremes are the values
// (parquet.DatePosInfinity …); a cell PostgreSQL answers that this engine
// refuses is pinned with its catalog row (temporal r2).
//
// At 8e681724 the text 'infinity' was 22007 in every position, and the
// fixture's infinite rows could not be stored (the writer refused the
// extremes 22008): the cells over ti_i, ti_p and ti_big answer the finite
// rows alone there.
func TestArcTIInfinityEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five doors over pgwire")
	}
	ctx := context.Background()
	cells := tiCells()
	seenName := map[string]bool{}
	for _, c := range cells {
		if seenName[c.name] {
			t.Fatalf("cell %q generated twice", c.name)
		}
		seenName[c.name] = true
	}
	dumpOut := os.Getenv("TI_DUMP")
	write := func(name string, dump []string) {
		if dumpOut == "" {
			return
		}
		if err := os.WriteFile(dumpOut+"."+name+".tsv", []byte(strings.Join(dump, "\n")+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	pins := tiPins(t)
	if dsn := os.Getenv("WADJET_PG_DSN"); dsn != "" {
		pg, err := pgconn.Connect(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer pg.Close(ctx)
		pre := append([]string{"SET statement_timeout = '30s'", "SET TimeZone = 'UTC'", "SET DateStyle = 'ISO, MDY'",
			"DROP SCHEMA IF EXISTS tiarc CASCADE", "CREATE SCHEMA tiarc", "SET search_path = tiarc"}, tiPGFixture()...)
		for _, s := range pre {
			if _, err := pg.Exec(ctx, s).ReadAll(); err != nil {
				t.Fatalf("%s: %v", s, err)
			}
		}
		var dump []string
		for _, c := range cells {
			got := tcRun(ctx, pg, c)
			dump = append(dump, c.name+"\t"+got)
			if p, ok := pins[c.name]; ok && p.pg != got {
				t.Errorf("PostgreSQL %s answered %s, pinned %s", c.name, got, p.pg)
			}
		}
		write("pg", dump)
		if dumpOut != "" {
			return
		}
	}
	serve := func(db *wadjet.DB, c *Coordinator) string {
		srv := pgwire.NewServer(db, pgwire.Config{}, nil)
		if c != nil {
			srv.SetRouter(NewQueryRouter(c))
		}
		if err := srv.Start("127.0.0.1:0"); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { srv.Shutdown() })
		return srv.Addr()
	}
	stand := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, tiTables())
		return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
	}
	doors := []struct{ name, addr string }{
		{"single", serve(tiStandalone(t, ctx, 0), nil)},
		{"spilled512k", serve(tiStandalone(t, ctx, 512*1024), nil)},
		{"dag", serve(tiStandalone(t, ctx, 0), stand(nil))},
		{"dag-shuffled", serve(tiStandalone(t, ctx, 0), stand(nil, func(c *Config) { c.BroadcastBytesOverride = 1 }))},
		{"dag-morsel4", serve(tiStandalone(t, ctx, 0), stand(func(w *worker.Config) { w.MorselWorkers = 4 }))},
	}
	for _, d := range doors {
		t.Run(d.name, func(t *testing.T) {
			conn, err := pgconn.Connect(ctx, "postgres://wadjet@"+d.addr+"/test?sslmode=disable")
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close(ctx)
			var dump []string
			for _, c := range cells {
				got := tcRun(ctx, conn, c)
				dump = append(dump, c.name+"\t"+got)
				if dumpOut != "" {
					continue
				}
				p, ok := pins[c.name]
				if !ok {
					t.Errorf("%s: no pinned PostgreSQL answer (engine %s)", c.name, got)
					continue
				}
				want, why := p.pg, ""
				if p.kept != "" {
					want, why = p.kept, "; kept: "+p.row
				}
				if got != want {
					t.Errorf("%s on %s: %s\n  got  %s\n  want %s (PostgreSQL 17.11 %s%s)", c.name, d.name, c.sql, got, want, p.pg, why)
				}
			}
			write(d.name, dump)
		})
	}
	if dumpOut != "" {
		return
	}
	for name := range pins {
		if !seenName[name] {
			t.Errorf("pinned cell %q is no longer generated", name)
		}
	}
}
