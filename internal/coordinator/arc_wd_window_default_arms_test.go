// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"fmt"
	"math/big"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// wdTables is the LAG / LEAD default fixture (#1435): one column of every
// value type the coverage table names, three partitions of sizes 3, 2 and 1,
// and a NULL row inside the first — so a default read as the value's type
// (2.5 truncated to 2), a default read from the wrong row, and a shifted
// value read as the default each answer differently from PostgreSQL.
//
//	id | g | i  | b  | d   | n    | s | bo    | dt         | ts
//	1  | 1 | 10 | 10 | 1.5 | 1.25 | a | true  | 2024-01-01 | 2024-01-01 10:00:00
//	2  | 1 | 20 | 20 | 2.5 | 2.25 | b | false | 2024-01-02 | 2024-01-02 10:00:00
//	3  | 1 | (every value column NULL)
//	4  | 2 | 40 | 40 | 4.5 | 4.25 | d | true  | 2024-01-04 | 2024-01-04 10:00:00
//	5  | 2 | 50 | 50 | 5.5 | 5.25 | e | false | 2024-01-05 | 2024-01-05 10:00:00
//	6  | 3 | 60 | 60 | 6.5 | 6.25 | f | true  | 2024-01-06 | 2024-01-06 10:00:00
func wdTables() []tmdTable {
	col := func(n string, t parquet.TypeID) parquet.Column {
		return parquet.Column{Name: n, Type: t, Nullable: true}
	}
	num := parquet.Column{Name: "n", Type: parquet.TypeDecimal, Precision: 10, Scale: 2, Nullable: true}
	t := tmdTable{name: "wd_t", schema: parquet.Schema{Columns: []parquet.Column{
		col("id", parquet.TypeInt64), col("g", parquet.TypeInt64), col("i", parquet.TypeInt32),
		col("b", parquet.TypeInt64), col("d", parquet.TypeFloat64), num, col("s", parquet.TypeString),
		col("bo", parquet.TypeBool), col("dt", parquet.TypeDate), col("ts", parquet.TypeTimestamp),
	}}}
	for _, r := range []struct {
		id, g int64
		v     int
		s     string
		bo    bool
	}{{1, 1, 1, "a", true}, {2, 1, 2, "b", false}, {3, 1, 0, "", false}, {4, 2, 4, "d", true}, {5, 2, 5, "e", false}, {6, 3, 6, "f", true}} {
		row := map[string]any{"id": r.id, "g": r.g}
		if r.v == 0 {
			for _, c := range []string{"i", "b", "d", "n", "s", "bo", "dt", "ts"} {
				row[c] = nil
			}
		} else {
			day := time.Date(2024, 1, r.v, 10, 0, 0, 0, time.UTC)
			row["i"], row["b"] = int32(r.v*10), int64(r.v*10)
			row["d"] = float64(r.v) + 0.5
			row["n"] = dtpDecimal128(big.NewInt(int64(r.v*100 + 25)))
			row["s"], row["bo"] = r.s, r.bo
			row["dt"] = day.Format("2006-01-02")
			row["ts"] = day.UnixMilli()
		}
		t.rows = append(t.rows, row)
	}
	return []tmdTable{t}
}

// wdPGFixture is the same fixture as PostgreSQL DDL + INSERT.
const wdPGFixture = `DROP TABLE IF EXISTS wd_t;
CREATE TABLE wd_t (id bigint, g bigint, i integer, b bigint, d double precision, n numeric(10,2), s text, bo boolean, dt date, ts timestamp);
INSERT INTO wd_t VALUES
 (1,1,10,10,1.5,1.25,'a',true,'2024-01-01','2024-01-01 10:00:00'),
 (2,1,20,20,2.5,2.25,'b',false,'2024-01-02','2024-01-02 10:00:00'),
 (3,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),
 (4,2,40,40,4.5,4.25,'d',true,'2024-01-04','2024-01-04 10:00:00'),
 (5,2,50,50,5.5,5.25,'e',false,'2024-01-05','2024-01-05 10:00:00'),
 (6,3,60,60,6.5,6.25,'f',true,'2024-01-06','2024-01-06 10:00:00');`

// wdCell is one statement: SELECT id, <expr> AS w FROM wd_t, or sql verbatim.
type wdCell struct{ name, sql string }

var (
	wdValues = []struct{ name, v string }{
		{"int", "i"}, {"bigint", "b"}, {"double", "d"}, {"numeric", "n"}, {"text", "s"},
		{"bool", "bo"}, {"date", "dt"}, {"timestamp", "ts"}, {"array", "ARRAY[i]"},
	}
	// The default spellings. `same` is the value column itself; `wide`
	// (NX's decimal carrier) and `sub` (SS's scalar subquery) are controls
	// of other arcs' seams, measured here, not fixed.
	wdDefaults = []struct{ name, v string }{
		{"omit", ""}, {"null", "NULL"}, {"int", "7"}, {"dec", "2.5"}, {"wide", "14.0000000000000000001"},
		{"dbl", "1e300"}, {"text", "'z'"}, {"qnum", "'7'"}, {"bool", "true"}, {"date", "DATE '2020-01-01'"},
		{"ts", "TIMESTAMP '2020-01-01 00:00:00'"}, {"same", ""}, {"colwide", "d"},
		{"cast", "CAST(7 AS BIGINT)"}, {"expr", "1 + 1"}, {"sub", "(SELECT 9)"}, {"arr", "ARRAY[9]"},
	}
	wdWindows = []struct{ name, over string }{
		{"ord", "(ORDER BY id)"},
		{"both", "(PARTITION BY g ORDER BY id)"},
		{"frame", "(ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)"},
	}
)

// wdCells is the coverage table, generated (the notes enumerate it once):
//
//   - type/<value>/<default>: every value type × every default spelling, as
//     LAG(v, 10, d) — past every partition, so every row is the default —
//     and LEAD(v, 1, d) under PARTITION BY, where shifted values and defaults
//     share the column: the declared type and both halves of its values;
//   - shape/<fn>/<offset>/<window>/<value>/<default>: the widening pairs ×
//     offset {1, 10, 0} × window, LAG and LEAD;
//   - use/: the consumers of a widened column — a comparison, arithmetic,
//     GROUP BY, ORDER BY, a CASE arm, a derived table;
//   - issue/: #1435's three statements verbatim.
func wdCells() []wdCell {
	sel := func(e string) string { return "SELECT id, " + e + " AS w FROM wd_t" }
	call := func(fn, v, off, def, over string) string {
		args := v + ", " + off
		if def != "" {
			args += ", " + def
		}
		return fn + "(" + args + ") OVER " + over
	}
	var out []wdCell
	for _, v := range wdValues {
		for _, d := range wdDefaults {
			dv := d.v
			if d.name == "same" {
				dv = v.v
			}
			out = append(out,
				wdCell{"type/lag/" + v.name + "/" + d.name, sel(call("LAG", v.v, "10", dv, "(ORDER BY id)"))},
				wdCell{"type/lead/" + v.name + "/" + d.name, sel(call("LEAD", v.v, "1", dv, "(PARTITION BY g ORDER BY id)"))})
		}
	}
	pairs := []struct{ value, vname, def, dname string }{
		{"b", "bigint", "2.5", "dec"}, {"i", "int", "2.5", "dec"}, {"b", "bigint", "d", "colwide"},
		{"n", "numeric", "0.125", "dec3"}, {"n", "numeric", "d", "colwide"}, {"i", "int", "CAST(7 AS BIGINT)", "cast"},
		{"b", "bigint", "'7'", "qnum"}, {"dt", "date", "ts", "colts"}, {"dt", "date", "TIMESTAMP '2020-01-01 00:00:00'", "ts"},
		{"b", "bigint", "id", "colsame"}, {"b", "bigint", "'a'", "text"},
	}
	for _, fn := range []string{"LAG", "LEAD"} {
		for _, off := range []string{"1", "10", "0"} {
			for _, w := range wdWindows {
				for _, p := range pairs {
					out = append(out, wdCell{"shape/" + strings.ToLower(fn) + "/" + off + "/" + w.name + "/" + p.vname + "/" + p.dname,
						sel(call(fn, p.value, off, p.def, w.over))})
				}
			}
		}
	}
	lag := "LAG(b, 1, 2.5) OVER (ORDER BY id)"
	out = append(out,
		wdCell{"use/eq", sel(lag + " = 2.5")},
		wdCell{"use/arith", sel(lag + " + 1")},
		wdCell{"use/mul", sel(lag + " * 2")},
		wdCell{"use/case", sel("CASE WHEN id = 1 THEN " + lag + " ELSE 0 END")},
		wdCell{"use/coalesce", sel("COALESCE(" + lag + ", 0)")},
		wdCell{"use/derived", "SELECT id, w FROM (SELECT id, " + lag + " AS w FROM wd_t) s WHERE w > 2"},
		wdCell{"use/group", "SELECT w, count(*) AS c FROM (SELECT " + lag + " AS w FROM wd_t) s GROUP BY w"},
		wdCell{"use/sum", "SELECT sum(w) AS w FROM (SELECT " + lag + " AS w FROM wd_t) s"},
		wdCell{"use/order", "SELECT id, " + lag + " AS w FROM wd_t ORDER BY w, id"},
		wdCell{"use/two", "SELECT id, LAG(b, 1, 2.5) OVER (ORDER BY id) AS w, LEAD(b, 1, d) OVER (ORDER BY id) AS w2 FROM wd_t"},
		wdCell{"use/derived_value", "SELECT id, LAG(v, 1, 2.5) OVER (ORDER BY id) AS w FROM (SELECT id, b * 2 AS v FROM wd_t) s"},
		wdCell{"use/derived_default", "SELECT id, LAG(b, 1, e) OVER (ORDER BY id) AS w FROM (SELECT id, b, d * 2 AS e FROM wd_t) s"},
		wdCell{"use/aggregate_value", "SELECT g, LAG(SUM(b), 1, 2.5) OVER (ORDER BY g) AS w FROM wd_t GROUP BY g"},
		wdCell{"use/cte", "WITH c AS (SELECT id, " + lag + " AS w FROM wd_t) SELECT id, w FROM c"},
		wdCell{"use/subquery_body", "SELECT (SELECT " + lag + " FROM wd_t ORDER BY id LIMIT 1) AS w"},
		wdCell{"use/union", "SELECT id, " + lag + " AS w FROM wd_t UNION ALL SELECT 0, 0.5"},
		// The value's ORIGIN: a VALUES list, a LATERAL output (the derived
		// table, CTE, aggregate output, subquery body and set operation are
		// above; a recursive CTE's column is the embedded gate's, since this
		// harness's DAG arms refuse a recursive CTE at base too).
		wdCell{"origin/values", "SELECT id, LAG(v, 1, 2.5) OVER (ORDER BY id) AS w FROM (VALUES (CAST(1 AS BIGINT), CAST(10 AS BIGINT)), (CAST(2 AS BIGINT), CAST(20 AS BIGINT))) AS s(id, v)"},
		wdCell{"origin/lateral", "SELECT id, LAG(l.x, 1, 2.5) OVER (ORDER BY id) AS w FROM wd_t t, LATERAL (SELECT t.b * 2 AS x) l"},
		// Over a GROUP BY the argument list is respelled one argument at a
		// time: the offset, the default and NTH_VALUE's n were dropped
		// (LAG(SUM(b), 2) read offset 1), and an aggregate in the default
		// was never computed.
		wdCell{"agg/offset", "SELECT g, LAG(SUM(b), 2) OVER (ORDER BY g) AS w FROM wd_t GROUP BY g"},
		wdCell{"agg/nth", "SELECT g, NTH_VALUE(SUM(b), 2) OVER (ORDER BY g) AS w FROM wd_t GROUP BY g"},
		wdCell{"agg/default_aggregate", "SELECT g, LEAD(SUM(b), 1, SUM(d)) OVER (ORDER BY g) AS w FROM wd_t GROUP BY g"},
		wdCell{"agg/default_key", "SELECT g, LAG(SUM(b), 1, g) OVER (ORDER BY g) AS w FROM wd_t GROUP BY g"},
		// A window key EXPRESSION over a derived table's computed alias
		// reads the alias's definition on the DAG arms too (the same respell
		// LAG / LEAD's materialized value and default take).
		wdCell{"derived/sum_key_expr", "SELECT id, SUM(v + 0) OVER (ORDER BY id) AS w FROM (SELECT id, b * 2 AS v FROM wd_t) s"},
		wdCell{"derived/partition_key_expr", "SELECT id, ROW_NUMBER() OVER (PARTITION BY v + 0 ORDER BY id) AS w FROM (SELECT id, b * 2 AS v FROM wd_t) s"},
		wdCell{"derived/lag_offset_default", "SELECT id, LEAD(v, 2, e) OVER (PARTITION BY g ORDER BY id) AS w FROM (SELECT id, g, b * 2 AS v, d * 2 AS e FROM wd_t) s"},
		// The default is evaluated on every row here, only on the rows it
		// fills in PostgreSQL (catalog aggregates-windows r21); a constant
		// default that does not coerce raises only where a row is read
		// (r22).
		wdCell{"gap/default_every_row", "SELECT id, LAG(b, 1, 10 / (id - 2)) OVER (ORDER BY id) AS w FROM wd_t"},
		wdCell{"gap/no_rows_text", "SELECT id, LAG(b, 1, 'a') OVER (ORDER BY id) AS w FROM wd_t WHERE id > 100"},
		wdCell{"issue/lag1", "SELECT id, LAG(b, 1, 2.5) OVER (ORDER BY id) AS w FROM wd_t"},
		wdCell{"issue/lag10part", "SELECT id, LAG(b, 10, 2.5) OVER (PARTITION BY g ORDER BY id) AS w FROM wd_t"},
		wdCell{"issue/lead10", "SELECT id, LEAD(b, 10, 2.5) OVER (ORDER BY id) AS w FROM wd_t"},
	)
	return append(append(out, wdComputedValueCells()...), wdNodeKindCells()...)
}

// wdComputedValueCells: a COMPUTED value (an expression, a function, a CAST)
// beside a default. The value's declaration is typed from its own tree, so
// the default widens it: `LAG(b * 2, 1, d)` is double precision 1.5 where it
// answered bigint 1, and `LEAD(CAST(d AS REAL), 1, 2.5)` answers instead of
// failing the write.
func wdComputedValueCells() []wdCell {
	values := []struct{ name, v string }{
		{"bmul", "b * 2"}, {"bplus", "b + 1"}, {"abs", "abs(b)"}, {"castbig", "CAST(b AS BIGINT)"},
		{"iplus", "i + 0"}, {"dmul", "d * 2"}, {"real", "CAST(d AS REAL)"}, {"num37", "CAST(b AS NUMERIC(37,0))"},
		{"num362", "CAST(b AS NUMERIC(36,2))"}, {"numu", "CAST(b AS NUMERIC)"}, {"nmul", "n * 1"},
		{"upper", "upper(s)"}, {"castdate", "CAST(dt AS DATE)"}, {"case", "CASE WHEN b > 20 THEN b ELSE 0 END"},
	}
	defaults := []struct{ name, v string }{
		{"dec", "2.5"}, {"half", "0.5"}, {"dec3", "0.125"}, {"int", "7"}, {"dcol", "d"}, {"ncol", "n"},
		{"null", "NULL"}, {"ts", "TIMESTAMP '2020-01-01 00:00:00'"}, {"tscol", "ts"}, {"text", "'q'"},
	}
	var out []wdCell
	for _, v := range values {
		for _, d := range defaults {
			out = append(out, wdCell{"cv/lag/" + v.name + "/" + d.name,
				"SELECT id, LAG(" + v.v + ", 1, " + d.v + ") OVER (ORDER BY id) AS w FROM wd_t"})
		}
		for _, d := range defaults[:5:5] {
			if d.name == "half" || d.name == "dec3" {
				continue
			}
			out = append(out, wdCell{"cv/lead/" + v.name + "/" + d.name,
				"SELECT id, LEAD(" + v.v + ", 1, " + d.v + ") OVER (PARTITION BY g ORDER BY id) AS w FROM wd_t"})
		}
	}
	return out
}

// wdNodeKindCells: the node kinds between a window and the derived column its
// value, default or key expression reads. LAG / LEAD materialize the widened
// value (`cast(v as …)`) and the default as window key expressions, and on
// the DAG arms a key expression over a COMPUTED derived alias read NULL unless
// the window's input reached a Scan through Projects and Filters alone — a
// JOIN (either side), a semi join, a LIMIT / OFFSET, a sort or a window
// below answered NULL for every shifted value.
func wdNodeKindCells() []wdCell {
	cols := "id, g, b, b * 2 AS v, d * 2 AS e"
	rels := []struct{ name, from string }{
		{"project", "(SELECT " + cols + " FROM wd_t) s"},
		{"filter", "(SELECT " + cols + " FROM wd_t WHERE id > 1) s"},
		{"join_lcomp", "(SELECT t.id, t.g, t.b, t.b * 2 AS v, t.d * 2 AS e FROM wd_t t JOIN wd_t u ON u.id = t.id + 1) s"},
		{"join_rcomp", "(SELECT t.id, t.g, t.b, u.b * 2 AS v, u.d * 2 AS e FROM wd_t t JOIN wd_t u ON u.id = t.id + 1) s"},
		{"left_join", "(SELECT t.id, t.g, t.b, u.b * 2 AS v, u.d * 2 AS e FROM wd_t t LEFT JOIN wd_t u ON u.id = t.id + 1) s"},
		{"semi_join", "(SELECT " + cols + " FROM wd_t WHERE id IN (SELECT id FROM wd_t WHERE g < 3)) s"},
		{"limit", "(SELECT " + cols + " FROM wd_t ORDER BY id LIMIT 5) s"},
		{"offset", "(SELECT " + cols + " FROM wd_t ORDER BY id LIMIT 10 OFFSET 1) s"},
		{"sort", "(SELECT " + cols + " FROM wd_t ORDER BY id DESC) s"},
		{"window", "(SELECT id, g, b, b * 2 + ROW_NUMBER() OVER (ORDER BY id) AS v, d * 2 + ROW_NUMBER() OVER (ORDER BY id) AS e FROM wd_t) s"},
		{"aggregate", "(SELECT g AS id, g, MAX(b) AS b, MAX(b) * 2 AS v, MAX(d) * 2 AS e FROM wd_t GROUP BY g) s"},
		{"distinct", "(SELECT DISTINCT " + cols + " FROM wd_t) s"},
		{"union_all", "(SELECT " + cols + " FROM wd_t UNION ALL SELECT 7, 3, 70, 140, 1.5) s"},
		{"union", "(SELECT " + cols + " FROM wd_t UNION SELECT 7, 3, 70, 140, 1.5) s"},
		{"intersect", "(SELECT " + cols + " FROM wd_t INTERSECT SELECT " + cols + " FROM wd_t WHERE id < 5) s"},
		{"except", "(SELECT " + cols + " FROM wd_t EXCEPT SELECT " + cols + " FROM wd_t WHERE id = 2) s"},
		{"values", "(SELECT id, g, b, b * 2 AS v, d * 2 AS e FROM (VALUES (CAST(1 AS BIGINT), CAST(1 AS BIGINT), CAST(10 AS BIGINT), CAST(1.5 AS DOUBLE PRECISION)), (2, 1, 20, 2.5), (3, 1, NULL, NULL), (4, 2, 40, 4.5)) x(id, g, b, d)) s"},
		{"lateral", "(SELECT t.id, t.g, t.b, l.v, l.e FROM wd_t t, LATERAL (SELECT t.b * 2 AS v, t.d * 2 AS e) l) s"},
		{"nested", "(SELECT id, g, b, v + 1 AS v, e + 1 AS e FROM (SELECT " + cols + " FROM wd_t) s1) s"},
		// A recursive CTE is the embedded gate's: this harness's DAG arms
		// refuse one at base too (`stage scan-0 has no dependencies`).
	}
	fns := []struct{ name, f string }{
		{"val", "LAG(v, 1, 2.5) OVER (ORDER BY id)"},
		{"def", "LAG(b, 1, e) OVER (ORDER BY id)"},
		{"sum", "SUM(v + 0) OVER (ORDER BY id)"},
		{"lead", "LEAD(v, 1, e) OVER (PARTITION BY g ORDER BY id)"},
	}
	var out []wdCell
	for _, r := range rels {
		for _, f := range fns {
			out = append(out, wdCell{"nk/" + r.name + "/" + f.name, "SELECT id, " + f.f + " AS w FROM " + r.from})
		}
	}
	cte := "WITH c AS (SELECT " + cols + " FROM wd_t) SELECT id, %s AS w FROM c"
	cteJoin := "WITH c AS (SELECT " + cols + " FROM wd_t) SELECT c.id, %s AS w FROM c JOIN wd_t u ON u.id = c.id"
	qualified := strings.NewReplacer("(v", "(c.v", "(b", "(c.b", ", e)", ", c.e)", "BY g", "BY c.g", "BY id", "BY c.id")
	for _, f := range fns {
		out = append(out,
			wdCell{"nk/cte/" + f.name, fmt.Sprintf(cte, f.f)},
			wdCell{"nk/cte_join/" + f.name, fmt.Sprintf(cteJoin, qualified.Replace(f.f))})
	}
	arms := "FROM (SELECT id, g, b * 2 AS w, d * 2 AS e FROM wd_t) x JOIN (SELECT id, b * 3 AS w, d * 3 AS e FROM wd_t) y ON x.id = y.id"
	derivedOver := func(f, from string) string { return "SELECT id, " + f + " AS w FROM " + from }
	out = append(out,
		wdCell{"nk/armjoin_x/val", "SELECT x.id, LAG(x.w, 1, 2.5) OVER (ORDER BY x.id) AS w " + arms},
		wdCell{"nk/armjoin_y/val", "SELECT x.id, LAG(y.w, 1, 2.5) OVER (ORDER BY x.id) AS w " + arms},
		wdCell{"nk/armjoin_y/def", "SELECT x.id, LAG(x.w, 1, y.e) OVER (ORDER BY x.id) AS w " + arms},
		wdCell{"nk/armjoin_x/sum", "SELECT x.id, SUM(x.w + 0) OVER (ORDER BY x.id) AS w " + arms},
		wdCell{"nk/armjoin_y/sum", "SELECT x.id, SUM(y.w + 0) OVER (ORDER BY x.id) AS w " + arms},
		wdCell{"nk/armjoin_y/lead", "SELECT x.id, LEAD(y.w, 1, x.e) OVER (PARTITION BY x.g ORDER BY x.id) AS w " + arms},
		wdCell{"nk/leftarm_y/val", "SELECT x.id, LAG(y.w, 1, 2.5) OVER (ORDER BY x.id) AS w FROM (SELECT id, g, b * 2 AS w FROM wd_t) x LEFT JOIN (SELECT id, b * 3 AS w FROM wd_t) y ON y.id = x.id + 1"},
		wdCell{"nk/scalar_sub/val", derivedOver("LAG(v, 1, 2.5) OVER (ORDER BY id)", "(SELECT id, b * (SELECT 2) AS v FROM wd_t) s")},
		wdCell{"nk/two_windows/val", "SELECT id, LAG(v, 1, 2.5) OVER (ORDER BY id) AS w, SUM(e + 0) OVER (ORDER BY id) AS w2 FROM (SELECT t.id, t.b * 2 AS v, t.d * 2 AS e FROM wd_t t JOIN wd_t u ON u.id = t.id + 1) s"},
		wdCell{"nk/window_slot/sum", "SELECT id, SUM(w + 0) OVER (ORDER BY id) AS w2 FROM (SELECT id, SUM(b) OVER (PARTITION BY g) AS w FROM wd_t) s"},
		wdCell{"nk/window_slot/val", "SELECT id, LAG(w, 1, 2.5) OVER (ORDER BY id) AS w2 FROM (SELECT id, SUM(b) OVER (PARTITION BY g) AS w FROM wd_t) s"},
		wdCell{"nk/window_slot/part", "SELECT id, ROW_NUMBER() OVER (PARTITION BY w + 0 ORDER BY id) AS w2 FROM (SELECT id, SUM(b) OVER (PARTITION BY g) AS w FROM wd_t) s"},
		wdCell{"nk/project/part", derivedOver("ROW_NUMBER() OVER (PARTITION BY v + 0 ORDER BY id)", "(SELECT id, b * 2 AS v FROM wd_t) s")},
		wdCell{"nk/join_lcomp/part", derivedOver("ROW_NUMBER() OVER (PARTITION BY v % 3 ORDER BY id)", "(SELECT t.id, t.b * 2 AS v FROM wd_t t JOIN wd_t u ON u.id = t.id + 1) s")},
		wdCell{"nk/limit/order", derivedOver("RANK() OVER (ORDER BY v * -1, id)", "(SELECT id, b * 2 AS v FROM wd_t ORDER BY id LIMIT 5) s")},
		// A computed alias over an AGGREGATE's outputs is published by the
		// aggregate stage itself; materializing it below the aggregate is
		// refused, so the key reads the published alias.
		wdCell{"nk/aggregate_comp/val", "SELECT g, LAG(v, 1, 2.5) OVER (ORDER BY g) AS w FROM (SELECT g, MAX(b) * 2 AS v FROM wd_t GROUP BY g) s"},
		wdCell{"nk/aggregate_comp/def", "SELECT g, LAG(m, 1, e) OVER (ORDER BY g) AS w FROM (SELECT g, MAX(b) AS m, MAX(d) * 2 AS e FROM wd_t GROUP BY g) s"},
		wdCell{"nk/nested_join/val", derivedOver("LAG(v, 1, 2.5) OVER (ORDER BY id)", "(SELECT id, v + 1 AS v FROM (SELECT t.id, t.b * 2 AS v FROM wd_t t JOIN wd_t u ON u.id = t.id + 1) s1) s2")},
	)
	return out
}

func wdPGAnswers(t *testing.T) map[string]string {
	t.Helper()
	f, err := os.Open("testdata/arc_wd_window_default_pg17.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string]string{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, want, ok := strings.Cut(line, "\t")
		if !ok {
			t.Fatalf("malformed answer line %q", line)
		}
		out[name] = want
	}
	return out
}

// TestArcWDGenerate dumps the cells as name<TAB>sql for the oracle run
// (WD_GEN=<path>); skipped otherwise.
func TestArcWDGenerate(t *testing.T) {
	path := os.Getenv("WD_GEN")
	if path == "" {
		t.Skip("WD_GEN unset")
	}
	var b strings.Builder
	for _, c := range wdCells() {
		b.WriteString(c.name + "\t" + c.sql + "\n")
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// wdResult is one arm's answer: the declared output and the rows.
type wdResult struct {
	schema []parquet.Column
	rows   [][]any
}

func wdStandalone(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
	t.Helper()
	cfg := wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"}
	if budget > 0 {
		cfg.MemoryBudget = budget
		cfg.SpillDir = t.TempDir()
	}
	db, err := wadjet.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("open standalone: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	for _, tbl := range wdTables() {
		if err := db.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
			t.Fatalf("create %s: %v", tbl.name, err)
		}
		ing := db.NewIngester(tbl.name, tbl.schema, nil, ingest.Config{MaxBufferRows: len(tbl.rows) + 1, RowGroupSize: 2})
		if err := ing.Ingest(ctx, tbl.rows); err != nil {
			t.Fatalf("ingest %s: %v", tbl.name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", tbl.name, err)
		}
	}
	return db
}

type wdArm struct {
	name string
	run  func(string) (wdResult, error)
}

func wdArms(t *testing.T, ctx context.Context) []wdArm {
	t.Helper()
	stand := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, wdTables())
		if wcfg != nil {
			return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
		}
		return tmdCoordinator(t, ctx, infra, opts...)
	}
	single, spilled := wdStandalone(t, ctx, 0), wdStandalone(t, ctx, 512*1024)
	runSingle := func(db *wadjet.DB) func(string) (wdResult, error) {
		return func(sql string) (res wdResult, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("PANIC: %v", r)
				}
			}()
			out, qerr := db.Query(ctx, sql)
			if qerr != nil {
				return wdResult{}, qerr
			}
			res.schema = out.OutputSchema
			for i := range out.Rows {
				res.rows = append(res.rows, out.Cells(i))
			}
			return res, nil
		}
	}
	runDAG := func(c *Coordinator) func(string) (wdResult, error) {
		return func(sql string) (res wdResult, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("PANIC: %v", r)
				}
			}()
			out, qerr := c.ExecuteSQL(ctx, sql)
			if qerr != nil {
				return wdResult{}, qerr
			}
			if out.Error != "" {
				return wdResult{}, fmt.Errorf("%s", out.Error)
			}
			res.schema = append([]parquet.Column(nil), out.OutputSchema()...)
			st := out.Stream()
			if st == nil {
				rows, rerr := out.Rows()
				if rerr != nil {
					return wdResult{}, rerr
				}
				for _, r := range rows {
					cells := make([]any, len(out.Columns))
					for j, c := range out.Columns {
						cells[j] = r[c]
					}
					res.rows = append(res.rows, cells)
				}
				return res, nil
			}
			defer st.Close()
			for {
				bb, berr := st.Next(ctx)
				if berr != nil {
					return wdResult{}, berr
				}
				if bb == nil {
					break
				}
				res.rows = append(res.rows, bb.ToRowValues()...)
			}
			return res, nil
		}
	}
	return []wdArm{
		{"single", runSingle(single)},
		{"spilled512k", runSingle(spilled)},
		{"dag", runDAG(stand(nil))},
		{"dag-shuffled", runDAG(stand(nil, func(c *Config) { c.BroadcastBytesOverride = 1 }))},
		{"dag-morsel4", runDAG(stand(func(w *worker.Config) { w.MorselWorkers = 4 }))},
	}
}

// wdPGType is PostgreSQL's name for a declared column type.
func wdPGType(c parquet.Column) string {
	switch c.Type {
	case parquet.TypeInt32:
		return "integer"
	case parquet.TypeInt64:
		return "bigint"
	case parquet.TypeFloat32:
		return "real"
	case parquet.TypeFloat64:
		return "double precision"
	case parquet.TypeDecimal:
		return "numeric"
	case parquet.TypeString:
		return "text"
	case parquet.TypeBool:
		return "boolean"
	case parquet.TypeDate:
		return "date"
	case parquet.TypeTimestamp:
		return "timestamp without time zone"
	case parquet.TypeArray:
		if c.ElementType != nil {
			return wdPGType(*c.ElementType) + "[]"
		}
		return "array"
	}
	return c.Type.String()
}

// wdFmt renders one value as psql prints it, read through its declared type.
func wdFmt(c parquet.Column, v any) string {
	if v == nil {
		return "NULL"
	}
	switch c.Type {
	case parquet.TypeTimestamp:
		if ms, ok := v.(int64); ok {
			return time.UnixMilli(ms).UTC().Format("2006-01-02 15:04:05")
		}
	case parquet.TypeFloat64, parquet.TypeFloat32:
		switch f := v.(type) {
		case float64:
			return fmt.Sprintf("%g", f)
		case float32:
			return fmt.Sprintf("%g", f)
		}
	case parquet.TypeArray:
		if xs, ok := v.([]any); ok {
			parts := make([]string, len(xs))
			el := parquet.Column{}
			if c.ElementType != nil {
				el = *c.ElementType
			}
			for i, x := range xs {
				parts[i] = wdFmt(el, x)
			}
			return "{" + strings.Join(parts, ",") + "}"
		}
	}
	return fmt.Sprint(v)
}

// wdRender is the declared type of every column after id, and the sorted
// row set.
func wdRender(res wdResult) string {
	var types []string
	for i, c := range res.schema {
		if i == 0 && len(res.schema) > 1 && c.Name == "id" {
			continue
		}
		types = append(types, wdPGType(c))
	}
	rows := make([]string, 0, len(res.rows))
	for _, r := range res.rows {
		cells := make([]string, len(r))
		for j, v := range r {
			var c parquet.Column
			if j < len(res.schema) {
				c = res.schema[j]
			}
			cells[j] = wdFmt(c, v)
		}
		rows = append(rows, strings.Join(cells, ","))
	}
	sort.Strings(rows)
	return fmt.Sprintf("type=%s rows=%d %s", strings.Join(types, ";"), len(rows), strings.Join(rows, " | "))
}

// wdNormalize strips trailing fractional zeros from the cells of a numeric
// answer: PostgreSQL's unconstrained numeric keeps each value's own scale,
// this engine one scale per column — the catalogued value divergence
// numeric-decimal r18 (docs/adr/0012-divergences/numeric-decimal.md), which
// is not this table's question; the digits and the type are.
var (
	wdTrailingZeros = regexp.MustCompile(`(\.\d*?)0+(,| |$)`)
	wdBareDot       = regexp.MustCompile(`\.(,| |$)`)
)

func wdNormalize(rendered string) string {
	if !strings.Contains(strings.SplitN(rendered, " ", 2)[0], "numeric") {
		return rendered
	}
	return wdBareDot.ReplaceAllString(wdTrailingZeros.ReplaceAllString(rendered, "$1$2"), "$1")
}

// wdKept is a cell whose answer here is not PostgreSQL's, for a reason
// outside #1435 that the notes record (want = this engine's answer, or
// "ERR <SQLSTATE> <substring>").
type wdKept struct{ want, why string }

func wdKeptCells() map[string]wdKept {
	kept := map[string]wdKept{}
	for _, c := range wdCells() {
		p := strings.Split(c.name, "/")
		if p[0] != "type" {
			continue
		}
		v, d := p[2], p[3]
		switch {
		// The exponent literal 1e300 and the 22-digit 14.0000000000000000001
		// are declared double precision here (the decimal-literal carrier,
		// arc NX's seam), numeric in PostgreSQL: the common type follows the
		// literal's declaration, as COALESCE's does at base.
		case (d == "wide" || d == "dbl") && (v == "int" || v == "bigint" || v == "numeric"):
			kept[c.name] = wdKept{"", "control: a wide or exponent numeric literal is declared double precision (NX)"}
		// A TEXT or BOOLEAN / DATE / TIMESTAMP value beside one of them: the
		// refusal agrees, the message names the literal's declaration.
		// `1 + 1` is declared bigint here, integer in PostgreSQL — the
		// constant fold's width, identical in COALESCE at base.
		case d == "expr" && (v == "int"):
			kept[c.name] = wdKept{"", "filing candidate: `1 + 1` is declared bigint (PostgreSQL integer), as in COALESCE"}
		// A quoted literal default of an ARRAY value: PostgreSQL 22P02
		// malformed array literal; here no CAST to an array is spelled and
		// the write refuses loudly.
		case v == "array" && (d == "text" || d == "qnum"):
			kept[c.name] = wdKept{"ERR - cannot store string into ARRAY vector", "loud: no CAST to an array type is spelled (window_lag_default.go castSpelling)"}
		}
	}
	kept["gap/default_every_row"] = wdKept{"ERR 22012 division by zero",
		"documented gap r21: the default is materialized as a column, evaluated on every row"}
	kept["gap/no_rows_text"] = wdKept{"", "kept superset r22: a constant default is coerced when a row is read"}
	// `i + 0` over an integer is declared bigint here, integer in PostgreSQL —
	// the integer arithmetic's own width, identical in `SELECT i + 0` and
	// `COALESCE(i + 0, 7)` at base (filing candidate 2's class).
	for _, n := range []string{"cv/lag/iplus/int", "cv/lag/iplus/null", "cv/lead/iplus/int"} {
		kept[n] = wdKept{"", "filing candidate: `i + 0` is declared bigint (PostgreSQL integer), as in a projection"}
	}
	kept["cv/lag/iplus/text"] = wdKept{"ERR 22P02 invalid input syntax for type bigint",
		"filing candidate: `i + 0` is declared bigint, so the quoted default is read as one"}
	// SUM over `v + 0` where v is a derived aggregate's or DISTINCT's column
	// declares double precision here, numeric in PostgreSQL; the values agree
	// and 978cd0e5 declared the same (a window accumulator's typing, outside
	// the default's seam).
	for _, n := range []string{"nk/aggregate/sum", "nk/distinct/sum"} {
		kept[n] = wdKept{"", "filing candidate: SUM(v + 0) OVER over a derived aggregate / DISTINCT column declares double precision"}
	}
	for name, rows := range wdKeptRows {
		k := kept[name]
		k.want = rows
		kept[name] = k
	}
	return kept
}

// LAG / LEAD'S RESULT IS THE COMMON TYPE OF THE VALUE AND THE DEFAULT, ON
// EVERY ARM (#1435). The generated coverage table (wdCells) against
// PostgreSQL 17.11's declared type and full sorted rows for every cell. At
// v0.25.3 the default was carried as a float64 or as its SQL text and written
// into a vector of the VALUE's type: `LAG(b, 1, 2.5)` over a bigint answered
// 2 where PostgreSQL answers 2.5 (numeric), a text / column / CAST / `1 + 1`
// default failed the write, and a DATE value's default answered NULL. A
// COMPUTED value (cv/) is typed from its own tree, and the widened value and
// default read a derived column through every node kind (nk/) on the DAG arms.
func TestArcWDWindowDefaultEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the LAG / LEAD default table")
	}
	answers := wdPGAnswers(t)
	kept := wdKeptCells()
	cells := wdCells()
	for _, c := range cells {
		if _, ok := answers[c.name]; !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", c.name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	t.Cleanup(cancel)
	arms := wdArms(t, ctx)
	agreeing, keptSeen := 0, 0
	for _, tc := range cells {
		want := answers[tc.name]
		why := "PostgreSQL 17.11"
		if k, ok := kept[tc.name]; ok {
			want, why = k.want, k.why
			keptSeen++
		} else {
			agreeing++
		}
		t.Run(tc.name, func(t *testing.T) {
			type result struct {
				res wdResult
				err error
			}
			results := make([]result, len(arms))
			var wg sync.WaitGroup
			for i, arm := range arms {
				wg.Add(1)
				go func() {
					defer wg.Done()
					results[i].res, results[i].err = arm.run(tc.sql)
				}()
			}
			wg.Wait()
			for i, arm := range arms {
				res, err := results[i].res, results[i].err
				if rest, ok := strings.CutPrefix(want, "ERR "); ok {
					state, msg, _ := strings.Cut(rest, " ")
					if state == "-" {
						state = ""
					}
					if err == nil {
						t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s %q (%s)", tc.sql, arm.name, wdRender(res), state, msg, why)
					} else if st := sqlerr.StateOf(err); st != state || !waErrorAgrees(err.Error(), msg) {
						t.Errorf("%s\n  arm  %s\n  got  %s %v\n  want %s %q (%s)", tc.sql, arm.name, st, err, state, msg, why)
					}
					continue
				}
				if err != nil {
					t.Errorf("%s\n  arm  %s\n  refused: %v\n  want %s (%s)", tc.sql, arm.name, err, want, why)
					continue
				}
				if got := wdRender(res); wdNormalize(got) != wdNormalize(want) {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (%s)", tc.sql, arm.name, got, want, why)
				}
			}
		})
	}
	if len(cells) < 800 || agreeing < 760 || keptSeen != len(kept) {
		t.Fatalf("%d cells, %d agreeing with PostgreSQL, %d kept of %d: the table must discriminate",
			len(cells), agreeing, keptSeen, len(kept))
	}
}

// wdKeptRows is this engine's answer for each ANSWERED kept cell, measured
// on all five arms at the arc's tip.
var wdKeptRows = map[string]string{
	"type/lag/bigint/dbl":    "type=double precision rows=6 1,1e+300 | 2,1e+300 | 3,1e+300 | 4,1e+300 | 5,1e+300 | 6,1e+300",
	"type/lag/bigint/wide":   "type=double precision rows=6 1,14 | 2,14 | 3,14 | 4,14 | 5,14 | 6,14",
	"type/lag/int/dbl":       "type=double precision rows=6 1,1e+300 | 2,1e+300 | 3,1e+300 | 4,1e+300 | 5,1e+300 | 6,1e+300",
	"type/lag/int/expr":      "type=bigint rows=6 1,2 | 2,2 | 3,2 | 4,2 | 5,2 | 6,2",
	"type/lag/int/wide":      "type=double precision rows=6 1,14 | 2,14 | 3,14 | 4,14 | 5,14 | 6,14",
	"type/lag/numeric/dbl":   "type=double precision rows=6 1,1e+300 | 2,1e+300 | 3,1e+300 | 4,1e+300 | 5,1e+300 | 6,1e+300",
	"type/lag/numeric/wide":  "type=double precision rows=6 1,14 | 2,14 | 3,14 | 4,14 | 5,14 | 6,14",
	"type/lead/bigint/dbl":   "type=double precision rows=6 1,20 | 2,NULL | 3,1e+300 | 4,50 | 5,1e+300 | 6,1e+300",
	"type/lead/bigint/wide":  "type=double precision rows=6 1,20 | 2,NULL | 3,14 | 4,50 | 5,14 | 6,14",
	"type/lead/int/dbl":      "type=double precision rows=6 1,20 | 2,NULL | 3,1e+300 | 4,50 | 5,1e+300 | 6,1e+300",
	"type/lead/int/expr":     "type=bigint rows=6 1,20 | 2,NULL | 3,2 | 4,50 | 5,2 | 6,2",
	"type/lead/int/wide":     "type=double precision rows=6 1,20 | 2,NULL | 3,14 | 4,50 | 5,14 | 6,14",
	"type/lead/numeric/dbl":  "type=double precision rows=6 1,2.25 | 2,NULL | 3,1e+300 | 4,5.25 | 5,1e+300 | 6,1e+300",
	"type/lead/numeric/wide": "type=double precision rows=6 1,2.25 | 2,NULL | 3,14 | 4,5.25 | 5,14 | 6,14",
	"gap/no_rows_text":       "type=bigint rows=0 ",
	"cv/lag/iplus/int":       "type=bigint rows=6 1,7 | 2,10 | 3,20 | 4,NULL | 5,40 | 6,50",
	"cv/lag/iplus/null":      "type=bigint rows=6 1,NULL | 2,10 | 3,20 | 4,NULL | 5,40 | 6,50",
	"cv/lead/iplus/int":      "type=bigint rows=6 1,20 | 2,NULL | 3,7 | 4,50 | 5,7 | 6,7",
	"nk/aggregate/sum":       "type=double precision rows=3 1,40 | 2,140 | 3,260",
	"nk/distinct/sum":        "type=double precision rows=6 1,20 | 2,60 | 3,60 | 4,140 | 5,240 | 6,360",
}
