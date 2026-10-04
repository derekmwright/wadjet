// SPDX-License-Identifier: MIT

package expr

import (
	"fmt"
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// tiBatch: row 0 infinity, row 1 -infinity, row 2 2024-01-15 10:30:00 /
// 2024-01-15, in a TIMESTAMP column ts and a DATE column d — the carriers'
// extremes, as stored.
func tiBatch() *batch.RecordBatch {
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "ts", Type: parquet.TypeTimestamp},
		{Name: "d", Type: parquet.TypeDate},
	}, 3)
	b.Columns[0].SetValue(0, int64(math.MaxInt64))
	b.Columns[0].SetValue(1, int64(math.MinInt64))
	b.Columns[0].SetValue(2, "2024-01-15 10:30:00")
	b.Columns[1].SetValue(0, int32(math.MaxInt32))
	b.Columns[1].SetValue(1, int32(math.MinInt32))
	b.Columns[1].SetValue(2, "2024-01-15")
	return b
}

// tiEval is one row's answer: its box rendered under the expression's
// produced temporal type, or the SQLSTATE a fatalEval raised.
func tiEval(e Expr, b *batch.RecordBatch, row int) (out string) {
	defer func() {
		if r := recover(); r != nil {
			f, ok := r.(fatalEval)
			if !ok {
				panic(r)
			}
			out = "ERR " + sqlerr.StateOf(f.err)
		}
	}()
	v := e.Eval(b, row)
	if s, ok := renderTemporalBox(e, b, v); ok {
		return s
	}
	if v == nil {
		return "NULL"
	}
	return fmt.Sprint(v)
}

// TestArcTIInfiniteValueOwners: each operation that owns a computation on a
// DATE or TIMESTAMP answers an infinite operand as PostgreSQL 17.11 does
// (measured: ti_author/enum/e.pg.tsv), or refuses it 22008 — never the
// instant the carrier's extreme integer would name. Rows: infinity,
// -infinity, a finite control.
func TestArcTIInfiniteValueOwners(t *testing.T) {
	b := tiBatch()
	for _, c := range []struct {
		sql  string
		want [3]string
	}{
		// casts keep the value (castTemporal)
		{"SELECT CAST(ts AS DATE)", [3]string{"infinity", "-infinity", "2024-01-15"}},
		{"SELECT CAST(d AS TIMESTAMP)", [3]string{"infinity", "-infinity", "2024-01-15 00:00:00"}},
		{"SELECT CAST(ts AS TEXT)", [3]string{"infinity", "-infinity", "2024-01-15 10:30:00"}},
		{"SELECT CAST(ts AS BIGINT)", [3]string{"ERR 22008", "ERR 22008", "1705314600000"}},
		{"SELECT CAST(d AS DOUBLE PRECISION)", [3]string{"ERR 22008", "ERR 22008", "19737"}},
		// ± INTERVAL keeps the value (intervalShift)
		{"SELECT ts + INTERVAL '1 day'", [3]string{"infinity", "-infinity", "2024-01-16 10:30:00"}},
		{"SELECT d - INTERVAL '1 day'", [3]string{"infinity", "-infinity", "2024-01-14 00:00:00"}},
		{"SELECT INTERVAL '1 day' + ts", [3]string{"infinity", "-infinity", "2024-01-16 10:30:00"}},
		{"SELECT ts + '1 day'", [3]string{"infinity", "-infinity", "2024-01-16 10:30:00"}},
		// date ± integer keeps the value; a difference is 22008 (dateArith)
		{"SELECT d + 1", [3]string{"infinity", "-infinity", "2024-01-16"}},
		{"SELECT 1 + d", [3]string{"infinity", "-infinity", "2024-01-16"}},
		{"SELECT d - 1", [3]string{"infinity", "-infinity", "2024-01-14"}},
		{"SELECT d - d", [3]string{"ERR 22008", "ERR 22008", "0"}},
		{"SELECT d - DATE '2024-01-01'", [3]string{"ERR 22008", "ERR 22008", "14"}},
		{"SELECT d - '2024-01-01'", [3]string{"ERR 22008", "ERR 22008", "14"}},
		{"SELECT ts - TIMESTAMP '2024-01-01 00:00:00'", [3]string{"ERR 22008", "ERR 22008", "1.2474e+09"}},
		{"SELECT ts - '2024-01-01 00:00:00'", [3]string{"ERR 22008", "ERR 22008", "1.2474e+09"}},
		// EXTRACT: year / epoch ±Infinity, every other field NULL
		{"SELECT extract(year FROM ts)", [3]string{"+Inf", "-Inf", "2024"}},
		{"SELECT extract(epoch FROM d)", [3]string{"+Inf", "-Inf", "1.7052768e+09"}},
		{"SELECT extract(month FROM ts)", [3]string{"NULL", "NULL", "1"}},
		{"SELECT extract(dow FROM d)", [3]string{"NULL", "NULL", "1"}},
		{"SELECT date_part('year', ts)", [3]string{"+Inf", "-Inf", "2024"}},
		{"SELECT date_part('hour', ts)", [3]string{"NULL", "NULL", "10"}},
		// date_trunc / timezone / time_bucket keep the value
		{"SELECT date_trunc('month', ts)", [3]string{"infinity", "-infinity", "2024-01-01 00:00:00"}},
		{"SELECT date_trunc('year', d)", [3]string{"infinity", "-infinity", "2024-01-01 00:00:00"}},
		{"SELECT timezone('UTC', ts)", [3]string{"infinity", "-infinity", "2024-01-15 10:30:00"}},
		{"SELECT time_bucket(INTERVAL '1 day', ts)", [3]string{"infinity", "-infinity", "2024-01-15 00:00:00"}},
		{"SELECT time_bucket(INTERVAL '1 day', TIMESTAMP '2024-01-01 00:00:00', ts)", [3]string{"ERR 22008", "ERR 22008", "2023-12-31 10:30:00"}},
		// engine-only temporal functions refuse an infinite argument
		{"SELECT date_add(ts, 1)", [3]string{"ERR 22008", "ERR 22008", "2024-01-16 10:30:00"}},
		{"SELECT last_day_of_month(d)", [3]string{"ERR 22008", "ERR 22008", "2024-01-31"}},
		{"SELECT to_unixtime(ts)", [3]string{"ERR 22008", "ERR 22008", "1.7053146e+09"}},
	} {
		e := compileSelectCol(t, c.sql)
		for row := 0; row < 3; row++ {
			if got := tiEval(e, b, row); got != c.want[row] {
				t.Errorf("%s row %d: got %s, want %s", c.sql, row, got, c.want[row])
			}
		}
	}
}

// TestArcTIConstructionNeverReachesAnInfiniteValue: arithmetic whose finite
// result would land on a carrier extreme is 22008, as PostgreSQL's date_pli /
// date_mii refuse it — an infinite value is read from text, never computed.
func TestArcTIConstructionNeverReachesAnInfiniteValue(t *testing.T) {
	b := tiBatch()
	for _, sql := range []string{
		"SELECT DATE '1970-01-02' + 2147483646",
		"SELECT DATE '1969-12-31' - 2147483647",
		"SELECT DATE '5874897-12-31' + 2440742",
		"SELECT CAST(2147483647 AS DATE)",
		"SELECT TIMESTAMP '294276-12-31 23:59:59' + INTERVAL '1 day'",
	} {
		if got := tiEval(compileSelectCol(t, sql), b, 2); got != "ERR 22008" {
			t.Errorf("%s: got %s, want ERR 22008", sql, got)
		}
	}
}
