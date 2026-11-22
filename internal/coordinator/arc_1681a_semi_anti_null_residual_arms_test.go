// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// The 1681a fixture (epic #1681): an outer relation nr_p and an inner nr_s
// with the SAME column names (so every residual resolves by qualifier), one
// value column per type family the semi/anti residual evaluator has a kernel
// for (and the families that fall through to its generic one), and rows that
// place a NULL in the outer operand, the inner operand, both, the equality
// key on either side, plus controls with no NULL. Each value column derives
// from one small integer v per row, so every family orders the rows alike;
// v=12 against an inner 9 probes an ordering that a TEXT rendering of the
// value would get backwards. nr_n is an inner side whose v is NULL on every
// row.
type nrRow struct {
	key        int // rid / sid
	id, id2, w *int
	v          *int
	nan        bool // the float columns are NaN, the others read v
}

func nrI(n int) *int { return &n }

var nrOuter = []nrRow{
	{key: 1, id: nrI(1), id2: nrI(1), w: nrI(10), v: nrI(5)},
	{key: 2, id: nrI(2), id2: nrI(2), w: nrI(10), v: nil},
	{key: 3, id: nrI(3), id2: nrI(3), w: nrI(10), v: nrI(5)},
	{key: 4, id: nrI(4), id2: nrI(4), w: nrI(10), v: nil},
	{key: 5, id: nrI(5), id2: nrI(5), w: nrI(10), v: nrI(5)},
	{key: 6, id: nrI(6), id2: nrI(6), w: nil, v: nrI(5)},
	{key: 7, id: nrI(7), id2: nrI(7), w: nrI(10), v: nrI(5)},
	{key: 8, id: nrI(8), id2: nrI(8), w: nrI(10), v: nrI(5)},
	{key: 9, id: nrI(9), id2: nrI(9), w: nrI(10), v: nil},
	{key: 10, id: nrI(10), id2: nrI(10), w: nrI(10), v: nrI(12)},
	{key: 11, id: nil, id2: nil, w: nrI(10), v: nrI(5)},
	{key: 12, id: nrI(12), id2: nrI(12), w: nrI(10), v: nrI(5), nan: true},
	{key: 13, id: nrI(13), id2: nrI(13), w: nrI(10), v: nrI(5)},
}

var nrInner = []nrRow{
	{key: 1, id: nrI(1), id2: nrI(1), w: nrI(1), v: nrI(3)},
	{key: 2, id: nrI(1), id2: nrI(99), w: nrI(1), v: nrI(7)},
	{key: 3, id: nrI(2), id2: nrI(2), w: nrI(1), v: nrI(9)},
	{key: 4, id: nrI(3), id2: nrI(3), w: nrI(1), v: nil},
	{key: 5, id: nrI(4), id2: nrI(4), w: nrI(1), v: nil},
	{key: 6, id: nrI(5), id2: nrI(5), w: nrI(20), v: nrI(9)},
	{key: 7, id: nrI(5), id2: nrI(5), w: nrI(1), v: nil},
	{key: 8, id: nrI(6), id2: nrI(6), w: nrI(1), v: nrI(2)},
	{key: 9, id: nrI(8), id2: nrI(8), w: nrI(1), v: nrI(5)},
	{key: 10, id: nrI(9), id2: nrI(9), w: nrI(1), v: nrI(3)},
	{key: 11, id: nrI(9), id2: nrI(9), w: nil, v: nil},
	{key: 12, id: nrI(10), id2: nrI(10), w: nrI(1), v: nrI(9)},
	{key: 13, id: nil, id2: nil, w: nrI(1), v: nrI(9)},
	{key: 14, id: nrI(12), id2: nrI(12), w: nrI(1), v: nrI(3)},
	{key: 15, id: nrI(13), id2: nrI(13), w: nrI(1), v: nrI(3), nan: true},
	{key: 16, id: nrI(6), id2: nrI(6), w: nil, v: nrI(9)},
}

// nrType is one value-column family: its name in both engines, its column,
// and v rendered as an engine value and as a PostgreSQL literal.
type nrType struct {
	col    string
	pg     string
	pq     parquet.Column
	val    func(v int, nan bool) any
	pgLit  func(v int, nan bool) string
	float  bool
	arith  bool // `s.c + 1 > p.c` is well-typed
	coal0  string
	before string // a literal below every v, for an inner-only residual
}

func nrTypes() []nrType {
	col := func(n string, t parquet.TypeID) parquet.Column {
		return parquet.Column{Name: n, Type: t, Nullable: true}
	}
	dec := col("c_dec", parquet.TypeDecimal)
	dec.Precision, dec.Scale = 18, 4
	dec2 := col("c_dec2", parquet.TypeDecimal)
	dec2.Precision, dec2.Scale = 10, 2
	decbig := col("c_decbig", parquet.TypeDecimal)
	decbig.Precision, decbig.Scale = 38, 10
	return []nrType{
		{col: "c_i32", pg: "int4", pq: col("c_i32", parquet.TypeInt32), arith: true, coal0: "0", before: "4",
			val: func(v int, _ bool) any { return int32(v) }, pgLit: func(v int, _ bool) string { return fmt.Sprint(v) }},
		{col: "c_i64", pg: "int8", pq: col("c_i64", parquet.TypeInt64), arith: true, coal0: "0", before: "4000",
			val: func(v int, _ bool) any { return int64(v) * 1000 }, pgLit: func(v int, _ bool) string { return fmt.Sprint(v * 1000) }},
		{col: "c_f64", pg: "float8", pq: col("c_f64", parquet.TypeFloat64), float: true, arith: true, coal0: "0", before: "4",
			val: func(v int, nan bool) any {
				if nan {
					return math.NaN()
				}
				return float64(v) + 0.5
			},
			pgLit: func(v int, nan bool) string {
				if nan {
					return "'NaN'"
				}
				return fmt.Sprintf("%d.5", v)
			}},
		{col: "c_f32", pg: "float4", pq: col("c_f32", parquet.TypeFloat32), float: true, arith: true, coal0: "0", before: "4",
			val: func(v int, nan bool) any {
				if nan {
					return float32(math.NaN())
				}
				return float32(v) + 0.25
			},
			pgLit: func(v int, nan bool) string {
				if nan {
					return "'NaN'"
				}
				return fmt.Sprintf("%d.25", v)
			}},
		{col: "c_dec", pg: "numeric(18,4)", pq: dec, arith: true, coal0: "0", before: "4",
			val: func(v int, _ bool) any { return float64(v) + 0.25 }, pgLit: func(v int, _ bool) string { return fmt.Sprintf("%d.25", v) }},
		{col: "c_txt", pg: "text", pq: col("c_txt", parquet.TypeString), coal0: "''", before: "'d'",
			val: func(v int, _ bool) any { return string(rune('a' + v)) }, pgLit: func(v int, _ bool) string { return "'" + string(rune('a'+v)) + "'" }},
		{col: "c_dt", pg: "date", pq: col("c_dt", parquet.TypeDate), before: "DATE '2024-01-04'",
			val: func(v int, _ bool) any { return fmt.Sprintf("2024-01-%02d", v) }, pgLit: func(v int, _ bool) string { return fmt.Sprintf("'2024-01-%02d'", v) }},
		{col: "c_ts", pg: "timestamp", pq: col("c_ts", parquet.TypeTimestamp), before: "TIMESTAMP '2024-01-01 04:00:00'",
			val:   func(v int, _ bool) any { return time.Date(2024, 1, 1, v, 0, 0, 0, time.UTC) },
			pgLit: func(v int, _ bool) string { return fmt.Sprintf("'2024-01-01 %02d:00:00'", v) }},
		{col: "c_b", pg: "bool", pq: col("c_b", parquet.TypeBool), before: "false",
			val: func(v int, _ bool) any { return v >= 6 }, pgLit: func(v int, _ bool) string { return fmt.Sprint(v >= 6) }},
		{col: "c_by", pg: "bytea", pq: col("c_by", parquet.TypeBytes), before: "'\\x04'::bytea",
			val: func(v int, _ bool) any { return []byte{byte(v)} }, pgLit: func(v int, _ bool) string { return fmt.Sprintf("decode('%02x','hex')", v) }},
		{col: "c_ip", pg: "inet", pq: col("c_ip", parquet.TypeIPv4), before: "'10.0.0.4'::inet",
			val: func(v int, _ bool) any { return fmt.Sprintf("10.0.0.%d", v) }, pgLit: func(v int, _ bool) string { return fmt.Sprintf("'10.0.0.%d'", v) }},
		{col: "c_uu", pg: "uuid", pq: col("c_uu", parquet.TypeUUID), before: "'00000000-0000-4000-8000-000000000004'::uuid",
			val:   func(v int, _ bool) any { return fmt.Sprintf("00000000-0000-4000-8000-%012x", v) },
			pgLit: func(v int, _ bool) string { return fmt.Sprintf("'00000000-0000-4000-8000-%012x'", v) }},
		// 1681b: the families below have no kernel of their own, or probe
		// what the typed slot gets wrong — negative values, text ordering
		// across lengths and case, a second numeric scale, a numeric beyond
		// 64 bits, IPv6, CIDR, MAC and interval.
		{col: "c_i64b", pg: "int8", pq: col("c_i64b", parquet.TypeInt64), arith: true, coal0: "0", before: "4",
			val: func(v int, _ bool) any { return int64(v) }, pgLit: func(v int, _ bool) string { return fmt.Sprint(v) }},
		{col: "c_neg", pg: "int8", pq: col("c_neg", parquet.TypeInt64), arith: true, coal0: "0", before: "-10",
			val: func(v int, _ bool) any { return int64((v - 6) * 7) }, pgLit: func(v int, _ bool) string { return fmt.Sprint((v - 6) * 7) }},
		{col: "c_dec2", pg: "numeric(10,2)", pq: dec2, arith: true, coal0: "0", before: "4",
			val: func(v int, _ bool) any { return fmt.Sprintf("%d.50", v) }, pgLit: func(v int, _ bool) string { return fmt.Sprintf("%d.50", v) }},
		{col: "c_decbig", pg: "numeric(38,10)", pq: decbig, arith: true, coal0: "0", before: "0",
			val: func(v int, _ bool) any { return nrDecBig(v) }, pgLit: func(v int, _ bool) string { return nrDecBig(v) }},
		{col: "c_txt2", pg: "text", pq: col("c_txt2", parquet.TypeString), coal0: "''", before: "'A'",
			val: func(v int, _ bool) any { return nrTxt2[v] }, pgLit: func(v int, _ bool) string { return "'" + nrTxt2[v] + "'" }},
		{col: "c_by2", pg: "bytea", pq: col("c_by2", parquet.TypeBytes), before: "decode('04','hex')",
			val:   func(v int, _ bool) any { return nrBy2(v) },
			pgLit: func(v int, _ bool) string { return fmt.Sprintf("decode('%x','hex')", nrBy2(v)) }},
		{col: "c_ip6", pg: "inet", pq: col("c_ip6", parquet.TypeIPv6), before: "'2001:db8::4'::inet",
			val: func(v int, _ bool) any { return fmt.Sprintf("2001:db8::%d", v) }, pgLit: func(v int, _ bool) string { return fmt.Sprintf("'2001:db8::%d'", v) }},
		{col: "c_cidr", pg: "cidr", pq: col("c_cidr", parquet.TypeCIDR), before: "'10.4.0.0/16'::cidr",
			val: func(v int, _ bool) any { return nrCIDR(v) }, pgLit: func(v int, _ bool) string { return "'" + nrCIDR(v) + "'" }},
		{col: "c_mac", pg: "macaddr", pq: col("c_mac", parquet.TypeMAC), before: "'00:00:00:00:00:04'::macaddr",
			val: func(v int, _ bool) any { return fmt.Sprintf("00:00:00:00:00:%02d", v) }, pgLit: func(v int, _ bool) string { return fmt.Sprintf("'00:00:00:00:00:%02d'", v) }},
		{col: "c_iv", pg: "interval", pq: col("c_iv", parquet.TypeDuration), before: "INTERVAL '4 hours'",
			val: func(v int, _ bool) any { return int64(v) * int64(time.Hour) }, pgLit: func(v int, _ bool) string { return fmt.Sprintf("'%d hours'", v) }},
	}
}

// nr1681cCols are fixture columns for the `<>` pairs of two types (line
// 1681c), outside nrTypes so the per-family cells do not grow: a float and a
// numeric holding v exactly (3 against 3.0 is equal, 3 against 3.25 is not),
// a timestamp at midnight of day v (equal to the date of day v), and int8
// values 2^60+v that float8 cannot tell apart, beside a float8 holding 2^60
// (PostgreSQL compares int8 with float8 in float8, so every one is equal).
func nr1681cCols() []nrType {
	col := func(n string, t parquet.TypeID) parquet.Column {
		return parquet.Column{Name: n, Type: t, Nullable: true}
	}
	deci := col("c_deci", parquet.TypeDecimal)
	deci.Precision, deci.Scale = 18, 4
	dec0 := col("c_dec0", parquet.TypeDecimal)
	dec0.Precision, dec0.Scale = 18, 0
	const two60 = int64(1) << 60
	return []nrType{
		{col: "c_f64i", pg: "float8", pq: col("c_f64i", parquet.TypeFloat64), float: true,
			val: func(v int, nan bool) any {
				if nan {
					return math.NaN()
				}
				return float64(v)
			},
			pgLit: func(v int, nan bool) string {
				if nan {
					return "'NaN'"
				}
				return fmt.Sprint(v)
			}},
		{col: "c_f32i", pg: "float4", pq: col("c_f32i", parquet.TypeFloat32), float: true,
			val: func(v int, nan bool) any {
				if nan {
					return float32(math.NaN())
				}
				return float32(v)
			},
			pgLit: func(v int, nan bool) string {
				if nan {
					return "'NaN'"
				}
				return fmt.Sprint(v)
			}},
		{col: "c_deci", pg: "numeric(18,4)", pq: deci,
			val: func(v int, _ bool) any { return fmt.Sprintf("%d.0000", v) }, pgLit: func(v int, _ bool) string { return fmt.Sprintf("%d.0000", v) }},
		{col: "c_dec0", pg: "numeric(18,0)", pq: dec0,
			val: func(v int, _ bool) any { return fmt.Sprint(v) }, pgLit: func(v int, _ bool) string { return fmt.Sprint(v) }},
		{col: "c_tsd", pg: "timestamp", pq: col("c_tsd", parquet.TypeTimestamp),
			val:   func(v int, _ bool) any { return time.Date(2024, 1, v, 0, 0, 0, 0, time.UTC) },
			pgLit: func(v int, _ bool) string { return fmt.Sprintf("'2024-01-%02d 00:00:00'", v) }},
		{col: "c_big", pg: "int8", pq: col("c_big", parquet.TypeInt64),
			val: func(v int, _ bool) any { return two60 + int64(v) }, pgLit: func(v int, _ bool) string { return fmt.Sprint(two60 + int64(v)) }},
		{col: "c_bigf", pg: "float8", pq: col("c_bigf", parquet.TypeFloat64),
			val: func(int, bool) any { return float64(two60) }, pgLit: func(int, bool) string { return fmt.Sprint(two60) }},
	}
}

// nrFixtureCols is every value column of nr_p / nr_s, in fixture order.
func nrFixtureCols() []nrType { return append(nrTypes(), nr1681cCols()...) }

// nrTxt2 varies length and case: in the C collation 'A' < 'B' < 'a' < 'aB' < 'ab' < 'b'.
var nrTxt2 = map[int]string{2: "B", 3: "ab", 5: "a", 6: "A", 7: "Ab", 9: "b", 12: "aB"}

// nrBy2 is v repeated (v%4)+1 times: lengths differ between rows.
func nrBy2(v int) []byte {
	b := make([]byte, v%4+1)
	for i := range b {
		b[i] = byte(v)
	}
	return b
}

// nrCIDR is 10.v.0.0 with a /16 or /24 mask.
func nrCIDR(v int) string {
	mask := 24
	if v%2 == 0 {
		mask = 16
	}
	return fmt.Sprintf("10.%d.0.0/%d", v, mask)
}

// nrDecBig is (v-6)·10^20 + v·10^-10: 31 significant digits, past int64.
func nrDecBig(v int) string {
	n := v - 6
	sign := ""
	if n < 0 {
		sign, n = "-", -n
	}
	return fmt.Sprintf("%s%d00000000000000000000.%010d", sign, n, v)
}

func nrIntOrNil(p *int) any {
	if p == nil {
		return nil
	}
	return int64(*p)
}

func nrPGInt(p *int) string {
	if p == nil {
		return "NULL"
	}
	return fmt.Sprint(*p)
}

// nrTables is the fixture for the engine; nrPGFixture is the same in
// PostgreSQL DDL.
func nrTables() []tmdTable {
	i64 := func(n string) parquet.Column { return parquet.Column{Name: n, Type: parquet.TypeInt64, Nullable: true} }
	mk := func(name, keyCol string, rows []nrRow) tmdTable {
		cols := []parquet.Column{i64(keyCol), i64("id"), i64("id2"), i64("w")}
		for _, ty := range nrFixtureCols() {
			cols = append(cols, ty.pq)
		}
		t := tmdTable{name: name, schema: parquet.Schema{Columns: cols}}
		for _, r := range rows {
			m := map[string]any{keyCol: int64(r.key), "id": nrIntOrNil(r.id), "id2": nrIntOrNil(r.id2), "w": nrIntOrNil(r.w)}
			for _, ty := range nrFixtureCols() {
				if r.v == nil {
					m[ty.col] = nil
					continue
				}
				m[ty.col] = ty.val(*r.v, r.nan && ty.float)
			}
			t.rows = append(t.rows, m)
		}
		return t
	}
	n := tmdTable{name: "nr_n", schema: parquet.Schema{Columns: []parquet.Column{i64("sid"), i64("id"), i64("c_i64")}}}
	for i := 1; i <= 13; i++ {
		n.rows = append(n.rows, map[string]any{"sid": int64(i), "id": int64(i), "c_i64": nil})
	}
	return []tmdTable{mk("nr_p", "rid", nrOuter), mk("nr_s", "sid", nrInner), n}
}

func nrPGFixture() string {
	var b strings.Builder
	b.WriteString("DROP TABLE IF EXISTS nr_p; DROP TABLE IF EXISTS nr_s; DROP TABLE IF EXISTS nr_n;\n")
	for _, tb := range []struct {
		name, key string
		rows      []nrRow
	}{{"nr_p", "rid", nrOuter}, {"nr_s", "sid", nrInner}} {
		fmt.Fprintf(&b, "CREATE TABLE %s (%s int8, id int8, id2 int8, w int8", tb.name, tb.key)
		for _, ty := range nrFixtureCols() {
			fmt.Fprintf(&b, ", %s %s", ty.col, ty.pg)
		}
		b.WriteString(");\n")
		for _, r := range tb.rows {
			fmt.Fprintf(&b, "INSERT INTO %s VALUES (%d, %s, %s, %s", tb.name, r.key, nrPGInt(r.id), nrPGInt(r.id2), nrPGInt(r.w))
			for _, ty := range nrFixtureCols() {
				if r.v == nil {
					b.WriteString(", NULL")
					continue
				}
				b.WriteString(", " + ty.pgLit(*r.v, r.nan && ty.float))
			}
			b.WriteString(");\n")
		}
	}
	b.WriteString("CREATE TABLE nr_n (sid int8, id int8, c_i64 int8);\n")
	for i := 1; i <= 13; i++ {
		fmt.Fprintf(&b, "INSERT INTO nr_n VALUES (%d, %d, NULL);\n", i, i)
	}
	return b.String()
}

type nrCell struct{ name, sql string }

// nrConstructs are the subquery predicate spellings over residual R (which
// names p.<cols> and s.<cols>) and inner relation FROM text F. `in` and the
// families after it correlate through the body's WHERE.
func nrConstructs() []struct{ name, f string } {
	return []struct{ name, f string }{
		{"exists", "SELECT p.rid FROM nr_p p WHERE EXISTS (SELECT 1 FROM {F} WHERE s.id = p.id AND {R})"},
		{"notExists", "SELECT p.rid FROM nr_p p WHERE NOT EXISTS (SELECT 1 FROM {F} WHERE s.id = p.id AND {R})"},
		{"in", "SELECT p.rid FROM nr_p p WHERE p.id IN (SELECT s.id FROM {F} WHERE {R})"},
		{"notIn", "SELECT p.rid FROM nr_p p WHERE p.id NOT IN (SELECT s.id FROM {F} WHERE {R})"},
		{"eqAny", "SELECT p.rid FROM nr_p p WHERE p.id = ANY (SELECT s.id FROM {F} WHERE {R})"},
		{"neAll", "SELECT p.rid FROM nr_p p WHERE p.id <> ALL (SELECT s.id FROM {F} WHERE {R})"},
	}
}

func nrAt(tmpl, from, r string) string {
	return strings.ReplaceAll(strings.ReplaceAll(tmpl, "{F}", from), "{R}", r)
}

// nrCells is the matrix, generated:
//
//   - type/<col>/<op>/<construct>: every type family × the six comparison
//     residuals (s.c OP p.c, and the outer-on-the-left spelling p.c < s.c)
//     under EXISTS and NOT EXISTS, and > under IN / NOT IN;
//   - res/<form>/<construct>: the residual forms over c_i64 under every
//     construct (= ANY and <> ALL included);
//   - shape/<shape>/<construct>: keyless, two keys, empty inner, derived and
//     CTE renames, an all-NULL inner side, under EXISTS / NOT EXISTS;
//   - pos/<position>/...: the subquery predicate under OR;
//   - ctl/...: INNER and LEFT joins carrying the same residual in ON or
//     WHERE, a scalar comparison, INTERSECT / EXCEPT — the controls.
func nrCells() []nrCell {
	var out []nrCell
	const s = "nr_s s"
	ops := []struct{ name, r string }{
		{"gt", "s.{c} > p.{c}"}, {"ge", "s.{c} >= p.{c}"}, {"lt", "s.{c} < p.{c}"},
		{"le", "s.{c} <= p.{c}"}, {"ne", "s.{c} <> p.{c}"}, {"flipLt", "p.{c} < s.{c}"},
	}
	cons := nrConstructs()
	for _, ty := range nrTypes() {
		for _, op := range ops {
			r := strings.ReplaceAll(op.r, "{c}", ty.col)
			for _, c := range cons[:2] {
				out = append(out, nrCell{"type/" + ty.col + "/" + op.name + "/" + c.name, nrAt(c.f, s, r)})
			}
			if op.name == "gt" {
				for _, c := range cons[2:4] {
					out = append(out, nrCell{"type/" + ty.col + "/" + op.name + "/" + c.name, nrAt(c.f, s, r)})
				}
			}
		}
		if ty.arith {
			r := "s." + ty.col + " + 1 > p." + ty.col
			for _, c := range cons[:2] {
				out = append(out, nrCell{"type/" + ty.col + "/arith/" + c.name, nrAt(c.f, s, r)})
			}
		}
		r := "s." + ty.col + " IS DISTINCT FROM p." + ty.col
		for _, c := range cons[:2] {
			out = append(out, nrCell{"type/" + ty.col + "/idf/" + c.name, nrAt(c.f, s, r)})
		}
		r = "s." + ty.col + " IS NOT DISTINCT FROM p." + ty.col
		for _, c := range cons[:2] {
			out = append(out, nrCell{"type/" + ty.col + "/indf/" + c.name, nrAt(c.f, s, r)})
		}
		r = "s." + ty.col + " > " + ty.before
		for _, c := range cons[:2] {
			out = append(out, nrCell{"type/" + ty.col + "/innerOnly/" + c.name, nrAt(c.f, s, r)})
		}
	}
	forms := []struct{ name, r string }{
		{"gt", "s.c_i64 > p.c_i64"},
		{"ne", "s.c_i64 <> p.c_i64"},
		{"eqNonKey", "s.c_i64 = p.c_i64"},
		{"idf", "s.c_i64 IS DISTINCT FROM p.c_i64"},
		{"indf", "s.c_i64 IS NOT DISTINCT FROM p.c_i64"},
		{"between", "s.c_i64 BETWEEN p.c_i64 AND p.c_i64 + 5000"},
		{"betweenRev", "p.c_i64 BETWEEN s.c_i64 AND s.c_i64 + 5000"},
		{"like", "s.c_txt LIKE p.c_txt"},
		{"arith", "s.c_i64 + 1 > p.c_i64"},
		{"or", "(s.c_i64 > p.c_i64 OR s.id = 3)"},
		{"not", "NOT (s.c_i64 > p.c_i64)"},
		{"and2", "s.c_i64 > p.c_i64 AND s.w < p.w"},
		{"and2rev", "s.w < p.w AND s.c_i64 > p.c_i64"},
		{"coalesce", "COALESCE(s.c_i64, 0) > p.c_i64"},
		{"coalesceOuter", "s.c_i64 > COALESCE(p.c_i64, 0)"},
		{"outerOnly", "p.c_i64 > 4000"},
		{"innerOnly", "s.c_i64 > 4000"},
		{"constant", "1 = 1"},
		{"wCol", "s.w < p.w"},
		{"twoCols", "s.c_i32 > p.c_i32 AND s.c_txt > p.c_txt"},
	}
	for _, f := range forms {
		for _, c := range cons {
			out = append(out, nrCell{"res/" + f.name + "/" + c.name, nrAt(c.f, s, f.r)})
		}
	}
	// 1681b: operands of two types (s.X OP p.Y), every operator family.
	mixed := []struct{ name, s, p string }{
		{"i32_i64b", "c_i32", "c_i64b"}, {"i64b_f64", "c_i64b", "c_f64"}, {"f64_i64b", "c_f64", "c_i64b"},
		{"i64b_dec", "c_i64b", "c_dec"}, {"dec2_dec", "c_dec2", "c_dec"}, {"f32_f64", "c_f32", "c_f64"},
		{"f64_dec", "c_f64", "c_dec"}, {"dt_ts", "c_dt", "c_ts"}, {"decbig_dec", "c_decbig", "c_dec"},
	}
	for _, m := range mixed {
		for _, op := range []struct{ name, op string }{{"gt", ">"}, {"le", "<="}, {"ne", "<>"}, {"idf", "IS DISTINCT FROM"}} {
			r := "s." + m.s + " " + op.op + " p." + m.p
			for _, c := range cons[:2] {
				out = append(out, nrCell{"mixed/" + m.name + "/" + op.name + "/" + c.name, nrAt(c.f, s, r)})
			}
		}
	}
	// 1681c: `<>` with an integer-class build value (the distinct-pair build
	// keys int32/int64/date values) against every probe type PostgreSQL
	// compares it with, by values that tell a numeric comparison from an
	// integer-key one, and the pairs PostgreSQL refuses.
	nePairs := []struct{ name, s, p string }{
		{"i64b_f64i", "c_i64b", "c_f64i"}, {"i64b_f32i", "c_i64b", "c_f32i"}, {"i64b_f32", "c_i64b", "c_f32"},
		{"i32_f64i", "c_i32", "c_f64i"}, {"i32_f32i", "c_i32", "c_f32i"}, {"i32_deci", "c_i32", "c_deci"},
		{"i64b_deci", "c_i64b", "c_deci"}, {"i64b_dec0", "c_i64b", "c_dec0"}, {"i64b_decbig", "c_i64b", "c_decbig"},
		{"big_bigf", "c_big", "c_bigf"}, {"bigf_big", "c_bigf", "c_big"}, {"dt_tsd", "c_dt", "c_tsd"},
		{"i64b_i32", "c_i64b", "c_i32"}, {"i64b_ts", "c_i64b", "c_ts"}, {"i64b_iv", "c_i64b", "c_iv"},
		{"dt_i32", "c_dt", "c_i32"},
	}
	for _, m := range nePairs {
		r := "s." + m.s + " <> p." + m.p
		n := 2
		if m.name == "i64b_f64i" || m.name == "big_bigf" {
			n = 4
		}
		for _, c := range cons[:n] {
			out = append(out, nrCell{"ne/" + m.name + "/" + c.name, nrAt(c.f, s, r)})
		}
	}
	for _, c := range cons[:2] {
		out = append(out, nrCell{"ne/flip_i64b_f64i/" + c.name, nrAt(c.f, s, "p.c_f64i <> s.c_i64b")})
	}
	// 1681b: residual shapes the decorrelation either carries (two or three
	// conjuncts, one of them IS DISTINCT FROM; parentheses) or declines to
	// the per-row path (a cast, an IN list, a function, a literal operand).
	forms2 := []struct{ name, r string }{
		{"mixIdf", "s.c_i64 > p.c_i64 AND s.w IS DISTINCT FROM p.w"},
		{"mixIdfRev", "s.w IS DISTINCT FROM p.w AND s.c_i64 > p.c_i64"},
		{"mixIndf", "s.c_dec > p.c_dec AND s.w IS NOT DISTINCT FROM p.w"},
		{"three", "s.c_i64 > p.c_i64 AND s.w < p.w AND s.c_txt > p.c_txt"},
		{"threeTyped", "s.c_ip > p.c_ip AND s.c_by >= p.c_by AND s.c_f64 > p.c_f64"},
		{"paren", "(s.c_dec > p.c_dec)"},
		{"cast", "CAST(s.c_i32 AS BIGINT) > p.c_i64b"},
		{"inList", "s.c_i64 IN (p.c_i64, 5000)"},
		{"isNull", "s.c_i64 IS NULL"},
		{"boolCol", "s.c_b"},
		{"boolNe", "s.c_b <> p.c_b"},
		{"boolGt", "s.c_b > p.c_b"},
		{"func", "abs(s.c_neg) > p.c_i64b"},
		{"litLeft", "5000 < s.c_i64"},
	}
	for _, f := range forms2 {
		for _, c := range cons[:2] {
			out = append(out, nrCell{"res2/" + f.name + "/" + c.name, nrAt(c.f, s, f.r)})
		}
	}
	gt := "s.c_i64 > p.c_i64"
	shapes := []struct{ name, sql string }{
		{"keyless/exists", "SELECT p.rid FROM nr_p p WHERE EXISTS (SELECT 1 FROM nr_s s WHERE s.c_i64 > p.c_i64)"},
		{"keyless/notExists", "SELECT p.rid FROM nr_p p WHERE NOT EXISTS (SELECT 1 FROM nr_s s WHERE s.c_i64 > p.c_i64)"},
		{"twoKeys/exists", "SELECT p.rid FROM nr_p p WHERE EXISTS (SELECT 1 FROM nr_s s WHERE s.id = p.id AND s.id2 = p.id2 AND " + gt + ")"},
		{"twoKeys/notExists", "SELECT p.rid FROM nr_p p WHERE NOT EXISTS (SELECT 1 FROM nr_s s WHERE s.id = p.id AND s.id2 = p.id2 AND " + gt + ")"},
		{"empty/exists", "SELECT p.rid FROM nr_p p WHERE EXISTS (SELECT 1 FROM (SELECT * FROM nr_s WHERE sid < 0) s WHERE s.id = p.id AND " + gt + ")"},
		{"empty/notExists", "SELECT p.rid FROM nr_p p WHERE NOT EXISTS (SELECT 1 FROM (SELECT * FROM nr_s WHERE sid < 0) s WHERE s.id = p.id AND " + gt + ")"},
		{"derived/exists", "SELECT p.rid FROM nr_p p WHERE EXISTS (SELECT 1 FROM (SELECT id, c_i64 AS r FROM nr_s) s WHERE s.id = p.id AND s.r > p.c_i64)"},
		{"derived/notExists", "SELECT p.rid FROM nr_p p WHERE NOT EXISTS (SELECT 1 FROM (SELECT id, c_i64 AS r FROM nr_s) s WHERE s.id = p.id AND s.r > p.c_i64)"},
		{"derivedTxt/exists", "SELECT p.rid FROM nr_p p WHERE EXISTS (SELECT 1 FROM (SELECT id, c_txt AS r FROM nr_s) s WHERE s.id = p.id AND s.r > p.c_txt)"},
		{"derivedOuter/exists", "SELECT q.rid FROM (SELECT rid, id, c_i64 AS x FROM nr_p) q WHERE EXISTS (SELECT 1 FROM nr_s s WHERE s.id = q.id AND s.c_i64 > q.x)"},
		{"derivedOuter/notExists", "SELECT q.rid FROM (SELECT rid, id, c_i64 AS x FROM nr_p) q WHERE NOT EXISTS (SELECT 1 FROM nr_s s WHERE s.id = q.id AND s.c_i64 > q.x)"},
		{"cte/exists", "WITH s AS (SELECT id, c_i64 AS r FROM nr_s) SELECT p.rid FROM nr_p p WHERE EXISTS (SELECT 1 FROM s WHERE s.id = p.id AND s.r > p.c_i64)"},
		{"cte/notExists", "WITH s AS (SELECT id, c_i64 AS r FROM nr_s) SELECT p.rid FROM nr_p p WHERE NOT EXISTS (SELECT 1 FROM s WHERE s.id = p.id AND s.r > p.c_i64)"},
		{"allNull/exists", "SELECT p.rid FROM nr_p p WHERE EXISTS (SELECT 1 FROM nr_n s WHERE s.id = p.id AND " + gt + ")"},
		{"allNull/notExists", "SELECT p.rid FROM nr_p p WHERE NOT EXISTS (SELECT 1 FROM nr_n s WHERE s.id = p.id AND " + gt + ")"},
		{"allNull/notIn", "SELECT p.rid FROM nr_p p WHERE p.id NOT IN (SELECT s.id FROM nr_n s WHERE " + gt + ")"},
		{"count/exists", "SELECT count(*) FROM nr_p p WHERE EXISTS (SELECT 1 FROM nr_s s WHERE s.id = p.id AND " + gt + ")"},
		{"count/notExists", "SELECT count(*) FROM nr_p p WHERE NOT EXISTS (SELECT 1 FROM nr_s s WHERE s.id = p.id AND " + gt + ")"},
		{"joinedOuter/exists", "SELECT p.rid, q.rid FROM nr_p p JOIN nr_p q ON q.rid = p.rid WHERE EXISTS (SELECT 1 FROM nr_s s WHERE s.id = p.id AND " + gt + ")"},
		{"joinedOuter/notExists", "SELECT p.rid, q.rid FROM nr_p p JOIN nr_p q ON q.rid = p.rid WHERE NOT EXISTS (SELECT 1 FROM nr_s s WHERE s.id = p.id AND " + gt + ")"},
	}
	for _, sh := range shapes {
		out = append(out, nrCell{"shape/" + sh.name, sh.sql})
	}
	positions := []struct{ name, sql string }{
		{"or/exists", "SELECT p.rid FROM nr_p p WHERE EXISTS (SELECT 1 FROM nr_s s WHERE s.id = p.id AND " + gt + ") OR p.rid = 1"},
		{"or/notExists", "SELECT p.rid FROM nr_p p WHERE NOT EXISTS (SELECT 1 FROM nr_s s WHERE s.id = p.id AND " + gt + ") OR p.rid = 1"},
		{"or/in", "SELECT p.rid FROM nr_p p WHERE p.id IN (SELECT s.id FROM nr_s s WHERE " + gt + ") OR p.rid = 1"},
		{"and/exists", "SELECT p.rid FROM nr_p p WHERE p.rid > 1 AND EXISTS (SELECT 1 FROM nr_s s WHERE s.id = p.id AND " + gt + ")"},
		{"select/exists", "SELECT p.rid, EXISTS (SELECT 1 FROM nr_s s WHERE s.id = p.id AND " + gt + ") FROM nr_p p"},
	}
	for _, ps := range positions {
		out = append(out, nrCell{"pos/" + ps.name, ps.sql})
	}
	controls := []struct{ name, sql string }{
		{"inner/on", "SELECT p.rid, s.sid FROM nr_p p JOIN nr_s s ON s.id = p.id AND " + gt},
		{"inner/where", "SELECT p.rid, s.sid FROM nr_p p JOIN nr_s s ON s.id = p.id WHERE " + gt},
		{"inner/onTxt", "SELECT p.rid, s.sid FROM nr_p p JOIN nr_s s ON s.id = p.id AND s.c_txt > p.c_txt"},
		{"inner/onNe", "SELECT p.rid, s.sid FROM nr_p p JOIN nr_s s ON s.id = p.id AND s.c_i64 <> p.c_i64"},
		{"left/on", "SELECT p.rid, s.sid FROM nr_p p LEFT JOIN nr_s s ON s.id = p.id AND " + gt},
		{"left/onTxt", "SELECT p.rid, s.sid FROM nr_p p LEFT JOIN nr_s s ON s.id = p.id AND s.c_txt > p.c_txt"},
		{"left/onNe", "SELECT p.rid, s.sid FROM nr_p p LEFT JOIN nr_s s ON s.id = p.id AND s.c_i64 <> p.c_i64"},
		{"scalar/lt", "SELECT p.rid FROM nr_p p WHERE p.c_i64 < (SELECT max(s.c_i64) FROM nr_s s WHERE s.id = p.id)"},
		{"setop/intersect", "SELECT id, c_i64 FROM nr_p INTERSECT SELECT id, c_i64 FROM nr_s"},
		{"setop/except", "SELECT id, c_i64 FROM nr_p EXCEPT SELECT id, c_i64 FROM nr_s"},
		{"nofilter/exists", "SELECT p.rid FROM nr_p p WHERE EXISTS (SELECT 1 FROM nr_s s WHERE s.id = p.id)"},
		{"nofilter/notExists", "SELECT p.rid FROM nr_p p WHERE NOT EXISTS (SELECT 1 FROM nr_s s WHERE s.id = p.id)"},
		{"nofilter/notIn", "SELECT p.rid FROM nr_p p WHERE p.id NOT IN (SELECT s.id FROM nr_s s)"},
	}
	for _, c := range controls {
		out = append(out, nrCell{"ctl/" + c.name, c.sql})
	}
	return out
}

// TestArc1681aGenerate dumps the cells (NR_GEN=<path>, name<TAB>sql) and the
// PostgreSQL fixture (NR_PGDDL=<path>) for the oracle run; skipped otherwise.
func TestArc1681aGenerate(t *testing.T) {
	cells, ddl := os.Getenv("NR_GEN"), os.Getenv("NR_PGDDL")
	if cells == "" && ddl == "" {
		t.Skip("NR_GEN / NR_PGDDL unset")
	}
	if cells != "" {
		var b strings.Builder
		for _, c := range nrCells() {
			b.WriteString(c.name + "\t" + c.sql + "\n")
		}
		if err := os.WriteFile(cells, []byte(b.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if ddl != "" {
		if err := os.WriteFile(ddl, []byte(nrPGFixture()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func nrPGAnswers(t *testing.T) map[string]string {
	t.Helper()
	f, err := os.Open("testdata/arc_1681a_semi_anti_null_residual_pg17.tsv")
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

func nrStandalone(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
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
	for _, tbl := range nrTables() {
		if err := db.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
			t.Fatalf("create %s: %v", tbl.name, err)
		}
		ing := db.NewIngester(tbl.name, tbl.schema, nil, ingest.Config{MaxBufferRows: len(tbl.rows) + 1, RowGroupSize: 4})
		if err := ing.Ingest(ctx, tbl.rows); err != nil {
			t.Fatalf("ingest %s: %v", tbl.name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", tbl.name, err)
		}
	}
	return db
}

// nrArms: single and spilled512k are the EMBEDDED engine (wadjet.DB, the
// MIT path); the three DAG arms run through the coordinator and its workers.
func nrArms(t *testing.T, ctx context.Context) []brArm {
	t.Helper()
	stand := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, nrTables())
		if wcfg != nil {
			return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
		}
		return tmdCoordinator(t, ctx, infra, opts...)
	}
	single, spilled := nrStandalone(t, ctx, 0), nrStandalone(t, ctx, 512*1024)
	coord := stand(nil)
	coordB := stand(nil, func(c *Config) { c.BroadcastBytesOverride = 1 })
	coordM := stand(func(w *worker.Config) { w.MorselWorkers = 4 })
	runSingle := func(db *wadjet.DB) func(string) (*oracle.Result, error) {
		return func(sql string) (*oracle.Result, error) { return tmdRunSingle(ctx, db, sql) }
	}
	runDAG := func(c *Coordinator) func(string) (*oracle.Result, error) {
		return func(sql string) (*oracle.Result, error) { return tmdRunDAG(ctx, c, sql) }
	}
	return []brArm{
		{"single", runSingle(single)},
		{"spilled512k", runSingle(spilled)},
		{"dag", runDAG(coord)},
		{"dag-shuffled", runDAG(coordB)},
		{"dag-morsel4", runDAG(coordM)},
	}
}

// nrVerdict is one arm's answer against PostgreSQL's: "right", "wrong" or
// "refused".
func nrVerdict(want string, res *oracle.Result, err error) (string, string) {
	if rest, ok := strings.CutPrefix(want, "ERR "); ok {
		state, _, _ := strings.Cut(rest, " ")
		if err != nil && sqlerr.StateOf(err) == state {
			return "right", ""
		}
		if err != nil {
			return "refused", err.Error()
		}
		return "wrong", brRender(res)
	}
	if err != nil {
		return "refused", err.Error()
	}
	if got := brRender(res); got != want {
		return "wrong", got
	}
	return "right", ""
}

// nrKnown names every cell that is still not PostgreSQL's answer on an arm,
// with its verdict and cause; each cause is its own defect, outside the
// residual comparison this table was built for (the 1681a and 1681b result
// notes list them for epic #1681). The test fails when a known cell starts answering as PostgreSQL does
// (delete its rule), changes verdict, or when any other cell is not
// PostgreSQL's.
func nrKnown(name, arm string) (verdict, cause string, known bool) {
	p := strings.Split(name, "/")
	single := arm == "single" || arm == "spilled512k"
	in := func(s string, set ...string) bool {
		for _, x := range set {
			if s == x {
				return true
			}
		}
		return false
	}
	if p[0] == "ne" && p[1] == "i64b_f64i" && in(p[2], "in", "notIn") {
		return "refused", "the per-row correlated re-run has no literal for a float outer value", true
	}
	if p[0] == "ne" && p[1] == "i64b_iv" {
		// 1681c, outside the residual: PostgreSQL has no bigint <> interval
		// operator (42883); this engine accepts the comparison.
		return "wrong", "bigint <> interval is accepted where PostgreSQL raises 42883", true
	}
	if p[0] != "type" {
		return "", "", false
	}
	col, op, con := p[1], p[2], p[3]
	switch {
	case (op == "arith" || (op == "gt" && in(con, "in", "notIn"))) && in(col, "c_f64", "c_f32"):
		return "refused", "the per-row correlated re-run has no literal for a float outer value", true
	case op == "gt" && in(con, "in", "notIn") && col == "c_b":
		return "wrong", "a BOOL comparison in a correlated IN body re-run per outer row", true
	case op == "innerOnly" && in(col, "c_by", "c_ip", "c_ip6"):
		return "refused", "the ::bytea / ::inet cast spelling in the body: type does not exist", true
	case op == "innerOnly" && col == "c_iv" && single:
		return "refused", "an INTERVAL literal in an inner-only body filter: invalid input syntax for type duration \"04:00:00\"", true
	case op == "innerOnly" && col == "c_iv":
		return "wrong", "an INTERVAL literal in an inner-only body filter on the DAG", true
	case op == "innerOnly" && col == "c_b" && single:
		return "wrong", "an inner-only BOOL comparison with false on the single-process path", true
	}
	return "", "", false
}

// A SEMI / ANTI JOIN'S NON-EQUALITY RESIDUAL READS A NULL OPERAND AS UNKNOWN,
// NOT AS A VALUE, ON EVERY ARM (epic #1681, line 1681a). The generated matrix
// (nrCells) against PostgreSQL 17.11's sorted rows. At b5db2669 a residual
// such as `s.v > p.v` with p.v NULL read the NULL slot's zero value, so
// EXISTS kept the row and NOT EXISTS dropped it, on the embedded engine and
// every DAG arm alike.
//
// AND COMPARES ITS OPERANDS AS POSTGRESQL DOES (line 1681b). At 9d476ca6 the
// residual compiler dropped an IS [NOT] DISTINCT FROM conjunct (existence
// only), compared numeric, bytea and the network types by printed text,
// compared NaN IEEE-style, read the wrong array for operands of two types,
// and a `<>` residual on a float / numeric column refused on the embedded
// engine because PruneBuildColumns left the build key index naming a pruned
// position. The matrix grew the 1681b families (negative, multi-length,
// second-scale and 38-digit numeric, IPv6, CIDR, MAC, interval), IS NOT
// DISTINCT FROM per family, operand pairs of two types, and residual shapes.
func TestArc1681aSemiAntiNullResidualEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the 1681a matrix")
	}
	answers := nrPGAnswers(t)
	cells := nrCells()
	if len(cells) != len(answers) || len(cells) < 380 {
		t.Fatalf("%d cells, %d PostgreSQL answers: re-measure the table", len(cells), len(answers))
	}
	for _, c := range cells {
		if _, ok := answers[c.name]; !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", c.name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	arms := nrArms(t, ctx)
	dump := os.Getenv("NR_DUMP")
	var dumpB strings.Builder
	var mu sync.Mutex
	for _, tc := range cells {
		want := answers[tc.name]
		t.Run(tc.name, func(t *testing.T) {
			type result struct {
				res *oracle.Result
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
				verdict, got := nrVerdict(want, results[i].res, results[i].err)
				if dump != "" {
					mu.Lock()
					fmt.Fprintf(&dumpB, "%s\t%s\t%s\t%s\n", tc.name, arm.name, verdict, got)
					mu.Unlock()
				}
				kv, cause, known := nrKnown(tc.name, arm.name)
				switch {
				case verdict == "right" && known:
					t.Errorf("%s\n  arm  %s\n  now answers as PostgreSQL 17.11 does (%s): delete its nrKnown rule (%s)", tc.sql, arm.name, want, cause)
				case verdict != "right" && !known:
					t.Errorf("%s\n  arm  %s\n  %s: %s\n  want %s (PostgreSQL 17.11)", tc.sql, arm.name, verdict, got, want)
				case known && verdict != kv:
					t.Errorf("%s\n  arm  %s\n  %s: %s\n  pinned %s (%s); want %s (PostgreSQL 17.11)", tc.sql, arm.name, verdict, got, kv, cause, want)
				}
			}
		})
	}
	if dump != "" {
		if err := os.WriteFile(dump, []byte(dumpB.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}
