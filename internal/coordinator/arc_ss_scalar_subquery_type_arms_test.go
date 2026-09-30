// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// A SCALAR SUBQUERY'S ANSWER IS A TYPED VALUE WHEREVER IT IS READ (#1428,
// #1431, #1427, #1422). The seam is the subquery's answer as an OPERAND: the
// value its SELECT list declares, read by a projection, a CAST, extract /
// date_trunc / date arithmetic, a comparison, arithmetic, a function or an
// aggregate argument, a GROUP BY / ORDER BY key, a CASE arm and a membership.
// This table is that seam enumerated once — result type × subquery form ×
// consumer — on five arms against PostgreSQL 17.11's full sorted rows AND the
// declared type class of every result column.
//
// The fixture is built so a value read in the wrong unit or at the wrong type
// answers differently from the right one: DATE / TIMESTAMP rows at the epoch,
// at 1969-12-31 (23:59:59.999, epoch ms -1), at 1000-01-01 and at 9999-12-31
// (the day count 2 932 896, which a magnitude guess reads as milliseconds),
// and DOUBLE / NUMERIC values with a fraction an integer declaration drops.
//
//	ss_t: id | i int | b bigint | f double | n numeric(10,2) | s text | o bool | d date | ts timestamp | u uuid | a int[]
//	ss_i: id | v int | g double | m numeric(10,2)

type ssRow struct {
	i, b, f, n, s, o, d, ts, u, a any
}

var ssRows = []ssRow{
	{int32(3), int64(30), 1.5, 2.25, "abc", true, "2024-03-04", "2024-03-04 12:00:00", "00000000-0000-4000-8000-000000000001", []any{int32(1), int32(2)}},
	{int32(-7), int64(-70), -2.5, -3.5, "Hello", false, "1970-01-01", "1970-01-01 00:00:00", "00000000-0000-4000-8000-000000000002", []any{int32(3)}},
	{int32(5), int64(9000000000), 0.25, 10.0, "zz", true, "9999-12-31", "9999-12-31 23:59:59.999", "00000000-0000-4000-8000-000000000003", []any{int32(4), int32(5), int32(6)}},
	{int32(0), int64(0), 0.0, 0.0, "", false, "1000-01-01", "1000-01-01 00:00:00", "00000000-0000-4000-8000-000000000004", []any{int32(7)}},
	{int32(1), int64(1), 100.125, 0.01, "x", true, "1969-12-31", "1969-12-31 23:59:59.999", "00000000-0000-4000-8000-000000000005", []any{int32(8)}},
	{nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
}

func ssMS(s string) int64 {
	layout := "2006-01-02 15:04:05"
	if strings.Contains(s, ".") {
		layout = "2006-01-02 15:04:05.000"
	}
	tm, err := time.Parse(layout, s)
	if err != nil {
		panic(err)
	}
	return tm.UnixMilli()
}

func ssTables() []tmdTable {
	elem := &parquet.Column{Name: "element", Type: parquet.TypeInt32, Nullable: true}
	tSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "i", Type: parquet.TypeInt32, Nullable: true},
		{Name: "b", Type: parquet.TypeInt64, Nullable: true},
		{Name: "f", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "n", Type: parquet.TypeDecimal, Precision: 10, Scale: 2, Nullable: true},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
		{Name: "o", Type: parquet.TypeBool, Nullable: true},
		{Name: "d", Type: parquet.TypeDate, Nullable: true},
		{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
		{Name: "u", Type: parquet.TypeUUID, Nullable: true},
		{Name: "a", Type: parquet.TypeArray, Nullable: true, ElementType: elem},
	}}
	var tRows []map[string]any
	for k, r := range ssRows {
		var ts any
		if r.ts != nil {
			ts = ssMS(r.ts.(string))
		}
		var a any
		if r.a != nil {
			a = r.a
		}
		tRows = append(tRows, map[string]any{"id": int64(k + 1), "i": r.i, "b": r.b, "f": r.f, "n": r.n,
			"s": r.s, "o": r.o, "d": r.d, "ts": ts, "u": r.u, "a": a})
	}
	iSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt32, Nullable: true},
		{Name: "g", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "m", Type: parquet.TypeDecimal, Precision: 10, Scale: 2, Nullable: true},
	}}
	iRows := []map[string]any{
		{"id": int64(1), "v": int32(5), "g": 0.5, "m": 1.25},
		{"id": int64(2), "v": int32(6), "g": 0.25, "m": nil},
	}
	return []tmdTable{{"ss_t", tSchema, tRows}, {"ss_i", iSchema, iRows}}
}

// ssPGFixture is the same fixture as PostgreSQL DDL.
func ssPGFixture() string {
	var b strings.Builder
	b.WriteString("DROP TABLE IF EXISTS ss_t; DROP TABLE IF EXISTS ss_i;\n")
	b.WriteString("CREATE TABLE ss_t (id bigint, i integer, b bigint, f double precision, n numeric(10,2), s text, o boolean, d date, ts timestamp, u uuid, a integer[]);\n")
	b.WriteString("CREATE TABLE ss_i (id bigint, v integer, g double precision, m numeric(10,2));\n")
	q := func(v any) string {
		switch x := v.(type) {
		case nil:
			return "NULL"
		case string:
			return "'" + strings.ReplaceAll(x, "'", "''") + "'"
		case []any:
			parts := make([]string, len(x))
			for i, e := range x {
				parts[i] = fmt.Sprint(e)
			}
			return "'{" + strings.Join(parts, ",") + "}'"
		}
		return fmt.Sprint(v)
	}
	for k, r := range ssRows {
		fmt.Fprintf(&b, "INSERT INTO ss_t VALUES (%d, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);\n",
			k+1, q(r.i), q(r.b), q(r.f), q(r.n), q(r.s), q(r.o), q(r.d), q(r.ts), q(r.u), q(r.a))
	}
	b.WriteString("INSERT INTO ss_i VALUES (1, 5, 0.5, 1.25), (2, 6, 0.25, NULL);\n")
	return b.String()
}

// ssType is one result type of the seam: the ss_t column that carries it, a
// literal of it, and which consumer families apply (PostgreSQL defines them).
type ssType struct {
	key, col, lit, pgLitType string
	agg                      string // aggregate over the column that keeps the type; "" none
	num, ord, text, temporal bool
	castable                 []string // CAST destinations PostgreSQL accepts from this type
}

func ssTypes() []ssType {
	return []ssType{
		{key: "int", col: "i", lit: "7", agg: "max", num: true, ord: true, castable: []string{"VARCHAR", "DOUBLE PRECISION", "INTEGER"}},
		{key: "bigint", col: "b", lit: "CAST(9000000000 AS BIGINT)", agg: "max", num: true, ord: true, castable: []string{"VARCHAR", "DOUBLE PRECISION", "INTEGER"}},
		{key: "double", col: "f", lit: "CAST(1.5 AS DOUBLE PRECISION)", agg: "max", num: true, ord: true, castable: []string{"VARCHAR", "DOUBLE PRECISION", "INTEGER"}},
		{key: "numeric", col: "n", lit: "CAST(2.25 AS NUMERIC(10,2))", agg: "max", num: true, ord: true, castable: []string{"VARCHAR", "DOUBLE PRECISION", "INTEGER"}},
		{key: "text", col: "s", lit: "CAST('abc' AS TEXT)", agg: "max", ord: true, text: true, castable: []string{"VARCHAR"}},
		{key: "bool", col: "o", lit: "true", castable: []string{"VARCHAR", "INTEGER"}},
		{key: "date", col: "d", lit: "DATE '2024-03-04'", agg: "max", ord: true, temporal: true, castable: []string{"VARCHAR", "DATE", "TIMESTAMP"}},
		{key: "timestamp", col: "ts", lit: "TIMESTAMP '2024-03-04 12:00:00'", agg: "max", ord: true, temporal: true, castable: []string{"VARCHAR", "DATE", "TIMESTAMP"}},
		{key: "uuid", col: "u", lit: "CAST('00000000-0000-4000-8000-000000000001' AS UUID)", castable: []string{"VARCHAR"}},
		{key: "array", col: "a", lit: "ARRAY[1,2]", castable: []string{"VARCHAR"}},
	}
}

// ssCell is one generated cell; its SQL is PostgreSQL's spelling too.
// ordered compares the rows in the order the statement returns them.
type ssCell struct {
	name, sql string
	ordered   bool
}

// ssCells is the whole table.
func ssCells() []ssCell {
	var out []ssCell
	add := func(name, sql string) { out = append(out, ssCell{name: name, sql: sql}) }
	addOrd := func(name, sql string) { out = append(out, ssCell{name: name, sql: sql, ordered: true}) }
	for _, ty := range ssTypes() {
		c := ty.col
		// The uncorrelated forms, each a subquery expression.
		forms := [][2]string{
			{"col", fmt.Sprintf("(SELECT %s FROM ss_t WHERE id = 1)", c)},
			{"lit", fmt.Sprintf("(SELECT %s)", ty.lit)},
			{"case", fmt.Sprintf("(SELECT CASE WHEN id = 1 THEN %s END FROM ss_t WHERE id = 1)", c)},
			{"coalesce", fmt.Sprintf("(SELECT COALESCE(%s, %s) FROM ss_t WHERE id = 6)", c, ty.lit)},
			{"null", fmt.Sprintf("(SELECT %s FROM ss_t WHERE id = 6)", c)},
			{"zero", fmt.Sprintf("(SELECT %s FROM ss_t WHERE id = 99)", c)},
		}
		if ty.agg != "" {
			// Without row 3, so the maximum is an ordinary value (2024-03-04
			// 12:00:00 for the TIMESTAMP, #1431's own) and not the 9999-12-31
			// boundary the boundary family owns.
			forms = append(forms, [2]string{"agg", fmt.Sprintf("(SELECT %s(%s) FROM ss_t WHERE id <> 3)", ty.agg, c)})
			forms = append(forms, [2]string{"aggMin", fmt.Sprintf("(SELECT min(%s) FROM ss_t)", c)})
		}
		for _, f := range forms {
			base := ty.key + "/" + f[0] + "/"
			sq := f[1]
			add(base+"proj", "SELECT "+sq+" AS x")
			add(base+"derived", "SELECT x FROM (SELECT "+sq+" AS x) s")
			add(base+"cte", "WITH w AS (SELECT "+sq+" AS x) SELECT x FROM w")
			add(base+"union", "SELECT "+sq+" AS x UNION ALL SELECT "+sq)
			add(base+"perRow", "SELECT id, "+sq+" AS x FROM ss_t")
			for _, dest := range ty.castable {
				add(base+"cast"+ssDestKey(dest), "SELECT CAST("+sq+" AS "+dest+") AS x")
			}
			add(base+"caseArm", "SELECT id, CASE WHEN id = 1 THEN "+sq+" END AS x FROM ss_t")
			add(base+"groupBy", "SELECT "+sq+" AS k, count(*) AS n FROM ss_t GROUP BY 1")
			if ty.key != "array" {
				add(base+"member", "SELECT "+sq+" IN (SELECT "+c+" FROM ss_t) AS x")
			}
			if ty.ord {
				add(base+"eqCol", "SELECT id FROM ss_t WHERE "+c+" = "+sq)
				add(base+"ltCol", "SELECT id FROM ss_t WHERE "+c+" < "+sq)
				add(base+"gtCol", "SELECT id FROM ss_t WHERE "+sq+" < "+c)
				add(base+"between", "SELECT id FROM ss_t WHERE "+c+" BETWEEN "+sq+" AND "+sq)
				add(base+"eqLit", "SELECT "+sq+" = "+ty.lit+" AS x")
				add(base+"eqSq", "SELECT "+sq+" = (SELECT "+c+" FROM ss_t WHERE id = 1) AS x")
				add(base+"maxArg", "SELECT max("+sq+") AS x FROM ss_t")
				addOrd(base+"orderBy", "SELECT id FROM ss_t ORDER BY "+sq+", id")
			}
			if ty.num {
				add(base+"plusCol", "SELECT id, "+c+" + "+sq+" AS x FROM ss_t")
				add(base+"plusLit", "SELECT "+sq+" + 1 AS x")
				add(base+"timesLit", "SELECT "+sq+" * 2 AS x")
				add(base+"abs", "SELECT abs("+sq+") AS x")
				add(base+"sumArg", "SELECT sum("+sq+") AS x FROM ss_t")
			}
			if ty.text {
				add(base+"length", "SELECT length("+sq+") AS x")
				add(base+"upper", "SELECT upper("+sq+") AS x")
			}
			if ty.temporal {
				add(base+"extractYear", "SELECT extract(year FROM "+sq+") AS x")
				add(base+"plusHour", "SELECT "+sq+" + INTERVAL '1 hour' AS x")
				add(base+"minusDay", "SELECT "+sq+" - INTERVAL '1 day' AS x")
				add(base+"perRowPlusHour", "SELECT id, "+sq+" + INTERVAL '1 hour' AS x FROM ss_t")
				add(base+"derivedPlusHour", "SELECT s.x FROM (SELECT "+sq+" + INTERVAL '1 hour' AS x) s")
			}
			if ty.key == "timestamp" {
				add(base+"dateTrunc", "SELECT date_trunc('day', "+sq+") AS x")
			}
			if ty.key == "date" {
				add(base+"plusInt", "SELECT "+sq+" + 1 AS x")
			}
		}
		// TWO ROWS: 21000 wherever the subquery is read.
		add(ty.key+"/two/proj", fmt.Sprintf("SELECT (SELECT %s FROM ss_t WHERE id IN (1, 2)) AS x", c))
		// The correlated form returning the OUTER column itself.
		corr := fmt.Sprintf("(SELECT o.%s FROM ss_i x WHERE x.id = 1)", c)
		cb := ty.key + "/corr/"
		add(cb+"proj", "SELECT o.id, "+corr+" AS x FROM ss_t o")
		add(cb+"derived", "SELECT id, x FROM (SELECT o.id, "+corr+" AS x FROM ss_t o) s")
		add(cb+"castVarchar", "SELECT o.id, CAST("+corr+" AS VARCHAR) AS x FROM ss_t o")
		if ty.ord {
			add(cb+"eqCol", "SELECT o.id FROM ss_t o WHERE o."+c+" = "+corr)
			addOrd(cb+"orderBy", "SELECT o.id FROM ss_t o ORDER BY "+corr+", o.id")
		}
		if ty.num {
			add(cb+"plusLit", "SELECT o.id, "+corr+" + 1 AS x FROM ss_t o")
			add(cb+"sumArg", "SELECT sum("+corr+") AS x FROM ss_t o")
		}
		if ty.temporal {
			add(cb+"extractYear", "SELECT o.id, extract(year FROM "+corr+") AS x FROM ss_t o")
			add(cb+"plusHour", "SELECT o.id, "+corr+" + INTERVAL '1 hour' AS x FROM ss_t o")
		}
	}
	// THE ISSUES' OWN STATEMENTS over this fixture (ss_t id 5 is
	// 1969-12-31 23:59:59.999, min(ts) is 1000-01-01, max(ts) without id 3
	// is 2024-03-04 12:00:00, id 3's d is 9999-12-31).
	add("issue/1428/castVarchar", "SELECT CAST((SELECT ts FROM ss_t WHERE id = 5) AS VARCHAR) AS x")
	add("issue/1428/extractYear", "SELECT extract(year FROM (SELECT min(ts) FROM ss_t)) AS x")
	add("issue/1431/plusHour", "SELECT (SELECT max(ts) FROM ss_t WHERE id <> 3) + INTERVAL '1 hour' AS x")
	add("issue/1431/minusDay", "SELECT (SELECT max(ts) FROM ss_t WHERE id <> 3) - INTERVAL '1 day' AS x")
	add("issue/1431/perRow", "SELECT a.id, (SELECT max(ts) FROM ss_t WHERE id <> 3) + INTERVAL '1 hour' AS x FROM ss_t a")
	add("issue/1431/derived", "SELECT s.x FROM (SELECT (SELECT max(ts) FROM ss_t WHERE id <> 3) + INTERVAL '1 hour' AS x) s")
	add("issue/1431/windowControl", "SELECT a.id, max(a.ts) OVER () + INTERVAL '1 hour' AS x FROM ss_t a WHERE a.id <> 3")
	add("issue/1431/dateControl", "SELECT (SELECT max(d) FROM ss_t WHERE id <> 3) + INTERVAL '1 hour' AS x")
	add("issue/1427/eq", "SELECT id FROM ss_t WHERE d = (SELECT d FROM ss_t WHERE id = 3)")
	add("issue/1422/outerFirst", "SELECT c.id, (SELECT c.f + x.v FROM ss_i x WHERE x.id = 1) AS x FROM ss_t c WHERE c.id = 1")
	add("issue/1422/outerFirstId2", "SELECT c.id, (SELECT c.f + x.v FROM ss_i x WHERE x.id = 2) AS x FROM ss_t c WHERE c.id = 1")
	add("issue/1422/innerFirst", "SELECT c.id, (SELECT x.v + c.f FROM ss_i x WHERE x.id = 1) AS x FROM ss_t c WHERE c.id = 1")
	// THE BOUNDARY CELLS (#1427): a DATE / TIMESTAMP column against a scalar
	// subquery of its own type, at the epoch, 1969-12-31, 1000-01-01 and
	// 9999-12-31 — ss_t ids 2, 5, 4, 3.
	for _, k := range []int{2, 3, 4, 5} {
		for _, col := range []string{"d", "ts"} {
			for _, op := range []string{"=", "<", ">", "<=", ">=", "<>"} {
				opk := map[string]string{"=": "eq", "<": "lt", ">": "gt", "<=": "le", ">=": "ge", "<>": "ne"}[op]
				add(fmt.Sprintf("boundary/%s/id%d/%s", col, k, opk),
					fmt.Sprintf("SELECT id FROM ss_t WHERE %s %s (SELECT %s FROM ss_t WHERE id = %d)", col, op, col, k))
			}
			add(fmt.Sprintf("boundary/%s/id%d/corr", col, k),
				fmt.Sprintf("SELECT o.id FROM ss_t o WHERE o.%s = (SELECT r.%s FROM ss_t r WHERE r.id = %d AND r.id >= o.id - 10)", col, col, k))
		}
	}
	// THE CORRELATED ARITHMETIC (#1422): an outer value and an inner column of
	// different types, BOTH operand orders.
	arith := [][3]string{
		{"fPlusV", "o.f + x.v", "x.v + o.f"},
		{"iPlusG", "o.i + x.g", "x.g + o.i"},
		{"nPlusV", "o.n + x.v", "x.v + o.n"},
		{"iPlusM", "o.i + x.m", "x.m + o.i"},
		{"fTimesV", "o.f * x.v", "x.v * o.f"},
		{"fMinusV", "o.f - x.v", "x.v - o.f"},
		{"dPlusV", "o.d + x.v", "x.v + o.d"},
		{"dPlusInterval", "o.d + INTERVAL '1 day'", "INTERVAL '1 day' + o.d"},
		{"tsMinusInterval", "o.ts - INTERVAL '1 hour'", "o.ts + INTERVAL '-1 hour'"},
	}
	for _, a := range arith {
		for i, e := range a[1:] {
			order := []string{"outerFirst", "innerFirst"}[i]
			base := "corrArith/" + a[0] + "/" + order + "/"
			sq := "(SELECT " + e + " FROM ss_i x WHERE x.id = 1)"
			add(base+"proj", "SELECT o.id, "+sq+" AS x FROM ss_t o WHERE o.id IN (1, 2, 5, 6)")
			add(base+"plusLit", "SELECT o.id, "+sq+" + 1 AS x FROM ss_t o WHERE o.id IN (1, 2, 5, 6)")
			add(base+"castVarchar", "SELECT o.id, CAST("+sq+" AS VARCHAR) AS x FROM ss_t o WHERE o.id IN (1, 2, 5, 6)")
		}
	}
	// The same arithmetic UNCORRELATED — the declaration is the subquery's,
	// whatever made its operands.
	for _, a := range [][3]string{{"gPlusV", "x.g + x.v", "x.v + x.g"}, {"mPlusV", "x.m + x.v", "x.v + x.m"}} {
		for i, e := range a[1:] {
			order := []string{"leftDouble", "leftInt"}[i]
			base := "innerArith/" + a[0] + "/" + order + "/"
			sq := "(SELECT " + e + " FROM ss_i x WHERE x.id = 1)"
			add(base+"proj", "SELECT "+sq+" AS x")
			add(base+"zeroRows", "SELECT "+e+" AS x FROM ss_i x WHERE x.id = 99")
			add(base+"derivedZero", "SELECT y FROM (SELECT "+e+" AS y FROM ss_i x) s WHERE y > 100")
		}
	}
	// The remaining forms of the brief's list: an aggregate subquery that is
	// not the column's own type, and a FROM-less literal of each kind.
	for _, f := range [][2]string{
		{"sumInt", "(SELECT sum(i) FROM ss_t)"},
		{"avgInt", "(SELECT avg(i) FROM ss_t)"},
		{"avgDouble", "(SELECT avg(f) FROM ss_t)"},
		{"count", "(SELECT count(*) FROM ss_t)"},
		{"lit15", "(SELECT 1.5)"},
		{"litText", "(SELECT 'abc')"},
		{"litInterval", "(SELECT INTERVAL '1 hour')"},
	} {
		base := "form/" + f[0] + "/"
		add(base+"proj", "SELECT "+f[1]+" AS x")
		add(base+"plusLit", "SELECT "+f[1]+" + 1 AS x")
		add(base+"castVarchar", "SELECT CAST("+f[1]+" AS VARCHAR) AS x")
	}
	return out
}

func ssDestKey(dest string) string {
	switch dest {
	case "DOUBLE PRECISION":
		return "Double"
	}
	return strings.ToUpper(dest[:1]) + strings.ToLower(dest[1:])
}

// ssTypeClass maps a declared type to its CLASS — the comparison the table
// asserts. Width inside a class (integer vs bigint, varchar vs text, a
// numeric's typmod) is deliberately NOT asserted: that is the numeric-
// declaration arcs' dimension, not this seam's (landing notes, excluded
// dimensions).
func ssTypeClass(t parquet.TypeID, elem *parquet.Column) string {
	switch t {
	case parquet.TypeInt32, parquet.TypeInt64, parquet.TypePort, parquet.TypeProtocol, parquet.TypeDuration:
		return "int"
	case parquet.TypeFloat32, parquet.TypeFloat64:
		return "float"
	case parquet.TypeDecimal:
		return "numeric"
	case parquet.TypeString:
		return "text"
	case parquet.TypeBool:
		return "bool"
	case parquet.TypeDate:
		return "date"
	case parquet.TypeTimestamp:
		return "timestamp"
	case parquet.TypeUUID:
		return "uuid"
	case parquet.TypeArray:
		if elem != nil {
			return ssTypeClass(elem.Type, nil) + "[]"
		}
		return "array"
	}
	return strings.ToLower(t.String())
}

// ssFmt renders one cell the way PostgreSQL's text output prints it, reading
// the box by the column's DECLARED type (a TIMESTAMP boxes as epoch ms).
func ssFmt(v any, t parquet.TypeID) string {
	if v == nil {
		return "NULL"
	}
	switch t {
	case parquet.TypeTimestamp:
		switch x := v.(type) {
		case int64:
			return batch.FormatTimestamp(x)
		case time.Time:
			return batch.FormatTimestamp(x.UnixMilli())
		}
	case parquet.TypeDate:
		switch x := v.(type) {
		case int64:
			return batch.FormatDate(int32(x))
		case int32:
			return batch.FormatDate(x)
		}
	}
	switch x := v.(type) {
	case bool:
		if x {
			return "t"
		}
		return "f"
	case float64:
		return ssFloat(x)
	case float32:
		return ssFloat(float64(x))
	case []any:
		parts := make([]string, len(x))
		for i, e := range x {
			parts[i] = ssFmt(e, parquet.TypeInt64)
		}
		return "{" + strings.Join(parts, ",") + "}"
	}
	return fmt.Sprint(v)
}

// ssFloat is float8out's shortest spelling: plain digits for a decimal
// exponent in [-4, 15), exponent form otherwise.
func ssFloat(f float64) string {
	if math.IsInf(f, 1) {
		return "Infinity"
	}
	if math.IsInf(f, -1) {
		return "-Infinity"
	}
	if math.IsNaN(f) {
		return "NaN"
	}
	if f == 0 {
		return "0"
	}
	exp := int(math.Floor(math.Log10(math.Abs(f))))
	if exp < -4 || exp >= 15 {
		s := strconv.FormatFloat(f, 'e', -1, 64)
		return strings.Replace(strings.Replace(s, "e+0", "e+", 1), "e-0", "e-", 1)
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// ssRender is a result as `{class,class} rows=N r1 | r2` — sorted unless the
// cell is ordered.
func ssRender(cols []parquet.Column, cells [][]any, ordered bool) string {
	classes := make([]string, len(cols))
	for i, c := range cols {
		classes[i] = ssTypeClass(c.Type, c.ElementType)
	}
	rows := make([]string, 0, len(cells))
	for _, r := range cells {
		parts := make([]string, len(r))
		for i, v := range r {
			t := parquet.TypeString
			if i < len(cols) {
				t = cols[i].Type
			}
			parts[i] = ssFmt(v, t)
		}
		rows = append(rows, strings.Join(parts, ","))
	}
	if !ordered {
		sort.Strings(rows)
	}
	return fmt.Sprintf("{%s} rows=%d %s", strings.Join(classes, ","), len(rows), strings.Join(rows, " | "))
}

type ssArm struct {
	name string
	run  func(sql string, ordered bool) (string, error)
}

func ssStandalone(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
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
	for _, tbl := range ssTables() {
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

func ssRunSingle(ctx context.Context, db *wadjet.DB, sql string, ordered bool) (out string, err error) {
	defer func() {
		if r := recover(); r != nil {
			out, err = "", fmt.Errorf("PANIC: %v", r)
		}
	}()
	res, qerr := db.Query(ctx, sql)
	if qerr != nil {
		return "", qerr
	}
	cols := make([]parquet.Column, len(res.ColumnMetas))
	for i, m := range res.ColumnMetas {
		cols[i] = parquet.Column{Name: m.Name, Type: m.TypeID, ElementType: m.ElementType}
	}
	var cells [][]any
	for i := range res.Rows {
		cells = append(cells, res.Cells(i))
	}
	return ssRender(cols, cells, ordered), nil
}

func ssRunDAG(ctx context.Context, coord *Coordinator, sql string, ordered bool) (out string, err error) {
	defer func() {
		if r := recover(); r != nil {
			out, err = "", fmt.Errorf("PANIC: %v", r)
		}
	}()
	res, qerr := coord.ExecuteSQL(ctx, sql)
	if qerr != nil {
		return "", qerr
	}
	if res.Error != "" {
		return "", fmt.Errorf("%s", res.Error)
	}
	schema := res.OutputSchema()
	var cells [][]any
	st := res.Stream()
	if st == nil {
		rows, rerr := res.Rows()
		if rerr != nil {
			return "", rerr
		}
		for _, r := range rows {
			row := make([]any, len(res.Columns))
			for j, c := range res.Columns {
				row[j] = r[c]
			}
			cells = append(cells, row)
		}
	} else {
		defer st.Close()
		for {
			bb, berr := st.Next(ctx)
			if berr != nil {
				return "", berr
			}
			if bb == nil {
				break
			}
			cells = append(cells, bb.ToRowValues()...)
		}
	}
	return ssRender(schema, cells, ordered), nil
}

func ssArms(t *testing.T, ctx context.Context) []ssArm {
	t.Helper()
	single := ssStandalone(t, ctx, 0)
	spilled := ssStandalone(t, ctx, 512*1024)
	stand := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, ssTables())
		if wcfg != nil {
			return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
		}
		return tmdCoordinator(t, ctx, infra, opts...)
	}
	coord := stand(nil)
	coordB := stand(nil, func(c *Config) { c.BroadcastBytesOverride = 1 })
	coordM := stand(func(w *worker.Config) { w.MorselWorkers = 4 })
	s := func(db *wadjet.DB) func(string, bool) (string, error) {
		return func(sql string, o bool) (string, error) { return ssRunSingle(ctx, db, sql, o) }
	}
	d := func(c *Coordinator) func(string, bool) (string, error) {
		return func(sql string, o bool) (string, error) { return ssRunDAG(ctx, c, sql, o) }
	}
	return []ssArm{
		{"single", s(single)}, {"spilled512k", s(spilled)},
		{"dag", d(coord)}, {"dag-shuffled", d(coordB)}, {"dag-morsel4", d(coordM)},
	}
}

// ssPGAnswers reads PostgreSQL 17.11's answer for every cell
// (testdata/arc_ss_scalar_subquery_type_pg17.tsv).
func ssPGAnswers(t *testing.T) map[string]string {
	t.Helper()
	f, err := os.Open("testdata/arc_ss_scalar_subquery_type_pg17.tsv")
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
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestArcSSGenerate dumps the cells as name<TAB>ordered<TAB>sql for the oracle
// run (SS_GEN=<path>), and the PostgreSQL fixture beside it (<path>.fixture.sql).
func TestArcSSGenerate(t *testing.T) {
	path := os.Getenv("SS_GEN")
	if path == "" {
		t.Skip("SS_GEN unset")
	}
	var b strings.Builder
	for _, c := range ssCells() {
		fmt.Fprintf(&b, "%s\t%t\t%s\n", c.name, c.ordered, c.sql)
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".fixture.sql", []byte(ssPGFixture()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// ssKeep is one cell whose answer here is a CATALOGUED divergence or a
// recorded filing candidate outside this seam, asserted as it stands so a
// change FAILS and is re-measured (testdata/arc_ss_scalar_subquery_type_kept.tsv:
// the arms it holds on — all five, or the three stage-DAG arms — this
// engine's answer, and why).
type ssKeep struct {
	dagOnly   bool
	want, why string
}

func ssKept(t *testing.T) map[string]ssKeep {
	t.Helper()
	f, err := os.Open("testdata/arc_ss_scalar_subquery_type_kept.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string]ssKeep{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "\t", 4)
		if len(parts) != 4 || (parts[1] != "all" && parts[1] != "dag") {
			t.Fatalf("malformed kept line %q", line)
		}
		out[parts[0]] = ssKeep{dagOnly: parts[1] == "dag", want: parts[2], why: parts[3]}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestArcSSScalarSubqueryTypedOperandEveryArm is the seam table on five arms.
func TestArcSSScalarSubqueryTypedOperandEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the scalar-subquery operand table")
	}
	answers := ssPGAnswers(t)
	cells := ssCells()
	for _, c := range cells {
		if _, ok := answers[c.name]; !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", c.name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	t.Cleanup(cancel)
	kept := ssKept(t)
	for name := range kept {
		if _, ok := answers[name]; !ok {
			t.Fatalf("kept cell %s is not in the table", name)
		}
	}
	arms := ssArms(t, ctx)
	var dump *os.File
	if p := os.Getenv("SS_DUMP"); p != "" {
		f, err := os.Create(p)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		dump = f
	}
	var dumpMu sync.Mutex
	for _, tc := range cells {
		pgWant := answers[tc.name]
		k, isKept := kept[tc.name]
		t.Run(tc.name, func(t *testing.T) {
			got := make([]string, len(arms))
			var wg sync.WaitGroup
			for i, arm := range arms {
				wg.Add(1)
				go func() {
					defer wg.Done()
					res, err := arm.run(tc.sql, tc.ordered)
					if err != nil {
						res = "ERR " + sqlerr.StateOf(err) + " " + strings.ReplaceAll(err.Error(), "\n", " ")
					}
					got[i] = res
				}()
			}
			wg.Wait()
			if dump != nil {
				dumpMu.Lock()
				for i, arm := range arms {
					fmt.Fprintf(dump, "%s\t%s\t%s\n", tc.name, arm.name, got[i])
				}
				dumpMu.Unlock()
			}
			for i, arm := range arms {
				want, why := pgWant, "PostgreSQL 17.11"
				if isKept && (!k.dagOnly || strings.HasPrefix(arm.name, "dag")) {
					want, why = k.want, "kept: "+k.why
				}
				if !ssMatches(got[i], want) {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (%s)", tc.sql, arm.name, got[i], want, why)
				}
			}
		})
	}
}

// ssMatches compares an arm's answer with the wanted one: a refusal by its
// SQLSTATE and message fragment, a result by its rendering.
func ssMatches(got, want string) bool {
	if rest, ok := strings.CutPrefix(want, "ERR "); ok {
		state, msg, _ := strings.Cut(rest, " ")
		g, ok := strings.CutPrefix(got, "ERR ")
		if !ok {
			return false
		}
		gs, gm, _ := strings.Cut(g, " ")
		return gs == state && strings.Contains(gm, msg)
	}
	return strings.TrimSpace(got) == strings.TrimSpace(want)
}
