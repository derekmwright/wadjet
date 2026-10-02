// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// A SCALAR RENDERER WRITES A VALUE AS ITS DECLARED TYPE'S TEXT (#1474 #1466
// #1467 #1481). The seam is the renderer that turns a typed value into text:
// json_build_object (a value and a key position), format (%s, width, %L,
// positional), regexp_replace (its flags, groups, \& and empty matches) and,
// as controls, concat and CAST(… AS VARCHAR). This table is that seam
// enumerated once — renderer × value kind × consumer (projection, WHERE,
// GROUP BY, ORDER BY) — on five arms against PostgreSQL 17.11's rows.
//
// The fixture discriminates: a DATE / TIMESTAMP whose day count or epoch
// milliseconds differ from its text (1000-01-01, 9999-12-31, 1969-12-31,
// .001), doubles whose Go spelling differs from float8out (6.375 beside
// 1e+21, NaN, -0), text with quotes, a backslash, a newline, `<>&` and
// non-ASCII letters, and repeated matches for regexp_replace.
//
//	rn_t: id | i int | b bigint | f double | n numeric(10,2) | s text | o bool |
//	      d date | ts timestamp | u uuid | a int[] | at text[] | bt bytea |
//	      r ROW(f1 int, f2 text)

type rnRow struct {
	i, b, f, n, s, o, d, ts, u, a, at, by, r any
}

var rnRows = []rnRow{
	{int32(3), int64(30), 6.375, 2.25, "q\"b\\s\nl<>&é", true, "2024-03-04", "2024-03-04 12:00:00.001",
		"00000000-0000-4000-8000-000000000001", []any{int32(1), int32(2)}, []any{"a b", nil, `c"d`}, []byte{0x00, 0xff}, map[string]any{"f1": int32(1), "f2": "x y"}},
	{int32(-7), int64(-70), 1e21, -3.5, "Hello", false, "1970-01-01", "1970-01-01 00:00:00",
		"00000000-0000-4000-8000-000000000002", []any{int32(0)}, []any{"x"}, []byte("A"), map[string]any{"f1": int32(-2), "f2": `q"\`}},
	{int32(5), int64(9000000000), math.NaN(), 10.0, "z0z0", true, "9999-12-31", "9999-12-31 23:59:59.999",
		"00000000-0000-4000-8000-000000000003", []any{int32(4), int32(5), int32(6)}, []any{}, []byte{}, map[string]any{"f1": nil, "f2": ""}},
	{int32(0), int64(0), math.Copysign(0, -1), 0.0, "", false, "1000-01-01", "1000-01-01 00:00:00",
		"00000000-0000-4000-8000-000000000004", []any{int32(7)}, []any{"NULL"}, []byte{0x5c}, map[string]any{"f1": int32(0), "f2": nil}},
	{int32(1), int64(1), 100.125, 0.01, "ÀÉ x", true, "1969-12-31", "1969-12-31 23:59:59.999",
		"00000000-0000-4000-8000-000000000005", []any{int32(8)}, []any{"é"}, []byte{0x01}, map[string]any{"f1": int32(7), "f2": "é,(x)"}},
	{nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
}

func rnTables() []tmdTable {
	intElem := &parquet.Column{Name: "element", Type: parquet.TypeInt32, Nullable: true}
	textElem := &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}
	schema := parquet.Schema{Columns: []parquet.Column{
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
		{Name: "a", Type: parquet.TypeArray, Nullable: true, ElementType: intElem},
		{Name: "at", Type: parquet.TypeArray, Nullable: true, ElementType: textElem},
		{Name: "bt", Type: parquet.TypeBytes, Nullable: true},
		{Name: "r", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
			{Name: "f1", Type: parquet.TypeInt32, Nullable: true},
			{Name: "f2", Type: parquet.TypeString, Nullable: true},
		}},
	}}
	var rows []map[string]any
	for k, r := range rnRows {
		var ts any
		if r.ts != nil {
			ts = ssMS(r.ts.(string))
		}
		rows = append(rows, map[string]any{"id": int64(k + 1), "i": r.i, "b": r.b, "f": r.f, "n": r.n,
			"s": r.s, "o": r.o, "d": r.d, "ts": ts, "u": r.u, "a": r.a, "at": r.at, "bt": r.by, "r": r.r})
	}
	return []tmdTable{{"rn_t", schema, rows}}
}

// rnPGFixture is the same fixture as PostgreSQL DDL.
func rnPGFixture() []string {
	out := []string{
		"DROP TABLE IF EXISTS rn_t",
		"DROP TYPE IF EXISTS rn_r",
		"CREATE TYPE rn_r AS (f1 integer, f2 text)",
		"CREATE TABLE rn_t (id bigint, i integer, b bigint, f double precision, n numeric(10,2), s text, o boolean, d date, ts timestamp, u uuid, a integer[], at text[], bt bytea, r rn_r)",
	}
	lit := func(v any) string {
		switch x := v.(type) {
		case nil:
			return "NULL"
		case string:
			return "E'" + strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(x, `\`, `\\`), "'", "''"), "\n", `\n`) + "'"
		case float64:
			switch {
			case math.IsNaN(x):
				return "'NaN'::float8"
			case x == 0 && math.Signbit(x):
				return "'-0'::float8"
			}
			return fmt.Sprintf("%v::float8", x)
		case []byte:
			return fmt.Sprintf(`'\x%x'::bytea`, x)
		}
		return fmt.Sprint(v)
	}
	arr := func(v any, typ string) string {
		x, ok := v.([]any)
		if !ok {
			return "NULL"
		}
		parts := make([]string, len(x))
		for i, e := range x {
			if s, ok := e.(string); ok {
				parts[i] = "'" + strings.ReplaceAll(s, "'", "''") + "'"
				continue
			}
			if e == nil {
				parts[i] = "NULL"
				continue
			}
			parts[i] = fmt.Sprint(e)
		}
		return "ARRAY[" + strings.Join(parts, ",") + "]::" + typ + "[]"
	}
	row := func(v any) string {
		m, ok := v.(map[string]any)
		if !ok {
			return "NULL"
		}
		return "ROW(" + lit(m["f1"]) + ", " + lit(m["f2"]) + ")::rn_r"
	}
	for k, r := range rnRows {
		out = append(out, fmt.Sprintf("INSERT INTO rn_t VALUES (%d, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
			k+1, lit(r.i), lit(r.b), lit(r.f), lit(r.n), lit(r.s), lit(r.o), lit(r.d), lit(r.ts), lit(r.u),
			arr(r.a, "integer"), arr(r.at, "text"), lit(r.by), row(r.r)))
	}
	return out
}

// rnKind is one value kind: the expression that produces it over rn_t t.
type rnKind struct{ name, expr string }

func rnKinds() []rnKind {
	return []rnKind{
		{"int", "t.i"}, {"bigint", "t.b"}, {"double", "t.f"}, {"numeric", "t.n"}, {"text", "t.s"},
		{"bool", "t.o"}, {"date", "t.d"}, {"timestamp", "t.ts"}, {"uuid", "t.u"},
		{"intArr", "t.a"}, {"textArr", "t.at"}, {"bytea", "t.bt"}, {"row", "t.r"},
		{"numArr", "ARRAY[t.n, 1]"}, {"dateArr", "ARRAY[t.d, t.d + 1]"}, {"tsArr", "ARRAY[t.ts, t.ts + interval '1 hour']"},
		{"decExpr", "t.i * t.n"}, {"dblExpr", "t.f * 2"}, {"dateExpr", "t.d + 1"},
		{"tsExpr", "t.ts + interval '1 hour'"}, {"dateCast", "CAST(t.ts AS DATE)"},
		{"subqDate", "(SELECT x.d FROM rn_t x WHERE x.id = t.id)"},
		{"subqNum", "(SELECT max(x.n) FROM rn_t x)"},
		{"null", "NULL"},
	}
}

// rnRenderer is one renderer as a template over the value expression (%[1]s).
type rnRenderer struct {
	name, tmpl string
	key        bool // a json key: only the scalar kinds
}

func rnRenderers() []rnRenderer {
	return []rnRenderer{
		{"json", "json_build_object('v', %[1]s)", false},
		{"jsonKey", "json_build_object(%[1]s, 1)", true},
		{"jsonNest", "json_build_object('o', json_build_object('v', %[1]s, 'w', t.id))", false},
		{"fmtS", "format('[%%s]', %[1]s)", false},
		{"fmtWidth", "format('[%%14s|%%-14s]', %[1]s, %[1]s)", false},
		{"fmtPosL", "format('%%1$s|%%1$L|%%2$s', %[1]s, t.id)", false},
		{"concat", "concat('[', %[1]s, ']')", false},
		{"castVarchar", "CAST(%[1]s AS VARCHAR)", false},
	}
}

// rnRegexpCells are regexp_replace's flags × sources (and, last, a json
// value's origins): the first match without
// 'g', every match with it; 'i' and its last-wins pair 'c'; 'q' literal;
// groups and \& in the replacement; an empty match; a multibyte source; a
// `.` across a newline; the flags this engine refuses (n, p, w, x, b, e) and
// PostgreSQL's own refusals (an unknown flag, a malformed pattern).
func rnRegexpCells() []rnKind {
	return []rnKind{
		{"rx/zeroFirst", "regexp_replace(CAST(t.a[1] * t.n AS TEXT), '0', 'z')"},
		{"rx/zeroG", "regexp_replace(CAST(t.a[1] * t.n AS TEXT), '0', 'z', 'g')"},
		{"rx/digitFirst", "regexp_replace(CAST(t.b AS TEXT), '[0-9]', '#')"},
		{"rx/digitG", "regexp_replace(CAST(t.b AS TEXT), '[0-9]', '#', 'g')"},
		{"rx/dateG", "regexp_replace(CAST(t.d AS TEXT), '-', '/', 'g')"},
		{"rx/dateFirst", "regexp_replace(CAST(t.d AS TEXT), '-', '/')"},
		{"rx/tsFirst", "regexp_replace(CAST(t.ts AS TEXT), '[0-9]{2}', '##')"},
		{"rx/textI", "regexp_replace(t.s, 'l|z|h', '_', 'i')"},
		{"rx/textGI", "regexp_replace(t.s, 'l|z|h', '_', 'gi')"},
		{"rx/textIC", "regexp_replace(t.s, 'L|Z|H', '_', 'gic')"},
		{"rx/textCI", "regexp_replace(t.s, 'L|Z|H', '_', 'gci')"},
		{"rx/groups", "regexp_replace(t.s, '(.)(.)', '\\2\\1')"},
		{"rx/groupsG", "regexp_replace(t.s, '(.)(.)', '\\2\\1', 'g')"},
		{"rx/amp", "regexp_replace(t.s, '[a-z]+', '<\\&>', 'g')"},
		{"rx/missingGroup", "regexp_replace(t.s, '(z)', '[\\2\\0$1]')"},
		{"rx/emptyFirst", "regexp_replace(t.s, 'q*', '-')"},
		{"rx/emptyG", "regexp_replace(t.s, 'q*', '-', 'g')"},
		{"rx/multibyte", "regexp_replace(t.s, '[éÉ]', '?', 'g')"},
		{"rx/dotNewline", "regexp_replace(t.s, 'b.*l', '#')"},
		{"rx/literalQ", "regexp_replace(t.s, '<>', '[]', 'q')"},
		{"rx/literalQdot", "regexp_replace(CAST(t.n AS TEXT), '.', ',', 'q')"},
		{"rx/flagsEmpty", "regexp_replace(t.s, 'z', 'Z', '')"},
		{"rx/flagsNull", "regexp_replace(t.s, 'z', 'Z', NULL)"},
		{"rx/flagsST", "regexp_replace(t.s, 'z', 'Z', 'gst')"},
		{"rx/flagN", "regexp_replace(t.s, 'b.*l', '#', 'n')"},
		{"rx/flagP", "regexp_replace(t.s, 'b.*l', '#', 'p')"},
		{"rx/flagW", "regexp_replace(t.s, 'b.*l', '#', 'w')"},
		{"rx/flagX", "regexp_replace(t.s, 'l l', '#', 'x')"},
		{"rx/flagB", "regexp_replace(t.s, 'z+', '#', 'b')"},
		{"rx/flagE", "regexp_replace(t.s, 'z+', '#', 'e')"},
		{"rx/flagBad", "regexp_replace(t.s, 'z', 'Z', 'gz')"},
		{"rx/badPattern", "regexp_replace(t.s, '(', 'Z')"},
		// The pattern is an ARE, read as the `~` operators read it: the
		// longest match of an all-greedy RE, \b a backspace, a back
		// reference refused; the match-preference and case-folding residuals.
		{"rx/altLongest", "regexp_replace(t.s, 'l|l<', '#')"},
		{"rx/backspace", "regexp_replace(t.s, '\\b', '#', 'g')"},
		{"rx/wordY", "regexp_replace(t.s, '\\y', '#', 'g')"},
		{"rx/backrefPattern", "regexp_replace(t.s, '(l)\\1', '#')"},
		{"rx/lazyRE", "regexp_replace(t.s, 'x*?H*', '#')"},
		{"rx/posixCaptures", "regexp_replace('abcd' || t.s, '(a|ab)(c|bcd)(d*)', '[\\1|\\2|\\3]')"},
		{"rx/icaseNonASCII", "regexp_replace(t.s, 'é', '_', 'gi')"},
		// format's grammar (#1467) and the quote functions %I and %L share.
		{"fx/positional", "format('%2$s|%1$s|%s|%%', t.s, t.i)"},
		{"fx/starWidth", "format('[%*s][%-*s]', t.i, t.s, t.i, t.d)"},
		{"fx/starPosWidth", "format('[%3$*1$s]', 6, t.s, t.d)"},
		{"fx/identL", "format('%I|%L', COALESCE(t.s, 'Select'), t.s)"},
		{"fx/badType", "format('%d', t.i)"},
		{"fx/tooFew", "format('%s %s', t.i)"},
		{"fx/identNull", "format('%I', t.s)"},
		{"fx/quoteIdent", "quote_ident(COALESCE(t.s, 'select'))"},
		{"fx/quoteLiteral", "quote_literal(t.s)"},
		{"fx/quoteNullable", "quote_nullable(t.s)"},
		// A json value's ORIGIN (#1474): one its own expression produces
		// nests as an object; one that reaches the call declared text (a
		// scalar subquery's answer, a CTE's or a derived table's column, a
		// COALESCE with a text arm) is written as a JSON string (#1470).
		{"jx/direct", "json_build_object('o', json_build_object('a', t.d))"},
		{"jx/castJson", "json_build_object('o', CAST('{\"a\":1}' AS JSON))"},
		{"jx/coalesce", "json_build_object('o', COALESCE(json_build_object('a', t.i), NULL))"},
		{"jx/case", "json_build_object('o', CASE WHEN t.id < 3 THEN json_build_object('a', t.i) END)"},
		{"jx/corrSubq", "json_build_object('o', (SELECT json_build_object('a', x.i) FROM rn_t x WHERE x.id = t.id))"},
		{"jx/uncorrSubq", "json_build_object('o', (SELECT json_build_object('a', max(x.i)) FROM rn_t x))"},
		{"jx/cte", "json_build_object('o', (WITH c AS (SELECT json_build_object('a', 1) AS j) SELECT j FROM c))"},
		{"jx/coalesceText", "json_build_object('o', COALESCE(json_build_object('a', t.i), '{}'))"},
	}
}

type rnCell struct {
	name, sql string
	ordered   bool
}

// rnProjCells are the projection cells; every other consumer is derived from
// them (rnCells), the WHERE cell from PostgreSQL's answer at id = 1.
func rnProjCells() []rnCell {
	var out []rnCell
	for _, r := range rnRenderers() {
		for _, k := range rnKinds() {
			if r.key && (strings.HasSuffix(k.name, "Arr") || k.name == "row" || k.name == "null") {
				continue
			}
			expr := fmt.Sprintf(r.tmpl, k.expr)
			from := "rn_t t"
			if r.key {
				// A NULL key is 22004 for the whole statement.
				from = "(SELECT * FROM rn_t WHERE id < 6) t"
			}
			out = append(out, rnCell{r.name + "/" + k.name + "/proj", "SELECT t.id, " + expr + " AS v FROM " + from, false})
		}
	}
	for _, c := range rnRegexpCells() {
		out = append(out, rnCell{c.name + "/proj", "SELECT t.id, " + c.expr + " AS v FROM rn_t t", false})
	}
	return out
}

// rnCells is the whole table. answers holds PostgreSQL's projection answers:
// the WHERE cell compares the renderer against PostgreSQL's text at id = 1, so
// a renderer that writes another text answers no row there.
func rnCells(answers map[string]string) []rnCell {
	proj := rnProjCells()
	out := append([]rnCell(nil), proj...)
	for _, c := range proj {
		base := strings.TrimSuffix(c.name, "/proj")
		expr, from, _ := strings.Cut(strings.TrimPrefix(c.sql, "SELECT t.id, "), " AS v FROM ")
		if strings.HasPrefix(expr, "json_build_object") {
			// json has no equality operator and no md5 on PostgreSQL
			// (42883): its consumers read the object's text.
			expr = "CAST(" + expr + " AS TEXT)"
		}
		out = append(out,
			rnCell{base + "/group", "SELECT " + expr + " AS k, count(*) AS c FROM " + from + " GROUP BY 1", false},
			rnCell{base + "/order", "SELECT t.id FROM " + from + " ORDER BY md5(" + expr + "), t.id", true})
		if answers == nil {
			continue
		}
		if v, ok := rnRowOneValue(answers[c.name]); ok {
			pred := expr + " IS NULL"
			if v != "NULL" {
				pred = expr + " = '" + strings.ReplaceAll(rnUnescape(v), "'", "''") + "'"
			}
			out = append(out, rnCell{base + "/where", "SELECT t.id FROM " + from + " WHERE " + pred, false})
		}
	}
	return out
}

// rnRowOneValue is the v of row `1,v` in a projection answer.
func rnRowOneValue(ans string) (string, bool) {
	if ans == "" || strings.HasPrefix(ans, "ERR ") {
		return "", false
	}
	_, rows, ok := strings.Cut(ans, " ")
	if !ok {
		return "", false
	}
	for _, r := range strings.Split(rows, " | ") {
		if v, ok := strings.CutPrefix(r, "1,"); ok {
			return v, true
		}
	}
	return "", false
}

// rnEscape keeps a rendered value on one printable line of the answer file:
// a backslash, newline, tab and `|` escaped, and any other control byte or
// byte of invalid UTF-8 written \xNN.
func rnEscape(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); {
		r, n := utf8.DecodeRuneInString(s[i:])
		switch {
		case r == '\\':
			b.WriteString(`\\`)
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\t':
			b.WriteString(`\t`)
		case r == '|':
			b.WriteString(`\p`)
		case (r == utf8.RuneError && n == 1) || r < 0x20 || r == 0x7f:
			fmt.Fprintf(&b, `\x%02x`, s[i])
		default:
			b.WriteString(s[i : i+n])
		}
		i += n
	}
	return b.String()
}

func rnUnescape(s string) string {
	return strings.NewReplacer(`\\`, `\`, `\n`, "\n", `\t`, "\t", `\p`, "|").Replace(s)
}

// rnRender is a result as `rows=N r1 | r2`, sorted unless ordered; a value is
// its text (NULL for a NULL), escaped onto one line.
func rnRender(rows [][]string, ordered bool) string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		parts := make([]string, len(r))
		for i, v := range r {
			parts[i] = rnEscape(v)
		}
		out = append(out, strings.Join(parts, ","))
	}
	if !ordered {
		sort.Strings(out)
	}
	return fmt.Sprintf("rows=%d %s", len(out), strings.Join(out, " | "))
}

func rnCellText(cols []parquet.Column, cells [][]any) [][]string {
	out := make([][]string, len(cells))
	for i, r := range cells {
		out[i] = make([]string, len(r))
		for j, v := range r {
			t := parquet.TypeString
			if j < len(cols) {
				t = cols[j].Type
			}
			out[i][j] = ssFmt(v, t)
		}
	}
	return out
}

func rnRunSingle(ctx context.Context, db *wadjet.DB, sql string, ordered bool) (out string, err error) {
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
	return rnRender(rnCellText(cols, cells), ordered), nil
}

func rnRunDAG(ctx context.Context, coord *Coordinator, sql string, ordered bool) (out string, err error) {
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
	return rnRender(rnCellText(schema, cells), ordered), nil
}

func rnStandalone(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
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
	for _, tbl := range rnTables() {
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

func rnArms(t *testing.T, ctx context.Context) []ssArm {
	t.Helper()
	single := rnStandalone(t, ctx, 0)
	spilled := rnStandalone(t, ctx, 512*1024)
	stand := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, rnTables())
		if wcfg != nil {
			return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
		}
		return tmdCoordinator(t, ctx, infra, opts...)
	}
	coord := stand(nil)
	coordB := stand(nil, func(c *Config) { c.BroadcastBytesOverride = 1 })
	coordM := stand(func(w *worker.Config) { w.MorselWorkers = 4 })
	s := func(db *wadjet.DB) func(string, bool) (string, error) {
		return func(sql string, o bool) (string, error) { return rnRunSingle(ctx, db, sql, o) }
	}
	d := func(c *Coordinator) func(string, bool) (string, error) {
		return func(sql string, o bool) (string, error) { return rnRunDAG(ctx, c, sql, o) }
	}
	return []ssArm{
		{"single", s(single)}, {"spilled512k", s(spilled)},
		{"dag", d(coord)}, {"dag-shuffled", d(coordB)}, {"dag-morsel4", d(coordM)},
	}
}

const rnPGFile = "testdata/arc_rn_renderers_pg17.tsv"
const rnKeptFile = "testdata/arc_rn_renderers_kept.tsv"

func rnReadTSV(t *testing.T, path string, fields int) map[string][]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string][]string{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "\t", fields)
		if len(parts) != fields {
			t.Fatalf("malformed line in %s: %q", path, line)
		}
		out[parts[0]] = parts[1:]
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestArcRNMeasurePostgres measures every cell on PostgreSQL 17.11
// (RN_PG_DSN=postgres://…) and writes the answer file: the projection cells
// first, then the cells derived from their answers.
func TestArcRNMeasurePostgres(t *testing.T) {
	dsn := os.Getenv("RN_PG_DSN")
	if dsn == "" {
		t.Skip("RN_PG_DSN unset")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	for _, s := range append([]string{"SET statement_timeout = '30s'"}, rnPGFixture()...) {
		if _, err := conn.Exec(ctx, s, pgx.QueryExecModeSimpleProtocol); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	run := func(c rnCell) string {
		rows, err := conn.Query(ctx, c.sql, pgx.QueryExecModeSimpleProtocol)
		if err != nil {
			return "ERR " + sqlerr.StateOf(err) + " " + err.Error()
		}
		var out [][]string
		for rows.Next() {
			raw := rows.RawValues()
			r := make([]string, len(raw))
			for i, v := range raw {
				if v == nil {
					r[i] = "NULL"
				} else {
					r[i] = string(v)
				}
			}
			out = append(out, r)
		}
		if err := rows.Err(); err != nil {
			return "ERR " + rnPGState(err) + " " + strings.ReplaceAll(err.Error(), "\n", " ")
		}
		return rnRender(out, c.ordered)
	}
	answers := map[string]string{}
	for _, c := range rnProjCells() {
		answers[c.name] = run(c)
	}
	var b strings.Builder
	b.WriteString("# arc RN: PostgreSQL 17.11's answer for every cell (TestArcRNMeasurePostgres)\n")
	for _, c := range rnCells(answers) {
		a, ok := answers[c.name]
		if !ok {
			a = run(c)
		}
		fmt.Fprintf(&b, "%s\t%s\n", c.name, a)
	}
	if err := os.WriteFile(rnPGFile, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

func rnPGState(err error) string {
	if s := sqlerr.StateOf(err); s != "" {
		return s
	}
	return "XX000"
}

// TestArcRNRendererTableEveryArm is the seam table on five arms.
func TestArcRNRendererTableEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the renderer table")
	}
	pg := rnReadTSV(t, rnPGFile, 2)
	answers := map[string]string{}
	for k, v := range pg {
		answers[k] = v[0]
	}
	cells := rnCells(answers)
	for _, c := range cells {
		if _, ok := answers[c.name]; !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", c.name)
		}
	}
	kept := rnKept(t)
	for name := range kept {
		if _, ok := answers[name]; !ok {
			t.Fatalf("kept cell %s is not in the table", name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Minute)
	t.Cleanup(cancel)
	arms := rnArms(t, ctx)
	var dump *os.File
	if p := os.Getenv("RN_DUMP"); p != "" {
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
		ks := kept[tc.name]
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
				dag := strings.HasPrefix(arm.name, "dag")
				for _, k := range ks {
					if k.scope == "all" || (k.scope == "dag") == dag {
						want, why = k.want, "kept: "+k.why
					}
				}
				if !rnMatches(got[i], want) {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (%s)", tc.sql, arm.name, got[i], want, why)
				}
			}
		})
	}
}

// rnKeep is one cell whose answer here is a CATALOGUED divergence or a
// recorded filing candidate, asserted as it stands so a change FAILS and is
// re-measured (testdata/arc_rn_renderers_kept.tsv: name, the arms it holds on
// — all, the two single-process arms (local) or the three stage-DAG arms
// (dag) — this engine's answer, and why).
type rnKeep struct{ scope, want, why string }

func rnKept(t *testing.T) map[string][]rnKeep {
	t.Helper()
	f, err := os.Open(rnKeptFile)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string][]rnKeep{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		p := strings.SplitN(line, "\t", 4)
		if len(p) != 4 || (p[1] != "all" && p[1] != "local" && p[1] != "dag") {
			t.Fatalf("malformed kept line %q", line)
		}
		out[p[0]] = append(out[p[0]], rnKeep{scope: p[1], want: p[2], why: p[3]})
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// rnMatches compares an arm's answer with the wanted one: a refusal by its
// SQLSTATE (PostgreSQL's message wording is not this engine's), a result by
// its rendering.
func rnMatches(got, want string) bool {
	if rest, ok := strings.CutPrefix(want, "ERR "); ok {
		state, _, _ := strings.Cut(rest, " ")
		g, ok := strings.CutPrefix(got, "ERR ")
		if !ok {
			return false
		}
		gs, _, _ := strings.Cut(g, " ")
		return gs == state
	}
	return strings.TrimSpace(got) == strings.TrimSpace(want)
}
