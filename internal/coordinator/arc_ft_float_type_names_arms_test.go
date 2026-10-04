// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// THE FLOATING-POINT TYPE NAMES MEAN WHAT POSTGRESQL'S MEAN, AT EVERY DOOR
// AND ON EVERY ARM (#1464, #1405).
//
// The coverage table is generated: every float spelling PostgreSQL 17.11
// documents (FLOAT, FLOAT(p) at both edges of both widths and past them,
// FLOAT4, FLOAT8, REAL, DOUBLE PRECISION, upper/lower/mixed case, the quoted
// catalog names "float4"/"float8", whitespace inside the parentheses) plus a
// DECIMAL control, at the sites {CREATE TABLE column, CAST, ::, CREATE TABLE
// AS from a CAST, the GROUP BY cast identity}, over values chosen so a float4
// and a float8 reading DIFFER (674999997 is past float4's 24-bit mantissa;
// 0.1 is not exact in either width and compares differently against the
// numeric 0.1; 1e39 is past float4's range and inside float8's), read back
// by the consumers {stored value, declared type and label, arithmetic, an
// equality and an ordering comparison, SUM, MIN/MAX, GROUP BY, ORDER BY} on
// the five arms. testdata/arc_ft_float_type_names_pg17.tsv is PostgreSQL
// 17.11's answer for every cell (TestArcFTMeasure regenerates it).
//
// At v0.25.3 a column declared FLOAT was float4 (674999997 stored as
// 675000000, 1e39 refused 22003), and REAL, FLOAT4, FLOAT8 and DOUBLE
// PRECISION could not declare a column at all (42704; DOUBLE PRECISION
// 42601 between column definitions), while CAST read every spelling right.

// ftName is one type spelling; twin, for a spelling PostgreSQL refuses and
// this engine keeps (a kept superset), is the PostgreSQL spelling whose
// answers it must give.
type ftName struct{ key, spell, twin string }

func ftNames() []ftName {
	return []ftName{
		{"float", "FLOAT", ""},
		{"floatLower", "float", ""},
		{"floatMixed", "Float", ""},
		{"float1", "FLOAT(1)", ""},
		{"float24", "FLOAT(24)", ""},
		{"float25", "FLOAT(25)", ""},
		{"float53", "FLOAT(53)", ""},
		{"floatSpaced", "float ( 25 )", ""},
		{"float4", "FLOAT4", ""},
		{"float4Quoted", `"float4"`, ""},
		{"float8", "FLOAT8", ""},
		{"float8Lower", "float8", ""},
		{"float8Quoted", `"float8"`, ""},
		{"real", "REAL", ""},
		{"realLower", "real", ""},
		{"dp", "DOUBLE PRECISION", ""},
		{"dpLower", "double  precision", ""},
		{"decimal", "DECIMAL(12,2)", ""},
		// Kept supersets: PostgreSQL 17.11 answers 42704 `type "…" does not
		// exist` (a quoted name resolves against pg_type.typname, and DOUBLE
		// is no PostgreSQL type). This engine resolves type names by text, so
		// the quoted keywords read as their unquoted twins, and DOUBLE has
		// always been its float8 spelling.
		{"floatQuoted", `"float"`, "float"},
		{"realQuoted", `"real"`, "real"},
		{"double", "DOUBLE", "dp"},
	}
}

// ftRefusedNames are spellings PostgreSQL refuses with the same SQLSTATE at
// every site; the engine must too.
func ftRefusedNames() []ftName {
	return []ftName{{"float0", "FLOAT(0)", ""}, {"float54", "FLOAT(54)", ""}}
}

// ftCell is one statement. ddl cells run once, in order, on the writer (the
// statements that build the fixture are themselves cells); read cells run on
// every arm.
type ftCell struct {
	name, sql string
	ddl       bool
	twin      string // the cell whose PostgreSQL answer this one must give
}

func ftTable(n ftName) string { return "ft_" + strings.ToLower(n.key) }

func ftCells() []ftCell {
	var out []ftCell
	twinOf := func(n ftName, suffix string) string {
		if n.twin == "" {
			return ""
		}
		return n.twin + suffix
	}
	for _, n := range ftNames() {
		t := ftTable(n)
		vals := "(1, 674999997), (2, 0.1), (3, 1e38), (4, CAST('-0' AS DOUBLE PRECISION)), " +
			"(5, CAST('NaN' AS DOUBLE PRECISION)), (6, CAST('Infinity' AS DOUBLE PRECISION)), (7, NULL)"
		if n.key == "decimal" {
			vals = "(1, 674999997), (2, 0.1), (7, NULL)"
		}
		out = append(out,
			ftCell{name: "ddl/" + n.key + "/create", sql: "CREATE TABLE " + t + " (id BIGINT, c " + n.spell + ")", ddl: true, twin: twinOf(n, "/create")},
			ftCell{name: "ddl/" + n.key + "/insert", sql: "INSERT INTO " + t + " VALUES " + vals, ddl: true, twin: twinOf(n, "/insert")},
		)
		if n.key != "decimal" {
			out = append(out, ftCell{name: "ddl/" + n.key + "/insert1e39", sql: "INSERT INTO " + t + " VALUES (8, 1e39)", ddl: true, twin: twinOf(n, "/insert1e39")})
		}
		out = append(out, ftCell{name: "ddl/" + n.key + "/ctas", sql: "CREATE TABLE " + t + "_ctas AS SELECT CAST(674999997 AS " + n.spell + ") AS c", ddl: true, twin: twinOf(n, "/ctas")})
	}
	for _, n := range ftRefusedNames() {
		out = append(out,
			ftCell{name: "ddl/" + n.key + "/create", sql: "CREATE TABLE " + ftTable(n) + " (id BIGINT, c " + n.spell + ")", ddl: true},
			ftCell{name: "cast/" + n.key + "/cast", sql: "SELECT CAST(1 AS " + n.spell + ")"},
			ftCell{name: "cast/" + n.key + "/colon", sql: "SELECT 1::" + n.spell},
		)
	}
	for _, n := range ftNames() {
		t := ftTable(n)
		reads := []struct{ name, sql string }{
			{"stored", "SELECT id, c FROM " + t + " ORDER BY id"},
			{"arith", "SELECT id, c + 1, c * 2, -c FROM " + t + " ORDER BY id"},
			{"eqInt", "SELECT id FROM " + t + " WHERE c = 674999997 ORDER BY id"},
			{"gtNumeric", "SELECT id FROM " + t + " WHERE c > 0.1 ORDER BY id"},
			{"sum", "SELECT sum(c) FROM " + t + " WHERE id IN (1, 2)"},
			{"minMax", "SELECT min(c), max(c) FROM " + t},
			{"groupBy", "SELECT c, count(*) FROM " + t + " GROUP BY c ORDER BY c"},
			{"orderBy", "SELECT id FROM " + t + " ORDER BY c, id"},
			{"orderByDesc", "SELECT id FROM " + t + " ORDER BY c DESC, id"},
			{"ctasRead", "SELECT c FROM " + t + "_ctas"},
			{"cast", "SELECT CAST(674999997 AS " + n.spell + "), CAST(0.1 AS " + n.spell + ")"},
			{"castLabel", "SELECT CAST(1 AS " + n.spell + ")"},
			{"colon", "SELECT 674999997::" + n.spell},
			{"cast1e39", "SELECT CAST(1e39 AS " + n.spell + ")"},
			{"castColumn", "SELECT CAST(id * 674999997 AS " + n.spell + ") FROM " + ftTable(ftNames()[0]) + " WHERE id = 1"},
		}
		if n.key == "decimal" {
			reads = reads[:len(reads)-2] // 1e39 is NUMERIC's own territory
		}
		for _, r := range reads {
			out = append(out, ftCell{name: "read/" + n.key + "/" + r.name, sql: r.sql, twin: twinOf(n, "/"+r.name)})
		}
	}
	// The GROUP BY identity of a cast: two spellings of one type are one
	// expression, two types are not (42803).
	for _, p := range []struct{ name, sel, grp string }{
		{"floatFloat8", "FLOAT", "FLOAT8"},
		{"float30Dp", "FLOAT(30)", "DOUBLE PRECISION"},
		{"float1Real", "FLOAT(1)", "REAL"},
		{"float24Float4", "FLOAT(24)", "FLOAT4"},
		{"float1Float", "FLOAT(1)", "FLOAT"},
		{"realFloat8", "REAL", "FLOAT8"},
	} {
		out = append(out, ftCell{name: "identity/" + p.name,
			sql: "SELECT CAST(id AS " + p.sel + ") FROM ft_float GROUP BY CAST(id AS " + p.grp + ") ORDER BY 1"})
	}
	for i := range out {
		if out[i].twin != "" {
			out[i].twin = strings.SplitN(out[i].name, "/", 2)[0] + "/" + out[i].twin
		}
	}
	return out
}

// ftTypeName is the PostgreSQL name of a declared column type.
func ftTypeName(c parquet.Column) string {
	switch c.Type {
	case parquet.TypeFloat32:
		return "real"
	case parquet.TypeFloat64:
		return "double precision"
	case parquet.TypeInt64:
		return "bigint"
	case parquet.TypeInt32:
		return "integer"
	case parquet.TypeDecimal:
		return "numeric"
	case parquet.TypeBool:
		return "boolean"
	case parquet.TypeString:
		return "text"
	}
	return c.Type.String()
}

// ftPGTypeName is the same name for a PostgreSQL result OID.
func ftPGTypeName(oid uint32) string {
	switch oid {
	case 700:
		return "real"
	case 701:
		return "double precision"
	case 20:
		return "bigint"
	case 23:
		return "integer"
	case 1700:
		return "numeric"
	case 16:
		return "boolean"
	case 25:
		return "text"
	}
	return fmt.Sprintf("oid%d", oid)
}

// ftFloatText is PostgreSQL's float4out / float8out: the shortest decimal
// that reads back to the same value, in exponent form when the decimal
// exponent is below -4 or at least the type's digit count (FLT_DIG 6,
// DBL_DIG 15).
func ftFloatText(f float64, bits int) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	case f == 0 && math.Signbit(f):
		return "-0"
	case f == 0:
		return "0"
	}
	e := strconv.FormatFloat(f, 'e', -1, bits)
	exp, _ := strconv.Atoi(e[strings.IndexByte(e, 'e')+1:])
	digits := 15
	if bits == 32 {
		digits = 6
	}
	if exp < -4 || exp >= digits {
		return e
	}
	return strconv.FormatFloat(f, 'f', -1, bits)
}

// ftValueText renders one engine cell as PostgreSQL's text for its declared
// type, so a float4 holding 675000000 renders 6.75e+08 exactly as a real does
// on PostgreSQL, and the same bits declared double precision render
// 675000000 — the declaration is part of the answer.
func ftValueText(v any, c parquet.Column) string {
	if v == nil {
		return "NULL"
	}
	var f float64
	isFloat := true
	switch x := v.(type) {
	case float32:
		f = float64(x)
	case float64:
		f = x
	default:
		isFloat = false
	}
	if isFloat {
		switch c.Type {
		case parquet.TypeFloat32:
			return ftFloatText(float64(float32(f)), 32)
		case parquet.TypeFloat64:
			return ftFloatText(f, 64)
		}
	}
	return fmt.Sprintf("%v", v)
}

// ftRender is the answer form both sides are reduced to: the header
// (name:type per column) and the rows in the order the statement returned
// them, or ERR <SQLSTATE>.
func ftRender(header []string, rows []string) string {
	if len(rows) == 0 {
		// PostgreSQL's extended-protocol reader reports no field
		// descriptions for an empty result; zero rows is the whole answer.
		return "(0 rows)"
	}
	return strings.Join(header, ",") + " | " + strings.Join(rows, "; ")
}

func ftEngineRender(cols []parquet.Column, cells [][]any) string {
	header := make([]string, len(cols))
	for i, c := range cols {
		header[i] = c.Name + ":" + ftTypeName(c)
	}
	rows := make([]string, len(cells))
	for i, r := range cells {
		f := make([]string, len(r))
		for j, v := range r {
			c := parquet.Column{}
			if j < len(cols) {
				c = cols[j]
			}
			f[j] = ftValueText(v, c)
		}
		rows[i] = strings.Join(f, ",")
	}
	return ftRender(header, rows)
}

func ftErr(err error) string { return "ERR " + sqlerr.StateOf(err) }

func ftRunSingle(ctx context.Context, db *wadjet.DB, sql string) (out string) {
	defer func() {
		if r := recover(); r != nil {
			out = fmt.Sprintf("PANIC %v", r)
		}
	}()
	res, err := db.Query(ctx, sql)
	if err != nil {
		return ftErr(err)
	}
	cols := make([]parquet.Column, len(res.ColumnMetas))
	for i, m := range res.ColumnMetas {
		cols[i] = parquet.Column{Name: m.Name, Type: m.TypeID, Precision: m.Precision, Scale: m.Scale}
	}
	cells := make([][]any, len(res.Rows))
	for i := range res.Rows {
		cells[i] = res.Cells(i)
	}
	return ftEngineRender(cols, cells)
}

func ftRunDAG(ctx context.Context, c *Coordinator, sql string) (out string) {
	defer func() {
		if r := recover(); r != nil {
			out = fmt.Sprintf("PANIC %v", r)
		}
	}()
	res, err := c.ExecuteSQL(ctx, sql)
	if err != nil {
		return ftErr(err)
	}
	if res.Error != "" {
		return "ERR " + res.Error
	}
	cols := append([]parquet.Column(nil), res.OutputSchema()...)
	var cells [][]any
	if st := res.Stream(); st != nil {
		defer st.Close()
		for {
			bb, berr := st.Next(ctx)
			if berr != nil {
				return ftErr(berr)
			}
			if bb == nil {
				break
			}
			cells = append(cells, bb.ToRowValues()...)
		}
	} else {
		rows, rerr := res.Rows()
		if rerr != nil {
			return ftErr(rerr)
		}
		for _, r := range rows {
			row := make([]any, len(cols))
			for j, c := range cols {
				row[j] = r[c.Name]
			}
			cells = append(cells, row)
		}
	}
	return ftEngineRender(cols, cells)
}

// ftDDLRender is a write's outcome: ok, or its refusal.
func ftDDLRender(err error) string {
	if err != nil {
		return ftErr(err)
	}
	return "ok"
}

const ftAnswersPath = "testdata/arc_ft_float_type_names_pg17.tsv"

func ftPGAnswers(t *testing.T) map[string]string {
	t.Helper()
	f, err := os.Open(ftAnswersPath)
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

// TestArcFTMeasure runs every cell against PostgreSQL (FT_PG_DSN, e.g.
// postgres://wadjet:wadjet@127.0.0.1:57995/wadjet_oracle) and rewrites the
// answer file; skipped otherwise.
func TestArcFTMeasure(t *testing.T) {
	dsn := os.Getenv("FT_PG_DSN")
	if dsn == "" {
		t.Skip("FT_PG_DSN unset")
	}
	ctx := context.Background()
	conn, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	run := func(sql string) *pgconn.Result {
		return conn.ExecParams(ctx, sql, nil, nil, nil, nil).Read()
	}
	if r := run("SET statement_timeout = '30s'"); r.Err != nil {
		t.Fatal(r.Err)
	}
	for _, n := range append(ftNames(), ftRefusedNames()...) {
		run("DROP TABLE IF EXISTS " + ftTable(n))
		run("DROP TABLE IF EXISTS " + ftTable(n) + "_ctas")
	}
	var b strings.Builder
	b.WriteString("# PostgreSQL 17.11 answers for arc_ft_float_type_names_arms_test.go (TestArcFTMeasure).\n")
	for _, c := range ftCells() {
		r := run(c.sql)
		var ans string
		switch {
		case r.Err != nil:
			pe, ok := r.Err.(*pgconn.PgError)
			if !ok {
				t.Fatalf("%s: %v", c.name, r.Err)
			}
			ans = "ERR " + pe.Code
		case c.ddl:
			ans = "ok"
		default:
			header := make([]string, len(r.FieldDescriptions))
			for i, fd := range r.FieldDescriptions {
				header[i] = fd.Name + ":" + ftPGTypeName(fd.DataTypeOID)
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
			ans = ftRender(header, rows)
		}
		b.WriteString(c.name + "\t" + ans + "\n")
	}
	if err := os.WriteFile(ftAnswersPath, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// ftWant is a cell's expected answer: PostgreSQL's own, or — for a kept
// superset's cell — its twin's.
func ftWant(t *testing.T, answers map[string]string, c ftCell) string {
	t.Helper()
	key := c.name
	if c.twin != "" {
		key = c.twin
	}
	want, ok := answers[key]
	if !ok {
		t.Fatalf("cell %s has no PostgreSQL answer under %s: re-measure the table", c.name, key)
	}
	return want
}

// ftArmsFixture is the writer (single-process, over the shared catalog) and
// the five arms reading what it wrote.
func ftArmsFixture(t *testing.T, ctx context.Context) (*wadjet.DB, []struct {
	name string
	run  func(string) string
}) {
	t.Helper()
	infra := tmdInfra(t, ctx)
	open := func(budget int64) *wadjet.DB {
		t.Helper()
		cfg := wadjet.Config{MetaKV: infra.kv, Store: infra.store, Bucket: "test", MemoryBudget: budget}
		if budget > 0 {
			cfg.SpillDir = t.TempDir()
		}
		db, err := wadjet.Open(ctx, cfg)
		if err != nil {
			t.Fatalf("open over the shared catalog: %v", err)
		}
		t.Cleanup(func() { db.Close() })
		return db
	}
	writer := open(0)
	spilled := open(512 * 1024)
	coord := tmdCoordinator(t, ctx, infra)
	coordB := tmdCoordinator(t, ctx, infra, func(c *Config) { c.BroadcastBytesOverride = 1 })
	coordM := tmdCoordinatorWithWorkers(t, ctx, infra, func(w *worker.Config) { w.MorselWorkers = 4 })
	return writer, []struct {
		name string
		run  func(string) string
	}{
		{"single", func(sql string) string { return ftRunSingle(ctx, writer, sql) }},
		{"spilled512k", func(sql string) string {
			restoreDrain := exec.ForceAggDrainEvery(1)
			restoreRuns := exec.ForceSmallSpillRuns(2)
			defer func() { restoreRuns(); exec.ForceAggDrainEvery(restoreDrain) }()
			return ftRunSingle(ctx, spilled, sql)
		}},
		{"dag", func(sql string) string { return ftRunDAG(ctx, coord, sql) }},
		{"dag-shuffled", func(sql string) string { return ftRunDAG(ctx, coordB, sql) }},
		{"dag-morsel4", func(sql string) string { return ftRunDAG(ctx, coordM, sql) }},
	}
}

// ftKnown is the pinned answer of a cell whose difference from PostgreSQL is
// OUTSIDE this arc's seam. A pin FAILS the moment the arm starts agreeing —
// delete it then. "" = no pin. The two pins this table carried (max over a
// column holding NaN on the single-process arms; the -0 group key on every
// arm) were deleted by arc FO when both started agreeing with PostgreSQL
// (#1488, #1489; arc_fo_float_total_order_arms_test.go).
func ftKnown(cell, arm, want string) string {
	return ""
}

func TestArcFTFloatTypeNamesEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the float type-name table")
	}
	answers := ftPGAnswers(t)
	cells := ftCells()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	writer, arms := ftArmsFixture(t, ctx)

	discriminating, pinned := 0, 0
	for _, c := range cells {
		want := ftWant(t, answers, c)
		if c.ddl {
			_, err := writer.Query(ctx, c.sql)
			if got := ftDDLRender(err); got != want {
				t.Errorf("%s\n  %s\n  writer got  %s (%v)\n  want        %s (PostgreSQL 17.11)", c.name, c.sql, got, err, want)
			}
			continue
		}
		if strings.Contains(want, "674999997") || strings.Contains(want, "1e+39") || strings.Contains(want, "6.75e+08") {
			discriminating++
		}
		for _, arm := range arms {
			got := arm.run(c.sql)
			if pin := ftKnown(c.name, arm.name, want); pin != "" {
				pinned++
				if got != pin {
					t.Errorf("%s on %s: the pinned answer changed\n  got  %s\n  pin  %s\n  PostgreSQL 17.11 %s", c.name, arm.name, got, pin, want)
				}
				continue
			}
			if got != want {
				t.Errorf("%s\n  %s\n  arm  %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.name, c.sql, arm.name, got, want)
			}
		}
	}
	if len(cells) < 300 || discriminating < 60 || pinned != 0 {
		t.Fatalf("%d cells, %d carrying a width-discriminating value, %d pinned: the table must discriminate", len(cells), discriminating, pinned)
	}
}
