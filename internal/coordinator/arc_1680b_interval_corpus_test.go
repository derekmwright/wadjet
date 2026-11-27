// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"sort"
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

// The corpus files (#1680b), one set per section under testdata/arc_1680b/:
//
//	<stem>.statements.tsv  name<TAB>settings<TAB>kind<TAB>sql
//	<stem>_pg17.tsv        name<TAB>PostgreSQL's answer
//	<stem>_engine.tsv      name<TAB>arms<TAB>class<TAB>this engine's answer
//	<stem>_manifest.tsv    name<TAB>stage<TAB>outcome<TAB>now (arc_1680b_interval_manifest_test.go)
//
// An answer is "ok" (DDL or a write), "ERR <SQLSTATE>: message", or
// "col:type(oid),… | row; row" with each field PostgreSQL's text (NULL for a
// NULL; tab and newline escaped); a result with no rows keeps its typed
// header: "col:type(oid),… | (0 rows)". A NOTICE or WARNING PostgreSQL raised
// while running the statement follows the answer as " !! <severity>
// <SQLSTATE>: message". kind is "sql", "ddl" (a fixture), "write" (a DML or
// CTAS statement PostgreSQL accepts) or "pg-only" (a plan observation).
//
// The engine file's arms is "*" when the six arms answer alike, otherwise one
// line per arm. Its answer is the same form with this engine's type names, an
// error as "ERR <SQLSTATE>: <sentence>" ("ERR : …" for an error that carries
// no SQLSTATE) and a panic as "PANIC …". Its class (iv80Classify) keeps apart
// what a gate must keep apart:
//
//	agree             value and declared types equal PostgreSQL's
//	agree-value       value equal, a declared type differs
//	differs           both answered, the values differ
//	answers-pg-error  this engine answered where PostgreSQL raised an error
//	pg-error          both raised an error with the same SQLSTATE
//	refusal           0A000 (an intentional capability refusal: its sentence names it)
//	wrong-sqlstate    both raised an error, another SQLSTATE than PostgreSQL's (not 0A000)
//	error             a coded error, not 0A000, where PostgreSQL answered
//	store-guard       the batch store's silent-write guard fired (#361): a hole in a rule, never an answer
//	internal          an error carrying no SQLSTATE
//	panic             a recovered panic
//	ok / pg-only      a fixture accepted / a plan observation not run here

const iv80Dir = "testdata/arc_1680b"

func iv80Path(stem, suffix string) string { return filepath.Join(iv80Dir, stem+suffix) }

func iv80Escape(s string) string {
	return strings.NewReplacer("\\", "\\\\", "\t", "\\t", "\n", "\\n").Replace(s)
}

func iv80Kind(c iv80Cell) string {
	switch {
	case c.write:
		return "write"
	case c.ddl:
		return "ddl"
	case c.pgOnly:
		return "pg-only"
	}
	return "sql"
}

func iv80Settings(c iv80Cell) string {
	if c.settings == "" {
		return "-"
	}
	return c.settings
}

// iv80StatementsFile is the statements file of a section's cells.
func iv80StatementsFile(cells []iv80Cell) string {
	var b strings.Builder
	b.WriteString("# #1680b interval corpus statements (arc_1680b_interval_corpus_cells_test.go). name<TAB>settings<TAB>kind<TAB>sql\n")
	for _, c := range cells {
		b.WriteString(c.name + "\t" + iv80Settings(c) + "\t" + iv80Kind(c) + "\t" + iv80Escape(c.sql) + "\n")
	}
	return b.String()
}

// iv80ReadTSV reads name -> rest-of-line (first field split off) from one
// corpus file, keeping the order of first appearance.
func iv80ReadTSV(t *testing.T, path string) (map[string][]string, []string) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string][]string{}
	var order []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<22), 1<<22)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, rest, ok := strings.Cut(line, "\t")
		if !ok {
			t.Fatalf("%s: malformed line %q", path, line)
		}
		if _, seen := out[name]; !seen {
			order = append(order, name)
		}
		out[name] = append(out[name], rest)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out, order
}

// iv80PGTypeName names a PostgreSQL result OID.
func iv80PGTypeName(oid uint32) string {
	switch oid {
	case 1186:
		return "interval"
	case 1114:
		return "timestamp"
	case 1184:
		return "timestamp with time zone"
	case 1082:
		return "date"
	case 1083:
		return "time"
	case 1266:
		return "time with time zone"
	case 1043:
		return "character varying"
	case 1042:
		return "character"
	case 1187:
		return "interval[]"
	case 1009:
		return "text[]"
	}
	if n := ftPGTypeName(oid); !strings.HasPrefix(n, "oid") {
		return n
	}
	return "unknown"
}

// iv80Render is an answer: the typed header and the rows, or the typed
// header and "(0 rows)" for a result with none (a zero-row result's types
// are part of the answer: Describe and an empty result must not lose them).
// A result with no columns at all is "(0 rows)".
func iv80Render(header, rows []string) string {
	if len(rows) == 0 {
		if len(header) == 0 {
			return "(0 rows)"
		}
		return strings.Join(header, ",") + " | (0 rows)"
	}
	return strings.Join(header, ",") + " | " + strings.Join(rows, "; ")
}

// iv80Notice splits an answer from the notices PostgreSQL raised with it.
func iv80Notice(ans string) (body, notice string) {
	body, notice, _ = strings.Cut(ans, " !! ")
	return body, notice
}

// iv80SectionTables are the tables every section creates, dropped first on
// the reference so a re-measure starts clean.
func iv80SectionTables() []string {
	return append(append(append([]string{}, iv80Tables...), iv80RelTables...), iv80WriteTables...)
}

// TestArc1680bMeasure runs every cell of every section against PostgreSQL
// (IV80_PG_DSN, e.g. postgres://postgres:pg@127.0.0.1:55480/postgres) and
// rewrites each section's statements and answers files; skipped otherwise.
func TestArc1680bMeasure(t *testing.T) {
	dsn := os.Getenv("IV80_PG_DSN")
	if dsn == "" {
		t.Skip("IV80_PG_DSN unset")
	}
	ctx := context.Background()
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	var notices []string
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
		notices = append(notices, n.Severity+" "+n.Code+": "+n.Message)
	}
	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	run := func(sql string) *pgconn.Result {
		return iv80Read(conn.ExecParams(ctx, sql, nil, nil, nil, nil))
	}
	one := func(sql string) string {
		r := run(sql)
		if r.Err != nil || len(r.Rows) != 1 {
			t.Fatalf("%s: %v", sql, r.Err)
		}
		return string(r.Rows[0][0])
	}
	reset := func() {
		for _, s := range []string{"RESET ALL", "SET statement_timeout = '30s'"} {
			if r := run(s); r.Err != nil {
				t.Fatal(r.Err)
			}
		}
	}
	reset()
	for _, tbl := range iv80SectionTables() {
		run("DROP TABLE IF EXISTS " + tbl)
	}
	notices = nil
	env := fmt.Sprintf("# %s; datcollate=%s datctype=%s server_encoding=%s; defaults IntervalStyle=%s DateStyle=%s TimeZone=%s; a cell's settings column overrides them for that cell only.\n",
		one("SELECT version()"), one("SELECT datcollate FROM pg_database WHERE datname = current_database()"),
		one("SELECT datctype FROM pg_database WHERE datname = current_database()"), one("SHOW server_encoding"),
		one("SHOW IntervalStyle"), one("SHOW DateStyle"), one("SHOW TimeZone"))
	if err := os.MkdirAll(iv80Dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, sec := range iv80Sections() {
		cells := sec.cells()
		var b strings.Builder
		b.WriteString("# PostgreSQL 17.11 answers for the #1680b interval corpus, section " + sec.stem + " (TestArc1680bMeasure; postgres:17-alpine, initdb --locale=C --encoding=UTF8, --memory=4g).\n")
		b.WriteString(env)
		b.WriteString("# name<TAB>answer: ok | ERR <SQLSTATE>: message | col:type(oid),... | row; row (or | (0 rows)), then any \" !! <severity> <SQLSTATE>: message\" PostgreSQL raised\n")
		for _, c := range cells {
			reset()
			if c.settings != "" {
				for _, kv := range strings.Split(c.settings, ";") {
					k, v, _ := strings.Cut(kv, "=")
					if r := run("SET " + k + " = '" + v + "'"); r.Err != nil {
						t.Fatalf("%s: SET %s: %v", c.name, kv, r.Err)
					}
				}
			}
			notices = nil
			r := run(c.sql)
			var ans string
			switch {
			case r.Err != nil:
				pe, ok := r.Err.(*pgconn.PgError)
				if !ok {
					t.Fatalf("%s: %v", c.name, r.Err)
				}
				ans = "ERR " + pe.Code + ": " + pe.Message
			case c.ddl:
				ans = "ok"
			default:
				header := make([]string, len(r.FieldDescriptions))
				for i, fd := range r.FieldDescriptions {
					header[i] = fmt.Sprintf("%s:%s(%d)", fd.Name, iv80PGTypeName(fd.DataTypeOID), fd.DataTypeOID)
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
				ans = iv80Render(header, rows)
			}
			if len(notices) > 0 {
				ans += " !! " + strings.Join(notices, " !! ")
			}
			b.WriteString(c.name + "\t" + iv80Escape(ans) + "\n")
		}
		reset()
		if err := os.WriteFile(iv80Path(sec.stem, ".statements.tsv"), []byte(iv80StatementsFile(cells)), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(iv80Path(sec.stem, "_pg17.tsv"), []byte(b.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// iv80Read is pgconn's ResultReader.Read keeping the field descriptions of
// a result with no rows: Read copies them inside its row loop only, so a
// zero-row result came back untyped (the round-1 recorder's "(0 rows)").
func iv80Read(rr *pgconn.ResultReader) *pgconn.Result {
	res := &pgconn.Result{}
	for rr.NextRow() {
		values := rr.Values()
		row := make([][]byte, len(values))
		for i, v := range values {
			if v != nil {
				row[i] = append([]byte{}, v...)
			}
		}
		res.Rows = append(res.Rows, row)
	}
	res.FieldDescriptions = append([]pgconn.FieldDescription(nil), rr.FieldDescriptions()...)
	res.CommandTag, res.Err = rr.Close()
	return res
}

// iv80PGValue is a PostgreSQL answer reduced to what an engine answer is
// compared against: column names without types, the error without its
// message, no notices.
func iv80PGValue(ans string) string {
	ans, _ = iv80Notice(ans)
	if strings.HasPrefix(ans, "ERR ") {
		code, _, _ := strings.Cut(ans, ":")
		return code
	}
	return ivNormalize(ans)
}

// iv80PGTypes is a PostgreSQL header with the OIDs dropped, as ivTypeNames
// spells this engine's.
func iv80PGTypes(ans string) string {
	ans, _ = iv80Notice(ans)
	head := ivHeaderTypes(ans)
	cols := strings.Split(head, ",")
	for i, c := range cols {
		if j := strings.LastIndexByte(c, '('); j > 0 && strings.HasSuffix(c, ")") {
			cols[i] = c[:j]
		}
	}
	return strings.Join(cols, ",")
}

// iv80ErrCode is the SQLSTATE of an "ERR <code>[ {<identity>}]: message"
// answer ("" when it carries none) and its message.
func iv80ErrCode(ans string) (code, msg string) {
	rest := strings.TrimPrefix(ans, "ERR ")
	code, msg, _ = strings.Cut(rest, ":")
	code, _, _ = strings.Cut(code, " {")
	return strings.TrimSpace(code), strings.TrimSpace(msg)
}

// iv80ErrIdentity is the identity an engine error was recorded with
// (iv80Err): an intentional refusal's stable reason (sqlerr.ReasonOf), or
// iv80Attached for a code attached to an uncoded failure (sqlerr.Attached);
// "" for any other answer.
func iv80ErrIdentity(ans string) string {
	if !strings.HasPrefix(ans, "ERR ") {
		return ""
	}
	head, _, _ := strings.Cut(strings.TrimPrefix(ans, "ERR "), ":")
	_, id, ok := strings.Cut(head, " {")
	if !ok {
		return ""
	}
	return strings.TrimSuffix(id, "}")
}

// iv80Attached is the identity iv80Err records for an error whose code was
// attached by a layer that did not raise it (sqlerr.Attached): by
// provenance an internal failure, whatever its SQLSTATE.
const iv80Attached = "attached"

// iv80Classify is the class of one engine answer against PostgreSQL's (the
// list in the file comment).
func iv80Classify(engine, pg string) string {
	pg, _ = iv80Notice(pg)
	pgErr := strings.HasPrefix(pg, "ERR ")
	pgCode, _ := iv80ErrCode(pg)
	switch {
	case strings.HasPrefix(engine, "PANIC"):
		return "panic"
	case strings.HasPrefix(engine, "ERR "):
		code, _ := iv80ErrCode(engine)
		switch {
		case code == "" || code == "XX000" || iv80ErrIdentity(engine) == iv80Attached:
			// By provenance (#1680c): no code, the internal class, or a
			// code attached to an uncoded failure.
			return "internal"
		case pgErr && code == pgCode:
			return "pg-error"
		case code == "0A000":
			return "refusal"
		case pgErr:
			return "wrong-sqlstate"
		}
		return "error"
	case pgErr:
		return "answers-pg-error"
	case engine == "ok" && pg == "ok":
		return "ok"
	case ivNormalize(engine) != iv80PGValue(pg):
		return "differs"
	case strings.Contains(engine, " | ") && ivTypeNames(ivHeaderTypes(engine)) != iv80PGTypes(pg):
		return "agree-value"
	}
	return "agree"
}

// iv80EngineText prints a TIMESTAMP column's values (the harness renders
// the box's epoch milliseconds) as PostgreSQL's text, so the class compares
// the instant and not the rendering. An answer whose rows do not split into
// its header's columns is left as it is.
func iv80EngineText(ans string) string {
	head, body, ok := strings.Cut(ans, " | ")
	if !ok || body == "(0 rows)" {
		return ans
	}
	cols := strings.Split(head, ",")
	var tsCols []int
	for i, c := range cols {
		if strings.HasSuffix(c, ":TIMESTAMP") {
			tsCols = append(tsCols, i)
		}
	}
	if len(tsCols) == 0 {
		return ans
	}
	rows := strings.Split(body, "; ")
	for r, row := range rows {
		f := strings.Split(row, ",")
		if len(f) != len(cols) {
			return ans
		}
		for _, i := range tsCols {
			ms, err := strconv.ParseInt(f[i], 10, 64)
			switch {
			case err != nil:
				continue
			case ms == math.MaxInt64:
				// The carrier's extremes are the infinite values (ADR-0012
				// temporal, E36), printed as the engine's printers do.
				f[i] = "infinity"
			case ms == math.MinInt64:
				f[i] = "-infinity"
			default:
				f[i] = time.UnixMilli(ms).UTC().Format("2006-01-02 15:04:05.999999")
			}
		}
		rows[r] = strings.Join(f, ",")
	}
	return head + " | " + strings.Join(rows, "; ")
}

// iv80Ids are the run-specific identifiers an error sentence can carry
// (query, task and stage ids), replaced so the baseline holds the sentence.
var iv80Ids = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|[0-9a-f]{16,}`)

// iv80Redact replaces the run-specific ids in an error sentence; a run of
// decimal digits (a value the sentence quotes) is kept.
func iv80Redact(msg string) string {
	return iv80Ids.ReplaceAllStringFunc(msg, func(m string) string {
		if strings.ContainsAny(m, "abcdef") {
			return "<id>"
		}
		return m
	})
}

// iv80Err is an engine error as the baseline records it: its SQLSTATE and
// the sentence a client is owed (sqlerr.SentenceOf), or the whole text of an
// error that carries no SQLSTATE, so an intentional refusal, a store guard
// and an internal error stay apart.
func iv80Err(err error) string {
	msg := sqlerr.SentenceOf(err)
	if msg == "" {
		msg = err.Error()
	}
	msg = iv80Redact(strings.Join(strings.Fields(msg), " "))
	if r := []rune(msg); len(r) > 240 {
		msg = string(r[:240]) + "…"
	}
	// The error's identity, read off its TYPE chain where it was raised
	// (never off the sentence): an intentional refusal's reason, or the
	// provenance of a code attached to an uncoded failure (#1680c).
	id := ""
	switch {
	case sqlerr.ReasonOf(err) != "":
		id = " {" + sqlerr.ReasonOf(err) + "}"
	case sqlerr.Attached(err):
		id = " {" + iv80Attached + "}"
	}
	return "ERR " + sqlerr.StateOf(err) + id + ": " + msg
}

// iv80EngineRender is ftEngineRender keeping a zero-row result's types.
func iv80EngineRender(cols []parquet.Column, cells [][]any) string {
	if len(cells) > 0 {
		return ftEngineRender(cols, cells)
	}
	header := make([]string, len(cols))
	for i, c := range cols {
		header[i] = c.Name + ":" + ftTypeName(c)
	}
	return iv80Render(header, nil)
}

// iv80RunSingle runs sql on the embedded engine.
func iv80RunSingle(ctx context.Context, db *wadjet.DB, sql string) (out string) {
	defer func() {
		if r := recover(); r != nil {
			out = fmt.Sprintf("PANIC %v", r)
		}
	}()
	res, err := db.Query(ctx, sql)
	if err != nil {
		return iv80Err(err)
	}
	cols := make([]parquet.Column, len(res.ColumnMetas))
	for i, m := range res.ColumnMetas {
		cols[i] = parquet.Column{Name: m.Name, Type: m.TypeID, Precision: m.Precision, Scale: m.Scale}
	}
	cells := make([][]any, len(res.Rows))
	varchar := map[int]bool{}
	for i := range res.Rows {
		cells[i] = res.Cells(i)
	}
	for i, m := range res.ColumnMetas {
		varchar[i] = m.TypeID == parquet.TypeString && m.StringLength != 0
	}
	return iv80MarkVarchar(iv80EngineRender(cols, cells), varchar)
}

// iv80MarkVarchar names a string column declared with a length channel
// (VARCHAR, what the wire sends as OID 1043, pgColumnOID) "character
// varying", as PostgreSQL's header does (#1680b round 3: cast/toVarchar's
// type is checkable; the shared ftTypeName prints every string "text").
func iv80MarkVarchar(ans string, varchar map[int]bool) string {
	head, body, ok := strings.Cut(ans, " | ")
	if !ok {
		return ans
	}
	cols := strings.Split(head, ",")
	if len(cols) != len(varchar) {
		return ans
	}
	for i, c := range cols {
		if n, typ, ok := strings.Cut(c, ":"); ok && varchar[i] && typ == "text" {
			cols[i] = n + ":character varying"
		}
	}
	return strings.Join(cols, ",") + " | " + body
}

// iv80RunDAG runs sql through a coordinator.
func iv80RunDAG(ctx context.Context, c *Coordinator, sql string) (out string) {
	defer func() {
		if r := recover(); r != nil {
			out = fmt.Sprintf("PANIC %v", r)
		}
	}()
	res, err := c.ExecuteSQL(ctx, sql)
	if err != nil {
		return iv80Err(err)
	}
	if res.Error != "" {
		return "ERR : " + iv80Redact(res.Error)
	}
	cols := append([]parquet.Column(nil), res.OutputSchema()...)
	var cells [][]any
	if st := res.Stream(); st != nil {
		defer st.Close()
		for {
			bb, berr := st.Next(ctx)
			if berr != nil {
				return iv80Err(berr)
			}
			if bb == nil {
				break
			}
			cells = append(cells, bb.ToRowValues()...)
		}
	} else {
		rows, rerr := res.Rows()
		if rerr != nil {
			return iv80Err(rerr)
		}
		for _, r := range rows {
			row := make([]any, len(cols))
			for j, c := range cols {
				row[j] = r[c.Name]
			}
			cells = append(cells, row)
		}
	}
	varchar := map[int]bool{}
	for i, c := range cols {
		varchar[i] = c.Type == parquet.TypeString && res.StringLength[c.Name] != 0
	}
	return iv80MarkVarchar(iv80EngineRender(cols, cells), varchar)
}

// iv80Arm is one execution arm.
type iv80Arm struct {
	name string
	run  func(string) string
}

// iv80ArmsFixture is ivArmsFixture's six arms over one shared catalog
// (single, spilled at a 512 KiB budget with forced aggregate drains and
// small sort runs, the DAG, the DAG with every build broadcast-refused so it
// shuffles, the DAG with four morsel workers, the small-query fast path),
// answering through iv80RunSingle / iv80RunDAG; the writer runs fixtures and
// writes.
func iv80ArmsFixture(t *testing.T, ctx context.Context) (func(string) string, []iv80Arm) {
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
	coordF := tmdCoordinator(t, ctx, infra, func(c *Config) { c.LocalFastPathBytes = 64 << 20 })
	spill := func(sql string) string {
		restoreDrain := exec.ForceAggDrainEvery(1)
		restoreRuns := exec.ForceSmallSpillRuns(2)
		defer func() { restoreRuns(); exec.ForceAggDrainEvery(restoreDrain) }()
		return iv80RunSingle(ctx, spilled, sql)
	}
	return func(sql string) string {
			if _, err := writer.Query(ctx, sql); err != nil {
				return iv80Err(err)
			}
			return "ok"
		}, []iv80Arm{
			{"single", func(sql string) string { return iv80RunSingle(ctx, writer, sql) }},
			{"spilled512k", spill},
			{"dag", func(sql string) string { return iv80RunDAG(ctx, coord, sql) }},
			{"dag-shuffled", func(sql string) string { return iv80RunDAG(ctx, coordB, sql) }},
			{"dag-morsel4", func(sql string) string { return iv80RunDAG(ctx, coordM, sql) }},
			{"dag-fastpath", func(sql string) string { return iv80RunDAG(ctx, coordF, sql) }},
		}
}

var iv80ArmNames = []string{"single", "spilled512k", "dag", "dag-shuffled", "dag-morsel4", "dag-fastpath"}

// iv80EngineRun runs every cell of every section on every arm and answers
// each section's engine file.
func iv80EngineRun(t *testing.T) map[string]string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	t.Cleanup(cancel)
	writer, arms := iv80ArmsFixture(t, ctx)
	out := map[string]string{}
	for _, sec := range iv80Sections() {
		pg, _ := iv80ReadTSV(t, iv80Path(sec.stem, "_pg17.tsv"))
		var b strings.Builder
		b.WriteString("# This engine's disposition of the #1680b interval corpus, section " + sec.stem + ", per arm (TestArc1680bRecordEngine; held by TestArc1680bEngineBaseline).\n")
		b.WriteString("# Settings are PostgreSQL's only: the engine runs each statement as written. A TIMESTAMP value is printed as text (iv80EngineText). name<TAB>arms<TAB>class<TAB>answer\n")
		for _, c := range sec.cells() {
			want := ""
			if a, ok := pg[c.name]; ok {
				want = a[0]
			}
			switch {
			case c.pgOnly:
				b.WriteString(c.name + "\t*\tpg-only\t-\n")
				continue
			case c.ddl:
				got := writer(c.sql)
				b.WriteString(c.name + "\twriter\t" + iv80Classify(got, want) + "\t" + iv80Escape(got) + "\n")
				continue
			}
			answers := make([]string, len(arms))
			same := true
			for i, arm := range arms {
				answers[i] = iv80EngineText(arm.run(c.sql))
				if answers[i] != answers[0] {
					same = false
				}
			}
			if same {
				b.WriteString(c.name + "\t*\t" + iv80Classify(answers[0], want) + "\t" + iv80Escape(answers[0]) + "\n")
				continue
			}
			for i, arm := range arms {
				b.WriteString(c.name + "\t" + arm.name + "\t" + iv80Classify(answers[i], want) + "\t" + iv80Escape(answers[i]) + "\n")
			}
		}
		out[sec.stem] = b.String()
	}
	return out
}

// TestArc1680bRecordEngine rewrites each section's engine file
// (IV80_RECORD_ENGINE=1); skipped otherwise.
func TestArc1680bRecordEngine(t *testing.T) {
	if os.Getenv("IV80_RECORD_ENGINE") == "" {
		t.Skip("IV80_RECORD_ENGINE unset")
	}
	for stem, body := range iv80EngineRun(t) {
		if err := os.WriteFile(iv80Path(stem, "_engine.tsv"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// TestArc1680bEngineBaseline holds the engine files: every cell's answer on
// every arm is the recorded one. It asserts nothing about agreement with
// PostgreSQL: it is the baseline the implementation items move, re-recorded
// (IV80_RECORD_ENGINE=1) with every moved cell named in the item's note.
// What a stage must reach is the manifest's (TestArc1680bStageManifest).
func TestArc1680bEngineBaseline(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: six arms over the interval corpus")
	}
	got := iv80EngineRun(t)
	for _, sec := range iv80Sections() {
		raw, err := os.ReadFile(iv80Path(sec.stem, "_engine.tsv"))
		if err != nil {
			t.Fatal(err)
		}
		want := strings.Split(string(raw), "\n")
		have := strings.Split(got[sec.stem], "\n")
		n := len(want)
		if len(have) > n {
			n = len(have)
		}
		moved := 0
		for i := 0; i < n; i++ {
			var w, h string
			if i < len(want) {
				w = want[i]
			}
			if i < len(have) {
				h = have[i]
			}
			if w != h {
				moved++
				if moved <= 20 {
					t.Errorf("%s: the engine's disposition moved\n  recorded %s\n  now      %s", sec.stem, w, h)
				}
			}
		}
		if moved > 20 {
			t.Errorf("%s: %d lines moved in all", sec.stem, moved)
		}
	}
}

var iv80Classes = map[string]bool{
	"agree": true, "agree-value": true, "differs": true, "answers-pg-error": true, "pg-error": true,
	"refusal": true, "wrong-sqlstate": true, "error": true, "store-guard": true, "internal": true,
	"panic": true, "ok": true, "pg-only": true,
}

// TestArc1680bCorpusWellFormed: every section's statements file is the cells
// as written, every statement has PostgreSQL's answer (typed, including a
// zero-row answer) and this engine's disposition on every arm, with a class
// that is the classification of that answer.
func TestArc1680bCorpusWellFormed(t *testing.T) {
	total := 0
	for _, sec := range iv80Sections() {
		cells := sec.cells()
		raw, err := os.ReadFile(iv80Path(sec.stem, ".statements.tsv"))
		if err != nil {
			t.Fatal(err)
		}
		if string(raw) != iv80StatementsFile(cells) {
			t.Errorf("%s: the statements file is not the cells as written: re-measure (TestArc1680bMeasure)", sec.stem)
		}
		pg, pgOrder := iv80ReadTSV(t, iv80Path(sec.stem, "_pg17.tsv"))
		eng, _ := iv80ReadTSV(t, iv80Path(sec.stem, "_engine.tsv"))
		seen := map[string]bool{}
		for _, c := range cells {
			total++
			if seen[c.name] {
				t.Errorf("%s: duplicate cell %s", sec.stem, c.name)
			}
			seen[c.name] = true
			a, ok := pg[c.name]
			if !ok || len(a) != 1 {
				t.Errorf("%s: %s has no single PostgreSQL answer", sec.stem, c.name)
				continue
			}
			ans, notice := iv80Notice(a[0])
			if notice != "" && !strings.Contains(notice, ": ") {
				t.Errorf("%s: %s: malformed notice %q", sec.stem, c.name, notice)
			}
			switch {
			case c.ddl && !c.write:
				if ans != "ok" {
					t.Errorf("%s: fixture %s failed on PostgreSQL: %s", sec.stem, c.name, ans)
				}
			case c.ddl:
			case strings.HasPrefix(ans, "ERR "):
				if len(ans) < 9 {
					t.Errorf("%s: %s: malformed error %q", sec.stem, c.name, ans)
				}
			case ans == "(0 rows)":
				t.Errorf("%s: %s: a zero-row answer without its column types", sec.stem, c.name)
			default:
				head, _, ok := strings.Cut(ans, " | ")
				if !ok {
					t.Errorf("%s: %s: malformed answer %q", sec.stem, c.name, ans)
					continue
				}
				for _, col := range strings.Split(head, ",") {
					if !strings.Contains(col, ":") || !strings.HasSuffix(col, ")") {
						t.Errorf("%s: %s: column %q carries no type OID", sec.stem, c.name, col)
					}
				}
			}
			lines := eng[c.name]
			switch {
			case c.pgOnly || c.ddl:
				if len(lines) != 1 {
					t.Errorf("%s: %s: want one engine line, have %d", sec.stem, c.name, len(lines))
				}
			case len(lines) == 1 && strings.HasPrefix(lines[0], "*\t"):
			default:
				var arms []string
				for _, l := range lines {
					a, _, _ := strings.Cut(l, "\t")
					arms = append(arms, a)
				}
				if strings.Join(arms, ",") != strings.Join(iv80ArmNames, ",") {
					t.Errorf("%s: %s: engine disposition arms %v, want * or %v", sec.stem, c.name, arms, iv80ArmNames)
				}
			}
			for _, l := range lines {
				f := strings.SplitN(l, "\t", 3)
				if len(f) != 3 {
					t.Errorf("%s: %s: malformed engine line %q", sec.stem, c.name, l)
					continue
				}
				if !iv80Classes[f[1]] {
					t.Errorf("%s: %s: unknown class %q", sec.stem, c.name, f[1])
				}
				if !c.pgOnly && f[1] != iv80Classify(iv80Unescape(f[2]), a[0]) {
					t.Errorf("%s: %s: class %q is not the classification of %q", sec.stem, c.name, f[1], f[2])
				}
			}
		}
		if len(pgOrder) != len(cells) {
			t.Errorf("%s: %d PostgreSQL answers for %d cells", sec.stem, len(pgOrder), len(cells))
		}
	}
	if total < 900 {
		t.Fatalf("only %d cells: the corpus must cover the sections", total)
	}
}

// iv80Unescape undoes iv80Escape.
func iv80Unescape(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			i++
			switch s[i] {
			case 't':
				b.WriteByte('\t')
			case 'n':
				b.WriteByte('\n')
			default:
				b.WriteByte(s[i])
			}
			continue
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// --- The reference model --------------------------------------------------

// iv80CmpValue is PostgreSQL's interval_cmp_value (src/backend/utils/adt/
// timestamp.c, REL_17_11): (months * 30 + days) * 86400000000 + micros, in
// 128 bits. It is the proposed comparison and hash key; this test checks it
// against PostgreSQL's measured order and grouping of the generated values.
func iv80CmpValue(v iv80ModelValue) *big.Int {
	days := new(big.Int).Mul(big.NewInt(int64(v.months)), big.NewInt(30))
	days.Add(days, big.NewInt(int64(v.days)))
	span := new(big.Int).Mul(days, big.NewInt(86400000000))
	return span.Add(span, big.NewInt(v.micros))
}

// iv80Int64Wrap is the same value computed in wrapping int64 arithmetic: what
// a 64-bit implementation would compare.
func iv80Int64Wrap(v iv80ModelValue) int64 {
	return (int64(v.months)*30+int64(v.days))*86400000000 + v.micros
}

// TestArc1680bReferenceModel: the 128-bit normalized value reproduces
// PostgreSQL's ORDER BY and GROUP BY over the generated values exactly; SQL
// equality implies PostgreSQL's hash equality (measured) and the model key's
// equality (by construction); a hash of the FIELDS would split SQL-equal
// groups, and a 64-bit normalized value would mis-order the values past 2^63.
// Every generated id is in every answer exactly once.
func TestArc1680bReferenceModel(t *testing.T) {
	vals := iv80ModelValues()
	pg, _ := iv80ReadTSV(t, iv80Path("09_model", "_pg17.tsv"))
	body := func(name string) []string {
		a := pg[name]
		if len(a) != 1 {
			t.Fatalf("no answer for %s", name)
		}
		_, rows, ok := strings.Cut(a[0], " | ")
		if !ok {
			t.Fatalf("%s: %s", name, a[0])
		}
		return strings.Split(rows, "; ")
	}
	// coverage: ids are exactly 1..len(vals), each once.
	coverage := func(name string, ids []int) {
		t.Helper()
		seen := make([]bool, len(vals)+1)
		for _, id := range ids {
			if id < 1 || id > len(vals) || seen[id] {
				t.Errorf("%s: id %d out of range or repeated", name, id)
				continue
			}
			seen[id] = true
		}
		if len(ids) != len(vals) {
			t.Errorf("%s: %d ids for %d generated values", name, len(ids), len(vals))
		}
	}
	// The stored fields are the input's, unnormalized.
	fieldRows := body("model/fields")
	if len(fieldRows) != len(vals) {
		t.Fatalf("model/fields: %d rows for %d values", len(fieldRows), len(vals))
	}
	for i, r := range fieldRows {
		f := strings.Split(r, ",")
		v := vals[i]
		want := fmt.Sprintf("%d,%d,%d,%d", i+1, v.months, v.days, v.micros)
		got := f[0] + "," + iv80TrimNumeric(f[1]) + "," + iv80TrimNumeric(f[2]) + "," + iv80TrimNumeric(f[3])
		if got != want {
			t.Fatalf("model/fields row %d: PostgreSQL stored %s, generated %s", i+1, got, want)
		}
	}
	// ORDER BY v, id.
	ids := make([]int, len(vals))
	for i := range ids {
		ids[i] = i + 1
	}
	sort.SliceStable(ids, func(a, b int) bool {
		c := iv80CmpValue(vals[ids[a]-1]).Cmp(iv80CmpValue(vals[ids[b]-1]))
		if c != 0 {
			return c < 0
		}
		return ids[a] < ids[b]
	})
	var modelOrder []string
	for _, id := range ids {
		modelOrder = append(modelOrder, strconv.Itoa(id))
	}
	var pgOrderIDs []int
	for _, s := range body("model/order") {
		n, err := strconv.Atoi(s)
		if err != nil {
			t.Fatalf("model/order: %q", s)
		}
		pgOrderIDs = append(pgOrderIDs, n)
	}
	coverage("model/order", pgOrderIDs)
	if got := strings.Join(body("model/order"), " "); got != strings.Join(modelOrder, " ") {
		t.Errorf("ORDER BY: PostgreSQL %s\n  model %s", got, strings.Join(modelOrder, " "))
	}
	// The same order under wrapping int64 disagrees (the values past 2^63).
	sort.SliceStable(ids, func(a, b int) bool {
		x, y := iv80Int64Wrap(vals[ids[a]-1]), iv80Int64Wrap(vals[ids[b]-1])
		if x != y {
			return x < y
		}
		return ids[a] < ids[b]
	})
	var wrapOrder []string
	for _, id := range ids {
		wrapOrder = append(wrapOrder, strconv.Itoa(id))
	}
	if strings.Join(wrapOrder, " ") == strings.Join(modelOrder, " ") {
		t.Errorf("the generated values do not discriminate a 64-bit normalized value")
	}
	// GROUP BY: membership is equality of the 128-bit value.
	groups := map[string][]int{}
	for i, v := range vals {
		k := iv80CmpValue(v).String()
		groups[k] = append(groups[k], i+1)
	}
	var modelGroups []string
	for _, g := range groups {
		var s []string
		for _, id := range g {
			s = append(s, strconv.Itoa(id))
		}
		modelGroups = append(modelGroups, fmt.Sprintf("%d,%d,%s", g[0], len(g), strings.Join(s, " ")))
	}
	sort.Slice(modelGroups, func(a, b int) bool {
		x, _ := strconv.Atoi(strings.SplitN(modelGroups[a], ",", 2)[0])
		y, _ := strconv.Atoi(strings.SplitN(modelGroups[b], ",", 2)[0])
		return x < y
	})
	for _, name := range []string{"model/groups", "model/groupsSort"} {
		rows := body(name)
		var members []int
		for _, r := range rows {
			f := strings.SplitN(r, ",", 3)
			if len(f) != 3 {
				t.Fatalf("%s: row %q", name, r)
			}
			for _, s := range strings.Fields(f[2]) {
				n, _ := strconv.Atoi(s)
				members = append(members, n)
			}
		}
		coverage(name, members)
		if got := strings.Join(rows, "; "); got != strings.Join(modelGroups, "; ") {
			t.Errorf("%s: PostgreSQL %s\n  model %s", name, got, strings.Join(modelGroups, "; "))
		}
	}
	// Hashing: PostgreSQL's interval_hash is equal within every group
	// (measured); a hash of the fields is not. Every id carries a hash.
	hash := map[int]string{}
	var hashIDs []int
	for _, r := range body("model/hash") {
		id, h, ok := strings.Cut(r, ",")
		n, err := strconv.Atoi(id)
		if !ok || err != nil || h == "" || h == "NULL" {
			t.Fatalf("model/hash: row %q", r)
		}
		hash[n] = h
		hashIDs = append(hashIDs, n)
	}
	coverage("model/hash", hashIDs)
	fieldSplit, multi := 0, 0
	for _, g := range groups {
		if len(g) < 2 {
			continue
		}
		multi++
		distinctFields := false
		for _, id := range g[1:] {
			if hash[id] != hash[g[0]] {
				t.Errorf("PostgreSQL hashed SQL-equal ids %d and %d apart", g[0], id)
			}
			if vals[id-1] != vals[g[0]-1] {
				distinctFields = true
			}
		}
		if distinctFields {
			fieldSplit++
		}
	}
	if multi < 20 || fieldSplit < 20 {
		t.Fatalf("%d multi-member groups, %d with members of different fields: the generator must collide", multi, fieldSplit)
	}
	t.Logf("%d values, %d groups, %d multi-member, %d with differing fields; 128-bit model matches PostgreSQL's order and grouping", len(vals), len(groups), multi, fieldSplit)
}

// iv80TrimNumeric drops a numeric's zero fraction ("30.000000" -> "30").
func iv80TrimNumeric(s string) string {
	if i := strings.IndexByte(s, '.'); i >= 0 {
		frac := strings.TrimRight(s[i+1:], "0")
		if frac == "" {
			return s[:i]
		}
		return s[:i] + "." + frac
	}
	return s
}
