// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// EVERY REGULAR-EXPRESSION CONSTRUCT READS ITS PATTERN AS AN ARE (#1499).
// The seam is the compile of a SQL-supplied pattern (expr.translateAndCompile):
// the `~` operators, SIMILAR TO, substring(s FROM p), regexp_like,
// regexp_count, regexp_replace and this engine's own regexp_extract,
// regexp_extract_all, regexp_split and payload_matches. This table is that
// seam enumerated once — construct × pattern origin (a literal, a COLUMN of
// patterns) × consumer (projection, WHERE, GROUP BY) — on five arms against
// PostgreSQL 17.11's rows. A construct PostgreSQL does not have is measured
// through its PostgreSQL equivalent (rxCell.pg): regexp_extract is
// regexp_substr, regexp_extract_all the array of regexp_substr occurrences,
// regexp_split regexp_split_to_array, payload_matches `~`.
//
// The fixture discriminates the two dialects: a subject holding a backspace
// (`\b` in an ARE), a word a `\b` word boundary would find, an alternation
// whose alternatives are prefixes of each other (leftmost-longest against
// leftmost-first), a `.` across a newline, a multibyte subject for the
// character start position, an empty match abutting a non-empty one.
//
//	rx_t: id | s text | p text (a pattern per row)

type rxRow struct{ s, p any }

var rxRows = []rxRow{
	{"abc", `\b`},
	{"a\bc", `a\bc`},
	{"the cat sat", `\ycat\y`},
	{"GETS /x", `GET|GETS`},
	{"line1\nline2", `1.l`},
	{"éaéa", `a`},
	{"aab", `a|aa`},
	{"baaac", `a*`},
	{"", `^$`},
	{nil, nil},
}

func rxTables() []tmdTable {
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
		{Name: "p", Type: parquet.TypeString, Nullable: true},
	}}
	var rows []map[string]any
	for k, r := range rxRows {
		rows = append(rows, map[string]any{"id": int64(k + 1), "s": r.s, "p": r.p})
	}
	return []tmdTable{{"rx_t", schema, rows}}
}

func rxPGFixture() []string {
	out := []string{
		"DROP TABLE IF EXISTS rx_t",
		"CREATE TABLE rx_t (id bigint, s text, p text)",
	}
	lit := func(v any) string {
		if v == nil {
			return "NULL"
		}
		s := v.(string)
		s = strings.NewReplacer(`\`, `\\`, "'", `\'`, "\n", `\n`, "\b", `\b`).Replace(s)
		return "E'" + s + "'"
	}
	for k, r := range rxRows {
		out = append(out, fmt.Sprintf("INSERT INTO rx_t VALUES (%d, %s, %s)", k+1, lit(r.s), lit(r.p)))
	}
	return out
}

// rxKind is one construct over rx_t t: this engine's expression and, when it
// differs, PostgreSQL's equivalent spelling.
type rxKind struct{ name, expr, pg string }

func rxKinds() []rxKind {
	all := func(s, p string) string {
		return fmt.Sprintf("CASE WHEN %[1]s IS NULL OR %[2]s IS NULL THEN NULL ELSE (SELECT coalesce(array_to_json(array_agg(regexp_substr(%[1]s, %[2]s, 1, n::int) ORDER BY n))::text, '[]') FROM generate_series(1, regexp_count(%[1]s, %[2]s)) n) END", s, p)
	}
	var out []rxKind
	for _, o := range []struct{ name, p string }{{"lit", `'\b'`}, {"col", "t.p"}} {
		p := o.p
		out = append(out,
			rxKind{"op/" + o.name, "t.s ~ " + p, ""},
			rxKind{"opI/" + o.name, "t.s ~* " + p, ""},
			rxKind{"opNot/" + o.name, "t.s !~ " + p, ""},
			rxKind{"like/" + o.name, "regexp_like(t.s, " + p + ")", ""},
			rxKind{"count/" + o.name, "regexp_count(t.s, " + p + ")", ""},
			rxKind{"replace/" + o.name, "regexp_replace(t.s, " + p + ", '#', 'g')", ""},
			rxKind{"substring/" + o.name, "substring(t.s FROM " + p + ")", ""},
			rxKind{"extract/" + o.name, "regexp_extract(t.s, " + p + ")", "regexp_substr(t.s, " + p + ")"},
			rxKind{"extractAll/" + o.name, "regexp_extract_all(t.s, " + p + ")", all("t.s", p)},
			rxKind{"split/" + o.name, "regexp_split(t.s, " + p + ")", "array_to_json(regexp_split_to_array(t.s, " + p + "))::text"},
			rxKind{"payload/" + o.name, "payload_matches(t.s, " + p + ")", "t.s ~ " + p},
		)
	}
	return append(out,
		// The dialect cells on a literal: RE2-only spellings are 2201B, the
		// ARE word boundary answers, a newline is matched by `.`.
		rxKind{"like/z", `regexp_like(t.s, 'c\z')`, ""},
		rxKind{"like/y", `regexp_like(t.s, '\ycat\y')`, ""},
		rxKind{"like/dotNewline", `regexp_like(t.s, '1.l')`, ""},
		rxKind{"like/invalid", `regexp_like(t.s, '(')`, ""},
		rxKind{"like/wordNonASCII", `regexp_like(t.s, '^\w')`, ""},
		rxKind{"extract/longest", "regexp_extract(t.s, 'GET|GETS')", "regexp_substr(t.s, 'GET|GETS')"},
		rxKind{"extract/group", "regexp_extract(t.s, '(a)(.)', 2)", "regexp_substr(t.s, '(a)(.)', 1, 1, '', 2)"},
		rxKind{"extract/lazy", "regexp_extract(t.s, '(.*?)a(.*)', 2)", "regexp_substr(t.s, '(.*?)a(.*)', 1, 1, '', 2)"},
		rxKind{"count/longest", "regexp_count(t.s, 'a|aa')", ""},
		rxKind{"count/abuttingEmpty", "regexp_count(t.s, 'a*')", ""},
		// The flags argument (regexp_like) and the start position in
		// characters (regexp_count), as the server reads them.
		rxKind{"likeFlags/i", "regexp_like(t.s, 'A', 'i')", ""},
		rxKind{"likeFlags/ic", "regexp_like(t.s, 'A', 'ic')", ""},
		rxKind{"likeFlags/q", "regexp_like(t.s, 'a.c', 'q')", ""},
		rxKind{"likeFlags/null", "regexp_like(t.s, 'a', NULL)", ""},
		rxKind{"likeFlags/g", "regexp_like(t.s, 'a', 'g')", ""},
		rxKind{"likeFlags/bad", "regexp_like(t.s, 'a', 'z')", ""},
		rxKind{"likeFlags/n", "regexp_like(t.s, '1.l', 'n')", ""},
		rxKind{"countStart/3", "regexp_count(t.s, 'a', 3)", ""},
		rxKind{"countStart/flags", "regexp_count(t.s, 'A', 2, 'i')", ""},
		rxKind{"countStart/zero", "regexp_count(t.s, 'a', 0)", ""},
		rxKind{"countStart/null", "regexp_count(t.s, 'a', NULL)", ""},
		rxKind{"countStart/past", "regexp_count(t.s, 'a', 40)", ""},
		rxKind{"countStart/wordY", `regexp_count(t.s, '\ya', 2)`, ""},
		// SIMILAR TO's rewrite compiles through the same seam: `_` and `%`
		// match a newline, an escaped y is the ARE word boundary.
		rxKind{"similar/underscore", "t.s SIMILAR TO 'line1_line2'", ""},
		rxKind{"similar/percent", "t.s SIMILAR TO 'line1%'", ""},
		rxKind{"similar/wordY", "t.s SIMILAR TO '%#ycat#y%' ESCAPE '#'", ""},
		rxKind{"similar/backspace", "t.s SIMILAR TO 'a#bc' ESCAPE '#'", ""},
		rxKind{"similar/backref", "t.s SIMILAR TO '(a)#1' ESCAPE '#'", ""},
		// Controls: LIKE / ILIKE are not regular expressions.
		rxKind{"likeOp/control", "t.s LIKE 'a_c'", ""},
		rxKind{"ilikeOp/control", "t.s ILIKE 'A%'", ""},
	)
}

// rxCell is one query: this engine's SQL and PostgreSQL's (the same unless
// the construct has another spelling there).
type rxCell struct {
	name, sql, pg string
	ordered       bool
}

func rxCells() []rxCell {
	var out []rxCell
	for _, k := range rxKinds() {
		pgExpr := k.pg
		if pgExpr == "" {
			pgExpr = k.expr
		}
		q := func(e, tmpl string) string { return fmt.Sprintf(tmpl, e) }
		for _, c := range []struct{ consumer, tmpl string }{
			{"proj", "SELECT t.id, %s AS v FROM rx_t t"},
			{"group", "SELECT %s AS k, count(*) AS c FROM rx_t t GROUP BY 1"},
			// The WHERE consumer: the rows whose answer is not NULL and
			// differs from the empty string's / false's reading, through a
			// comparison of the value's text.
			{"where", "SELECT t.id FROM rx_t t WHERE CAST(%s AS TEXT) IN ('true', 't', '1', 'ab', 'GETS', 'c', 'cat', '[\"\"]', 'b')"},
		} {
			out = append(out, rxCell{k.name + "/" + c.consumer, q(k.expr, c.tmpl), q(pgExpr, c.tmpl), false})
		}
	}
	return out
}

func rxStandalone(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
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
	for _, tbl := range rxTables() {
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

func rxArms(t *testing.T, ctx context.Context) []ssArm {
	t.Helper()
	single := rxStandalone(t, ctx, 0)
	spilled := rxStandalone(t, ctx, 512*1024)
	stand := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, rxTables())
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

const rxPGFile = "testdata/arc_rx_regex_pg17.tsv"
const rxKeptFile = "testdata/arc_rx_regex_kept.tsv"

// TestArcRXMeasurePostgres measures every cell on PostgreSQL 17.11
// (RX_PG_DSN=postgres://…) and writes the answer file.
func TestArcRXMeasurePostgres(t *testing.T) {
	dsn := os.Getenv("RX_PG_DSN")
	if dsn == "" {
		t.Skip("RX_PG_DSN unset")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	for _, s := range append([]string{"SET statement_timeout = '30s'"}, rxPGFixture()...) {
		if _, err := conn.Exec(ctx, s, pgx.QueryExecModeSimpleProtocol); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	var b strings.Builder
	b.WriteString("# arc RX: PostgreSQL 17.11's answer for every cell (TestArcRXMeasurePostgres)\n")
	for _, c := range rxCells() {
		a := func() string {
			rows, err := conn.Query(ctx, c.pg, pgx.QueryExecModeSimpleProtocol)
			if err != nil {
				return "ERR " + rnPGState(err) + " " + strings.ReplaceAll(err.Error(), "\n", " ")
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
		}()
		fmt.Fprintf(&b, "%s\t%s\n", c.name, a)
	}
	if err := os.WriteFile(rxPGFile, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestArcRXRegexTableEveryArm runs every cell on the five arms and compares
// with PostgreSQL 17.11's answer, or — for a cell in the kept file — with
// the catalogued divergence or refusal it holds.
func TestArcRXRegexTableEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the regular-expression table")
	}
	pg := rnReadTSV(t, rxPGFile, 2)
	kept := rxKept(t)
	cells := rxCells()
	names := map[string]bool{}
	for _, c := range cells {
		names[c.name] = true
		if _, ok := pg[c.name]; !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", c.name)
		}
	}
	for name := range kept {
		if !names[name] {
			t.Fatalf("kept cell %s is not in the table", name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	arms := rxArms(t, ctx)
	var dump *os.File
	if p := os.Getenv("RX_DUMP"); p != "" {
		f, err := os.Create(p)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		dump = f
	}
	var dumpMu sync.Mutex
	for _, tc := range cells {
		pgWant := pg[tc.name][0]
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

// rxKept reads testdata/arc_rx_regex_kept.tsv (name, scope, this engine's
// answer, why): the cells whose answer here is a catalogued divergence or
// refusal, asserted as they stand so a change FAILS and is re-measured.
func rxKept(t *testing.T) map[string][]rnKeep {
	t.Helper()
	out := map[string][]rnKeep{}
	for name, p := range rnReadTSV(t, rxKeptFile, 4) {
		if p[0] != "all" && p[0] != "local" && p[0] != "dag" {
			t.Fatalf("malformed kept line for %s", name)
		}
		out[name] = append(out[name], rnKeep{scope: p[0], want: p[1], why: p[2]})
	}
	return out
}
