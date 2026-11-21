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
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #1209 line 3 on FIVE ARMS: one case mapping, the C locale's (ASCII letters
// only), in lower(), upper(), ILIKE, `~*` and the regex i flag, in every
// position: projection, WHERE, a CASE-wrapped WHERE, GROUP BY key, ORDER BY
// key, JOIN key and JOIN predicate, a derived table, and the constant-folded
// literal. The 18 representative pairs are the 2026-11-17 fold matrix's;
// every answer is PostgreSQL 17.11's (postgres:17-alpine, `initdb
// --locale=C --encoding=UTF8`; TestCaseFold1209MeasurePostgres writes
// testdata/case_fold_1209_pg17.tsv and refuses any other locale).
//
//	cf_t: id | u text | l text   (u the upper-case form, l the lower-case one)

var cfPairs = [][2]string{
	{"É", "é"}, {"ẞ", "ß"}, {"Σ", "σ"}, {"Σ", "ς"},
	{"İ", "i"}, {"I", "ı"}, {"Д", "д"}, {"Ա", "ա"},
	{"Ａ", "ａ"}, {"K", "k"}, {"S", "ſ"}, {"Ǆ", "ǆ"},
	{"ǅ", "ǆ"}, {"\U00010400", "\U00010428"}, {"A", "a"}, {"É", "é"},
	{"Ꭰ", "ꭰ"}, {"Ა", "ა"},
}

const cfPGFile = "testdata/case_fold_1209_pg17.tsv"

func cfTables() []tmdTable {
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "u", Type: parquet.TypeString, Nullable: true},
		{Name: "l", Type: parquet.TypeString, Nullable: true},
	}}
	var rows []map[string]any
	for k, p := range cfPairs {
		rows = append(rows, map[string]any{"id": int64(k + 1), "u": p[0], "l": p[1]})
	}
	rows = append(rows, map[string]any{"id": int64(len(cfPairs) + 1), "u": nil, "l": nil})
	return []tmdTable{{"cf_t", schema, rows}}
}

func cfPGFixture() []string {
	out := []string{"DROP TABLE IF EXISTS cf_t", "CREATE TABLE cf_t (id bigint, u text, l text)"}
	for k, p := range cfPairs {
		out = append(out, fmt.Sprintf("INSERT INTO cf_t VALUES (%d, '%s', '%s')", k+1, p[0], p[1]))
	}
	return append(out, fmt.Sprintf("INSERT INTO cf_t VALUES (%d, NULL, NULL)", len(cfPairs)+1))
}

func cfCells() []rxCell {
	values := map[string]string{
		"lower(u)": "lower(t.u)", "upper(l)": "upper(t.l)", "upper(u)": "upper(t.u)", "lower(l)": "lower(t.l)",
		"lower(u||COLE)": "lower(t.u || 'COLE')", "replace_i": "regexp_replace(t.u, t.l, '#', 'i')",
	}
	preds := map[string]string{
		"lower(u)=l": "lower(t.u) = t.l", "upper(l)=u": "upper(t.l) = t.u",
		"lower(u)=lower(l)": "lower(t.u) = lower(t.l)", "upper(u)=upper(l)": "upper(t.u) = upper(t.l)",
		"u ILIKE l": "t.u ILIKE t.l", "l ILIKE u": "t.l ILIKE t.u", "u NOT ILIKE l": "t.u NOT ILIKE t.l",
		"u||x ILIKE l||%": "(t.u || 'x') ILIKE (t.l || '%')", "u ILIKE l ESCAPE !": "t.u ILIKE t.l ESCAPE '!'",
		"u ~~* l": "t.u ~~* t.l", "u ~* l": "t.u ~* t.l", "l ~* u": "t.l ~* t.u", "u !~* l": "t.u !~* t.l",
		"regexp_like_i": "regexp_like(t.u, t.l, 'i')", "embedded_i": "t.u ~ ('(?i)' || t.l)",
		"u ILIKE l||%": "t.u ILIKE (t.l || '%')",
	}
	var out []rxCell
	add := func(name, sql string) { out = append(out, rxCell{name: name, sql: sql, pg: sql}) }
	for name, e := range values {
		add("V/"+name+"/proj", "SELECT t.id, md5("+e+") AS v FROM cf_t t")
		add("V/"+name+"/group", "SELECT md5("+e+") AS k, count(*) AS c FROM cf_t t GROUP BY "+e)
		add("V/"+name+"/sort", "SELECT t.id, row_number() OVER (ORDER BY "+e+", t.id) AS r FROM cf_t t")
		add("V/"+name+"/derived", "SELECT d.id, md5(d.k) AS v FROM (SELECT t.id, "+e+" AS k FROM cf_t t) d")
	}
	for name, e := range preds {
		add("P/"+name+"/proj", "SELECT t.id, "+e+" AS v FROM cf_t t")
		add("P/"+name+"/where", "SELECT t.id FROM cf_t t WHERE "+e)
		add("P/"+name+"/case", "SELECT t.id FROM cf_t t WHERE CASE WHEN "+e+" THEN true ELSE false END")
		add("P/"+name+"/group", "SELECT "+e+" AS k, count(*) AS c FROM cf_t t GROUP BY 1")
		for i, p := range cfPairs {
			lit := strings.NewReplacer("t.u", "'"+p[0]+"'", "t.l", "'"+p[1]+"'").Replace(e)
			add(fmt.Sprintf("P/%s/const/%d", name, i+1), "SELECT "+lit+" AS v")
		}
	}
	add("J/lower(a.u)=b.l", "SELECT a.id AS x, b.id AS y FROM cf_t a JOIN cf_t b ON lower(a.u) = b.l")
	add("J/lower(a.u)=lower(b.l)", "SELECT a.id AS x, b.id AS y FROM cf_t a JOIN cf_t b ON lower(a.u) = lower(b.l)")
	add("J/upper(a.l)=b.u", "SELECT a.id AS x, b.id AS y FROM cf_t a JOIN cf_t b ON upper(a.l) = b.u")
	add("J/a.u ILIKE b.l", "SELECT a.id AS x, b.id AS y FROM cf_t a JOIN cf_t b ON a.u ILIKE b.l")
	add("J/a.u ~* b.l", "SELECT a.id AS x, b.id AS y FROM cf_t a JOIN cf_t b ON a.u ~* b.l")
	add("J/derived-key", "SELECT a.id AS x, b.id AS y FROM (SELECT id, lower(u) AS k FROM cf_t) a JOIN (SELECT id, l AS k FROM cf_t) b ON a.k = b.k")
	// The issue's three statements over one expression must agree.
	add("C/select-both", "SELECT t.id, lower(t.u) = lower(t.l) AS e, md5(lower(t.u)) AS v FROM cf_t t")
	add("C/where", "SELECT t.id, md5(lower(t.u)) AS v FROM cf_t t WHERE lower(t.u) = lower(t.l)")
	add("C/group", "SELECT md5(lower(t.u)) AS k, count(*) AS c FROM cf_t t GROUP BY lower(t.u)")
	add("C/ecole", "SELECT t.id, lower(t.u || 'COLE') = 'école' AS e FROM cf_t t WHERE t.id IN (1, 15)")
	return out
}

// TestCaseFold1209MeasurePostgres measures every cell on the reference
// (CF_PG_DSN=postgres://…) and writes cfPGFile.
func TestCaseFold1209MeasurePostgres(t *testing.T) {
	dsn := os.Getenv("CF_PG_DSN")
	if dsn == "" {
		t.Skip("CF_PG_DSN unset")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	var ver, collate, ctype, enc string
	if err := conn.QueryRow(ctx, `SELECT current_setting('server_version'), datcollate, datctype,
		pg_encoding_to_char(encoding) FROM pg_database WHERE datname = current_database()`).Scan(&ver, &collate, &ctype, &enc); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(ver, "17.11") || collate != "C" || ctype != "C" || enc != "UTF8" {
		t.Fatalf("not the reference installation: %s %s %s %s", ver, collate, ctype, enc)
	}
	for _, s := range append([]string{"SET statement_timeout = '30s'"}, cfPGFixture()...) {
		if _, err := conn.Exec(ctx, s, pgx.QueryExecModeSimpleProtocol); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# #1209 line 3: PostgreSQL %s, datcollate %s, datctype %s, %s (TestCaseFold1209MeasurePostgres)\n", ver, collate, ctype, enc)
	for _, c := range cfCells() {
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
	if err := os.WriteFile(cfPGFile, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestCaseFold1209EveryArm runs every cell on the five arms against
// PostgreSQL's answer. CF_DUMP=<file> writes every arm's answer per cell.
func TestCaseFold1209EveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the case-folding table")
	}
	cells := cfCells()
	pg := rnReadTSV(t, cfPGFile, 2)
	for _, c := range cells {
		if _, ok := pg[c.name]; !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", c.name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	arms := rxArms(t, ctx, cfTables())
	var dump *os.File
	if p := os.Getenv("CF_DUMP"); p != "" {
		f, err := os.Create(p)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		dump = f
	}
	var dumpMu sync.Mutex
	for _, tc := range cells {
		want := pg[tc.name][0]
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
				if !rnMatches(got[i], want) {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (PostgreSQL 17.11, --locale=C)", tc.sql, arm.name, got[i], want)
				}
			}
		})
	}
}
