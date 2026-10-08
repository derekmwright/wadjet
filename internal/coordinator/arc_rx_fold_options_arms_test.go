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

// CASE-INSENSITIVE MATCHING AND THE EMBEDDED OPTIONS, IN EVERY CONSTRUCT
// (#1499). Case-insensitivity — the `i` flag, `~*`, an
// embedded `(?i)` — matches each pattern letter's lower- and upper-case
// forms (towlower / towupper, as PostgreSQL does), whatever the script; the
// newline options m n p w and the expanded syntax x answer with
// PostgreSQL's meaning, embedded or as a flags argument. Construct × spelling
// × subject script / option on five arms against PostgreSQL 17.11; the
// engine's own DuckDB-origin functions (regexp_extract, regexp_split,
// payload_matches) read RE2 syntax and are asserted against their answer at
// base 4256886b (testdata/arc_rx_fold_options_re2.tsv), the regex engine's
// own Unicode folding and RE2's options.
//
//	rxf_t: id | s text | p text (a pattern per row, for the case cells)

var rxfRows = []rxRow{
	{"ÄBC", "äbc"},
	{"ПРИВЕТ", "привет"},
	{"ς", "σ"},
	{"Σ", "ς"},
	{"İ", "i"},
	{"i", "İ"},
	{"ẞ", "ß"},
	{"Ж", "[а-я]"},
	{"Ä", "[^ä]"},
	{"µ", "Μ"},
	{"ⓐ", "Ⓐ"},
	{"line1\nline2", nil},
	{"k: v\nk2: w", nil},
	{"a\nb", nil},
	{nil, nil},
}

func rxfTables() []tmdTable {
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
		{Name: "p", Type: parquet.TypeString, Nullable: true},
	}}
	var rows []map[string]any
	for k, r := range rxfRows {
		rows = append(rows, map[string]any{"id": int64(k + 1), "s": r.s, "p": r.p})
	}
	return []tmdTable{{"rxf_t", schema, rows}}
}

func rxfPGFixture() []string {
	out := []string{"DROP TABLE IF EXISTS rxf_t", "CREATE TABLE rxf_t (id bigint, s text, p text)"}
	lit := func(v any) string {
		if v == nil {
			return "NULL"
		}
		return "E'" + strings.NewReplacer(`\`, `\\`, "'", `\'`, "\n", `\n`).Replace(v.(string)) + "'"
	}
	for k, r := range rxfRows {
		out = append(out, fmt.Sprintf("INSERT INTO rxf_t VALUES (%d, %s, %s)", k+1, lit(r.s), lit(r.p)))
	}
	return out
}

func rxfKinds() []rxKind {
	out := []rxKind{
		// Case-insensitivity, every spelling and construct.
		{"fold/opI", "t.s ~* t.p", ""},
		{"fold/opNotI", "t.s !~* t.p", ""},
		{"fold/likeEmb", "regexp_like(t.s, '(?i)' || t.p)", ""},
		{"fold/likeFlag", "regexp_like(t.s, t.p, 'i')", ""},
		{"fold/countFlag", "regexp_count(t.s || ' ' || t.s, t.p, 1, 'i')", ""},
		{"fold/countEmb", "regexp_count(t.s || ' ' || t.s, '(?i)' || t.p)", ""},
		{"fold/substring", "substring(t.s FROM '(?i)' || t.p)", ""},
		{"fold/extract", "regexp_extract(t.s, '(?i)' || t.p)", "regexp_substr(t.s, '(?i)' || t.p)"},
		{"fold/payload", "payload_matches(t.s, '(?i)' || t.p)", "t.s ~* t.p"},
		{"fold/replace", "regexp_replace(t.s, t.p, '#', 'gi')", ""},
		{"fold/ilikeControl", "t.s ILIKE t.p", ""},
	}
	for _, o := range []string{"m", "n", "p", "w", "s"} {
		out = append(out,
			rxKind{"opt/" + o + "/likeEmb", "regexp_like(t.s, '(?" + o + ")^line2$')", ""},
			rxKind{"opt/" + o + "/likeFlag", "regexp_like(t.s, '^line2$', '" + o + "')", ""},
			rxKind{"opt/" + o + "/dot", "regexp_like(t.s, '(?" + o + ")a.b')", ""},
			rxKind{"opt/" + o + "/negBracket", "regexp_like(t.s, 'a[^x]b', '" + o + "')", ""},
			rxKind{"opt/" + o + "/substring", "substring(t.s FROM '(?" + o + ")^k2: (.*)$')", ""},
			rxKind{"opt/" + o + "/count", "regexp_count(t.s, '(?" + o + ")^.')", ""},
			rxKind{"opt/" + o + "/countFlag", "regexp_count(t.s, '$', 1, '" + o + "')", ""},
			rxKind{"opt/" + o + "/extract", "regexp_extract(t.s, '(?" + o + ")^line2')", "regexp_substr(t.s, '(?" + o + ")^line2')"},
			rxKind{"opt/" + o + "/split", "regexp_split(t.s, '(?" + o + ")^l')", "array_to_json(regexp_split_to_array(t.s, '(?" + o + ")^l'))::text"},
			rxKind{"opt/" + o + "/payload", "payload_matches(t.s, '(?" + o + ")^line2')", "t.s ~ '(?" + o + ")^line2'"},
			rxKind{"opt/" + o + "/op", "t.s ~ '(?" + o + ")^b'", ""},
			rxKind{"opt/" + o + "/replace", "regexp_replace(t.s, '^.', '#', 'g" + o + "')", ""},
		)
	}
	return append(out,
		rxKind{"opt/x/likeEmb", "regexp_like(t.s, '(?x) ^ l i n e 1 # a comment')", ""},
		rxKind{"opt/x/likeFlag", "regexp_like(t.s, 'k2 : \\  w', 'x')", ""},
		rxKind{"opt/x/bracket", "regexp_like(t.s, '(?x)a[\n]b')", ""},
		rxKind{"opt/x/substring", "substring(t.s FROM '(?x) (l i n e) 1')", ""},
		rxKind{"opt/xi/likeEmb", "regexp_like(t.s, '(?xi) Ä b c')", ""},
		rxKind{"opt/b/refused", "regexp_like(t.s, '(?b)a')", ""},
		rxKind{"opt/e/refused", "regexp_like(t.s, 'a', 'e')", ""},
	)
}

func rxfCells() []rxCell {
	var out []rxCell
	for _, k := range rxfKinds() {
		pgExpr := k.pg
		if pgExpr == "" {
			pgExpr = k.expr
		}
		for _, c := range []struct{ consumer, tmpl string }{
			{"proj", "SELECT t.id, %s AS v FROM rxf_t t"},
			{"group", "SELECT %s AS k, count(*) AS c FROM rxf_t t GROUP BY 1"},
			{"where", "SELECT t.id FROM rxf_t t WHERE CAST(%s AS TEXT) IN ('true', 't', '1', '2', 'w', 'line2', 'ÄBC', 'ς', 'Σ', 'i', 'ẞ', 'Ж')"},
		} {
			out = append(out, rxCell{name: k.name + "/" + c.consumer, sql: fmt.Sprintf(c.tmpl, k.expr), pg: fmt.Sprintf(c.tmpl, pgExpr), own: rxOwnFunction(k.expr)})
		}
	}
	return out
}

const rxfPGFile = "testdata/arc_rx_fold_options_pg17.tsv"
const rxfKeptFile = "testdata/arc_rx_fold_options_kept.tsv"
const rxfRE2File = "testdata/arc_rx_fold_options_re2.tsv"

// TestArcRXFoldOptionsMeasurePostgres measures every cell on PostgreSQL
// 17.11 (RX_PG_DSN=postgres://…) and writes the answer file.
func TestArcRXFoldOptionsMeasurePostgres(t *testing.T) {
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
	for _, s := range append([]string{"SET statement_timeout = '30s'"}, rxfPGFixture()...) {
		if _, err := conn.Exec(ctx, s, pgx.QueryExecModeSimpleProtocol); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	var b strings.Builder
	b.WriteString("# arc RX round 2: PostgreSQL 17.11's answer for every cell (TestArcRXFoldOptionsMeasurePostgres)\n")
	for _, c := range rxfCells() {
		if c.own {
			continue
		}
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
	if err := os.WriteFile(rxfPGFile, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestArcRXFoldOptionsEveryArm runs every cell on the five arms against its
// oracle's answer (rxOracle: PostgreSQL 17.11, or base for a DuckDB-origin
// function), or a kept cell's catalogued answer.
func TestArcRXFoldOptionsEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the case-folding and option table")
	}
	kept := map[string][]rnKeep{}
	for name, p := range rnReadTSV(t, rxfKeptFile, 4) {
		kept[name] = append(kept[name], rnKeep{scope: p[0], want: p[1], why: p[2]})
	}
	cells := rxfCells()
	oracle := rxOracle(t, cells, rxfPGFile, rxfRE2File)
	names := map[string]bool{}
	for _, c := range cells {
		names[c.name] = true
	}
	for name := range kept {
		if !names[name] {
			t.Fatalf("kept cell %s is not in the table", name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	arms := rxArms(t, ctx, rxfTables())
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
		o := oracle[tc.name]
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
				want, why := o[0], o[1]
				for _, k := range ks {
					if k.scope == "all" || (k.scope == "dag") == strings.HasPrefix(arm.name, "dag") {
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
