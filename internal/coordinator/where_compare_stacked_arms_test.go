// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// TWO AND THREE CORRELATED SCALAR SUBQUERIES IN ONE WHERE (OR HAVING), ON ALL
// FIVE ARMS (#1701).
//
// Each decorrelated scalar subquery of one Filter becomes its own LEFT join
// against an aggregate, the joins stacked one above the other, and the Filter
// reads every `__scalar_N` slot above the top one. On the single-process path
// the column-needs partition at the upper join sent the LOWER join's slot to
// neither side, the lower join's probe OutputFilter dropped the column, and
// the Filter's reference to it read NULL: the first conjunct of two (the first
// two of three) compared against NULL (logical.decorrelatedScalarSlots). The
// stage-DAG arms answered PostgreSQL's rows. Every want is PostgreSQL 17.11's
// (testdata/where_compare_stacked_pg17.tsv, regenerated with
// WC_PG_DSN=<dsn> -run TestWhereCompareStackedGenerate); the fixture is
// wcTables / wcPGFixture.

// wcStackedCells is the enumeration.
//
// Pairs: body x body over (wc_i, wc_i) and (wc_i, wc_j), two comparison pairs
// each (rotating through = >=, >= =, < >, > <), four outer inputs, both orders
// of the conjuncts, in WHERE and in HAVING. Triples: six body triples over one
// table and over two, two comparison triples, four inputs, forward and
// reversed, WHERE and HAVING. Then round 2's five statements by name.
func wcStackedCells() []gsCell {
	bodies := []struct{ name, sel, tail string }{
		{"count", "count(*)", ""},
		{"countPlus2", "count(*) + 2", ""},
		{"sum", "sum(x.c)", ""},
		{"max", "max(x.c)", ""},
		{"approx", "approx_distinct(x.c)", ""},
		{"gkCount", "count(*)", " GROUP BY x.k"},
	}
	body := func(i int, table string) string {
		b := bodies[i]
		return "SELECT " + b.sel + " FROM " + table + " x WHERE x.k = o.k" + b.tail
	}
	tableTag := map[string]string{"wc_i": "i", "wc_j": "j"}
	cmpPairs := [][2]string{{"=", ">="}, {">=", "="}, {"<", ">"}, {">", "<"}}
	cmpName := map[string]string{"=": "eq", "<": "lt", ">": "gt", ">=": "ge"}
	inputs := []struct{ name, table string }{
		{"m1", "wc_m1"}, {"mn", "wc_mn"}, {"nm", "wc_nm"}, {"nk", "wc_nk"},
	}
	ctxs := []struct{ name, tmpl string }{
		{"where", "SELECT o.k, o.p FROM {O} o WHERE {X}"},
		{"having", "SELECT o.k, o.p FROM {O} o GROUP BY o.k, o.p HAVING {X}"},
	}
	var out []gsCell
	emit := func(name string, conj []string) {
		orders := []struct {
			tag  string
			list []string
		}{{"fwd", conj}}
		rev := make([]string, len(conj))
		for i := range conj {
			rev[i] = conj[len(conj)-1-i]
		}
		orders = append(orders, struct {
			tag  string
			list []string
		}{"rev", rev})
		for _, x := range ctxs {
			for _, ord := range orders {
				for _, in := range inputs {
					sql := strings.NewReplacer("{X}", strings.Join(ord.list, " AND "), "{O}", in.table).Replace(x.tmpl)
					out = append(out, gsCell{name + "/" + x.name + "/" + ord.tag + "/" + in.name, sql})
				}
			}
		}
	}
	conjunct := func(op, sub string) string { return "o.p " + op + " (" + sub + ")" }

	rot := 0
	for b1 := range bodies {
		for b2 := range bodies {
			for _, t2 := range []string{"wc_i", "wc_j"} {
				for n := 0; n < 2; n++ {
					cp := cmpPairs[rot%len(cmpPairs)]
					rot++
					name := fmt.Sprintf("stk2/%s.i+%s.%s/%s-%s", bodies[b1].name, bodies[b2].name, tableTag[t2],
						cmpName[cp[0]], cmpName[cp[1]])
					emit(name, []string{conjunct(cp[0], body(b1, "wc_i")), conjunct(cp[1], body(b2, t2))})
				}
			}
		}
	}

	idx := map[string]int{}
	for i, b := range bodies {
		idx[b.name] = i
	}
	triples := [][3]string{
		{"count", "max", "sum"},
		{"gkCount", "countPlus2", "approx"},
		{"max", "max", "max"},
		{"sum", "count", "gkCount"},
		{"approx", "sum", "count"},
		{"countPlus2", "gkCount", "max"},
	}
	cmpTriples := [][3]string{{"=", ">=", ">="}, {">", "<", ">="}}
	for _, tr := range triples {
		for _, tables := range [][3]string{{"wc_i", "wc_i", "wc_i"}, {"wc_i", "wc_j", "wc_i"}} {
			for _, ct := range cmpTriples {
				name := fmt.Sprintf("stk3/%s.%s+%s.%s+%s.%s/%s-%s-%s",
					tr[0], tableTag[tables[0]], tr[1], tableTag[tables[1]], tr[2], tableTag[tables[2]],
					cmpName[ct[0]], cmpName[ct[1]], cmpName[ct[2]])
				var conj []string
				for i := 0; i < 3; i++ {
					conj = append(conj, conjunct(ct[i], body(idx[tr[i]], tables[i])))
				}
				emit(name, conj)
			}
		}
	}

	// Round 2's five statements, as written there.
	const cnt = "SELECT count(*) FROM wc_i x WHERE x.k = o.k"
	const mx = "SELECT max(x.c) FROM wc_i x WHERE x.k = o.k"
	out = append(out,
		gsCell{"r2/s1", "SELECT o.k, o.p FROM wc_m1 o WHERE o.p > (" + cnt + ") AND o.p < (SELECT count(*) + 2 FROM wc_i x WHERE x.k = o.k)"},
		gsCell{"r2/s2", "SELECT o.k, o.p FROM wc_mn o WHERE o.p > (" + cnt + ") AND o.p < (SELECT count(*) + 2 FROM wc_i x WHERE x.k = o.k)"},
		gsCell{"r2/s3", "SELECT o.k, o.p FROM wc_mn o WHERE o.p = (" + cnt + ") AND 0 <= (" + cnt + ")"},
		gsCell{"r2/s4", "SELECT o.k, o.p FROM wc_mn o WHERE 0 <= (" + cnt + ") AND o.p = (" + cnt + ")"},
		gsCell{"r2/s5", "SELECT o.k, o.p FROM wc_mn o WHERE o.p = (" + mx + ") AND 0 <= (" + mx + ")"},
	)
	// Neighbours of the stacked joins: a derived block and its enclosing query
	// each minting their own `__scalar_0`; the stacked joins over a written
	// join, beside an EXISTS or an IN, under a select-list scalar subquery and
	// under a GROUP BY.
	out = append(out,
		gsCell{"nb/derivedOwnSlot", "SELECT d.k, d.p FROM (SELECT o.k, o.p FROM wc_mn o WHERE o.p >= (SELECT min(x.c) FROM wc_i x WHERE x.k = o.k)) d WHERE d.p = (SELECT count(*) FROM wc_i x WHERE x.k = d.k)"},
		gsCell{"nb/derivedStacked", "SELECT d.k, d.p FROM (SELECT o.k, o.p FROM wc_mn o WHERE o.p >= (SELECT min(x.c) FROM wc_i x WHERE x.k = o.k) AND o.p <= (SELECT max(x.c) FROM wc_i x WHERE x.k = o.k)) d WHERE d.p = (SELECT count(*) FROM wc_j x WHERE x.k = d.k) + 1"},
		gsCell{"nb/overJoin", "SELECT o.k, o.p, m.p FROM wc_mn o JOIN wc_m1 m ON m.p = o.p WHERE o.p = (" + mx + ") AND m.p >= (SELECT max(x.c) FROM wc_i x WHERE x.k = m.k)"},
		gsCell{"nb/besideExists", "SELECT o.k, o.p FROM wc_mn o WHERE o.p = (" + mx + ") AND EXISTS (SELECT 1 FROM wc_j y WHERE y.k = o.k) AND 0 <= (SELECT count(*) FROM wc_j x WHERE x.k = o.k)"},
		gsCell{"nb/selectListScalar", "SELECT o.k, o.p, (" + mx + ") AS s FROM wc_mn o WHERE o.p = (" + mx + ") AND 0 <= (SELECT count(*) FROM wc_j x WHERE x.k = o.k)"},
		gsCell{"nb/underGroupBy", "SELECT o.k, count(*) FROM wc_mn o WHERE o.p >= (SELECT min(x.c) FROM wc_i x WHERE x.k = o.k) AND o.p <= (SELECT max(x.c) FROM wc_j x WHERE x.k = o.k) GROUP BY o.k"},
		gsCell{"nb/besideIn", "SELECT o.k, o.p FROM wc_mn o WHERE o.p = (" + mx + ") AND o.p IN (SELECT y.c + 1 FROM wc_j y WHERE y.k = o.k) AND 0 <= (SELECT count(*) FROM wc_j x WHERE x.k = o.k)"},
	)
	return out
}

// TestWhereCompareStackedGenerate measures PostgreSQL 17.11 for every stacked
// cell (WC_PG_DSN=postgres://...) and writes the answer table.
func TestWhereCompareStackedGenerate(t *testing.T) {
	dsn := os.Getenv("WC_PG_DSN")
	if dsn == "" {
		t.Skip("WC_PG_DSN unset")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "SET statement_timeout = '30s'"); err != nil {
		t.Fatal(err)
	}
	for _, stmt := range strings.Split(wcPGFixture, ";") {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	var b strings.Builder
	b.WriteString("# name\tPostgreSQL 17.11 answer (sorted rows joined by \" | \"; ERR <SQLSTATE> for a refusal)\n")
	for _, c := range wcStackedCells() {
		fmt.Fprintf(&b, "%s\t%s\n", c.name, wcPGAnswer(ctx, conn, wcPGStatement(c.sql)))
	}
	if err := os.WriteFile("testdata/where_compare_stacked_pg17.tsv", []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// wcPGAnswer is one statement's PostgreSQL answer, rendered as gsRender does.
func wcPGAnswer(ctx context.Context, conn *pgx.Conn, sql string) string {
	rows, err := conn.Query(ctx, sql)
	if err != nil {
		return gsPGErr(err)
	}
	defer rows.Close()
	var all [][]any
	for rows.Next() {
		v, err := rows.Values()
		if err != nil {
			return gsPGErr(err)
		}
		for j := range v {
			if n, ok := v[j].(pgtype.Numeric); ok {
				if n.Valid {
					dv, _ := n.Value()
					v[j] = dv
				} else {
					v[j] = nil
				}
			}
		}
		all = append(all, v)
	}
	if err := rows.Err(); err != nil {
		return gsPGErr(err)
	}
	return gsRender(all)
}

// TestWhereCompareStackedEveryArm is the stacked table on five arms.
// WC_MEASURE=1 logs every cell instead of asserting; WC_ONLY=<regexp> narrows
// it.
func TestWhereCompareStackedEveryArm(t *testing.T) {
	if testing.Short() && os.Getenv("WC_ARMS") == "" {
		t.Skip("-short: five arms over the stacked scalar subquery table (WC_ARMS=1 runs it)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	answers := wcStackedPGAnswers(t)
	arms := wcArms(t, ctx)
	measure := os.Getenv("WC_MEASURE") != ""
	var only *regexp.Regexp
	if s := os.Getenv("WC_ONLY"); s != "" {
		only = regexp.MustCompile(s)
	}
	for _, c := range wcStackedCells() {
		if only != nil && !only.MatchString(c.name) {
			continue
		}
		pg, ok := answers[c.name]
		if !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", c.name)
		}
		for _, arm := range arms {
			got := arm.run(c.sql)
			if measure {
				status := "RIGHT"
				if got != pg {
					status = "DIFF"
				}
				t.Logf("CELL\t%s\t%s\t%s\tgot=%s\tpg=%s", status, c.name, arm.name, got, pg)
				continue
			}
			if k, ok := wcStackedKept[c.name][arm.name]; ok {
				if got != k {
					t.Errorf("%s [%s]: %s\n got    %s\n pinned %s (a residual: %s; PostgreSQL 17.11 answers %s)",
						c.name, arm.name, c.sql, got, k, wcStackedKeptWhy[c.name], pg)
				}
				continue
			}
			if got != pg {
				t.Errorf("%s [%s]: %s\n got  %s\n want %s (PostgreSQL 17.11)", c.name, arm.name, c.sql, got, pg)
			}
		}
	}
}

// wcStackedKept is a cell an arm answers differently from PostgreSQL 17.11 at
// the base (039245c9, and 1237d768 before #1701) and at the fix alike, pinned
// at that arm's answer.
var wcStackedKept = map[string]map[string]string{
	"nb/derivedOwnSlot": {"dag": "1,1", "dag-shuffled": "1,1", "dag-morsel4": "1,1"},
}

var wcStackedKeptWhy = map[string]string{
	"nb/derivedOwnSlot": "the derived block's Filter and the enclosing Filter each mint their own __scalar_0, " +
		"and the stage-DAG arms answer as if the enclosing predicate read the derived block's min (1,1, not 1,3); " +
		"inferred from the answer, not traced (a #1701 line)",
}

func wcStackedPGAnswers(t *testing.T) map[string]string {
	t.Helper()
	b, err := os.ReadFile("testdata/where_compare_stacked_pg17.tsv")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, line := range strings.Split(string(b), "\n") {
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
