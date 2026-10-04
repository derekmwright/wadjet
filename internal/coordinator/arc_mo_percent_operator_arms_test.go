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
)

// THE `%` OPERATOR IS MOD (#1527). PostgreSQL implements `a % b` and
// `mod(a, b)` by the same pg_proc entries (int2mod, int4mod, int8mod,
// numeric_mod), so the two spellings resolve their operands, declare their
// result, raise their errors and answer their values alike — measured on
// PostgreSQL 17.11 over every cell below, where the two differ only in the
// error message's wording. Here `a % b` parses to the mod() call, so it has
// MOD's typing and kernel on every path. Before that the operator had a
// second implementation that disagreed with MOD and with itself across the
// arms: `SUM(t.n % '2.5')` answered -1 on the single-process arms and 1.26
// on the DAG arms (PostgreSQL 1.26), `NULL % t.n` raised XX000 "integer
// divide by zero", `SUM(t.i % NULL)` answered 0 (PostgreSQL NULL).
//
// The table is operand kind (left × right) × consumer × spelling × value
// edge, each cell in both spellings. Every cell is asserted twice: its `%`
// spelling answers exactly what its MOD spelling answers on the same arm
// (one rule), and it answers PostgreSQL 17.11's answer
// (testdata/arc_mo_percent_operator_pg17.tsv) unless
// testdata/arc_mo_percent_operator_kept.tsv records it as a catalogued
// divergence or a recorded filing candidate, asserted as it stands.

type moSpelling struct{ name, op string }

var moSpellings = []moSpelling{{"pct", "%"}, {"mod", "mod"}}

func moSpell(op, a, b string) string {
	if op == "mod" {
		return "MOD(" + a + ", " + b + ")"
	}
	return a + " " + op + " " + b
}

type moOperand struct{ key, sql string }

var moLeft = []moOperand{{"i", "t.i"}, {"b", "t.b"}, {"n", "t.n"}, {"f", "t.f"}, {"l8", "8"}, {"ln", "7.5"},
	{"q75", "'7.5'"}, {"q8", "'8'"}, {"q0", "'0'"}, {"qabc", "'abc'"}, {"nul", "NULL"}, {"tnul", "CAST(NULL AS INT)"},
	{"sqi", "(SELECT v FROM ss_i WHERE id = 1)"}, {"sqn", "(SELECT m FROM ss_i WHERE id = 1)"}}

var moRight = []moOperand{{"i", "t.i"}, {"b", "t.b"}, {"n", "t.n"}, {"f", "t.f"}, {"l3", "3"}, {"l0", "0"}, {"ln", "2.5"},
	{"q25", "'2.5'"}, {"q3", "'3'"}, {"q0", "'0'"}, {"qabc", "'abc'"}, {"nul", "NULL"}, {"tnul", "CAST(NULL AS INT)"},
	{"sqi", "(SELECT v FROM ss_i WHERE id = 1)"}, {"sqn", "(SELECT m FROM ss_i WHERE id = 1)"}}

func moIsCol(k string) bool { return k == "i" || k == "b" || k == "n" || k == "f" }

// moFrom keeps row 4 (every column 0) out of a cell whose divisor is a
// column, so its other rows answer instead of the 22012 the zero raises;
// the u/ cells keep it.
func moFrom(rk string) string {
	if moIsCol(rk) {
		return "ss_t t WHERE t.id <> 4"
	}
	return "ss_t t"
}

// moOperandSQL is an operand's SQL by its key, read from the left kinds
// first for a dividend and from the right kinds for a divisor (`ln` is 7.5
// as a dividend and 2.5 as a divisor).
func moOperandSQL(k string, divisor bool) string {
	lists := [][]moOperand{moLeft, moRight}
	if divisor {
		lists = [][]moOperand{moRight}
	}
	for _, l := range lists {
		for _, o := range l {
			if o.key == k {
				return o.sql
			}
		}
	}
	panic("operand " + k)
}

func moCells() []nxCell {
	var out []nxCell
	add := func(name string, ordered bool, sql string) {
		w := strings.Contains(sql, "CREATE TABLE")
		out = append(out, nxCell{name: name, sql: sql, ordered: ordered, embedded: w})
	}
	// The grid: projection and SUM over every operand pair.
	for _, l := range moLeft {
		for _, r := range moRight {
			for _, s := range moSpellings {
				e := moSpell(s.op, l.sql, r.sql)
				add(fmt.Sprintf("g/proj/%s_%s/%s", l.key, r.key, s.name), true,
					fmt.Sprintf("SELECT t.id, %s FROM %s ORDER BY t.id", e, moFrom(r.key)))
				add(fmt.Sprintf("g/sum/%s_%s/%s", l.key, r.key, s.name), true,
					fmt.Sprintf("SELECT SUM(%s) FROM %s", e, moFrom(r.key)))
			}
		}
	}
	// Every consumer over the pairs the issue names and their neighbours.
	for _, p := range [][2]string{{"n", "q25"}, {"i", "q25"}, {"l8", "q25"}, {"i", "q3"}, {"nul", "n"}, {"nul", "i"},
		{"i", "nul"}, {"n", "nul"}, {"ln", "nul"}, {"n", "q0"}, {"q8", "n"}, {"i", "l3"}, {"b", "l3"}, {"n", "ln"},
		{"i", "ln"}, {"b", "ln"}, {"tnul", "i"}, {"i", "tnul"}, {"sqi", "i"}, {"n", "sqn"}, {"i", "i"}, {"q75", "i"}, {"f", "ln"}} {
		lk, rk := p[0], p[1]
		for _, s := range moSpellings {
			e := moSpell(s.op, moOperandSQL(lk, false), moOperandSQL(rk, true))
			k := fmt.Sprintf("%s_%s/%s", lk, rk, s.name)
			tag := fmt.Sprintf("%s_%s_%s", lk, rk, s.name)
			f := moFrom(rk)
			fw := "ss_t t WHERE"
			if moIsCol(rk) {
				fw = "ss_t t WHERE t.id <> 4 AND"
			}
			add("c/avg/"+k, true, fmt.Sprintf("SELECT AVG(%s) FROM %s", e, f))
			add("c/minmax/"+k, true, fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", e, e, f))
			add("c/where/"+k, true, fmt.Sprintf("SELECT t.id FROM %s (%s) > 0 ORDER BY t.id", fw, e))
			add("c/group/"+k, false, fmt.Sprintf("SELECT %s AS k, COUNT(*) FROM %s GROUP BY 1", e, f))
			add("c/order/"+k, true, fmt.Sprintf("SELECT t.id FROM %s ORDER BY %s, t.id", f, e))
			add("c/ctas/"+k, true, fmt.Sprintf("DROP TABLE IF EXISTS mo_c_%s ;; CREATE TABLE mo_c_%s AS SELECT t.id, %s AS v FROM %s ;; SELECT * FROM mo_c_%s ORDER BY id", tag, tag, e, f, tag))
			for _, ty := range [][2]string{{"int", "INTEGER"}, {"num", "NUMERIC(10,2)"}, {"dbl", "DOUBLE PRECISION"}} {
				tn := ty[0]
				add("c/ins"+tn+"/"+k, true, fmt.Sprintf("DROP TABLE IF EXISTS mo_%s_%s ;; CREATE TABLE mo_%s_%s (id BIGINT, v %s) ;; INSERT INTO mo_%s_%s SELECT t.id, %s FROM %s ;; SELECT * FROM mo_%s_%s ORDER BY id",
					tn, tag, tn, tag, ty[1], tn, tag, e, f, tn, tag))
			}
			add("c/win/"+k, true, fmt.Sprintf("SELECT t.id, SUM(%s) OVER (ORDER BY t.id) FROM %s ORDER BY t.id", e, f))
			add("c/case/"+k, true, fmt.Sprintf("SELECT t.id, CASE WHEN t.id > 2 THEN %s ELSE NULL END FROM %s ORDER BY t.id", e, f))
		}
	}
	// Column ORIGINS: each publishes its declaration through its own reader.
	for _, og := range [][2]string{
		{"dt", "(SELECT id, i, n FROM ss_t) t"},
		{"cte", "WITH c AS (SELECT id, i, n FROM ss_t) SELECT {SEL} FROM c t"},
		{"vals", "(VALUES (1, 3, 2.25), (2, -7, -3.5), (3, NULL, NULL)) AS t(id, i, n)"},
		{"union", "(SELECT id, i, n FROM ss_t WHERE id <= 3 UNION ALL SELECT id, i, n FROM ss_t WHERE id > 3) t"},
		{"win", "(SELECT id, SUM(i) OVER (ORDER BY id) AS i, n FROM ss_t) t"},
	} {
		for _, ex := range [][3]string{{"n_q25", "t.n", "'2.5'"}, {"i_q25", "t.i", "'2.5'"}, {"nul_n", "NULL", "t.n"}, {"i_nul", "t.i", "NULL"}, {"n_q0", "t.n", "'0'"}} {
			for _, s := range moSpellings {
				e := moSpell(s.op, ex[1], ex[2])
				one := func(sel string) string {
					if strings.Contains(og[1], "{SEL}") {
						return strings.Replace(og[1], "{SEL}", sel, 1)
					}
					return "SELECT " + sel + " FROM " + og[1]
				}
				add(fmt.Sprintf("o/%s/%s/%s", og[0], ex[0], s.name), true, one("t.id, "+e)+" ORDER BY t.id")
				add(fmt.Sprintf("o/%s/%s/sum/%s", og[0], ex[0], s.name), true, one("SUM("+e+")"))
			}
		}
	}
	// Value edges: a zero divisor (22012), the sign (the dividend's), the
	// MinInt % -1 rows (0), NaN and the infinities.
	for _, v := range [][3]string{
		{"z_ii", "8", "0"}, {"z_ni", "t.n", "0"}, {"z_nn", "t.n", "0.0"}, {"z_bi", "t.b", "0"}, {"z_ln", "2.5", "0"}, {"z_li", "8", "t.i"},
		{"z_ii0", "t.i", "0"}, {"z_nq", "t.n", "'0'"}, {"z_qn", "'8'", "t.n"}, {"z_lq0", "8", "'0'"}, {"z_fl", "t.f", "0"},
		{"s_nl", "-7", "3"}, {"s_ln", "7", "-3"}, {"s_nn", "-7", "-3"}, {"s_ci", "t.i", "-3"}, {"s_fn", "-7.5", "2"}, {"s_cn", "t.n", "-2.5"}, {"s_bn", "t.b", "-7"},
		{"m_i8", "(-9223372036854775807 - 1)", "-1"}, {"m_cb", "CAST(-9223372036854775808 AS BIGINT)", "-1"}, {"m_i4", "(-2147483647 - 1)", "-1"},
		{"m_i2", "CAST(-32768 AS SMALLINT)", "CAST(-1 AS SMALLINT)"}, {"m_i4c", "CAST(-2147483648 AS INT)", "-1"}, {"m_bigq", "9223372036854775807", "'2'"},
		{"x_fnan", "CAST('NaN' AS DOUBLE PRECISION)", "2"}, {"x_finf", "CAST('Infinity' AS DOUBLE PRECISION)", "2"}, {"x_nnan", "CAST('NaN' AS NUMERIC)", "2"},
		{"x_ninf", "CAST('Infinity' AS NUMERIC)", "2"}, {"x_rninf", "5", "CAST('Infinity' AS NUMERIC)"}, {"x_rfinf", "5", "CAST('Infinity' AS DOUBLE PRECISION)"},
		{"x_ff", "5.5::float8", "2.0::float8"},
	} {
		for _, s := range moSpellings {
			e := moSpell(s.op, v[1], v[2])
			if strings.Contains(e, "t.") {
				add(fmt.Sprintf("e/%s/%s", v[0], s.name), true, "SELECT t.id, "+e+" FROM ss_t t ORDER BY t.id")
			} else {
				add(fmt.Sprintf("e/%s/%s", v[0], s.name), true, "SELECT "+e)
			}
		}
	}
	// A column divisor over every row, row 4's zero included.
	for _, l := range moLeft {
		for _, rk := range []string{"i", "b", "n"} {
			for _, s := range moSpellings {
				e := moSpell(s.op, l.sql, moOperandSQL(rk, true))
				tag := fmt.Sprintf("%s_%s_%s", l.key, rk, s.name)
				add(fmt.Sprintf("u/proj/%s_%s/%s", l.key, rk, s.name), true, "SELECT t.id, "+e+" FROM ss_t t ORDER BY t.id")
				add(fmt.Sprintf("u/sum/%s_%s/%s", l.key, rk, s.name), true, "SELECT SUM("+e+") FROM ss_t t")
				add(fmt.Sprintf("u/ctas/%s_%s/%s", l.key, rk, s.name), true, fmt.Sprintf("DROP TABLE IF EXISTS mo_u_%s ;; CREATE TABLE mo_u_%s AS SELECT t.id, %s AS v FROM ss_t t ;; SELECT * FROM mo_u_%s ORDER BY id", tag, tag, e, tag))
			}
		}
	}
	// Derived operand shapes.
	for _, x := range [][3]string{
		{"ci2", "CAST(t.i AS SMALLINT)", "3"}, {"cl8", "CAST(8 AS INT)", "3"}, {"cb", "CAST(t.i AS BIGINT)", "3"}, {"neg", "-t.i", "3"},
		{"add", "(t.i + 1)", "3"}, {"abs", "ABS(t.i)", "3"}, {"cn", "CAST(t.i AS NUMERIC)", "3"}, {"nn", "t.n", "t.n"}, {"ib", "t.i", "t.b"},
		{"bi", "t.b", "t.i"}, {"in", "t.i", "t.n"}, {"ni", "t.n", "t.i"}, {"pctpct", "t.i % 3", "2"}, {"nest", "MOD(t.i, 3)", "2"},
		{"l8neg", "8", "-3"}, {"q2i", "t.i", "'-3'"}, {"qsp", "t.i", "' 3 '"}, {"qe", "t.n", "'1e1'"},
	} {
		for _, s := range moSpellings {
			add(fmt.Sprintf("x/%s/%s", x[0], s.name), true,
				"SELECT t.id, "+moSpell(s.op, x[1], x[2])+" FROM ss_t t WHERE t.id <> 4 ORDER BY t.id")
		}
	}
	return out
}

// TestArcMOGenerate dumps the cells as name<TAB>ordered<TAB>sql for the
// oracle run (MO_GEN=<path>).
func TestArcMOGenerate(t *testing.T) {
	path := os.Getenv("MO_GEN")
	if path == "" {
		t.Skip("MO_GEN unset")
	}
	var b strings.Builder
	for _, c := range moCells() {
		fmt.Fprintf(&b, "%s\t%t\t%s\n", c.name, c.ordered, c.sql)
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// moAggregate names a cell whose answer is a SUM, AVG or window SUM of
// doubles: its last bits depend on the order partial states meet (ADR-0013),
// so it matches a double within nxFloatSumClose's few units in the last place.
func moAggregate(name string) bool {
	return strings.Contains(name, "/sum/") || strings.Contains(name, "/avg/") || strings.Contains(name, "/win/") ||
		strings.HasSuffix(name, "/sum/pct") || strings.HasSuffix(name, "/sum/mod")
}

func moMatches(name, got, want string) bool {
	return nxMatches(got, want) || moAggregate(name) && nxFloatSumClose(got, want)
}

func TestArcMOPercentIsModEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the % / MOD table")
	}
	answers := nxReadTSV(t, "testdata/arc_mo_percent_operator_pg17.tsv", 2)
	kept := nxReadTSV(t, "testdata/arc_mo_percent_operator_kept.tsv", 4)
	cells := moCells()
	names := map[string]bool{}
	for _, c := range cells {
		if _, ok := answers[c.name]; !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", c.name)
		}
		names[c.name] = true
	}
	for name, ks := range kept {
		if !names[name] {
			t.Fatalf("kept cell %s is not in the table", name)
		}
		for _, k := range ks {
			if k[0] != "all" && k[0] != "dag" && k[0] != "single" {
				t.Fatalf("kept cell %s: arms %q", name, k[0])
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	t.Cleanup(cancel)
	arms := ssArms(t, ctx)
	got := map[string][]string{}
	var mu sync.Mutex
	for _, tc := range cells {
		g := make([]string, len(arms))
		var wg sync.WaitGroup
		for i, arm := range arms {
			if tc.embedded && strings.HasPrefix(arm.name, "dag") {
				continue // the DAG arms have no CREATE TABLE AS / INSERT … SELECT
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				g[i] = nxRunCell(arm, tc)
			}()
		}
		wg.Wait()
		mu.Lock()
		got[tc.name] = g
		mu.Unlock()
	}
	if p := os.Getenv("MO_DUMP"); p != "" {
		var b strings.Builder
		for _, tc := range cells {
			for i, arm := range arms {
				if got[tc.name][i] != "" {
					fmt.Fprintf(&b, "%s\t%s\t%s\n", tc.name, arm.name, got[tc.name][i])
				}
			}
		}
		if err := os.WriteFile(p, []byte(b.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range cells {
		pgWant := answers[tc.name][0][0]
		ks := kept[tc.name]
		t.Run(tc.name, func(t *testing.T) {
			for i, arm := range arms {
				g := got[tc.name][i]
				if g == "" {
					continue
				}
				if strings.HasPrefix(g, "ERR XX000") || strings.HasPrefix(g, "ERR  ") {
					t.Errorf("%s\n  arm %s: an internal or uncoded error is never an answer: %s", tc.sql, arm.name, g)
				}
				if twin, ok := strings.CutSuffix(tc.name, "/pct"); ok {
					m := got[twin+"/mod"][i]
					if !moMatches(tc.name, g, m) {
						t.Errorf("%s\n  arm  %s\n  %%    %s\n  MOD  %s (the operator is MOD)", tc.sql, arm.name, g, m)
					}
				}
				want, why := pgWant, "PostgreSQL 17.11"
				dag := strings.HasPrefix(arm.name, "dag")
				for _, k := range ks {
					if k[0] == "all" || k[0] == "dag" && dag || k[0] == "single" && !dag {
						want, why = k[1], "kept: "+k[2]
					}
				}
				if !moMatches(tc.name, g, want) {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (%s)", tc.sql, arm.name, g, want, why)
				}
			}
		})
	}
}
