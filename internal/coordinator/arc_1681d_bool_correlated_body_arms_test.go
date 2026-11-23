// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
)

// The 1681d matrix (epic #1681): a comparison or test over BOOLEAN values
// inside a correlated subquery body, over the 1681a fixture (nr_p / nr_s,
// whose c_b holds false for v<6, true for v>=6 and NULL where v is NULL, on
// both sides). Every cell projects integers only.

// bcCorrConstructs are the subquery spellings over a body predicate {R} that
// names p.<cols> and s.<cols>: the eight correlated constructs the brief
// names, plus a keyless EXISTS.
func bcCorrConstructs() []struct{ name, f string } {
	return []struct{ name, f string }{
		{"in", "SELECT p.rid FROM nr_p p WHERE p.id IN (SELECT s.id FROM nr_s s WHERE {R})"},
		{"notIn", "SELECT p.rid FROM nr_p p WHERE p.id NOT IN (SELECT s.id FROM nr_s s WHERE {R})"},
		{"eqAny", "SELECT p.rid FROM nr_p p WHERE p.id = ANY (SELECT s.id FROM nr_s s WHERE {R})"},
		{"neAll", "SELECT p.rid FROM nr_p p WHERE p.id <> ALL (SELECT s.id FROM nr_s s WHERE {R})"},
		{"exists", "SELECT p.rid FROM nr_p p WHERE EXISTS (SELECT 1 FROM nr_s s WHERE s.id = p.id AND {R})"},
		{"notExists", "SELECT p.rid FROM nr_p p WHERE NOT EXISTS (SELECT 1 FROM nr_s s WHERE s.id = p.id AND {R})"},
		{"keylessExists", "SELECT p.rid FROM nr_p p WHERE EXISTS (SELECT 1 FROM nr_s s WHERE {R})"},
		{"scalar", "SELECT p.rid, (SELECT count(*) FROM nr_s s WHERE {R}) FROM nr_p p"},
		{"lateral", "SELECT p.rid, x.n FROM nr_p p CROSS JOIN LATERAL (SELECT count(*) AS n FROM nr_s s WHERE {R}) x"},
	}
}

// bcLitContexts place an inner-only predicate {P} in a plain filter, a join,
// an uncorrelated IN, a keyed EXISTS / NOT EXISTS, a per-row correlated IN
// (the companion `s.w < p.w` keeps the body correlated) and a LATERAL.
func bcLitContexts() []struct{ name, f string } {
	return []struct{ name, f string }{
		{"plain", "SELECT s.sid FROM nr_s s WHERE {P}"},
		{"join", "SELECT p.rid, s.sid FROM nr_p p JOIN nr_s s ON s.id = p.id WHERE {P}"},
		{"uncorrIn", "SELECT p.rid FROM nr_p p WHERE p.id IN (SELECT s.id FROM nr_s s WHERE {P})"},
		{"exists", "SELECT p.rid FROM nr_p p WHERE EXISTS (SELECT 1 FROM nr_s s WHERE s.id = p.id AND {P})"},
		{"notExists", "SELECT p.rid FROM nr_p p WHERE NOT EXISTS (SELECT 1 FROM nr_s s WHERE s.id = p.id AND {P})"},
		{"corrIn", "SELECT p.rid FROM nr_p p WHERE p.id IN (SELECT s.id FROM nr_s s WHERE {P} AND s.w < p.w)"},
		{"lateral", "SELECT p.rid, x.n FROM nr_p p CROSS JOIN LATERAL (SELECT count(*) AS n FROM nr_s s WHERE {P} AND s.id = p.id) x"},
	}
}

func bcCells() []nrCell {
	var out []nrCell
	at := func(tmpl, key, v string) string { return strings.ReplaceAll(tmpl, key, v) }
	// corr/: the body compares the inner bool with the outer bool.
	corr := []struct{ name, r string }{
		{"gt", "s.c_b > p.c_b"}, {"ge", "s.c_b >= p.c_b"}, {"lt", "s.c_b < p.c_b"}, {"le", "s.c_b <= p.c_b"},
		{"eq", "s.c_b = p.c_b"}, {"ne", "s.c_b <> p.c_b"},
		{"idf", "s.c_b IS DISTINCT FROM p.c_b"}, {"indf", "s.c_b IS NOT DISTINCT FROM p.c_b"},
		{"flipGt", "p.c_b > s.c_b"}, {"flipLe", "p.c_b <= s.c_b"},
	}
	for _, r := range corr {
		for _, c := range bcCorrConstructs() {
			out = append(out, nrCell{"corr/" + r.name + "/" + c.name, at(c.f, "{R}", r.r)})
		}
	}
	// lit/: the inner bool against a literal, or tested.
	lits := []struct{ name, p string }{
		{"gtF", "s.c_b > false"}, {"gtT", "s.c_b > true"}, {"geF", "s.c_b >= false"}, {"geT", "s.c_b >= true"},
		{"ltF", "s.c_b < false"}, {"ltT", "s.c_b < true"}, {"leF", "s.c_b <= false"}, {"leT", "s.c_b <= true"},
		{"eqT", "s.c_b = true"}, {"eqF", "s.c_b = false"}, {"neT", "s.c_b <> true"}, {"neF", "s.c_b <> false"},
		{"flipLtF", "false < s.c_b"}, {"flipGtT", "true > s.c_b"}, {"flipGeT", "true >= s.c_b"},
		{"gtQuoted", "s.c_b > 'f'"}, {"leQuoted", "s.c_b <= 'no'"},
		{"isT", "s.c_b IS TRUE"}, {"isNotF", "s.c_b IS NOT FALSE"}, {"isF", "s.c_b IS FALSE"},
		{"isNotT", "s.c_b IS NOT TRUE"}, {"isUnk", "s.c_b IS UNKNOWN"},
		{"bare", "s.c_b"}, {"not", "NOT s.c_b"},
		{"gtOr", "(s.c_b > false OR s.sid = 0)"}, {"betweenFT", "s.c_b BETWEEN false AND true"},
		{"betweenTT", "s.c_b BETWEEN true AND true"},
	}
	for _, l := range lits {
		for _, c := range bcLitContexts() {
			out = append(out, nrCell{"lit/" + l.name + "/" + c.name, at(c.f, "{P}", l.p)})
		}
	}
	// expr/: a boolean EXPRESSION on one side.
	exprs := []struct{ name, r string }{
		{"innerExprGt", "(s.c_i32 > 6) > p.c_b"}, {"innerExprEq", "(s.c_i32 > 6) = p.c_b"},
		{"innerExprFlip", "p.c_b < (s.c_i32 > 6)"}, {"innerExprIdf", "(s.c_i32 > 6) IS DISTINCT FROM p.c_b"},
		{"outerExprGt", "s.c_b > (p.c_i32 > 6)"}, {"outerExprLe", "s.c_b <= (p.c_i32 > 6)"},
	}
	for _, e := range exprs {
		for _, c := range bcCorrConstructs()[:6] {
			out = append(out, nrCell{"expr/" + e.name + "/" + c.name, at(c.f, "{R}", e.r)})
		}
	}
	// outer/: every fixture type as an outer value substituted into a
	// per-row re-run of a correlated IN body and a LATERAL.
	for _, ty := range nrTypes() {
		for _, op := range []struct{ name, r string }{
			{"eq", "s.{c} = p.{c}"}, {"ne", "s.{c} <> p.{c}"}, {"ge", "s.{c} >= p.{c}"}, {"indf", "s.{c} IS NOT DISTINCT FROM p.{c}"},
		} {
			r := strings.ReplaceAll(op.r, "{c}", ty.col)
			out = append(out, nrCell{"outer/" + ty.col + "/" + op.name + "/in", at(bcCorrConstructs()[0].f, "{R}", r)})
		}
		r := "s." + ty.col + " = p." + ty.col
		out = append(out, nrCell{"outer/" + ty.col + "/eq/lateral", at(bcCorrConstructs()[8].f, "{R}", r)})
	}
	return out
}

// TestArc1681dGenerate dumps the cells (BC_GEN=<path>, name<TAB>sql) for the
// PostgreSQL oracle run; the fixture DDL is TestArc1681aGenerate's.
func TestArc1681dGenerate(t *testing.T) {
	path := os.Getenv("BC_GEN")
	if path == "" {
		t.Skip("BC_GEN unset")
	}
	var b strings.Builder
	for _, c := range bcCells() {
		b.WriteString(c.name + "\t" + c.sql + "\n")
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

func bcPGAnswers(t *testing.T) map[string]string {
	t.Helper()
	f, err := os.Open("testdata/arc_1681d_bool_correlated_body_pg17.tsv")
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

// bcKnown names every cell still not PostgreSQL's answer on an arm, with its
// verdict and cause. The test fails when a known cell starts answering as
// PostgreSQL does (delete its rule), changes verdict, or when any other cell
// is not PostgreSQL's.
func bcKnown(name, arm string) (verdict, cause string, known bool) {
	p := strings.Split(name, "/")
	if p[0] == "corr" && p[2] == "lateral" && p[1] != "eq" {
		return "refused", "an aggregated LATERAL body whose correlated predicate is not an equality is refused (the body publishes no column to evaluate it against)", true
	}
	if p[0] == "outer" && (p[1] == "c_f64" || p[1] == "c_f32") && p[3] == "in" && p[2] != "eq" {
		return "refused", "the per-row correlated re-run has no literal for a float outer value", true
	}
	return "", "", false
}

// A COMPARISON OVER BOOLEAN VALUES INSIDE A CORRELATED SUBQUERY BODY IS
// POSTGRESQL'S ANSWER ON EVERY ARM (epic #1681, line 1681d). The generated
// matrix (bcCells) against PostgreSQL 17.11's sorted rows, on the 1681a
// fixture and arms: single and spilled512k are the embedded engine
// (wadjet.DB) for every cell, the three DAG arms go through the coordinator.
//
// At 0ff2efdf the vectorized `column OP constant` filter over a BOOLEAN
// column (kernel.compareFilterBool) read only = and <>: >, >=, <, <= selected
// the FALSE rows. The per-row re-run of a correlated body spells its outer
// boolean as a `true` / `false` literal, so `s.c_b > p.c_b` became
// `s.c_b > false` and answered wrong on every arm (the DAG answers a per-row
// body on the coordinator's local pipeline); an inner-only `s.c_b > false`
// was wrong on the embedded engine only, because the DAG worker compiles a
// scan filter through the row evaluator, which ordered booleans right.
//
// The spilled arm asserts the LATERAL cells too. Until #1706's fourth site it
// was allowed to refuse them with `memory budget exceeded … forced by "spill
// tracking"` (bcBudgetBounded): every partitioned clone of the LATERAL
// count(*) charged a 4096-slot group-state chunk (about 107 KB for one group),
// six or seven clones held more than the 512 KiB budget, and which cells
// refused depended on how many had released before the join first reserved
// (4 to 7 of the eight `outer/<col>/eq/lateral` cells that ever refused in
// every free base run, 7 or 8 with the releases held). With the chunk bounded
// by the input estimate a clone charges about 9 KB and all 24 together fit
// beside the join's first arrival, so no `outer/<col>/eq/lateral` cell
// refused in 210 runs over GOMAXPROCS 1, 2, 4 and the default and both forced
// orders (the join's first reservation delayed, every aggregate release
// held); the rows are PostgreSQL 17.11's, re-measured over the 59 LATERAL
// cells (postgres:17-alpine, --locale=C).
func TestArc1681dBoolCorrelatedBodyEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the 1681d matrix")
	}
	answers := bcPGAnswers(t)
	cells := bcCells()
	if len(cells) != len(answers) {
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
	dump := os.Getenv("BC_DUMP")
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
				kv, cause, known := bcKnown(tc.name, arm.name)
				switch {
				case verdict == "right" && known:
					t.Errorf("%s\n  arm  %s\n  now answers as PostgreSQL 17.11 does (%s): delete its bcKnown rule (%s)", tc.sql, arm.name, want, cause)
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
