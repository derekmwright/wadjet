// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// Arc CI2 (ADR-0047 stage 2, #1393): a node's declared output is read by the
// column a reference is BOUND to, never by cutting an expression's text at
// its last dot. Every cell joins ss_t to a relation u that publishes the
// names ss_t has with OTHER types (u.i is double precision where t.i is
// integer, u.n bigint where t.n is numeric(10,2), …), from eight origins
// (derived table, CTE, grouped, DISTINCT, UNION ALL, window, LIMIT, nested
// star), under nine expressions whose text ends in a qualified name of the
// other type, read by thirteen consumer sites (the select list, a zero-row
// result, a derived table and SUM over it, a GROUP BY key, a zero-row key, SUM
// and arithmetic over a grouped or DISTINCT derived table, an aggregate and a
// window argument, LAG with the expression as its default, CASE); and the
// same sites over ONE relation t from eight origins, whose expression's text
// ends in a qualified name of another type (`t.f + t.i`, `t.n * t.n`). Expectations are PostgreSQL 17.11's
// (testdata/arc_ci2_declared_output_pg17.tsv) except the kept lines
// (testdata/arc_ci2_declared_output_kept.tsv): the stage-DAG arms' base
// answers (ADR-0047 stage 5/7) and the single-process limits each line names.
// A kept line that answers PostgreSQL's value fails: deleting it is the proof.
func TestArcCI2DeclaredOutputByIdentityEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the declared-output identity table")
	}
	cells := ci2ReadCells(t, "testdata/arc_ci2_declared_output_cells.tsv")
	answers := gkReadTSV(t, "testdata/arc_ci2_declared_output_pg17.tsv", 2)
	kept := ci2KeptLines(t, "testdata/arc_ci2_declared_output_kept.tsv")
	seen := map[string]bool{}
	for _, c := range cells {
		if seen[c.name] {
			t.Fatalf("duplicate cell %s", c.name)
		}
		seen[c.name] = true
		if _, ok := answers[c.name]; !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", c.name)
		}
	}
	orderedOf := map[string]bool{}
	for _, c := range cells {
		orderedOf[c.name] = c.ordered
	}
	for name, ks := range kept {
		if !seen[name] {
			t.Fatalf("kept cell %s is not in the table", name)
		}
		for _, k := range ks {
			if ci2Matches(k.want, answers[name], orderedOf[name]) {
				t.Fatalf("kept line for %s (%s) answers PostgreSQL 17.11: delete it", name, k.arms)
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	arms := ssArms(t, ctx)
	var dump *os.File
	if p := os.Getenv("CI2_DUMP"); p != "" {
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
				want, why, pinned := pgWant, "PostgreSQL 17.11", false
				for _, k := range ks {
					if k.holdsOn(arm.name) {
						want, why, pinned = k.want, "kept: "+k.why, true
					}
				}
				switch {
				case pinned && ci2Matches(got[i], pgWant, tc.ordered):
					t.Errorf("%s: %s\n  now answers PostgreSQL's %s: delete its kept line (%s)", arm.name, tc.sql, pgWant, why)
				case !ci2Matches(got[i], want, tc.ordered):
					t.Errorf("%s: %s\n  got  %s\n  want %s (%s)", arm.name, tc.sql, got[i], want, why)
				}
			}
		})
	}
	if len(cells) < 1650 {
		t.Fatalf("the table shrank: %d cells", len(cells))
	}
}

// ci2Matches compares an arm's answer with the wanted one up to two things
// that are not this seam's: the sign of a zero (#1489), re-sorting an
// unordered answer's rows after it is normalized, and the last bits of a
// double SUM, which depend on the order partial states meet (ADR-0013;
// nxFloatSumClose).
func ci2Matches(got, want string, ordered bool) bool {
	norm := func(s string) string {
		s = strings.TrimSpace(s)
		if strings.HasPrefix(s, "ERR") {
			return s
		}
		for i := 0; i < 2; i++ {
			s = re2NegZero.ReplaceAllString(s, "${1}0${2}")
		}
		if ordered {
			return s
		}
		head, rest, ok := strings.Cut(s, " rows=")
		if !ok {
			return s
		}
		n, rows, _ := strings.Cut(rest, " ")
		if rows == "" {
			return s
		}
		parts := strings.Split(rows, " | ")
		sort.Strings(parts)
		return head + " rows=" + n + " " + strings.Join(parts, " | ")
	}
	g, w := norm(got), norm(want)
	return ssMatches(g, w) || nxFloatSumClose(g, w)
}

type ci2Cell struct {
	name, sql string
	ordered   bool
}

func ci2ReadCells(t *testing.T, path string) []ci2Cell {
	t.Helper()
	var out []ci2Cell
	for name, rest := range ci2Lines(t, path) {
		ordered, sql, ok := strings.Cut(rest, "\t")
		if !ok {
			t.Fatalf("malformed cell %s in %s", name, path)
		}
		out = append(out, ci2Cell{name: name, sql: sql, ordered: ordered == "true"})
	}
	return out
}

// ci2Lines yields name<TAB>rest lines in file order, skipping comments.
func ci2Lines(t *testing.T, path string) func(func(string, string) bool) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	var lines [][2]string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, rest, ok := strings.Cut(line, "\t")
		if !ok {
			t.Fatalf("malformed line %q in %s", line, path)
		}
		lines = append(lines, [2]string{name, rest})
	}
	f.Close()
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return func(yield func(string, string) bool) {
		for _, l := range lines {
			if !yield(l[0], l[1]) {
				return
			}
		}
	}
}

// ci2Kept is one kept line: name<TAB>arms<TAB>answer<TAB>why, where arms is
// "dagAll" (the three stage-DAG arms), "singleAll" (single and spilled512k)
// or one arm's name.
type ci2Kept struct{ arms, want, why string }

func (k ci2Kept) holdsOn(arm string) bool {
	switch k.arms {
	case "dagAll":
		return strings.HasPrefix(arm, "dag")
	case "singleAll":
		return arm == "single" || arm == "spilled512k"
	}
	return k.arms == arm
}

func ci2KeptLines(t *testing.T, path string) map[string][]ci2Kept {
	t.Helper()
	out := map[string][]ci2Kept{}
	for name, rest := range ci2Lines(t, path) {
		parts := strings.SplitN(rest, "\t", 3)
		if len(parts) != 3 || parts[2] == "" {
			t.Fatalf("kept line %s needs arms, answer and why", name)
		}
		out[name] = append(out[name], ci2Kept{arms: parts[0], want: parts[1], why: parts[2]})
	}
	return out
}
