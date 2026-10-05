// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// wdConsumerTable reads a name<TAB>sql (or name<TAB>answer) table.
func wdConsumerTable(t *testing.T, path string) [][2]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out [][2]string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, rest, ok := strings.Cut(line, "\t")
		if !ok {
			t.Fatalf("%s: malformed line %q", path, line)
		}
		out = append(out, [2]string{name, rest})
	}
	return out
}

// wdConsumerKeptArms is this engine's answer on ONE arm where it is not
// PostgreSQL's, by cell and arm. The other four arms answer PostgreSQL's rows.
//
// A CTE self-join over two stacked windows under the 512 KiB spilled arm: the
// stacked windows' spill tracking holds ~410 KB of the budget when the join
// builds its 115 KB hash table, and the build refuses loudly. The `bare`
// reading refuses at 978cd0e5 and 9420d256 too; the `issue` reading's numeric
// result (2.5 widens the window column) is the 240 bytes that cross the
// budget there. Filing candidate, not this seam: the value is never wrong.
var wdConsumerKeptArms = map[string]map[string]string{
	"cons/p2/bare/stack/cte_self":  {"spilled512k": "ERR - hash join build: query: memory budget exceeded"},
	"cons/p2/issue/stack/cte_self": {"spilled512k": "ERR - hash join build: query: memory budget exceeded"},
}

// EVERY CONSUMER ABOVE A WINDOW OVER A DERIVED TABLE THAT SHADOWS A COLUMN OF
// ITS OWN INPUT, ON EVERY ARM (#1435). Above `(SELECT id, g, b * 2 AS
// b, b AS ob FROM wd_t) s` the name `b` is the derived table's computed column
// and `ob` its rename of the source; the shadowed source column itself does
// not exist. The stage DAG's producer emits the table's declared columns under
// their own names and every name walk stops at the window
// (logical.WindowShadowedInput), so no consumer maps one name to another.
//
// cons/<producer>/<reading>/<shape>/<consumer>: producer p1 (the alias alone)
// or p2 (the alias beside a rename of its source); the window reads the alias
// bare (`SUM(b)`), as the issue's LAG with a widening default (`LAG(b, 1,
// 2.5)`), as a default (`LAG(id, 1, b)`), as a PARTITION BY key, or not at all
// (`ROW_NUMBER()`); one window, two side by side, or a second window stacked
// over it (`LAG(t.w3, 1, 7)`); and the consumer above it — the SELECT list, an
// expression, WHERE, an inner join either side or keyed on the alias (an
// expression key and a column key) or on the rename, LEFT, CROSS, IN and NOT
// EXISTS, a scalar aggregate, GROUP BY on another column / the alias / the
// rename, HAVING, DISTINCT, ORDER BY + LIMIT on the alias / the rename, UNION
// ALL, a window over the window ordered or partitioned by the alias / the
// rename, a CTE self-join, a nested derived table, a scalar subquery. j/ (seven
// join shapes × six windows × a shadowing or non-shadowing alias), h2/ and
// r4h/ (stacked windows, whose window-key names collided on the DAG), r4v/
// (eight window readings × 33 consumers), ag/scalar_agg/ and c/sh/agg/ (a
// window key over an alias computed over an aggregate) are the measured
// cells.
//
// At bba3c944 the DAG arms answered a JOIN above the window from the shadowed
// SOURCE (`b AS w3` 10, 20, … where PostgreSQL answers 20, 40, …) and an outer
// LAG / LEAD default over an inner window from the inner window's default
// column; 978cd0e5 answered a consumer above the window from the alias where
// it named the rename (`ob`), and the alias where the window did not read it,
// from the source.
func TestArcWDWindowConsumersEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the consumers above a window")
	}
	cells := wdConsumerTable(t, "testdata/arc_wd_window_consumers_cells.tsv")
	answers := map[string]string{}
	for _, a := range wdConsumerTable(t, "testdata/arc_wd_window_consumers_pg17.tsv") {
		answers[a[0]] = a[1]
	}
	for _, c := range cells {
		if _, ok := answers[c[0]]; !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", c[0])
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	t.Cleanup(cancel)
	arms := wdArms(t, ctx)
	keptSeen := 0
	for _, c := range cells {
		name, sql := c[0], c[1]
		if _, ok := wdConsumerKeptArms[name]; ok {
			keptSeen++
		}
		t.Run(name, func(t *testing.T) {
			type result struct {
				res wdResult
				err error
			}
			results := make([]result, len(arms))
			var wg sync.WaitGroup
			for i, arm := range arms {
				wg.Add(1)
				go func() {
					defer wg.Done()
					results[i].res, results[i].err = arm.run(sql)
				}()
			}
			wg.Wait()
			for i, arm := range arms {
				want, why := answers[name], "PostgreSQL 17.11"
				if k, ok := wdConsumerKeptArms[name][arm.name]; ok {
					want, why = k, "kept on this arm (wdConsumerKeptArms)"
				}
				res, err := results[i].res, results[i].err
				if rest, ok := strings.CutPrefix(want, "ERR "); ok {
					state, msg, _ := strings.Cut(rest, " ")
					if state == "-" {
						state = ""
					}
					if err == nil {
						t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s %q (%s)", sql, arm.name, wdRender(res), state, msg, why)
					} else if st := sqlerr.StateOf(err); st != state || !waErrorAgrees(err.Error(), msg) {
						t.Errorf("%s\n  arm  %s\n  got  %s %v\n  want %s %q (%s)", sql, arm.name, st, err, state, msg, why)
					}
					continue
				}
				if err != nil {
					t.Errorf("%s\n  arm  %s\n  refused: %v\n  want %s (%s)", sql, arm.name, err, want, why)
					continue
				}
				if got := wdRender(res); wdNormalize(got) != wdNormalize(want) {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (%s)", sql, arm.name, got, want, why)
				}
			}
		})
	}
	if len(cells) < 1100 || keptSeen != len(wdConsumerKeptArms) {
		t.Fatalf("%d cells, %d of %d kept cells present: the table must discriminate",
			len(cells), keptSeen, len(wdConsumerKeptArms))
	}
}
