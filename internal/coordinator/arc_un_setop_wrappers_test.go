// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"regexp"
	"testing"
	"time"
)

// Forwarding wrappers keep the inner operation's role. Computing a new
// value resets it to neutral. Compare text with the same leaves written flat
// on every execution arm, including the neutral constrained-column expression.
func TestArcUNSetOperationForwardingWrappers(t *testing.T) {
	if testing.Short() {
		t.Skip("eleven execution arms")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	arms := unibArms(t, ctx)
	prefix := regexp.MustCompile(`^type=\S* `)
	for _, inner := range []struct{ name, left string }{
		{"neutral", "SELECT v+0 AS v FROM rv_n"},
		{"marked", "SELECT v FROM un_x"},
		{"veto", "SELECT v FROM rv_n"},
	} {
		right := "SELECT 2.5 AS v FROM un_u"
		outer := "SELECT v FROM un_x"
		pair := inner.left + " UNION ALL " + right
		read := func(body string) string { return "SELECT CAST(v AS TEXT) AS x FROM (" + body + ") s ORDER BY 1" }
		flat := read(pair + " UNION ALL " + outer)
		forms := []struct{ name, sql string }{
			{"flat", flat},
			{"left_nested", read("(" + pair + ") UNION ALL " + outer)},
			{"right_nested", read(inner.left + " UNION ALL (" + right + " UNION ALL " + outer + ")")},
			{"derived", read("SELECT v FROM (" + pair + ") d UNION ALL " + outer)},
			{"cte", "WITH d AS (" + pair + ") " + read("SELECT v FROM d UNION ALL "+outer)},
			{"order", read("SELECT v FROM (" + pair + " ORDER BY 1) d UNION ALL " + outer)},
			{"limit", read("SELECT v FROM (" + pair + " LIMIT 1000) d UNION ALL " + outer)},
			{"order_limit", read("SELECT v FROM (" + pair + " ORDER BY 1 LIMIT 1000) d UNION ALL " + outer)},
			{"forwarded", read("SELECT v FROM (SELECT v FROM (" + pair + ") d) e UNION ALL " + outer)},
			{"rename", read("SELECT w AS v FROM (SELECT v AS w FROM (" + pair + ") d) e UNION ALL " + outer)},
		}
		var want string
		for _, arm := range arms {
			for _, form := range forms {
				t.Run(fmt.Sprintf("%s/%s/%s", inner.name, arm.name, form.name), func(t *testing.T) {
					r, err := arm.run(form.sql)
					if err != nil {
						t.Fatalf("%s: %v", form.sql, err)
					}
					got := prefix.ReplaceAllString(unRender(r), "")
					if want == "" {
						want = got
					}
					if got != want {
						t.Errorf("%s\ngot %s\nflat %s", form.sql, got, want)
					}
				})
			}
		}
	}
	// The inner result vetoes; arithmetic over it supplies a new neutral value.
	// PostgreSQL 17.11 prints the two alike (`2.5` three times, `2.50` once):
	// each value keeps its own display scale, `v + 0` included. Since arc PS
	// stage 1 (ADR-0024 §1 as amended) the flat form prints PostgreSQL's text
	// on every arm; arithmetic still answers at the carrier scale until stage
	// 2, so `v + 0` over the inner union's `2.5` prints 2.50 (numeric-decimal
	// r18's in-flight-arithmetic sub-cell). Both are asserted on every arm.
	computed := "SELECT CAST(v AS TEXT) AS x FROM (SELECT v+0 AS v FROM (SELECT v FROM rv_n UNION ALL SELECT 2.5 AS v FROM un_u) d UNION ALL SELECT v FROM un_x) s ORDER BY 1"
	flat := "SELECT CAST(v AS TEXT) AS x FROM (SELECT v+0 AS v FROM rv_n UNION ALL SELECT 2.5 AS v FROM un_u UNION ALL SELECT v FROM un_x) s ORDER BY 1"
	const pgText = "rows=15 0.00 | 0.0000000001 | 1 | 1 | 1.00 | 1.5 | 1.50 | 2.5 | 2.5 | 2.5 | 2.50 | 7 | 7.00 | NULL | NULL"
	const keptComputed = "rows=15 0.00 | 0.0000000001 | 1 | 1 | 1.00 | 1.5 | 1.50 | 2.50 | 2.50 | 2.50 | 2.50 | 7 | 7.00 | NULL | NULL"
	for _, arm := range arms {
		a, e := arm.run(computed)
		if e != nil {
			t.Errorf("%s: %v", arm.name, e)
			continue
		}
		b, e := arm.run(flat)
		if e != nil {
			t.Fatal(e)
		}
		if got := prefix.ReplaceAllString(unRender(b), ""); got != pgText {
			t.Errorf("%s flat: %s, want PostgreSQL's %s", arm.name, got, pgText)
		}
		if got := prefix.ReplaceAllString(unRender(a), ""); got != keptComputed {
			t.Errorf("%s computed wrapper: %s, want the kept %s (PostgreSQL %s; stage 2)", arm.name, got, keptComputed, pgText)
		}
	}
}
