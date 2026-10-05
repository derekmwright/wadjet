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
	computed := "SELECT CAST(v AS TEXT) AS x FROM (SELECT v+0 AS v FROM (SELECT v FROM rv_n UNION ALL SELECT 2.5 AS v FROM un_u) d UNION ALL SELECT v FROM un_x) s ORDER BY 1"
	flat := "SELECT CAST(v AS TEXT) AS x FROM (SELECT v+0 AS v FROM rv_n UNION ALL SELECT 2.5 AS v FROM un_u UNION ALL SELECT v FROM un_x) s ORDER BY 1"
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
		if prefix.ReplaceAllString(unRender(a), "") != prefix.ReplaceAllString(unRender(b), "") {
			t.Errorf("%s computed wrapper: %s; flat: %s", arm.name, unRender(a), unRender(b))
		}
	}
}
