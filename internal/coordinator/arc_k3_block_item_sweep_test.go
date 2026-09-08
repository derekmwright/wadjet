package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
)

// NO EXPRESSION KIND MAY ROUTE SILENTLY — the sweep behind arc K3's boundary
// rule (#984, round 3).
//
// The rule the arc adopts is that the coordinator-local route may carry only
// what was WRONG or LOUD without it, because the route is not
// answer-preserving: its ORDER BY is wrong for shapes the DAG gets right. The
// rule was broken twice by the same mechanism and in the same way — the pass
// marked a block, could not TYPE one of its items, and the refusal handed a
// correct distributed query to the local pipeline. `NULL AS c` in round 1;
// a scalar-subquery item, an all-NULL `CASE` and `ARRAY[amount]` in round 2.
//
// Three special cases were three too many. A computed item over a producer
// that MATERIALIZES it is already on the stream, so it is not a divergence at
// all whatever its type — and this sweep is what keeps that true for a kind
// nobody has written yet. It walks EVERY column of the type-matrix corpus as a
// derived-block SELECT item, in the four expression shapes any type admits,
// and asserts of each that the DAG arms EXECUTED it: counter unchanged, and
// the same rendering the single-process arm produced.
//
// It deliberately does NOT transcribe PostgreSQL's rows. The claim here is a
// DISPOSITION and an agreement between paths; the values for these types are
// the type matrix's own business and are gated there.
func TestArcK3NoBlockItemKindRoutesSilently(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	t.Cleanup(cancel)
	arms := e3Arms(t, ctx)

	// The four shapes every type admits. A rename is the control — it
	// INTRODUCES a name and is published, so it must execute too; the other
	// three are computed and must never be read as introduced.
	shapes := []struct{ name, tmpl string }{
		{"rename", "%s AS v"},
		{"case-both-arms", "CASE WHEN g = 1 THEN %s ELSE %s END AS v"},
		{"case-null-arm", "CASE WHEN g = 1 THEN %s ELSE NULL END AS v"},
		{"coalesce", "COALESCE(%s, %s) AS v"},
	}

	for _, col := range typematrix.Columns() {
		if !col.Flat {
			continue // the nested table has its own fixture and its own gate
		}
		for _, sh := range shapes {
			item := strings.ReplaceAll(sh.tmpl, "%s", col.Name)
			name := col.Name + "/" + sh.name
			t.Run(name, func(t *testing.T) {
				sql := fmt.Sprintf(
					`SELECT * FROM typemx_dim d JOIN (SELECT g, %s FROM typemx) s `+
						`ON s.g = d.k ORDER BY d.k, v`, item)
				var reference string
				for _, arm := range arms {
					var routesBefore int64
					if arm.coord != nil {
						routesBefore = arm.coord.LateralProjectionLocalRoutes()
					}
					cols, rows, err := arm.run(sql)
					if arm.coord != nil &&
						arm.coord.LateralProjectionLocalRoutes() > routesBefore {
						t.Fatalf("%s arm ROUTED this off the DAG. A block item's KIND may "+
							"not decide the disposition: a computed item over a producer "+
							"that materializes it is already on the stream, and the route "+
							"may carry only what was wrong or loud without it\n  SQL: %s",
							arm.name, sql)
					}
					if err != nil {
						// A refusal is a disposition too, and it must be the
						// SAME one on every arm — a shape the single-process
						// path cannot answer is not this gate's business, but
						// one only the DAG refuses is.
						if arm.coord != nil && reference != "ERR" {
							t.Fatalf("%s arm failed where the single-process path answered "+
								"%s\n  %v\n  SQL: %s", arm.name, reference, err, sql)
						}
						if arm.coord == nil {
							reference = "ERR"
						}
						continue
					}
					got := e3Render(cols, rows)
					if arm.coord == nil {
						if reference == "" {
							reference = got
						}
						continue
					}
					if reference == "ERR" {
						t.Fatalf("%s arm answered %s where the single-process path "+
							"failed\n  SQL: %s", arm.name, got, sql)
					}
					if got != reference {
						t.Fatalf("%s arm: %s\n  single-process arm: %s\n  SQL: %s",
							arm.name, got, reference, sql)
					}
				}
			})
		}
	}
}
