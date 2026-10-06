// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/planner/physical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// THE STAGE DAG'S CATEGORY FOLD STAYS CHEAP ON A DEEP OR WIDE PLAN (#381, arc
// RE round 4; COMMON §34).
//
// physical.PlanPGCategories folds every node's emitted categories by name
// for the stage DAG, and when a name is left out (a stored column created
// from `sqrt(…)` beside a double precision column of the same name)
// planPGCategoryLoss walks the plan again, asking each node's emitted
// categories and types, then planHasRoundingSite reads every expression of
// the plan. Every shape here carries that conflict so all three walks run:
// derived tables nested 16 and 64 deep over the conflicting UNION ALL, a
// GROUP BY of 64 keys, a 200-item select list and a 12-way join. Each fold
// must name the lost column and finish in milliseconds (the bounds leave
// room for -race on the pre-push hook).
func TestArcREPlanCategoryLossPlanningDepth(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var wide []string
	for i := 0; i < 64; i++ {
		wide = append(wide, fmt.Sprintf("k%d BIGINT", i))
	}
	for _, q := range []string{
		"CREATE TABLE ld_a (id BIGINT, f DOUBLE PRECISION)",
		"INSERT INTO ld_a VALUES (3, 2.5)",
		"CREATE TABLE ld_m AS SELECT id, sqrt(6.25 + id * 0) AS f FROM ld_a",
		"CREATE TABLE ld_w (id BIGINT, " + strings.Join(wide, ", ") + ")",
	} {
		if _, err := db.Query(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	nested := func(depth int) string {
		q := "SELECT f FROM ld_m UNION ALL SELECT f FROM ld_a"
		for i := 0; i < depth; i++ {
			q = fmt.Sprintf("SELECT round(f) AS f FROM (%s) d%d", q, i)
		}
		return q
	}
	var keys, items, joins []string
	for i := 0; i < 64; i++ {
		keys = append(keys, fmt.Sprintf("w.k%d", i))
	}
	for i := 0; i < 200; i++ {
		items = append(items, fmt.Sprintf("round(m.f + %d) AS r%d", i, i))
	}
	for i := 0; i < 11; i++ {
		joins = append(joins, fmt.Sprintf("JOIN ld_a a%d ON a%d.id = m.id", i, i))
	}
	for _, c := range []struct {
		name  string
		sql   string
		bound time.Duration
	}{
		{"depth16", nested(16), 500 * time.Millisecond},
		{"depth64", nested(64), 10 * time.Second},
		{"groupby64", "SELECT round(m.f), a.f, " + strings.Join(keys, ", ") + " FROM ld_w w JOIN ld_m m ON m.id = w.id JOIN ld_a a ON a.id = w.id GROUP BY m.f, a.f, " + strings.Join(keys, ", "), time.Second},
		{"select200", "SELECT a.f, " + strings.Join(items, ", ") + " FROM ld_m m JOIN ld_a a ON a.id = m.id", time.Second},
		{"join12", "SELECT round(m.f), a0.f FROM ld_m m " + strings.Join(joins, " "), time.Second},
	} {
		t.Run(c.name, func(t *testing.T) {
			parsed, err := plansql.Parse(c.sql)
			if err != nil {
				t.Fatal(err)
			}
			_, plan, _, err := db.ctasPlan(ctx, parsed)
			if err != nil {
				t.Fatal(err)
			}
			start := time.Now()
			_, _, loss := physical.PlanPGCategories(plan)
			elapsed := time.Since(start)
			t.Logf("%s: %s", c.name, elapsed)
			if loss != "f" {
				t.Fatalf("the fold names %q as lost, want f: the shape no longer runs the loss walk", loss)
			}
			if elapsed > c.bound {
				t.Errorf("PlanPGCategories took %s, want < %s", elapsed, c.bound)
			}
		})
	}
}
