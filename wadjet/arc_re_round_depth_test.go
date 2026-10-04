// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// ROUND over a column a derived table materialized reads the PLAN's category
// of that column (#381), which each projection asks of its child; nested 16
// deep, every level a round() over the one below and a float-carried numeric
// at the bottom, planning and running stays linear in the depth and every
// level keeps the numeric rule (2.5 rounds to 3, then stays 3).
func TestArcRERoundOverNestedDerivedTablesDepth(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Query(ctx, "CREATE TABLE e_d (id BIGINT, f DOUBLE PRECISION)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query(ctx, "INSERT INTO e_d VALUES (1, 2.5)"); err != nil {
		t.Fatal(err)
	}
	for _, base := range []struct{ name, q, want string }{
		{"numeric", "SELECT DISTINCT 5 / 2.0 + id * 0 AS v FROM e_d", "3"},
		{"float8", "SELECT DISTINCT f AS v FROM e_d", "2"},
	} {
		for _, depth := range []int{4, 8, 16} {
			t.Run(fmt.Sprintf("%s/depth%d", base.name, depth), func(t *testing.T) {
				q := base.q
				for i := 0; i < depth; i++ {
					q = "SELECT round(v) AS v FROM (" + q + ") d"
				}
				start := time.Now()
				res, err := db.Query(ctx, q)
				elapsed := time.Since(start)
				if err != nil {
					t.Fatal(err)
				}
				if len(res.Rows) != 1 || fmt.Sprint(res.Cells(0)[0]) != base.want {
					t.Fatalf("depth %d: got %v, want %s (PostgreSQL 17.11)", depth, res.Rows, base.want)
				}
				t.Logf("depth=%d elapsed=%s", depth, elapsed)
				if depth == 16 && elapsed > 2*time.Second {
					t.Errorf("depth 16 took %s, want < 2s: the category walk is not linear in depth", elapsed)
				}
			})
		}
	}
}
