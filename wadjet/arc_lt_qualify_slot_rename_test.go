// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// TWO BLOCKS EACH CARRYING A QUALIFY UNDER ONE JOIN READ THEIR OWN WINDOW.
//
// A QUALIFY is lowered to a Filter over the Window operator's output whose
// predicate names the window's slot, and slots are minted PER BLOCK, so two
// blocks under one join both mint `__win_0`. The physical planner's
// slot-collision pass steps the second window to `__win_1` — and used to leave
// the second block's FILTER reading `__win_0`, so the single-process path
// failed with `filter column "__win_0" does not exist in the input schema`
// (arc LT; the planner-minted per-outer-row bound of #1019 reaches the same
// seam as a second spelling, and this is the user-written first one). Fails
// at 51addfb6 on the two-block statement; the one-block control answered
// before and after.
func TestArcLTTwoQualifyBlocksUnderOneJoinReadTheirOwnWindows(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, ddl := range []string{
		`CREATE TABLE ltq_ord (id BIGINT)`,
		`INSERT INTO ltq_ord VALUES (1),(2),(3)`,
		`CREATE TABLE ltq_item (id BIGINT, order_id BIGINT, product VARCHAR, amount DOUBLE)`,
		`INSERT INTO ltq_item VALUES (1,1,'Widget',50),(2,1,'Gadget',100),(3,2,'Widget',75),(4,2,'Doohickey',125)`,
	} {
		if _, err := db.Query(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	cases := []struct{ name, sql, want string }{
		{"two blocks, each with its own QUALIFY",
			`SELECT o.id AS a, s.m AS m, t.p AS p FROM ltq_ord o ` +
				`JOIN (SELECT i.order_id AS k, i.amount AS m FROM ltq_item i QUALIFY ROW_NUMBER() OVER (PARTITION BY i.order_id ORDER BY i.amount DESC) <= 1) s ON s.k = o.id ` +
				`JOIN (SELECT j.order_id AS k2, j.product AS p FROM ltq_item j QUALIFY ROW_NUMBER() OVER (PARTITION BY j.order_id ORDER BY j.amount) <= 1) t ON t.k2 = o.id ` +
				`ORDER BY a, m, p`,
			"1,100,Widget;2,125,Widget"},
		{"control: one block",
			`SELECT o.id AS a, s.m AS m FROM ltq_ord o ` +
				`JOIN (SELECT i.order_id AS k, i.amount AS m FROM ltq_item i QUALIFY ROW_NUMBER() OVER (PARTITION BY i.order_id ORDER BY i.amount DESC) <= 1) s ON s.k = o.id ` +
				`ORDER BY a, m`,
			"1,100;2,125"},
		{"two blocks, the block with the DESC window written second",
			`SELECT o.id AS a, t.p AS p, s.m AS m FROM ltq_ord o ` +
				`JOIN (SELECT j.order_id AS k2, j.product AS p FROM ltq_item j QUALIFY ROW_NUMBER() OVER (PARTITION BY j.order_id ORDER BY j.amount) <= 1) t ON t.k2 = o.id ` +
				`JOIN (SELECT i.order_id AS k, i.amount AS m FROM ltq_item i QUALIFY ROW_NUMBER() OVER (PARTITION BY i.order_id ORDER BY i.amount DESC) <= 1) s ON s.k = o.id ` +
				`ORDER BY a, p, m`,
			"1,Widget,100;2,Widget,125"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res, err := db.Query(ctx, c.sql)
			if err != nil {
				t.Fatalf("%s\n  %v", c.sql, err)
			}
			rows := make([]string, 0, len(res.Rows))
			for i := range res.Rows {
				cells := res.Cells(i)
				s := make([]string, len(cells))
				for j, v := range cells {
					s[j] = ltRenderCell(v)
				}
				rows = append(rows, strings.Join(s, ","))
			}
			if got := strings.Join(rows, ";"); got != c.want {
				t.Fatalf("%s\n  got  %s\n  want %s (PostgreSQL 17.11's rows for the same top-1-per-order)", c.sql, got, c.want)
			}
		})
	}
}

func ltRenderCell(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	default:
		return fmt.Sprint(x)
	}
}
