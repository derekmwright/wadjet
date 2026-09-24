// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// ARC JP on the EMBEDDED arm — the two issues' own statements over their own
// fixture (the lat_ord / lat_item pair), with PostgreSQL 17.11's rows. The
// five-arm seam tables are coordinator.TestArcJPA*; this is the embedded
// user's view of the same two defects, runnable without the MIT/AGPL line.
func TestArcJPAJoinArmKeyIsTheColumnTheQueryWrote(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, q := range []string{
		"CREATE TABLE lat_ord (id INT64, customer STRING, total FLOAT64)",
		"CREATE TABLE lat_item (id INT64, order_id INT64, product STRING, amount FLOAT64)",
	} {
		if _, err := db.Query(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	for _, q := range []string{
		"INSERT INTO lat_ord VALUES (1,'Alice',150),(2,'Bob',200),(3,'Carol',0)",
		"INSERT INTO lat_item VALUES (1,1,'Widget',50),(2,1,'Gadget',100),(3,2,'Widget',75),(4,2,'Doohickey',125)",
	} {
		if _, err := db.Execute(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	cases := []struct{ name, sql, want string }{
		// #1299: at 83cd4a93 `1,2,3,3` — o=1's s paired with o=2's t and u.
		{"selfJoinThreeArms",
			"SELECT o.id AS a, s.id AS b, t.id AS c, u.id AS d FROM lat_ord o " +
				"JOIN lat_item s ON s.order_id = o.id AND s.id IN (2,4) " +
				"JOIN lat_item t ON t.order_id = o.id AND t.id IN (1,3) " +
				"JOIN lat_item u ON u.order_id = o.id AND u.id IN (1,3)",
			"1,2,1,1 | 2,4,3,3"},
		// #1302: at 83cd4a93 zero rows, with and without the bound.
		{"lateralOuterExpressionKey",
			"SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (" +
				"SELECT i.id AS m FROM lat_item i WHERE i.order_id = o.id - 0) s ON true",
			"1,1 | 1,2 | 2,3 | 2,4"},
		{"lateralOuterExpressionKeyBounded",
			"SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (" +
				"SELECT i.id AS m FROM lat_item i WHERE i.order_id = o.id - 0 ORDER BY i.id LIMIT 1) s ON true",
			"1,1 | 2,3"},
		{"lateralConstantValuedOuterExpression",
			"SELECT o.id AS a, s.m AS m FROM lat_ord o JOIN LATERAL (" +
				"SELECT i.id AS m FROM lat_item i WHERE i.order_id = o.id - o.id + 1) s ON true",
			"1,1 | 1,2 | 2,1 | 2,2 | 3,1 | 3,2"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res, err := db.Query(ctx, c.sql)
			if err != nil {
				t.Fatalf("%s: %v", c.sql, err)
			}
			rows := make([]string, 0, len(res.Rows))
			for i := range res.Rows {
				cells := res.Cells(i)
				parts := make([]string, len(cells))
				for j, v := range cells {
					parts[j] = fmt.Sprint(v)
				}
				rows = append(rows, strings.Join(parts, ","))
			}
			sort.Strings(rows)
			if got := strings.Join(rows, " | "); got != c.want {
				t.Errorf("%s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.sql, got, c.want)
			}
		})
	}
	// The bare star over the expression-keyed lateral is refused, 0A000.
	_, err = db.Query(ctx, "SELECT * FROM lat_ord o JOIN LATERAL (SELECT i.id AS m FROM lat_item i "+
		"WHERE i.order_id = o.id - 0) s ON true")
	if err == nil || !strings.Contains(err.Error(), "has an EXPRESSION on its outer side") {
		t.Errorf("a bare star over an expression-keyed LATERAL: got %v, want the 0A000 refusal", err)
	}
}
