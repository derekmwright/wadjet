// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// THE EMBEDDED ARM OF ARC WA (#1394 #1399): the two issues' own statements,
// and what an INSERT … SELECT of one stores, on the engine an embedded user
// opens. Every `want` is live PostgreSQL 17.11's answer over the same rows.
//
// At v0.25.2 a window function over a bare LITERAL argument answered NULL —
// the argument was never materialized as an input column, so the operator
// read a column named `5` or `2` that does not exist — and an INSERT of it
// stored NULL into every target type; `LAG(x, 0)` / `LEAD(x, 0)` answered the
// neighbour row, because an explicit offset of 0 was read as the default 1.
// The five-arm table is coordinator.TestArcWAWindowArgumentsEveryArm.
func TestArcWAWindowArgumentsOnTheEmbeddedEngine(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	for _, q := range []string{
		"CREATE TABLE wak (id INTEGER, five INTEGER)",
		"INSERT INTO wak VALUES (1, 5), (2, 5)",
		"CREATE TABLE wak1 (id INTEGER, five INTEGER)",
		"INSERT INTO wak1 VALUES (1, 5)",
		"CREATE TABLE was (id INTEGER, x INTEGER)",
		"INSERT INTO was VALUES (1, 10), (2, 20), (3, 30)",
		"CREATE TABLE wa_int (id INTEGER, v INTEGER)",
		"CREATE TABLE wa_num (id INTEGER, v NUMERIC(10,2))",
		"CREATE TABLE wa_dbl (id INTEGER, v DOUBLE)",
		"CREATE TABLE wa_lag (id INTEGER, v INTEGER)",
		// An INSERT … SELECT of one: SUM(2.5) over a single row is 2.5.
		"INSERT INTO wa_int SELECT id, SUM(2.5) OVER () FROM wak1",
		"INSERT INTO wa_num SELECT id, SUM(2.5) OVER () FROM wak1",
		"INSERT INTO wa_dbl SELECT id, SUM(2.5) OVER () FROM wak1",
		"INSERT INTO wa_lag SELECT id, LAG(x, 0) OVER (ORDER BY id) FROM was",
	} {
		if _, err := db.Query(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	for _, c := range []struct{ issue, sql, want string }{
		{"#1394", "SELECT id, SUM(2.5) OVER (), FIRST_VALUE(2.5) OVER (ORDER BY id), MAX(2.5) OVER (), " +
			"SUM(2) OVER (), SUM(2.5 * 1) OVER () FROM wak ORDER BY id",
			"1|5.0|2.5|2.5|4|5.0 2|5.0|2.5|2.5|4|5.0"},
		{"#1394", "SELECT id, LAG(5) OVER (ORDER BY id), COUNT(2) OVER (), MIN('b') OVER () FROM wak ORDER BY id",
			"1|NULL|2|b 2|5|2|b"},
		{"#1394", "SELECT * FROM wa_int", "1|3"},
		{"#1394", "SELECT * FROM wa_num", "1|2.50"},
		{"#1394", "SELECT * FROM wa_dbl", "1|2.5"},
		{"#1399", "SELECT id, LAG(x, 0) OVER (ORDER BY id) FROM was ORDER BY id", "1|10 2|20 3|30"},
		{"#1399", "SELECT id, LEAD(x, 0) OVER (ORDER BY id) FROM was ORDER BY id", "1|10 2|20 3|30"},
		{"#1399", "SELECT id, LAG(x, 0) OVER (PARTITION BY id ORDER BY id) FROM was ORDER BY id", "1|10 2|20 3|30"},
		{"#1399", "SELECT * FROM wa_lag ORDER BY id", "1|10 2|20 3|30"},
		// The rest of the offset's own grammar, measured on 17.11: a
		// negative offset reads the other way and NULL answers NULL.
		{"#1399", "SELECT id, LAG(x, -1) OVER (ORDER BY id) FROM was ORDER BY id", "1|20 2|30 3|NULL"},
		{"#1399", "SELECT id, LEAD(x, NULL) OVER (ORDER BY id) FROM was ORDER BY id", "1|NULL 2|NULL 3|NULL"},
	} {
		res, err := db.Query(ctx, c.sql)
		if err != nil {
			t.Errorf("%s: %s refused: %v", c.issue, c.sql, err)
			continue
		}
		var rows []string
		for i := range res.Rows {
			var cells []string
			for _, v := range res.Cells(i) {
				if v == nil {
					cells = append(cells, "NULL")
				} else {
					cells = append(cells, fmt.Sprint(v))
				}
			}
			rows = append(rows, strings.Join(cells, "|"))
		}
		if got := strings.Join(rows, " "); got != c.want {
			t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.issue, c.sql, got, c.want)
		}
	}
}
