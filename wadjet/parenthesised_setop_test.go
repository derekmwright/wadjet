package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// A set operation's arms may be PARENTHESISED, and the parentheses scope the
// arm's own ORDER BY and LIMIT (#693).
//
// PostgreSQL accepts all of it. Wadjet's set-operation parser called
// parseSingleSelect for each arm, which expects the token SELECT, so every
// spelling below was 42601 `expected SELECT` — including inside a derived
// table, which is where #656's review found it.
//
// Every Want measured live on postgres:17-alpine over `su(id int)` holding
// 1, 2, 3.
func TestParenthesisedSetOperationArms(t *testing.T) {
	ctx := context.Background()
	db := setOpDB(t, ctx)
	for _, tc := range []struct {
		name string
		sql  string
		rows []string
	}{
		{name: "in a derived table",
			sql:  "SELECT COUNT(*) AS c FROM ((SELECT id FROM su) UNION ALL (SELECT id FROM su)) u",
			rows: []string{"[6]"}},
		{name: "both arms at the top level",
			sql:  "(SELECT id FROM su) UNION ALL (SELECT id FROM su) ORDER BY 1",
			rows: []string{"[1]", "[1]", "[2]", "[2]", "[3]", "[3]"}},
		{name: "an arm-scoped ORDER BY and LIMIT",
			sql: "(SELECT id FROM su ORDER BY id LIMIT 1) UNION ALL " +
				"(SELECT id FROM su ORDER BY id DESC LIMIT 1)",
			rows: []string{"[1]", "[3]"}},
		{name: "the whole operation parenthesised",
			sql:  "((SELECT id FROM su) UNION ALL (SELECT id FROM su)) ORDER BY 1",
			rows: []string{"[1]", "[1]", "[2]", "[2]", "[3]", "[3]"}},
		{name: "only the left arm",
			sql:  "(SELECT id FROM su) UNION ALL SELECT id FROM su ORDER BY 1",
			rows: []string{"[1]", "[1]", "[2]", "[2]", "[3]", "[3]"}},
		{name: "only the right arm",
			sql:  "SELECT id FROM su UNION ALL (SELECT id FROM su) ORDER BY 1",
			rows: []string{"[1]", "[1]", "[2]", "[2]", "[3]", "[3]"}},
		{name: "three arms",
			sql: "(SELECT id FROM su) UNION ALL (SELECT id FROM su) UNION ALL " +
				"(SELECT id FROM su) ORDER BY 1",
			rows: []string{"[1]", "[1]", "[1]", "[2]", "[2]", "[2]", "[3]", "[3]", "[3]"}},
		{name: "a nested set operation as an arm",
			sql: "((SELECT id FROM su) UNION ALL (SELECT id FROM su)) UNION ALL " +
				"(SELECT id FROM su) ORDER BY 1",
			rows: []string{"[1]", "[1]", "[1]", "[2]", "[2]", "[2]", "[3]", "[3]", "[3]"}},
		{name: "INTERSECT",
			sql:  "(SELECT id FROM su WHERE id < 3) INTERSECT (SELECT id FROM su WHERE id > 1) ORDER BY 1",
			rows: []string{"[2]"}},
		{name: "EXCEPT",
			sql:  "(SELECT id FROM su) EXCEPT (SELECT id FROM su WHERE id = 1) ORDER BY 1",
			rows: []string{"[2]", "[3]"}},
		{name: "an arm-scoped LIMIT with no ORDER BY",
			sql:  "(SELECT id FROM su LIMIT 2) UNION ALL (SELECT id FROM su LIMIT 1) ORDER BY 1",
			rows: []string{"[1]", "[1]", "[2]"}},
		// The control: the unparenthesised spelling, which always worked and
		// must keep working.
		{name: "ctl: no parentheses at all",
			sql:  "SELECT id FROM su UNION ALL SELECT id FROM su ORDER BY 1",
			rows: []string{"[1]", "[1]", "[2]", "[2]", "[3]", "[3]"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := db.Query(ctx, tc.sql)
			if err != nil {
				t.Fatalf("%s: %v", tc.sql, err)
			}
			if len(res.Rows) != len(tc.rows) {
				t.Fatalf("%d rows, want %d\n  SQL: %s", len(res.Rows), len(tc.rows), tc.sql)
			}
			for i, w := range tc.rows {
				if got := fmt.Sprint(res.Cells(i)); got != w {
					t.Errorf("row %d = %s, want %s\n  SQL: %s", i, got, w, tc.sql)
				}
			}
		})
	}
}

func setOpDB(t *testing.T, ctx context.Context) *DB {
	t.Helper()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Query(ctx, "CREATE TABLE su (id INT32)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, "INSERT INTO su VALUES (1),(2),(3)"); err != nil {
		t.Fatal(err)
	}
	return db
}
