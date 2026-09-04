package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// An unquoted identifier folds; a folded reference still finds a CamelCase
// column; a delimited one is byte-exact (#731).
//
// The fixture is ClickBench's shape and the reason the issue's two halves
// could not land separately: `hits` is registered from parquet with
// `WatchID`, `UserID`, `EventDate`, `SearchPhrase`, and every one of the 43
// queries spells them CamelCase. Folding the reference without resolving it
// against a CamelCase schema turns 43 working queries into 43 columns of
// NULL — right-to-wrong, which is a blocker, not a trade.
//
// PostgreSQL 17, measured live: over a table declared `("WatchID" bigint)`,
// `SELECT WatchID` is 42703 `column "watchid" does not exist` and
// `SELECT "WatchID"` answers. Wadjet answers BOTH — the recorded divergence
// in ADR-0012: a folded reference resolves case-insensitively when exactly
// one column matches. Every value it produces is PostgreSQL's answer to the
// quoted spelling of the same query; the only thing it does that PostgreSQL
// does not is resolve the unquoted spelling at all.
func TestIdentifierCaseOverACamelCaseSchema(t *testing.T) {
	ctx := context.Background()
	db := identCaseDB(t, ctx)
	for _, tc := range []struct {
		name string
		sql  string
		want []string // one string per row, rendered positionally
		cols []string // the RowDescription names, PostgreSQL's own
	}{
		{"unquoted reference folds and still resolves",
			"SELECT WatchID FROM hitscase ORDER BY WatchID",
			[]string{"[1]", "[2]", "[3]"}, []string{"watchid"}},
		{"the folded spelling is the same reference",
			"SELECT watchid FROM hitscase ORDER BY watchid",
			[]string{"[1]", "[2]", "[3]"}, []string{"watchid"}},
		{"a delimited reference matching the schema is byte-exact",
			`SELECT "WatchID" FROM hitscase ORDER BY "WatchID"`,
			[]string{"[1]", "[2]", "[3]"}, []string{"WatchID"}},
		{"a star publishes the schema's own spellings",
			"SELECT * FROM hitscase ORDER BY 1",
			[]string{"[1 10 20 abc]", "[2 10 21 abd]", "[3 11 20 zzz]"},
			[]string{"WatchID", "UserID", "EventDate", "SearchPhrase"}},
		{"a filter", "SELECT UserID FROM hitscase WHERE WatchID > 1 ORDER BY 1",
			[]string{"[10]", "[11]"}, []string{"userid"}},
		{"a qualified reference", "SELECT h.WatchID FROM hitscase h WHERE h.UserID > 0 ORDER BY 1",
			[]string{"[1]", "[2]", "[3]"}, []string{"watchid"}},
		{"a GROUP BY key", "SELECT EventDate, COUNT(*) AS n FROM hitscase GROUP BY EventDate ORDER BY EventDate",
			[]string{"[20 2]", "[21 1]"}, []string{"eventdate", "n"}},
		{"an aggregate input", "SELECT MAX(WatchID) AS mx, SUM(UserID) AS s FROM hitscase",
			[]string{"[3 31]"}, []string{"mx", "s"}},
		{"a sort key that is not projected", "SELECT UserID FROM hitscase ORDER BY WatchID DESC",
			[]string{"[11]", "[10]", "[10]"}, []string{"userid"}},
		{"a join key", "SELECT a.WatchID FROM hitscase a JOIN hitscase b ON a.WatchID = b.WatchID ORDER BY 1",
			[]string{"[1]", "[2]", "[3]"}, []string{"watchid"}},
		{"DISTINCT", "SELECT DISTINCT SearchPhrase FROM hitscase ORDER BY 1",
			[]string{"[abc]", "[abd]", "[zzz]"}, []string{"searchphrase"}},
		{"a window partition and order key",
			"SELECT WatchID, ROW_NUMBER() OVER (PARTITION BY UserID ORDER BY WatchID) AS rn FROM hitscase ORDER BY 1",
			[]string{"[1 1]", "[2 2]", "[3 1]"}, []string{"watchid", "rn"}},
		{"a derived table", "SELECT s.w FROM (SELECT WatchID AS w FROM hitscase) s ORDER BY 1",
			[]string{"[1]", "[2]", "[3]"}, []string{"w"}},
		{"a CTE", "WITH c AS (SELECT WatchID, UserID FROM hitscase) SELECT SUM(UserID) AS s FROM c",
			[]string{"[31]"}, []string{"s"}},
		{"a set operation", "SELECT WatchID FROM hitscase UNION ALL SELECT UserID FROM hitscase ORDER BY 1",
			[]string{"[1]", "[2]", "[3]", "[10]", "[10]", "[11]"}, []string{"watchid"}},
		{"an IN subquery", "SELECT WatchID FROM hitscase WHERE UserID IN (SELECT UserID FROM hitscase) ORDER BY 1",
			[]string{"[1]", "[2]", "[3]"}, []string{"watchid"}},
		{"an EXISTS subquery",
			"SELECT COUNT(*) AS n FROM hitscase a WHERE EXISTS (SELECT 1 FROM hitscase b WHERE b.UserID = a.UserID)",
			[]string{"[3]"}, []string{"n"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := db.Query(ctx, tc.sql)
			if err != nil {
				t.Fatalf("%s: %v", tc.sql, err)
			}
			if len(res.Columns) != len(tc.cols) {
				t.Fatalf("published %v, want %v\n  SQL: %s", res.Columns, tc.cols, tc.sql)
			}
			for i, c := range tc.cols {
				if res.Columns[i] != c {
					t.Errorf("column %d published as %q, want %q (PostgreSQL's own name)\n  SQL: %s",
						i, res.Columns[i], c, tc.sql)
				}
			}
			if len(res.Rows) != len(tc.want) {
				t.Fatalf("%d rows, want %d\n  SQL: %s", len(res.Rows), len(tc.want), tc.sql)
			}
			for i, w := range tc.want {
				if got := fmt.Sprint(res.Cells(i)); got != w {
					t.Errorf("row %d = %s, want %s\n  SQL: %s", i, got, w, tc.sql)
				}
			}
		})
	}
}

// The boundary, from both sides (rule 11). A DELIMITED reference is held to
// the bytes PostgreSQL holds it to: `SELECT "G"` over a column `g` is 42703
// there, and answering it with a column of NULLs — which is what this engine
// did — is a silent wrong answer. The controls beside it are the delimited
// spellings that must keep WORKING.
func TestADelimitedIdentifierIsByteExact(t *testing.T) {
	ctx := context.Background()
	db := identCaseDB(t, ctx)
	for _, tc := range []struct {
		name  string
		sql   string
		state string // "" = must answer
	}{
		{"a delimited name no column carries", `SELECT "WATCHID" FROM hitscase`, "42703"},
		{"a delimited name differing only by case", `SELECT "Watchid" FROM hitscase`, "42703"},
		{"a delimited name in a filter", `SELECT UserID FROM hitscase WHERE "WATCHID" > 1`, "42703"},
		{"a delimited name in a GROUP BY", `SELECT "EVENTDATE" FROM hitscase GROUP BY "EVENTDATE"`, "42703"},
		{"ctl: the schema's own spelling", `SELECT "WatchID" FROM hitscase`, ""},
		{"ctl: an all-lower-case delimited name", `SELECT "watchid" FROM hitscase`, ""},
		{"ctl: a delimited ALIAS keeps its bytes", `SELECT WatchID AS "Kk" FROM hitscase ORDER BY "Kk"`, ""},
		{"ctl: the unquoted spelling", `SELECT WATCHID FROM hitscase`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := db.Query(ctx, tc.sql)
			if tc.state == "" {
				if err != nil {
					t.Fatalf("%s: refused with %v", tc.sql, err)
				}
				if len(res.Rows) != 3 {
					t.Fatalf("%s: %d rows, want 3", tc.sql, len(res.Rows))
				}
				return
			}
			if err == nil {
				t.Fatalf("%s: answered %d rows; PostgreSQL is %s", tc.sql, len(res.Rows), tc.state)
			}
			if got := sqlerr.StateOf(err); got != tc.state {
				t.Errorf("%s: SQLSTATE %q, want %q (err: %v)", tc.sql, got, tc.state, err)
			}
		})
	}
}

// A DELIMITED table alias is byte-exact too, and it names a DIFFERENT relation
// from an unquoted one spelled the same way (round-0 B2).
//
// PostgreSQL 17, measured live over `FROM clt1 t, clt2 "T"`: `t.c1` reads
// clt1's column, `"T".c1` reads clt2's, and `SELECT "T".k FROM clt1 T` — where
// the FROM declares the UNQUOTED alias — is 42P01 `missing FROM-clause entry
// for table "T"`. Wadjet folded the qualifier at both the binder and the
// resolver, so `t.c1` answered clt2's column: a wrong VALUE, not a wrong name.
func TestADelimitedQualifierIsByteExact(t *testing.T) {
	ctx := context.Background()
	db := identQualDB(t, ctx)
	for _, tc := range []struct {
		name  string
		sql   string
		want  string
		state string
	}{
		{name: "the unquoted alias reads its own relation",
			sql: `SELECT t.g AS x FROM qa t, qb "T"`, want: "[5]"},
		{name: "the delimited alias reads its own relation",
			sql: `SELECT "T".g AS y FROM qa t, qb "T"`, want: "[7]"},
		{name: "both, side by side",
			sql: `SELECT t.g AS x, "T".g AS y FROM qa t, qb "T"`, want: "[5 7]"},
		{name: "a delimited qualifier the FROM never declared",
			sql: `SELECT "T".k AS x FROM qa T`, state: "42P01"},
		{name: "ctl: two unquoted aliases",
			sql: "SELECT t.g AS x, u.g AS y FROM qa t, qb u", want: "[5 7]"},
		{name: "ctl: the delimited alias alone",
			sql: `SELECT "T".g AS y FROM qb "T"`, want: "[7]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := db.Query(ctx, tc.sql)
			if tc.state != "" {
				if err == nil {
					t.Fatalf("%s: answered %d rows; want SQLSTATE %s", tc.sql, len(res.Rows), tc.state)
				}
				if got := sqlerr.StateOf(err); got != tc.state {
					t.Fatalf("%s: SQLSTATE %q, want %q (err: %v)", tc.sql, got, tc.state, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s: %v", tc.sql, err)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("%s: %d rows, want 1", tc.sql, len(res.Rows))
			}
			if got := fmt.Sprint(res.Cells(0)); got != tc.want {
				t.Errorf("%s: %s, want %s", tc.sql, got, tc.want)
			}
		})
	}
}

func identQualDB(t *testing.T, ctx context.Context) *DB {
	t.Helper()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, ddl := range []string{
		"CREATE TABLE qa (k INT64, g INT64)",
		"CREATE TABLE qb (k INT64, g INT64)",
	} {
		if _, err := db.Query(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Execute(ctx, "INSERT INTO qa VALUES (1,5)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, "INSERT INTO qb VALUES (1,7)"); err != nil {
		t.Fatal(err)
	}
	return db
}

// A DDL declaration takes the same rule end to end: an unquoted name is
// stored folded, a delimited one keeps its bytes, and both are readable.
// Before #731 `parquet.DeclaredColumn` lowercased every declaration, so the
// one spelling PostgreSQL guarantees — the delimited one — was the one that
// could not be stored.
func TestDDLKeepsADelimitedColumnNamesBytes(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Query(ctx, `CREATE TABLE Ddlcase (Plain BIGINT, "MixedCase" BIGINT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, "INSERT INTO ddlcase VALUES (1, 2)"); err != nil {
		t.Fatal(err)
	}
	res, err := db.Query(ctx, "SELECT * FROM ddlcase")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"plain", "MixedCase"}
	if len(res.Columns) != 2 || res.Columns[0] != want[0] || res.Columns[1] != want[1] {
		t.Fatalf("stored columns %v, want %v", res.Columns, want)
	}
	for _, sql := range []string{
		"SELECT MixedCase FROM ddlcase",
		"SELECT mixedcase FROM ddlcase",
		`SELECT "MixedCase" FROM ddlcase`,
		"SELECT PLAIN FROM ddlcase",
	} {
		if _, err := db.Query(ctx, sql); err != nil {
			t.Errorf("%s: %v", sql, err)
		}
	}
}

func identCaseSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "WatchID", Type: parquet.TypeInt64},
		{Name: "UserID", Type: parquet.TypeInt64},
		{Name: "EventDate", Type: parquet.TypeInt32},
		{Name: "SearchPhrase", Type: parquet.TypeString},
	}}
}

// identCaseDB registers the fixture the way ClickBench's `hits` is
// registered: a schema probed off a parquet file, CamelCase names and all,
// through the Go API rather than the DDL parser.
func identCaseDB(t *testing.T, ctx context.Context) *DB {
	t.Helper()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := identCaseSchema()
	if err := db.CreateTable(ctx, "hitscase", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := []map[string]any{
		{"WatchID": int64(1), "UserID": int64(10), "EventDate": int32(20), "SearchPhrase": "abc"},
		{"WatchID": int64(2), "UserID": int64(10), "EventDate": int32(21), "SearchPhrase": "abd"},
		{"WatchID": int64(3), "UserID": int64(11), "EventDate": int32(20), "SearchPhrase": "zzz"},
	}
	ing := db.NewIngester("hitscase", schema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 2})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}
