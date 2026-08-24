package wadjet

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// #483 end to end, at the embedded API. Standalone reaches the bug by holding
// two *catalog.Catalog over one KV (pgwire sends SELECT to the coordinator's
// and DML/DDL to the DB's), and cmd/wadjet builds the pgwire DB with
// wadjet.Open over the SAME MetaKV the coordinator holds. Two DB handles over
// one MetaKV + one store is that shape with nothing else in the way: writes
// through one must be visible to the very next read through the other.
func twoDBsOverOneCatalog(t *testing.T) (writer, reader *DB) {
	t.Helper()
	ctx := context.Background()
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatalf("make bucket: %v", err)
	}
	kv := catalog.NewMemKV()
	open := func(what string) *DB {
		db, err := Open(ctx, Config{Store: store, Bucket: "test", MetaKV: kv})
		if err != nil {
			t.Fatalf("open %s DB: %v", what, err)
		}
		t.Cleanup(func() { db.Close() })
		return db
	}
	return open("writer"), open("reader")
}

// runSQL sends DDL through Query and DML through Execute, the same split
// pgwire makes.
func runSQL(t *testing.T, db *DB, sql string) {
	t.Helper()
	ctx := context.Background()
	head := strings.ToUpper(strings.Fields(strings.TrimSpace(sql))[0])
	switch head {
	case "INSERT", "UPDATE", "DELETE":
		if _, err := db.Execute(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	default:
		if _, err := db.Query(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
}

// queryColumn returns one column of a query's rows, rendered and sorted so a
// comparison does not depend on scan order.
func queryColumn(t *testing.T, db *DB, sql, col string) []string {
	t.Helper()
	res, err := db.Query(context.Background(), sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	out := make([]string, 0, len(res.Rows))
	for _, row := range res.Rows {
		out = append(out, fmt.Sprint(row[col]))
	}
	sort.Strings(out)
	return out
}

func TestSelectAfterInsertSeesTheInsert(t *testing.T) {
	writer, reader := twoDBsOverOneCatalog(t)

	runSQL(t, writer, "CREATE TABLE tp6 (c0 BIGINT, c1 TEXT)")
	runSQL(t, writer, "INSERT INTO tp6 VALUES (1, 'a')")

	// The first read is the statement that used to freeze the reader's view
	// of tp6 for as long as its cache entry lived.
	if got := queryColumn(t, reader, "SELECT c0, c1 FROM tp6", "c0"); len(got) != 1 {
		t.Fatalf("first read: %v, want 1 row", got)
	}

	runSQL(t, writer, "INSERT INTO tp6 VALUES (2, 'b')")
	if got, want := queryColumn(t, reader, "SELECT c0, c1 FROM tp6", "c0"), []string{"1", "2"}; !equalStrings(got, want) {
		t.Fatalf("after the second INSERT: %v, want %v", got, want)
	}

	// A never-before-run query shape against the same table, to rule out a
	// text- or plan-keyed result cache as the thing being tested.
	runSQL(t, writer, "INSERT INTO tp6 VALUES (3, 'c')")
	if got, want := queryColumn(t, reader, "SELECT c1, c0 FROM tp6 WHERE c0 > 0", "c0"), []string{"1", "2", "3"}; !equalStrings(got, want) {
		t.Fatalf("after the third INSERT, new query shape: %v, want %v", got, want)
	}
}

func TestSelectAfterDeleteAndUpdateSeesTheMutation(t *testing.T) {
	writer, reader := twoDBsOverOneCatalog(t)

	runSQL(t, writer, "CREATE TABLE probe4 (c0 BIGINT, c1 TEXT)")
	runSQL(t, writer, "INSERT INTO probe4 VALUES (1, 'a')")
	runSQL(t, writer, "INSERT INTO probe4 VALUES (2, 'b')")
	if got := queryColumn(t, reader, "SELECT * FROM probe4", "c0"); len(got) != 2 {
		t.Fatalf("first read: %v, want 2 rows", got)
	}

	// DELETE is merge-on-read: it writes delete markers into the manifest,
	// so a frozen manifest is a DELETE that reports success and never
	// happens — #483's opening repro.
	runSQL(t, writer, "DELETE FROM probe4 WHERE c0 = 1")
	if got, want := queryColumn(t, reader, "SELECT * FROM probe4", "c0"), []string{"2"}; !equalStrings(got, want) {
		t.Fatalf("after DELETE: %v, want %v", got, want)
	}

	// UPDATE is delete markers plus a re-ingest, so a frozen manifest shows
	// it either as nothing at all or as a phantom duplicate of the old row.
	runSQL(t, writer, "UPDATE probe4 SET c0 = 5 WHERE c1 = 'b'")
	if got, want := queryColumn(t, reader, "SELECT * FROM probe4", "c0"), []string{"5"}; !equalStrings(got, want) {
		t.Fatalf("after UPDATE: %v, want %v", got, want)
	}
}

func TestDropAndRecreateDoesNotServeThePreviousIncarnation(t *testing.T) {
	t.Run("same schema", func(t *testing.T) {
		writer, reader := twoDBsOverOneCatalog(t)
		runSQL(t, writer, "CREATE TABLE repro4 (c0 BIGINT)")
		runSQL(t, writer, "INSERT INTO repro4 VALUES (111)")
		if got, want := queryColumn(t, reader, "SELECT * FROM repro4", "c0"), []string{"111"}; !equalStrings(got, want) {
			t.Fatalf("first incarnation: %v, want %v", got, want)
		}

		runSQL(t, writer, "DROP TABLE repro4")
		runSQL(t, writer, "CREATE TABLE repro4 (c0 BIGINT)")
		runSQL(t, writer, "INSERT INTO repro4 VALUES (222)")

		// Encoding-compatible schemas: nothing downstream can catch this one,
		// so a stale manifest here is a silently wrong answer.
		if got, want := queryColumn(t, reader, "SELECT * FROM repro4", "c0"), []string{"222"}; !equalStrings(got, want) {
			t.Fatalf("recreated table: %v, want %v", got, want)
		}
	})

	t.Run("different schema", func(t *testing.T) {
		writer, reader := twoDBsOverOneCatalog(t)
		runSQL(t, writer, "CREATE TABLE repro2 (c0 BIGINT, c1 TEXT)")
		runSQL(t, writer, "INSERT INTO repro2 VALUES (1, 'hello')")
		if got, want := queryColumn(t, reader, "SELECT * FROM repro2", "c1"), []string{"hello"}; !equalStrings(got, want) {
			t.Fatalf("first incarnation: %v, want %v", got, want)
		}

		runSQL(t, writer, "DROP TABLE repro2")
		runSQL(t, writer, "CREATE TABLE repro2 (c0 BIGINT, c1 BIGINT)")
		runSQL(t, writer, "INSERT INTO repro2 VALUES (2, 999)")

		// The dropped incarnation's file stores c1 as STRING. If it reaches
		// this scan the reader either decodes old bytes under the new
		// schema — the parquet-safety class — or refuses at decode time,
		// which is what #483 actually observed. Neither may happen.
		if got, want := queryColumn(t, reader, "SELECT * FROM repro2", "c1"), []string{"999"}; !equalStrings(got, want) {
			t.Fatalf("recreated table with a new column type: %v, want %v", got, want)
		}
	})
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
