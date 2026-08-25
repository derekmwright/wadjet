package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestOuterJoinKeepsNullKeyedBuildRowsForEveryKeyType is #496's gate.
//
// A RIGHT / FULL OUTER join over an INTEGER key silently dropped every build
// row whose key was NULL. Such a row is unmatched by definition — NULL equals
// nothing — so the outer join owes it a NULL-padded output row, and instead it
// vanished. The same fixture with a TEXT key answered correctly, which is what
// said the defect was in the key ENCODING and not in the join.
//
// The integer build paths skipped the ARENA APPEND along with the hash-index
// insert, and FlushUnmatched / FlushAntiMatched enumerate the arena — so a row
// that never reached it was invisible to them. The serialized-key path append-
// ed every build row including the NULL-keyed ones and was right by accident.
// Skipping the index insert is correct (a NULL key must not match); skipping
// the row is what lost it.
//
// Every fixture below is the SAME SHAPE — two NULL keys, one key value shared
// by two rows, one unique key — so PostgreSQL's answer is the same for every
// type: INNER 5, LEFT 7, RIGHT 7, FULL 9 (live postgres:17-alpine transcript
// over `nki(id bigint, k bigint)` with (1,NULL),(2,NULL),(3,10),(4,10),(5,20)).
// The types are the axis, because which build loop runs is decided by the key
// COLUMN's type: single int, two ints, float, and the serialized fallback are
// four different loops.
func TestOuterJoinKeepsNullKeyedBuildRowsForEveryKeyType(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	// Five rows per table: two NULL keys, a duplicated key, a unique key.
	// dup/uniq are the two non-null key values in that type's domain.
	types := []struct {
		name     string
		colType  parquet.TypeID
		dup, uni any
	}{
		{"int64", parquet.TypeInt64, int64(10), int64(20)},
		{"int32", parquet.TypeInt32, int32(10), int32(20)},
		{"string", parquet.TypeString, "ten", "twenty"},
		{"float64", parquet.TypeFloat64, float64(10.5), float64(20.5)},
		{"date", parquet.TypeDate, int32(19000), int32(19100)},
		{"timestamp", parquet.TypeTimestamp, int64(1700000000000000), int64(1800000000000000)},
		{"bool", parquet.TypeBool, true, false},
	}

	for _, ty := range types {
		table := "nk_" + ty.name
		schema := parquet.Schema{Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "k", Type: ty.colType, Nullable: true},
			// A second key column, for the two-integer-key build path: it
			// tracks k exactly, so the two-column join has the same answer.
			{Name: "k2", Type: parquet.TypeInt64, Nullable: true},
		}}
		if err := db.CreateTable(ctx, table, schema, nil); err != nil {
			t.Fatalf("create %s: %v", table, err)
		}
		rows := []map[string]any{
			{"id": int64(1), "k": nil, "k2": nil},
			{"id": int64(2), "k": nil, "k2": nil},
			{"id": int64(3), "k": ty.dup, "k2": int64(10)},
			{"id": int64(4), "k": ty.dup, "k2": int64(10)},
			{"id": int64(5), "k": ty.uni, "k2": int64(20)},
		}
		ing := db.NewIngester(table, schema, nil, ingest.Config{MaxBufferRows: len(rows) + 1, RowGroupSize: 8})
		if err := ing.Ingest(ctx, rows); err != nil {
			t.Fatalf("ingest %s: %v", table, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", table, err)
		}
	}

	// PostgreSQL 17 over the same shape. The bool fixture has no "unique"
	// value in a two-value domain, so it gets its own row below.
	kinds := []struct {
		join string
		want int64
	}{
		{"JOIN", 5},
		{"LEFT JOIN", 7},
		{"RIGHT JOIN", 7},
		{"FULL OUTER JOIN", 9},
	}
	// TRUE twice, FALSE once, two NULLs: INNER 2×2 + 1×1 = 5, and the two
	// NULL rows are what each outer side adds.
	boolKinds := []struct {
		join string
		want int64
	}{
		{"JOIN", 5},
		{"LEFT JOIN", 7},
		{"RIGHT JOIN", 7},
		{"FULL OUTER JOIN", 9},
	}

	for _, ty := range types {
		table := "nk_" + ty.name
		want := kinds
		if ty.name == "bool" {
			want = boolKinds
		}
		for _, k := range want {
			t.Run(ty.name+"/"+sqlName(k.join), func(t *testing.T) {
				sql := fmt.Sprintf(`SELECT COUNT(*) AS n FROM %s a %s %s b ON a.k = b.k`,
					table, k.join, table)
				assertCount(t, ctx, db, sql, k.want)
			})
			t.Run(ty.name+"/dual_key_"+sqlName(k.join), func(t *testing.T) {
				// Two key columns takes a different build loop (dualIntKey
				// when both are integers, the serialized one otherwise), and
				// it had the same missing arena append.
				sql := fmt.Sprintf(`SELECT COUNT(*) AS n FROM %s a %s %s b ON a.k = b.k AND a.k2 = b.k2`,
					table, k.join, table)
				assertCount(t, ctx, db, sql, k.want)
			})
		}
	}

	// The two-integer-key path proper: both key columns integers, so
	// tryEnableIntKey takes useDualIntKey rather than the serialized
	// fallback. nk_int64.k is Int64 and k2 is Int64, which the dual-key
	// subtests above already cover — this asserts the VALUES an outer join
	// owes rather than only the count, because a right count with the wrong
	// rows is what a pin is supposed to catch (ADR-0013 §Pins).
	res, err := db.Query(ctx,
		`SELECT b.id AS id FROM nk_int64 a RIGHT JOIN nk_int64 b ON a.k = b.k WHERE a.id IS NULL ORDER BY b.id`)
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("RIGHT JOIN emitted %d NULL-padded rows, want 2 (the NULL-keyed build rows)", len(res.Rows))
	}
	for i, want := range []int64{1, 2} {
		if got := res.Rows[i]["id"]; got != want {
			t.Errorf("NULL-padded row %d is id=%v, want %d", i, got, want)
		}
	}
}

// sqlName turns a join keyword into a subtest-safe name.
func sqlName(join string) string {
	switch join {
	case "JOIN":
		return "inner"
	case "LEFT JOIN":
		return "left"
	case "RIGHT JOIN":
		return "right"
	default:
		return "full"
	}
}

func assertCount(t *testing.T, ctx context.Context, db *DB, sql string, want int64) {
	t.Helper()
	res, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("query error: %v\n  SQL: %s", err, sql)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("got %d rows, want 1 (scalar COUNT)\n  SQL: %s", len(res.Rows), sql)
	}
	if got := res.Rows[0]["n"]; got != want {
		t.Errorf("COUNT(*) = %v, want %d (PostgreSQL 17)\n  SQL: %s", got, want, sql)
	}
}
