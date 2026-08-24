package wadjet

import (
	"context"
	"math/big"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #474: GROUP BY, DISTINCT and hash join over a DECIMAL column keyed on the
// float64 BITS of the value, so two DECIMALs differing past double precision
// were one key. The comparator, the predicate kernels, ORDER BY and — since
// #455 — the aggregates themselves were all exact, so the wrong answer arrived
// through the key alone.
//
// The two values are the issue's own: DECIMAL(38,10) 977777777887777.7577887713
// and ...714, one unit apart in the 25th significant digit. float64 carries
// ~16, so both round to the same double and shared a key.
//
// The repair cannot be "key on the raw 16 bytes of the unscaled Int128" — that
// makes the key depend on the SCALE, and 12.75 in a DECIMAL(9,2) would stop
// matching 12.75 in a DECIMAL(18,4) in a join between two tables that declare
// the same quantity differently. The key is the value's canonical (unscaled,
// minimal scale) form instead, which is exact AND scale-blind, so it agrees
// with kernel.CompareDecimalAt in both directions (ADR-0012 item 8).
//
// Live postgres:17-alpine over the same rows:
//
//	CREATE TABLE keyt (d numeric(38,10));
//	INSERT INTO keyt VALUES (977777777887777.7577887713),(977777777887777.7577887714);
//	SELECT count(*) FROM (SELECT d FROM keyt GROUP BY d) g      -> 2
//	SELECT count(DISTINCT d) FROM keyt                          -> 2
//	SELECT count(*) FROM keyt a JOIN keyt b ON a.d = b.d        -> 2
//	-- and, across scales:
//	SELECT numeric '12.75' = numeric '12.7500'                  -> t

const dkWide = "9777777778877777577887713" // unscaled, scale 10

func dkInt128(t *testing.T, digits string) parquet.Decimal128 {
	t.Helper()
	n, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		t.Fatalf("bad fixture %q", digits)
	}
	m := new(big.Int).Set(n)
	if m.Sign() < 0 {
		m.Add(m, new(big.Int).Lsh(big.NewInt(1), 128))
	}
	var b [16]byte
	m.FillBytes(b[:])
	hi := new(big.Int).Rsh(new(big.Int).SetBytes(b[:]), 64).Uint64()
	lo := new(big.Int).And(new(big.Int).SetBytes(b[:]), new(big.Int).SetUint64(^uint64(0))).Uint64()
	return parquet.Decimal128{Hi: int64(hi), Lo: lo}
}

func dkIngest(t *testing.T, ctx context.Context, db *DB, name string, schema parquet.Schema, rows []map[string]any) {
	t.Helper()
	if err := db.CreateTable(ctx, name, schema, nil); err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	ing := db.NewIngester(name, schema, nil, ingest.Config{MaxBufferRows: len(rows) + 1, RowGroupSize: 8})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatalf("ingest %s: %v", name, err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush %s: %v", name, err)
	}
}

// dkCount runs a single-cell COUNT query and returns it. Shared with the
// float-key gate in float_key_pg_order_test.go.
func dkCount(t *testing.T, ctx context.Context, db *DB, sql string) int64 {
	t.Helper()
	res, err := tmRun(ctx, db, sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("%s: %d rows, want 1", sql, len(res.Rows))
	}
	n, ok := tmAsInt64(res.Rows[0][res.Columns[0]])
	if !ok {
		t.Fatalf("%s: count came back as %#v", sql, res.Rows[0][res.Columns[0]])
	}
	return n
}

// TestDecimalKeysAreExactPastDoublePrecision is the issue's reproduction.
func TestDecimalKeysAreExactPastDoublePrecision(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Four values, pairwise one unit apart in the 25th digit and the 22nd —
	// all four collapse to two doubles, and the two 25th-digit neighbours
	// collapse to ONE. Each appears twice, so a correct engine reports four
	// groups of two and a wrong one reports fewer, larger groups.
	base, _ := new(big.Int).SetString(dkWide, 10)
	vals := []*big.Int{
		new(big.Int).Set(base),
		new(big.Int).Add(base, big.NewInt(1)),
		new(big.Int).Add(base, big.NewInt(1000)),
		new(big.Int).Add(base, big.NewInt(1001)),
	}
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 38, Scale: 10, Nullable: true},
	}}
	var rows []map[string]any
	for i, v := range vals {
		for rep := 0; rep < 2; rep++ {
			rows = append(rows, map[string]any{
				"id": int64(len(rows)), "d": dkInt128(t, v.String()),
			})
		}
		_ = i
	}
	dkIngest(t, ctx, db, "keyt", schema, rows)

	for _, tc := range []struct {
		name string
		sql  string
		want int64
	}{
		{"group_by", "SELECT COUNT(*) AS n FROM (SELECT d, COUNT(*) AS c FROM keyt GROUP BY d) g", 4},
		{"group_by_counts", "SELECT COUNT(*) AS n FROM (SELECT d, COUNT(*) AS c FROM keyt GROUP BY d HAVING COUNT(*) = 2) g", 4},
		{"count_distinct", "SELECT COUNT(DISTINCT d) AS n FROM keyt", 4},
		{"select_distinct", "SELECT COUNT(*) AS n FROM (SELECT DISTINCT d FROM keyt) s", 4},
		// 8 rows, each matching its own value's two rows: 4 values x 2 x 2.
		{"self_join", "SELECT COUNT(*) AS n FROM keyt a JOIN keyt b ON a.d = b.d", 16},
		// A second group column alongside forces the multi-column key path.
		{"group_by_pair", "SELECT COUNT(*) AS n FROM (SELECT d, id % 2 AS p, COUNT(*) AS c FROM keyt GROUP BY d, p) g", 8},
		// UNION's dedup is the same key machinery.
		{"union_dedup", "SELECT COUNT(*) AS n FROM (SELECT d FROM keyt UNION SELECT d FROM keyt) u", 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := dkCount(t, ctx, db, tc.sql); got != tc.want {
				t.Errorf("%s\n  got %d, want %d (live PostgreSQL 17)", tc.sql, got, tc.want)
			}
		})
	}
}

// TestDecimalKeysJoinAcrossScales is the other half of the invariant, and the
// reason the key is not simply the unscaled integer: the same quantity stored
// at two different scales must still join, group and dedup as one value —
// which is exactly what kernel.CompareDecimalAt already says about it.
func TestDecimalKeysJoinAcrossScales(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// dk2 declares DECIMAL(9,2), dk4 declares DECIMAL(18,4), and both hold
	// 12.75, 3.00 and 0.01 — plus one value only one of them has.
	s2 := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
	}}
	s4 := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true},
	}}
	dkIngest(t, ctx, db, "dk2", s2, []map[string]any{
		{"id": int64(1), "d": dkInt128(t, "1275")}, // 12.75
		{"id": int64(2), "d": dkInt128(t, "300")},  // 3.00
		{"id": int64(3), "d": dkInt128(t, "1")},    // 0.01
		{"id": int64(4), "d": dkInt128(t, "999")},  // 9.99, no partner
	})
	dkIngest(t, ctx, db, "dk4", s4, []map[string]any{
		{"id": int64(1), "d": dkInt128(t, "127500")}, // 12.7500
		{"id": int64(2), "d": dkInt128(t, "30000")},  // 3.0000
		{"id": int64(3), "d": dkInt128(t, "100")},    // 0.0100
		{"id": int64(4), "d": dkInt128(t, "12751")},  // 1.2751, no partner
	})

	for _, tc := range []struct {
		name string
		sql  string
		want int64
	}{
		{"join", "SELECT COUNT(*) AS n FROM dk2 a JOIN dk4 b ON a.d = b.d", 3},
		{"join_reversed", "SELECT COUNT(*) AS n FROM dk4 a JOIN dk2 b ON a.d = b.d", 3},
		{"left_join", "SELECT COUNT(*) AS n FROM dk2 a LEFT JOIN dk4 b ON a.d = b.d", 4},
		{"semi", "SELECT COUNT(*) AS n FROM dk2 a WHERE EXISTS (SELECT 1 FROM dk4 b WHERE b.d = a.d)", 3},
		{"anti", "SELECT COUNT(*) AS n FROM dk2 a WHERE NOT EXISTS (SELECT 1 FROM dk4 b WHERE b.d = a.d)", 1},
		// GROUP BY over the concatenation is the key layer again, and it
		// unifies the scales: 5 distinct values (3 shared + 9.99 + 1.2751).
		{"group_by_union_all", "SELECT COUNT(*) AS n FROM (SELECT u.d, COUNT(*) AS c FROM " +
			"(SELECT d FROM dk2 UNION ALL SELECT d FROM dk4) u GROUP BY u.d) g", 5},
		// NOTE: `SELECT d FROM dk2 UNION SELECT d FROM dk4` still answers 8.
		// That is NOT this key layer: the single-process set-op path boxes
		// every row into a map[string]any and dedups on rowHashKey
		// (internal/planner/physical/plan.go, setOpSourceAdapter), where a
		// DECIMAL is its RENDERED TEXT at its own scale — "12.75" against
		// "12.7500". Filed separately; the columnar key above is what this
		// test is about.
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := dkCount(t, ctx, db, tc.sql); got != tc.want {
				t.Errorf("%s\n  got %d, want %d (live PostgreSQL 17)", tc.sql, got, tc.want)
			}
		})
	}
}
