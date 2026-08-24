package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #459: the predicate kernels, the primary (non-spilling) GROUP BY / DISTINCT
// hash key and the hash-join key still compared and hashed floats as raw
// IEEE754 bits, while ORDER BY, rank() and the spilled merge key had already
// moved to PostgreSQL's float total order — NaN greatest, NaN equal to itself,
// -0.0 equal to +0.0 (ADR-0012 item 8).
//
// Every expected value below was read off live postgres:17-alpine over the
// same eight rows:
//
//	INSERT INTO ft VALUES (1,'NaN'),(2,0.0),(3,-0.0),(4,'Infinity'),
//	                      (5,'-Infinity'),(6,1.0),(7,2.0),(8,NULL);
//
//	WHERE f = f                 -> 7          (NaN equals itself)
//	WHERE f > 1e300             -> {1,4}      (NaN is greater than everything)
//	WHERE f >= 1e300            -> {1,4}
//	WHERE f <  1e300            -> {2,3,5,6,7}
//	WHERE f <> 1e300            -> {1,...,7}
//	WHERE f =  'NaN'            -> {1}
//	WHERE f <= 'NaN'            -> {1,...,7}
//	WHERE f >  'NaN'            -> {}
//	GROUP BY f                  -> 7 groups, the two zeros in ONE of count 2
//	SELECT DISTINCT f           -> 7 rows
//	ft a JOIN ft b ON a.f=b.f   -> 9 pairs: (1,1) NaN-NaN, the four zero
//	                               cross-pairs, and no pair for the NULL
//	ft a LEFT JOIN ft b         -> 10 rows
//	WHERE f IN ('NaN', 1.0)     -> {1,6}
//	WHERE f NOT IN ('NaN', 1.0) -> {2,3,4,5,7}
//
// The fixture STORES a kind tag rather than the values: ingest JSON-encodes
// row-group statistics into the catalog manifest and encoding/json refuses
// NaN and ±Inf, so those cannot be ingested today (the same constraint
// internal/coordinator/nan_minmax_two_path_test.go documents for #457). The
// query manufactures them with CAST, which is also the only way a NaN reaches
// a wadjet table in practice. `z` is stored, and carries the ±0.0 half on the
// real scan path.
const fkTable = "floatkey"

// fkValueExpr rebuilds the eight-row column from the stored tag.
const fkValueExpr = `CASE kind
	WHEN 'nan' THEN CAST('NaN' AS DOUBLE PRECISION)
	WHEN 'pinf' THEN CAST('Infinity' AS DOUBLE PRECISION)
	WHEN 'ninf' THEN CAST('-Infinity' AS DOUBLE PRECISION)
	WHEN 'null' THEN CAST(NULL AS DOUBLE PRECISION)
	ELSE z
	END`

func fkSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "kind", Type: parquet.TypeString},
		{Name: "z", Type: parquet.TypeFloat64, Nullable: true},
	}}
}

func fkRows() []map[string]any {
	negZero := float64(0)
	negZero = -negZero
	return []map[string]any{
		{"id": int64(1), "kind": "nan", "z": nil},
		{"id": int64(2), "kind": "val", "z": 0.0},
		{"id": int64(3), "kind": "val", "z": negZero},
		{"id": int64(4), "kind": "pinf", "z": nil},
		{"id": int64(5), "kind": "ninf", "z": nil},
		{"id": int64(6), "kind": "val", "z": 1.0},
		{"id": int64(7), "kind": "val", "z": 2.0},
		{"id": int64(8), "kind": "null", "z": nil},
	}
}

func fkOpen(t *testing.T, ctx context.Context) *DB {
	t.Helper()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := fkSchema()
	if err := db.CreateTable(ctx, fkTable, schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := fkRows()
	// Two row groups, so the scan really produces more than one batch and a
	// group/join key formed in one has to agree with one formed in the other.
	ing := db.NewIngester(fkTable, schema, nil, ingest.Config{
		MaxBufferRows: len(rows) + 1, RowGroupSize: 4,
	})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

// TestFloatPredicatesFollowPostgresNaNOrder is the predicate half of the
// matrix, run with pruning both on and off — the row-group prune reads the
// same predicate and must not delete a row the filter keeps, which for a
// float column means it cannot prune `>`, `>=` or `<>` at all (a NaN is
// invisible to min/max statistics by the parquet specification).
func TestFloatPredicatesFollowPostgresNaNOrder(t *testing.T) {
	ctx := context.Background()
	db := fkOpen(t, ctx)
	v := fkValueExpr

	cases := []struct {
		name string
		sql  string
		want int64
	}{
		{"self_eq", fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE (%s) = (%s)", fkTable, v, v), 7},
		{"gt_1e300", fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE (%s) > 1e300", fkTable, v), 2},
		{"ge_1e300", fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE (%s) >= 1e300", fkTable, v), 2},
		{"lt_1e300", fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE (%s) < 1e300", fkTable, v), 5},
		{"le_1e300", fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE (%s) <= 1e300", fkTable, v), 5},
		{"eq_1e300", fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE (%s) = 1e300", fkTable, v), 0},
		{"ne_1e300", fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE (%s) <> 1e300", fkTable, v), 7},
		{"eq_nan", fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE (%s) = CAST('NaN' AS DOUBLE PRECISION)", fkTable, v), 1},
		{"ne_nan", fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE (%s) <> CAST('NaN' AS DOUBLE PRECISION)", fkTable, v), 6},
		{"lt_nan", fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE (%s) < CAST('NaN' AS DOUBLE PRECISION)", fkTable, v), 6},
		{"le_nan", fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE (%s) <= CAST('NaN' AS DOUBLE PRECISION)", fkTable, v), 7},
		{"gt_nan", fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE (%s) > CAST('NaN' AS DOUBLE PRECISION)", fkTable, v), 0},
		{"ge_nan", fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE (%s) >= CAST('NaN' AS DOUBLE PRECISION)", fkTable, v), 1},
		{"in_nan", fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE (%s) IN (CAST('NaN' AS DOUBLE PRECISION), 1.0)", fkTable, v), 2},
		{"not_in_nan", fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE (%s) NOT IN (CAST('NaN' AS DOUBLE PRECISION), 1.0)", fkTable, v), 5},
		// The stored column, so the predicate meets the scan's own kernel and
		// the row-group prune rather than an expression result.
		// z is nil for the four computed kinds, so its non-null values are
		// 0.0, -0.0, 1.0 and 2.0 - all four are >= 0.
		{"stored_ge_zero", fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE z >= 0", fkTable), 4},
		{"stored_eq_zero", fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE z = 0", fkTable), 2},
		{"stored_gt_ten", fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE z > 10", fkTable), 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, prune := range []bool{true, false} {
				prevStats := scan.StatsPrune.Set(prune)
				prevDict := scan.DictPrune.Set(prune)
				got := dkCount(t, ctx, db, tc.sql)
				scan.StatsPrune.Set(prevStats)
				scan.DictPrune.Set(prevDict)
				if got != tc.want {
					t.Errorf("prune=%v: %s\n  got %d, want %d (live PostgreSQL 17)", prune, tc.sql, got, tc.want)
				}
			}
		})
	}
}

// TestFloatGroupKeyFoldsNaNAndNegativeZero is the KEY half: GROUP BY,
// DISTINCT and the hash join must put -0.0 with +0.0 and every NaN in one
// place, because the comparator does and because ORDER BY already did.
func TestFloatGroupKeyFoldsNaNAndNegativeZero(t *testing.T) {
	ctx := context.Background()
	db := fkOpen(t, ctx)
	v := fkValueExpr
	derived := fmt.Sprintf("(SELECT id, (%s) AS f FROM %s)", v, fkTable)

	cases := []struct {
		name string
		sql  string
		want int64
	}{
		// 7 groups: NaN, 0 (both zeros), +Inf, -Inf, 1, 2, NULL.
		{"group_by", fmt.Sprintf("SELECT COUNT(*) AS n FROM (SELECT (%s) AS f, COUNT(*) AS c FROM %s GROUP BY 1) g", v, fkTable), 7},
		{"distinct", fmt.Sprintf("SELECT COUNT(*) AS n FROM (SELECT DISTINCT (%s) AS f FROM %s) d", v, fkTable), 7},
		{"count_distinct", fmt.Sprintf("SELECT COUNT(DISTINCT (%s)) AS n FROM %s", v, fkTable), 6}, // COUNT(DISTINCT) skips NULL
		// The stored ±0.0 column, straight off the scan: one group, one
		// distinct value, and the two rows join four ways.
		{"stored_group_by_zero", fmt.Sprintf("SELECT COUNT(*) AS n FROM (SELECT z, COUNT(*) AS c FROM %s WHERE z = 0 GROUP BY z) g", fkTable), 1},
		{"stored_distinct_zero", fmt.Sprintf("SELECT COUNT(*) AS n FROM (SELECT DISTINCT z FROM %s WHERE z = 0) d", fkTable), 1},
		// Self hash join. 9 pairs, from live PostgreSQL: (1,1) NaN with
		// itself, {2,3}x{2,3} the two zeros, and (4,4) (5,5) (6,6) (7,7).
		// The NULL row (8) matches NOTHING, itself included.
		{"self_join", fmt.Sprintf("SELECT COUNT(*) AS n FROM %s a JOIN %s b ON a.f = b.f", derived, derived), 9},
		{"self_left_join", fmt.Sprintf("SELECT COUNT(*) AS n FROM %s a LEFT JOIN %s b ON a.f = b.f", derived, derived), 10},
		{"stored_self_join_zero", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM (SELECT id, z FROM %s WHERE z = 0) a JOIN (SELECT id, z FROM %s WHERE z = 0) b ON a.z = b.z",
			fkTable, fkTable), 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := dkCount(t, ctx, db, tc.sql); got != tc.want {
				t.Errorf("%s\n  got %d, want %d (live PostgreSQL 17)", tc.sql, got, tc.want)
			}
		})
	}
}

// TestHashJoinNullKeyMatchesNothing pins the separate defect #459 names in
// passing: the serialized ("string") join key encoded a NULL as a lone flag
// byte with no payload, so two NULL rows produced identical key bytes and the
// hash table — which matches by byte equality — joined them. SQL says `=` is
// UNKNOWN against a NULL, so a NULL never matches, not even another NULL; the
// integer key paths have always agreed, so which answer a query got depended
// on whether its key columns happened to be integers.
//
// The key column here is a STRING so the join takes the serialized path.
func TestHashJoinNullKeyMatchesNothing(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "k", Type: parquet.TypeString, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "nk", schema, nil); err != nil {
		t.Fatal(err)
	}
	// Two NULL keys, two rows sharing "a", one "b".
	rows := []map[string]any{
		{"id": int64(1), "k": nil},
		{"id": int64(2), "k": nil},
		{"id": int64(3), "k": "a"},
		{"id": int64(4), "k": "a"},
		{"id": int64(5), "k": "b"},
	}
	ing := db.NewIngester("nk", schema, nil, ingest.Config{MaxBufferRows: 16, RowGroupSize: 8})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	// Live PostgreSQL over the same five rows: the inner self-join is 5 pairs
	// (2x2 on "a" plus (5,5) on "b") and the NULLs contribute none; the LEFT
	// join adds one row per unmatched NULL, and the FULL join adds them on
	// both sides.
	for _, tc := range []struct {
		name string
		sql  string
		want int64
	}{
		{"inner", "SELECT COUNT(*) AS n FROM nk a JOIN nk b ON a.k = b.k", 5},
		{"left", "SELECT COUNT(*) AS n FROM nk a LEFT JOIN nk b ON a.k = b.k", 7},
		{"right", "SELECT COUNT(*) AS n FROM nk a RIGHT JOIN nk b ON a.k = b.k", 7},
		{"full", "SELECT COUNT(*) AS n FROM nk a FULL OUTER JOIN nk b ON a.k = b.k", 9},
		{"semi", "SELECT COUNT(*) AS n FROM nk a WHERE EXISTS (SELECT 1 FROM nk b WHERE b.k = a.k)", 3},
		{"anti", "SELECT COUNT(*) AS n FROM nk a WHERE NOT EXISTS (SELECT 1 FROM nk b WHERE b.k = a.k)", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := dkCount(t, ctx, db, tc.sql); got != tc.want {
				t.Errorf("%s\n  got %d, want %d (live PostgreSQL 17)", tc.sql, got, tc.want)
			}
		})
	}
}
