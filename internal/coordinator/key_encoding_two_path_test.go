package coordinator

import (
	"context"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The distributed half of #474 and #459's key arms.
//
// #474: a DECIMAL group / DISTINCT / join key was the float64 BITS of the
// value, so values differing past ~16 significant digits shared a key.
// #459: a FLOAT key was the raw IEEE bits, so -0.0 and +0.0 were two groups
// and two NaNs of different payload were two more — while the comparator,
// ORDER BY and the spilled merge key had already folded them.
//
// Both are fixed in one place, `appendColumnValue` (exec/aggregate.go), which
// the in-process gates in internal/engine/exec and wadjet cover. What they
// cannot see is the SHUFFLE: `hashRowsIntoPartitions`
// (internal/worker/partitioned_shuffle_sink.go) routes rows to exchange
// partitions with its OWN hash, and a router that disagrees with the key
// sends two equal values to different workers, where no key comparison can
// ever bring them back together. That hash keyed a DECIMAL on the raw Int128
// (scale-dependent, so a cross-scale join stops co-partitioning) and a float
// on raw IEEE bits (so ±0.0 and two NaN payloads split). This gate is the
// proof the two agree, over the same rows, through the real DAG.
const ketTable = "keyenc"

// ketValueExpr manufactures the NaN/±Inf values, which cannot be ingested:
// ingest JSON-encodes row-group statistics into the catalog manifest and
// encoding/json refuses them (see nan_minmax_two_path_test.go).
const ketValueExpr = `CASE kind
	WHEN 'nan' THEN CAST('NaN' AS DOUBLE PRECISION)
	WHEN 'pinf' THEN CAST('Infinity' AS DOUBLE PRECISION)
	WHEN 'ninf' THEN CAST('-Infinity' AS DOUBLE PRECISION)
	WHEN 'null' THEN CAST(NULL AS DOUBLE PRECISION)
	ELSE z
	END`

func ketSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "kind", Type: parquet.TypeString},
		{Name: "z", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 38, Scale: 10, Nullable: true},
	}}
}

// ketWide is 977777777887777.7577887713 unscaled at scale 10 — the value
// #474 names. The fixture holds it and three neighbours, two of which differ
// from it only in the 25th digit, so every pair shares a float64.
const ketWide = "9777777778877777577887713"

func ketDecimal(offset int64) parquet.Decimal128 {
	n, _ := new(big.Int).SetString(ketWide, 10)
	n.Add(n, big.NewInt(offset))
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

// ketData: 16 rows. The four DECIMAL values appear four times each; the float
// column carries the two zeros, the two computed infinities, a NaN, a NULL and
// two ordinary values, so both key classes ride the same scan and the same
// shuffle.
func ketData() []map[string]any {
	negZero := float64(0)
	negZero = -negZero
	kinds := []struct {
		kind string
		z    any
	}{
		{"val", 0.0}, {"val", negZero}, {"nan", nil}, {"null", nil},
		{"val", 1.0}, {"val", 2.0}, {"pinf", nil}, {"ninf", nil},
		{"val", negZero}, {"val", 0.0}, {"nan", nil}, {"val", 1.0},
		{"val", 2.0}, {"null", nil}, {"pinf", nil}, {"ninf", nil},
	}
	offsets := []int64{0, 1, 1000, 1001}
	out := make([]map[string]any, len(kinds))
	for i, k := range kinds {
		out[i] = map[string]any{
			"id": int64(i), "kind": k.kind, "z": k.z, "d": ketDecimal(offsets[i%4]),
		}
	}
	return out
}

func ketInt(t *testing.T, arm string, got any) int64 {
	t.Helper()
	switch v := got.(type) {
	case int64:
		return v
	case int32:
		return int64(v)
	case int:
		return int64(v)
	}
	t.Fatalf("%s: count came back as %#v (%T)", arm, got, got)
	return 0
}

func TestKeyEncodingTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)
	v := ketValueExpr

	// Every expectation below is PostgreSQL's answer over the same values
	// (verified live against postgres:17-alpine): the two zeros are ONE
	// value, the NaNs are ONE value equal to itself, and the four DECIMALs
	// are FOUR values however close their doubles are.
	cases := []struct {
		name string
		sql  string
		want int64
	}{
		{"decimal_groups", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM (SELECT d, COUNT(*) AS c FROM %s GROUP BY d) g", ketTable), 4},
		{"decimal_count_distinct", fmt.Sprintf("SELECT COUNT(DISTINCT d) AS n FROM %s", ketTable), 4},
		// 16 rows, four values, four rows each: 4 x 4 x 4.
		{"decimal_self_join", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM %s a JOIN %s b ON a.d = b.d", ketTable, ketTable), 64},
		// Float groups over the stored ±0.0 column: 0 (four rows), 1, 2, and
		// the NULLs. Four groups, not five.
		{"float_stored_groups", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM (SELECT z, COUNT(*) AS c FROM %s GROUP BY z) g", ketTable), 4},
		{"float_stored_zero_group", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM (SELECT z, COUNT(*) AS c FROM %s WHERE z = 0 GROUP BY z HAVING COUNT(*) = 4) g",
			ketTable), 1},
		// Computed column: NaN, 0, 1, 2, +Inf, -Inf, NULL = 7 groups.
		{"float_computed_groups", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM (SELECT (%s) AS f, COUNT(*) AS c FROM %s GROUP BY 1) g", v, ketTable), 7},
		{"float_self_join_zero", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM (SELECT id, z FROM %s WHERE z = 0) a JOIN (SELECT id, z FROM %s WHERE z = 0) b ON a.z = b.z",
			ketTable, ketTable), 16},
		// The predicate arms, distributed.
		{"float_gt_1e300", fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE (%s) > 1e300", ketTable, v), 4},
		{"float_self_eq", fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE (%s) = (%s)", ketTable, v, v), 14},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var single64, dag64 int64
			for _, arm := range []struct {
				name string
				dag  bool
			}{{"single", false}, {"dag", true}} {
				rows := dtpRun(t, ctx, single, coord, tc.sql, arm.dag)
				if len(rows) != 1 {
					t.Fatalf("%s: %d rows, want 1", arm.name, len(rows))
				}
				got := ketInt(t, arm.name, rows[0]["n"])
				if arm.dag {
					dag64 = got
				} else {
					single64 = got
				}
				if got != tc.want {
					t.Errorf("%s: %s\n  got %d, want %d (live PostgreSQL 17)", arm.name, tc.sql, got, tc.want)
				}
			}
			if single64 != dag64 {
				t.Errorf("the two paths disagree: single-process %d, stage DAG %d", single64, dag64)
			}
		})
	}
}
