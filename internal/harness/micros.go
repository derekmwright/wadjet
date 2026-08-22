package harness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/jackc/pgx/v5"
)

// microTable holds a synthetic table's schema and generated rows.
type microTable struct {
	schema parquet.Schema
	rows   []map[string]any
}

// microSchemas defines the schemas for all synthetic micro tables.
var microSchemas = map[string]parquet.Schema{
	"micro_lineitem": {Columns: []parquet.Column{
		{Name: "l_orderkey", Type: parquet.TypeInt64},
		{Name: "l_partkey", Type: parquet.TypeInt64},
		{Name: "l_quantity", Type: parquet.TypeFloat64},
	}},
	"micro_orders": {Columns: []parquet.Column{
		{Name: "o_orderkey", Type: parquet.TypeInt64},
		{Name: "o_totalprice", Type: parquet.TypeFloat64},
	}},
	"micro_build": {Columns: []parquet.Column{
		{Name: "build_key", Type: parquet.TypeInt64},
		{Name: "build_val", Type: parquet.TypeInt64},
		{Name: "build_pad", Type: parquet.TypeString},
	}},
	"micro_probe": {Columns: []parquet.Column{
		{Name: "probe_key", Type: parquet.TypeInt64},
		{Name: "probe_val", Type: parquet.TypeInt64},
	}},
	"micro_agg": {Columns: []parquet.Column{
		{Name: "group_key", Type: parquet.TypeString},
		{Name: "value", Type: parquet.TypeInt64},
	}},
}

// generateMicroData creates deterministic synthetic data for all micro tables.
func generateMicroData() map[string]microTable {
	rng := rand.New(rand.NewSource(42))
	data := make(map[string]microTable, len(microSchemas))

	// micro_lineitem: 200K rows, l_orderkey in [1, 20000] (matches micro_orders)
	{
		rows := make([]map[string]any, 200_000)
		for i := range rows {
			rows[i] = map[string]any{
				"l_orderkey": int64(rng.Intn(20_000) + 1),
				"l_partkey":  int64(rng.Intn(100_000) + 1),
				"l_quantity": float64(rng.Intn(50) + 1),
			}
		}
		data["micro_lineitem"] = microTable{schema: microSchemas["micro_lineitem"], rows: rows}
	}

	// micro_orders: 20K rows, o_orderkey in [1, 20000] (unique keys)
	{
		rows := make([]map[string]any, 20_000)
		for i := range rows {
			rows[i] = map[string]any{
				"o_orderkey":   int64(i + 1),
				"o_totalprice": float64(rng.Intn(500_000)) / 100.0,
			}
		}
		data["micro_orders"] = microTable{schema: microSchemas["micro_orders"], rows: rows}
	}

	// micro_build: 500K rows, high-cardinality keys, padded strings to
	// inflate memory. The pad is sized so the build side's tracked memory
	// (internal/worker's "tracker_peak_mb" per-task log field) clears the
	// large slice's --shared-pool-budget (SliceConfigs[SliceLarge].
	// MemoryBudget, 64 MB) by a wide margin: a 64-byte pad measured only
	// ~28-48 MB of tracked memory in practice (run-to-run variance from GC
	// timing / batch boundaries), not reliably over the 64 MB budget, so
	// ExpectSpill's "did a task's tracked memory saturate its budget"
	// check (internal/harness/spillcheck.go) could not tell a real spill
	// apart from a task that simply never got close. 256 bytes puts the
	// raw string data alone at ~500K x 256B = 128 MB, comfortably 2x the
	// budget regardless of that variance.
	{
		const unit = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" // 64 bytes
		pad := strings.Repeat(unit, 4)                                                  // 256 bytes
		rows := make([]map[string]any, 500_000)
		for i := range rows {
			rows[i] = map[string]any{
				"build_key": int64(rng.Intn(50_000) + 1),
				"build_val": int64(rng.Int63()),
				"build_pad": fmt.Sprintf("%s-%d", pad, i),
			}
		}
		data["micro_build"] = microTable{schema: microSchemas["micro_build"], rows: rows}
	}

	// micro_probe: 50K rows, keys in [1, 50000] (overlap with micro_build)
	{
		rows := make([]map[string]any, 50_000)
		for i := range rows {
			rows[i] = map[string]any{
				"probe_key": int64(i + 1),
				"probe_val": int64(rng.Int63()),
			}
		}
		data["micro_probe"] = microTable{schema: microSchemas["micro_probe"], rows: rows}
	}

	// micro_agg: 200K rows, exactly 100K distinct group keys (2 rows per key).
	// Keys are assigned by i%100_000 so the test can assert an exact distinct
	// count. Random sampling like rng.Intn(100_000) would give ~86.5K distinct
	// keys (coupon-collector occupancy: 100k(1−e⁻²)) and break the assertion.
	{
		rows := make([]map[string]any, 200_000)
		for i := range rows {
			rows[i] = map[string]any{
				"group_key": fmt.Sprintf("grp_%06d", i%100_000),
				"value":     int64(rng.Intn(10_000)),
			}
		}
		// Shuffle row order so the keys aren't grouped contiguously — the
		// distributed aggregator should still produce the same distinct
		// count regardless of input order, and shuffling stresses the
		// hash-partition path more realistically.
		rng.Shuffle(len(rows), func(i, j int) { rows[i], rows[j] = rows[j], rows[i] })
		data["micro_agg"] = microTable{schema: microSchemas["micro_agg"], rows: rows}
	}

	return data
}

// runMicroQuery is the shared execution logic for all micro-benchmarks and
// skew-suite queries. It opens a pgx connection, runs the query, collects
// row count + checksum, and returns the measurement from the collector window.
func runMicroQuery(ctx context.Context, coordURL string, name string, sql string, timeout time.Duration, collector *MeasurementCollector) (QueryMeasurement, error) {
	collector.StartWindow(name)

	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	conn, err := pgx.Connect(queryCtx, coordURL)
	if err != nil {
		return collector.EndWindow(name), fmt.Errorf("pgx connect: %w", err)
	}
	defer conn.Close(context.Background())

	rows, err := conn.Query(queryCtx, sql)
	if err != nil {
		return collector.EndWindow(name), err
	}
	defer rows.Close()

	hash := sha256.New()
	var rowCount int64
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return collector.EndWindow(name), err
		}
		fmt.Fprintf(hash, "%v\n", vals)
		rowCount++
	}
	if err := rows.Err(); err != nil {
		return collector.EndWindow(name), err
	}

	m := collector.EndWindow(name)
	m.RowCount = rowCount
	m.RowChecksum = hex.EncodeToString(hash.Sum(nil))
	return m, nil
}

// RunMicroReverseBloom forces the reverseBloomBridge into its spill path
// by joining a large build side (micro_lineitem, 200K rows) against a small
// probe side (micro_orders, 20K rows), then asserts spill occurred.
func RunMicroReverseBloom(ctx context.Context, coordURL string, collector *MeasurementCollector) (QueryMeasurement, error) {
	sql := `SELECT o.o_orderkey, SUM(l.l_quantity)
FROM micro_lineitem l
JOIN micro_orders o ON l.l_orderkey = o.o_orderkey
GROUP BY o.o_orderkey`

	m, err := runMicroQuery(ctx, coordURL, "micro_reverse_bloom", sql, 2*time.Minute, collector)
	if err != nil {
		return m, err
	}
	if m.RowCount == 0 {
		return m, fmt.Errorf("micro_reverse_bloom: expected rows, got 0")
	}
	return m, nil
}

// RunMicroGraceHashJoin forces grace hash join partitioning by joining a
// memory-heavy build side (micro_build, 500K padded rows) against a smaller
// probe side (micro_probe, 50K rows), then asserts spill occurred.
func RunMicroGraceHashJoin(ctx context.Context, coordURL string, collector *MeasurementCollector) (QueryMeasurement, error) {
	sql := `SELECT b.build_key, b.build_val, p.probe_val
FROM micro_build b
JOIN micro_probe p ON b.build_key = p.probe_key`

	m, err := runMicroQuery(ctx, coordURL, "micro_grace_hash_join", sql, 2*time.Minute, collector)
	if err != nil {
		return m, err
	}
	if m.RowCount == 0 {
		return m, fmt.Errorf("micro_grace_hash_join: expected rows, got 0")
	}
	return m, nil
}

// RunMicroHashAggHighCard runs a high-cardinality GROUP BY (100K distinct keys)
// and asserts allocation discipline — no per-row allocation leak.
func RunMicroHashAggHighCard(ctx context.Context, coordURL string, collector *MeasurementCollector) (QueryMeasurement, error) {
	sql := `SELECT group_key, COUNT(*), SUM(value)
FROM micro_agg
GROUP BY group_key`

	m, err := runMicroQuery(ctx, coordURL, "micro_hash_agg_high_card", sql, 2*time.Minute, collector)
	if err != nil {
		return m, err
	}
	// micro_agg has 100K distinct group keys; verify exact count.
	if m.RowCount != 100_000 {
		return m, fmt.Errorf("micro_hash_agg_high_card: expected 100000 rows, got %d", m.RowCount)
	}
	return m, nil
}
