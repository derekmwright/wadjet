package tpch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// Baseline files: per-query expected row count plus a checksum over the
// query's full output. The checksum locks in row contents AND output
// ordering — any silent regression in values, types, or ORDER BY fails the
// corresponding subtest. SF0.01 runs by default in CI; SF1 is opt-in
// (~1.5M lineitem rows, slower datagen).
const (
	baselineFileSF001 = "baseline-sf001.json"
	baselineFileSF1   = "baseline-sf1.json"
)

type baselineEntry struct {
	RowCount int    `json:"row_count"`
	Checksum string `json:"checksum"`
}

// flakyChecksumQueries lists queries whose row content can flip across runs
// due to inherent floating-point border-row instability — the HAVING /
// threshold subquery sums billions of partial values whose ordering varies
// by ~1 ULP across parallel executions, and rows whose group sum lands
// within that ULP of the threshold flip in and out of the result.
//
// For these queries the test gates row count only (within a small tolerance)
// instead of comparing the full content checksum. They still catch big
// regressions (Q06's revenue=0 vs 1.19M would change the row count or push
// it well outside tolerance) without false-positive-flapping on float noise.
//
// Keyed by (scale-factor file, query number).
var flakyChecksumQueries = map[string]map[int]int{
	// SF0.01 had no flaky queries in 10/10 stability runs.
	baselineFileSF1: {
		11: 5, // SF1 Q11: HAVING SUM(ps_supplycost*ps_availqty) > 0.0001 * total
	},
}

// canonicalRow returns a stable string form of one query result row. Columns
// are visited in alphabetical order so map-iteration randomness can't change
// the hash. Float values are rendered with %g for consistency. The output is
// also used as the unit of the per-query hash, so any change to a single
// cell flips the row digest.
func canonicalRow(row map[string]any) string {
	keys := make([]string, 0, len(row))
	for k := range row {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(';')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(canonicalCell(row[k]))
	}
	return b.String()
}

// canonicalCell normalises a value for hashing. Floats render at 6 significant
// figures so non-deterministic float64 accumulation order across parallel
// workers (e.g., 4.9417625839e+08 vs 4.9417625840e+08, ~1e-9 relative noise)
// doesn't flip the checksum, while real semantic drift like Q06's revenue=0
// vs 1.19e6 still trips it. NaN/Inf normalise to literal tokens.
func canonicalCell(v any) string {
	if v == nil {
		return "<NULL>"
	}
	switch x := v.(type) {
	case float64:
		if math.IsNaN(x) {
			return "<NaN>"
		}
		if math.IsInf(x, 1) {
			return "<+Inf>"
		}
		if math.IsInf(x, -1) {
			return "<-Inf>"
		}
		return strconv.FormatFloat(x, 'g', 6, 64)
	case float32:
		return canonicalCell(float64(x))
	case string:
		return x
	default:
		return fmt.Sprintf("%v", v)
	}
}

// queryChecksum hashes the canonical row strings concatenated in output
// order. Output order matters because ORDER BY rows must remain stable.
func queryChecksum(rows []map[string]any) string {
	h := sha256.New()
	for _, r := range rows {
		h.Write([]byte(canonicalRow(r)))
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// loadBaseline reads the on-disk JSON, returning an empty map if absent so
// the regeneration path can populate it from scratch.
func loadBaseline(t *testing.T, file string) map[int]baselineEntry {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(".", file))
	if err != nil {
		if os.IsNotExist(err) {
			return map[int]baselineEntry{}
		}
		t.Fatalf("read baseline: %v", err)
	}
	// JSON keys are strings; decode then convert.
	var raw map[string]baselineEntry
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("parse baseline: %v", err)
	}
	out := make(map[int]baselineEntry, len(raw))
	for k, v := range raw {
		n, err := strconv.Atoi(k)
		if err != nil {
			t.Fatalf("parse baseline key %q: %v", k, err)
		}
		out[n] = v
	}
	return out
}

func writeBaseline(t *testing.T, file string, baseline map[int]baselineEntry) {
	t.Helper()
	raw := make(map[string]baselineEntry, len(baseline))
	for k, v := range baseline {
		raw[fmt.Sprintf("%d", k)] = v
	}
	data, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		t.Fatalf("marshal baseline: %v", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(filepath.Join(".", file), data, 0o644); err != nil {
		t.Fatalf("write baseline: %v", err)
	}
}

// runBaselineCheck shares the per-query verify/regenerate flow between
// scale factors. The two TestTPCHBaseline* tests are thin wrappers so they
// can wire scale-specific datagen and the env-var name that opts SF1 in.
func runBaselineCheck(t *testing.T, sf ScaleFactor, file string, regenerate bool) {
	db := setupTPCH(t, sf)
	ctx := context.Background()

	queryNums := make([]int, 0, len(TPCHQueries))
	for n := range TPCHQueries {
		queryNums = append(queryNums, n)
	}
	sort.Ints(queryNums)

	baseline := loadBaseline(t, file)
	updated := make(map[int]baselineEntry, len(baseline))

	for _, qNum := range queryNums {
		q := TPCHQueries[qNum]
		t.Run(fmt.Sprintf("Q%02d_%s", qNum, q.Name), func(t *testing.T) {
			res, err := db.Query(ctx, q.SQL)
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			cs := queryChecksum(res.Rows)
			got := baselineEntry{RowCount: len(res.Rows), Checksum: cs}

			if regenerate {
				updated[qNum] = got
				t.Logf("Q%02d (regen): rows=%d checksum=%s", qNum, got.RowCount, got.Checksum)
				return
			}

			want, ok := baseline[qNum]
			if !ok {
				t.Errorf("no baseline for Q%02d (got rows=%d checksum=%s); regenerate with the appropriate WADJET_REGENERATE_BASELINE_* env var",
					qNum, got.RowCount, got.Checksum)
				return
			}
			tolerance := flakyChecksumQueries[file][qNum]
			diff := got.RowCount - want.RowCount
			if diff < -tolerance || diff > tolerance {
				t.Errorf("Q%02d row count drift: got %d want %d (±%d)", qNum, got.RowCount, want.RowCount, tolerance)
			}
			if tolerance > 0 {
				// Flaky-by-design: row count within tolerance is the gate.
				return
			}
			if got.Checksum != want.Checksum {
				t.Errorf("Q%02d content checksum drift: got %s want %s\nfirst row: %v",
					qNum, got.Checksum, want.Checksum, firstRow(res.Rows))
			}
		})
	}

	if regenerate {
		writeBaseline(t, file, updated)
		t.Logf("wrote %s with %d entries", file, len(updated))
	}
}

// TestTPCHBaselineSF001 verifies each query's row count AND content checksum
// against the on-disk baseline at SF0.01. The audit found 7 queries that
// returned wrong values silently because the prior gate only checked row
// counts; this test catches both classes of regression.
//
// To regenerate the baseline (e.g. after an intentional output change):
//
//	WADJET_REGENERATE_BASELINE=1 go test -run TestTPCHBaselineSF001 ./benchmarks/tpch/
func TestTPCHBaselineSF001(t *testing.T) {
	regenerate := os.Getenv("WADJET_REGENERATE_BASELINE") == "1"
	runBaselineCheck(t, SF001, baselineFileSF001, regenerate)
}

// TestTPCHBaselineSF1 mirrors SF001 at scale 1 (~6M lineitem rows). Several
// SF0.01 bugs were data-shape-dependent (Q06's leftover-row-group trigger
// would not appear without specific row-group boundaries); SF1 has different
// boundaries and far more groups in HashAggregate, so it surfaces shape and
// scale issues that SF001 cannot.
//
// Opt-in via WADJET_TPCH_SF1=1 — datagen materialises ~6M rows in memory
// (~30 s wall) so it is too heavy to run on every CI invocation. Regenerate
// with WADJET_REGENERATE_BASELINE_SF1=1 alongside that flag.
func TestTPCHBaselineSF1(t *testing.T) {
	if os.Getenv("WADJET_TPCH_SF1") != "1" {
		t.Skip("set WADJET_TPCH_SF1=1 to enable SF1 baseline (heavy: ~6M lineitem rows, ~30 s datagen)")
	}
	regenerate := os.Getenv("WADJET_REGENERATE_BASELINE_SF1") == "1"
	runBaselineCheck(t, SF1, baselineFileSF1, regenerate)
}

func firstRow(rows []map[string]any) any {
	if len(rows) == 0 {
		return nil
	}
	return rows[0]
}
