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

// baselineFile is the on-disk artifact: per-query expected row count plus a
// checksum over the query's full output. The checksum locks in row contents
// AND output ordering — any silent regression in values, types, or ORDER BY
// fails the corresponding subtest.
const baselineFile = "baseline-sf001.json"

type baselineEntry struct {
	RowCount int    `json:"row_count"`
	Checksum string `json:"checksum"`
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
func loadBaseline(t *testing.T) map[int]baselineEntry {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(".", baselineFile))
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

func writeBaseline(t *testing.T, baseline map[int]baselineEntry) {
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
	if err := os.WriteFile(filepath.Join(".", baselineFile), data, 0o644); err != nil {
		t.Fatalf("write baseline: %v", err)
	}
}

// TestTPCHBaselineSF001 verifies each query's row count AND content checksum
// against the on-disk baseline. The audit found 7 queries that returned
// wrong values silently because the prior gate only checked row counts;
// this test catches both classes of regression.
//
// To regenerate the baseline (e.g. after an intentional output change):
//
//	WADJET_REGENERATE_BASELINE=1 go test -run TestTPCHBaselineSF001 ./benchmarks/tpch/
func TestTPCHBaselineSF001(t *testing.T) {
	regenerate := os.Getenv("WADJET_REGENERATE_BASELINE") == "1"

	db := setupTPCH(t, SF001)
	ctx := context.Background()

	queryNums := make([]int, 0, len(TPCHQueries))
	for n := range TPCHQueries {
		queryNums = append(queryNums, n)
	}
	sort.Ints(queryNums)

	baseline := loadBaseline(t)
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
				t.Errorf("no baseline for Q%02d (got rows=%d checksum=%s); regenerate with WADJET_REGENERATE_BASELINE=1",
					qNum, got.RowCount, got.Checksum)
				return
			}
			if got.RowCount != want.RowCount {
				t.Errorf("Q%02d row count drift: got %d want %d", qNum, got.RowCount, want.RowCount)
			}
			if got.Checksum != want.Checksum {
				t.Errorf("Q%02d content checksum drift: got %s want %s\nfirst row: %v",
					qNum, got.Checksum, want.Checksum, firstRow(res.Rows))
			}
		})
	}

	if regenerate {
		writeBaseline(t, updated)
		t.Logf("wrote %s with %d entries", baselineFile, len(updated))
	}
}

func firstRow(rows []map[string]any) any {
	if len(rows) == 0 {
		return nil
	}
	return rows[0]
}
