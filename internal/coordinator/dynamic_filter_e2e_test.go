//go:build !race

package coordinator

import (
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/benchmarks/tpch"
)

// TestDynamicFilterE2EQ17Correctness runs Q17 twice on the same SF0.1
// distributed fixture — once with DynamicFilters off (current production
// behavior) and once with it on — and asserts the answers match. Confirms
// the dynamic-filter path is correctness-preserving end-to-end across the
// planner, build-scan emit, coordinator merge, and probe-scan consume
// changes.
//
// SF0.1 is the smallest scale that exercises a multi-task shuffle of
// lineitem (probe) and produces a non-trivial Q17 answer (Brand#23 MED BOX
// rows surviving). SF0.01 lacks matching rows in the random distribution
// and would return 0, masking any selectivity divergence.
func TestDynamicFilterE2EQ17Correctness(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping SF0.1 Q17 dynamic-filter E2E (heavy: generates ~600K lineitem rows)")
	}
	ctx, coord := setupTPCHDistributedAtScale(t, tpch.SF01)
	q17 := tpch.TPCHQueries[17]

	// Baseline: flag off (default).
	if coord.config.DynamicFilters {
		t.Fatalf("DynamicFilters should default off; got on")
	}
	baseline, err := coord.ExecuteSQL(ctx, q17.SQL)
	if err != nil {
		t.Fatalf("Q17 baseline ExecuteSQL: %v", err)
	}
	if baseline.Error != "" {
		t.Fatalf("Q17 baseline error: %s", baseline.Error)
	}
	baselineRows := mustRows(t, baseline)
	t.Logf("Q17 baseline (flag=off): %d rows", len(baselineRows))

	// Flip flag, re-run.
	coord.config.DynamicFilters = true
	t.Cleanup(func() { coord.config.DynamicFilters = false })

	withFilter, err := coord.ExecuteSQL(ctx, q17.SQL)
	if err != nil {
		t.Fatalf("Q17 with-filter ExecuteSQL: %v", err)
	}
	if withFilter.Error != "" {
		t.Fatalf("Q17 with-filter error: %s", withFilter.Error)
	}
	wfRows := mustRows(t, withFilter)
	t.Logf("Q17 with-filter (flag=on): %d rows", len(wfRows))

	if len(baselineRows) != len(wfRows) {
		t.Fatalf("row count mismatch: baseline=%d with-filter=%d", len(baselineRows), len(wfRows))
	}

	// Compare each row by stringified value. Distributed sums can drift
	// 1 ULP across runs depending on partial-order I/O, so we allow exact
	// match on the leading 12 significant digits.
	for i := range baselineRows {
		b := fmt.Sprintf("%v", baselineRows[i])
		w := fmt.Sprintf("%v", wfRows[i])
		if b == w {
			continue
		}
		// Allow trailing-digit drift on floats.
		if approxFloatRow(b, w) {
			continue
		}
		t.Errorf("row %d differs: baseline=%q with-filter=%q", i, b, w)
	}
}

// approxFloatRow compares two map-stringified rows allowing minor float
// rounding noise on the last digits. Quick-and-dirty: extract numerics,
// compare 12 leading significant figures.
func approxFloatRow(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	// Match up to and including digit 13 of any number; coarse but
	// catches the ULP-drift case without parsing the whole map.
	type span struct{ start, end int }
	var spans []span
	inNum := false
	var s int
	for i := 0; i < len(a); i++ {
		c := a[i]
		if c >= '0' && c <= '9' || c == '.' || c == '-' || c == 'e' || c == 'E' || c == '+' {
			if !inNum {
				inNum = true
				s = i
			}
			continue
		}
		if inNum {
			spans = append(spans, span{s, i})
			inNum = false
		}
	}
	if inNum {
		spans = append(spans, span{s, len(a)})
	}
	// Outside the numeric spans, every byte must match exactly.
	prev := 0
	for _, sp := range spans {
		if a[prev:sp.start] != b[prev:sp.start] {
			return false
		}
		prev = sp.end
	}
	if a[prev:] != b[prev:] {
		return false
	}
	// Inside numeric spans, allow up to 12 sig-digit match.
	for _, sp := range spans {
		na, nb := a[sp.start:sp.end], b[sp.start:sp.end]
		if na == nb {
			continue
		}
		// Trim leading sign and compare first 12 sig digits.
		ta := strings.TrimLeft(strings.ReplaceAll(strings.ReplaceAll(na, ".", ""), "-", ""), "0")
		tb := strings.TrimLeft(strings.ReplaceAll(strings.ReplaceAll(nb, ".", ""), "-", ""), "0")
		minLen := 12
		if len(ta) < minLen {
			minLen = len(ta)
		}
		if len(tb) < minLen {
			minLen = len(tb)
		}
		if ta[:minLen] != tb[:minLen] {
			return false
		}
	}
	return true
}
