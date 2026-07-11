package harness

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/citc-tech/wadjet/benchmarks/skew"
)

// Skew-suite integration (docs/design/skew-aware-shuffle.md Phase 3): the
// hot-key A/B fixture runs through the harness local mode so the flag-off /
// flag-on arms get the same cluster spawn, measurement, checksum, and hang
// machinery as everything else. Opt-in only — the fixture is ~2 GB of
// generated data, so it is seeded and run ONLY when --queries names a skew
// query. Typical A/B:
//
//	tpch-harness --mode=local --workers=3 --no-compare \
//	  --queries=skew_join_agg,skew_left_join                      # off arm
//	tpch-harness --mode=local --workers=3 --no-compare \
//	  --queries=skew_join_agg,skew_left_join \
//	  --serve-args=--skew-split                                   # on arm
//
// Mechanism marker: grep the preserved coordinator log for "skew split
// planned" (WADJET_HARNESS_KEEP=1 keeps the run dir on success).

// skewQueryTimeout bounds one skew-suite query. The flag-off arm's straggler
// task probes the entire ~1.6 GB hot group serially, so the micro suite's
// 2-minute bound is too tight; WADJET_HARNESS_QUERY_TIMEOUT overrides.
const skewQueryTimeout = 15 * time.Minute

// isSkewQuery reports whether name belongs to the skew suite.
func isSkewQuery(name string) bool {
	_, ok := skew.Queries[name]
	return ok
}

// anySkewQuery reports whether the resolved query list includes any
// skew-suite query, i.e. whether loadSampleData must seed the fixture.
func anySkewQuery(names []string) bool {
	for _, n := range names {
		if isSkewQuery(n) {
			return true
		}
	}
	return false
}

// skewFixtureConfig returns the local fixture config, honoring the
// WADJET_SKEW_EVENTS_ROWS / WADJET_SKEW_DIMS_ROWS overrides used for quick
// plumbing smoke runs (undersized fixtures don't engage the mechanism —
// production thresholds need the defaults).
func skewFixtureConfig() (skew.Config, error) {
	cfg := skew.DefaultLocalConfig()
	if v := os.Getenv("WADJET_SKEW_EVENTS_ROWS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return cfg, fmt.Errorf("WADJET_SKEW_EVENTS_ROWS: %w", err)
		}
		cfg.EventsRows = n
	}
	if v := os.Getenv("WADJET_SKEW_DIMS_ROWS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return cfg, fmt.Errorf("WADJET_SKEW_DIMS_ROWS: %w", err)
		}
		cfg.DimsRows = n
		if ks := int64(n) * 4 / 3; cfg.KeySpace <= int64(n) || cfg.KeySpace > ks {
			cfg.KeySpace = ks
		}
		if cfg.HotKey >= int64(n) {
			cfg.HotKey = int64(n) / 2
		}
	}
	return cfg, cfg.Validate()
}

// RunSkewQuery executes one skew-suite query by name and asserts its result
// shape (both queries return at least one row; row values are checksummed by
// the shared runner for cross-arm parity).
func RunSkewQuery(ctx context.Context, coordURL string, name string, collector *MeasurementCollector) (QueryMeasurement, error) {
	sql, ok := skew.Queries[name]
	if !ok {
		return collector.EndWindow(name), fmt.Errorf("unknown skew query %q", name)
	}
	timeout := skewQueryTimeout
	if override := os.Getenv("WADJET_HARNESS_QUERY_TIMEOUT"); override != "" {
		if d, err := time.ParseDuration(override); err == nil {
			timeout = d
		}
	}
	m, err := runMicroQuery(ctx, coordURL, name, sql, timeout, collector)
	if err != nil {
		return m, err
	}
	if m.RowCount == 0 {
		return m, fmt.Errorf("%s: expected rows, got 0", name)
	}
	return m, nil
}
