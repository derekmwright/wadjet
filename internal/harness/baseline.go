package harness

import (
	"encoding/json"
	"fmt"
	"os"
)

// BaselineFile is the on-disk schema for the calibration table. The version
// field is incremented when incompatible changes are made.
type BaselineFile struct {
	Version           int                      `json:"version"`
	CapturedAt        string                   `json:"captured_at"`
	CapturedOn        string                   `json:"captured_on"`
	Queries           map[string]QueryBaseline `json:"queries"`
	ProjectionFactors map[string]Projection    `json:"projection_factors"`
}

// QueryBaseline holds the golden numbers and tolerances for one query.
type QueryBaseline struct {
	WallMsP50              int64   `json:"wall_ms_p50"`
	WallMsTolerancePct     float64 `json:"wall_ms_tolerance_pct"`
	PeakHeapMB             int64   `json:"peak_heap_mb"`
	PeakHeapTolerancePct   float64 `json:"peak_heap_tolerance_pct"`
	AllocCount             int64   `json:"alloc_count"`
	AllocCountTolerancePct float64 `json:"alloc_count_tolerance_pct"`
	SpillBytesWritten      int64   `json:"spill_bytes_written"`
	SpillTolerancePct      float64 `json:"spill_tolerance_pct"`
	RowCount               int64   `json:"row_count"`
	RowChecksum            string  `json:"row_checksum"`
	// ValueSig is the canonical per-column numeric-sum signature
	// (valuesig.go), compared with ValueSigRelTol relative tolerance —
	// unlike RowChecksum it is order-insensitive and float-wobble-proof,
	// so it can gate VALUES (the #278 / eager-§14.3 corruption class that
	// row counts cannot see). Empty = not gated.
	ValueSig string `json:"value_sig,omitempty"`
}

// Projection maps a local-mode metric to the equivalent golden-mode value
// via per-metric multipliers. local / multiplier = projected golden value.
type Projection struct {
	WallMsMultiplier     float64 `json:"wall_ms_multiplier"`
	HeapMultiplier       float64 `json:"heap_multiplier"`
	AllocCountMultiplier float64 `json:"alloc_count_multiplier"`
	SpillMultiplier      float64 `json:"spill_multiplier"`
}

// LoadBaseline reads and parses a baseline file.
func LoadBaseline(path string) (*BaselineFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var bf BaselineFile
	if err := json.Unmarshal(data, &bf); err != nil {
		return nil, fmt.Errorf("parsing baseline %s: %w", path, err)
	}
	if bf.Version != 1 {
		return nil, fmt.Errorf("unsupported baseline version %d (want 1)", bf.Version)
	}
	return &bf, nil
}

// Project converts a local-mode measurement into projected golden-mode
// values using the projection factors for the given slice.
func (bf *BaselineFile) Project(sliceKey string, local QueryMeasurement) (QueryMeasurement, error) {
	pf, ok := bf.ProjectionFactors[sliceKey]
	if !ok {
		return QueryMeasurement{}, fmt.Errorf("no projection factors for slice %q", sliceKey)
	}
	projected := local
	if pf.WallMsMultiplier > 0 {
		projected.WallMs = int64(float64(local.WallMs) / pf.WallMsMultiplier)
	}
	if pf.HeapMultiplier > 0 {
		projected.PeakHeapMB = int64(float64(local.PeakHeapMB) / pf.HeapMultiplier)
	}
	if pf.AllocCountMultiplier > 0 {
		projected.AllocCount = int64(float64(local.AllocCount) / pf.AllocCountMultiplier)
	}
	if pf.SpillMultiplier > 0 {
		projected.SpillBytes = int64(float64(local.SpillBytes) / pf.SpillMultiplier)
	}
	return projected, nil
}

// Compare returns one QueryDelta per metric for the given measurement.
// Status is "PASS" if drift is within tolerance, "REGRESS" otherwise.
// Row count and checksum mismatches always REGRESS regardless of tolerance.
func (bf *BaselineFile) Compare(m QueryMeasurement) []QueryDelta {
	qb, ok := bf.Queries[m.Query]
	if !ok {
		return nil
	}

	var out []QueryDelta
	check := func(metric string, baseline, observed float64, tolerancePct float64) {
		if baseline == 0 {
			return
		}
		drift := (observed - baseline) / baseline * 100
		status := "PASS"
		if drift > tolerancePct {
			status = "REGRESS"
		}
		out = append(out, QueryDelta{
			Query:        m.Query,
			Metric:       metric,
			Baseline:     baseline,
			Projected:    observed,
			DriftPct:     drift,
			TolerancePct: tolerancePct,
			Status:       status,
		})
	}

	check("wall_ms", float64(qb.WallMsP50), float64(m.WallMs), qb.WallMsTolerancePct)
	check("peak_heap_mb", float64(qb.PeakHeapMB), float64(m.PeakHeapMB), qb.PeakHeapTolerancePct)
	check("alloc_count", float64(qb.AllocCount), float64(m.AllocCount), qb.AllocCountTolerancePct)
	check("spill_bytes", float64(qb.SpillBytesWritten), float64(m.SpillBytes), qb.SpillTolerancePct)

	if qb.RowCount != 0 && qb.RowCount != m.RowCount {
		// Unlike wall_ms/peak_heap_mb/alloc_count/spill_bytes above, a
		// row_count mismatch always REGRESSes regardless of tolerance (see
		// the method doc) — TolerancePct is left at 0 to reflect that. But
		// DriftPct is still a real, useful percentage here (how far off
		// the row count is), so populate it instead of leaving it at its
		// zero value: printing "drift=0.0%" for e.g. a query that returned
		// 0 rows instead of 6 reads as "no drift" for what is actually a
		// 100% miss.
		baseline := float64(qb.RowCount)
		out = append(out, QueryDelta{
			Query:     m.Query,
			Metric:    "row_count",
			Baseline:  baseline,
			Projected: float64(m.RowCount),
			DriftPct:  (float64(m.RowCount) - baseline) / baseline * 100,
			Status:    "REGRESS",
		})
	}
	if qb.RowChecksum != "" && qb.RowChecksum != m.RowChecksum {
		out = append(out, QueryDelta{
			Query:  m.Query,
			Metric: "row_checksum",
			Status: "REGRESS",
		})
	}
	if qb.ValueSig != "" {
		if ok, detail := CompareValueSigs(qb.ValueSig, m.ValueSig, ValueSigRelTol); !ok {
			out = append(out, QueryDelta{
				Query:  m.Query,
				Metric: "value_signature",
				Status: "REGRESS",
				Detail: detail,
			})
		}
	}
	return out
}

// Save writes the baseline to disk as pretty-printed JSON.
func (bf *BaselineFile) Save(path string) error {
	data, err := json.MarshalIndent(bf, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}
