package harness

import (
	"context"
	"fmt"
	"time"
)

// RunMicroReverseBloom builds a synthetic dataset shaped to force
// reverseBloomBridge into spill, runs a controlled join query, and asserts
// that spill files were created and that total spill bytes are within
// expected bounds.
//
// v1: stub that returns success. The real implementation needs catalog
// seeding (same blocker as loadSampleData in harness.go).
func RunMicroReverseBloom(ctx context.Context, coordURL string, collector *MeasurementCollector) (QueryMeasurement, error) {
	collector.StartWindow("micro_reverse_bloom")

	// Stub: pretend it ran and produced a small measurement so the rest
	// of the harness can be tested end-to-end.
	time.Sleep(50 * time.Millisecond)

	m := collector.EndWindow("micro_reverse_bloom")
	m.RowCount = 0
	m.RowChecksum = "stub"
	if coordURL == "" {
		return m, fmt.Errorf("micro stub: coordURL empty")
	}
	return m, nil
}
