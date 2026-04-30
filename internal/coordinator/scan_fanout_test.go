package coordinator

import "testing"

// TestScanShardCountForSingleFile covers the row-group sharding decision for
// single-file scans. The threshold is exposed as a var so we lower it here
// to deterministic small bytes; the production default is 64 MB.
func TestScanShardCountForSingleFile(t *testing.T) {
	orig := singleFileShardThresholdBytes
	t.Cleanup(func() { singleFileShardThresholdBytes = orig })
	singleFileShardThresholdBytes = 1024 * 1024 // 1 MB for testing

	tests := []struct {
		name        string
		workerCount int
		capacity    int
		fileBytes   int64
		want        int
	}{
		// Below threshold: no sharding even with many workers.
		{name: "below_threshold_small_dim", workerCount: 6, capacity: 12, fileBytes: 500_000, want: 1},

		// Above threshold, multi-worker: shard to capacity.
		{name: "sf10_partsupp_compacted", workerCount: 3, capacity: 6, fileBytes: 700_000_000, want: 6},

		// Above threshold but single worker: no parallelism gain → stay 1.
		{name: "single_worker_no_gain", workerCount: 1, capacity: 1, fileBytes: 100_000_000, want: 1},

		// Above threshold, capacity unreported: fall back to workerCount.
		{name: "capacity_unreported", workerCount: 3, capacity: 0, fileBytes: 100_000_000, want: 3},

		// Capacity below workerCount: prefer workerCount.
		{name: "capacity_below_workers", workerCount: 4, capacity: 2, fileBytes: 100_000_000, want: 4},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := scanShardCountForSingleFile(tc.workerCount, tc.capacity, tc.fileBytes)
			if got != tc.want {
				t.Errorf("scanShardCountForSingleFile(%d, %d, %d) = %d, want %d",
					tc.workerCount, tc.capacity, tc.fileBytes, got, tc.want)
			}
		})
	}
}

func TestScanFanOutTaskCount(t *testing.T) {
	tests := []struct {
		name        string
		workerCount int
		capacity    int
		fileCount   int
		want        int
	}{
		// Capacity > workerCount: prefer capacity (auto-tuned concurrency).
		// Q04 SF10 case: 3 workers × MaxConcurrent=2 = 6 capacity, 600 files.
		{name: "sf10_q04_lineitem", workerCount: 3, capacity: 6, fileCount: 600, want: 6},

		// Capacity unreported (first-query startup): fall back to workerCount.
		{name: "capacity_unreported", workerCount: 3, capacity: 0, fileCount: 600, want: 3},

		// Capacity reported lower than workerCount: stick with workerCount
		// (workers downscaled but we still want at least one task per worker).
		{name: "capacity_below_workers", workerCount: 4, capacity: 2, fileCount: 600, want: 4},

		// Few files: never produce more tasks than files.
		{name: "more_capacity_than_files", workerCount: 3, capacity: 6, fileCount: 4, want: 4},

		// Single file: one task.
		{name: "single_file", workerCount: 3, capacity: 6, fileCount: 1, want: 1},

		// Zero workers (edge case): floor at 1.
		{name: "zero_workers_zero_capacity", workerCount: 0, capacity: 0, fileCount: 5, want: 1},

		// Equal capacity and workerCount: deterministic.
		{name: "equal", workerCount: 3, capacity: 3, fileCount: 12, want: 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := scanFanOutTaskCount(tc.workerCount, tc.capacity, tc.fileCount)
			if got != tc.want {
				t.Errorf("scanFanOutTaskCount(%d, %d, %d) = %d, want %d",
					tc.workerCount, tc.capacity, tc.fileCount, got, tc.want)
			}
		})
	}
}
