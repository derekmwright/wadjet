package coordinator

import (
	"reflect"
	"testing"
)

func TestSplitFilesEvenly(t *testing.T) {
	cases := []struct {
		name    string
		files   []string
		n       int
		wantLen int   // expected number of non-nil slices
		wantTotal int // expected total files across all slices
	}{
		{
			name:      "even split 6 into 3",
			files:     []string{"a", "b", "c", "d", "e", "f"},
			n:         3,
			wantLen:   3,
			wantTotal: 6,
		},
		{
			name:      "uneven split 7 into 3",
			files:     []string{"a", "b", "c", "d", "e", "f", "g"},
			n:         3,
			wantLen:   3,
			wantTotal: 7,
		},
		{
			name:      "more workers than files: 2 files 3 workers",
			files:     []string{"a", "b"},
			n:         3,
			wantLen:   2, // splitFilesEvenly caps n at len(files)
			wantTotal: 2,
		},
		{
			name:      "exact 1 file 1 worker",
			files:     []string{"only"},
			n:         1,
			wantLen:   1,
			wantTotal: 1,
		},
		{
			name:      "12 files 4 workers",
			files:     makeFiles(12),
			n:         4,
			wantLen:   4,
			wantTotal: 12,
		},
		{
			name:      "1 file 4 workers",
			files:     []string{"solo"},
			n:         4,
			wantLen:   1,
			wantTotal: 1,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := splitFilesEvenly(tc.files, tc.n)
			if len(got) != tc.wantLen {
				t.Errorf("got %d slices, want %d", len(got), tc.wantLen)
			}
			total := 0
			for _, s := range got {
				total += len(s)
			}
			if total != tc.wantTotal {
				t.Errorf("got %d total files, want %d", total, tc.wantTotal)
			}
			// Verify no duplicates and all original files are present.
			seen := make(map[string]bool)
			for _, s := range got {
				for _, f := range s {
					if seen[f] {
						t.Errorf("duplicate file %q", f)
					}
					seen[f] = true
				}
			}
			for _, f := range tc.files[:tc.wantTotal] {
				if !seen[f] {
					t.Errorf("file %q missing from output", f)
				}
			}
		})
	}
}

func TestSplitFilesEvenlyEmpty(t *testing.T) {
	if got := splitFilesEvenly(nil, 4); got != nil {
		t.Errorf("splitFilesEvenly(nil, 4) = %v, want nil", got)
	}
	if got := splitFilesEvenly([]string{}, 4); got != nil {
		t.Errorf("splitFilesEvenly([], 4) = %v, want nil", got)
	}
	if got := splitFilesEvenly([]string{"a"}, 0); got != nil {
		t.Errorf("splitFilesEvenly([a], 0) = %v, want nil", got)
	}
	if got := splitFilesEvenly([]string{"a"}, -1); got != nil {
		t.Errorf("splitFilesEvenly([a], -1) = %v, want nil", got)
	}
}

func TestAssignPartitionsToWorker(t *testing.T) {
	cases := []struct {
		name        string
		w           int
		workerCount int
		numParts    int
		want        []int
	}{
		{
			name:        "3 workers 12 partitions — worker 0",
			w:           0,
			workerCount: 3,
			numParts:    12,
			want:        []int{0, 1, 2, 3},
		},
		{
			name:        "3 workers 12 partitions — worker 1",
			w:           1,
			workerCount: 3,
			numParts:    12,
			want:        []int{4, 5, 6, 7},
		},
		{
			name:        "3 workers 12 partitions — worker 2",
			w:           2,
			workerCount: 3,
			numParts:    12,
			want:        []int{8, 9, 10, 11},
		},
		{
			name:        "4 workers 16 partitions — worker 3",
			w:           3,
			workerCount: 4,
			numParts:    16,
			want:        []int{12, 13, 14, 15},
		},
		{
			name:        "1 worker 4 partitions",
			w:           0,
			workerCount: 1,
			numParts:    4,
			want:        []int{0, 1, 2, 3},
		},
		{
			name:        "out-of-range worker",
			w:           5,
			workerCount: 3,
			numParts:    12,
			want:        nil,
		},
		{
			name:        "zero workers",
			w:           0,
			workerCount: 0,
			numParts:    12,
			want:        nil,
		},
		{
			name:        "zero partitions",
			w:           0,
			workerCount: 3,
			numParts:    0,
			want:        nil,
		},
		{
			name:        "negative worker index",
			w:           -1,
			workerCount: 3,
			numParts:    12,
			want:        nil,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := assignPartitionsToWorker(tc.w, tc.workerCount, tc.numParts)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("assignPartitionsToWorker(%d, %d, %d) = %v, want %v",
					tc.w, tc.workerCount, tc.numParts, got, tc.want)
			}
		})
	}
}

// TestAssignPartitionsToWorkerCoverage verifies that, when numParts is evenly
// divisible by workerCount, every partition is assigned to exactly one worker.
func TestAssignPartitionsToWorkerCoverage(t *testing.T) {
	cases := []struct {
		workerCount int
		numParts    int
	}{
		{3, 12},
		{4, 16},
		{1, 4},
		{2, 8},
		{5, 20},
	}
	for _, tc := range cases {
		assigned := make(map[int]int) // partition → worker
		for w := 0; w < tc.workerCount; w++ {
			parts := assignPartitionsToWorker(w, tc.workerCount, tc.numParts)
			for _, p := range parts {
				if prev, ok := assigned[p]; ok {
					t.Errorf("workers=%d parts=%d: partition %d assigned to both worker %d and %d",
						tc.workerCount, tc.numParts, p, prev, w)
				}
				assigned[p] = w
			}
		}
		if len(assigned) != tc.numParts {
			t.Errorf("workers=%d parts=%d: only %d/%d partitions assigned",
				tc.workerCount, tc.numParts, len(assigned), tc.numParts)
		}
	}
}

// TestParsePartitionFromPath tests the path parsing helper.
func TestParsePartitionFromPath(t *testing.T) {
	cases := []struct {
		path    string
		want    int
		wantErr bool
	}{
		{"queries/qid/shuffle/build/partition=0000/task01.wshf", 0, false},
		{"queries/qid/shuffle/build/partition=0007/abc123.wshf", 7, false},
		{"queries/qid/shuffle/probe/partition=0042/xyz.wshf", 42, false},
		{"queries/qid/shuffle/probe/partition=1023/xyz.wshf", 1023, false},
		{"no-partition-segment/file.wshf", 0, true},
		{"queries/qid/partition=/file.wshf", 0, true}, // empty number
		{"", 0, true},
		{"shuffle/q1/build/partition=7/abc.wshf", 7, false},
		{"shuffle/q1/build/partition=abc/file.wshf", 0, true},
	}
	for _, tc := range cases {
		got, err := parsePartitionFromPath(tc.path)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parsePartitionFromPath(%q) = %d, nil; want error", tc.path, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parsePartitionFromPath(%q) error: %v", tc.path, err)
			continue
		}
		if got != tc.want {
			t.Errorf("parsePartitionFromPath(%q) = %d, want %d", tc.path, got, tc.want)
		}
	}
}

// makeFiles creates a slice of n file path strings for testing.
func makeFiles(n int) []string {
	files := make([]string, n)
	for i := range files {
		files[i] = "file" + string(rune('a'+i%26)) + string(rune('0'+i/26))
	}
	return files
}
