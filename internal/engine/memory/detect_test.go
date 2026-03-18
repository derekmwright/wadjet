package memory

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDetectBudgetNonNegative(t *testing.T) {
	budget := DetectBudget()
	if budget < 0 {
		t.Fatalf("DetectBudget() returned negative value: %d", budget)
	}
}

func TestReadLimitCgroupV2(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    int64
	}{
		{"numeric limit", "8589934592\n", 8589934592},
		{"max (unlimited)", "max\n", 0},
		{"empty file", "", 0},
		{"zero", "0\n", 0},
		{"negative", "-1\n", 0},
		{"garbage", "notanumber\n", 0},
		{"whitespace padded", "  4294967296  \n", 4294967296},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeTempFile(t, tt.content)
			got := readLimit(path, false)
			if got != tt.want {
				t.Errorf("readLimit(%q, v2) = %d, want %d", tt.content, got, tt.want)
			}
		})
	}
}

func TestReadLimitCgroupV1(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    int64
	}{
		{"numeric limit", "4294967296\n", 4294967296},
		{"kernel unlimited sentinel", "9223372036854771712\n", 0},
		{"above sentinel", "9223372036854775807\n", 0},
		{"empty file", "", 0},
		{"zero", "0\n", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeTempFile(t, tt.content)
			got := readLimit(path, true)
			if got != tt.want {
				t.Errorf("readLimit(%q, v1) = %d, want %d", tt.content, got, tt.want)
			}
		})
	}
}

func TestReadLimitMissingFile(t *testing.T) {
	got := readLimit("/nonexistent/path/memory.max", false)
	if got != 0 {
		t.Errorf("readLimit(missing file) = %d, want 0", got)
	}
}

func TestHeadroomCalculation(t *testing.T) {
	limit := int64(8_000_000_000)
	budget := int64(float64(limit) * headroomFactor)
	want := int64(6_000_000_000)
	if budget != want {
		t.Errorf("75%% of %d = %d, want %d", limit, budget, want)
	}
}

func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cgroup_mem")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}
