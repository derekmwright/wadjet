package partition

import (
	"testing"
	"time"
)

func TestNewStrategyAndPartitionPath(t *testing.T) {
	s := NewStrategy([]string{"year", "month", "day"})

	values := map[string]string{"year": "2026", "month": "03", "day": "15"}
	got := s.PartitionPath(values)
	want := "year=2026/month=03/day=15"
	if got != want {
		t.Errorf("PartitionPath() = %q, want %q", got, want)
	}
}

func TestPartitionPathKeyOrder(t *testing.T) {
	s := NewStrategy([]string{"day", "year"})
	values := map[string]string{"year": "2026", "day": "15"}
	got := s.PartitionPath(values)
	want := "day=15/year=2026"
	if got != want {
		t.Errorf("PartitionPath() = %q, want %q (key order not respected)", got, want)
	}
}

func TestPartitionPathEscapesValues(t *testing.T) {
	s := NewStrategy([]string{"region"})
	values := map[string]string{"region": "us east/1"}
	got := s.PartitionPath(values)
	want := "region=us%20east%2F1"
	if got != want {
		t.Errorf("PartitionPath() = %q, want %q", got, want)
	}
}

func TestFilePath(t *testing.T) {
	s := NewStrategy([]string{"year", "month"})
	values := map[string]string{"year": "2026", "month": "03"}
	got := s.FilePath("tables/events", values, "chunk_0001_abc")
	want := "tables/events/year=2026/month=03/chunk_0001_abc.parquet"
	if got != want {
		t.Errorf("FilePath() = %q, want %q", got, want)
	}
}

func TestTablePrefix(t *testing.T) {
	got := TablePrefix("events")
	want := "tables/events"
	if got != want {
		t.Errorf("TablePrefix() = %q, want %q", got, want)
	}
}

func TestParsePartitionPathRoundTrip(t *testing.T) {
	s := NewStrategy([]string{"year", "month", "day"})
	original := map[string]string{"year": "2026", "month": "03", "day": "15"}
	path := s.PartitionPath(original)

	parsed, err := ParsePartitionPath(path)
	if err != nil {
		t.Fatalf("ParsePartitionPath() error: %v", err)
	}
	for k, want := range original {
		if got := parsed[k]; got != want {
			t.Errorf("parsed[%q] = %q, want %q", k, got, want)
		}
	}
}

func TestParsePartitionPathWithEscapedValues(t *testing.T) {
	s := NewStrategy([]string{"region"})
	values := map[string]string{"region": "us east/1"}
	path := s.PartitionPath(values)

	parsed, err := ParsePartitionPath(path)
	if err != nil {
		t.Fatalf("ParsePartitionPath() error: %v", err)
	}
	if got := parsed["region"]; got != "us east/1" {
		t.Errorf("parsed[region] = %q, want %q", got, "us east/1")
	}
}

func TestParsePartitionPathSkipsNonPartitionSegments(t *testing.T) {
	parsed, err := ParsePartitionPath("tables/events/year=2026/chunk.parquet")
	if err != nil {
		t.Fatalf("ParsePartitionPath() error: %v", err)
	}
	if len(parsed) != 1 {
		t.Fatalf("expected 1 key, got %d", len(parsed))
	}
	if got := parsed["year"]; got != "2026" {
		t.Errorf("parsed[year] = %q, want %q", got, "2026")
	}
}

func TestTimePartitionValues(t *testing.T) {
	ts := time.Date(2026, 3, 15, 9, 30, 0, 0, time.UTC)

	values := TimePartitionValues(ts, []string{"year", "month", "day", "hour"})

	expected := map[string]string{
		"year":  "2026",
		"month": "03",
		"day":   "15",
		"hour":  "09",
	}
	for k, want := range expected {
		if got := values[k]; got != want {
			t.Errorf("TimePartitionValues[%q] = %q, want %q", k, got, want)
		}
	}
}

func TestTimePartitionValuesSubset(t *testing.T) {
	ts := time.Date(2026, 3, 15, 9, 0, 0, 0, time.UTC)
	values := TimePartitionValues(ts, []string{"year", "month"})
	if len(values) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(values))
	}
	if _, ok := values["day"]; ok {
		t.Error("unexpected key 'day' in result")
	}
}

func TestMatchesFilterAllMatch(t *testing.T) {
	part := map[string]string{"year": "2026", "month": "03", "day": "15"}
	filter := map[string]string{"year": "2026", "month": "03"}
	if !MatchesFilter(part, filter) {
		t.Error("MatchesFilter() = false, want true")
	}
}

func TestMatchesFilterMismatch(t *testing.T) {
	part := map[string]string{"year": "2026", "month": "03"}
	filter := map[string]string{"year": "2025"}
	if MatchesFilter(part, filter) {
		t.Error("MatchesFilter() = true, want false")
	}
}

func TestMatchesFilterEmptyFilter(t *testing.T) {
	part := map[string]string{"year": "2026"}
	if !MatchesFilter(part, map[string]string{}) {
		t.Error("MatchesFilter() = false for empty filter, want true")
	}
}

func TestMatchesFilterMissingKey(t *testing.T) {
	part := map[string]string{"year": "2026"}
	filter := map[string]string{"month": "03"}
	if MatchesFilter(part, filter) {
		t.Error("MatchesFilter() = true for missing key, want false")
	}
}

func TestPrunePartitions(t *testing.T) {
	partitions := []PartitionInfo{
		{Path: "year=2026/month=01", Values: map[string]string{"year": "2026", "month": "01"}},
		{Path: "year=2026/month=03", Values: map[string]string{"year": "2026", "month": "03"}},
		{Path: "year=2025/month=03", Values: map[string]string{"year": "2025", "month": "03"}},
	}

	result := PrunePartitions(partitions, map[string]string{"year": "2026"})
	if len(result) != 2 {
		t.Fatalf("PrunePartitions() returned %d partitions, want 2", len(result))
	}
	if result[0].Path != "year=2026/month=01" || result[1].Path != "year=2026/month=03" {
		t.Errorf("PrunePartitions() returned wrong partitions: %+v", result)
	}
}

func TestPrunePartitionsEmptyFilter(t *testing.T) {
	partitions := []PartitionInfo{
		{Path: "a", Values: map[string]string{"year": "2026"}},
		{Path: "b", Values: map[string]string{"year": "2025"}},
	}
	result := PrunePartitions(partitions, nil)
	if len(result) != 2 {
		t.Fatalf("PrunePartitions(nil filter) returned %d, want 2", len(result))
	}
}

func TestPrunePartitionsNoMatch(t *testing.T) {
	partitions := []PartitionInfo{
		{Path: "year=2026", Values: map[string]string{"year": "2026"}},
	}
	result := PrunePartitions(partitions, map[string]string{"year": "2020"})
	if len(result) != 0 {
		t.Fatalf("PrunePartitions() returned %d, want 0", len(result))
	}
}

func TestSortPartitions(t *testing.T) {
	partitions := []PartitionInfo{
		{Path: "year=2026/month=03"},
		{Path: "year=2025/month=01"},
		{Path: "year=2026/month=01"},
	}
	SortPartitions(partitions)

	expected := []string{"year=2025/month=01", "year=2026/month=01", "year=2026/month=03"}
	for i, want := range expected {
		if partitions[i].Path != want {
			t.Errorf("SortPartitions()[%d].Path = %q, want %q", i, partitions[i].Path, want)
		}
	}
}

func TestFilePathNoPartitionKeys(t *testing.T) {
	s := NewStrategy(nil)
	got := s.FilePath("tables/events", nil, "chunk_0001_abc")
	want := "tables/events/chunk_0001_abc.parquet"
	if got != want {
		t.Errorf("FilePath() with no partition keys = %q, want %q", got, want)
	}
}

func TestFilePathNoPartitionKeysNoDoubleSlash(t *testing.T) {
	s := NewStrategy([]string{})
	got := s.FilePath("tables/events", map[string]string{}, "chunk_0001_abc")
	want := "tables/events/chunk_0001_abc.parquet"
	if got != want {
		t.Errorf("FilePath() with empty partition keys = %q, want %q", got, want)
	}
}

func TestSortPartitionsAlreadySorted(t *testing.T) {
	partitions := []PartitionInfo{
		{Path: "a"},
		{Path: "b"},
		{Path: "c"},
	}
	SortPartitions(partitions)
	if partitions[0].Path != "a" || partitions[1].Path != "b" || partitions[2].Path != "c" {
		t.Error("SortPartitions() changed already-sorted list")
	}
}
