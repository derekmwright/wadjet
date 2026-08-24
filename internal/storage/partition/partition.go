// Package partition implements Hive-style partitioning for table data.
package partition

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"
)

// Strategy determines how rows are partitioned.
type Strategy struct {
	Keys []string // partition key columns, in order
}

// NewStrategy creates a partition strategy with the given keys.
func NewStrategy(keys []string) *Strategy {
	return &Strategy{Keys: keys}
}

// PartitionPath generates a Hive-style partition path from partition values.
// Example: year=2026/month=03/day=15
func (s *Strategy) PartitionPath(values map[string]string) string {
	parts := make([]string, 0, len(s.Keys))
	for _, key := range s.Keys {
		val := values[key]
		parts = append(parts, fmt.Sprintf("%s=%s", key, url.PathEscape(val)))
	}
	return strings.Join(parts, "/")
}

// FilePath generates a full file path for a chunk within a partition.
// Example: tables/events/year=2026/month=03/day=15/chunk_<uuidv7>.parquet
// (the caller — storage/ingest — builds chunkID; see #494 for why it must
// carry a full 128-bit identifier, not a truncated one).
func (s *Strategy) FilePath(tablePrefix string, values map[string]string, chunkID string) string {
	partPath := s.PartitionPath(values)
	if partPath == "" {
		return fmt.Sprintf("%s/%s.parquet", tablePrefix, chunkID)
	}
	return fmt.Sprintf("%s/%s/%s.parquet", tablePrefix, partPath, chunkID)
}

// TablePrefix returns the base path for a table's data files.
func TablePrefix(tableName string) string {
	return "tables/" + tableName
}

// ParsePartitionPath extracts partition key-value pairs from a Hive-style path.
func ParsePartitionPath(path string) (map[string]string, error) {
	values := make(map[string]string)
	parts := strings.Split(path, "/")
	for _, part := range parts {
		if !strings.Contains(part, "=") {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}
		val, err := url.PathUnescape(kv[1])
		if err != nil {
			return nil, fmt.Errorf("unescaping partition value %q: %w", kv[1], err)
		}
		values[kv[0]] = val
	}
	return values, nil
}

// TimePartitionValues generates partition values from a timestamp using common
// time partition keys (year, month, day, hour).
func TimePartitionValues(t time.Time, keys []string) map[string]string {
	values := make(map[string]string, len(keys))
	for _, key := range keys {
		switch key {
		case "year":
			values[key] = fmt.Sprintf("%04d", t.Year())
		case "month":
			values[key] = fmt.Sprintf("%02d", int(t.Month()))
		case "day":
			values[key] = fmt.Sprintf("%02d", t.Day())
		case "hour":
			values[key] = fmt.Sprintf("%02d", t.Hour())
		}
	}
	return values
}

// MatchesFilter returns true if the partition values match all of the given filter values.
func MatchesFilter(partValues map[string]string, filter map[string]string) bool {
	for k, v := range filter {
		if partValues[k] != v {
			return false
		}
	}
	return true
}

// PrunePartitions returns only the partitions that match the filter.
func PrunePartitions(partitions []PartitionInfo, filter map[string]string) []PartitionInfo {
	if len(filter) == 0 {
		return partitions
	}
	var result []PartitionInfo
	for _, p := range partitions {
		if MatchesFilter(p.Values, filter) {
			result = append(result, p)
		}
	}
	return result
}

// PartitionInfo holds information about a single partition.
type PartitionInfo struct {
	Path   string
	Values map[string]string
	Files  []string
}

// SortPartitions sorts partition infos by path for deterministic ordering.
func SortPartitions(partitions []PartitionInfo) {
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i].Path < partitions[j].Path
	})
}
