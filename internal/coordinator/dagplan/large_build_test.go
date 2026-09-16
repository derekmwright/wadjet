// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/physical"
)

func TestLargeBuildScans(t *testing.T) {
	const gb = 1024 * 1024 * 1024
	stages := []physical.Stage{
		{ID: "s1", Type: "scan", ScanAlias: "lineitem", EstimatedBytes: 60 * gb, ScanFiles: []string{"l1.parquet"}},
		{ID: "s2", Type: "scan", ScanAlias: "orders", EstimatedBytes: 15 * gb, ScanFiles: []string{"o1.parquet"}},
		{ID: "s3", Type: "scan", ScanAlias: "partsupp", EstimatedBytes: 8 * gb, ScanFiles: []string{"ps1.parquet"}},
		{ID: "s4", Type: "scan", ScanAlias: "nation", EstimatedBytes: 1024, ScanFiles: []string{"n1.parquet"}},
		{ID: "s5", Type: "hash_join", ScanAlias: ""},
	}
	threshold := int64(2 * gb)

	t.Run("excludes probe alias and small tables", func(t *testing.T) {
		large := LargeBuildScans(stages, "lineitem", threshold)
		// should include orders and partsupp, but not lineitem (probe) or nation (small) or hash_join
		if len(large) != 2 {
			t.Fatalf("want 2 large build scans, got %d: %v", len(large), large)
		}
		aliases := map[string]bool{}
		for _, s := range large {
			aliases[s.ScanAlias] = true
		}
		if !aliases["orders"] || !aliases["partsupp"] {
			t.Errorf("expected orders and partsupp, got %v", aliases)
		}
		if aliases["lineitem"] {
			t.Errorf("probe alias lineitem should be excluded")
		}
	})

	t.Run("no large scans below threshold", func(t *testing.T) {
		large := LargeBuildScans(stages, "lineitem", 100*gb)
		if len(large) != 0 {
			t.Fatalf("want 0 large build scans at 100GB threshold, got %d", len(large))
		}
	})

	t.Run("empty stages", func(t *testing.T) {
		large := LargeBuildScans(nil, "lineitem", threshold)
		if len(large) != 0 {
			t.Fatalf("want 0 for nil stages, got %d", len(large))
		}
	})

	t.Run("probe alias is only large scan", func(t *testing.T) {
		onlyProbe := []physical.Stage{
			{ID: "s1", Type: "scan", ScanAlias: "lineitem", EstimatedBytes: 60 * gb},
			{ID: "s2", Type: "scan", ScanAlias: "nation", EstimatedBytes: 1024},
		}
		large := LargeBuildScans(onlyProbe, "lineitem", threshold)
		if len(large) != 0 {
			t.Fatalf("want 0 when only large scan is probe, got %d", len(large))
		}
	})
}
