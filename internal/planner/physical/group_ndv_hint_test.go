package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

func ndvScan(stats map[string]logical.ScanColumnStats, rows int64) *logical.Node {
	scan := logical.NewScan("t", "")
	scan.ScanColStats = stats
	scan.ScanRowEstimate = rows
	return scan
}

func TestGroupKeyNDVEstimate(t *testing.T) {
	stats := map[string]logical.ScanColumnStats{
		"UserID":   {NDV: 17_000_000},
		"RegionID": {NDV: 9_000},
		"nostats":  {NDV: 0},
	}
	scan := ndvScan(stats, 100_000_000)

	if got := groupKeyNDVEstimate(scan, []string{"userid"}); got != 17_000_000 {
		t.Fatalf("single col (case-folded) = %d, want 17M", got)
	}
	// Product capped by the scan row estimate.
	if got := groupKeyNDVEstimate(scan, []string{"UserID", "RegionID"}); got != 100_000_000 {
		t.Fatalf("product cap = %d, want row estimate 100M", got)
	}
	// Any key without HLL stats declines the hint entirely.
	if got := groupKeyNDVEstimate(scan, []string{"UserID", "nostats"}); got != 0 {
		t.Fatalf("missing NDV = %d, want 0", got)
	}
	if got := groupKeyNDVEstimate(scan, []string{"__gb_expr_0"}); got != 0 {
		t.Fatalf("synthetic key = %d, want 0", got)
	}
	// The walk crosses Filter but stops at anything that can rebind
	// columns or change shape.
	filter := &logical.Node{Type: logical.NodeFilter, Children: []*logical.Node{scan}}
	if got := groupKeyNDVEstimate(filter, []string{"UserID"}); got != 17_000_000 {
		t.Fatalf("through filter = %d, want 17M", got)
	}
	proj := &logical.Node{Type: logical.NodeProject, Children: []*logical.Node{scan}}
	if got := groupKeyNDVEstimate(proj, []string{"UserID"}); got != 0 {
		t.Fatalf("through project = %d, want 0 (declines)", got)
	}
}
