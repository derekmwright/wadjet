package physical

import (
	"context"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/optswitch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Metadata COUNT(*): a bare `SELECT COUNT(*) FROM t` answered from the
// catalog manifest instead of the scan pipeline. The rowcount-only scan
// sentinel already narrows the read to one column, but it still DECODES
// that column for every row — 100 parts x 1M rows of dictionary
// resolution to produce a number the manifest (and every parquet footer)
// already stores. On the ClickBench 100-part layout that was the entire
// 212ms local / ~140ms c6a floor for Q01; the manifest read is
// single-digit ms. This is the first cut at the sub-second floor: the
// leaders answer metadata-class queries in milliseconds.
//
// Engagement is deliberately narrow: scalar COUNT(*) (one aggregate, no
// DISTINCT, no GROUP BY / grouping sets) directly over a plain catalog
// scan — no filters of any kind, no table functions, no TABLESAMPLE, no
// partition filter — on the local planning path only (fragment/probe-
// split modes bail), and only when the manifest carries no delete
// markers (merge-on-read deletes make file NumRows an overcount).
// Kill switch: WADJET_META_COUNT=0.

// MetadataCountsPlanned counts metadata-answered COUNT(*) plans
// (observability-first, mirrors TopNLateMatPlanned).
var MetadataCountsPlanned atomic.Int64

var metadataCountToggle = optswitch.Register("meta-count", "WADJET_META_COUNT",
	"bare COUNT(*) answered from catalog manifest row counts instead of scanning")

// tryBuildMetadataCount returns a one-row source when the aggregate
// qualifies; ok=false falls back to the ordinary build.
func (p *Planner) tryBuildMetadataCount(ctx context.Context, node *logical.Node) (exec.Source, bool) {
	if !metadataCountToggle.On() {
		return nil, false
	}
	if p.StreamingSources != nil || p.MaterializedInputs != nil || p.ScanFileFilter != nil {
		return nil, false
	}
	if len(node.GroupBy) != 0 || len(node.GroupingSets) != 0 {
		return nil, false
	}
	if len(node.AggExprs) != 1 {
		return nil, false
	}
	agg := node.AggExprs[0]
	if agg.Func != "count" || agg.Distinct || agg.InputExpr != nil ||
		(agg.InputCol != "" && agg.InputCol != "*") {
		return nil, false
	}
	if len(node.Children) != 1 {
		return nil, false
	}
	scan := node.Children[0]
	if scan.Type != logical.NodeScan || scan.IsTableFunc || scan.SampleMethod != "" ||
		len(scan.ScanPredicates) != 0 || len(scan.PartitionFilter) != 0 {
		return nil, false
	}

	manifest, err := p.getManifest(ctx, scan.TableName)
	if err != nil {
		return nil, false
	}
	if len(manifest.DeleteMarkers) != 0 {
		return nil, false
	}
	var total int64
	for _, part := range manifest.Partitions {
		for _, f := range part.Files {
			if f.NumRows < 0 {
				return nil, false // unknown rowcount recorded — count for real
			}
			total += f.NumRows
		}
	}

	outCol := agg.OutputCol
	if outCol == "" {
		outCol = "count(*)"
	}
	out := batch.NewRecordBatch([]parquet.Column{{Name: outCol, Type: parquet.TypeInt64}}, 1)
	out.Columns[0].Int64Data[0] = total

	MetadataCountsPlanned.Add(1)
	return exec.NewBatchSource([]*batch.RecordBatch{out}), true
}
