package physical

import (
	"github.com/derekmwright/wadjet/internal/storage/catalog"
)

// deleteMarkerMap flattens a manifest's merge-on-read delete markers into
// the plan-carried form: data-file path → file-absolute deleted row indices.
//
// The manifest already merges markers per file (catalog.AddDeleteMarkers
// rebuilds one entry per path), but this appends rather than overwrites so a
// manifest written by an older build with two entries for one file still
// contributes both — dropping one silently resurrects its rows, and the cost
// of being defensive here is one append.
//
// Returns nil when the table has no deletes, which is the common case and
// the fast path every consumer checks.
func deleteMarkerMap(markers []catalog.DeleteMarker) map[string][]int64 {
	if len(markers) == 0 {
		return nil
	}
	out := make(map[string][]int64, len(markers))
	for _, dm := range markers {
		if dm.FilePath == "" || len(dm.RowIndices) == 0 {
			continue
		}
		out[dm.FilePath] = append(out[dm.FilePath], dm.RowIndices...)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// rememberScanDeletes records what walkStages read for one table so
// annotateScanDeletes can replay it onto the final stage list. Called with
// the map derived from the SAME manifest object the stage's ScanFiles came
// from; a nil map (no deletes) is recorded too, so a later stage over that
// table is annotated as "known to have none" rather than left unvisited.
func (p *Planner) rememberScanDeletes(table string, deletes map[string][]int64) {
	if table == "" {
		return
	}
	if p.scanDeletes == nil {
		p.scanDeletes = make(map[string]map[string][]int64, 4)
	}
	if _, seen := p.scanDeletes[table]; seen {
		return
	}
	p.scanDeletes[table] = deletes
}

// annotateScanDeletes stamps each stage's table's delete state onto the
// FINAL stage list — the counterpart to annotateScanSchemas, and for the
// same reason: the stage that ends up owning a table's files is not always
// the scan stage walkStages emitted. fuseScanAggregateShuffle, the join
// fusions and the merge-tree collapse all rewrite the list, and a stage
// that acquired ScanFiles without ScanDeletes would scan a table's deleted
// rows back into the answer.
//
// Unlike annotateScanSchemas this consults NO catalog: it replays the
// snapshot walkStages took, because the markers and the file list have to
// come from one manifest revision (Stage.ScanDeletes). A stage already
// carrying markers is left alone, and a stage pinned to a remote cluster is
// skipped — its files live under that cluster's manifest, which this
// planner did not read.
func (p *Planner) annotateScanDeletes(stages []Stage) {
	if len(p.scanDeletes) == 0 {
		return
	}
	for i := range stages {
		name := stages[i].TableName
		if name == "" || len(stages[i].ScanDeletes) > 0 || stages[i].ClusterID != "" {
			continue
		}
		if d, ok := p.scanDeletes[name]; ok && len(d) > 0 {
			stages[i].ScanDeletes = d
		}
	}
}
