package physical

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/optswitch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Top-N late materialization: ORDER BY <col> LIMIT n over a wide scan
// (SELECT *, ClickBench Q24) decodes every projected column of every row
// just to keep n of them. The rewrite runs the scan→filter→top-N phase
// over ONLY the columns the filter and sort keys touch, plus a synthetic
// __row_loc column carrying each row's (row-group unit, row) identity,
// then refetches the n winning rows' full width straight from the
// parquet files. On a 105-column table where the narrow set is 2-3
// columns, phase 1 skips ~97% of all decode work; phase 2 decodes at
// most n row groups.
//
// The rewrite engages only on the local catalog-scan path (standalone /
// small-query fast path): Sort→Filter*→Scan of a real table, plain-column
// sort keys, no nested types (the row-group-parallel native scan is what
// makes row identity well-defined), no scan cache, no probe-split /
// scan-split fragment modes. Everything else falls back to the ordinary
// top-N build. Kill switch: WADJET_TOPN_LATEMAT=0.

// RowLocColumn is the synthetic column carrying (rgUnit ordinal << 32 |
// row-in-group) through the narrow phase. The "__" prefix keeps it out of
// user column namespaces (sanitizeScanNeeds already passes such names).
const RowLocColumn = "__row_loc"

const (
	// lateMatMinSavedCols: engage only when the narrow phase avoids
	// decoding at least this many columns — below that the refetch's
	// per-row-group decode isn't paying for itself.
	lateMatMinSavedCols = 8
	// lateMatMaxRows bounds the refetch (and the single output batch).
	lateMatMaxRows = 4096
)

// TopNLateMatPlanned counts top-N pipelines planned with late
// materialization. Observability-first (mirrors LateMatJoinsPlanned):
// dormancy tests assert zero with the switch off; engagement tests use it
// instead of inferring from wall clock.
var TopNLateMatPlanned atomic.Int64

var topNLateMatToggle = optswitch.Register("topn-latemat", "WADJET_TOPN_LATEMAT",
	"top-N late materialization: sort on key columns + row locator, re-fetch survivors")

// tryBuildTopNLateMat attempts the rewrite for Limit(n) over sortNode.
// Returns ok=false (nil error) when the shape doesn't qualify — the caller
// falls back to the ordinary top-N build.
func (p *Planner) tryBuildTopNLateMat(ctx context.Context, sortNode *logical.Node, n int) (exec.Source, bool, error) {
	if !topNLateMatToggle.On() || n <= 0 || n > lateMatMaxRows {
		return nil, false, nil
	}
	// Fragment/probe-split modes plan scans with different source types and
	// alias-keyed side tables; the rewrite is local-pipeline only.
	if p.StreamingSources != nil || p.MaterializedInputs != nil || p.ScanFileFilter != nil {
		return nil, false, nil
	}
	if len(sortNode.Children) != 1 || len(sortNode.OrderBy) == 0 {
		return nil, false, nil
	}

	// Walk Sort → Filter* → Scan.
	cur := sortNode.Children[0]
	var filters []*logical.Node
	for cur != nil && cur.Type == logical.NodeFilter && len(cur.Children) == 1 {
		filters = append(filters, cur)
		cur = cur.Children[0]
	}
	if cur == nil || cur.Type != logical.NodeScan || cur.IsTableFunc || cur.SampleMethod != "" {
		return nil, false, nil
	}
	scanNode := cur
	// A scan the optimizer already narrowed doesn't have enough width to
	// pay for the refetch; the wide case (SELECT *) leaves RequiredColumns
	// empty ("all columns").
	if len(scanNode.RequiredColumns) != 0 {
		return nil, false, nil
	}
	// Multi-scan queries share a populate-once cache keyed by table; a
	// row-loc-stamped populate would leak the synthetic column to the
	// other consumers.
	if p.scanCache != nil {
		if _, shared := p.scanCache[scanNode.TableName]; shared {
			return nil, false, nil
		}
	}

	meta, err := p.catalog.GetTable(ctx, scanNode.TableName)
	if err != nil {
		return nil, false, nil
	}
	schema := meta.Schema.Columns
	// Nested schemas take the row-based fallback scan, which has no
	// row-group units to anchor row identity to.
	if (&parquet.Schema{Columns: schema}).HasNestedColumns() {
		return nil, false, nil
	}
	canon := make(map[string]string, len(schema))
	for _, c := range schema {
		canon[strings.ToLower(c.Name)] = c.Name
	}

	// Narrow set: sort keys (plain columns only) + filter-referenced
	// columns + scan-level pruning predicate columns.
	narrow := make(map[string]bool, 8)
	for _, ob := range sortNode.OrderBy {
		name, ok := canon[strings.ToLower(cleanExpr(ob.Column))]
		if !ok {
			return nil, false, nil // expression or unknown key — not this rewrite
		}
		narrow[name] = true
	}
	for _, f := range filters {
		for _, c := range logical.NodeColumnRefs(f) {
			name, ok := canon[strings.ToLower(c)]
			if !ok {
				return nil, false, nil // references something the scan can't provide
			}
			narrow[name] = true
		}
	}
	for _, pred := range scanNode.ScanPredicates {
		if pred.Column != "" {
			if name, ok := canon[strings.ToLower(pred.Column)]; ok {
				narrow[name] = true
			}
		}
	}
	if len(schema)-len(narrow) < lateMatMinSavedCols {
		return nil, false, nil
	}
	narrowCols := make([]string, 0, len(narrow))
	for _, c := range schema { // schema order, deterministic
		if narrow[c.Name] {
			narrowCols = append(narrowCols, c.Name)
		}
	}

	// Clone the chain with the scan narrowed, then build it through the
	// ordinary pipeline machinery so filters compile exactly as they would
	// have.
	scanClone := *scanNode
	scanClone.RequiredColumns = narrowCols
	head := &scanClone
	for i := len(filters) - 1; i >= 0; i-- {
		fc := *filters[i]
		fc.Children = []*logical.Node{head}
		head = &fc
	}
	childSource, childOps, _, err := p.buildPipeline(ctx, head)
	if err != nil {
		return nil, false, err
	}
	css, ok := childSource.(*catalogScanSource)
	if !ok || css.cache != nil {
		return nil, false, nil
	}
	css.emitRowLoc = true

	var keys []exec.SortKey
	for _, ob := range sortNode.OrderBy {
		order := exec.Ascending
		if ob.Desc {
			order = exec.Descending
		}
		keys = append(keys, exec.SortKey{
			Column:    cleanExpr(ob.Column),
			Order:     order,
			NullsLast: resolveNullsLast(ob),
		})
	}
	sortOp := exec.NewSort(keys)
	sortOp.Limit = n
	if sm := p.getSpillManager(); sm != nil {
		sortOp.Spill = sm
	}

	TopNLateMatPlanned.Add(1)
	return &topNLateMatSource{
		child:  childSource,
		ops:    childOps,
		sort:   sortOp,
		limitN: n,
		scan:   css,
	}, true, nil
}

// topNLateMatSource runs the narrow scan→filter pipeline into a top-N
// sort, then refetches the winners' full rows and emits them in sort
// order as a single batch.
type topNLateMatSource struct {
	child  exec.Source
	ops    []exec.UnaryOperator
	sort   *exec.Sort
	limitN int
	scan   *catalogScanSource

	ran bool
	out *batch.RecordBatch
}

// ServesHeldState marks the output phase as a held-state drain — exempt
// from heap-backpressure pauses (exec.HeldStateSource), like the other
// pipeline-breaker source adapters.
func (s *topNLateMatSource) ServesHeldState() bool { return true }

func (s *topNLateMatSource) Init(ctx context.Context) error { return nil }

func (s *topNLateMatSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	if !s.ran {
		s.ran = true
		pipe := &exec.Pipeline{
			Source:  s.child,
			Ops:     s.ops,
			Sink:    s.sort,
			Workers: innerPipelineWorkers(s.child),
		}
		if err := pipe.Run(ctx); err != nil {
			return nil, err
		}
		s.sort.Truncate(s.limitN)

		// Drain the sorted narrow rows into an ordered row-loc list.
		var locs []int64
		locIdx := -1
		for {
			b, err := s.sort.Next(ctx)
			if err != nil {
				return nil, err
			}
			if b == nil {
				break
			}
			if locIdx < 0 {
				for i, c := range b.Schema {
					if c.Name == RowLocColumn {
						locIdx = i
						break
					}
				}
				if locIdx < 0 {
					return nil, fmt.Errorf("top-N late materialization: %s column missing from sorted output", RowLocColumn)
				}
			}
			vec := b.Columns[locIdx]
			if b.Sel != nil {
				for _, r := range b.Sel {
					locs = append(locs, vec.Int64Data[r])
				}
			} else {
				for i := 0; i < b.Len; i++ {
					locs = append(locs, vec.Int64Data[i])
				}
			}
		}
		if len(locs) == 0 {
			return nil, nil
		}
		wide, err := s.scan.RefetchRows(ctx, locs)
		if err != nil {
			return nil, fmt.Errorf("top-N late materialization refetch: %w", err)
		}
		s.out = wide
	}
	b := s.out
	s.out = nil
	return b, nil
}

func (s *topNLateMatSource) Close() error {
	s.sort.Close()
	return s.child.Close()
}

func (s *topNLateMatSource) RowsScanned() int64 {
	if sp, ok := s.child.(exec.ScanStatsProvider); ok {
		return sp.RowsScanned()
	}
	return 0
}
