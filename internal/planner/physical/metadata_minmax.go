package physical

import (
	"context"
	"math"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/optswitch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Metadata MIN/MAX: an un-grouped, un-filtered `SELECT MIN(a), MAX(b) FROM t`
// answered from parquet footer statistics instead of the scan pipeline. Every
// column chunk's footer already carries the chunk's min and max; a query whose
// only outputs are whole-table MIN/MAX (plus, optionally, COUNT(*)) is a fold
// over those numbers, not a decode of the column. ClickBench Q07
// (`SELECT MIN(EventDate), MAX(EventDate) FROM hits`) spent 3.79 CPU-seconds
// dictionary-decoding 100M values whose extremes sit in 100 footers.
//
// This is the MIN/MAX sibling of tryBuildMetadataCount (metadata_count.go) and
// shares its engagement rules: scalar aggregates (no DISTINCT, no GROUP BY /
// grouping sets) directly over a plain catalog scan — no filters, no table
// functions, no TABLESAMPLE, no partition filter — on the local planning path
// only, and only when the manifest carries no delete markers (a merge-on-read
// delete can remove the very row holding the extreme value, and the footer
// cannot know that). Kill switch: WADJET_META_MINMAX=0.
//
// Statistics are read from each file's footer rather than the catalog's
// persisted RG-metadata blob (catalog.TableRGMeta): the blob stores stat
// VALUES but not each file's parquet schema, and without the file's own column
// type we cannot rule out a scan-side type coercion (copyNativeCoercedDirect
// converts Int64→Int32, Int64→Float64, Date→String when the file and the
// catalog disagree) that would make the footer value and the scanned value
// different numbers. The footer read is metadata-only and concurrent — the
// same read buildRGUnits performs for any scan of a never-analyzed table.
//
// Type support is deliberately narrow, limited to types whose statistics are
// both exactly representable and ordered the way the engine orders them:
// Int32, Int64, Date, Timestamp (stats decode to int64) and Float32, Float64
// (stats decode to float64). Explicitly NOT supported:
//   - String/Bytes. Parquet min/max for BYTE_ARRAY may be TRUNCATED, with
//     is_min_value_exact / is_max_value_exact flagging it — and our
//     ColumnStats does not carry those flags out of the thrift layer
//     (parquet.Statistics has them; parquet.ColumnStats drops them). A
//     truncated max is a PREFIX, i.e. smaller than the true max, so trusting
//     it would return a wrong answer. Declined until the exactness flags are
//     plumbed through.
//   - Decimal. The engine finalizes MIN/MAX through a float64 conversion of a
//     scaled Int128; the footer holds an unscaled physical value whose logical
//     type our reader does not attach to the stat. Not provably identical.
//   - Bool, network types, UUID, nested types, Vector: no benefit, no
//     verified ordering, or (FIXED_LEN_BYTE_ARRAY) no stats emitted at all.
//
// NULL semantics: MIN/MAX ignore NULLs, and so do parquet stats — a chunk's
// min/max are computed over its non-null values only. A row group whose column
// is entirely NULL carries no min/max at all (see leafBuffer.buildStats: it
// emits null_count and nothing else), so it is skipped. When every row group
// is all-null — or the table is empty — no value is ever folded and the result
// is NULL, exactly what the aggregate produces. Any row group that is NOT
// all-null yet lacks readable min/max statistics aborts the optimization: we
// never guess, we scan.

// MetadataMinMaxPlanned counts metadata-answered MIN/MAX plans
// (observability-first, mirrors MetadataCountsPlanned).
var MetadataMinMaxPlanned atomic.Int64

var metadataMinMaxToggle = optswitch.Register("meta-minmax", "WADJET_META_MINMAX",
	"un-grouped MIN/MAX answered from parquet row-group statistics instead of scanning")

// mmKind is the accumulator family an engine column type folds into.
type mmKind uint8

const (
	mmInt   mmKind = iota + 1 // Int32/Int64/Date/Timestamp — statsToNative yields int64
	mmFloat                   // Float32/Float64 — statsToNative yields float64
)

// mmColumn accumulates the MIN and MAX of one scan column across every row
// group of every file in the table.
type mmColumn struct {
	name    string
	kind    mmKind
	colType parquet.TypeID // the column's type in the catalog schema
	outType parquet.TypeID // the type MIN/MAX of that column emits

	found bool
	minI  int64
	maxI  int64
	minF  float64
	maxF  float64
}

// mmOutput is one output column of the metadata-answered aggregate.
type mmOutput struct {
	name    string
	isCount bool // COUNT(*) — answered from the manifest, as tryBuildMetadataCount does
	col     int  // index into the mmColumn slice (MIN/MAX only)
	isMax   bool
}

// mmTypeFor maps a catalog column type to its accumulator family and to the
// type MIN/MAX emits for it. The output type must match
// exec.minMaxOutputType, which the scan path resolves from the observed input
// vector — a mismatch would change the value's rendering (a Date emitted as
// int64 surfaces raw epoch days instead of a date string). ok=false means the
// type is not answerable from statistics.
func mmTypeFor(t parquet.TypeID) (kind mmKind, out parquet.TypeID, ok bool) {
	switch t {
	case parquet.TypeInt32:
		return mmInt, parquet.TypeInt64, true // int32-class MIN/MAX widens to int64
	case parquet.TypeInt64:
		return mmInt, parquet.TypeInt64, true
	case parquet.TypeDate:
		return mmInt, parquet.TypeDate, true
	case parquet.TypeTimestamp:
		return mmInt, parquet.TypeTimestamp, true
	case parquet.TypeFloat32, parquet.TypeFloat64:
		return mmFloat, parquet.TypeFloat64, true
	}
	return 0, 0, false
}

// mmFoldRowGroup folds one row group's footer statistics into every
// accumulator. It returns false when the statistics cannot answer MIN/MAX for
// some column, which obliges the caller to abandon the optimization and scan.
func mmFoldRowGroup(cols []mmColumn, rg parquet.RowGroupStats) bool {
	if rg.NumRows <= 0 {
		return true // empty row group contributes nothing
	}
	for i := range cols {
		c := &cols[i]
		cs, ok := rg.Columns[c.name]
		if !ok || !cs.HasStats {
			return false // no statistics written for this chunk
		}
		if cs.MinValue == nil || cs.MaxValue == nil {
			// No min/max recorded. That is what an all-NULL chunk looks
			// like (our writer emits null_count only), and an all-NULL
			// chunk contributes nothing to MIN/MAX. Anything else is
			// statistics we cannot read — decline rather than guess.
			if cs.NullCount == rg.NumRows {
				continue
			}
			return false
		}
		switch c.kind {
		case mmInt:
			lo, okLo := cs.MinValue.(int64)
			hi, okHi := cs.MaxValue.(int64)
			if !okLo || !okHi {
				return false // stat is not the native form this type decodes to
			}
			if c.colType == parquet.TypeDate &&
				(lo < math.MinInt32 || hi > math.MaxInt32) {
				return false // would not survive the int32 day vector
			}
			if !c.found {
				c.found, c.minI, c.maxI = true, lo, hi
				continue
			}
			if lo < c.minI {
				c.minI = lo
			}
			if hi > c.maxI {
				c.maxI = hi
			}
		case mmFloat:
			lo, okLo := cs.MinValue.(float64)
			hi, okHi := cs.MaxValue.(float64)
			if !okLo || !okHi {
				return false
			}
			if math.IsNaN(lo) || math.IsNaN(hi) {
				// A NaN in the first position of a chunk poisons that
				// chunk's stats (every later `v < min` is false against
				// NaN). Merging a poisoned extreme is not the aggregate's
				// answer — scan.
				return false
			}
			if !c.found {
				c.found, c.minF, c.maxF = true, lo, hi
				continue
			}
			if lo < c.minF {
				c.minF = lo
			}
			if hi > c.maxF {
				c.maxF = hi
			}
		default:
			return false
		}
	}
	return true
}

// mmMerge folds a per-file accumulator set into the shared one.
func mmMerge(dst []mmColumn, src []mmColumn) {
	for i := range dst {
		s := &src[i]
		if !s.found {
			continue
		}
		d := &dst[i]
		if !d.found {
			d.found, d.minI, d.maxI, d.minF, d.maxF = true, s.minI, s.maxI, s.minF, s.maxF
			continue
		}
		switch d.kind {
		case mmInt:
			if s.minI < d.minI {
				d.minI = s.minI
			}
			if s.maxI > d.maxI {
				d.maxI = s.maxI
			}
		case mmFloat:
			if s.minF < d.minF {
				d.minF = s.minF
			}
			if s.maxF > d.maxF {
				d.maxF = s.maxF
			}
		}
	}
}

// mmFileUsable reports whether a file's own parquet schema lets us trust its
// statistics for these columns. Three conditions, each of them a way the
// footer value could otherwise differ from the scanned value:
//
//   - the column is a TOP-LEVEL leaf of this file (a repeated or nested leaf
//     has per-element statistics, not per-row);
//   - its terminal name is unique among the file's leaves — RowGroupStats
//     keys by terminal name, so a same-named nested field would alias it;
//   - the file's type and the catalog's type share a storage class, i.e. the
//     scan copies page values into the vector verbatim. A class mismatch is
//     precisely when the decoder converts (Int64→Int32 truncation,
//     Int64→Float64, Date→String), and a converted value is not the footer's.
func mmFileUsable(fr *parquet.FileReader, cols []mmColumn) bool {
	schema := fr.Schema()
	leaves := fr.Leaves()
	for i := range cols {
		topLevel := false
		for _, sc := range schema.Columns {
			if sc.Name == cols[i].name {
				topLevel = true
				break
			}
		}
		if !topLevel {
			return false
		}
		var leaf *parquet.SchemaNode
		n := 0
		for _, lf := range leaves {
			if lf.Name == cols[i].name {
				leaf = lf
				n++
			}
		}
		if n != 1 || leaf == nil || leaf.MaxRepLevel != 0 {
			return false
		}
		fileType := parquet.TypeIDFromSchemaNode(leaf)
		if scan.StorageClass(fileType) != scan.StorageClass(cols[i].colType) {
			return false
		}
	}
	return true
}

// tryBuildMetadataMinMax returns a one-row source when every aggregate in the
// node is answerable from footer statistics; ok=false falls back to the
// ordinary build.
func (p *Planner) tryBuildMetadataMinMax(ctx context.Context, node *logical.Node) (exec.Source, bool) {
	if !metadataMinMaxToggle.On() || p.catalog == nil {
		return nil, false
	}
	if p.StreamingSources != nil || p.MaterializedInputs != nil || p.ScanFileFilter != nil {
		return nil, false
	}
	if len(node.GroupBy) != 0 || len(node.GroupingSets) != 0 || len(node.AggExprs) == 0 {
		return nil, false
	}
	if len(node.Children) != 1 {
		return nil, false
	}
	scanNode := node.Children[0]
	if scanNode.Type != logical.NodeScan || scanNode.IsTableFunc || scanNode.SampleMethod != "" ||
		len(scanNode.ScanPredicates) != 0 || len(scanNode.Predicates) != 0 ||
		len(scanNode.PartitionFilter) != 0 {
		return nil, false
	}

	tableMeta, err := p.catalog.GetTable(ctx, scanNode.TableName)
	if err != nil || tableMeta == nil {
		return nil, false
	}

	var cols []mmColumn
	outputs := make([]mmOutput, 0, len(node.AggExprs))
	minMaxSeen := false
	for _, agg := range node.AggExprs {
		if agg.Distinct {
			return nil, false
		}
		outName := agg.OutputCol
		switch agg.Func {
		case "count":
			if agg.InputExpr != nil || (agg.InputCol != "" && agg.InputCol != "*") {
				return nil, false // COUNT(col) counts non-NULLs — not this path
			}
			if outName == "" {
				outName = "count(*)"
			}
			outputs = append(outputs, mmOutput{name: outName, isCount: true})
		case "min", "max":
			colName := agg.InputCol
			if agg.InputExpr != nil {
				cr, ok := agg.InputExpr.(*plansql.ColRef)
				if !ok {
					return nil, false // MIN(expr) — not a bare column
				}
				colName = cr.Column
			}
			if colName == "" || outName == "" {
				return nil, false
			}
			idx := -1
			for i := range cols {
				if cols[i].name == colName {
					idx = i
					break
				}
			}
			if idx < 0 {
				var schemaCol *parquet.Column
				for i := range tableMeta.Schema.Columns {
					if tableMeta.Schema.Columns[i].Name == colName {
						schemaCol = &tableMeta.Schema.Columns[i]
						break
					}
				}
				if schemaCol == nil {
					return nil, false
				}
				kind, outType, ok := mmTypeFor(schemaCol.Type)
				if !ok {
					return nil, false
				}
				cols = append(cols, mmColumn{
					name: colName, kind: kind, colType: schemaCol.Type, outType: outType,
				})
				idx = len(cols) - 1
			}
			minMaxSeen = true
			outputs = append(outputs, mmOutput{name: outName, col: idx, isMax: agg.Func == "max"})
		default:
			return nil, false
		}
	}
	if !minMaxSeen {
		return nil, false // pure COUNT(*) belongs to tryBuildMetadataCount
	}

	manifest, err := p.catalog.GetManifest(ctx, scanNode.TableName)
	if err != nil || manifest == nil {
		return nil, false
	}
	if len(manifest.DeleteMarkers) != 0 {
		return nil, false
	}
	var files []catalog.FileEntry
	var totalRows int64
	for _, part := range manifest.Partitions {
		for _, f := range part.Files {
			if f.NumRows < 0 {
				return nil, false
			}
			totalRows += f.NumRows
			files = append(files, f)
		}
	}

	if !p.mmFoldFiles(ctx, files, cols) {
		return nil, false
	}

	schema := make([]parquet.Column, len(outputs))
	for i, o := range outputs {
		if o.isCount {
			schema[i] = parquet.Column{Name: o.name, Type: parquet.TypeInt64}
			continue
		}
		schema[i] = parquet.Column{Name: o.name, Type: cols[o.col].outType, Nullable: true}
	}
	out := batch.NewRecordBatch(schema, 1)
	for i, o := range outputs {
		vec := out.Columns[i]
		if o.isCount {
			vec.Int64Data[0] = totalRows
			continue
		}
		c := &cols[o.col]
		if !c.found {
			vec.Nulls.SetNull(0) // every row group all-NULL, or no rows at all
			continue
		}
		switch c.outType {
		case parquet.TypeInt64, parquet.TypeTimestamp:
			if o.isMax {
				vec.Int64Data[0] = c.maxI
			} else {
				vec.Int64Data[0] = c.minI
			}
		case parquet.TypeDate:
			if o.isMax {
				vec.Int32Data[0] = int32(c.maxI)
			} else {
				vec.Int32Data[0] = int32(c.minI)
			}
		case parquet.TypeFloat64:
			if o.isMax {
				vec.Float64Data[0] = c.maxF
			} else {
				vec.Float64Data[0] = c.minF
			}
		default:
			return nil, false
		}
	}

	MetadataMinMaxPlanned.Add(1)
	return exec.NewBatchSource([]*batch.RecordBatch{out}), true
}

// mmFoldFiles reads every file's footer concurrently and folds its row-group
// statistics into cols. Returns false if any file is unreadable, carries a
// schema we cannot trust, or holds a row group without usable statistics — in
// every such case the caller must scan.
func (p *Planner) mmFoldFiles(ctx context.Context, files []catalog.FileEntry, cols []mmColumn) bool {
	if len(files) == 0 {
		return true // empty table: nothing folded, every MIN/MAX is NULL
	}
	workers := runtime.NumCPU()
	if workers > len(files) {
		workers = len(files)
	}
	// Each worker folds into its OWN accumulator set (copied from the shared
	// one while it is still untouched, so the copies race with nothing) and
	// the parent merges them after the join.
	var (
		declined atomic.Bool
		idx      int64
		wg       sync.WaitGroup
	)
	results := make([][]mmColumn, workers)
	for w := range results {
		results[w] = make([]mmColumn, len(cols))
		copy(results[w], cols)
	}
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(local []mmColumn) {
			defer wg.Done()
			for {
				n := int(atomic.AddInt64(&idx, 1) - 1)
				if n >= len(files) || declined.Load() || ctx.Err() != nil {
					return
				}
				fr, release, err := p.mmOpenFooter(ctx, files[n])
				if err != nil {
					declined.Store(true)
					return
				}
				ok := mmFileUsable(fr, local)
				if ok {
					for rg := 0; rg < fr.NumRowGroups(); rg++ {
						if !mmFoldRowGroup(local, fr.RowGroupStats(rg)) {
							ok = false
							break
						}
					}
				}
				release()
				if !ok {
					declined.Store(true)
					return
				}
			}
		}(results[w])
	}
	wg.Wait()
	if declined.Load() || ctx.Err() != nil {
		return false
	}
	for _, r := range results {
		mmMerge(cols, r)
	}
	return true
}

// mmOpenFooter opens a data file's parquet metadata, preferring a ranged
// footer read when the store supports random access (buildRGUnits' pattern).
// The returned release func must be called when the reader is done.
func (p *Planner) mmOpenFooter(ctx context.Context, entry catalog.FileEntry) (*parquet.FileReader, func(), error) {
	store := p.catalog.Store()
	bucket := p.catalog.Bucket()
	if ras, ok := store.(objstore.ReaderAtStore); ok {
		ra, size, err := ras.GetReaderAt(ctx, bucket, entry.Path)
		if err != nil {
			return nil, nil, err
		}
		fr, err := parquet.OpenFileReaderMetadata(ra, size)
		if err != nil {
			ra.Close()
			return nil, nil, err
		}
		return fr, func() { ra.Close() }, nil
	}
	rc, _, err := store.Get(ctx, bucket, entry.Path)
	if err != nil {
		return nil, nil, err
	}
	data, err := readAllSized(rc, entry.SizeBytes, false)
	rc.Close()
	if err != nil {
		return nil, nil, err
	}
	reader, err := parquet.NewReaderFromBytes(data)
	if err != nil {
		return nil, nil, err
	}
	return reader.FileReader(), func() {}, nil
}
