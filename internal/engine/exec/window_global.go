package exec

import (
	"fmt"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ---- Global (empty PARTITION BY) window: two-pass streaming over runs ----
//
// A window spec with no PARTITION BY makes the whole input one partition, so
// the partition-at-a-time walker degenerates to full materialization — the
// last remaining unbounded-memory path in Window. Instead, the sorted runs
// on disk let us stream the single partition twice:
//
//   pass 1  stream the merge once, collecting the partition-level scalars a
//           row can depend on (row count n, whole-partition aggregates,
//           first/last/nth values);
//   pass 2  re-open the merge and stream output batch-at-a-time, computing
//           each function incrementally with O(1) carried state.
//
// Per-function streaming needs (mirroring computePartitionColumnar exactly —
// the golden A/B tests enforce value-identity with the in-memory path):
//
//   row_number, rank, dense_rank, percent_rank   prev-row peer compare
//   sum/count/avg/min/max WITH order by           running state, peer-close
//   sum/count/avg/min/max WITHOUT order by        pass-1 scalar
//   first_value                                   pass-1 scalar
//   nth_value, last_value (no order by)           pass-1 scalar
//   nth_value, last_value (with order by)         peer-close
//   ntile                                         counter state + n
//   lag(k)                                        k-slot ring buffer
//   lead(k)                                       k-row lookahead
//   cume_dist                                     peer-group lookahead
//
// "peer-close" is the frame: with an ORDER BY and no explicit ROWS/RANGE
// clause, the frame is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW,
// which ends at the end of the row's ORDER-BY PEER GROUP — so every row of a
// group takes the same value and that value is known the moment the group
// closes (backfillPeerFrame). Writing the running per-row value instead is
// the same answer only when no two rows tie, which is what this path used to
// do (#350).
//
// Lead, cume_dist and the peer-close columns hold rows back; everything else
// resolves the row the moment it arrives. Memory is bounded by max lead
// offset plus the largest ORDER-BY peer group (an all-equal-keys input
// degenerates to the full partition — documented bound, charged to the
// tracker).
//
// An EXPLICIT frame does not come here at all: it can reach forward or move
// its lower end, which needs rows this pass has not produced or has already
// dropped, so groupNeedsMaterializedFrame routes those to the
// partition-at-a-time walker instead.

// globalWindowStats carries the pass-1 partition-level scalars.
type globalWindowStats struct {
	n   int64
	sum []float64 // per group column; only aggregates fill these
	// decSum[i] is the EXACT Int128 total for a SUM/AVG column whose input
	// and output are both DECIMAL, and decOverflow[i] says it left the
	// carrier's range. The float sum beside it is unused for those columns:
	// a windowed SUM over a DECIMAL answers what the grouped one answers,
	// which is exact (#586, ADR-0012 item 9).
	decSum      []Int128Sum
	decOverflow []bool
	// cnt[i] counts the NON-NULL rows a SUM/AVG column saw. SQL excludes
	// NULLs from an aggregate's input, so AVG divides by this rather than by
	// the row count, and a column whose every row is NULL answers NULL
	// rather than 0.
	cnt []int64
	// nonNull[i] counts the NON-NULL rows a whole-input COUNT(col) column
	// saw. Separate from cnt[i], which only SUM/AVG fill: the two are the
	// same number for a column both read, but a query that windows COUNT
	// without SUM fills only this one (#670).
	nonNull []int64
	// notSummable[i] marks a SUM/AVG column whose input type has no numeric
	// reading (IPV4, MAC, the byte-backed types, the containers). Its answer
	// is NULL, the same as the grouped aggregate's — see vecFloat64 (#412).
	notSummable []bool
	first       []any
	last        []any
	nth         []any
	minV        []any
	maxV        []any
}

// Int128Sum is batch.Int128 under a name that says what it holds here. The
// alias keeps the streaming state's declarations readable next to the float
// ones they parallel.
type Int128Sum = batch.Int128

// globalDecAgg describes one group column's exact-DECIMAL SUM/AVG, resolved
// ONCE from the pass schema rather than per row.
//
// The two paths through this file — the pass-1 scalar collector and the
// pass-2 running streamer — both consult it, so the decision "is this column
// exact" is made in one place and cannot come out differently in the two
// passes over the same query.
type globalDecAgg struct {
	exact bool
	// addScale is how many digits the AVG division adds: the declared output
	// scale minus the input's. Zero for SUM, which keeps the input's scale.
	addScale int
}

func globalDecAggs(schema []parquet.Column, g windowSpecGroup, inputIdxs []int) []globalDecAgg {
	out := make([]globalDecAgg, len(g.cols))
	for i, wc := range g.cols {
		ii := inputIdxs[i]
		if !windowAccumulates(wc.Func) || ii < 0 || ii >= len(schema) {
			continue
		}
		in := schema[ii]
		oc := windowOutputColumn(wc, schema)
		if in.Type != parquet.TypeDecimal || oc.Type != parquet.TypeDecimal {
			continue
		}
		if oc.Scale < in.Scale {
			continue // see windowDecimalFrames: never produced by the rule
		}
		out[i] = globalDecAgg{exact: true, addScale: oc.Scale - in.Scale}
	}
	return out
}

// decAvgMemo caches one column's last exact AVG division. Both writers below
// answer MANY rows from ONE (sum, count) — every row of a whole-input window,
// every row of a closed ORDER-BY peer group — and batch.DecimalAvg falls to an
// allocating big.Int division whenever the sum scaled by 10^addScale leaves
// int64, which a wide DECIMAL's does routinely. windowDecimalFrames carries
// the same memo for the same reason.
type decAvgMemo struct {
	sum   Int128Sum
	count int64
	q     Int128Sum
	valid bool
}

// writeGlobalDecAgg writes one exact-DECIMAL SUM/AVG answer for row r, or
// leaves it NULL when the frame contributed no non-NULL row.
func writeGlobalDecAgg(vec *batch.Vector, r int, wc WindowColumn, da globalDecAgg,
	sum Int128Sum, cnt int64, overflow bool, memo *decAvgMemo) error {
	if overflow {
		if wc.Func == WinAvg {
			return windowDecimalAvgUnrepresentable(wc.OutputCol)
		}
		return windowDecimalSumOverflow(wc.OutputCol)
	}
	if cnt == 0 {
		return nil
	}
	if wc.Func == WinAvg {
		if !memo.valid || memo.count != cnt || !memo.sum.Equal(sum) {
			q, ok := batch.DecimalAvg(sum, cnt, da.addScale)
			if !ok {
				return windowDecimalAvgUnrepresentable(wc.OutputCol)
			}
			*memo = decAvgMemo{sum: sum, count: cnt, q: q, valid: true}
		}
		vec.DecimalData.Data[r] = memo.q
	} else {
		vec.DecimalData.Data[r] = sum
	}
	vec.Nulls.SetValid(r)
	return nil
}

// globalInputIdxs resolves each group column's input column index in schema
// (-1 when the function takes no input).
func globalInputIdxs(schema []parquet.Column, g windowSpecGroup) []int {
	idxs := make([]int, len(g.cols))
	for i, wc := range g.cols {
		idxs[i] = -1
		if wc.InputCol == "" {
			continue
		}
		for j, c := range schema {
			if c.Name == wc.InputCol {
				idxs[i] = j
				break
			}
		}
	}
	return idxs
}

// globalInputCompares resolves one boxed comparator per group column, from
// the DECLARED input column rather than from the box.
//
// MIN/MAX here carry a running value across batches, so the vector it came
// from is long gone by the next comparison and the columnar comparator cannot
// be used directly. What the declaration supplies is the part the box drops:
// a ROW's field ORDER and a DECIMAL's scale. Without it this path ordered a
// ROW by field NAME while ORDER BY over the same column ordered it
// positionally (#444).
func globalInputCompares(schema []parquet.Column, inputIdxs []int) []boxedCompare {
	cmps := make([]boxedCompare, len(inputIdxs))
	for i, ii := range inputIdxs {
		if ii < 0 || ii >= len(schema) {
			cmps[i] = compareAny
			continue
		}
		cmps[i] = newBoxedCompare(schema[ii])
	}
	return cmps
}

// collectGlobalWindowStats streams the merger once (consuming it) and
// returns the partition-level scalars for g's columns.
func collectGlobalWindowStats(m *runMerger, schema []parquet.Column, g windowSpecGroup) (*globalWindowStats, error) {
	nc := len(g.cols)
	st := &globalWindowStats{
		sum:         make([]float64, nc),
		decSum:      make([]Int128Sum, nc),
		decOverflow: make([]bool, nc),
		cnt:         make([]int64, nc),
		nonNull:     make([]int64, nc),
		notSummable: make([]bool, nc),
		first:       make([]any, nc),
		last:        make([]any, nc),
		nth:         make([]any, nc),
		minV:        make([]any, nc),
		maxV:        make([]any, nc),
	}
	inputIdxs := globalInputIdxs(schema, g)
	cmps := globalInputCompares(schema, inputIdxs)
	decAgg := globalDecAggs(schema, g, inputIdxs)
	for {
		b, err := m.Next()
		if err != nil {
			return nil, err
		}
		if b == nil {
			return st, nil
		}
		for r := 0; r < b.Len; r++ {
			rowIdx := st.n
			st.n++
			for i, wc := range g.cols {
				ii := inputIdxs[i]
				if ii < 0 || ii >= len(b.Columns) {
					continue
				}
				switch wc.Func {
				case WinSum, WinAvg:
					if len(wc.OrderBy) == 0 {
						col := b.Columns[ii]
						if decAgg[i].exact {
							if col.Nulls.IsNullFast(r) {
								continue
							}
							v, ok := st.decSum[i].AddChecked(col.DecimalData.Data[r])
							st.decSum[i] = v
							st.decOverflow[i] = st.decOverflow[i] || !ok
							st.cnt[i]++
							continue
						}
						f, ok := vecFloat64(col, r)
						if !ok {
							st.notSummable[i] = true
							continue
						}
						if !col.Nulls.IsNullFast(r) {
							st.cnt[i]++
						}
						st.sum[i] += f
					}
				// A whole-input COUNT(col) needs the partition's non-NULL
				// total in pass 1, the same way SUM needs its sum: pass 2
				// writes the same number into every row. COUNT(*) never
				// reaches here — globalInputIdxs leaves its index -1 and the
				// guard above skips it — which is exactly the distinction
				// (#670).
				case WinCount:
					if len(wc.OrderBy) == 0 && !b.Columns[ii].Nulls.IsNullFast(r) {
						st.nonNull[i]++
					}
				case WinMin:
					if len(wc.OrderBy) == 0 {
						v := b.Columns[ii].GetValue(r)
						if st.minV[i] == nil || (v != nil && cmps[i](v, st.minV[i]) < 0) {
							st.minV[i] = v
						}
					}
				case WinMax:
					if len(wc.OrderBy) == 0 {
						v := b.Columns[ii].GetValue(r)
						if st.maxV[i] == nil || (v != nil && cmps[i](v, st.maxV[i]) > 0) {
							st.maxV[i] = v
						}
					}
				case WinFirstValue:
					if rowIdx == 0 {
						st.first[i] = b.Columns[ii].GetValue(r)
					}
				case WinLastValue:
					if len(wc.OrderBy) == 0 {
						st.last[i] = b.Columns[ii].GetValue(r)
					}
				case WinNthValue:
					nth := wc.NthValueN
					if nth <= 0 {
						nth = 1
					}
					if rowIdx == int64(nth-1) {
						st.nth[i] = b.Columns[ii].GetValue(r)
					}
				}
			}
		}
	}
}

// pendingGlobalBatch is an input batch whose appended output vectors may
// still have unresolved rows (lead lookahead, open cume_dist peer group).
type pendingGlobalBatch struct {
	b        *batch.RecordBatch // input columns + appended group output vectors
	startRow int64              // global row index of row 0
	bytes    int64              // charged to the tracker while pending
}

// globalWindowStreamer is the pass-2 engine. Next() returns augmented
// batches (input schema + group columns) in stream order.
type globalWindowStreamer struct {
	m      *runMerger
	schema []parquet.Column
	g      windowSpecGroup
	stats  *globalWindowStats
	charge func(int64)

	inputIdxs []int
	orderIdxs []int
	compare   []kernel.SortCompareKernel
	// inputCmp[i] orders two BOXED values of group column i's input column,
	// resolved from the declaration (see globalInputCompares). The running
	// MIN/MAX below outlive the vector, so this is the boxed twin of
	// compare's columnar kernels — one order, two representations.
	inputCmp []boxedCompare
	resolved bool

	// decAgg[i] marks a SUM/AVG column that accumulates EXACTLY (both its
	// input and its output are DECIMAL) and carries AVG's added scale;
	// avgMemo[i] caches its last division (see decAvgMemo).
	decAgg  []globalDecAgg
	avgMemo []decAvgMemo

	// carried state
	rowIdx    int64 // rows consumed from the merger
	rank      int64 // current rank (rank/percent_rank)
	denseRank int64
	runSum    []float64 // per col, running aggregates
	// runDecSum is runSum's exact twin for a DECIMAL column, with the
	// overflow flag ADR-0012 item 9 makes sticky, and runNonNull is the
	// count both of them divide by: SQL excludes NULLs from an aggregate's
	// input, so AVG's denominator is the rows that contributed and a frame
	// of only NULLs answers NULL rather than 0.
	runDecSum  []Int128Sum
	runDecOver []bool
	runNonNull []int64
	runCount   []int64
	// notSummable[i] marks an ORDER-BY'd SUM/AVG column whose input type has
	// no numeric reading. Its rows stay NULL, matching the grouped aggregate
	// (vecFloat64, #412). The peer backfill reads it, so it cannot be a
	// local decision at the row.
	notSummable []bool
	runMin      []any
	runMax      []any
	lagRings    [][]any // per col, ring of size offset
	// ntile state (replicates the in-memory loop exactly)
	ntileBucket []int64
	ntileCount  []int
	ntileLimit  []int
	ntileInit   []bool

	// prev row for peer compare: ref into a pending or emitted batch (run
	// merger batches are pool-free, safe to retain)
	prevB   *batch.RecordBatch
	prevRow int

	// lookahead
	maxLead      int     // max lead offset across cols (0 = none)
	leadCursor   []int64 // per col, next global row still needing its lead value
	needCumeDist bool
	peerStart    int64 // global row index where the open peer group began
	cdCursor     int64 // next global row still needing its cume_dist value
	// holdPeers is set when a frame-sensitive column has an ORDER BY. The
	// default frame ends at CURRENT ROW in RANGE mode, i.e. at the end of
	// the row's ORDER-BY PEER GROUP, so those rows cannot be answered until
	// the group closes — the same hold cume_dist already needed, now general.
	holdPeers  bool
	peerCursor int64 // next global row still needing its frame value

	pending  []*pendingGlobalBatch
	emitFrom int // pending[:emitFrom] fully resolved, ready to emit
	eof      bool
}

func newGlobalWindowStreamer(m *runMerger, schema []parquet.Column, g windowSpecGroup, stats *globalWindowStats, charge func(int64)) *globalWindowStreamer {
	nc := len(g.cols)
	s := &globalWindowStreamer{
		m: m, schema: schema, g: g, stats: stats, charge: charge,
		inputIdxs:   globalInputIdxs(schema, g),
		rank:        1,
		denseRank:   1,
		runSum:      make([]float64, nc),
		runDecSum:   make([]Int128Sum, nc),
		runDecOver:  make([]bool, nc),
		runNonNull:  make([]int64, nc),
		runCount:    make([]int64, nc),
		notSummable: make([]bool, nc),
		runMin:      make([]any, nc),
		runMax:      make([]any, nc),
		lagRings:    make([][]any, nc),
		ntileBucket: make([]int64, nc),
		ntileCount:  make([]int, nc),
		ntileLimit:  make([]int, nc),
		ntileInit:   make([]bool, nc),
		leadCursor:  make([]int64, nc),
	}
	s.inputCmp = globalInputCompares(schema, s.inputIdxs)
	s.decAgg = globalDecAggs(schema, g, s.inputIdxs)
	s.avgMemo = make([]decAvgMemo, nc)
	for i, wc := range g.cols {
		switch wc.Func {
		case WinLag:
			off := wc.LagLeadOffset
			if off <= 0 {
				off = 1
			}
			s.lagRings[i] = make([]any, off)
		case WinLead:
			off := wc.LagLeadOffset
			if off <= 0 {
				off = 1
			}
			if off > s.maxLead {
				s.maxLead = off
			}
		case WinCumeDist:
			s.needCumeDist = true
		}
		if peerDeferred(wc.Func) && len(wc.OrderBy) > 0 {
			s.holdPeers = true
		}
	}
	// ORDER BY column indices + null-aware compare kernels (group-level:
	// every column in a spec group shares the same ORDER BY).
	for _, key := range g.orderBy {
		idx := -1
		for j, c := range schema {
			if c.Name == key.Column {
				idx = j
				break
			}
		}
		s.orderIdxs = append(s.orderIdxs, idx)
	}
	return s
}

// resolveKernels picks compare kernels from the first batch's column types.
//
// An ORDER BY column that resolves to no comparator is an error, not a
// skip: every type this engine has resolves (kernel.ResolveSortCompare), so
// nil means the column's type cannot be ordered at all, and silently
// dropping it from the comparison — as samePeer used to — merges every ORDER
// BY peer group that differs only on that key into one, exactly as a
// dropped PARTITION BY key merges every partition. Mirrors
// SortMergeJoin.resolveCompareKernels (sort_merge_join.go), which already
// fails loudly on the same condition.
func (s *globalWindowStreamer) resolveKernels(b *batch.RecordBatch) error {
	s.compare = make([]kernel.SortCompareKernel, len(s.orderIdxs))
	for i, idx := range s.orderIdxs {
		if idx < 0 || idx >= len(b.Columns) {
			continue
		}
		cmp := kernel.ResolveSortCompare(b.Columns[idx].Type)
		if cmp == nil {
			return fmt.Errorf("window: unsupported ORDER BY type %d for column index %d", b.Columns[idx].Type, idx)
		}
		s.compare[i] = cmp
	}
	s.resolved = true
	return nil
}

// samePeer reports whether two rows are ORDER-BY peers (null==null equal,
// matching sameColumnar's semantics on the in-memory path).
func (s *globalWindowStreamer) samePeer(ba *batch.RecordBatch, ra int, bb *batch.RecordBatch, rb int) bool {
	for i, idx := range s.orderIdxs {
		// idx < 0 is a column resolve could not find — a legitimate skip,
		// unrelated to type support. A resolved column always carries a
		// non-nil compare: resolveKernels errors out before returning
		// otherwise.
		if idx < 0 {
			continue
		}
		if s.compare[i](ba.Columns[idx], ra, bb.Columns[idx], rb) != 0 {
			return false
		}
	}
	return true
}

// outVec returns pending batch pb's output vector for group column i.
func (pb *pendingGlobalBatch) outVec(numInputCols, i int) *batch.Vector {
	return pb.b.Columns[numInputCols+i]
}

// locate maps a global row index to (pending batch, local row). Only rows
// still pending are addressable.
func (s *globalWindowStreamer) locate(row int64) (*pendingGlobalBatch, int) {
	for _, pb := range s.pending {
		if row >= pb.startRow && row < pb.startRow+int64(pb.b.Len) {
			return pb, int(row - pb.startRow)
		}
	}
	return nil, 0
}

// Next returns the next fully-resolved augmented batch, or nil at EOF.
func (s *globalWindowStreamer) Next() (*batch.RecordBatch, error) {
	for {
		if s.emitFrom > 0 {
			pb := s.pending[0]
			s.pending = s.pending[1:]
			s.emitFrom--
			if s.charge != nil && pb.bytes > 0 {
				s.charge(-pb.bytes)
			}
			return pb.b, nil
		}
		if s.eof {
			return nil, nil
		}
		nb, err := s.m.Next()
		if err != nil {
			return nil, err
		}
		if nb == nil {
			s.eof = true
			if err := s.finishEOF(); err != nil {
				return nil, err
			}
			s.markResolved()
			continue
		}
		if nb.Len == 0 {
			continue
		}
		if !s.resolved {
			if err := s.resolveKernels(nb); err != nil {
				return nil, err
			}
		}
		if err := s.ingest(nb); err != nil {
			return nil, err
		}
		s.markResolved()
	}
}

// ingest appends the group's output vectors to nb, computes every
// immediately-resolvable value, and advances lookahead resolution.
func (s *globalWindowStreamer) ingest(nb *batch.RecordBatch) error {
	numInput := len(s.schema)
	// Augment: copy the schema header before appending (it aliases the pass
	// schema slice; appending in place could clobber its backing array).
	outSchema := make([]parquet.Column, numInput, numInput+len(s.g.cols))
	copy(outSchema, s.schema)
	for _, wc := range s.g.cols {
		outCol := windowOutputColumn(wc, s.schema)
		vec := batch.NewColumnVector(outCol, nb.Len)
		vec.Nulls = batch.NewBitmapAllNull(nb.Len)
		nb.Columns = append(nb.Columns, vec)
		outSchema = append(outSchema, outCol)
	}
	nb.Schema = outSchema

	pb := &pendingGlobalBatch{b: nb, startRow: s.rowIdx}
	if s.charge != nil {
		pb.bytes = nb.MemBytes()
		s.charge(pb.bytes)
	}
	s.pending = append(s.pending, pb)

	n := s.stats.n
	for r := 0; r < nb.Len; r++ {
		i64 := s.rowIdx // global row index of this row
		// Peer-group boundary detection (rank family + cume_dist).
		newPeer := false
		if i64 > 0 {
			newPeer = !s.samePeer(s.prevB, s.prevRow, nb, r)
		}
		if i64 > 0 && newPeer {
			s.rank = i64 + 1
			s.denseRank++
			if s.holdPeers {
				// Peer group [peerCursor, i64) closed: its rows' frames end
				// here, and the running state now covers exactly them.
				if err := s.backfillPeerFrame(i64); err != nil {
					return err
				}
			}
			if s.needCumeDist {
				// Peer group [peerStart, i64) closed: cume_dist = i64/n.
				s.backfillCumeDist(i64)
				s.peerStart = i64
			}
		}

		for i, wc := range s.g.cols {
			vec := pb.outVec(numInput, i)
			var inVec *batch.Vector
			if ii := s.inputIdxs[i]; ii >= 0 && ii < numInput {
				inVec = nb.Columns[ii]
			}
			if err := s.computeImmediate(wc, i, vec, r, i64, n, inVec, nb); err != nil {
				return err
			}
		}

		s.prevB, s.prevRow = nb, r
		s.rowIdx++
	}

	// Lead resolution: rows up to rowIdx-1 are visible, so any row r with
	// r+offset <= rowIdx-1 can resolve its lead value now.
	s.resolveLeads()
	return nil
}

// computeImmediate writes row r's value for every function that needs no
// lookahead. Lead and cume_dist rows are left for their resolvers.
func (s *globalWindowStreamer) computeImmediate(wc WindowColumn, i int, vec *batch.Vector, r int, rowIdx, n int64, inVec *batch.Vector, nb *batch.RecordBatch) error {
	switch wc.Func {
	case WinRowNumber:
		vec.Int64Data[r] = rowIdx + 1
		vec.Nulls.SetValid(r)

	case WinRank:
		vec.Int64Data[r] = s.rank
		vec.Nulls.SetValid(r)

	case WinDenseRank:
		vec.Int64Data[r] = s.denseRank
		vec.Nulls.SetValid(r)

	case WinPercentRank:
		if n <= 1 {
			vec.Float64Data[r] = 0
		} else {
			vec.Float64Data[r] = float64(s.rank-1) / float64(n-1)
		}
		vec.Nulls.SetValid(r)

	// The frame-sensitive aggregates with an ORDER BY only ACCUMULATE here.
	// Their frame ends at the end of the row's peer group, so the value is
	// written by backfillPeerFrame once that group closes; writing the
	// running value per row is the same answer only when no two rows tie.
	case WinSum, WinAvg:
		if len(wc.OrderBy) > 0 {
			return s.accumulateRunning(wc, i, r, inVec)
		}
		if s.stats.notSummable[i] {
			return nil
		}
		if s.decAgg[i].exact {
			return writeGlobalDecAgg(vec, r, wc, s.decAgg[i],
				s.stats.decSum[i], s.stats.cnt[i], s.stats.decOverflow[i], &s.avgMemo[i])
		}
		if s.stats.cnt[i] == 0 {
			return nil // every row NULL: SQL says NULL, not 0
		}
		if wc.Func == WinAvg {
			vec.Float64Data[r] = s.stats.sum[i] / float64(s.stats.cnt[i])
		} else {
			vec.Float64Data[r] = s.stats.sum[i]
		}
		vec.Nulls.SetValid(r)

	// COUNT(*) counts rows; COUNT(col) counts the rows where col is not
	// NULL (#670). inVec is nil exactly for the star form — globalInputIdxs
	// finds no column named "*" — so the two spellings are told apart the
	// same way here as on the partition-at-a-time path.
	case WinCount:
		if len(wc.OrderBy) > 0 {
			if inVec == nil || !inVec.Nulls.IsNullFast(r) {
				s.runCount[i]++
			}
			return nil
		}
		if inVec == nil {
			vec.Int64Data[r] = n
		} else {
			vec.Int64Data[r] = s.stats.nonNull[i]
		}
		vec.Nulls.SetValid(r)

	case WinMin:
		if len(wc.OrderBy) > 0 {
			var v any
			if inVec != nil {
				v = inVec.GetValue(r)
			}
			if s.runMin[i] == nil || (v != nil && s.inputCmp[i](v, s.runMin[i]) < 0) {
				s.runMin[i] = v
			}
			return nil
		}
		vec.SetValue(r, s.stats.minV[i])

	case WinMax:
		if len(wc.OrderBy) > 0 {
			var v any
			if inVec != nil {
				v = inVec.GetValue(r)
			}
			if s.runMax[i] == nil || (v != nil && s.inputCmp[i](v, s.runMax[i]) > 0) {
				s.runMax[i] = v
			}
			return nil
		}
		vec.SetValue(r, s.stats.maxV[i])

	case WinLag:
		off := wc.LagLeadOffset
		if off <= 0 {
			off = 1
		}
		ring := s.lagRings[i]
		if rowIdx >= int64(off) {
			// SetValue is nil-safe: a nil lagged value writes NULL while
			// still advancing bytes offsets (sequential-write contract).
			vec.SetValue(r, ring[rowIdx%int64(off)])
		} else if wc.LagLeadDefault != nil {
			vec.SetValue(r, wc.LagLeadDefault)
		} else {
			vec.SetValue(r, nil)
		}
		var cur any
		if inVec != nil {
			cur = inVec.GetValue(r)
		}
		ring[rowIdx%int64(off)] = cur

	case WinLead:
		// resolved by resolveLeads / finishEOF

	case WinFirstValue:
		vec.SetValue(r, s.stats.first[i])

	case WinLastValue:
		if len(wc.OrderBy) > 0 {
			return nil // backfillPeerFrame: the last row of the peer group
		}
		vec.SetValue(r, s.stats.last[i])

	case WinNtile:
		buckets := wc.NtileBuckets
		if buckets <= 0 {
			buckets = 1
		}
		if !s.ntileInit[i] {
			s.ntileBucket[i] = 1
			s.ntileCount[i] = 0
			s.ntileLimit[i] = int(n) / buckets
			if int(n)%buckets > 0 {
				s.ntileLimit[i]++
			}
			s.ntileInit[i] = true
		}
		vec.Int64Data[r] = s.ntileBucket[i]
		vec.Nulls.SetValid(r)
		s.ntileCount[i]++
		base := int(n) / buckets
		remainder := int(n) % buckets
		if s.ntileCount[i] >= s.ntileLimit[i] && int(s.ntileBucket[i]) < buckets {
			s.ntileBucket[i]++
			s.ntileCount[i] = 0
			if int(s.ntileBucket[i]) <= remainder {
				s.ntileLimit[i] = base + 1
			} else {
				s.ntileLimit[i] = base
			}
		}

	case WinCumeDist:
		// resolved by backfillCumeDist / finishEOF

	case WinNthValue:
		if len(wc.OrderBy) > 0 {
			return nil // backfillPeerFrame: NULL until the frame reaches n rows
		}
		nth := wc.NthValueN
		if nth <= 0 {
			nth = 1
		}
		if int64(nth) <= n {
			vec.SetValue(r, s.stats.nth[i])
		} else {
			vec.SetValue(r, nil)
		}
	}
	return nil
}

// accumulateRunning folds row r into the running SUM/AVG state of an
// ORDER-BY'd column. Its value is not written here: the default frame ends at
// the row's ORDER-BY PEER GROUP, so backfillPeerFrame writes it once that
// group closes.
//
// The DECIMAL arm is exact and its overflow is STICKY, matching the grouped
// aggregate and the in-memory frame accumulator (windowDecimalFrames): a
// running total that leaves the carrier's range fails the query rather than
// reporting a wrapped number, and it stays failed even if later rows bring
// it back (ADR-0012 item 9).
func (s *globalWindowStreamer) accumulateRunning(wc WindowColumn, i, r int, inVec *batch.Vector) error {
	if s.decAgg[i].exact {
		if inVec.Nulls.IsNullFast(r) {
			return nil
		}
		v, ok := s.runDecSum[i].AddChecked(inVec.DecimalData.Data[r])
		s.runDecSum[i] = v
		s.runNonNull[i]++
		if !ok {
			s.runDecOver[i] = true
			if wc.Func == WinAvg {
				return windowDecimalAvgUnrepresentable(wc.OutputCol)
			}
			return windowDecimalSumOverflow(wc.OutputCol)
		}
		return nil
	}
	f, ok := vecFloat64(inVec, r)
	if !ok {
		// No numeric reading for this column type: NULL, which is what the
		// output vector already holds and what the grouped SUM answers.
		s.notSummable[i] = true
		return nil
	}
	if inVec != nil && !inVec.Nulls.IsNullFast(r) {
		s.runNonNull[i]++
	}
	s.runSum[i] += f
	return nil
}

// peerDeferred reports whether f's value under the DEFAULT frame depends on
// where the row's ORDER-BY peer group ENDS, and so cannot be written until
// that group closes.
//
// FIRST_VALUE is the frame-sensitive function that is NOT on this list: the
// default frame always starts at the partition's first row, so its answer is
// a pass-1 scalar no matter where the peer group ends.
func peerDeferred(f WindowFunc) bool {
	switch f {
	case WinSum, WinCount, WinAvg, WinMin, WinMax, WinLastValue, WinNthValue:
		return true
	}
	return false
}

// backfillPeerFrame writes the deferred columns for the peer group
// [peerCursor, end).
//
// Every row of a peer group sees the SAME frame under the default spec —
// RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ends at the group's last
// row for all of them — so one value per group serves the whole group, and it
// is exactly known the moment the group closes: the carried running state has
// consumed rows [0, end) and nothing else.
//
// Writes go out in ascending row order, per column, across successive calls,
// which is what a variable-length output vector's offsets require.
func (s *globalWindowStreamer) backfillPeerFrame(end int64) error {
	numInput := len(s.schema)
	for i, wc := range s.g.cols {
		if !peerDeferred(wc.Func) || len(wc.OrderBy) == 0 {
			continue
		}
		var val any
		switch wc.Func {
		case WinLastValue:
			if pb, lr := s.locate(end - 1); pb != nil {
				if ii := s.inputIdxs[i]; ii >= 0 && ii < numInput {
					val = pb.b.Columns[ii].GetValue(lr)
				}
			}
		case WinNthValue:
			nth := wc.NthValueN
			if nth <= 0 {
				nth = 1
			}
			if int64(nth) <= end {
				val = s.stats.nth[i]
			}
		case WinMin:
			val = s.runMin[i]
		case WinMax:
			val = s.runMax[i]
		}
		for row := s.peerCursor; row < end; row++ {
			pb, lr := s.locate(row)
			if pb == nil {
				continue
			}
			vec := pb.outVec(numInput, i)
			switch wc.Func {
			case WinSum, WinAvg:
				if s.notSummable[i] {
					continue // no numeric reading: NULL, as vec already is
				}
				if s.decAgg[i].exact {
					if err := writeGlobalDecAgg(vec, lr, wc, s.decAgg[i],
						s.runDecSum[i], s.runNonNull[i], s.runDecOver[i], &s.avgMemo[i]); err != nil {
						return err
					}
					continue
				}
				if s.runNonNull[i] == 0 {
					continue // frame holds only NULLs: SQL says NULL, not 0
				}
				if wc.Func == WinAvg {
					// The rows that CONTRIBUTED, not `end`: a NULL is not
					// part of an aggregate's input, and dividing by the row
					// count answered a number PostgreSQL does not.
					vec.Float64Data[lr] = s.runSum[i] / float64(s.runNonNull[i])
				} else {
					vec.Float64Data[lr] = s.runSum[i]
				}
				vec.Nulls.SetValid(lr)
			case WinCount:
				vec.Int64Data[lr] = s.runCount[i]
				vec.Nulls.SetValid(lr)
			default:
				vec.SetValue(lr, val)
			}
		}
	}
	s.peerCursor = end
	return nil
}

// resolveLeads fills lead values for rows whose lookahead target has
// arrived. Source values are read from pending batches (a lead target is
// always at a HIGHER row index than the row being resolved, so it is still
// pending when the resolved row is).
func (s *globalWindowStreamer) resolveLeads() {
	numInput := len(s.schema)
	for i, wc := range s.g.cols {
		if wc.Func != WinLead {
			continue
		}
		off := int64(wc.LagLeadOffset)
		if off <= 0 {
			off = 1
		}
		for s.leadCursor[i]+off < s.rowIdx {
			row := s.leadCursor[i]
			pb, lr := s.locate(row)
			if pb == nil {
				// Row already emitted — cannot happen: a batch only emits
				// once every lead cursor has passed it (markResolved).
				s.leadCursor[i]++
				continue
			}
			srcPB, sr := s.locate(row + off)
			var v any
			if srcPB != nil {
				if ii := s.inputIdxs[i]; ii >= 0 {
					v = srcPB.b.Columns[ii].GetValue(sr)
				}
			}
			// nil-safe SetValue keeps bytes offsets advancing on NULLs.
			pb.outVec(numInput, i).SetValue(lr, v)
			s.leadCursor[i]++
		}
	}
}

// backfillCumeDist writes cume_dist for the closed peer group
// [s.peerStart, end) — cd = end/n — into the pending batches.
func (s *globalWindowStreamer) backfillCumeDist(end int64) {
	numInput := len(s.schema)
	cd := float64(end) / float64(s.stats.n)
	for i, wc := range s.g.cols {
		if wc.Func != WinCumeDist {
			continue
		}
		for row := s.cdCursor; row < end; row++ {
			pb, lr := s.locate(row)
			if pb == nil {
				continue
			}
			vec := pb.outVec(numInput, i)
			vec.Float64Data[lr] = cd
			vec.Nulls.SetValid(lr)
		}
	}
	s.cdCursor = end
}

// finishEOF resolves everything still outstanding once the stream ends:
// lead rows past the end (default/NULL) and the final cume_dist group.
func (s *globalWindowStreamer) finishEOF() error {
	numInput := len(s.schema)
	for i, wc := range s.g.cols {
		if wc.Func != WinLead {
			continue
		}
		off := int64(wc.LagLeadOffset)
		if off <= 0 {
			off = 1
		}
		// Resolve in-range targets first, then defaults for the tail.
		for s.leadCursor[i] < s.rowIdx {
			row := s.leadCursor[i]
			pb, lr := s.locate(row)
			if pb == nil {
				s.leadCursor[i]++
				continue
			}
			vec := pb.outVec(numInput, i)
			if row+off < s.rowIdx {
				srcPB, sr := s.locate(row + off)
				var v any
				if srcPB != nil {
					if ii := s.inputIdxs[i]; ii >= 0 {
						v = srcPB.b.Columns[ii].GetValue(sr)
					}
				}
				vec.SetValue(lr, v)
			} else if wc.LagLeadDefault != nil {
				vec.SetValue(lr, wc.LagLeadDefault)
			} else {
				vec.SetValue(lr, nil)
			}
			s.leadCursor[i]++
		}
	}
	if s.holdPeers && s.rowIdx > s.peerCursor {
		if err := s.backfillPeerFrame(s.rowIdx); err != nil { // final peer group closes at EOF
			return err
		}
	}
	if s.needCumeDist && s.rowIdx > s.cdCursor {
		s.backfillCumeDist(s.rowIdx) // final group: cd = n/n = 1
	}
	return nil
}

// markResolved advances emitFrom past every pending batch whose rows are all
// resolved: every lead cursor and the cume_dist cursor have moved beyond it.
func (s *globalWindowStreamer) markResolved() {
	frontier := s.rowIdx
	for i, wc := range s.g.cols {
		if wc.Func == WinLead && s.leadCursor[i] < frontier {
			frontier = s.leadCursor[i]
		}
	}
	if s.needCumeDist && s.cdCursor < frontier {
		frontier = s.cdCursor
	}
	if s.holdPeers && s.peerCursor < frontier {
		frontier = s.peerCursor
	}
	for s.emitFrom < len(s.pending) {
		pb := s.pending[s.emitFrom]
		if pb.startRow+int64(pb.b.Len) <= frontier {
			s.emitFrom++
		} else {
			break
		}
	}
}

// globalWindowDiskPass is windowDiskPass for an empty-PARTITION-BY group:
// two streaming passes instead of one full materialization. Returns the new
// run list and augmented schema.
func globalWindowDiskPass(dir string, schema []parquet.Column, runs []string, g windowSpecGroup, charge func(int64)) ([]string, []parquet.Column, error) {
	merger, runs, err := openRunMerger(dir, schema, g.sortKeys, runs)
	if err != nil {
		return nil, nil, err
	}
	stats, err := collectGlobalWindowStats(merger, schema, g)
	merger.close()
	if err != nil {
		removeRunFiles(runs)
		return nil, nil, err
	}

	outSchema := make([]parquet.Column, len(schema), len(schema)+len(g.cols))
	copy(outSchema, schema)
	for _, wc := range g.cols {
		outSchema = append(outSchema, windowOutputColumn(wc, schema))
	}

	merger2, runs2, err := openRunMerger(dir, schema, g.sortKeys, runs)
	if err != nil {
		// openRunMerger deleted the run files on its own error paths.
		return nil, nil, err
	}
	defer merger2.close()
	streamer := newGlobalWindowStreamer(merger2, schema, g, stats, charge)

	sw, err := newSpillBatchWriter(dir, "window-global-pass")
	if err != nil {
		removeRunFiles(runs2)
		return nil, nil, err
	}
	for {
		b, err := streamer.Next()
		if err != nil {
			streamer.release()
			sw.abort()
			removeRunFiles(runs2)
			return nil, nil, err
		}
		if b == nil {
			break
		}
		for _, chunk := range chunkBatch(b, batch.DefaultBatchSize) {
			if err := sw.writeBatch(chunk); err != nil {
				streamer.release()
				sw.abort()
				removeRunFiles(runs2)
				return nil, nil, err
			}
		}
	}
	path, err := sw.close()
	if err != nil {
		removeRunFiles(runs2)
		return nil, nil, err
	}
	removeRunFiles(runs2)
	if path == "" {
		return nil, outSchema, nil
	}
	return []string{path}, outSchema, nil
}

// release drops the tracker charge for any still-pending batches (early
// close before the stream drained).
func (s *globalWindowStreamer) release() {
	if s.charge == nil {
		return
	}
	for _, pb := range s.pending {
		if pb.bytes > 0 {
			s.charge(-pb.bytes)
		}
	}
	s.pending = nil
}
