package exec

import (
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
	n     int64
	sum   []float64 // per group column; only aggregates fill these
	first []any
	last  []any
	nth   []any
	minV  []any
	maxV  []any
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

// collectGlobalWindowStats streams the merger once (consuming it) and
// returns the partition-level scalars for g's columns.
func collectGlobalWindowStats(m *runMerger, schema []parquet.Column, g windowSpecGroup) (*globalWindowStats, error) {
	nc := len(g.cols)
	st := &globalWindowStats{
		sum:   make([]float64, nc),
		first: make([]any, nc),
		last:  make([]any, nc),
		nth:   make([]any, nc),
		minV:  make([]any, nc),
		maxV:  make([]any, nc),
	}
	inputIdxs := globalInputIdxs(schema, g)
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
						st.sum[i] += vecFloat64(b.Columns[ii], r)
					}
				case WinMin:
					if len(wc.OrderBy) == 0 {
						v := b.Columns[ii].GetValue(r)
						if st.minV[i] == nil || (v != nil && compareAny(v, st.minV[i]) < 0) {
							st.minV[i] = v
						}
					}
				case WinMax:
					if len(wc.OrderBy) == 0 {
						v := b.Columns[ii].GetValue(r)
						if st.maxV[i] == nil || (v != nil && compareAny(v, st.maxV[i]) > 0) {
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
	resolved  bool

	// carried state
	rowIdx    int64 // rows consumed from the merger
	rank      int64 // current rank (rank/percent_rank)
	denseRank int64
	runSum    []float64 // per col, running aggregates
	runCount  []int64
	runMin    []any
	runMax    []any
	lagRings  [][]any // per col, ring of size offset
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
		runCount:    make([]int64, nc),
		runMin:      make([]any, nc),
		runMax:      make([]any, nc),
		lagRings:    make([][]any, nc),
		ntileBucket: make([]int64, nc),
		ntileCount:  make([]int, nc),
		ntileLimit:  make([]int, nc),
		ntileInit:   make([]bool, nc),
		leadCursor:  make([]int64, nc),
	}
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
func (s *globalWindowStreamer) resolveKernels(b *batch.RecordBatch) {
	s.compare = make([]kernel.SortCompareKernel, len(s.orderIdxs))
	for i, idx := range s.orderIdxs {
		if idx >= 0 && idx < len(b.Columns) {
			s.compare[i] = kernel.ResolveSortCompare(b.Columns[idx].Type)
		}
	}
	s.resolved = true
}

// samePeer reports whether two rows are ORDER-BY peers (null==null equal,
// matching sameColumnar's semantics on the in-memory path).
func (s *globalWindowStreamer) samePeer(ba *batch.RecordBatch, ra int, bb *batch.RecordBatch, rb int) bool {
	for i, idx := range s.orderIdxs {
		if idx < 0 || s.compare[i] == nil {
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
			s.finishEOF()
			s.markResolved()
			continue
		}
		if nb.Len == 0 {
			continue
		}
		if !s.resolved {
			s.resolveKernels(nb)
		}
		s.ingest(nb)
		s.markResolved()
	}
}

// ingest appends the group's output vectors to nb, computes every
// immediately-resolvable value, and advances lookahead resolution.
func (s *globalWindowStreamer) ingest(nb *batch.RecordBatch) {
	numInput := len(s.schema)
	// Augment: copy the schema header before appending (it aliases the pass
	// schema slice; appending in place could clobber its backing array).
	outSchema := make([]parquet.Column, numInput, numInput+len(s.g.cols))
	copy(outSchema, s.schema)
	for _, wc := range s.g.cols {
		vec := batch.NewVector(wc.OutputType, nb.Len)
		vec.Nulls = batch.NewBitmapAllNull(nb.Len)
		nb.Columns = append(nb.Columns, vec)
		outSchema = append(outSchema, parquet.Column{Name: wc.OutputCol, Type: wc.OutputType, Nullable: true})
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
				s.backfillPeerFrame(i64)
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
			s.computeImmediate(wc, i, vec, r, i64, n, inVec, nb)
		}

		s.prevB, s.prevRow = nb, r
		s.rowIdx++
	}

	// Lead resolution: rows up to rowIdx-1 are visible, so any row r with
	// r+offset <= rowIdx-1 can resolve its lead value now.
	s.resolveLeads()
}

// computeImmediate writes row r's value for every function that needs no
// lookahead. Lead and cume_dist rows are left for their resolvers.
func (s *globalWindowStreamer) computeImmediate(wc WindowColumn, i int, vec *batch.Vector, r int, rowIdx, n int64, inVec *batch.Vector, nb *batch.RecordBatch) {
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
	case WinSum:
		if len(wc.OrderBy) > 0 {
			s.runSum[i] += vecFloat64(inVec, r)
			return
		}
		vec.Float64Data[r] = s.stats.sum[i]
		vec.Nulls.SetValid(r)

	case WinCount:
		if len(wc.OrderBy) > 0 {
			s.runCount[i]++
			return
		}
		vec.Int64Data[r] = n
		vec.Nulls.SetValid(r)

	case WinAvg:
		if len(wc.OrderBy) > 0 {
			s.runSum[i] += vecFloat64(inVec, r)
			return
		}
		vec.Float64Data[r] = s.stats.sum[i] / float64(n)
		vec.Nulls.SetValid(r)

	case WinMin:
		if len(wc.OrderBy) > 0 {
			var v any
			if inVec != nil {
				v = inVec.GetValue(r)
			}
			if s.runMin[i] == nil || (v != nil && compareAny(v, s.runMin[i]) < 0) {
				s.runMin[i] = v
			}
			return
		}
		vec.SetValue(r, s.stats.minV[i])

	case WinMax:
		if len(wc.OrderBy) > 0 {
			var v any
			if inVec != nil {
				v = inVec.GetValue(r)
			}
			if s.runMax[i] == nil || (v != nil && compareAny(v, s.runMax[i]) > 0) {
				s.runMax[i] = v
			}
			return
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
			return // backfillPeerFrame: the last row of the peer group
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
			return // backfillPeerFrame: NULL until the frame reaches n rows
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
func (s *globalWindowStreamer) backfillPeerFrame(end int64) {
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
			case WinSum:
				vec.Float64Data[lr] = s.runSum[i]
				vec.Nulls.SetValid(lr)
			case WinCount:
				vec.Int64Data[lr] = s.runCount[i]
				vec.Nulls.SetValid(lr)
			case WinAvg:
				vec.Float64Data[lr] = s.runSum[i] / float64(end)
				vec.Nulls.SetValid(lr)
			default:
				vec.SetValue(lr, val)
			}
		}
	}
	s.peerCursor = end
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
func (s *globalWindowStreamer) finishEOF() {
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
		s.backfillPeerFrame(s.rowIdx) // final peer group closes at EOF
	}
	if s.needCumeDist && s.rowIdx > s.cdCursor {
		s.backfillCumeDist(s.rowIdx) // final group: cd = n/n = 1
	}
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
		outSchema = append(outSchema, parquet.Column{Name: wc.OutputCol, Type: wc.OutputType, Nullable: true})
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
