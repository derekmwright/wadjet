package exec

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Counters for the three things a bloom filter can do that are not a normal
// filtering decision. Exported so tests and the harness can assert on them:
// #543 was invisible precisely because a filter rejecting 100% of its input
// produced no counter, no log line and no failure.
var (
	// BloomSelfCheckFailures counts filters that could not match keys taken
	// from their own insert side. Every one is a wrong-answer bug.
	BloomSelfCheckFailures atomic.Int64
	// BloomKeyTypeMismatches counts filters disengaged because the column
	// they resolved encodes its keys differently from the column they were
	// built from.
	BloomKeyTypeMismatches atomic.Int64
	// BloomFullRejections counts filters observed rejecting every row they
	// have seen. Legal (disjoint key sets) but always worth a look.
	BloomFullRejections atomic.Int64
)

// bloomAdaptiveBatches is how many batches the adaptive rules observe before
// deciding anything.
const bloomAdaptiveBatches = 32

// BloomFilterOp is a UnaryOperator that pre-filters probe batches using the
// build-side bloom filter. Rows whose join key hash is not in the bloom are
// eliminated via selection vector before reaching the probe operator.
//
// This is read-only on the bloom data, so multiple clones safely share it.
type BloomFilterOp struct {
	bloom     []uint64 // shared, read-only after Build()
	bloomMask uint64

	leftKeys      []string // probe-side key column names
	useIntKey     bool     // single-column integer fast path
	useDualIntKey bool     // two-column integer fast path
	// keyTypes[i] is the resolved COMMON type of the i'th join key pair
	// (exec.HashJoin.KeyTypes, join_key_width.go), set by BloomPushdownOp.
	// The bloom's BITS came from the build index, whose keys are at that
	// type; a probe key encoded at the probe column's own width would hash
	// elsewhere and the filter would reject rows that MATCH — the one error
	// a bloom is not allowed (#615).
	keyTypes []batch.TypeID
	keyIdx   []int // resolved column indices (lazy)
	resolved bool
	selBuf   []uint32 // scratch for selection vector
	keyBuf   []byte   // scratch for multi-column key serialization

	// Adaptive disabling: if the bloom filter isn't filtering enough rows,
	// the hash computation + random memory accesses cost more than they save.
	// Track rejection rate over the first 32 batches; disable if <5% rejected.
	// 32 batches (vs 8) avoids premature disable from partition skew where
	// early batches all come from the same key range.
	totalChecked int
	totalPassed  int
	batchesSeen  int
	disabled     bool

	// keyType is the type of the column whose values were INSERTED into this
	// bloom, and selfCheck a copy of a sample of those values. Both are set
	// by BloomBuilder.FilterOp, and together they are what makes a bloom that
	// cannot match its own keys LOUD instead of silent (#543): the type guard
	// fires on the first batch, the sample replay at the moment the filter
	// starts rejecting everything.
	keyType         batch.TypeID
	keyTypeSet      bool
	selfCheck       *batch.RecordBatch
	selfChecked     bool
	fullRejectNoted bool
}

func (op *BloomFilterOp) Init(_ context.Context) error { return nil }

func (op *BloomFilterOp) Execute(_ context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	if in == nil || in.ActiveLen() == 0 {
		return in, nil
	}

	// Adaptive bypass: if the bloom filter isn't rejecting enough rows,
	// skip it entirely. The hash + two random reads per row cost more
	// than the probe savings when rejection rate is below 5%.
	if op.disabled {
		return in, nil
	}

	// Lazy column index resolution on first batch
	if !op.resolved {
		op.keyIdx = make([]int, len(op.leftKeys))
		for i, col := range op.leftKeys {
			op.keyIdx[i] = in.ResolveColumnIndex(col)
		}
		op.resolved = true
		if !op.keyTypesAgree(in) {
			op.disabled = true
			return in, nil
		}
	}

	activeLen := in.ActiveLen()
	if cap(op.selBuf) < activeLen {
		op.selBuf = make([]uint32, 0, activeLen)
	}
	sel := op.selBuf[:0]

	// If key column not found, pass all rows through (don't filter).
	// This happens when the bloom targets a different scan than expected.
	allFound := true
	for _, idx := range op.keyIdx {
		if idx < 0 {
			allFound = false
			break
		}
	}
	if !allFound {
		return in, nil
	}

	if op.useIntKey && len(op.keyIdx) == 1 && op.keyIdx[0] >= 0 {
		col := in.Columns[op.keyIdx[0]]
		if in.Sel != nil {
			if !col.Nulls.HasNulls() {
				// Null-free + selection: skip null checks, inline typed access
				switch col.Type {
				case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
					data := col.Int32Data
					for _, idx := range in.Sel {
						if bloomContains(op.bloom, op.bloomMask, bloomHashInt(int64(data[idx]))) {
							sel = append(sel, idx)
						}
					}
				default:
					data := col.Int64Data
					for _, idx := range in.Sel {
						if bloomContains(op.bloom, op.bloomMask, bloomHashInt(data[idx])) {
							sel = append(sel, idx)
						}
					}
				}
			} else {
				for _, idx := range in.Sel {
					if col.Nulls.IsNullFast(int(idx)) {
						continue
					}
					key, ok := intKeyFromVector(col, int(idx))
					if ok && bloomContains(op.bloom, op.bloomMask, bloomHashInt(key)) {
						sel = append(sel, idx)
					}
				}
			}
		} else {
			if !col.Nulls.HasNulls() {
				// Null-free: inline typed data access, no null checks
				switch col.Type {
				case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
					data := col.Int32Data
					for i := 0; i < in.Len; i++ {
						if bloomContains(op.bloom, op.bloomMask, bloomHashInt(int64(data[i]))) {
							sel = append(sel, uint32(i))
						}
					}
				default:
					data := col.Int64Data
					for i := 0; i < in.Len; i++ {
						if bloomContains(op.bloom, op.bloomMask, bloomHashInt(data[i])) {
							sel = append(sel, uint32(i))
						}
					}
				}
			} else {
				for i := 0; i < in.Len; i++ {
					if col.Nulls.IsNullFast(i) {
						continue
					}
					key, ok := intKeyFromVector(col, i)
					if ok && bloomContains(op.bloom, op.bloomMask, bloomHashInt(key)) {
						sel = append(sel, uint32(i))
					}
				}
			}
		}
	} else if op.useDualIntKey && len(op.keyIdx) == 2 && op.keyIdx[0] >= 0 && op.keyIdx[1] >= 0 {
		col0, col1 := in.Columns[op.keyIdx[0]], in.Columns[op.keyIdx[1]]
		noNulls := !col0.Nulls.HasNulls() && !col1.Nulls.HasNulls()
		if in.Sel != nil {
			if noNulls {
				for _, idx := range in.Sel {
					a := intValFromCol(col0, int(idx))
					b := intValFromCol(col1, int(idx))
					if bloomContains(op.bloom, op.bloomMask, bloomHashInt(dualIntHash(a, b))) {
						sel = append(sel, idx)
					}
				}
			} else {
				for _, idx := range in.Sel {
					a, b, ok := dualIntKeyFromVectors(col0, col1, int(idx))
					if ok && bloomContains(op.bloom, op.bloomMask, bloomHashInt(dualIntHash(a, b))) {
						sel = append(sel, idx)
					}
				}
			}
		} else {
			if noNulls {
				for i := 0; i < in.Len; i++ {
					a := intValFromCol(col0, i)
					b := intValFromCol(col1, i)
					if bloomContains(op.bloom, op.bloomMask, bloomHashInt(dualIntHash(a, b))) {
						sel = append(sel, uint32(i))
					}
				}
			} else {
				for i := 0; i < in.Len; i++ {
					a, b, ok := dualIntKeyFromVectors(col0, col1, i)
					if ok && bloomContains(op.bloom, op.bloomMask, bloomHashInt(dualIntHash(a, b))) {
						sel = append(sel, uint32(i))
					}
				}
			}
		}
	} else {
		// General string key path
		if in.Sel != nil {
			for _, idx := range in.Sel {
				if op.probeKeyHash(in, int(idx)) {
					sel = append(sel, idx)
				}
			}
		} else {
			for i := 0; i < in.Len; i++ {
				if op.probeKeyHash(in, i) {
					sel = append(sel, uint32(i))
				}
			}
		}
	}

	// Rejection accounting, and it runs BEFORE the all-rejected early return
	// below rather than after it. That ordering is the whole blind spot #543
	// hid in: a filter rejecting every row of every batch returned here on
	// each one, so batchesSeen never advanced, and the adaptive rule — the
	// only code watching this filter at all — never ran even once.
	op.totalChecked += activeLen
	op.totalPassed += len(sel)
	op.batchesSeen++

	// The self-check runs on the FIRST batch, unconditionally, and no
	// rejection rate is allowed to gate it.
	//
	// A rate cannot be the trigger, because a broken filter does not reject
	// everything: it rejects everything the bloom's own false positives do not
	// wave through. At the design ~1% FPR over 100k STRING keys, a #543-shaped
	// divergence rejects about 89% — comfortably between a 5% floor and any
	// ceiling near 100%, so a rate rule of any shape misses it. And at
	// DefaultBatchSize a fully rejected BATCH may never occur at all: 2048
	// rows at 3% FPR pass ~60 rows, every time. (A test that zeroes the bloom
	// bits does see 100% rejection, which is exactly why it is not sufficient
	// evidence that a runtime rule works.)
	//
	// The check costs one pass over a bounded sample, once per operator, and
	// it answers the only question that matters: can this filter match keys
	// that were put INTO it.
	if !op.selfChecked {
		op.selfChecked = true
		if err := op.SelfCheck(); err != nil {
			BloomSelfCheckFailures.Add(1)
			op.disabled = true
			slog.Error("bloom filter cannot match its own build keys — disengaged; rows already rejected are LOST",
				"columns", op.leftKeys, "rows_rejected", op.totalChecked-op.totalPassed, "err", err)
			return in, nil // unfiltered: this batch's rejections were not trustworthy
		}
	}

	op.noteFullRejection(len(sel))

	if op.batchesSeen >= bloomAdaptiveBatches && op.totalChecked > 0 {
		rejected := op.totalChecked - op.totalPassed
		if rejected*20 < op.totalChecked {
			// <5% rejection: the hash plus two random reads per row cost
			// more than the probe lookups they save. NOTE this rule now sees
			// fully-rejected batches too (they used to return before the
			// accounting), which is a behaviour change for the FORWARD bloom
			// as well: a filter that rejects a lot no longer looks, to this
			// rule, like a filter that was never asked.
			op.disabled = true
		}
	}

	if len(sel) == 0 {
		return nil, nil
	}

	op.selBuf = sel
	in.Sel = sel
	return in, nil
}

// keyTypesAgree reports whether the column this op just resolved can be read
// the way this op intends to read it.
//
// Two claims are checked, and they ask DIFFERENT questions of the type — which
// is the whole subtlety here, because the two predicates differ by exactly
// four types and using either one for both claims is a defect.
//
// The first claim is about STORAGE: useIntKey says "index Int32Data or
// Int64Data directly", so the question is isIntKeyColumn — the same predicate
// that set the flag in the first place (join.go). That admits TIMESTAMP, IPv4,
// MAC and DURATION alongside the obvious five, because all four live in
// Int64Data and intKeyFromVector reads them correctly. This claim holds even
// for a bloom that arrived over the WIRE with no record of what built it: the
// DAG's dynamic filters are integer-only by planner gate (columnIntType at
// every emit site) and the worker's apply path hardcodes UseIntKey, so nothing
// between the two re-checks it against the column that actually shows up.
// Pointed at a column with neither slice, that is not a filter answering
// wrongly, it is a panic.
//
// The second claim is about ENCODING, and needs a builder: the resolved column
// must key the way the inserted column keyed. That question is bloomIntKey,
// which admits only the five types appendColumnValue does NOT own — a
// TIMESTAMP bloom key goes through appendColumnValue's eight bytes, so a
// bloom built on INT64 must refuse a TIMESTAMP column even though both are
// Int64Data and both pass the first claim. Widening this one to
// isIntKeyColumn would let two incompatible encodings meet.
//
// Disengaging costs a scan filter; guessing costs rows, or the process.
func (op *BloomFilterOp) keyTypesAgree(in *batch.RecordBatch) bool {
	if op.useIntKey || op.useDualIntKey {
		for _, idx := range op.keyIdx {
			if idx < 0 {
				continue
			}
			if got := in.Columns[idx].Type; !isIntKeyColumn(got) {
				BloomKeyTypeMismatches.Add(1)
				slog.Error("bloom filter takes the integer fast path over a column with no integer storage — filter disengaged",
					"columns", op.leftKeys, "resolved", got.String())
				return false
			}
		}
	}
	if !op.keyTypeSet || len(op.keyIdx) != 1 || op.keyIdx[0] < 0 {
		return true
	}
	got := in.Columns[op.keyIdx[0]].Type
	if got == op.keyType || (bloomIntKey(got) && bloomIntKey(op.keyType)) {
		return true
	}
	BloomKeyTypeMismatches.Add(1)
	slog.Error("bloom filter key type mismatch — filter disengaged",
		"column", op.leftKeys[0], "built_from", op.keyType.String(), "resolved", got.String())
	return false
}

// noteFullRejection records, once per operator, a filter observed rejecting
// every row it has seen. It is observability, not a decision: by the time this
// runs the self-check has already passed, so a total rejection means the two
// key sets are genuinely disjoint — legal, and the reason the query is fast.
//
// It waits for at least one full batch. A three-row probe side rejecting all
// three is not a signal about anything, and logging it made the line noise on
// correct behaviour — which is how a warning stops being read.
func (op *BloomFilterOp) noteFullRejection(passed int) {
	if op.fullRejectNoted || passed != 0 || op.totalChecked < batch.DefaultBatchSize {
		return
	}
	op.fullRejectNoted = true
	BloomFullRejections.Add(1)
	if op.selfCheck == nil {
		// A bloom that arrived over the wire, or one built through
		// NewBloomFilterOp: there is nothing to verify it against, so this
		// carries no information beyond "disjoint or broken, cannot say".
		slog.Debug("bloom filter rejected every row it has seen; no build-key sample to verify it against",
			"columns", op.leftKeys, "rows_checked", op.totalChecked)
		return
	}
	slog.Warn("bloom filter rejected every row it has seen and does match its own build keys — the key sets are disjoint",
		"columns", op.leftKeys, "rows_checked", op.totalChecked)
}

// SelfCheck probes the filter with a sample of the very values that were
// inserted into it. All of them must survive. A miss means the insert side and
// the probe side hashed different byte streams, which is not a lost
// optimization but a lost row on every rejection the filter makes (#543).
//
// Returns nil when the op carries no sample — see BloomBuilder.FilterOp.
func (op *BloomFilterOp) SelfCheck() error {
	if op.selfCheck == nil || op.selfCheck.Len == 0 {
		return nil
	}
	// A fresh op over the same bits, so the check exercises the real Execute
	// dispatch (integer fast path included) without touching this op's
	// resolution or adaptive state.
	probe, ok := op.Clone().(*BloomFilterOp)
	if !ok {
		return nil
	}
	probe.selfCheck = nil
	probe.selfChecked = true     // no recursion into SelfCheck from Execute
	probe.fullRejectNoted = true // and no log line from the sample itself
	// Value copy: Execute writes Sel, and concurrent clones share the batch.
	sample := *op.selfCheck
	sample.Sel = nil
	out, err := probe.Execute(context.Background(), &sample)
	if err != nil {
		return fmt.Errorf("probing the build-key sample: %w", err)
	}
	got := 0
	if out != nil {
		got = out.ActiveLen()
	}
	if got != op.selfCheck.Len {
		return fmt.Errorf("bloom rejected %d of %d values taken from its own insert side (column %q, type %s)",
			op.selfCheck.Len-got, op.selfCheck.Len, op.leftKeys[0], op.keyType.String())
	}
	return nil
}

// probeKeyHash builds the key buffer for a row and checks the bloom filter.
func (op *BloomFilterOp) probeKeyHash(in *batch.RecordBatch, row int) bool {
	op.keyBuf = op.keyBuf[:0]
	for i, idx := range op.keyIdx {
		if idx < 0 {
			op.keyBuf = append(op.keyBuf, 1) // null flag
			continue
		}
		v := in.Columns[idx]
		if v.Nulls.IsNullFast(row) {
			return false // null key → no match
		}
		op.keyBuf = append(op.keyBuf, 0) // not-null flag
		// Same-type direct — see HashJoin.buildKeyFromBatch.
		if t := KeyTypeAt(op.keyTypes, i, v.Type); t != v.Type {
			op.keyBuf = appendCoercedKeyValue(op.keyBuf, v, row, t)
		} else {
			op.keyBuf = appendColumnValue(op.keyBuf, v, row, v.Type)
		}
	}
	return bloomContains(op.bloom, op.bloomMask, bloomHashBytes(op.keyBuf))
}

func (op *BloomFilterOp) Close() error { return nil }

// KeyColumns returns the probe-side key column names this bloom filter checks.
func (op *BloomFilterOp) KeyColumns() []string { return op.leftKeys }

// Clone returns a new BloomFilterOp sharing the same bloom data.
func (op *BloomFilterOp) Clone() UnaryOperator {
	return &BloomFilterOp{
		bloom:         op.bloom,
		bloomMask:     op.bloomMask,
		leftKeys:      op.leftKeys,
		useIntKey:     op.useIntKey,
		useDualIntKey: op.useDualIntKey,
		keyTypes:      op.keyTypes,
		keyType:       op.keyType,
		keyTypeSet:    op.keyTypeSet,
		selfCheck:     op.selfCheck,
	}
}

// intValFromCol reads an int64 value from a column without null checks.
// Caller must ensure the row is not null.
func intValFromCol(col *batch.Vector, row int) int64 {
	switch col.Type {
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return int64(col.Int32Data[row])
	default:
		return col.Int64Data[row]
	}
}

// bloomContains checks if a hash may be in the bloom filter.
// Standalone function so BloomFilterOp can call it without a HashJoin receiver.
func bloomContains(bloom []uint64, mask, hash uint64) bool {
	h1 := hash & mask
	h2 := (hash >> 17) & mask
	b1 := hash & 63
	b2 := (hash >> 6) & 63
	return (bloom[h1]>>b1)&1 != 0 && (bloom[h2]>>b2)&1 != 0
}

// BloomScanFilter holds bloom filter data for row-group-level scan pushdown.
type BloomScanFilter struct {
	Bloom     []uint64 // shared, read-only
	BloomMask uint64
	Column    string // probe-side join key column name
	UseIntKey bool   // true for single integer column join key
}

// BloomScanFilter returns a BloomScanFilter for scan-level pushdown.
// Only applicable for single-column integer join keys.
// Returns nil if not applicable.
func (op *BloomFilterOp) BloomScanFilter() *BloomScanFilter {
	if !op.useIntKey || len(op.leftKeys) != 1 {
		return nil
	}
	return &BloomScanFilter{
		Bloom:     op.bloom,
		BloomMask: op.bloomMask,
		Column:    op.leftKeys[0],
		UseIntKey: true,
	}
}

// BloomHashInt computes the bloom filter hash for an integer key.
// Exported for use by the scan layer for row-group-level pruning.
func BloomHashInt(key int64) uint64 {
	return bloomHashInt(key)
}

// BloomContains checks if a hash may be in the bloom filter.
// Exported for use by the scan layer for row-group-level pruning.
func BloomContains(bloom []uint64, mask, hash uint64) bool {
	return bloomContains(bloom, mask, hash)
}

// NewBloomFilterOp creates a BloomFilterOp from pre-built bloom data.
// Used for reverse bloom pushdown where the probe side's key set filters
// the build side's scan.
func NewBloomFilterOp(bloom []uint64, bloomMask uint64, keys []string, useIntKey bool) *BloomFilterOp {
	return &BloomFilterOp{
		bloom:     bloom,
		bloomMask: bloomMask,
		leftKeys:  keys, // "left" from the op's perspective = the column to check
		useIntKey: useIntKey,
	}
}

// NewBloomSized allocates a bloom filter for totalRows keys (~10 bits per
// key for ~1% FPR). Returns nil bloom for zero rows.
func NewBloomSized(totalRows int) (bloom []uint64, bloomMask uint64) {
	if totalRows == 0 {
		return nil, 0
	}
	nSlots := 1
	for nSlots*64 < totalRows*10 {
		nSlots *= 2
	}
	if nSlots < 8 {
		nSlots = 8
	}
	return make([]uint64, nSlots), uint64(nSlots - 1)
}

// bloomIntKey reports whether a bloom KEY of this type is hashed as an int64
// rather than through appendColumnValue. It is the ONE place that question is
// answered: the insert side and the probe side used to decide it
// independently, from different inputs, and #543 is what that costs when the
// two answers disagree.
//
// Note which int-backed types are NOT here. TIMESTAMP, IPv4, MAC and DURATION
// live in Int64Data but encode through appendColumnValue like everything
// else, because that is what the hash join's own key builder does with them.
//
// It is therefore NOT the predicate for "can the integer fast path read this
// column" — that is isIntKeyColumn (join.go), which admits those four. The two
// are close enough to substitute for each other by accident and differ in a
// way that matters: keyTypesAgree needs one for its storage claim and the
// other for its encoding claim, and swapping them disengages a correct
// forward bloom on four shipped types.
func bloomIntKey(t batch.TypeID) bool {
	switch t {
	case batch.TypeInt32, batch.TypeInt64, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return true
	}
	return false
}

// appendBloomKey appends ONE key column's canonical join-key bytes: the
// not-null flag the hash join's own key builders write (join.go's
// buildProbeKey and the build-side strIndex key), then appendColumnValue's
// encoding of the value.
//
// Every bloom key on every side goes through here, so a bloom's key set is a
// SUBSET of the join's by construction rather than by two edits agreeing —
// float NaN/-0.0 folding, DECIMAL scale normalization, container encodings and
// all. The build side used to hash the raw col.BytesData.Value(row) instead,
// which is the same bytes for no type at all: every bytes-backed key
// (STRING, BYTES, IPv6, CIDR, UUID) missed the length prefix and the flag, and
// every other non-integer type indexed a BytesColumn its vector does not have
// (#543).
func appendBloomKey(buf []byte, v *batch.Vector, row int) []byte {
	return appendBloomKeyAt(buf, v, row, v.Type)
}

// appendBloomKeyAt is appendBloomKey at a RESOLVED key type — the pair's
// common type for a cross-width join key (join_key_width.go). `target` equal
// to v.Type is the same bytes appendBloomKey always produced.
func appendBloomKeyAt(buf []byte, v *batch.Vector, row int, target batch.TypeID) []byte {
	buf = append(buf, 0) // not-null flag
	return AppendWidenedKeyValue(buf, v, row, target)
}

// bloomSet marks both of a hash's bits.
func bloomSet(bloom []uint64, mask, hash uint64) {
	h1 := hash & mask
	h2 := (hash >> 17) & mask
	bloom[h1] |= 1 << (hash & 63)
	bloom[h2] |= 1 << ((hash >> 6) & 63)
}

// The retained self-check sample is bounded twice: a cap on the whole sample,
// and a cap on what any ONE batch may contribute, so the sample spans the
// build instead of being the head of its first batch.
//
// A build of fewer than eight batches therefore retains FEWER than the cap —
// eight values from a single-batch build — and that is deliberate rather than
// a shortfall. The check is a differential between two key encoders over one
// column type: they agree for every value of the type or for none, so one
// value already decides it. The sample is generous, not load-bearing.
const (
	bloomSelfCheckMaxRows  = 64
	bloomSelfCheckPerBatch = 8
)

// BloomBuilder accumulates one key column's values into a bloom and then hands
// back the BloomFilterOp that probes it.
//
// It exists so the two sides cannot disagree about the key encoding: the
// encoding is decided once, here, from the inserted column's own type, frozen
// against a later batch that disagrees, and handed to the op the builder
// produces along with a sample of the keys that went in (#543).
type BloomBuilder struct {
	bloom []uint64
	mask  uint64

	keyType   batch.TypeID
	typed     bool
	useIntKey bool
	resolved  bool // the key column was found in at least one batch
	inserted  int64
	keyBuf    []byte

	sample   *batch.Vector // copies of inserted values, replayed by SelfCheck
	noSample bool
}

// NewBloomBuilder allocates a builder sized for totalRows keys. Returns nil
// for zero rows, matching NewBloomSized.
func NewBloomBuilder(totalRows int) *BloomBuilder {
	bloom, mask := NewBloomSized(totalRows)
	if bloom == nil {
		return nil
	}
	return &BloomBuilder{bloom: bloom, mask: mask}
}

// Inserted returns how many non-NULL keys went into the bloom, and Resolved
// whether the key column was ever found. A caller must not install a filter
// built from an unresolved column: it rejects everything, which for an
// anti-join means inventing unmatched probe rows.
func (bb *BloomBuilder) Inserted() int64 { return bb.inserted }

// Resolved reports whether the key column was found in at least one batch.
func (bb *BloomBuilder) Resolved() bool { return bb.resolved }

// Add hashes one batch's key column into the bloom. A batch whose key column
// has a different type from the first one's is refused rather than encoded
// two ways.
func (bb *BloomBuilder) Add(b *batch.RecordBatch, keyCol string) error {
	if b == nil {
		return nil
	}
	colIdx := b.ResolveColumnIndex(keyCol)
	if colIdx < 0 {
		return nil
	}
	col := b.Columns[colIdx]
	if !bb.typed {
		bb.keyType, bb.useIntKey, bb.typed = col.Type, bloomIntKey(col.Type), true
		if !bb.noSample {
			bb.sample = batch.NewVectorLike(col)
		}
	} else if col.Type != bb.keyType {
		return fmt.Errorf("bloom key column %q arrived as %s after %s: one bloom, one encoding",
			keyCol, col.Type.String(), bb.keyType.String())
	}
	bb.resolved = true

	taken := 0
	add := func(row int) {
		if col.Nulls.IsNull(row) {
			return // NULL matches nothing, itself included — see probeKeyHash
		}
		if bb.useIntKey {
			bloomSet(bb.bloom, bb.mask, bloomHashInt(intValFromCol(col, row)))
		} else {
			bb.keyBuf = appendBloomKey(bb.keyBuf[:0], col, row)
			bloomSet(bb.bloom, bb.mask, bloomHashBytes(bb.keyBuf))
		}
		bb.inserted++
		if bb.sample != nil && bb.sample.Len < bloomSelfCheckMaxRows && taken < bloomSelfCheckPerBatch {
			bb.sample.AppendFrom(col, row)
			taken++
		}
	}

	if b.Sel != nil {
		for _, si := range b.Sel {
			add(int(si))
		}
	} else {
		for row := 0; row < b.Len; row++ {
			add(row)
		}
	}
	return nil
}

// FilterOp returns the operator that probes this bloom over the named column,
// carrying the encoding the builder froze and the sample SelfCheck replays.
func (bb *BloomBuilder) FilterOp(column string) *BloomFilterOp {
	op := &BloomFilterOp{
		bloom:      bb.bloom,
		bloomMask:  bb.mask,
		leftKeys:   []string{column},
		useIntKey:  bb.useIntKey,
		keyType:    bb.keyType,
		keyTypeSet: bb.typed,
	}
	if bb.sample != nil && bb.sample.Len > 0 {
		sc := parquet.Column{Name: column, Type: bb.keyType, Nullable: true}
		switch bb.keyType {
		case batch.TypeDecimal:
			sc.Scale = bb.sample.DecimalData.Scale
		case batch.TypeVector:
			sc.Dimension = bb.sample.VectorDim
		}
		op.selfCheck = &batch.RecordBatch{
			Schema:  []parquet.Column{sc},
			Columns: []*batch.Vector{bb.sample},
			Len:     bb.sample.Len,
		}
	}
	return op
}

// Bloom returns the raw bits and mask, for callers that hand a bloom on rather
// than probing it here.
func (bb *BloomBuilder) Bloom() ([]uint64, uint64) { return bb.bloom, bb.mask }

// BloomAddBatch hashes one batch's key column into the bloom. Incremental
// counterpart of BuildBloomFromBatches, used when the source batches stream
// from a spill-backed collector instead of sitting in memory all at once.
//
// Prefer BloomBuilder: it freezes the encoding and retains the sample that
// makes a broken filter loud. This wrapper keeps the free-function shape for
// callers that already own the bits, and shares the builder's one encoder so
// it cannot drift from the probe side again.
func BloomAddBatch(bloom []uint64, bloomMask uint64, b *batch.RecordBatch, keyCol string) {
	bb := &BloomBuilder{bloom: bloom, mask: bloomMask, noSample: true}
	_ = bb.Add(b, keyCol)
}

// BuildBloomFromBatches constructs a bloom filter from a column across
// multiple batches. Returns nil bloom if no rows. Used for reverse bloom
// pushdown: the probe side's join key values filter the build side's scan.
func BuildBloomFromBatches(batches []*batch.RecordBatch, keyCol string) (bloom []uint64, bloomMask uint64) {
	totalRows := 0
	for _, b := range batches {
		totalRows += b.ActiveLen()
	}
	bloom, bloomMask = NewBloomSized(totalRows)
	if bloom == nil {
		return nil, 0
	}
	bb := &BloomBuilder{bloom: bloom, mask: bloomMask, noSample: true}
	for _, b := range batches {
		if err := bb.Add(b, keyCol); err != nil {
			// One column, two types across the batches: no single encoding
			// is right, so answer with no filter rather than a wrong one.
			return nil, 0
		}
	}
	return bloom, bloomMask
}
