// SPDX-License-Identifier: MIT

package physical

import (
	"context"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/optswitch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A scan's READ-AHEAD is admitted by what decoding a row group will hold, not by
// its compressed bytes (#1706, ADR-0006's 2026-11-21 amendment).
//
// A scan's workers decode row groups before the consumer asks for them, and
// the decoded batch is charged AFTER the decode, forced (producer 3). When one
// decoded row group is a large part of the budget, that charge and the
// consumer's own reservation for the batch it has just taken race, and the
// winner decided the verdict: a hash join build refused when the read-ahead's
// charge landed first and answered when it landed second, on the same data at
// the same budget.
//
// The rule, one for the row-group and the whole-file load: a worker may decode
// a row group ahead of its consumer only when the room exists, now, for what
// that decode will hold at its peak — the bytes it still has to load, the
// decoded batch, and the decode's transient. The admission is a decision, not
// a reservation: the decode then takes exactly the charges it always took, so
// admitting it puts nothing on the ledger the base did not (a reservation
// standing in for the decode was measured to refuse a cross join's build, which
// cannot spill, on shapes that answered at the base — 1706b_r1.md). When the
// room does not exist, or the footer cannot bound the decode,
// the worker does not decode it and does not wait: it hands the row group to
// the consumer undecoded, and the consumer decodes it on its own goroutine when
// it asks for it. From that point the scan decodes every remaining row group on
// demand, after the consumer is done with the previous batch. (One admission
// can still see room a consumer has taken a batch for and not yet reserved;
// a further decode is admitted only against room that exists beside the
// first, where the base admitted it on its compressed bytes alone — which is
// what the refusals needed. ADR-0006 records the window.)
//
// The floor is unchanged: a decode the consumer is waiting for is never refused
// and never waits on the budget — it is the same load and the same forced,
// reported decoded charge as before. Without a budget the rule costs nothing:
// every read-ahead is admitted without touching the ledger.

// scanReadAheadAdmission is the kill switch. Off, a worker decodes every row
// group it reaches, and charges it after the decode, as before #1706.
var scanReadAheadAdmission = optswitch.Register("scan-readahead-admission", "WADJET_SCAN_READAHEAD_ADMISSION",
	"admit a scan's read-ahead decode against the decoded bytes it will hold, and decode on the consumer's demand when it does not fit")

// footerDecodeEstimate bounds what decoding row group rg of fr into cols will
// hold, from the footer alone: decoded is the decoded vectors with their null
// bitmaps; transient is the largest single column's working set beside them
// (the decompressed page and the level and value buffers the column decoder
// builds before it copies into the vector). ok=false when the footer cannot
// bound it: a byte-array chunk with a dictionary page, whose decoded bytes are
// the dictionary's entries EXPANDED per row, a size no footer field carries, or
// a type this estimate does not model.
func footerDecodeEstimate(fr *parquet.FileReader, rg int, cols []parquet.Column) (decoded, transient int64, ok bool) {
	meta := fr.RowGroupMeta(rg)
	if meta == nil {
		return 0, 0, false
	}
	rows := meta.NumRows
	chunks := make(map[string]*parquet.ColumnMetaData, len(meta.Columns))
	for i := range meta.Columns {
		md := meta.Columns[i].MetaData
		if md == nil || len(md.PathInSchema) == 0 {
			return 0, 0, false
		}
		chunks[strings.ToLower(md.PathInSchema[0])] = md
	}
	bitmap := ((rows + 63) / 64) * 8
	perCol := make([]int64, 0, len(cols))
	defer func() {
		// The native decoder decodes up to GOMAXPROCS columns AT ONCE
		// (scan.readRowGroupNative), so that many columns' working sets can
		// be live together: the largest of them, summed.
		sort.Slice(perCol, func(i, j int) bool { return perCol[i] > perCol[j] })
		for i := 0; i < len(perCol) && i < runtime.GOMAXPROCS(0); i++ {
			transient += perCol[i]
		}
	}()
	for _, c := range cols {
		md := chunks[strings.ToLower(c.Name)]
		var unc int64
		if md != nil {
			unc = md.TotalUncompressedSize
		}
		// Measured (1706b_r1.md, the peak table): every flat type's decode
		// allocates at most about 3x its chunk's uncompressed bytes plus a
		// per-row level buffer beside its vector.
		perCol = append(perCol, 3*unc+16*rows)
		var width int64
		switch c.Type {
		case parquet.TypeBool:
			width = 1
		case parquet.TypeInt32, parquet.TypeDate, parquet.TypePort, parquet.TypeProtocol, parquet.TypeFloat32:
			width = 4
		case parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeFloat64, parquet.TypeIPv4,
			parquet.TypeMAC, parquet.TypeDuration:
			width = 8
		case parquet.TypeDecimal:
			width = 16
		case parquet.TypeString, parquet.TypeBytes, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeUUID:
			if md != nil && md.DictionaryPageOffset > 0 {
				return 0, 0, false
			}
			// Offsets, then the value bytes, which a PLAIN chunk carries
			// uncompressed with a length prefix per value, so its uncompressed
			// size bounds them from above.
			decoded += bitmap + (rows+1)*4 + unc
			continue
		default:
			return 0, 0, false
		}
		decoded += bitmap + rows*width
	}
	return decoded, transient, true
}

// readAheadCost is what admitting row group unitIdx's decode ahead of the
// consumer must find room for: hold is the decode's own working set (the
// decoded batch and its transient); load is the bytes the decode still has to
// land. ok=false when the footer cannot bound it.
func (inner *scanSourceInner) readAheadCost(unitIdx int) (hold, load int64, ok bool) {
	unit := inner.rgUnits[unitIdx]
	slot := unit.slot
	if slot == nil {
		return 0, 0, false
	}
	slot.mu.Lock()
	loaded, rdr, rgs, staged := slot.loaded, slot.nativeReader, slot.rgs, slot.stagedFile != nil
	slot.mu.Unlock()
	if !loaded {
		rdr = parquet.LookupFooter(footerCacheIdentity(inner.cat, slot.entry, slot.entry.SizeBytes))
	}
	if rdr == nil {
		return 0, 0, false
	}
	decoded, transient, ok := footerDecodeEstimate(rdr, unit.rgIndex, inner.readSchema())
	if !ok {
		return 0, 0, false
	}
	rgBytes := int64(-1)
	if meta := rdr.RowGroupMeta(unit.rgIndex); meta != nil {
		rgBytes = 0
		for i := range meta.Columns {
			if md := meta.Columns[i].MetaData; md != nil {
				rgBytes += md.TotalCompressedSize
			}
		}
	}
	switch {
	case !loaded && scanRowGroupBuffers.On() && rgBytes >= 0:
		load = rgBytes // the row-group load lands this row group
	case !loaded:
		load = slot.entry.SizeBytes // the whole-file load lands the file
	case rgs != nil && rgBytes >= 0:
		load = rgBytes
	case staged:
		load = 0 // staged pread holds no buffer on the ledger
	default:
		load = 0 // the whole file is already landed and charged
	}
	return decoded + transient, load, true
}

// admitReadAhead decides whether a worker may decode row group unitIdx before
// its consumer asks for it: only when the ledger has room, now, for the decode's
// whole peak. It is a decision and not a reservation — the decode's charges are
// the ones it always took (the load's, then the decoded batch's), so an
// admitted read-ahead holds nothing on the ledger that the base did not, and a
// consumer that cannot spill (a cross join's build) is never refused by a
// reservation standing in for a decode. On refusal the scan switches to
// decoding on demand for the rest of its life.
func (inner *scanSourceInner) admitReadAhead(unitIdx int) bool {
	t := inner.memTracker
	if t == nil || t.Budget() <= 0 || !scanReadAheadAdmission.On() {
		return true
	}
	if inner.onDemand.Load() {
		return false
	}
	hold, load, known := inner.readAheadCost(unitIdx)
	if !known || t.Used()+hold+load > t.Budget() {
		inner.onDemand.Store(true)
		return false
	}
	return true
}

// decodeOrDefer is a worker's step for one row group: decode it, when its
// read-ahead is admitted, or report that the consumer must decode it.
func (inner *scanSourceInner) decodeOrDefer(ctx context.Context, unitIdx int) (b *batch.RecordBatch, deferred bool) {
	inner.awaitConsumerFirst(ctx)
	if !inner.admitReadAhead(unitIdx) {
		return nil, true
	}
	return inner.readRG(ctx, unitIdx), false
}

// --- test-only ordering hook ---

// Read-ahead orders a test can force. The two events whose order decided
// #1706's verdict are a scan's read-ahead decoded charge and a grace build's
// reservation for the batch it has just taken.
const (
	ReadAheadOrderFree          int32 = iota
	ReadAheadOrderDecodeFirst         // every read-ahead charge lands before each build reservation
	ReadAheadOrderConsumerFirst       // a worker decodes only what its consumer is asking for
)

var (
	readAheadOrder atomic.Int32
	// orderedScans holds the row-group scans alive while DecodeFirst is armed.
	orderedScans sync.Map // *scanSourceInner -> struct{}
)

// ForceScanReadAheadOrder fixes the order of scans' read-ahead decodes against
// a grace build's reservations and returns the restore. TEST-ONLY, and it
// orders the events with no sleep:
//
//   - DecodeFirst: before EVERY arrival (and arrival-chunk) reservation of a
//     grace build, wait until every live scan's workers have run ahead as far
//     as they can — each parked on a full batchCh with what it decoded or
//     deferred, or exited. Every read-ahead charge then lands before the
//     build's reservation, the order that refused #1706's statement.
//   - ConsumerFirst: a worker starts each decode only when its consumer is
//     asking for an item the workers have not sent, so the consumer's work on
//     the batch it took, reservation included, is done first.
func ForceScanReadAheadOrder(order int32) func() {
	prev := readAheadOrder.Swap(order)
	var restoreHook func()
	if order == ReadAheadOrderDecodeFirst {
		restoreHook = exec.SetArrivalReserveHook(awaitReadAheadParked)
	}
	return func() {
		if restoreHook != nil {
			restoreHook()
		}
		readAheadOrder.Store(prev)
	}
}

// registerOrderedScan / unregisterOrderedScan track the scans the DecodeFirst
// hook waits on. No-ops unless it is armed.
func registerOrderedScan(inner *scanSourceInner) {
	if readAheadOrder.Load() == ReadAheadOrderDecodeFirst {
		orderedScans.Store(inner, struct{}{})
	}
}

func unregisterOrderedScan(inner *scanSourceInner) { orderedScans.Delete(inner) }

// awaitReadAheadParked waits until every registered scan's workers are parked
// on a full batchCh or have exited.
func awaitReadAheadParked() {
	for {
		settled := true
		orderedScans.Range(func(k, _ any) bool {
			inner := k.(*scanSourceInner)
			if inner.workersDone.Load() {
				return true
			}
			live := inner.liveWorkers.Load()
			if live > 0 && inner.sending.Load() >= live && len(inner.batchCh) == cap(inner.batchCh) {
				return true
			}
			settled = false
			return false
		})
		if settled {
			return
		}
		hookYield()
	}
}

// awaitConsumerFirst holds a worker's next decode until the consumer is asking
// for an item the workers have not sent yet.
func (inner *scanSourceInner) awaitConsumerFirst(ctx context.Context) {
	if readAheadOrder.Load() != ReadAheadOrderConsumerFirst {
		return
	}
	for inner.demands.Load() <= inner.sent.Load() && ctx.Err() == nil {
		hookYield()
	}
}

// hookYield gives the goroutines the ordering hooks wait on the processor.
// Sleep(0) returns at once; it is there so a GOMAXPROCS=1 run reschedules.
func hookYield() {
	runtime.Gosched()
	time.Sleep(0)
}
