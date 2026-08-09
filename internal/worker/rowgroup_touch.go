package worker

import (
	"os"
	"sync/atomic"
)

// Row-group touch-ahead: the residency guarantee MADV_WILLNEED cannot
// make. The 2026-08-08 SF10 capped repro measured the steady regime
// re-faulting synchronously inside token-holding decode spans DESPITE
// the I/O-ahead advises (run-2 decode +23% ns/byte, token stalls 14x,
// majflt climbing all run) at read rates far below the device ceiling:
// under a saturated page-cache LRU the kernel throttles or skips
// advisory readahead, and advised pages can be evicted again before
// decode reaches them. The toucher is a per-mmap goroutine that
// consumes the same advise ranges and physically faults the pages in
// (one byte read per page) — it cannot be throttled away, and it is
// deliberately outside the CPU-token budget because page-fault wait is
// I/O, not compute. Decode workers then hold tokens for decode alone.
// WILLNEED is still issued first for I/O overlap; the toucher rides
// behind it and usually finds the pages already arriving.
//
// Lifecycle contract: enqueue only from decode workers (joined by
// iter.Close), stop() before munmap — same ordering the Advise seam
// documents. stop() abandons queued ranges immediately; a dying scan
// must not wait out a fault backlog.
//
// WADJET_ROWGROUP_TOUCH=0 is the kill switch (cap-wrapper forwards it);
// WADJET_ROWGROUP_READAHEAD=0 disables the whole advise seam including
// this.
var rowGroupTouchEnabled = os.Getenv("WADJET_ROWGROUP_TOUCH") != "0"

// Batched population: MADV_POPULATE_READ faults a whole range in one
// syscall at device streaming speed instead of one 4 KiB fault per
// round-trip. The 2026-08-09 SF100 cache-on re-profile measured the
// per-page walk as the steady-regime scan ceiling: 81-140 MB/s per
// worker from an NVMe capable of multi-GB/s, with decode workers
// faulting inline under held CPU tokens once the toucher fell behind
// (docs/benchmarks/cache-on-reprofile-2026-08-09.md). Ranges are
// populated in bounded chunks so stop() keeps its promptness contract.
// A toucher that sees madvise fail (pre-5.14 kernel, or a non-mmap
// backing slice as in tests) falls back to the byte walk permanently —
// per toucher, since the failure is a property of its mapping.
// WADJET_TOUCH_POPULATE=0 forces the byte walk (same-binary A/B lever).
var touchPopulateEnabled = os.Getenv("WADJET_TOUCH_POPULATE") != "0"

// touchPopulateChunk bounds one populate syscall. 8 MiB ≈ single-digit
// milliseconds at NVMe streaming rates, so a pending stop() (munmap is
// waiting) is honored promptly between chunks.
const touchPopulateChunk = 8 << 20

// Engagement markers beside readaheadAdviseBytes on the drop-behind
// stats line: bytes actually walked by touchers, and ranges dropped
// because a toucher's queue was full (WILLNEED-only fallback — decode
// may fault those itself, exactly the pre-toucher behavior).
var (
	touchAheadBytes atomic.Int64
	touchAheadDrops atomic.Int64
	// touchPopulateBytes is the subset of touchAheadBytes paged in via
	// MADV_POPULATE_READ — engagement marker for the batched path.
	touchPopulateBytes atomic.Int64
)

// rangeToucher pages-in advise ranges over one scan mmap.
type rangeToucher struct {
	data  []byte
	ch    chan [2]int64 // [off, end) — clamped, off page-aligned
	stopc chan struct{}
	done  chan struct{}
	// populate flips false on the first madvise failure; the toucher
	// then byte-walks for its remaining lifetime.
	populate bool
}

func newRangeToucher(data []byte) *rangeToucher {
	t := &rangeToucher{
		data: data,
		// One advise call is one column chunk; a full assignment wave is
		// groups x projected leaves of them. 1024 absorbs several waves;
		// overflow degrades to WILLNEED-only, counted, never blocking.
		ch:       make(chan [2]int64, 1024),
		stopc:    make(chan struct{}),
		done:     make(chan struct{}),
		populate: touchPopulateEnabled,
	}
	go t.loop()
	return t
}

// enqueue clamps and queues one file-relative range. Never blocks: the
// toucher is an accelerator, not a dependency.
func (t *rangeToucher) enqueue(off, n int64) {
	if off < 0 || n <= 0 || off >= int64(len(t.data)) {
		return
	}
	end := min(off+n, int64(len(t.data)))
	off &^= int64(os.Getpagesize()) - 1
	select {
	case t.ch <- [2]int64{off, end}:
	default:
		touchAheadDrops.Add(1)
	}
}

// stop halts the toucher and waits for it to exit, abandoning any
// queued ranges. The mmap may be unmapped as soon as stop returns.
func (t *rangeToucher) stop() {
	if t == nil {
		return
	}
	close(t.stopc)
	<-t.done
}

func (t *rangeToucher) loop() {
	defer close(t.done)
	pg := int64(os.Getpagesize())
	var sink byte
	defer func() { touchSink.Store(int32(sink)) }() // keep the loads observable
	for {
		select {
		case <-t.stopc:
			return
		case r := <-t.ch:
			off, end := r[0], r[1]
			// off is page-aligned (enqueue) and the chunk size is a page
			// multiple, so every chunk boundary except end stays aligned.
			for off < end {
				// A stop mid-chunk must win promptly: a range can span
				// hundreds of cold pages (or several populate chunks)
				// and munmap is waiting behind us.
				select {
				case <-t.stopc:
					return
				default:
				}
				chunk := min(off+touchPopulateChunk, end)
				n := chunk - off
				if t.populate {
					if madvisePopulateRead(t.data[off:chunk]) == nil {
						touchPopulateBytes.Add(n)
						touchAheadBytes.Add(n)
						off = chunk
						continue
					}
					t.populate = false
				}
				for ; off < chunk; off += pg {
					select {
					case <-t.stopc:
						return
					default:
					}
					sink += t.data[off]
				}
				touchAheadBytes.Add(n)
			}
		}
	}
}

// touchSink defeats dead-code elimination of the touch loads.
var touchSink atomic.Int32
