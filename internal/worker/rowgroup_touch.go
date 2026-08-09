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

// Engagement markers beside readaheadAdviseBytes on the drop-behind
// stats line: bytes actually walked by touchers, and ranges dropped
// because a toucher's queue was full (WILLNEED-only fallback — decode
// may fault those itself, exactly the pre-toucher behavior).
var (
	touchAheadBytes atomic.Int64
	touchAheadDrops atomic.Int64
)

// rangeToucher pages-in advise ranges over one scan mmap.
type rangeToucher struct {
	data  []byte
	ch    chan [2]int64 // [off, end) — clamped, off page-aligned
	stopc chan struct{}
	done  chan struct{}
}

func newRangeToucher(data []byte) *rangeToucher {
	t := &rangeToucher{
		data: data,
		// One advise call is one column chunk; a full assignment wave is
		// groups x projected leaves of them. 1024 absorbs several waves;
		// overflow degrades to WILLNEED-only, counted, never blocking.
		ch:    make(chan [2]int64, 1024),
		stopc: make(chan struct{}),
		done:  make(chan struct{}),
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
			for ; off < end; off += pg {
				// A stop mid-range must win promptly: a range can span
				// hundreds of cold pages (~ms each under load) and munmap
				// is waiting behind us.
				select {
				case <-t.stopc:
					return
				default:
				}
				sink += t.data[off]
			}
			touchAheadBytes.Add(end - r[0])
		}
	}
}

// touchSink defeats dead-code elimination of the touch loads.
var touchSink atomic.Int32
