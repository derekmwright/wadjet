package worker

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/wshf"
)

// These are the obligation tests (ADR-0019): a boundary is only sound if it
// discharges everything the dying goroutine owed — queued work, unclosed
// channels, held locks, ledger charges — not just the unit in flight. Each
// one below failed on the first cut of the boundary, and each failure was a
// hang, a silent truncation or a slow starve rather than a crash, which is
// why they are permanent and unguarded.

type panicAfterReader struct {
	src     *bytes.Reader
	served  int
	panicAt int
	piece   int
}

func (p *panicAfterReader) Read(b []byte) (int, error) {
	if p.served >= p.panicAt {
		panic("injected: transport read blew up")
	}
	if len(b) > p.piece {
		b = b[:p.piece]
	}
	n, err := p.src.Read(b)
	p.served += n
	return n, err
}

func (p *panicAfterReader) Close() error { return nil }

// TestDecodeAheadScannerPanicSurfacesAsError: the decode-ahead scanner owns
// `defer close(d.delivery)`. A boundary registered OUTSIDE it runs after that
// close, so the recovery's own fail() send lands on a closed channel — an
// unrecoverable panic raised from inside the deferred recovery, which is the
// one thing a boundary must never do.
//
// It must also not "fix" that by staying silent: the consumer would then read
// a clean EOF after however many chunks happened to be staged, and a
// truncated shuffle stream is a wrong answer, not a failure.
func TestDecodeAheadScannerPanicSurfacesAsError(t *testing.T) {
	wire := buildMultiTypeWSHF(t, 42, 16, 257)
	codec, _ := wshf.CodecForMagic([4]byte{wire[0], wire[1], wire[2], wire[3]})
	body := &panicAfterReader{src: bytes.NewReader(wire[4:]), panicAt: 100 << 10, piece: 32}
	r, err := newStreamingShuffleReader(io.ReadCloser(body), codec)
	if err != nil {
		t.Fatalf("newStreamingShuffleReader: %v", err)
	}
	r.startDecodeAhead(4, nil, nil, false, nil)
	if r.da == nil {
		t.Fatalf("decode-ahead did not engage")
	}
	defer r.Close()

	type outcome struct {
		batches int
		err     error
	}
	done := make(chan outcome, 1)
	go func() {
		n := 0
		for {
			b, err := r.Next()
			if err != nil {
				done <- outcome{n, err}
				return
			}
			if b == nil {
				done <- outcome{n, nil}
				return
			}
			n++
		}
	}()

	select {
	case got := <-done:
		if got.err == nil {
			t.Fatalf("SILENT TRUNCATION: the scanner panicked but Next reported clean EOF "+
				"after %d of 16 chunks", got.batches)
		}
		if !strings.Contains(got.err.Error(), "transport read blew up") {
			t.Errorf("error %q lost the panic value", got.err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("HANG: Next never returned after the scanner panicked")
	}
}

type panicOnGetStore struct{ objstore.Store }

func (p *panicOnGetStore) Get(context.Context, string, string) (io.ReadCloser, objstore.ObjectInfo, error) {
	panic("injected: store Get blew up")
}

// TestPrefetcherPanicResolvesEveryQueuedIndex: the jobs channel is pre-filled
// with every file index and the worker pool is the only thing draining it. A
// boundary that resolves just the index in flight and lets the goroutine exit
// abandons the rest — once all scanPrefetchConcurrency workers have died that
// way, take() blocks forever on slots nobody owns.
func TestPrefetcherPanicResolvesEveryQueuedIndex(t *testing.T) {
	ctx := context.Background()
	mem := objstore.NewMemStore()
	const bucket = "test"
	keys, _ := writePrefetchFixture(t, mem, bucket, 9, 10)

	e := &Executor{store: &panicOnGetStore{Store: mem}, spillDir: t.TempDir()}
	s := newCachedFileStreamSource(e, "", bucket, keys)
	p := startFilePrefetcher(ctx, s)
	defer p.Close()

	for idx := 0; idx < len(keys); idx++ {
		got := make(chan *prefetchResult, 1)
		go func() { got <- p.take(ctx, idx) }()
		select {
		case res := <-got:
			if res.err == nil && !res.skipped {
				t.Errorf("take(%d) resolved with neither an error nor a skip", idx)
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("HANG: take(%d) never resolved — the prefetch goroutines died on "+
				"earlier indices and abandoned the rest of the jobs channel", idx)
		}
	}
}

type panicOnReadBody struct{}

func (panicOnReadBody) Read([]byte) (int, error) { panic("injected: transport body blew up") }
func (panicOnReadBody) Close() error             { return nil }

type panicBodyStore struct {
	objstore.Store
	size int64
}

func (p *panicBodyStore) Get(_ context.Context, _, key string) (io.ReadCloser, objstore.ObjectInfo, error) {
	return panicOnReadBody{}, objstore.ObjectInfo{Key: key, Size: p.size}, nil
}

// TestCachedStoreReleasesLedgerOnPanic: CachedStore.readFully charges the
// WORKER-LIFETIME memory tracker before reading. A panic unwinding past the
// release used to be harmless only because the process died with the ledger.
// Now the worker survives, so the charge is permanent and every LATER query
// on that worker runs with a smaller budget — a slow starve instead of a
// crash, and one nothing in the engine would ever attribute back to here.
func TestCachedStoreReleasesLedgerOnPanic(t *testing.T) {
	const size = 64 << 20
	tracker := memory.NewTracker("worker", 1<<30)
	cs := NewCachedStore(&panicBodyStore{Store: objstore.NewMemStore(), size: size},
		NewLRUCache(1<<30), tracker)

	before := tracker.Used()
	var recovered error
	func() {
		defer exec.CatchQueryPanic(context.Background(), "test", func(err error) { recovered = err })
		_, _, _ = cs.Get(context.Background(), "b", "k")
	}()
	if recovered == nil {
		t.Fatal("the injected panic did not reach the boundary")
	}
	if after := tracker.Used(); after != before {
		t.Fatalf("LEDGER LEAK: worker tracker Used() %d -> %d (%d bytes charged and never "+
			"released) after a recovered panic", before, after, after-before)
	}
}
