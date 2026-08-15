package worker

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
)

// buildIndexedWSHF is buildMultiTypeWSHF plus the WIDX footer the file
// sinks append at close: same rng stream through the same writer, with the
// writer kept in hand so writeFooter sees its recorded chunk offsets.
func buildIndexedWSHF(tb testing.TB, seed int64, nChunks, rowsPerChunk int) []byte {
	tb.Helper()
	rng := rand.New(rand.NewSource(seed))
	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, multiTypeSchema)
	if err := sw.writeHeader(); err != nil {
		tb.Fatalf("writeHeader: %v", err)
	}
	for c := 0; c < nChunks; c++ {
		b := batch.NewRecordBatch(multiTypeSchema, rowsPerChunk)
		for i := 0; i < rowsPerChunk; i++ {
			if rng.Intn(8) == 0 {
				continue
			}
			b.Columns[0].Nulls.SetValid(i)
			b.Columns[0].Int64Data[i] = rng.Int63()
			b.Columns[1].Nulls.SetValid(i)
			b.Columns[1].Int32Data[i] = int32(rng.Int31())
			b.Columns[2].Nulls.SetValid(i)
			b.Columns[2].Float64Data[i] = rng.Float64()
			b.Columns[3].Nulls.SetValid(i)
			b.Columns[3].Float32Data[i] = rng.Float32()
			b.Columns[4].Nulls.SetValid(i)
			b.Columns[4].BoolData[i] = rng.Intn(2) == 1
			b.Columns[5].Nulls.SetValid(i)
			s := fmt.Sprintf("row-%d-%x", i, rng.Int63())
			if rng.Intn(10) == 0 {
				s = ""
			}
			b.Columns[5].BytesData.Set(i, []byte(s))
			b.Columns[6].Nulls.SetValid(i)
			b.Columns[6].DecimalData.Data[i] = batch.Int128{Lo: rng.Uint64(), Hi: rng.Int63() - rng.Int63()}
		}
		if err := sw.writeChunk(b.Columns, nil, rowsPerChunk); err != nil {
			tb.Fatalf("writeChunk %d: %v", c, err)
		}
	}
	if err := sw.writeFooter(); err != nil {
		tb.Fatalf("writeFooter: %v", err)
	}
	out := buf.Bytes()
	binary.LittleEndian.PutUint32(out[4:], sw.numChunks)
	return out
}

// headerEndOf computes the absolute offset where chunk data begins.
func headerEndOf(tb testing.TB, wire []byte) int64 {
	tb.Helper()
	return openStreaming(tb, wire).headerEnd
}

// openIndexedFile writes wire to a temp file and opens it the way
// openShuffleFromFileStreaming does: magic consumed, reader constructed,
// extent index offered.
func openIndexedFile(tb testing.TB, wire []byte, owned bool) (*streamingShuffleReader, *os.File) {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), "chunk.wshf")
	if err := os.WriteFile(path, wire, 0o644); err != nil {
		tb.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		tb.Fatal(err)
	}
	var magic [4]byte
	if _, err := io.ReadFull(f, magic[:]); err != nil {
		tb.Fatal(err)
	}
	if magic != shuffleMagic {
		tb.Fatalf("bad magic %q", magic[:])
	}
	r, err := newStreamingShuffleReader(f, codecNone)
	if err != nil {
		tb.Fatalf("newStreamingShuffleReader: %v", err)
	}
	r.tryEnableExtentIndex(f, int64(len(wire)), owned)
	return r, f
}

// TestShuffleDecodeAhead_WorkerCeilingOverride pins the WADJET_SHUFFLE_
// DA_WORKERS override seam: the package var feeds startDecodeAhead's
// default and the GOMAXPROCS cap still binds above it.
func TestShuffleDecodeAhead_WorkerCeilingOverride(t *testing.T) {
	orig := shuffleDecodeAheadWorkers
	defer func() { shuffleDecodeAheadWorkers = orig }()

	shuffleDecodeAheadWorkers = 2
	wire := buildMultiTypeWSHF(t, 101, 12, 64)
	want := drain(t, openStreaming(t, wire).Next)
	r := openDecodeAhead(t, wire, 0, newCPUTokens(4), nil, false, nil)
	got := drain(t, r.Next)
	requireBatchesEqual(t, want, got)
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// An override above GOMAXPROCS is capped, not honored blindly — the
	// parity drain must still hold.
	shuffleDecodeAheadWorkers = 4096
	r = openDecodeAhead(t, wire, 0, newCPUTokens(4), nil, false, nil)
	got = drain(t, r.Next)
	requireBatchesEqual(t, want, got)
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestShuffleExtentIndex_FooterRoundTrip pins the WIDX layout: the footer
// parses back to offsets that reconstruct every chunk exactly (row counts,
// extents validating under the stage walk's checks), anchored at the
// header end and ending at the table.
func TestShuffleExtentIndex_FooterRoundTrip(t *testing.T) {
	wire := buildIndexedWSHF(t, 61, 12, 100)
	plain := buildMultiTypeWSHF(t, 61, 12, 100)
	if !bytes.Equal(wire[:len(plain)], plain) {
		t.Fatal("footer is not a pure append")
	}
	hdrEnd := headerEndOf(t, wire)
	offs := parseShuffleExtentIndex(bytes.NewReader(wire), int64(len(wire)), 12, hdrEnd)
	if offs == nil {
		t.Fatal("valid footer did not parse")
	}
	if len(offs) != 13 {
		t.Fatalf("offsets = %d, want 13", len(offs))
	}
	if offs[0] != hdrEnd {
		t.Fatalf("offs[0] = %d, want header end %d", offs[0], hdrEnd)
	}
	if offs[12] != int64(len(plain)) {
		t.Fatalf("offs[N] = %d, want end of last chunk %d", offs[12], len(plain))
	}
	r := openStreaming(t, wire)
	for i := 0; i < 12; i++ {
		numRows := int(binary.LittleEndian.Uint32(wire[offs[i]:]))
		if numRows != 100 {
			t.Fatalf("chunk %d rows = %d, want 100", i, numRows)
		}
		if err := validateShuffleChunkBytes(r.schema, numRows, wire[offs[i]+4:offs[i+1]]); err != nil {
			t.Fatalf("chunk %d extent invalid: %v", i, err)
		}
	}
}

// TestShuffleExtentIndex_InvalidFootersFallBack: every corruption class
// must parse to nil (walk mode), never to a wrong index.
func TestShuffleExtentIndex_InvalidFootersFallBack(t *testing.T) {
	wire := buildIndexedWSHF(t, 67, 8, 64)
	hdrEnd := headerEndOf(t, wire)
	parse := func(w []byte) []int64 {
		return parseShuffleExtentIndex(bytes.NewReader(w), int64(len(w)), 8, hdrEnd)
	}
	if parse(wire) == nil {
		t.Fatal("valid footer did not parse")
	}

	mutate := func(name string, f func(w []byte) []byte) {
		w := f(append([]byte(nil), wire...))
		if parse(w) != nil {
			t.Fatalf("%s: corrupt footer parsed", name)
		}
	}
	mutate("bad magic", func(w []byte) []byte { w[len(w)-1] ^= 0xff; return w })
	mutate("bad version", func(w []byte) []byte { w[len(w)-5] = 99; return w })
	mutate("truncated", func(w []byte) []byte { return w[:len(w)-7] })
	mutate("bad tableOff", func(w []byte) []byte {
		binary.LittleEndian.PutUint64(w[len(w)-13:], 12345)
		return w
	})
	mutate("bad count", func(w []byte) []byte {
		binary.LittleEndian.PutUint32(w[len(w)-17:], 9)
		return w
	})
	mutate("non-monotonic offsets", func(w []byte) []byte {
		tableOff := int64(binary.LittleEndian.Uint64(w[len(w)-13:]))
		binary.LittleEndian.PutUint64(w[tableOff+8:], binary.LittleEndian.Uint64(w[tableOff:]))
		return w
	})
	mutate("first offset off-anchor", func(w []byte) []byte {
		tableOff := int64(binary.LittleEndian.Uint64(w[len(w)-13:]))
		binary.LittleEndian.PutUint64(w[tableOff:], uint64(hdrEnd+1))
		return w
	})

	// Footer-less wire of the exact same chunks: nil, silently.
	plain := buildMultiTypeWSHF(t, 67, 8, 64)
	if parse(plain) != nil {
		t.Fatal("footer-less wire parsed an index")
	}

	// Read kill switch: a valid footer is ignored.
	orig := shuffleIndexRead
	shuffleIndexRead = false
	if parse(wire) != nil {
		t.Fatal("footer parsed with WADJET_SHUFFLE_INDEX_READ=0")
	}
	shuffleIndexRead = orig
}

// TestShuffleExtentIndex_WriteKillSwitch: WADJET_SHUFFLE_INDEX=0 emits no
// footer — the file is byte-identical to the pre-index format.
func TestShuffleExtentIndex_WriteKillSwitch(t *testing.T) {
	orig := shuffleIndexWrite
	shuffleIndexWrite = false
	defer func() { shuffleIndexWrite = orig }()
	wire := buildIndexedWSHF(t, 71, 6, 32)
	plain := buildMultiTypeWSHF(t, 71, 6, 32)
	if !bytes.Equal(wire, plain) {
		t.Fatalf("kill-switch wire differs from pre-index format (%d vs %d bytes)", len(wire), len(plain))
	}
}

// TestShuffleDecodeAhead_IndexedMatchesSerial is the index-mode parity
// gate: identical batches to the serial reader over the footer-less wire,
// zero scanner staging, engagement visible in the markers.
func TestShuffleDecodeAhead_IndexedMatchesSerial(t *testing.T) {
	wire := buildIndexedWSHF(t, 73, 24, 150)
	want := drain(t, openStreaming(t, buildMultiTypeWSHF(t, 73, 24, 150)).Next)

	var stats shuffleDecodeAheadStats
	r, _ := openIndexedFile(t, wire, false)
	r.startDecodeAhead(4, newCPUTokens(4), nil, false, &stats)
	if r.da == nil {
		t.Fatal("decode-ahead did not engage")
	}
	if r.da.idx == nil {
		t.Fatal("index mode did not engage on a valid footer")
	}
	got := drain(t, r.Next)
	requireBatchesEqual(t, want, got)
	if r.Delivered() != 24 {
		t.Fatalf("Delivered = %d, want 24", r.Delivered())
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if stats.indexedFiles.Load() != 1 {
		t.Fatalf("indexedFiles = %d, want 1", stats.indexedFiles.Load())
	}
	if stats.stageNs.Load() != 0 {
		t.Fatalf("stageNs = %d, want 0 (index mode must not stage)", stats.stageNs.Load())
	}
	if stats.preadNs.Load() == 0 {
		t.Fatal("preadNs = 0, want > 0")
	}
	if stats.chunks.Load() != 24 {
		t.Fatalf("chunks = %d, want 24", stats.chunks.Load())
	}
}

// TestShuffleDecodeAhead_IndexedEmptyChunk: a bare zero row-count word
// (the empty-chunk wire encoding) rides the index as a 4-byte extent and
// is skipped exactly like the serial reader skips it.
func TestShuffleDecodeAhead_IndexedEmptyChunk(t *testing.T) {
	base := buildIndexedWSHF(t, 79, 8, 64)
	hdrEnd := headerEndOf(t, base)
	offs := parseShuffleExtentIndex(bytes.NewReader(base), int64(len(base)), 8, hdrEnd)
	if offs == nil {
		t.Fatal("base footer did not parse")
	}
	chunksEnd := offs[8]

	// Splice a bare row-count word after the last chunk and rebuild the
	// footer around it: 9 chunks, the last a 4-byte extent.
	var w bytes.Buffer
	w.Write(base[:chunksEnd])
	w.Write([]byte{0, 0, 0, 0})
	newOffs := append(append([]int64(nil), offs[:8]...), chunksEnd)
	var scratch [13]byte
	for _, off := range newOffs {
		binary.LittleEndian.PutUint64(scratch[:8], uint64(off))
		w.Write(scratch[:8])
	}
	binary.LittleEndian.PutUint32(scratch[:4], 9)
	w.Write(scratch[:4])
	binary.LittleEndian.PutUint64(scratch[:8], uint64(chunksEnd+4))
	scratch[8] = shuffleIndexVersion
	copy(scratch[9:], shuffleIndexMagic[:])
	w.Write(scratch[:13])
	wire := w.Bytes()
	binary.LittleEndian.PutUint32(wire[4:], 9)

	want := drain(t, openStreaming(t, buildMultiTypeWSHF(t, 79, 8, 64)).Next)
	r, _ := openIndexedFile(t, wire, false)
	r.startDecodeAhead(2, newCPUTokens(2), nil, false, nil)
	if r.da == nil || r.da.idx == nil {
		t.Fatal("index mode did not engage")
	}
	got := drain(t, r.Next)
	requireBatchesEqual(t, want, got)
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestShuffleDecodeAhead_IndexedTruncationFallsBack: truncation destroys
// the trailer, so the reader lands on the walk path and surfaces the walk
// path's truncation error at the walk path's position — never a footer
// complaint, never silent EOF.
func TestShuffleDecodeAhead_IndexedTruncationFallsBack(t *testing.T) {
	wire := buildIndexedWSHF(t, 83, 10, 80)
	plain := buildMultiTypeWSHF(t, 83, 10, 80)
	cut := len(plain) * 2 / 3
	truncated := wire[:cut]

	var stats shuffleDecodeAheadStats
	r, _ := openIndexedFile(t, truncated, false)
	if r.idx != nil {
		t.Fatal("truncated file produced a valid index")
	}
	r.startDecodeAhead(2, newCPUTokens(2), nil, false, &stats)
	if r.da == nil {
		t.Fatal("decode-ahead did not engage")
	}
	var daErr error
	for {
		b, err := r.Next()
		if err != nil {
			daErr = err
			break
		}
		if b == nil {
			t.Fatal("truncated file drained without error")
		}
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if stats.indexedFiles.Load() != 0 {
		t.Fatal("truncated file engaged index mode")
	}

	// Serial reader over the same truncated bytes: same error class at the
	// same chunk position.
	sr := openStreaming(t, truncated)
	var serialErr error
	for {
		b, err := sr.Next()
		if err != nil {
			serialErr = err
			break
		}
		if b == nil {
			t.Fatal("serial truncated drain without error")
		}
	}
	if !strings.Contains(daErr.Error(), "truncated") || !strings.Contains(serialErr.Error(), "truncated") {
		t.Fatalf("errors not truncation-class: da=%v serial=%v", daErr, serialErr)
	}
	daPos := strings.SplitAfter(daErr.Error(), "chunk ")[1][:4]
	serialPos := strings.SplitAfter(serialErr.Error(), "chunk ")[1][:4]
	if daPos != serialPos {
		t.Fatalf("truncation position diverged: da at %q, serial at %q", daPos, serialPos)
	}
}

// TestShuffleDecodeAhead_IndexedTokensBalance: index mode returns every
// token, both on a clean drain and on early close with queued extents.
func TestShuffleDecodeAhead_IndexedTokensBalance(t *testing.T) {
	wire := buildIndexedWSHF(t, 89, 20, 100)

	tokens := newCPUTokens(3)
	r, _ := openIndexedFile(t, wire, false)
	r.startDecodeAhead(3, tokens, nil, false, nil)
	_ = drain(t, r.Next)
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := tokens.InUse(); got != 0 {
		t.Fatalf("tokens leaked after drain: %d", got)
	}

	tokens = newCPUTokens(3)
	r, _ = openIndexedFile(t, wire, false)
	r.startDecodeAhead(3, tokens, nil, false, nil)
	if b, err := r.Next(); err != nil || b == nil {
		t.Fatalf("first Next: %v %v", b, err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("early Close: %v", err)
	}
	if got := tokens.InUse(); got != 0 {
		t.Fatalf("tokens leaked after early close: %d", got)
	}
}

// TestShuffleExtentIndex_EndToEndMemStore runs the production seam: an
// indexed WSHF file compressed into the upload envelope, staged through
// the MemStore-backed cachedFileStreamSource (which decompresses to a
// local temp), consumed via decode-ahead. The footer must survive the
// round trip and engage index mode; rows must match.
func TestShuffleExtentIndex_EndToEndMemStore(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	const bucket = "test"
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatal(err)
	}
	wire := buildIndexedWSHF(t, 97, 30, 120)
	want := drain(t, openStreaming(t, buildMultiTypeWSHF(t, 97, 30, 120)).Next)
	// Force the compressed envelope (CompressShuffleData's ≥10% heuristic
	// may skip random payloads) so the staging DECOMPRESSION path is the
	// one that must carry the footer through verbatim.
	var cb bytes.Buffer
	cb.Write(compressedMagic[:])
	s2w := acquireS2Writer(&cb)
	if _, err := s2w.Write(wire); err != nil {
		t.Fatal(err)
	}
	if err := s2w.Close(); err != nil {
		t.Fatal(err)
	}
	releaseS2Writer(s2w)
	body := cb.Bytes()
	key := "queries/test/partition=0000/task.wshf"
	if _, err := store.Put(ctx, bucket, key, bytes.NewReader(body), int64(len(body)), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}

	e := &Executor{store: store, spillDir: t.TempDir(), shuffleDecodeAhead: true, cpuTokens: newCPUTokens(4)}
	src := newCachedFileStreamSource(e, "", bucket, []string{key})
	if err := src.Init(ctx); err != nil {
		t.Fatal(err)
	}
	var got int
	for {
		b, err := src.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		got += b.Len
	}
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	var wantRows int
	for _, b := range want {
		wantRows += b.Len
	}
	if got != wantRows {
		t.Fatalf("rows = %d, want %d", got, wantRows)
	}
	if e.shuffleDecodeAheadStats.indexedFiles.Load() == 0 {
		t.Fatal("index mode did not engage through the staging seam")
	}
	if e.shuffleDecodeAheadStats.stageNs.Load() != 0 {
		t.Fatal("indexed consumption still staged bytes")
	}
}
