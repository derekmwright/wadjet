# Shuffle extent index (WIDX footer: index-staged decode-ahead)

Status: IMPLEMENTED (2026-08-15, this arc). Env gates:
`WADJET_SHUFFLE_INDEX=0` disables footer emission AND consumption;
`WADJET_SHUFFLE_INDEX_READ=0` disables consumption alone (files still
carry footers — the reader-side A/B arm). Readers auto-fall back to
the stage walk on any absent or invalid footer, so no flag state can
strand data.

Predicted by shuffle-decode-ahead.md §4 and measured by the
claim-donation SF100 window
(`shuffle-da-claim-donation-sf100-2026-08-15.md`): with token stalls
served by donation and pressure at zero, the warm join-6 trio pins at
eff ~6.5 of 15 with per-worker `stage_ms` (30–34s) EXCEEDING
`decode_ms` (~24s) — the scanner's serial chunk-stream walk is the
fragment pacer. The walk exists only because WSHF chunk boundaries
were discoverable solely by reading every length prefix in order.
That is a writer choice, not a law: the writer knows every chunk
offset as it writes.

## 1. The structure

The same window measured WSHF consumption as overwhelmingly
file-backed: `file_pread_files=21,121 / file_pread_bytes=110.3 GB`
against 241 direct transport streams (w0, 4-run cumulative). The
stage walk almost always runs over a local, seekable file (staged S3
download, tier-0 cache hit, or prefetched download —
shuffle-pread-reads.md). Sequential-only consumption is a constraint
inherited from the transport-stream case that the dominant path does
not have.

## 2. Design

**Writer** (`shuffleWriter`): every byte already flows through the
writer from offset 0 (header, then chunks; the only out-of-band write
is the 4-byte NumChunks patch, which overwrites, never appends). A
counting writer records each chunk's start offset. At the three
file-sink close seams (partitioned exchange, shuffle stream sink,
unpartitioned stage sink — all already flush-then-patch), the writer
appends a footer before the flush:

```
offsets:  numChunks × u64 LE   — absolute offset of each chunk's row-count word
count:    u32 LE               — numChunks (cross-check vs header)
trailer:  tableOff u64 LE | version u8 (=1) | magic "WIDX"   (13 bytes)
```

The gather/reply sink (in-memory, coordinator-consumed) emits no
footer. Compression envelopes (WSHC/WSHZ) and the staging
decompressor carry the WSHF payload verbatim, so footers survive the
S3 round trip. Every existing reader is `numChunks`-bounded and never
reads past the last chunk — the mmap kill-switch reader, both serial
readers, `shuffleReadBatches`, and the coordinator paths are all
footer-blind by construction; no version fork.

**Reader** (`streamingShuffleReader` + `shuffleDecodeAhead`): the
file-backed open path (`openShuffleFromFileStreaming`) offers the fd
and size to the reader after the header parse. The reader preads the
trailer and validates exhaustively — magic, version, exact size
arithmetic (`tableOff + N*8 + 17 == size`), count == header
numChunks, offsets starting at the header end, strictly increasing
with ≥4-byte extents, table-bounded. Any failure → nil → the walk
path unchanged. A truncated file loses its trailer and therefore
falls back automatically, surfacing today's truncation error at
today's position.

With a validated index, decode-ahead swaps its scan loop
(`scanIndexed`): the scanner walks the OFFSET TABLE — no file bytes —
admitting each non-empty extent against the exact same
window/pressure/token/donation rules (est = extent size, cursor
exemption intact) and emitting extent slots. Decode workers pread
their own extents (`os.File.ReadAt`, goroutine-safe, GC-safe read
syscalls — the frozen-spin posture is preserved), parse the row-count
word, re-run the stage walk's bounds validation over the in-memory
extent (`validateShuffleChunkBytes` — `readColumnData` slices without
bounds checks, and "a decoder must not trust lengths it has not
checked" applies to pread bytes exactly as to stream bytes), then run
the unchanged `decodeShuffleChunk`. Delivery, credit, `Delivered()`,
error positions, truncation class, and token balance are identical to
the walk path.

**The serial floor collapses from full-file staging (syscall + memcpy
of every byte) to an offset-table loop.** Staging I/O moves into the
workers, parallel and token-governed, and the double-copy
(page cache → staging buffer) becomes a single pread per chunk.

**Drop-behind**: owned single-pass temps are read via
`DropBehindReader` on the walk path; index-mode workers bypass it. An
offset-driven `diskio.DropBehindCursor` (same window shape, same
counters, same advisory-failure latch) advances on each in-order
delivery credit, so the transient page-cache bound is preserved.
Cache-owned files keep their pages, exactly as before.

## 2.1 Extent readahead (2026-08-16, post-3-arm window)

The 3-arm A/B (memo `shuffle-index-3arm-2026-08-15.md`) measured the
index reader ~5–8s slower on q08 and ~12–18s on the suite at R2
(early-warm), converging with the walk reader by R3+ — a
warmth-dependent penalty, so the cause is I/O pattern, not token
economics: k interleaved worker preads defeat kernel readahead on
not-yet-resident files, where the walk scanner's sequential read
warmed pages for free. The idle index scanner therefore keeps
FADV_WILLNEED issued `shuffleIndexReadaheadBytes` (32 MiB) ahead of
its cursor over the extent table — the same posture as the parquet
scan path's `fdWillNeedAdviser`, one syscall per window step, counted
in `readahead_advise_bytes`. Confirm-window judgment: R2 q08 back to
~18s; fallback if it fails is defaulting the reader to walk
(`WADJET_SHUFFLE_INDEX_READ=0` shipping as default) with the footer
kept.

## 3. Alternatives rejected

- **Seek-based skip-walk (reader-only, the §4 sketch as literally
  written)**: headers interleave with payloads every ~16 KB, so
  "headers only" means either a seek per column segment (~8M
  lseek+read syscalls per window at 785k chunks × ~10 columns) or a
  small bufio that re-reads most payload bytes anyway. It also keeps
  the walk serial — a smaller floor, not a removed one. The footer
  costs 8 bytes/chunk (~0.005% of file size) and deletes the problem.
- **Block-staging with in-place header parse (zero-copy slots into
  large pread blocks)**: halves the scanner's memory traffic but
  keeps staging serial and adds block-lifetime refcounting across
  slots; the ceiling stays the scanner.
- **Extent index in the coordinator/catalog instead of the file**:
  the file is the unit that moves (S3, peers, caches); out-of-band
  metadata would need its own consistency story for every hop the
  bytes already survive.
- **Footer in gather replies too**: coordinator replies are
  low-volume, in-memory, and consumed once by `numChunks`-bounded
  readers; bytes for nothing.

## 4. Honest bounds

- Only file-backed consumption benefits; direct transport streams
  (241 reads/window vs 21k file opens) keep the walk. WSHC/WSHZ
  streams cannot seek regardless.
- Files written by older binaries (or with the writer gate off) fall
  back to the walk silently — `indexed_files` vs `file_pread_files`
  is the engagement check.
- Worker preads are ~64–300 KB in ascending order across k=4 workers;
  kernel readahead plus the existing `fadviseSequential` keep this
  effectively sequential. If pread latency surfaces, `pread_ns` is
  the marker to watch before believing anything else.
- The footer adds 8 bytes/chunk + 17 to every file-sink WSHF file and
  its compressed envelopes: ~0.005% of upload bytes at 2048-row
  chunks.

## 5. Markers

`indexed_files` (readers that engaged index mode), `pread_ns`
(worker-side extent read time — the successor to the scanner's
`stage_ns`, which collapses toward zero on indexed files). Both fold
into the existing decode-ahead stats lines. Success judgment at
SF100: `indexed_files` ≈ file-pread opens, `stage_ms` collapse, then
warm join-6 eff/dry (target: the 6.5/7.1 trio joins the eff ~12
mid-shape), then q08/q09 wall.
