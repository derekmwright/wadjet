package worker

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	runtimemetrics "runtime/metrics"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/dataplane"
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/metrics"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/wshf"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const inlineResultThreshold = 512 * 1024 // 512 KB — avoids S3 round-trip for small dimension tables and aggregation results

// natsKVResultThreshold is the max result size stored in NATS KV for
// cross-worker inter-stage transfer. Results below this threshold skip S3
// entirely, reducing inter-stage latency from ~500ms to ~10ms.
const natsKVResultThreshold = 4 * 1024 * 1024 // 4 MB — within NATS 8 MB max payload

// maxBufferedRows caps in-memory row accumulation during scan tasks to prevent
// unbounded memory growth. When this limit is reached, rows are flushed to the
// result file and the buffer is reused. Set to 0 for unlimited (legacy behavior).
const maxBufferedRows = 500_000

// Executor dispatches task types to the appropriate execution logic.
type Executor struct {
	store          objstore.Store
	js             jetstream.JetStream // for catalog access in pipeline tasks
	nc             *nats.Conn          // for Gather-task reply streaming (nil = Gather disabled)
	dpClient       *dataplane.Client   // optional gRPC data-plane; when connected, gather sinks prefer it
	cache          *LRUCache
	resultStore    *ResultStore             // in-memory result passing between stages (nil = disabled)
	resultKV       jetstream.KeyValue       // NATS KV for cross-worker inter-stage results (nil = disabled)
	localCache     *LocalStageCache         // same-worker stage-output local-disk cache (nil = disabled)
	baseTableCache *objstore.BaseTableCache // base-table cache layer for serving peer fetches (nil = disabled)
	baseTableOwns  func(key string) bool    // rendezvous-ownership check for owner read-through (nil = read-through disabled)
	memoryBudget   int64                    // per-task memory budget in bytes (0 = unlimited)
	spillDir       string                   // directory for spill files
	metrics        *metrics.Metrics
	logger         *slog.Logger

	// Worker-level shared memory pool. All concurrent tasks Reserve against
	// the same Tracker, so operators (HashJoin, HashAggregate) spill under
	// cumulative worker pressure instead of per-task budgets. Matches the
	// Trino MemoryPool / Spark ExecutionMemoryPool model: scheduling
	// decisions stay cheap (dispatch freely, worker governs), and N
	// concurrent tasks that would each hold their own independent hash
	// table now share one budget and cooperatively spill.
	sharedTracker *memory.Tracker
	sharedSpill   *memory.SpillManager

	// Phase 3: system-reservoir registry, pushed into every SpillManager for
	// ACCOUNTING. floatingBudgetActive is the deploy-gated flag that decides
	// whether the SpillManager thresholds against the floating budget; default
	// false keeps ShouldSpillFor on the static path.
	reservoirs           *memory.ReservoirRegistry
	floatingBudgetActive bool

	// Same-worker dedup of broadcast-join build state. Probe tasks for the
	// same broadcast (max_concurrent=3 concurrent tasks reading the same
	// build cache) share a single *exec.HashJoin instead of each rebuilding
	// from scratch. See broadcast_join_cache.go.
	broadcastCache *broadcastJoinCache

	// Attach-on-arrival dynamic filters: singleflight pollers for staged
	// merge artifacts, keyed by bucket/key (dynamic_filter_attach.go).
	dfAttachMu    sync.Mutex
	dfAttachPolls map[string]*pendingBloom

	// Streaming-exchange state: fetch tokens + peer-location hints from task
	// specs, and the outbound PeerClient (nil client = peer reads disabled).
	// Always non-nil; dormant without --streaming-exchange.
	peers *peerExchange

	// uploads runs Phase-B background S3 uploads for AsyncUpload tasks.
	// Always non-nil; dormant unless tasks arrive with the flag set.
	uploads *uploadManager

	// Morsel-driven intra-fragment parallelism (SetMorselWorkers,
	// docs/design/morsel-execution.md). morselWorkers: 0/1 = serial (zero
	// value is dormant-safe), -1 = auto, N>1 = fixed width. cpuTokens bounds
	// the EXTRA compute goroutines across all concurrent tasks; nil when
	// morsels are disabled. morselCollapses counts pressure-collapse events
	// (parallel breaker consume reverting to the serial spill path).
	morselWorkers   int
	cpuTokens       *cpuTokens
	morselCollapses atomic.Int64

	// Streaming shuffle read (docs/design/exchange-streaming-consumption.md
	// §3 D1): decode WSHF/WSHC exchange inputs directly from the peer/S3
	// byte stream instead of staging the whole file to NVMe + mmap first.
	// Counters are the §5 rollout markers: reads = files opened streaming,
	// fallbacks = mid-stream failures re-resolved from the durable copy,
	// skipResumes = batches discarded by fallbacks to keep the
	// no-double-delivery contract.
	streamingShuffleRead     bool
	shuffleStreamReads       atomic.Int64
	shuffleStreamFallbacks   atomic.Int64
	shuffleStreamSkipResumes atomic.Int64

	// Read-staged local .wshf opens (shuffle_pread.go): files decoded via
	// sequential read() into heap scratch instead of an mmap walk. The
	// engagement markers for the WSHF-pread lever — on a staged run expect
	// file_pread_bytes to carry what the local/s3/peer tiers serve.
	shuffleFilePreadFiles atomic.Int64
	shuffleFilePreadBytes atomic.Int64

	// Per-tier shuffle-read transfer accounting: which tier served each
	// exchange input open and how many bytes moved. peer/s3 bytes are
	// wire bytes (WSHC stays compressed in transit); local/kv bytes are
	// the materialized payload size. Read via ShuffleIOStats for the
	// worker's 60s "shuffle io stats" marker.
	shuffleIO shuffleIOCounters

	// decodedCache is the worker-lifetime decoded-chunk cache
	// (docs/design/decoded-rowgroup-cache.md); nil = disabled (the
	// default). decodedCacheRegistered guards the one-time
	// RegisterAccounted with the shared SpillManager — either
	// SetDecodedCache or SetSharedPoolBudget runs second and links them
	// (the setter-order self-healing pattern).
	decodedCache           *scan.DecodedChunkCache
	decodedCacheRegistered bool

	// Scan decode pipelining (docs/design/scan-decode-pipelining.md):
	// parquet scan sources decode row groups ahead of consumption with a
	// bounded window instead of one group per Next. Counters are the §5
	// rollout markers, aggregated across sources on iterator close.
	scanDecodeAhead               bool
	scanDecodeAheadBytes          int64
	scanDecodeAheadGroups         atomic.Int64
	scanDecodeAheadWindowFulls    atomic.Int64
	scanDecodeAheadPressureStalls atomic.Int64
	scanDecodeAheadTokenStalls    atomic.Int64
	scanDecodeAheadLedgerStalls   atomic.Int64
	// Stall durations (ns) behind the four stall counters above — counts
	// rank frequency, these rank cost (2026-07-20 Q08 diagnosis).
	scanDecodeAheadWindowFullNs atomic.Int64
	scanDecodeAheadPressureNs   atomic.Int64
	scanDecodeAheadTokenNs      atomic.Int64
	scanDecodeAheadLedgerNs     atomic.Int64
	// Decode-span totals (scan.DecodeAheadIter.DecodeSpans): wall time
	// inside ReadRowGroupNative and the compressed bytes decoded. The
	// stall buckets above only see parked waiters — inline mmap fault
	// time under a held CPU token is visible only here, as ns/byte excess
	// over a page-cache-hot run (rowgroup-readahead residual diagnosis).
	scanDecodeAheadDecodeNs    atomic.Int64
	scanDecodeAheadDecodeBytes atomic.Int64
	// Scan row-group backing reuse (docs/design/scan-output-backing-reuse.md):
	// decodes that reused a released-and-unclaimed backing, decodes that had
	// to mint one, and releases refused because a consumer had claimed the
	// batch. Folded in when each scan source closes; read on the periodic
	// "worker stats" line, never only at drain (ADR-0011 §6).
	scanBackingHits    atomic.Int64
	scanBackingMisses  atomic.Int64
	scanBackingClaimed atomic.Int64

	// Row groups skipped by dynamic-filter pruning (bloom + range),
	// engagement marker for iterator-level attach — dispatch-time AND
	// late (attach-on-arrival) deliveries both land here.
	scanDecodeAheadPrunedGroups atomic.Int64

	// Shuffle decode-ahead (docs/design/shuffle-decode-ahead.md): WSHF
	// chunk decode fans out to workers behind the streaming reader's
	// scanner. Readers add directly to the shared counter sink.
	shuffleDecodeAhead      bool
	shuffleDecodeAheadStats shuffleDecodeAheadStats

	// activeForeground counts tasks currently inside Execute — the busy
	// signal the background-upload QoS gate yields to.
	activeForeground atomic.Int64
	// scanDecodeAheadByQuery attributes the counters above per source
	// identity — the task's QueryID, which on stage-input sources is
	// stage-scoped (e.g. "st-join-10-<query>"), so SF100 logs separate a
	// query's scan legs from its exchange-repartition legs (memo §9.3:
	// Q05's collapse-vs-width tension is phase-local and invisible in
	// worker-lifetime totals). Entries are emitted and deleted by
	// sweepScanDecodeAheadQueryStats once idle, so the map stays bounded
	// on long-running workers.
	scanDecodeAheadByQuery sync.Map // task QueryID string -> *decodeAheadQueryStats
}

// decodeAheadQueryStats is one query's decode-ahead counter slice.
// emitted holds the counter snapshot at the last sweep; a sweep that
// finds the counters unchanged emits the final line and drops the entry.
// Indices 5-8 are the stall durations (ns) behind counters 1-4; 9-10
// are the decode-span totals (ns, compressed bytes); 11 is the dynamic-
// filter pruned-group count.
type decodeAheadQueryStats struct {
	groups, windowFulls, pressureStalls, tokenStalls, ledgerStalls atomic.Int64
	windowFullNs, pressureNs, tokenNs, ledgerNs                    atomic.Int64
	decodeNs, decodeBytes                                          atomic.Int64
	prunedGroups                                                   atomic.Int64
	emitted                                                        [12]int64
}

func (s *decodeAheadQueryStats) snapshot() [12]int64 {
	return [12]int64{s.groups.Load(), s.windowFulls.Load(), s.pressureStalls.Load(),
		s.tokenStalls.Load(), s.ledgerStalls.Load(),
		s.windowFullNs.Load(), s.pressureNs.Load(), s.tokenNs.Load(), s.ledgerNs.Load(),
		s.decodeNs.Load(), s.decodeBytes.Load(), s.prunedGroups.Load()}
}

// SetStreamingShuffleRead enables streaming decode of shuffle inputs
// (--streaming-shuffle-read). Call before Worker.Start.
func (e *Executor) SetStreamingShuffleRead(on bool) { e.streamingShuffleRead = on }

// ShuffleStreamStats returns the streaming-shuffle-read counters:
// streaming opens, staged fallbacks, and batches skipped by fallbacks.
func (e *Executor) ShuffleStreamStats() (reads, fallbacks, skipResumes int64) {
	return e.shuffleStreamReads.Load(), e.shuffleStreamFallbacks.Load(), e.shuffleStreamSkipResumes.Load()
}

// ShuffleFilePreadStats returns the read-staged local shuffle open
// counters: files opened via the WSHF-pread path and their total bytes.
func (e *Executor) ShuffleFilePreadStats() (files, bytes int64) {
	return e.shuffleFilePreadFiles.Load(), e.shuffleFilePreadBytes.Load()
}

// shuffleIOCounters is the per-tier shuffle-read ledger: one files/bytes
// pair per serving tier (same-worker LocalStageCache, NATS KV, peer
// exchange, durable S3). Peer counts include prefetched peer downloads —
// the transfer happens at fetch time regardless of when the file is opened.
type shuffleIOCounters struct {
	localFiles, localBytes atomic.Int64
	kvFiles, kvBytes       atomic.Int64
	peerFiles, peerBytes   atomic.Int64
	s3Files, s3Bytes       atomic.Int64
}

// ShuffleIOSnapshot is one point-in-time reading of the per-tier
// shuffle-read ledger.
type ShuffleIOSnapshot struct {
	LocalFiles, LocalBytes int64
	KVFiles, KVBytes       int64
	PeerFiles, PeerBytes   int64
	S3Files, S3Bytes       int64
}

// ShuffleIOStats returns the per-tier shuffle-read transfer counters.
func (e *Executor) ShuffleIOStats() ShuffleIOSnapshot {
	return ShuffleIOSnapshot{
		LocalFiles: e.shuffleIO.localFiles.Load(), LocalBytes: e.shuffleIO.localBytes.Load(),
		KVFiles: e.shuffleIO.kvFiles.Load(), KVBytes: e.shuffleIO.kvBytes.Load(),
		PeerFiles: e.shuffleIO.peerFiles.Load(), PeerBytes: e.shuffleIO.peerBytes.Load(),
		S3Files: e.shuffleIO.s3Files.Load(), S3Bytes: e.shuffleIO.s3Bytes.Load(),
	}
}

// SetScanDecodeAhead enables decode-ahead on parquet scan sources
// (--scan-decode-ahead). windowBytes <= 0 selects the default. Call
// before Worker.Start.
func (e *Executor) SetScanDecodeAhead(on bool, windowBytes int64) {
	e.scanDecodeAhead = on
	e.scanDecodeAheadBytes = windowBytes
}

// SetShuffleDecodeAhead enables chunk-parallel decode on WSHF shuffle
// inputs (--shuffle-decode-ahead). Call before Worker.Start.
func (e *Executor) SetShuffleDecodeAhead(on bool) { e.shuffleDecodeAhead = on }

// ShuffleDecodeAheadStats returns the shuffle decode-ahead markers:
// chunks decoded ahead, the parked/spent spans (ns) per class —
// window-full, token, pressure on the admission side; stage (the serial
// scanner walk, the structural floor) and decode (worker time) on the
// throughput side — and tokens accepted via producer donation (§2.2;
// expect token stalls to fall as this rises).
func (e *Executor) ShuffleDecodeAheadStats() (chunks, windowFullNs, tokenNs, pressureNs, stageNs, decodeNs, donated, preadNs, indexedFiles int64) {
	s := &e.shuffleDecodeAheadStats
	return s.chunks.Load(), s.windowFullNs.Load(), s.tokenStallNs.Load(),
		s.pressureNs.Load(), s.stageNs.Load(), s.decodeNs.Load(), s.donated.Load(),
		s.preadNs.Load(), s.indexedFiles.Load()
}

// CPUTokenAdmissionStats reports the worker pool's decode-class admission
// counters: pool capacity, the reserved decode floor, decode-class tokens
// granted, how many of those were granted while morsel consumers were
// queued (the admissions the old strict-FIFO policy refused — the direct
// measure of the fix), and how often a release was held back to keep the
// floor reachable. Read alongside scan/shuffle token_stall_ms, which is
// what these are meant to move.
func (e *Executor) CPUTokenAdmissionStats() (capacity, reserve, admits, bypasses, holdbacks int64) {
	return e.cpuTokens.admissionStats()
}

// ScanBackingStats returns the row-group backing-reuse counters
// (docs/design/scan-output-backing-reuse.md): decodes served from a released
// backing, decodes that minted one, and releases refused by a Detach claim.
func (e *Executor) ScanBackingStats() (hits, misses, claimed int64) {
	return e.scanBackingHits.Load(), e.scanBackingMisses.Load(), e.scanBackingClaimed.Load()
}

// foldScanDecodeAheadQueryStats adds one closed iterator's counters to
// the per-query accumulator. queryID may be empty (embedded callers);
// those fold under the "-" bucket rather than being dropped.
func (e *Executor) foldScanDecodeAheadQueryStats(queryID string, groups, windowFulls, pressureStalls, tokenStalls, ledgerStalls, windowFullNs, pressureNs, tokenNs, ledgerNs, decodeNs, decodeBytes, prunedGroups int64) {
	if queryID == "" {
		queryID = "-"
	}
	v, _ := e.scanDecodeAheadByQuery.LoadOrStore(queryID, &decodeAheadQueryStats{})
	qs := v.(*decodeAheadQueryStats)
	qs.groups.Add(groups)
	qs.windowFulls.Add(windowFulls)
	qs.pressureStalls.Add(pressureStalls)
	qs.tokenStalls.Add(tokenStalls)
	qs.ledgerStalls.Add(ledgerStalls)
	qs.windowFullNs.Add(windowFullNs)
	qs.pressureNs.Add(pressureNs)
	qs.tokenNs.Add(tokenNs)
	qs.ledgerNs.Add(ledgerNs)
	qs.decodeNs.Add(decodeNs)
	qs.decodeBytes.Add(decodeBytes)
	qs.prunedGroups.Add(prunedGroups)
}

// sweepScanDecodeAheadQueryStats emits per-query decode-ahead stats
// lines. A query whose counters are unchanged since the previous sweep
// is finished (sources fold on close): its line is emitted and the
// entry dropped. Queries still accumulating are held for the next
// sweep. final=true emits and drops everything — the worker is
// stopping and there is no next sweep.
func (e *Executor) sweepScanDecodeAheadQueryStats(final bool) {
	logger := e.logger
	if logger == nil {
		logger = slog.Default()
	}
	e.scanDecodeAheadByQuery.Range(func(k, v any) bool {
		qs := v.(*decodeAheadQueryStats)
		cur := qs.snapshot()
		if !final && cur != qs.emitted {
			qs.emitted = cur
			return true // still moving — hold for the next sweep
		}
		e.scanDecodeAheadByQuery.Delete(k)
		logger.Info("scan decode-ahead query stats",
			"query", k,
			"groups", cur[0], "window_fulls", cur[1],
			"pressure_stalls", cur[2], "token_stalls", cur[3],
			"ledger_stalls", cur[4],
			"window_full_ms", cur[5]/1e6, "pressure_stall_ms", cur[6]/1e6,
			"token_stall_ms", cur[7]/1e6, "ledger_stall_ms", cur[8]/1e6,
			"decode_ms", cur[9]/1e6, "decode_bytes", cur[10],
			"rg_pruned", cur[11])
		return true
	})
}

// ScanDecodeAheadStats returns the decode-ahead counters: row groups
// decoded ahead, worker stalls on a full window, admissions refused
// under heap pressure, per-group admissions deferred for lack of a cpu
// token, and admissions denied by the shared memory pool ledger (each
// affected group still decodes, serially at worst).
func (e *Executor) ScanDecodeAheadStats() (groups, windowFulls, pressureStalls, tokenStalls, ledgerStalls int64) {
	return e.scanDecodeAheadGroups.Load(), e.scanDecodeAheadWindowFulls.Load(),
		e.scanDecodeAheadPressureStalls.Load(), e.scanDecodeAheadTokenStalls.Load(),
		e.scanDecodeAheadLedgerStalls.Load()
}

// ScanDecodeAheadPrunedGroups returns the row groups skipped by dynamic-
// filter pruning at the iterator layer (bloom + range combined).
func (e *Executor) ScanDecodeAheadPrunedGroups() int64 {
	return e.scanDecodeAheadPrunedGroups.Load()
}

// ScanDecodeAheadStallNs returns the total blocked time (ns) behind the
// four stall counters of ScanDecodeAheadStats, same order.
func (e *Executor) ScanDecodeAheadStallNs() (windowFullNs, pressureNs, tokenNs, ledgerNs int64) {
	return e.scanDecodeAheadWindowFullNs.Load(), e.scanDecodeAheadPressureNs.Load(),
		e.scanDecodeAheadTokenNs.Load(), e.scanDecodeAheadLedgerNs.Load()
}

// ScanDecodeAheadDecodeSpans returns the total wall time (ns) inside
// row-group decode calls and the projected compressed bytes decoded —
// the inline-fault discriminator the stall counters cannot see.
func (e *Executor) ScanDecodeAheadDecodeSpans() (ns, bytes int64) {
	return e.scanDecodeAheadDecodeNs.Load(), e.scanDecodeAheadDecodeBytes.Load()
}

// NewExecutor creates a new task executor.
func NewExecutor(store objstore.Store, cache *LRUCache, js jetstream.JetStream) *Executor {
	e := &Executor{
		store:          store,
		js:             js,
		cache:          cache,
		logger:         slog.Default(),
		broadcastCache: newBroadcastJoinCache(),
		peers:          newPeerExchange(),
	}
	e.uploads = newUploadManager(store, nil, e.logger)
	e.uploads.SetForegroundProbe(func() bool { return e.activeForeground.Load() > 0 })
	return e
}

// SetMemoryBudget configures the per-task memory budget and the spill
// directory. For backward compatibility it also initializes a shared
// pool of the same size, so existing callers that pass a single budget
// continue to get cooperative spill across concurrent tasks. Callers
// that want a different pool size (typically larger than per-task
// budget) should call SetSharedPoolBudget afterward to override.
func (e *Executor) SetMemoryBudget(budget int64, spillDir string) {
	e.memoryBudget = budget
	e.spillDir = spillDir
	if budget > 0 {
		e.SetSharedPoolBudget(budget)
	}
}

// degradedSpillKey carries a task-scoped reduced-budget SpillManager view
// through the context for a #318 poison-suspect retry.
type degradedSpillKey struct{}

// degradedBudgetDivisor sizes the degraded retry's spill budget as
// sharedBudget/divisor (floored at degradedBudgetFloor). A var for tests.
var degradedBudgetDivisor int64 = 4

const degradedBudgetFloor = 64 << 20

// DegradedTaskBudget returns the reduced spill budget a poison-suspect
// retry runs under on this executor, or 0 when no pool is configured (no
// degradation possible — the quarantine rung still applies).
func (e *Executor) DegradedTaskBudget() int64 {
	full := e.memoryBudget
	if e.sharedTracker != nil {
		full = e.sharedTracker.Budget()
	}
	if full <= 0 {
		return 0
	}
	b := full / degradedBudgetDivisor
	if b < degradedBudgetFloor {
		b = degradedBudgetFloor
	}
	if b > full {
		b = full
	}
	return b
}

// withTaskSpill installs a reduced-budget spill view on the context when
// the task is flagged for a degraded retry (#318). One view per task, so
// all its operators share one registry and one set of thresholds.
func (e *Executor) withTaskSpill(ctx context.Context, task distributed.Task) context.Context {
	if !task.DegradedMemory || e.sharedSpill == nil {
		return ctx
	}
	budget := e.DegradedTaskBudget()
	if budget <= 0 {
		return ctx
	}
	e.logger.Warn("executing task under a degraded memory budget (poison-suspect retry)",
		"task_id", task.ID, "query_id", task.QueryID,
		"degraded_budget_mb", budget/(1<<20))
	return context.WithValue(ctx, degradedSpillKey{}, e.sharedSpill.DegradedView(budget))
}

// spillFor resolves the spill manager a task's operators should use: the
// context's degraded view when the #318 defense installed one, otherwise
// the worker-shared manager.
func (e *Executor) spillFor(ctx context.Context) *memory.SpillManager {
	if sm, ok := ctx.Value(degradedSpillKey{}).(*memory.SpillManager); ok {
		return sm
	}
	return e.sharedSpill
}

// SharedPoolStats returns (used, budget) bytes for the worker-wide
// memory pool, or (0, 0) if no pool is configured. Used by the worker
// heartbeat loop to publish pool pressure for coord-side dispatch
// backpressure.
func (e *Executor) SharedPoolStats() (used, budget int64) {
	if e.sharedTracker == nil {
		return 0, 0
	}
	return e.sharedTracker.Used(), e.sharedTracker.Budget()
}

// HeapDrift returns the Phase-4 accounting drift in bytes for the supplied
// HeapInuse sample — HeapInuse − (operator owned + reservoir actual) — or 0 when
// no shared spill manager is configured. Observability only.
func (e *Executor) HeapDrift(heapInuse int64) int64 {
	if e.sharedSpill == nil {
		return 0
	}
	return e.sharedSpill.HeapDrift(heapInuse)
}

// SetSharedPoolBudget creates the worker-wide memory pool that all
// concurrent tasks Reserve against. Operators (HashJoin build, sort
// run accumulation, hash aggregate state) cooperatively spill when the
// pool fills, regardless of which task is holding the bytes. Matches the
// Trino MemoryPool / Spark ExecutionMemoryPool model.
//
// Pool budget should be the FULL worker envelope (after cache reservation),
// not a per-task slice. With 32GB physical RAM and a 24GB GOMEMLIMIT,
// pool budget is roughly 21GB (envelope − cache).
//
// Calling this with budget<=0 disables the shared pool and falls back to
// per-task tracking via SetMemoryBudget.
func (e *Executor) SetSharedPoolBudget(budget int64) {
	if budget <= 0 {
		e.sharedTracker = nil
		e.sharedSpill = nil
		return
	}
	e.sharedTracker = memory.NewTracker("worker", budget)
	dir := e.spillDir
	if dir == "" {
		dir = os.TempDir()
	}
	sm, err := memory.NewSpillManager(dir, e.sharedTracker)
	if err != nil {
		e.logger.Warn("failed to create worker spill manager; tasks run without spill governance",
			"error", err)
		return
	}
	e.sharedSpill = sm
	// Self-healing wiring: whichever of SetSharedPoolBudget / SetReservoirs runs
	// second links the registry into the freshly-built SpillManager (the
	// compiler can't enforce setter ordering — feedback_setter_before_start).
	if e.reservoirs != nil {
		e.sharedSpill.SetReservoirs(e.reservoirs)
		e.sharedSpill.SetFloatingBudgetActive(e.floatingBudgetActive)
	}
	if e.decodedCache != nil && !e.decodedCacheRegistered {
		e.sharedSpill.RegisterAccounted(e.decodedCache)
		e.decodedCacheRegistered = true
	}
}

// SetDecodedCache attaches the worker-lifetime decoded-chunk cache
// (docs/design/decoded-rowgroup-cache.md) consulted by parquet scan
// sources. When the shared SpillManager exists it also registers the cache
// as an AccountedOperator so RequestRelief can shed cached bytes —
// eviction, the cheapest relief in the process, runs before any operator
// pays a real spill. Same setter-order self-healing as SetReservoirs.
// Call before any task executes; nil leaves scans uncached.
func (e *Executor) SetDecodedCache(c *scan.DecodedChunkCache) {
	e.decodedCache = c
	// Admission pause: while either pressure channel is live, the cache
	// stops cloning new entries (hits keep serving) — the between-shed
	// half of the §9.4 pressure-coupling fix. Uses the same test-seam
	// package vars as the fragment runner.
	if c != nil {
		// RAW heap gauge on purpose: the adjusted gauge discounts the
		// cache's own bytes (memory.SetReclaimableBytesFunc below), so it
		// goes quiet exactly when residency is the pressure source — the
		// case admission must pause for.
		c.SetPressureFunc(func() bool {
			return rawHeapPressureActive() || pageCachePressureActive()
		})
		// Gauge deduction: the cache is evictable on demand, so its
		// resident bytes must not read as heap pressure to execution
		// decisions (morsel collapse, backpressure pauses, the spill
		// breaker). See the 2026-08-12 collapse evidence at
		// memory.SetReclaimableBytesFunc.
		memory.SetReclaimableBytesFunc(c.Size)
	}
	if c != nil && e.sharedSpill != nil && !e.decodedCacheRegistered {
		e.sharedSpill.RegisterAccounted(c)
		e.decodedCacheRegistered = true
	}
}

// SetReservoirs wires the system-reservoir registry into the executor and the
// shared SpillManager for ACCOUNTING. It does NOT activate the floating spill
// threshold (see SetFloatingBudgetActive). Call before any task executes.
func (e *Executor) SetReservoirs(rr *memory.ReservoirRegistry) {
	e.reservoirs = rr
	if e.sharedSpill != nil {
		e.sharedSpill.SetReservoirs(rr)
	}
}

// SetFloatingBudgetActive propagates the deploy-gated floating-threshold flag to
// the shared SpillManager. Default false keeps ShouldSpillFor static.
func (e *Executor) SetFloatingBudgetActive(active bool) {
	e.floatingBudgetActive = active
	if e.sharedSpill != nil {
		e.sharedSpill.SetFloatingBudgetActive(active)
	}
}

// SetResultStore attaches an in-memory result store for inter-stage result
// passing. When a reservoir registry is wired, it registers the result store as
// a hard reservoir backed by the store's live UsedBytes accessor so its
// occupancy (≈496 MB at SF100) feeds Available()/drift. Requires SetReservoirs
// to have run first.
func (e *Executor) SetResultStore(rs *ResultStore) {
	e.resultStore = rs
	if e.reservoirs != nil && rs != nil && rs.MaxBytes() > 0 {
		e.reservoirs.Register(memory.NewReservoirFunc("resultstore", rs.MaxBytes(), rs.UsedBytes))
	}
}

// SetLocalStageCache attaches a same-worker local-disk stage-output cache.
// Producers register their local spill files in it after upload succeeds;
// consumers consult it before falling back to KV/S3. Lifecycle is driven by
// query-complete / cancel signals from the coordinator.
func (e *Executor) SetLocalStageCache(c *LocalStageCache) {
	e.localCache = c
}

// SetNATSConn attaches a NATS connection used by Gather tasks to stream
// batches back to the coordinator's reply subject (and by the async upload
// manager for UploadComplete notifications).
func (e *Executor) SetNATSConn(nc *nats.Conn) {
	e.nc = nc
	if e.uploads != nil {
		e.uploads.nc = nc
	}
}

// SetDataPlaneClient enables gRPC result streaming for gather sinks
// constructed by this executor. When set and Connected, each
// gatherReplySink prefers the gRPC stream over NATS Publish. nil
// reverts to NATS-only delivery.
func (e *Executor) SetDataPlaneClient(c *dataplane.Client) {
	e.dpClient = c
}

// SetResultKV attaches a NATS KV store for cross-worker inter-stage result transfer.
// Results below natsKVResultThreshold are stored here instead of S3, reducing
// inter-stage latency from ~500ms (S3 round-trip) to ~10ms (NATS KV).
func (e *Executor) SetResultKV(kv jetstream.KeyValue) {
	e.resultKV = kv
}

// SetMetrics attaches Prometheus metrics for spill/memory tracking.
func (e *Executor) SetMetrics(m *metrics.Metrics) {
	e.metrics = m
}

// SetLogger sets the executor's logger.
func (e *Executor) SetLogger(l *slog.Logger) {
	e.logger = l
	if e.uploads != nil {
		e.uploads.logger = l
	}
}

// SetMorselWorkers configures intra-fragment parallel pipeline consumers
// (morsel-driven execution, docs/design/morsel-execution.md). 0 and 1 =
// serial (today's behavior); -1 = auto (width adapts to fragment input size
// and idle CPU tokens); N>1 = fixed width of N. Must be called before the
// worker starts executing tasks.
func (e *Executor) SetMorselWorkers(n int) {
	e.morselWorkers = n
	if n == -1 || n > 1 {
		e.cpuTokens = newCPUTokens(defaultCPUTokenCapacity())
	} else {
		e.cpuTokens = nil
	}
}

// newSpillManager creates a Tracker + SpillManager for a task.
//
// When the worker has a configured shared memory pool (via SetMemoryBudget),
// the task tracker is a CHILD of the shared pool — its Reserve calls bubble
// up so spill triggers fire on cumulative worker pressure, not per-task
// quotas. Matches the Trino MemoryPool / Spark ExecutionMemoryPool model:
// every concurrent task allocates from one budget, and operators
// cooperatively spill when the pool fills, regardless of which task is
// holding the bytes.
//
// Without a shared pool (SetMemoryBudget never called or budget==0), returns
// nil/nil — no tracking, no spill. Same behaviour as the old per-task path.
func (e *Executor) newSpillManager(taskID string) (*memory.SpillManager, *memory.Tracker) {
	if e.sharedTracker != nil {
		return e.sharedSpill, e.sharedTracker.Child(taskID)
	}
	if e.memoryBudget <= 0 {
		return nil, nil
	}
	// Fallback: legacy per-task pool when SetMemoryBudget wasn't called but
	// memoryBudget is set directly (test paths, embedded callers).
	tracker := memory.NewTracker(taskID, e.memoryBudget)
	dir := e.spillDir
	if dir == "" {
		dir = os.TempDir()
	}
	sm, err := memory.NewSpillManager(dir, tracker)
	if err != nil {
		e.logger.Warn("failed to create spill manager, running without spill",
			"task_id", taskID, "error", err)
		return nil, tracker
	}
	return sm, tracker
}

// newSpillManagerScaled is preserved for the legacy per-task-pool path —
// when the shared pool is active (the prod path), join scaling has no
// meaning because the pool is sized for cumulative pressure across all
// concurrent tasks and operators. Scaling per-task budgets would
// over-provision against a shared pool that already accounts for them.
//
// joinCount is therefore only honoured on the legacy path.
func (e *Executor) newSpillManagerScaled(taskID string, joinCount int) (*memory.SpillManager, *memory.Tracker) {
	if e.sharedTracker != nil {
		return e.sharedSpill, e.sharedTracker.Child(taskID)
	}
	if e.memoryBudget <= 0 {
		return nil, nil
	}
	budget := e.memoryBudget
	if joinCount > 1 {
		budget = e.memoryBudget * int64(joinCount)
	}
	tracker := memory.NewTracker(taskID, budget)
	dir := e.spillDir
	if dir == "" {
		dir = os.TempDir()
	}
	sm, err := memory.NewSpillManager(dir, tracker)
	if err != nil {
		e.logger.Warn("failed to create spill manager, running without spill",
			"task_id", taskID, "error", err)
		return nil, tracker
	}
	return sm, tracker
}

// Execute runs a task and returns the result notification.
func (e *Executor) Execute(ctx context.Context, task distributed.Task, workerID string) distributed.ResultNotification {
	start := time.Now()
	// Foreground-activity signal for the background-upload QoS gate
	// (upload_manager.go): while any task executes, drains yield — but
	// only inside the protection window a NEW root query's first task
	// opens (the v3 epoch clock).
	e.activeForeground.Add(1)
	defer e.activeForeground.Add(-1)
	e.uploads.NoteForegroundQuery(distributed.TaskRootQueryID(&task))

	result := distributed.ResultNotification{
		TaskID:    task.ID,
		QueryID:   task.QueryID,
		StageID:   task.StageID,
		WorkerID:  workerID,
		Timestamp: time.Now(),
	}

	// Streaming exchange: record the task's fetch token (for serving
	// validation) and peer-location hints (for Tier-1.5 reads) before any
	// input is opened. No-op without --streaming-exchange.
	e.peers.registerTask(&task)

	// Poison-suspect retry (#318): run this task's operators against a
	// reduced-budget spill view so they spill early instead of rebuilding
	// the heap profile that killed the previous attempt's worker. The view
	// rides the context; e.spillFor(ctx) resolves it at every operator
	// wiring site.
	ctx = e.withTaskSpill(ctx, task)

	// Worker-side ABAC enforcement: validate column access policies before execution.
	if err := e.enforcePolicyDecision(task); err != nil {
		result.Duration = time.Since(start)
		result.Error = fmt.Sprintf("policy enforcement: %s", err)
		return result
	}

	peakTracker := newTaskPeakHeapTracker(ctx)

	var err error
	switch task.Type {
	case distributed.TaskTypePipeline:
		err = e.executePipeline(ctx, task, &result)
	case distributed.TaskTypeGather:
		// Ordered gather: the coordinator attached a fragment pipeline
		// ([ShuffleSource, OpSort, GatherSink]) because the upstream's
		// fused sort may have fanned out at dispatch — merge-sort the
		// pre-sorted inputs and apply the limit here, then stream to the
		// reply subject.
		// Native-DAG Gather: stream upstream files → gatherReplySink. No SQL.
		// Legacy Gather (set via executePipeline sink swap) is still reachable
		// when StageType is empty + Inputs is empty — rare today; callers
		// should prefer Inputs-based routing.
		switch {
		case len(task.Operators) > 0:
			err = e.executeFragment(ctx, task, &result)
		case len(task.Inputs) > 0:
			err = e.executeGatherStage(ctx, task, &result)
		default:
			err = e.executePipeline(ctx, task, &result)
		}
	case distributed.TaskTypeShuffle:
		err = e.executeShuffle(ctx, task, &result)
	case distributed.TaskTypeStage:
		err = e.executeStage(ctx, task, &result)
	default:
		err = fmt.Errorf("unsupported task type: %s", task.Type)
	}

	peakTracker.Stop()

	result.Duration = time.Since(start)
	if err != nil {
		result.Success = false
		result.Error = err.Error()
		// Streaming exchange Phase B: surface unresolvable hinted inputs
		// structurally so the coordinator can classify against producer
		// liveness + durability instead of parsing error text.
		var miss *missingInputError
		if errors.As(err, &miss) {
			result.MissingInputKey = miss.key
		}
	} else {
		result.Success = true
	}

	// Ensure TaskStats is always populated (fallback for tasks without spill)
	if result.TaskStats == nil {
		result.TaskStats = &distributed.TaskStats{RSS: distributed.ProcessRSS()}
	}
	result.TaskStats.PeakHeapMB = peakTracker.PeakMB()

	// Per-operator peak attribution: ask the shared SpillManager for every
	// currently-registered Spillable's name + peak/current footprint. This
	// runs at task end so unregistered operators (e.g. completed broadcast
	// builds) don't appear. When tasks run concurrently against the same
	// shared spill, attribution is union-of-all-active — accept that loss
	// of per-task isolation as the cost of the lightweight wiring.
	if e.sharedSpill != nil {
		snaps := e.sharedSpill.Inspect()
		if len(snaps) > 0 {
			result.TaskStats.OperatorPeaks = make([]distributed.OperatorPeak, len(snaps))
			for i, s := range snaps {
				result.TaskStats.OperatorPeaks[i] = distributed.OperatorPeak{
					Name:     s.Name,
					Peak:     s.OwnedBytes,     // departed entries carry true peak
					Current:  s.SpillableBytes, // reclaimable now
					Owned:    s.OwnedBytes,
					Retained: s.RetainedBytes,
					State:    s.State.String(),
					Closed:   s.Departed,
				}
			}
		}
	}
	if e.sharedTracker != nil {
		result.TaskStats.TrackerPeak = e.sharedTracker.Peak()
	}

	// Phase-4 per-task accounting observability (diagnostic only; no decision).
	// STW-free heap read: task ends bunch at stage completion (24-task
	// stages drain in seconds), so a ReadMemStats here compounded the
	// taskPeakHeapTracker STW storm — see that type's doc comment.
	// HeapInuse == heap/objects + heap/unused in runtime/metrics terms.
	if e.sharedSpill != nil {
		s := []runtimemetrics.Sample{
			{Name: "/memory/classes/heap/objects:bytes"},
			{Name: "/memory/classes/heap/unused:bytes"},
		}
		runtimemetrics.Read(s)
		heapInuse := int64(s[0].Value.Uint64() + s[1].Value.Uint64())
		rss := result.TaskStats.RSS
		if rss == 0 {
			rss = distributed.ProcessRSS()
		}
		driftMB, _, mmapMB := computeStatsGauges(heapInuse, rss, e.sharedSpill.HeapDrift(heapInuse))
		result.TaskStats.DriftMB = driftMB
		result.TaskStats.MmapRSSMB = mmapMB
	}

	return result
}

// enforcePolicyDecision validates ABAC column policies at the worker before
// task execution. If a denied column appears in the task's requested columns,
// the task is rejected. This provides defense-in-depth: the coordinator
// applies row filters at planning time, and the worker re-checks column
// policies at execution time.
func (e *Executor) enforcePolicyDecision(task distributed.Task) error {
	if len(task.PolicyDecisionJSON) == 0 {
		return nil
	}
	var sd auth.SerializedDecision
	if err := json.Unmarshal(task.PolicyDecisionJSON, &sd); err != nil {
		return fmt.Errorf("unmarshaling policy decision: %w", err)
	}
	if !sd.Allowed {
		return fmt.Errorf("access denied by policy")
	}

	// Check column-level policies for the task's target table
	tableName := task.TableName
	if tableName == "" {
		return nil // non-table tasks (aggregate, sort, etc.) don't need column checks
	}
	td, ok := sd.TableDecisions[tableName]
	if !ok || td == nil {
		return nil
	}
	if !td.Allowed {
		return fmt.Errorf("access denied for table %q: %s", tableName, td.Reason)
	}

	// Check each requested column against column-level decisions
	requestedCols := make(map[string]bool, len(task.Columns))
	for _, c := range task.Columns {
		requestedCols[c] = true
	}
	for _, cd := range td.Columns {
		if !cd.Allowed && requestedCols[cd.Column] {
			return fmt.Errorf("access denied for column %q in table %q", cd.Column, tableName)
		}
	}
	if e.logger != nil {
		e.logger.Debug("worker policy enforcement passed",
			"task_id", task.ID,
			"table", tableName,
			"columns", len(td.Columns),
		)
	}
	return nil
}

func (e *Executor) executePipeline(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	if task.SQLText == "" {
		return fmt.Errorf("pipeline task missing SQL text")
	}
	if e.js == nil {
		return fmt.Errorf("pipeline task requires JetStream for catalog access")
	}

	bucket := task.DataBucket
	if bucket == "" {
		bucket = task.ResultBucket
	}

	// Create a catalog from NATS KV (same metadata the coordinator uses).
	// Wrap the object store with CachedStore so that scanners benefit from
	// the worker's cross-query LRU file cache instead of re-reading S3.
	kv, err := catalog.NewNATSKV(e.js)
	if err != nil {
		return fmt.Errorf("creating catalog KV: %w", err)
	}
	cachedStore := NewCachedStore(e.store, e.cache, e.sharedTracker)
	cat := catalog.New(kv, cachedStore, bucket)
	if err := cat.Init(ctx); err != nil {
		return fmt.Errorf("initializing catalog: %w", err)
	}

	// Parse SQL
	parsed, err := plansql.Parse(task.SQLText)
	if err != nil {
		return fmt.Errorf("parse: %w", err)
	}
	selectInfo, err := plansql.ExtractSelect(parsed)
	if err != nil {
		return fmt.Errorf("extract: %w", err)
	}

	// Build and optimize logical plan
	logicalPlan, err := logical.BuildFromSelect(selectInfo)
	if err != nil {
		return fmt.Errorf("logical plan: %w", err)
	}
	planner := physical.NewPlanner(cat)
	planner.AnnotateScanColumns(ctx, logicalPlan)
	scanAnnotator := func(plan *logical.Node) {
		planner.AnnotateScanColumns(ctx, plan)
	}
	logicalPlan = logical.Optimize(logicalPlan, scanAnnotator)

	// Shuffle-distributed aggregate (spec: 2026-04-18-shuffle-distributed-
	// aggregate.md): when the coordinator pre-computed derived aggregate
	// subplans (e.g. Q17's decorrelated inner AVG-per-partkey), walk the
	// logical plan and replace each matching Aggregate subtree with a
	// synthetic scan of the cache files. Must run AFTER optimize (so the
	// decorrelator has landed the aggregate) and BEFORE physical planning
	// (so the scan substitution is visible to buildScan).
	precompAliasFiles := make(map[string][]string)
	if len(task.PreComputedAggregates) > 0 {
		sigs := make([]logical.PreComputedAggregate, 0, len(task.PreComputedAggregates))
		for i, pa := range task.PreComputedAggregates {
			alias := fmt.Sprintf("__precomp_agg_%d", i)
			aggOut := make([]string, len(pa.AggSpecs))
			for j, spec := range pa.AggSpecs {
				aggOut[j] = spec.OutputCol
			}
			sigs = append(sigs, logical.PreComputedAggregate{
				InputTable:     pa.InputTable,
				GroupByCols:    pa.GroupByCols,
				AggOutputCols:  aggOut,
				SyntheticAlias: alias,
			})
			precompAliasFiles[alias] = pa.CacheFiles
		}
		used, subErr := logical.SubstitutePreComputedAggregates(logicalPlan, sigs)
		if subErr != nil {
			return fmt.Errorf("substitute pre-computed aggregates: %w", subErr)
		}
		for alias := range precompAliasFiles {
			if !used[alias] {
				// Signature didn't match — fine, falls back to in-plan execution.
				// Drop the unused alias so it doesn't pollute StreamingSources.
				delete(precompAliasFiles, alias)
			}
		}
		e.logger.Info("pre-computed aggregate substitution",
			"sig_count", len(sigs), "matched", len(used), "unmatched_aliases_dropped", len(sigs)-len(used))
	}

	// Build standalone physical plan (single pipeline, no stages).
	// Set memory budget and spill directory so the planner can install spill
	// managers on pipeline-breaking operators. Without this, concurrent pipeline
	// tasks bypass memory tracking and risk OOM under multi-join pressure.
	if e.memoryBudget > 0 {
		planner.MemoryBudget = e.memoryBudget
	}
	if e.spillDir != "" {
		planner.SpillDir = e.spillDir
	}
	// Inject the worker-level shared pool so concurrent pipeline tasks
	// compete for one budget instead of each creating a private
	// Tracker+SpillManager. Without this, two concurrent pipeline tasks
	// could each allocate up to MemoryBudget and OOM the worker.
	if e.sharedTracker != nil {
		planner.SharedTracker = e.sharedTracker
	}
	if sm := e.spillFor(ctx); sm != nil {
		planner.SharedSpillMgr = sm
	}

	// Scan-split pipeline mode: create lazy streaming sources for pre-scanned
	// build-cache files. Each source downloads and parses files one at a time,
	// yielding batches on demand. This avoids materializing the entire build
	// side into memory — the hash join's grace spill handles memory pressure.
	if len(task.PreScannedInputs) > 0 || len(precompAliasFiles) > 0 || len(task.Inputs) > 0 {
		// No taskDeleteSets/SetDeleteMarkers here for PreScannedInputs or
		// precompAliasFiles (unlike task.Inputs below, handled inside
		// sourceForAlias). Both file lists are query-scoped .wshf output
		// (queries/<id>/build-cache/... and queries/<id>/aggregate-cache/...)
		// from an earlier TaskTypePipeline sub-query — build_cache.go's
		// preScanOneTable / aggregate_shuffle.go's preComputeDerivedAggregate
		// — that already re-planned against a live catalog and applied
		// manifest.DeleteMarkers itself when producing them (executePipeline
		// always re-plans this way: see planner.Plan below). They can never
		// be base-table parquet, so a marker keyed by base-table path could
		// never match one of these keys even if this task carried one (#491
		// discussion; build_cache.go asserts the invariant on the write
		// side). A source built from either map is reading rows a DELETE
		// has already removed, not rows it still needs to filter.
		streamingSources := make(map[string]exec.Source, len(task.PreScannedInputs)+len(precompAliasFiles)+len(task.Inputs))
		for tableName, files := range task.PreScannedInputs {
			src := newCachedFileStreamSource(e, task.QueryID, bucket, files)
			streamingSources[tableName] = src
			e.logger.Debug("streaming pre-scanned input",
				"table", tableName, "files", len(files))
		}
		for alias, files := range precompAliasFiles {
			src := newCachedFileStreamSource(e, task.QueryID, bucket, files)
			streamingSources[alias] = src
			e.logger.Debug("streaming pre-computed aggregate",
				"alias", alias, "files", len(files))
		}
		// Phase 3 native-DAG: Task.Inputs carries upstream stage output keyed
		// by scan/alias name. sourceForAlias classifies file patterns and
		// fails fast on planner bugs that mix partitioned and flat outputs.
		for alias, files := range task.Inputs {
			if _, already := streamingSources[alias]; already {
				return fmt.Errorf("alias %q populated by both Inputs and legacy pre-scanned paths", alias)
			}
			src, err := e.sourceForAlias(task, bucket, alias, files)
			if err != nil {
				return fmt.Errorf("source for alias %q: %w", alias, err)
			}
			streamingSources[alias] = src
			e.logger.Debug("streaming stage input",
				"alias", alias, "files", len(files))
		}
		planner.StreamingSources = streamingSources
	}

	// Probe-split pipeline mode: restrict scan files for the probe table.
	// Each worker reads its assigned partition of the probe table while
	// scanning build tables in full.
	if len(task.ScanFileFilter) > 0 {
		planner.ScanFileFilter = task.ScanFileFilter
		e.logger.Debug("probe-split scan file filter",
			"aliases", len(task.ScanFileFilter))
	}

	// Partial aggregate mode: strip top Sort+Limit so each worker produces
	// complete partial aggregates. The coordinator merges and applies final
	// ordering.
	if task.PartialAggregate {
		logicalPlan = logical.StripTopSortLimit(logicalPlan)
		e.logger.Debug("stripped top sort/limit for partial aggregate")
	}

	physPlan, err := planner.Plan(ctx, logicalPlan)
	if err != nil {
		return fmt.Errorf("physical plan: %w", err)
	}
	if physPlan.Cleanup != nil {
		defer physPlan.Cleanup()
	}

	pipeline := physPlan.Pipeline
	if pipeline == nil {
		return nil
	}

	// Build-cache pre-scan tasks (StageID == "build-cache-scan") read whole
	// tables that may not fit in worker memory. Replace the default CollectSink
	// with a streaming shuffle sink that writes each batch to a spill file as
	// it arrives, so memory stays bounded by one batch instead of growing to
	// the full table size. Without this, scanning partsupp at SF100 (~12GB)
	// peaks at ~24GB during serializeBatches and OOM-kills the worker.
	// Build-cache and aggregate-cache pre-scans both produce a cached .wshf
	// file that downstream probe tasks stream from via StreamingSources.
	// They share the streaming-sink path to produce the WSHF format that
	// cachedFileStreamSource expects; the default result path writes WSHC
	// (compressed) which would fail with "invalid shuffle magic".
	if (task.StageID == "build-cache-scan" || task.StageID == "aggregate-cache-compute") && e.spillDir != "" {
		return e.executeBuildCachePreScan(ctx, task, pipeline, result)
	}

	// Native-DAG Gather task: swap CollectSink for gatherReplySink so output
	// streams to the coordinator's reply subject instead of materializing
	// in-process. Schema is captured lazily from the first batch (gather sink
	// copies it on first Consume).
	if task.Type == distributed.TaskTypeGather {
		if task.ReplySubject == "" {
			return fmt.Errorf("gather task missing ReplySubject")
		}
		if e.nc == nil {
			return fmt.Errorf("gather task requires NATS connection")
		}
		pipeline.Sink = newGatherReplySink(e.nc, task.ReplySubject, result.WorkerID, nil).withDataPlane(e.dpClient)
		if err := pipeline.Run(ctx); err != nil {
			return fmt.Errorf("gather pipeline: %w", err)
		}
		return nil
	}

	// Worker stage outputs are consumed columnar via Batches(); without
	// SkipFinalizeToRows, Finalize boxed every collected batch into
	// map[string]any rows that were thrown away — a full extra boxed copy
	// of the stage output, per task.
	collectSink, ok := pipeline.Sink.(*exec.CollectSink)
	if !ok {
		return fmt.Errorf("pipeline sink is not CollectSink")
	}
	collectSink.SkipFinalizeToRows = true

	// Execute the pipeline — same path as standalone mode
	if err := pipeline.Run(ctx); err != nil {
		return fmt.Errorf("pipeline execution: %w", err)
	}

	batches := collectSink.Batches()
	var totalRows int64
	for _, b := range batches {
		totalRows += int64(b.ActiveLen())
	}

	e.logger.Info("pipeline task completed",
		"task_id", task.ID,
		"sql_length", len(task.SQLText),
		"rows", totalRows,
		"batches", len(batches),
	)

	result.NumRows = totalRows
	if totalRows == 0 {
		return nil
	}

	return e.writeBatchResult(ctx, task, batches, result)
}

// executeBuildCachePreScan runs a build-cache pre-scan pipeline with a
// streaming sink that writes batches to a local spill file as they arrive,
// then uploads the file to S3. Avoids the OOM that the default CollectSink
// path triggers when scanning very large build tables.
func (e *Executor) executeBuildCachePreScan(ctx context.Context, task distributed.Task, pipeline *exec.Pipeline, result *distributed.ResultNotification) error {
	streamSink := newShuffleStreamSink(e.spillDir)

	// Replace CollectSink with the streaming sink. The pipeline doesn't care
	// what sink it has — it just calls Consume on each batch.
	//
	// IMPORTANT: do NOT call streamSink.Init here. Pipeline.Run will call
	// p.Sink.Init(ctx) itself; calling it twice would create two temp files
	// (the first one then orphaned and uploaded as a 0-byte blob).
	pipeline.Sink = streamSink

	// Make sure we always close the file handle and remove the spill file,
	// even on error paths. spillPath is captured AFTER Run (after Init).
	defer func() {
		_ = streamSink.Close()
		if path := streamSink.FilePath(); path != "" {
			_ = os.Remove(path)
		}
	}()

	if err := pipeline.Run(ctx); err != nil {
		return fmt.Errorf("build cache pre-scan pipeline: %w", err)
	}

	spillPath := streamSink.FilePath()
	totalRows := streamSink.NumRows()
	e.logger.Info("build cache pre-scan completed",
		"task_id", task.ID,
		"table_sql", task.SQLText,
		"rows", totalRows,
		"spill_file", spillPath,
	)

	result.NumRows = totalRows
	if totalRows == 0 {
		// Empty table: nothing to upload. Coordinator handles the no-rows case.
		return nil
	}
	if spillPath == "" {
		return fmt.Errorf("build cache pre-scan reported %d rows but no spill file path", totalRows)
	}

	// Re-open the spill file for upload (the writer keeps an fd, but the
	// streaming Put needs its own reader positioned at the start).
	f, err := os.Open(spillPath)
	if err != nil {
		return fmt.Errorf("opening spill file for upload: %w", err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat spill file: %w", err)
	}
	if fi.Size() == 0 {
		return fmt.Errorf("build cache pre-scan reported %d rows but spill file is empty", totalRows)
	}
	resultPath := task.ResultPrefix + task.ID + ".wshf"
	if _, err := e.store.Put(ctx, task.ResultBucket, resultPath, f, fi.Size(), "application/octet-stream"); err != nil {
		return fmt.Errorf("uploading build cache result to S3: %w", err)
	}
	result.ResultPath = resultPath
	result.SizeBytes = fi.Size()
	return nil
}

// executeShuffle reads source Parquet files from S3, hash-partitions every row
// on task.ShuffleKeys into task.NumPartitions output .wshf files, and uploads
// each non-empty partition file to S3 under
//
//	<ResultBucket>/<ResultPrefix>/partition=NNNN/<TaskID>.wshf
//
// Populated result fields: ResultFiles, NumRows, SizeBytes.
func (e *Executor) executeShuffle(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	if len(task.ShuffleKeys) == 0 {
		return fmt.Errorf("shuffle task %s: ShuffleKeys must not be empty", task.ID)
	}
	if task.NumPartitions <= 0 {
		return fmt.Errorf("shuffle task %s: NumPartitions must be > 0, got %d", task.ID, task.NumPartitions)
	}
	if len(task.Files) == 0 {
		return fmt.Errorf("shuffle task %s: Files must not be empty", task.ID)
	}

	bucket := task.DataBucket
	if bucket == "" {
		bucket = task.ResultBucket
	}

	// Per-phase timing. With streaming, source-read and sink-consume are
	// interleaved per batch — we capture them as a single stream_ms instead
	// of trying to attribute time between them. finalize_ms and upload_ms
	// remain separately measured.
	tStart := time.Now()
	var tStreamEnd, tFinalizeEnd time.Time

	// Validate input files share a single kind early — a planner bug that
	// mixes parquet and .wshf in one task surfaces here as a clear error.
	inputKind, err := classifyInputFiles(task.Files)
	if err != nil {
		return fmt.Errorf("shuffle task %s: %w", task.ID, err)
	}

	// Streaming source. cachedFileStreamSource handles parquet and .wshf
	// alike, opening one input file at a time. The previous implementation
	// materialised every input batch into a slice before pushing into the
	// partitioning sink — at SF10 that meant ~3.2 GB of lineitem batches
	// resident per shuffle task (8 × ~400 MB filtered scan outputs), which
	// pushed Q04 workers to 8-9 GB RSS and triggered the reap cycle that
	// stalled the query (project_q04_sf10_followup.md, 2026-04-30). With
	// streaming, the working set is bounded by one file's batches plus the
	// sink's per-partition bufio buffers (48 × 256 KB ≈ 12 MB).
	src := newCachedFileStreamSourceWithProjection(e, task.QueryID, bucket, task.Files, task.Columns)
	// Which rows the shuffle's implicit parquet scan must NOT read. A
	// scan-absorbed leaf feeding an exchange reads base-table parquet
	// directly, so a DELETE the manifest recorded has to reach it here or
	// the deleted rows are hash-partitioned into every consumer (#491).
	shuffleDeletes, err := taskDeleteSets(task)
	if err != nil {
		return err
	}
	src.SetDeleteMarkers(shuffleDeletes)
	// What the shuffle's implicit parquet scan is reading. Base-table files
	// written before the declared-schema footer key existed cannot say what
	// nine of their column types are, and the WSHF this task writes would
	// then carry raw storage form to every consumer downstream (#423).
	src.SetDeclaredSchema(execColumns(task.ColumnTypes))
	// Dynamic-filter pushdown for shuffle's implicit parquet scan. Operates
	// at two layers:
	//   - Row-group level (via cachedFileStreamSource.SetDynamicFilters →
	//     RowGroupIter): skips entire row groups whose stats don't intersect
	//     the bloom. Effective when per-row-group key range is small (≤1024)
	//     — typically only at SF100+ where row groups are narrow.
	//   - Row level (via BloomFilterOp inserted in the shuffle loop below):
	//     marks ineligible rows in the selection vector before they reach
	//     the hash-partitioning sink. Effective at any scale because it
	//     prunes pre-shuffle-write. Adaptive-disables if rejection <5% to
	//     avoid paying lookup cost on non-selective workloads.
	// Together they cover the SF1→SF100 selectivity spectrum. Deferred
	// (attach-on-arrival) specs reach BOTH layers too — mid-scan, via the
	// promotion in the read loop below.
	var shuffleBloomOps []*exec.BloomFilterOp
	var pendingShuffleDF []*deferredBloomFilter
	if len(task.DynamicFilters) > 0 {
		ranges, blooms, err := e.materializeDynamicFilters(task.DynamicFilters)
		if err != nil {
			e.logger.Warn("shuffle task: dynamic-filter materialize failed; proceeding without filter",
				"task_id", task.ID, "error", err)
		} else if len(ranges) > 0 || len(blooms) > 0 {
			src.SetDynamicFilters(ranges, blooms)
			for _, bf := range blooms {
				if bf == nil {
					continue
				}
				op := exec.NewBloomFilterOp(bf.Bloom, bf.BloomMask, []string{bf.Column}, bf.UseIntKey)
				if err := op.Init(ctx); err == nil {
					shuffleBloomOps = append(shuffleBloomOps, op)
				}
			}
		}
		// Deferred (attach-on-arrival) specs skip the materialization above —
		// their artifact may not exist yet. Register the singleflight pollers
		// and promote resolved filters mid-scan in the read loop below, into
		// BOTH layers: iterator-level on the source (row-group pruning +
		// prune-aware advises for groups/files not yet read) and a row-level
		// op. Drop-only semantics — batches already read pass unfiltered.
		if dfLateGroupAttach {
			pendingShuffleDF = pendingInSpecOrder(task.DynamicFilters, e.registerDeferredBlooms(task.DynamicFilters))
		}
	}
	if err := src.Init(ctx); err != nil {
		return fmt.Errorf("shuffle task %s: source init: %w", task.ID, err)
	}
	defer src.Close()

	// Appended expression columns (exchange subsumption dedup): compiled
	// once, evaluated per batch, appended ahead of the partitioning sink so
	// the flag ships inside every partition file.
	computedAppenders, err := newComputedColAppenders(task.ComputedCols)
	if err != nil {
		return fmt.Errorf("shuffle task %s: %w", task.ID, err)
	}

	// WSHF-input shuffle projection: chained shuffles decode full-width
	// upstream payloads (projectColumns is parquet-only), so the declared
	// column set is applied post-decode before rows are re-partitioned and
	// re-materialized. ComputedCols/DropCols tasks are ineligible — their
	// expressions may read columns the declaration omits (the dispatcher
	// enforces this too; the guard here keeps the worker safe against
	// older coordinators). Resolved lazily from the first batch's schema.
	pruneWSHF := inputKind != inputKindParquet && len(task.Columns) > 0 &&
		len(task.ComputedCols) == 0 && len(task.DropCols) == 0
	var wshfPrune *exec.ColumnPrune
	wshfPruneResolved := false

	// Set up the spill directory for the sink's partition files.
	spillDir := filepath.Join(e.spillDir, "shuffle-"+task.ID)
	if e.spillDir == "" {
		spillDir = filepath.Join(os.TempDir(), "shuffle-"+task.ID)
	}
	if err := os.MkdirAll(spillDir, 0o755); err != nil {
		return fmt.Errorf("shuffle task %s: creating spill dir: %w", task.ID, err)
	}
	// Cleanup failure must be visible: a leaked partition dir eats spill
	// volume silently until a later query dies on ENOSPC.
	defer func() {
		if rmErr := os.RemoveAll(spillDir); rmErr != nil {
			e.logger.Warn("shuffle spill dir cleanup failed; disk space may leak",
				"task_id", task.ID, "dir", spillDir, "error", rmErr)
		}
	}()

	// Per-task progress reporter — each AddRows call updates the worker's
	// TaskProgress counter, which the worker's per-task progress goroutine
	// reads on a 2s cadence and publishes to coord. Coord's WorkerRegistry
	// treats those messages as heartbeat-equivalent liveness signals
	// (workers.go:138, PR #78). Without per-batch progress here, a shuffle
	// task processing many lineitem inputs goes silent for the entire
	// duration; if the global heartbeat goroutine is also briefly delayed
	// (NATS reconnect, heavy GC), coord reaps the worker after 90s and
	// JetStream redelivers — exactly the Q03 SF10 reap loop observed on the
	// 2026-05-01 streaming-refactor deploy.
	progress := exec.ProgressReporterFromContext(ctx)

	// Sender-side partial aggregation (exchange partial agg): pre-combine
	// rows on the planner-proven keys before partitioning. Rows entering
	// the sink are the operator's flushed partials, so the sink's schema
	// comes from the FIRST FLUSH, never from a raw batch. Kill switch
	// WADJET_EXCHANGE_PARTIAL_AGG=0 is applied at plan time; the worker
	// honors whatever the task carries.
	var partialAgg *cappedPartialAgg
	if len(task.PartialAggKeys) > 0 && len(task.PartialAggSpecs) > 0 {
		partialAgg = newCappedPartialAggPartitioned(task.PartialAggKeys, task.PartialAggSpecs, task.ShuffleKeys, 0)
	}

	// Sink is created lazily on the first non-empty batch — it needs the
	// schema upfront, and discovering it from the first batch lets us avoid
	// any pre-pass over the input.
	var sink *partitionedShuffleSink
	var totalRows int64
	defer func() {
		if sink != nil {
			sink.Close()
		}
	}()
	consumeToSink := func(b *batch.RecordBatch) error {
		if sink == nil {
			sink = newPartitionedShuffleSink(spillDir, task.ShuffleKeys, task.NumPartitions, b.Schema)
			if err := sink.Init(ctx); err != nil {
				return fmt.Errorf("shuffle task %s: init sink: %w", task.ID, err)
			}
		}
		if err := sink.Consume(ctx, b); err != nil {
			return fmt.Errorf("shuffle task %s: consuming batch: %w", task.ID, err)
		}
		return nil
	}
	for {
		b, err := src.Next(ctx)
		if err != nil {
			return fmt.Errorf("shuffle task %s: source next: %w", task.ID, err)
		}
		if b == nil {
			break
		}
		if len(pendingShuffleDF) > 0 {
			pendingShuffleDF = promoteDeferredFilters(ctx, pendingShuffleDF, e.logger, task.ID, task.StageID, true,
				func(op *exec.BloomFilterOp, ranges []exec.DynamicRange, bsf *exec.BloomScanFilter) {
					if op != nil {
						shuffleBloomOps = append(shuffleBloomOps, op)
					}
					src.AddDynamicFilters(ranges, []*exec.BloomScanFilter{bsf})
				})
		}
		n := int64(b.ActiveLen())
		if n == 0 {
			continue
		}
		// Row-level dynamic-filter prune (post-row-group-prune). Applies
		// every bloom in sequence; each modifies b.Sel. An op that returns
		// nil means every row was bloom-rejected — skip the batch entirely.
		if len(shuffleBloomOps) > 0 {
			for _, op := range shuffleBloomOps {
				bb, err := op.Execute(ctx, b)
				if err != nil {
					return fmt.Errorf("shuffle task %s: bloom filter op: %w", task.ID, err)
				}
				if bb == nil {
					b = nil
					break
				}
				b = bb
			}
			if b == nil || b.ActiveLen() == 0 {
				continue
			}
			n = int64(b.ActiveLen())
		}
		if pruneWSHF {
			if !wshfPruneResolved {
				wshfPruneResolved = true
				if keep := wshfShufflePruneKeep(b.Schema, task.Columns, task.ShuffleKeys); keep != nil {
					wshfPrune = exec.NewColumnPrune(keep)
					if initErr := wshfPrune.Init(ctx); initErr != nil {
						return fmt.Errorf("shuffle task %s: init wshf prune: %w", task.ID, initErr)
					}
				}
			}
			if wshfPrune != nil {
				pb, pruneErr := wshfPrune.Execute(ctx, b)
				if pruneErr != nil {
					return fmt.Errorf("shuffle task %s: wshf column prune: %w", task.ID, pruneErr)
				}
				b = pb
			}
		}
		b = applyComputedCols(computedAppenders, task.DropCols, b)
		if partialAgg != nil {
			flushed, aggErr := partialAgg.consume(ctx, b)
			if aggErr != nil {
				return fmt.Errorf("shuffle task %s: %w", task.ID, aggErr)
			}
			for _, fb := range flushed {
				if fb.ActiveLen() == 0 {
					continue
				}
				if err := consumeToSink(fb); err != nil {
					return err
				}
			}
		} else if err := consumeToSink(b); err != nil {
			return err
		}
		totalRows += n
		if progress != nil {
			progress.AddRows(n)
		}
	}
	for _, d := range pendingShuffleDF {
		e.logger.Info("dynamic_filter: late attach never arrived",
			"task_id", task.ID, "stage_id", task.StageID,
			"filter_id", d.filterID, "batches_seen", d.batches)
	}
	if partialAgg != nil {
		flushed, aggErr := partialAgg.drain(ctx)
		if aggErr != nil {
			return fmt.Errorf("shuffle task %s: %w", task.ID, aggErr)
		}
		for _, fb := range flushed {
			if fb.ActiveLen() == 0 {
				continue
			}
			if err := consumeToSink(fb); err != nil {
				return err
			}
		}
		// born_flat / conversions are the group-index layout audit: a bounded
		// sink must show born_flat=true and conversions=0. Anything else on
		// this line is a sink paying a flat->bucketed rehash once per epoch
		// with nothing after it to amortize against (exec/two_level_hash.go,
		// twoLevelBoundedMinGroups).
		e.logger.Info("shuffle partial agg",
			"task_id", task.ID, "in_rows", partialAgg.inRows,
			"out_rows", partialAgg.outRows, "flushes", partialAgg.flushes,
			"dropped_phantom_keys", partialAgg.droppedKeys, "disabled", partialAgg.disabled,
			"born_flat", partialAgg.bornFlat, "group_ceiling", partialAgg.groupCeiling,
			"conversions", partialAgg.conversions, "cap_mb", partialAgg.capBytes/(1<<20))
	}

	tStreamEnd = time.Now()
	if sink == nil {
		// Source produced no rows — nothing to upload.
		return nil
	}
	if err := sink.Finalize(ctx); err != nil {
		return fmt.Errorf("shuffle task %s: finalizing sink: %w", task.ID, err)
	}
	tFinalizeEnd = time.Now()

	// Per-partition accounting for coordinator-side skew detection. Rows
	// come from the sink's counters; bytes are filled from the per-partition
	// stat calls the upload paths below already make.
	result.PartitionRows = sink.PartitionRowCounts()
	result.PartitionBytes = make([]int64, task.NumPartitions)

	// Phase-B async upload: report completion now — every partition file is
	// finalized on local disk — adopt each into the LocalStageCache (peers
	// and the background upload read the adopted copy), and hand the S3
	// PUTs to the upload manager. Consumers overwhelmingly peer-fetch; the
	// durable copy lands in the background and UploadComplete flips the
	// coordinator's per-key durability bits.
	if root, asyncOK := e.asyncUploadEligible(&task); asyncOK {
		var jobs []uploadJob
		for p, localPath := range sink.PartitionFiles() {
			if localPath == "" {
				continue // empty partition
			}
			key := fmt.Sprintf("%spartition=%04d/%s.wshf", task.ResultPrefix, p, task.ID)
			fi, statErr := os.Stat(localPath)
			if statErr != nil {
				return fmt.Errorf("shuffle task %s: stat partition %d: %w", task.ID, p, statErr)
			}
			result.PartitionBytes[p] = fi.Size()
			if job, ok := e.finishStageOutputAsync(ctx, &task, key, localPath, fi.Size(), true, result); ok {
				jobs = append(jobs, job)
				continue
			}
			// Adoption failed (cross-device rename etc.) — this partition
			// must upload synchronously: its local file dies with the task
			// spill dir and nothing else could produce the durable copy.
			// Synchronous fallback: the QUERY is waiting on this PUT, so it
			// runs ungoverned (nil query state, no yield budget — chunkGate
			// pauses only background streams; a nil jobYieldNs is scratch).
			if upErr := e.uploads.uploadOnce(ctx, nil, 0, uploadJob{
				bucket: task.ResultBucket, key: key, srcPath: localPath,
				compress: true, tmpDir: e.spillDir,
			}, nil); upErr != nil {
				return fmt.Errorf("shuffle task %s: partition %d sync-fallback upload: %w", task.ID, p, upErr)
			}
			result.ResultFiles = append(result.ResultFiles, key)
			result.SizeBytes += fi.Size()
		}
		e.uploads.StartTask(root, task.ID, result.WorkerID, jobs, task.UploadPolicy)
		result.NumRows = totalRows
		e.logger.Info("shuffle task completed (async upload pending)",
			"task_id", task.ID, "rows", totalRows,
			"partitions", len(result.ResultFiles), "background_uploads", len(jobs),
			"stream_ms", tStreamEnd.Sub(tStart).Milliseconds(),
			"finalize_ms", tFinalizeEnd.Sub(tStreamEnd).Milliseconds())
		return nil
	}

	// Upload partition files in parallel. Q18 SF1 shuffle-17 produces 8 × 100MB
	// partitions; serial uploads take ~4.4s on FileStore, parallel ~1.4s. The
	// gap is much wider on real S3 where each Put is dominated by network
	// latency rather than throughput. We cap at 8 concurrent so a fanout of 24
	// (typical Q03 SF1) doesn't oversaturate the local disk / S3 endpoint.
	partFiles := sink.PartitionFiles()
	type partResult struct {
		key  string
		size int64
		err  error
	}
	partResults := make([]partResult, len(partFiles))

	const uploadConcurrency = 8
	sem := make(chan struct{}, uploadConcurrency)
	var wg sync.WaitGroup
	for p, localPath := range partFiles {
		if localPath == "" {
			continue // empty partition
		}
		wg.Add(1)
		go func(p int, localPath string) {
			defer wg.Done()
			// Compressing and uploading a partition on a goroutine with no
			// recover above it: unrecovered, a panic here ends the WORKER
			// process rather than the task (#511).
			defer exec.CatchQueryPanic(ctx, "shuffle partition upload", func(err error) {
				partResults[p] = partResult{err: err}
			})
			sem <- struct{}{}
			defer func() { <-sem }()

			key := fmt.Sprintf("%spartition=%04d/%s.wshf", task.ResultPrefix, p, task.ID)

			// Stream-compress, stream-upload, mmap-cache. Replaces the
			// previous os.ReadFile → CompressShuffleData → resultStore.Put
			// chain that held the entire partition payload (compressed AND
			// uncompressed) in Go heap. At SF100 Q05 that chain pinned
			// 8.78 GB of process heap per task (62 % of GOMEMLIMIT, per
			// 2026-05-21 pprof on the reaped worker). Heap cost of this
			// path is bounded by the s2.Writer block buffer (~64 KB)
			// regardless of partition size or count.
			//
			// Stat first to learn the on-disk uncompressed size (used for
			// result.SizeBytes accounting, matching the old len(data)
			// semantics).
			fi, statErr := os.Stat(localPath)
			if statErr != nil {
				partResults[p] = partResult{err: fmt.Errorf("stat partition %d: %w", p, statErr)}
				return
			}
			uncompressedSize := fi.Size()

			// Stream-compress to a sibling temp file. CompressShuffleFile
			// applies the same ≥10 % savings heuristic as the in-memory
			// CompressShuffleData; useCompressed=false signals we should
			// upload the raw source instead.
			compressedPath := localPath + ".s2"
			_, useCompressed, compErr := CompressShuffleFile(localPath, compressedPath)
			if compErr != nil {
				_ = os.Remove(compressedPath)
				partResults[p] = partResult{err: fmt.Errorf("compressing partition %d: %w", p, compErr)}
				return
			}

			var uploadPath string
			if useCompressed {
				// Compression saved enough; drop the raw source and keep
				// the compressed file as the artifact.
				if err := os.Remove(localPath); err != nil {
					e.logger.Warn("shuffle: failed to remove uncompressed partition",
						"task_id", task.ID, "partition", p, "path", localPath, "error", err)
				}
				uploadPath = compressedPath
			} else {
				// Compression didn't pay; drop the temp and upload raw.
				_ = os.Remove(compressedPath)
				uploadPath = localPath
			}

			// Stream the chosen file to S3. The objstore Put signature
			// takes an io.Reader and a size; passing *os.File means S3
			// reads in chunks from the file descriptor with no full-file
			// buffer in heap.
			f, openErr := os.Open(uploadPath)
			if openErr != nil {
				_ = os.Remove(uploadPath)
				partResults[p] = partResult{err: fmt.Errorf("opening partition %d: %w", p, openErr)}
				return
			}
			fi2, statErr2 := f.Stat()
			if statErr2 != nil {
				f.Close()
				_ = os.Remove(uploadPath)
				partResults[p] = partResult{err: fmt.Errorf("stat upload partition %d: %w", p, statErr2)}
				return
			}
			uploadSize := fi2.Size()
			if _, uploadErr := e.store.Put(ctx, task.ResultBucket, key, f, uploadSize, "application/octet-stream"); uploadErr != nil {
				f.Close()
				_ = os.Remove(uploadPath)
				partResults[p] = partResult{err: fmt.Errorf("uploading partition %d: %w", p, uploadErr)}
				return
			}
			f.Close()

			// Adopt the local file into the LocalStageCache so a
			// downstream same-worker consumer mmap's it instead of
			// re-downloading from S3. Adopt renames the file out of the
			// per-task spill dir into the cache's per-query dir; on
			// failure we fall back to removing the local file (S3 is
			// durable). This replaces the heap-resident resultStore.Put
			// path that was the actual SF100 OOM source.
			if e.localCache != nil {
				if adopted := e.localCache.Adopt(task.QueryID, key, uploadPath); adopted == "" {
					_ = os.Remove(uploadPath)
				}
			} else {
				_ = os.Remove(uploadPath)
			}

			partResults[p] = partResult{key: key, size: uncompressedSize}
		}(p, localPath)
	}
	wg.Wait()

	for p := range partResults {
		if partResults[p].err != nil {
			return fmt.Errorf("shuffle task %s: %w", task.ID, partResults[p].err)
		}
		if partResults[p].key == "" {
			continue
		}
		result.ResultFiles = append(result.ResultFiles, partResults[p].key)
		result.SizeBytes += partResults[p].size
		result.PartitionBytes[p] = partResults[p].size
	}

	result.NumRows = totalRows
	tEnd := time.Now()

	e.logger.Info("shuffle task completed",
		"task_id", task.ID,
		"rows", totalRows,
		"partitions", len(result.ResultFiles),
		"size_bytes", result.SizeBytes,
		"stream_ms", tStreamEnd.Sub(tStart).Milliseconds(),
		"finalize_ms", tFinalizeEnd.Sub(tStreamEnd).Milliseconds(),
		"upload_ms", tEnd.Sub(tFinalizeEnd).Milliseconds(),
	)
	return nil
}

// getFileData retrieves raw Parquet bytes with 3-tier caching:
// in-memory result store → LRU cache → object store (S3).
func (e *Executor) getFileData(ctx context.Context, bucket, path string) ([]byte, error) {
	// Tier 1: in-memory result store (same-worker, fastest)
	if e.resultStore != nil {
		if data, ok := e.resultStore.Get(path); ok {
			return data, nil
		}
	}

	// Tier 2: NATS KV result store (cross-worker, ~10ms vs ~500ms for S3)
	if e.resultKV != nil {
		kvKey := natsKVKey(path)
		if entry, kvErr := e.resultKV.Get(ctx, kvKey); kvErr == nil {
			data := entry.Value()
			// Populate LRU cache for subsequent reads
			e.cache.Put(bucket+"/"+path, data)
			return data, nil
		}
	}

	// Tier 3: LRU cache (cached S3 reads)
	cacheKey := bucket + "/" + path
	if data, ok := e.cache.Get(cacheKey); ok {
		return data, nil
	}

	// Tier 4: S3 object store (slowest, ~250-500ms)
	rc, _, err := e.store.Get(ctx, bucket, path)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("reading file: %w", err)
	}

	// Cache the file data
	e.cache.Put(cacheKey, data)
	return data, nil
}

// readParquetFileBatches reads a Parquet file directly into columnar RecordBatches,
// bypassing the map[string]any intermediate. One batch per row group.
// When the store supports range reads and column projection is active, uses
// lazy io.ReaderAt to fetch only the needed column chunks from S3 (5-10x I/O
// reduction on wide tables).
func (e *Executor) readParquetFileBatches(ctx context.Context, bucket, path string, selectedCols []string) ([]*batch.RecordBatch, error) {
	// Check LRU cache first — if the full file is cached, use it directly.
	cacheKey := bucket + "/" + path
	if data, ok := e.cache.Get(cacheKey); ok {
		reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			return nil, err
		}
		return scan.ReadFileBatches(reader, reader.Schema().Columns, selectedCols)
	}

	// For column-pruned queries, use range reads to fetch only needed chunks.
	// This avoids downloading the full file when only a few columns are needed.
	if len(selectedCols) > 0 {
		if ras, ok := e.store.(objstore.ReaderAtStore); ok {
			ra, size, err := ras.GetReaderAt(ctx, bucket, path)
			if err == nil {
				defer ra.Close()
				reader, err := parquet.NewReader(ra, size)
				if err != nil {
					return nil, fmt.Errorf("opening parquet via range read: %w", err)
				}
				return scan.ReadFileBatches(reader, reader.Schema().Columns, selectedCols)
			}
			// Fall through to full download on GetReaderAt error.
		}
	}

	// Fallback: full file download + cache.
	data, err := e.getFileData(ctx, bucket, path)
	if err != nil {
		return nil, err
	}
	reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}
	return scan.ReadFileBatches(reader, reader.Schema().Columns, selectedCols)
}

// readParquetFilesConcurrentBatches reads multiple Parquet files in parallel (up to 8
// goroutines), returning all batches concatenated in file order.
func (e *Executor) readParquetFilesConcurrentBatches(ctx context.Context, bucket string, files []string, selectedCols []string) ([]*batch.RecordBatch, error) {
	if len(files) == 0 {
		return nil, nil
	}
	if len(files) == 1 {
		return e.readParquetFileBatches(ctx, bucket, files[0], selectedCols)
	}

	type result struct {
		batches []*batch.RecordBatch
		err     error
	}
	results := make([]result, len(files))

	const maxConcurrency = 8
	sem := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup

	for i, f := range files {
		wg.Add(1)
		go func(idx int, filePath string) {
			defer wg.Done()
			// Parquet decoding is the classic panic surface and this runs
			// on a goroutine nobody joins for errors — unrecovered it took
			// the worker process with it (#511).
			defer exec.CatchQueryPanic(ctx, "parquet file decode", func(err error) {
				results[idx] = result{err: err}
			})
			sem <- struct{}{}
			defer func() { <-sem }()

			batches, err := e.readParquetFileBatches(ctx, bucket, filePath, selectedCols)
			results[idx] = result{batches: batches, err: err}
		}(i, f)
	}
	wg.Wait()

	var allBatches []*batch.RecordBatch
	for i, r := range results {
		if r.err != nil {
			return nil, fmt.Errorf("reading file %s: %w", files[i], r.err)
		}
		allBatches = append(allBatches, r.batches...)
	}
	return allBatches, nil
}

// readInputFilesBatches reads files that may be in binary shuffle format (.wshf)
// or Parquet format, auto-detecting based on file magic bytes.
func (e *Executor) readInputFilesBatches(ctx context.Context, bucket string, files []string, selectedCols []string) ([]*batch.RecordBatch, error) {
	if len(files) == 0 {
		return nil, nil
	}

	type result struct {
		batches []*batch.RecordBatch
		err     error
	}
	results := make([]result, len(files))

	const maxConcurrency = 8
	sem := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup

	for i, f := range files {
		wg.Add(1)
		go func(idx int, filePath string) {
			defer wg.Done()
			// Decompressing and decoding a shuffle file — untrusted bytes
			// on a goroutine with no recover above it (#511).
			defer exec.CatchQueryPanic(ctx, "shuffle file decode", func(err error) {
				results[idx] = result{err: err}
			})
			sem <- struct{}{}
			defer func() { <-sem }()

			data, err := e.getFileData(ctx, bucket, filePath)
			if err != nil {
				results[idx] = result{err: err}
				return
			}

			data, decErr := DecompressShuffleData(data)
			if decErr != nil {
				results[idx] = result{err: decErr}
				return
			}
			if wshf.IsShuffleFormat(data) {
				batches, err := wshf.DecodeBatches(data)
				results[idx] = result{batches: batches, err: err}
			} else {
				reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
				if err != nil {
					results[idx] = result{err: err}
					return
				}
				schema := reader.Schema().Columns
				batches, err := scan.ReadFileBatches(reader, schema, selectedCols)
				results[idx] = result{batches: batches, err: err}
			}
		}(i, f)
	}
	wg.Wait()

	var allBatches []*batch.RecordBatch
	for i, r := range results {
		if r.err != nil {
			return nil, fmt.Errorf("reading file %s: %w", files[i], r.err)
		}
		allBatches = append(allBatches, r.batches...)
	}
	return allBatches, nil
}

// readParquetFilesConcurrent reads multiple Parquet files in parallel (up to 8
// goroutines), returning all rows concatenated in file order. This significantly
// reduces latency for S3-backed reads where each GET is a network round-trip.
func (e *Executor) serializeBatches(batches []*batch.RecordBatch) ([]byte, error) {
	if len(batches) == 0 {
		return nil, fmt.Errorf("no batches to serialize")
	}

	schema := batches[0].Schema

	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, schema)
	if err := sw.writeHeader(); err != nil {
		return nil, fmt.Errorf("writing header: %w", err)
	}
	for _, b := range batches {
		nRows := b.ActiveLen()
		if nRows == 0 {
			continue
		}
		if b.Sel != nil {
			if err := sw.writeChunk(b.Columns, b.Sel, nRows); err != nil {
				return nil, fmt.Errorf("writing chunk: %w", err)
			}
		} else {
			if err := sw.writeChunk(b.Columns, nil, nRows); err != nil {
				return nil, fmt.Errorf("writing chunk: %w", err)
			}
		}
	}

	// Patch chunk count
	data := buf.Bytes()
	if len(data) >= 8 {
		binary.LittleEndian.PutUint32(data[4:8], sw.numChunks)
	}

	// Compress for inter-node transfer
	return CompressShuffleData(data), nil
}

// writeBatchResult serializes batches and writes via inline/ResultStore/S3 tiering.
func (e *Executor) writeBatchResult(ctx context.Context, task distributed.Task, batches []*batch.RecordBatch, result *distributed.ResultNotification) error {
	data, err := e.serializeBatches(batches)
	if err != nil {
		return err
	}

	result.SizeBytes = int64(len(data))

	// Small result fast path: include inline
	if len(data) <= inlineResultThreshold {
		result.InlineData = data
		return nil
	}

	resultPath := task.ResultPrefix + task.ID + ".wshf"

	// Always write to S3 as the durable store. NATS KV has a 5-minute TTL
	// and 1 GB size cap — entries can expire or be evicted before downstream
	// stages read them (e.g., SF100 Q04 pipeline takes 11+ minutes).
	_, err = e.store.Put(ctx, task.ResultBucket, resultPath, bytes.NewReader(data), int64(len(data)), "application/octet-stream")
	if err != nil {
		return fmt.Errorf("writing result to S3: %w", err)
	}
	result.ResultPath = resultPath

	// Also populate NATS KV as a fast read cache for cross-worker reads.
	// Workers check KV (tier 2, ~10ms) before falling back to S3 (~500ms).
	if e.resultKV != nil && len(data) <= natsKVResultThreshold {
		kvKey := natsKVKey(resultPath)
		e.resultKV.Put(ctx, kvKey, data) // best-effort; S3 is the source of truth
	}

	// Cache locally for same-node reads.
	if e.resultStore != nil {
		e.resultStore.Put(task.QueryID, resultPath, data)
	}
	return nil
}

// natsKVKey converts an S3 result path to a valid NATS KV key.
// NATS KV keys don't support '.' so we replace with '_'.
func natsKVKey(path string) string {
	return strings.ReplaceAll(path, ".", "_")
}

// batchSource wraps a slice of RecordBatches as an exec.Source.
func (e *Executor) collectTaskStats(spill *memory.SpillManager, tracker *memory.Tracker) *distributed.TaskStats {
	stats := &distributed.TaskStats{
		RSS: distributed.ProcessRSS(),
	}

	if spill != nil {
		files := spill.SpilledFiles()
		stats.SpillFiles = len(files)
		for _, f := range files {
			if info, err := os.Stat(f); err == nil {
				stats.SpillBytes += info.Size()
			}
		}
		if e.metrics != nil && stats.SpillFiles > 0 {
			e.metrics.SpillEvents.Add(float64(stats.SpillFiles))
			e.metrics.SpillBytesWritten.Add(float64(stats.SpillBytes))
		}
	}

	if tracker != nil {
		stats.MemUsed = tracker.Used()
		stats.MemBudget = tracker.Budget()
		stats.TrackerPeak = tracker.Peak()
		if e.metrics != nil {
			e.metrics.MemoryBudgetBytes.Set(float64(stats.MemBudget))
			e.metrics.MemoryUsedBytes.Set(float64(stats.MemUsed))
		}
	}

	// Per-operator peak attribution: ask each Spillable registered with the
	// task's SpillManager for its name + current/peak footprint. This is the
	// signal we read from worker logs when investigating which operator
	// pinned the heap on a given task (Q18-class debugging).
	if spill != nil {
		snaps := spill.Inspect()
		if len(snaps) > 0 {
			stats.OperatorPeaks = make([]distributed.OperatorPeak, len(snaps))
			for i, s := range snaps {
				stats.OperatorPeaks[i] = distributed.OperatorPeak{
					Name:     s.Name,
					Peak:     s.OwnedBytes,
					Current:  s.SpillableBytes,
					Owned:    s.OwnedBytes,
					Retained: s.RetainedBytes,
					State:    s.State.String(),
					Closed:   s.Departed,
				}
			}
		}
	}

	return stats
}

// aggregateNeededCols returns the minimal set of columns needed for an
// aggregate task: group-by columns + aggregate input columns. Extracts raw
// column references from expression strings (e.g., "substr(l_shipdate, 1, 4)"
// → "l_shipdate"). Returns nil (read all) if no columns are specified.

// readFileBytes reads a file from the object store into memory.
func (e *Executor) readFileBytes(ctx context.Context, bucket, path string) ([]byte, error) {
	reader, _, err := e.store.Get(ctx, bucket, path)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}
