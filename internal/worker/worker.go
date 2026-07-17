package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/citc-tech/wadjet/internal/dataplane"
	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/diskio"
	"github.com/citc-tech/wadjet/internal/engine/exec"
	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/metrics"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/telemetry"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Config holds worker configuration.
type Config struct {
	NATSUrl          string
	WorkerID         string
	ClusterID        string // cluster this worker belongs to (for federated routing)
	MaxConcurrent    int    // max concurrent tasks
	// MaxTaskDuration caps how long a single task may run before its context
	// is cancelled, guaranteeing its concurrency slot is eventually released
	// even if it wedges. The coordinator's per-task DeadlineUnixNano (the
	// query deadline) takes precedence when supplied; this is the floor for
	// the gRPC dispatch path when no deadline rides the dispatch. 0 = no cap.
	MaxTaskDuration  time.Duration
	CacheBytes       int64  // local LRU cache size
	MemoryBudget     int64  // per-task memory budget in bytes (0 = unlimited, no spill); used as legacy fallback when SharedPoolBudget is unset
	SharedPoolBudget int64  // worker-wide memory pool in bytes (0 = derived as MemoryBudget*MaxConcurrent). All concurrent tasks Reserve against this pool; spill triggers fire on cumulative worker pressure.
	SpillDir         string // directory for spill files (default: os temp dir)
	// DrainTimeout bounds Drain(): how long to wait for in-flight tasks and
	// then pending stage-output uploads before escalating to a hard stop.
	// 0 = unbounded (the platform's kill timeout — e.g. the Kubernetes
	// termination grace period — is the backstop).
	DrainTimeout     time.Duration
	ResultStoreBytes int64  // in-memory result store capacity (0 = disabled, results go to S3)

	// Reservoirs is the Phase-3 system-reservoir registry. When non-nil, the
	// worker registers its cache + result-store reservoirs against it and wires
	// it into the shared SpillManager for ACCOUNTING (Available()/drift). nil =
	// the legacy static-budget path.
	Reservoirs *memory.ReservoirRegistry
	// FloatingBudgetActive enables the deploy-gated floating spill threshold.
	// Default false: ShouldSpillFor stays on the tuned static 40%/90% path even
	// with reservoirs wired (the floating budget is mmap-blind until Phase-4
	// RSS-sampling).
	FloatingBudgetActive bool

	// StreamingShuffleRead decodes WSHF/WSHC exchange inputs directly from
	// the peer/S3 byte stream instead of staging whole files to NVMe +
	// mmap first (docs/design/exchange-streaming-consumption.md §3 D1).
	// Default on at the CLI (SF100-validated); false is the kill switch.
	StreamingShuffleRead bool

	// ScanDecodeAhead decodes parquet row groups ahead of scan consumption
	// with a bounded window instead of one group per pull
	// (docs/design/scan-decode-pipelining.md). Default false pending SF100
	// validation. ScanDecodeAheadBytes bounds decoded-but-unconsumed bytes
	// per scan source; <= 0 selects the engine default (256 MiB).
	ScanDecodeAhead      bool
	ScanDecodeAheadBytes int64

	// MmapRelief enables the Phase-5 MADV_DONTNEED relief of cold mmap'd cache
	// files under heap pressure. Default false: fully dormant (no region
	// tracking, no per-Next cost, no syscall). Deploy-gated.
	MmapRelief bool
	// MmapReliefThresholdMB is the TOTAL process RSS ceiling in MB at/above
	// which relief MADV_DONTNEEDs the coldest mmap to bring RSS back down. Tune
	// below the worker's cgroup memory.max so relief has headroom (e.g. ~16000
	// on a ~20 GB per-proc envelope). 0 leaves the default. Only meaningful when
	// MmapRelief is true.
	MmapReliefThresholdMB int64

	// BoundedDirtyWrites enables windowed sync_file_range writeback (plus
	// FADV_DONTNEED on spill-class files) for all large sequential disk
	// writes, capping each writer's dirty page-cache footprint so kernel
	// reclaim stops evicting the mmap'd cache pages concurrent tasks are
	// walking (see internal/engine/diskio). Default false: writes rely on
	// kernel writeback as before. Deploy-gated.
	BoundedDirtyWrites bool

	// PeerListenAddr enables the streaming-exchange PeerExchange server
	// (worker→worker shuffle fetches, docs/design/streaming-exchange.md)
	// on the given listen address (e.g. ":9095"; ":0" in tests). Empty
	// (the zero value) = no peer server: this worker serves no fetches
	// and advertises no peer address, so coordinators never hint at it.
	PeerListenAddr string
	// PeerAdvertiseAddr overrides the peer address carried in heartbeats.
	// Empty = derived from the bound listener (specific IP, else the first
	// non-loopback unicast IPv4).
	PeerAdvertiseAddr string

	// MorselWorkers controls intra-fragment parallel pipeline consumers per
	// task (morsel-driven execution, docs/design/morsel-execution.md). 0
	// (zero value) and 1 = serial, today's behavior — dormant-safe. -1 =
	// auto: width adapts to fragment input size and idle CPU tokens. N>1 =
	// fixed width of N (bypasses the size gate; testing/benchmark knob).
	// Extra consumers are bounded worker-wide by a CPU-token pool sized
	// GOMAXPROCS−2 so concurrent tasks cannot oversubscribe cores.
	MorselWorkers int
}

// DefaultConfig returns default worker configuration.
func DefaultConfig() Config {
	return Config{
		MaxConcurrent:    4,
		MaxTaskDuration:  30 * time.Minute, // matches the coordinator's default query timeout
		CacheBytes:       256 * 1024 * 1024, // 256 MB
		ResultStoreBytes: 512 * 1024 * 1024, // 512 MB — avoids S3 round-trips for inter-stage results
	}
}

// Worker pulls tasks from NATS, executes them, and publishes results.
// Pool-pressure-gated task pull (see taskLoop). Threshold is just under
// the spill trigger (~75%) so we hold new tasks back BEFORE existing
// tasks have to spill. Backoff is short — the goal is to defer the next
// fetch by a beat, not to stall progress; the pull loop re-enters
// immediately and re-checks pressure on the next iteration.
const (
	poolPressurePullThreshold = 0.70
	poolPressurePullBackoff   = 100 * time.Millisecond
	// dispatchBackpressureWarn is how often a saturated worker logs that it is
	// holding a dispatched task it has no free slot for. Makes a wedged worker
	// visible in its own logs instead of stalling silently (#101).
	dispatchBackpressureWarn = 15 * time.Second
)

type Worker struct {
	config Config
	store  objstore.Store
	nc     *nats.Conn
	// controlNC is an optional second NATS connection used exclusively for
	// the heartbeat publish path. When set, the heartbeat goroutine never
	// shares fate with high-volume data-plane traffic on nc (gather chunks,
	// task results, TaskProgress). When nil, heartbeat falls back to nc.
	// See project_q16_q18_jetstream_dispatch_stall_2026-05-02 for the
	// motivating EC2 observation: workers with bursty data-plane load had
	// their heartbeats fall behind even though publish-into-buffer was
	// fast (the buffer wasn't draining because the connection's write
	// goroutine was wedged behind larger sends).
	controlNC *nats.Conn
	js        jetstream.JetStream
	executor  *Executor
	logger    *slog.Logger
	metrics   *metrics.Metrics

	cancel context.CancelFunc
	// wg tracks the TASK side of the lifecycle: intake loops (which exit on
	// drain) and per-task goroutines. bgWG tracks background services
	// (heartbeat, stats, heap profiler) that exit only on context cancel.
	// The split is what makes Drain possible: wait for wg (tasks finish)
	// while bgWG keeps heartbeating Draining=true, THEN cancel and wait for
	// bgWG. With a single group, Drain's pre-cancel Wait deadlocked on the
	// heartbeat loop.
	wg        sync.WaitGroup
	bgWG      sync.WaitGroup
	drainCh   chan struct{} // closed when drain is requested
	drainOnce sync.Once

	cancelledMu sync.RWMutex
	cancelled   map[string]time.Time // queryID → cancellation time

	activeTasksMu sync.RWMutex
	activeTasks   map[string]struct{} // task IDs currently executing

	// dpClient is the optional data-plane gRPC client. When non-nil, the
	// worker skips the JetStream Fetch loop and instead executes tasks
	// pushed via dpClient's TaskDispatch handler. Gather sinks also prefer
	// gRPC for result delivery in this mode. nil = legacy NATS-only path.
	dpClient *dataplane.Client

	// peerServer serves streaming-exchange FetchShuffle requests from other
	// workers (nil unless Config.PeerListenAddr is set); peerClient issues
	// them and is always constructed (idle without location hints).
	peerServer *dataplane.PeerServer
	peerClient *dataplane.PeerClient

	// CPU profiling state — started/stopped via NATS profile requests.
	profMu  sync.Mutex
	profBuf *bytes.Buffer // nil when not profiling

	otel *telemetry.Provider // nil = no OTel tracing
}

// New creates a new Worker.
func New(cfg Config, store objstore.Store, nc *nats.Conn, js jetstream.JetStream, logger *slog.Logger) *Worker {
	if cfg.WorkerID == "" {
		cfg.WorkerID = "worker-" + uuid.New().String()[:8]
	}
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = 4
	}
	if cfg.MaxTaskDuration <= 0 {
		// Floor so a wedged task always releases its slot even when no
		// per-dispatch deadline rides the task (e.g. the NATS path). Matches
		// the coordinator's default query timeout.
		cfg.MaxTaskDuration = 30 * time.Minute
	}
	if cfg.CacheBytes <= 0 {
		cfg.CacheBytes = 256 * 1024 * 1024
	}
	if logger == nil {
		logger = slog.Default()
	}

	cache := NewLRUCache(cfg.CacheBytes)
	executor := NewExecutor(store, cache, js)
	pool := cfg.SharedPoolBudget
	if pool <= 0 && cfg.MemoryBudget > 0 && cfg.MaxConcurrent > 0 {
		// Legacy callers pass per-task MemoryBudget; reconstruct the
		// worker-wide pool size as `per-task × MaxConcurrent` so all
		// concurrent tasks share one budget. This matches what the
		// per-task budget was originally a slice of.
		pool = cfg.MemoryBudget * int64(cfg.MaxConcurrent)
	}
	executor.SetMemoryBudget(cfg.MemoryBudget, cfg.SpillDir)
	if pool > 0 {
		executor.SetSharedPoolBudget(pool)
	}
	executor.SetLogger(logger)
	executor.SetNATSConn(nc)
	executor.SetMorselWorkers(cfg.MorselWorkers)
	executor.SetStreamingShuffleRead(cfg.StreamingShuffleRead)
	executor.SetScanDecodeAhead(cfg.ScanDecodeAhead, cfg.ScanDecodeAheadBytes)
	// Phase 3: wire the system-reservoir registry for ACCOUNTING (Available()/
	// drift) — this does NOT activate the floating spill threshold; that stays
	// gated on FloatingBudgetActive (default false → tuned static 40%/90% path).
	// Register the LRU cache (live Size accessor) here; SetResultStore registers
	// the result store; both must run before LogInvariant. SetReservoirs must
	// precede SetResultStore so the store reservoir lands in the same registry.
	if cfg.Reservoirs != nil {
		cfg.Reservoirs.Register(memory.NewReservoirFunc("lru-cache/parquet-metadata", cfg.CacheBytes, cache.Size))
		executor.SetReservoirs(cfg.Reservoirs)
		executor.SetFloatingBudgetActive(cfg.FloatingBudgetActive)
	}
	if cfg.ResultStoreBytes > 0 {
		executor.SetResultStore(NewResultStore(cfg.ResultStoreBytes))
	}
	if cfg.Reservoirs != nil {
		cfg.Reservoirs.LogInvariant(logger)
	}
	// Phase 5: configure mmap relief (dormant unless MmapRelief is set).
	SetMmapRelief(cfg.MmapRelief, cfg.MmapReliefThresholdMB<<20)
	// Write-side page-cache pressure control (dormant unless flagged).
	diskio.SetEnabled(cfg.BoundedDirtyWrites)
	// Same-worker LocalStageCache: producers register their local spill
	// files in here after upload, downstream tasks landing on the same
	// worker mmap them directly instead of round-tripping S3. Skipped when
	// no spill dir is configured (tests / minimal embeddings).
	if cfg.SpillDir != "" {
		executor.SetLocalStageCache(NewLocalStageCache(filepath.Join(cfg.SpillDir, "stage-cache")))
	}
	// Best-effort bind to the coordinator's shared result KV bucket so small
	// stage outputs can round-trip via NATS (~10ms) instead of S3 (~500ms).
	// The coordinator creates the bucket in New(); workers just open it.
	// If unavailable (e.g., KV disabled on coord, different NATS cluster),
	// stage writes/reads silently fall back to S3.
	if kv, kvErr := js.KeyValue(context.Background(), "wadjet_results_data"); kvErr == nil {
		executor.SetResultKV(kv)
		logger.Info("worker KV fast-path enabled", "bucket", "wadjet_results_data")
	} else {
		logger.Debug("worker KV fast-path unavailable", "error", kvErr)
	}

	w := &Worker{
		config:      cfg,
		store:       store,
		nc:          nc,
		js:          js,
		executor:    executor,
		logger:      logger,
		cancelled:   make(map[string]time.Time),
		activeTasks: make(map[string]struct{}),
		drainCh:     make(chan struct{}),
		peerClient:  dataplane.NewPeerClient(nil),
	}
	executor.SetPeerClient(w.peerClient)
	if cfg.PeerListenAddr != "" {
		w.peerServer = dataplane.NewPeerServer(dataplane.PeerServerConfig{
			Addr:          cfg.PeerListenAddr,
			AdvertiseAddr: cfg.PeerAdvertiseAddr,
		}, executor, logger)
	}
	return w
}

// SetMetrics attaches Prometheus metrics for spill/memory tracking.
func (w *Worker) SetMetrics(m *metrics.Metrics) {
	w.metrics = m
	w.executor.SetMetrics(m)
}

// SetControlConn provides a dedicated NATS connection for the heartbeat
// publish path. When set, heartbeats go through this connection instead of
// the data-plane nc. Callers should pass a separately-dialed *nats.Conn
// (NOT the same one as nc). Optional — heartbeat falls back to nc when
// unset, preserving the historical single-connection topology used by
// in-process tests.
func (w *Worker) SetControlConn(nc *nats.Conn) {
	w.controlNC = nc
}

// SetDataPlaneClient enables the gRPC data plane for this worker.
// When set:
//   - gather sinks prefer the gRPC stream over the NATS reply subject
//     (Phase B behavior; safe even if dispatch stays on NATS),
//   - the worker skips its JetStream Fetch loop and executes tasks
//     pushed via TaskDispatch on the same stream (Phase C).
//
// nil = legacy NATS-only path (default). Must be called before Start.
func (w *Worker) SetDataPlaneClient(c *dataplane.Client) {
	w.dpClient = c
	w.executor.SetDataPlaneClient(c)
}

// Start begins the worker task loop and heartbeat.
func (w *Worker) Start(ctx context.Context) error {
	ctx, w.cancel = context.WithCancel(ctx)

	// Streaming exchange: bind the peer-fetch listener before the first
	// heartbeat so the advertised address is never stale.
	if w.peerServer != nil {
		if err := w.peerServer.Start(); err != nil {
			return fmt.Errorf("starting peer exchange server: %w", err)
		}
	}

	// Sweep stale build-cache spill files left over from a previous worker
	// crash. Without this, a c7gd.4xlarge worker that crashes mid-query at
	// SF100 leaves multi-GB *.wshf files on the NVMe spill volume; subsequent
	// runs accumulate them and eventually exhaust the disk. Files are named
	// "build-cache-*.wshf" (write-side sink) and "build-cache-load-*.wshf"
	// (read-side mmap source) — both safe to delete on startup since any
	// in-flight reader/writer would be holding an open fd into them.
	if w.config.SpillDir != "" {
		w.sweepStaleBuildCacheFiles()
	}

	// Filter subject is computed even in gRPC mode purely for the startup
	// log line below — gRPC dispatch is routed by worker_id, not subject.
	filterSubject := distributed.SubjectTasksAll
	if w.config.ClusterID != "" {
		filterSubject = distributed.ClusterTasksFilter(w.config.ClusterID)
	}

	// Create a shared durable consumer per cluster. All workers in the same
	// cluster pull from the same consumer — NATS distributes messages across them.
	// WorkQueuePolicy streams don't allow multiple consumers with the same filter.
	// Skipped when the gRPC data plane is in use: coord pushes TaskDispatch
	// over the bidi stream and the JetStream Fetch loop stays dormant.
	var consumer jetstream.Consumer
	if w.dpClient == nil {
		consumerName := "tasks"
		if w.config.ClusterID != "" {
			consumerName = "tasks-" + w.config.ClusterID
		}
		c, err := w.js.CreateOrUpdateConsumer(ctx, distributed.StreamTasks, jetstream.ConsumerConfig{
			Durable:       consumerName,
			FilterSubject: filterSubject,
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       10 * time.Minute,
			MaxDeliver:    3,
		})
		if err != nil {
			return fmt.Errorf("creating consumer: %w", err)
		}
		consumer = c
	}

	// Subscribe to query cancellation messages
	cancelSub, err := w.nc.Subscribe(distributed.CancelSubjectAll(), func(msg *nats.Msg) {
		queryID := string(msg.Data)
		w.cancelledMu.Lock()
		w.cancelled[queryID] = time.Now()
		// Prune on insert: the coordinator broadcasts a cancel for EVERY
		// terminal query (cleanupQuery), so this map gains one entry per
		// query for the process lifetime. 30 minutes comfortably outlives
		// any straggler task the entry exists to suppress.
		cutoff := time.Now().Add(-30 * time.Minute)
		for q, at := range w.cancelled {
			if at.Before(cutoff) {
				delete(w.cancelled, q)
			}
		}
		w.cancelledMu.Unlock()
		// Free per-query state. Cancelled queries won't read any further
		// stage outputs, so their spill files are pure leak otherwise.
		if w.executor.localCache != nil {
			w.executor.localCache.CleanupQuery(queryID)
		}
		if w.executor.broadcastCache != nil {
			w.executor.broadcastCache.CleanupQuery(queryID)
		}
		w.executor.peers.CleanupQuery(queryID)
		// Abort pending background uploads — a terminal query's scratch is
		// about to be purged; landing more of it is waste (and re-leaks).
		w.executor.uploads.CancelQuery(queryID)
		w.logger.Debug("query cancelled", "query_id", queryID)
	})
	if err != nil {
		return fmt.Errorf("subscribing to cancellations: %w", err)
	}

	// Subscribe to query completion messages — same cleanup as cancel, but
	// for queries that finished normally. Coordinator publishes once per
	// query in cleanupQuery().
	completeSub, err := w.nc.Subscribe(distributed.CompleteSubjectAll(), func(msg *nats.Msg) {
		queryID := string(msg.Data)
		if w.executor.localCache != nil {
			n := w.executor.localCache.CleanupQuery(queryID)
			if n > 0 {
				w.logger.Debug("released local stage cache",
					"query_id", queryID, "entries", n)
			}
		}
		if w.executor.broadcastCache != nil {
			if n := w.executor.broadcastCache.CleanupQuery(queryID); n > 0 {
				w.logger.Debug("released broadcast-join cache",
					"query_id", queryID, "entries", n)
			}
		}
		// Also drop in-memory ResultStore entries for this query.
		// Without this, every shuffle output that the coordinator
		// fix `2c22e08` populates into ResultStore stays resident
		// past the query's lifetime, eating per-worker envelope
		// across queries. Caught at SF10 Q04 (2026-04-30 deploy):
		// shuffle outputs from Q01-Q03 stayed in ResultStore,
		// the per-process pool (9.6 GB shared + 512 MB ResultStore
		// + 1 GB LRU + working set) ran out, GC-thrashed, the same
		// task got reaped 10+ times.
		if w.executor.resultStore != nil {
			w.executor.resultStore.CleanupQuery(queryID)
		}
		w.executor.peers.CleanupQuery(queryID)
		w.executor.uploads.CancelQuery(queryID)
	})
	if err != nil {
		return fmt.Errorf("subscribing to completions: %w", err)
	}

	// Subscribe to drain requests (sent by coordinator or admin)
	drainSubject := fmt.Sprintf("wadjet.worker.%s.drain", w.config.WorkerID)
	w.nc.Subscribe(drainSubject, func(msg *nats.Msg) {
		w.logger.Info("received drain request via NATS")
		w.BeginDrain()
		if msg.Reply != "" {
			msg.Respond([]byte("ok"))
		}
	})

	// Subscribe to profile requests (NATS request/reply)
	w.nc.Subscribe(distributed.SubjectProfileStart, func(msg *nats.Msg) {
		w.handleProfileStart(msg)
	})
	w.nc.Subscribe(distributed.SubjectProfileCollect, func(msg *nats.Msg) {
		w.handleProfileCollect(msg)
	})

	// Unsubscribe on shutdown
	go func() {
		<-ctx.Done()
		cancelSub.Unsubscribe()
		completeSub.Unsubscribe()
	}()

	// Start task pull loop
	sem := make(chan struct{}, w.config.MaxConcurrent)

	// Start heartbeat (needs sem to report active task count). Background
	// group: must keep running through a drain so the coordinator sees
	// Draining=true (dispatch exclusion) instead of heartbeat silence
	// (reap + re-dispatch of work this worker is still finishing).
	w.bgWG.Add(1)
	go func() {
		defer w.bgWG.Done()
		w.heartbeatLoop(ctx, sem)
	}()

	// Snapshot a heap profile whenever HeapBackpressureActive fires so a
	// post-deploy analyst can attribute the SF100 22 GB heap-pin to its
	// actual allocation sources (vs. the existing peak_heap_mb tracker
	// number that just confirms "this stage hit 22 GB").
	w.startHeapPressureProfiler(ctx)

	dispatchMode := "nats"
	if w.dpClient != nil {
		dispatchMode = "grpc"
		// Bounded queue between the gRPC recv loop and the per-task
		// goroutines. Capacity = MaxConcurrent so coord can have
		// MaxConcurrent in-flight assignments before HTTP/2 flow control
		// stalls the next Send. The receive path is single-threaded
		// inside the dataplane client, so blocking the handler blocks
		// recv → coord's stream.Send blocks → backpressure propagates.
		pending := make(chan dataplane.TaskDispatch, w.config.MaxConcurrent)
		w.dpClient.RegisterDispatchHandler(func(td dataplane.TaskDispatch) {
			// Fast path: the bounded queue has room.
			select {
			case pending <- td:
				return
			case <-ctx.Done():
				return
			default:
			}
			// Queue full — the worker is at MaxConcurrent. We still block (which
			// applies HTTP/2 flow-control backpressure to coord), but we no
			// longer do so silently: a worker wedged on stuck tasks used to
			// swallow the next dispatch with zero visibility, the invisible half
			// of the Q07 final_aggregate stall (#101). Surface it periodically.
			warn := time.NewTicker(dispatchBackpressureWarn)
			defer warn.Stop()
			for {
				select {
				case pending <- td:
					return
				case <-ctx.Done():
					return
				case <-warn.C:
					w.logger.Warn("dispatch queue full; worker saturated, holding dispatched task",
						"task_id", td.TaskID, "query_id", td.QueryID, "stage_id", td.StageID,
						"max_concurrent", w.config.MaxConcurrent)
				}
			}
		})
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			w.dispatchLoop(ctx, pending, sem)
		}()
	} else {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			w.taskLoop(ctx, consumer, sem)
		}()
	}

	w.logger.Info("worker started",
		"worker_id", w.config.WorkerID,
		"cluster_id", w.config.ClusterID,
		"max_concurrent", w.config.MaxConcurrent,
		"filter_subject", filterSubject,
		"dispatch", dispatchMode,
	)

	// Streaming-shuffle-read marker loop (memo §5): periodic greppable
	// stats line, emitted only when the counters moved.
	if w.config.StreamingShuffleRead {
		w.startShuffleStreamMarkerLoop(ctx)
	}
	if w.config.ScanDecodeAhead {
		w.startScanDecodeAheadMarkerLoop(ctx)
	}

	return nil
}

// startScanDecodeAheadMarkerLoop runs the scan-decode-ahead stats marker
// (docs/design/scan-decode-pipelining.md §5) on bgWG — same contract as
// startShuffleStreamMarkerLoop below: ctx-bound loops must never join
// w.wg or they deadlock Drain.
func (w *Worker) startScanDecodeAheadMarkerLoop(ctx context.Context) {
	w.bgWG.Add(1)
	go func() {
		defer w.bgWG.Done()
		var last [4]int64
		t := time.NewTicker(60 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				groups, windowFulls, pressureStalls, tokenStalls := w.executor.ScanDecodeAheadStats()
				cur := [4]int64{groups, windowFulls, pressureStalls, tokenStalls}
				if cur != last {
					last = cur
					w.logger.Info("scan decode-ahead stats",
						"groups", groups, "window_fulls", windowFulls,
						"pressure_stalls", pressureStalls, "token_stalls", tokenStalls)
				}
			}
		}
	}()
}

// startShuffleStreamMarkerLoop runs the streaming-shuffle-read stats marker
// on bgWG: it exits only on ctx cancel, so it must NOT join w.wg — Drain
// waits on w.wg before cancelling, and a ctx-bound goroutine there deadlocks
// the drain (see the wg/bgWG split comment on the Worker struct).
func (w *Worker) startShuffleStreamMarkerLoop(ctx context.Context) {
	w.bgWG.Add(1)
	go func() {
		defer w.bgWG.Done()
		var lastReads, lastFallbacks, lastSkips int64
		t := time.NewTicker(60 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				reads, fallbacks, skips := w.executor.ShuffleStreamStats()
				if reads != lastReads || fallbacks != lastFallbacks || skips != lastSkips {
					lastReads, lastFallbacks, lastSkips = reads, fallbacks, skips
					w.logger.Info("streaming shuffle read stats",
						"reads", reads, "fallbacks", fallbacks, "skip_resumes", skips)
				}
			}
		}
	}()
}

// dispatchLoop consumes TaskDispatch envelopes pushed by coord over
// the gRPC data plane and runs them against the same per-worker
// MaxConcurrent semaphore the legacy taskLoop uses. The pending queue
// is bounded; when full, the gRPC recv handler blocks → HTTP/2 flow
// control backs the pressure all the way to coord.
func (w *Worker) dispatchLoop(ctx context.Context, in <-chan dataplane.TaskDispatch, sem chan struct{}) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.drainCh:
			// Without this case an IDLE worker never exits the loop on
			// drain — it sits blocked on `in` and Drain's task-group Wait
			// hangs (caught by the SIGTERM e2e, 2026-07-04). A task already
			// popped from `in` still runs to completion below.
			return
		case td := <-in:
			if w.Draining() {
				return
			}
			var task distributed.Task
			if err := distributed.Unmarshal(td.TaskBlob, &task); err != nil {
				w.logger.Error("dataplane: unmarshal task",
					"error", err, "task_id", td.TaskID, "query_id", td.QueryID)
				continue
			}
			// Acquire a concurrency slot. Coord-side scheduling already
			// bounds in-flight count per worker, but the sem also feeds
			// heartbeat's ActiveTasks metric so we keep using it.
			select {
			case <-ctx.Done():
				return
			case sem <- struct{}{}:
			}
			// Memory-aware admission: hold task START while the shared pool
			// is above the pull threshold, mirroring taskLoop's pre-fetch
			// gate (the gRPC path previously had NO pressure gate — tasks
			// started immediately regardless of pool state). Bounded wait:
			// pressure relaxes as in-flight tasks finish and release; if it
			// doesn't within the cap, proceed anyway — running tasks may be
			// waiting on cooperative spill that the new task's relief
			// participation can unblock, and the spill machinery remains the
			// backstop.
			w.waitForPoolHeadroom(ctx, task.EstimatedBytes)
			w.wg.Add(1)
			go func(t distributed.Task, deadlineNano int64) {
				defer w.wg.Done()
				defer func() { <-sem }()
				// Bound the task's lifetime so a wedged task (e.g. one stranded
				// on a dependency) always releases its concurrency slot rather
				// than permanently starving the dispatch loop (#101).
				taskCtx, cancel := w.taskContext(ctx, deadlineNano)
				defer cancel()
				w.executeIncomingTask(taskCtx, t, noopAcker{})
			}(task, td.DeadlineUnixNano)
		}
	}
}

// taskContext derives the execution context for one dispatched task, applying a
// deadline so a wedged task always releases its concurrency slot. Precedence:
// the coordinator-supplied DeadlineUnixNano (the query deadline), then the
// worker's MaxTaskDuration floor; if neither is set the parent context is used
// unchanged.
func (w *Worker) taskContext(parent context.Context, deadlineUnixNano int64) (context.Context, context.CancelFunc) {
	if deadlineUnixNano > 0 {
		return context.WithDeadline(parent, time.Unix(0, deadlineUnixNano))
	}
	if w.config.MaxTaskDuration > 0 {
		return context.WithTimeout(parent, w.config.MaxTaskDuration)
	}
	return context.WithCancel(parent)
}

// poolHeadroomMaxWait caps how long dispatchLoop delays a task start while
// waiting for shared-pool pressure to drop below poolPressurePullThreshold.
// A cap (rather than wait-forever) avoids a stall where in-flight tasks are
// themselves blocked on cooperative spill that needs forward progress.
const poolHeadroomMaxWait = 30 * time.Second

// waitForPoolHeadroom blocks until shared-pool pressure falls below the pull
// threshold AND, when the task carries a size estimate, the pool has room
// for its estimated input footprint. Returns on ctx done or after
// poolHeadroomMaxWait. Mirrors taskLoop's pre-fetch pressure gate for the
// gRPC dispatch path; the size requirement only exists here because the
// gRPC path has the task in hand before start (the NATS pull gate is
// task-blind by construction, and blocking a fetched JetStream message
// risks AckWait redelivery = duplicate execution = more memory, not less).
//
// estBytes <= 0 means unknown — only the pressure threshold applies,
// identical to the pre-estimate behavior.
func (w *Worker) waitForPoolHeadroom(ctx context.Context, estBytes int64) {
	deadline := time.Now().Add(poolHeadroomMaxWait)
	for {
		used, budget := w.executor.SharedPoolStats()
		if budget <= 0 {
			return
		}
		pressureOK := float64(used)/float64(budget) <= poolPressurePullThreshold
		fitsOK := estBytes <= 0 || used+estBytes <= budget
		if pressureOK && fitsOK {
			return
		}
		if time.Now().After(deadline) {
			w.logger.Warn("pool pressure gate timed out; starting task anyway",
				"pool_used_mb", used>>20, "pool_budget_mb", budget>>20,
				"task_estimated_mb", estBytes>>20)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(poolPressurePullBackoff):
		}
	}
}

// BeginDrain marks the worker as draining without blocking: intake loops
// stop pulling new tasks and the next heartbeat advertises Draining=true so
// the coordinator excludes this worker from dispatch. Idempotent. The full
// shutdown sequence is Drain(); process owners typically select on
// DrainRequested() and call Drain when it fires.
func (w *Worker) BeginDrain() {
	w.drainOnce.Do(func() {
		w.logger.Info("worker entering drain mode", "worker_id", w.config.WorkerID)
		close(w.drainCh)
	})
}

// DrainRequested returns a channel that is closed once drain has been
// requested from ANY source: BeginDrain, the NATS drain subject (the
// coordinator sends it on reap), or the /drain admin endpoint.
func (w *Worker) DrainRequested() <-chan struct{} { return w.drainCh }

// Drain performs a graceful shutdown: stop pulling new tasks, let in-flight
// tasks run to completion, flush pending stage-output uploads to the object
// store (so consumers of this worker's outputs keep their durable fallback
// once the peer-exchange server goes away), then stop background services.
// Use this for zero-downtime rolling updates and Kubernetes pod termination
// (SIGTERM). Config.DrainTimeout bounds the whole sequence; on timeout the
// remaining work is aborted exactly like Stop.
func (w *Worker) Drain() {
	w.BeginDrain()

	var deadline <-chan time.Time
	var deadlineAt time.Time
	if w.config.DrainTimeout > 0 {
		deadlineAt = time.Now().Add(w.config.DrainTimeout)
		t := time.NewTimer(w.config.DrainTimeout)
		defer t.Stop()
		deadline = t.C
	}

	// Phase 1: in-flight tasks. The intake loops exit on the drain flag;
	// heartbeats (bgWG) keep publishing Draining=true throughout.
	tasksDone := make(chan struct{})
	go func() { w.wg.Wait(); close(tasksDone) }()
	select {
	case <-tasksDone:
	case <-deadline:
		w.logger.Warn("drain timeout waiting for in-flight tasks; escalating to hard stop",
			"worker_id", w.config.WorkerID, "timeout", w.config.DrainTimeout)
		w.Stop()
		return
	}

	// Phase 2: flush pending async stage-output uploads. Tasks are done, so
	// no new uploads can start; a single pass suffices. Peer fetches are
	// still served during the flush.
	flushCtx := context.Background()
	if !deadlineAt.IsZero() {
		var cancel context.CancelFunc
		flushCtx, cancel = context.WithDeadline(flushCtx, deadlineAt)
		defer cancel()
	}
	if err := w.executor.uploads.Flush(flushCtx); err != nil {
		w.logger.Warn("drain timeout flushing stage-output uploads; remaining uploads cancelled",
			"worker_id", w.config.WorkerID, "error", err)
	}

	// Phase 3: tear down. Consumers mid-fetch fall through to the (now
	// flushed) S3 copies.
	if w.peerServer != nil {
		w.peerServer.Stop()
	}
	if w.peerClient != nil {
		w.peerClient.Close()
	}
	w.executor.uploads.Drain()
	if w.cancel != nil {
		w.cancel()
	}
	w.bgWG.Wait()
	w.logger.Info("worker drained and stopped", "worker_id", w.config.WorkerID)
}

// Draining returns true if the worker is in drain mode.
func (w *Worker) Draining() bool {
	select {
	case <-w.drainCh:
		return true
	default:
		return false
	}
}

// Stop gracefully stops the worker. The shared per-cluster consumer is left in
// place so other workers can continue pulling tasks.
func (w *Worker) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
	w.wg.Wait()
	w.bgWG.Wait()

	if w.peerServer != nil {
		w.peerServer.Stop()
	}
	if w.peerClient != nil {
		w.peerClient.Close()
	}
	w.executor.uploads.Drain()

	w.logger.Info("worker stopped", "worker_id", w.config.WorkerID)
}

// SetTelemetry enables OpenTelemetry tracing on the worker.
func (w *Worker) SetTelemetry(tp *telemetry.Provider) {
	w.otel = tp
}

// sweepStaleBuildCacheFiles deletes leftover build-cache spill files in the
// configured spill directory. Called once at startup. Errors are logged but
// not fatal — the worker can still operate (it just may run out of disk if
// crashes are frequent enough).
func (w *Worker) sweepStaleBuildCacheFiles() {
	dir := w.config.SpillDir
	entries, err := os.ReadDir(dir)
	if err != nil {
		// Spill dir may not exist yet on a fresh worker; that's fine.
		if !os.IsNotExist(err) {
			w.logger.Warn("scanning spill dir for stale build-cache files",
				"dir", dir, "error", err)
		}
		return
	}
	var removed int
	var bytesFreed int64
	for _, e := range entries {
		name := e.Name()
		// Spill-dir artifacts from a prior worker process, all orphaned by
		// definition (nothing in this process references them):
		//   build-cache-*.wshf       — write-side spill from shuffleStreamSink
		//   build-cache-load-*.wshf  — read-side mmap source download
		//   parquet-stream-*.parquet — read-side parquet mmap download
		//   shuffle-<taskID>/        — partitioned shuffle sink output dirs
		//   wadjet-spill/            — SpillManager operator scratch (sort
		//     runs, merge intermediates, window passes, join/agg spills);
		//     normally deleted by the owning operator, but error paths can
		//     orphan them and the dir has no per-task RemoveAll. Contents
		//     are swept, the dir itself kept (an already-constructed
		//     SpillManager holds the path).
		isDir := e.IsDir()
		if isDir && name == "wadjet-spill" {
			r, b := sweepDirContents(filepath.Join(dir, name), w.logger)
			removed += r
			bytesFreed += b
			continue
		}
		switch {
		case !isDir && strings.HasPrefix(name, "build-cache-") && strings.HasSuffix(name, ".wshf"):
		case !isDir && strings.HasPrefix(name, "parquet-stream-") && strings.HasSuffix(name, ".parquet"):
		case isDir && strings.HasPrefix(name, "shuffle-"):
		default:
			continue
		}
		full := filepath.Join(dir, name)
		if info, statErr := e.Info(); statErr == nil && !isDir {
			bytesFreed += info.Size()
		}
		if rmErr := os.RemoveAll(full); rmErr != nil {
			w.logger.Warn("removing stale spill artifact",
				"path", full, "error", rmErr)
			continue
		}
		removed++
	}
	if removed > 0 {
		w.logger.Info("swept stale spill artifacts",
			"dir", dir, "items", removed, "bytes_freed", bytesFreed)
	}
}

// sweepDirContents removes every entry inside dir (keeping dir itself) and
// returns the count and bytes freed. Used for scratch directories whose
// entire contents are prior-process orphans.
func sweepDirContents(dir string, logger *slog.Logger) (int, int64) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, 0
	}
	var removed int
	var bytesFreed int64
	for _, e := range entries {
		full := filepath.Join(dir, e.Name())
		if info, statErr := e.Info(); statErr == nil && !e.IsDir() {
			bytesFreed += info.Size()
		}
		if rmErr := os.RemoveAll(full); rmErr != nil {
			logger.Warn("removing stale spill artifact", "path", full, "error", rmErr)
			continue
		}
		removed++
	}
	return removed, bytesFreed
}

func (w *Worker) taskLoop(ctx context.Context, consumer jetstream.Consumer, sem chan struct{}) {
	// Batch fetch: pull up to available concurrency slots at once to
	// amortize the NATS round-trip overhead.
	for {
		if ctx.Err() != nil {
			return
		}
		// In drain mode, stop pulling new tasks.
		if w.Draining() {
			return
		}

		// Pool-pressure-gated pull: if the worker's shared memory pool is
		// near capacity, hold off on accepting new tasks. Lets in-flight
		// tasks complete + GC reclaim before we add more allocations on top.
		// 2026-05-03 SF10 Q11 stalled because the worker accepted 4 new
		// scan tasks 15s after completing 8 broadcast-join tasks
		// (peak_heap=3.5GB each); the new tasks made zero row progress for
		// 2m10s while the process thrashed. Gating the pull at 70% pool
		// pressure prevents that overlap. Threshold matches the worker's
		// internal spill trigger (~75%), giving slack to cleanly finish
		// what's already in flight before pulling more.
		if used, budget := w.executor.SharedPoolStats(); budget > 0 {
			pressure := float64(used) / float64(budget)
			if pressure > poolPressurePullThreshold {
				select {
				case <-ctx.Done():
					return
				case <-time.After(poolPressurePullBackoff):
				}
				continue
			}
		}

		// Count available slots (non-blocking drain)
		available := 0
		for {
			select {
			case sem <- struct{}{}:
				available++
				if available >= w.config.MaxConcurrent {
					goto fetch
				}
			default:
				goto fetch
			}
		}
	fetch:
		if available == 0 {
			// All slots occupied — wait for one to free up. The drain case
			// matters twice over: without it a saturated worker blocks here
			// through the whole drain, and when a slot finally freed it
			// would pull one MORE task past the Draining check at the loop
			// top (intake leak during drain).
			select {
			case <-ctx.Done():
				return
			case <-w.drainCh:
				return
			case sem <- struct{}{}:
				available = 1
			}
		}

		msgs, err := consumer.Fetch(available, jetstream.FetchMaxWait(500*time.Millisecond))
		if err != nil {
			for i := 0; i < available; i++ {
				<-sem
			}
			if ctx.Err() != nil {
				return
			}
			continue
		}

		dispatched := 0
		for msg := range msgs.Messages() {
			dispatched++
			w.wg.Add(1)
			go func(m jetstream.Msg) {
				defer w.wg.Done()
				defer func() { <-sem }()
				w.handleTask(ctx, m)
			}(msg)
		}

		// Return unused slots
		for i := dispatched; i < available; i++ {
			<-sem
		}

		if msgs.Error() != nil && ctx.Err() == nil {
			w.logger.Debug("fetch returned", "error", msgs.Error())
		}
	}
}

func (w *Worker) isCancelled(queryID string) bool {
	w.cancelledMu.RLock()
	_, ok := w.cancelled[queryID]
	w.cancelledMu.RUnlock()
	return ok
}

// taskAcker abstracts the JetStream message control surface so
// handleTask can run on both the legacy JetStream path and the gRPC
// data-plane path. JetStream wraps a real jetstream.Msg; gRPC uses a
// no-op (no AckWait, no redelivery — coord owns retry via task_id
// idempotency on disconnect).
type taskAcker interface {
	Ack()
	Nak()
	Term()
	InProgress()
}

type jetstreamAcker struct{ msg jetstream.Msg }

func (a jetstreamAcker) Ack()        { a.msg.Ack() }
func (a jetstreamAcker) Nak()        { a.msg.Nak() }
func (a jetstreamAcker) Term()       { a.msg.Term() }
func (a jetstreamAcker) InProgress() { a.msg.InProgress() }

// noopAcker is the gRPC-side acker. The data plane has no
// redelivery model — coord re-dispatches on TCP disconnect, workers
// dedupe on task_id.
type noopAcker struct{}

func (noopAcker) Ack()        {}
func (noopAcker) Nak()        {}
func (noopAcker) Term()       {}
func (noopAcker) InProgress() {}

func (w *Worker) handleTask(ctx context.Context, msg jetstream.Msg) {
	var task distributed.Task
	if err := distributed.Unmarshal(msg.Data(), &task); err != nil {
		w.logger.Error("failed to unmarshal task", "error", err)
		msg.Term()
		return
	}
	w.executeIncomingTask(ctx, task, jetstreamAcker{msg: msg})
}

// executeIncomingTask runs a task previously parsed from either the
// JetStream consumer or the gRPC data-plane stream. The acker bridges
// the two paths' control surfaces.
func (w *Worker) executeIncomingTask(ctx context.Context, task distributed.Task, msg taskAcker) {
	// Skip tasks for cancelled queries (local cache from broadcast).
	// Cancellation arrives via the wadjet.cancel.> pubsub at line 194; if
	// a task races ahead of the broadcast and starts executing, the
	// per-task cancelTicker (500ms cadence at line 570) re-checks
	// w.isCancelled and aborts in-flight. We previously did a synchronous
	// nc.Request(SubjectQueryActive) here as belt-and-suspenders against
	// watchdog-style query kills, but that request shared the data-plane
	// connection — under buffer pressure, the publish-into-buffer
	// completed fast yet the connection's write goroutine wedged behind
	// larger gather sends, so the request hung past JS AckWait (10 min).
	// Each redelivery hit the same wedge and consumed a MaxDeliver attempt;
	// the result was the 10:48-min Q16 stall observed on the 2026-05-02
	// EC2 deploy. Trade the millisecond-window race protection for a
	// non-blocking handleTask entry — wasted work on a stale task is
	// bounded by per-task cancelTicker reactivity (~500ms) plus task
	// duration.
	if w.isCancelled(task.QueryID) {
		w.logger.Debug("skipping task for cancelled query",
			"task_id", task.ID, "query_id", task.QueryID)
		msg.Term()
		return
	}

	// Track active task for heartbeat progress reporting
	w.activeTasksMu.Lock()
	w.activeTasks[task.ID] = struct{}{}
	w.activeTasksMu.Unlock()
	defer func() {
		w.activeTasksMu.Lock()
		delete(w.activeTasks, task.ID)
		w.activeTasksMu.Unlock()
	}()

	// Long-task watcher: one-shot goroutine + heap snapshot if this task
	// runs past longTaskWatchAt. Cancelled here on completion so short
	// tasks pay nothing beyond the goroutine spawn.
	stopWatcher := w.startLongTaskWatcher(ctx, task)
	defer stopWatcher()

	// Inject trace context from the task into the Go context
	logAttrsStart := []any{
		"task_id", task.ID,
		"type", task.Type,
		"query_id", task.QueryID,
	}
	if task.TraceID != "" {
		logAttrsStart = append(logAttrsStart, "trace_id", task.TraceID)
	}
	w.logger.Info("executing task", logAttrsStart...)

	// Create a cancellable context for this task, with trace propagation
	taskCtx, taskCancel := context.WithCancel(ctx)
	if task.TraceID != "" {
		tc := distributed.TraceContext{
			TraceID:    task.TraceID,
			SpanID:     task.SpanID,
			TraceFlags: task.TraceFlags,
		}
		taskCtx = distributed.ContextWithTrace(taskCtx, tc)
	}
	// Per-task progress reporter. Operators along the hot loop (sources,
	// sinks, exchange writers) call AddRows / AddBytes via
	// exec.ProgressReporterFromContext; the heartbeat goroutine below
	// reads it to make AckWait extension conditional on actual forward
	// progress and to publish TaskProgress to coord.
	taskProgress := &TaskProgress{}
	taskCtx = exec.WithProgressReporter(taskCtx, taskProgress)
	defer taskCancel()

	// Start OTel child span linked to coordinator's trace
	if w.otel != nil && task.TraceID != "" {
		var span trace.Span
		taskCtx, span = w.otel.StartRemoteSpan(taskCtx, "worker.ExecuteTask",
			task.TraceID, task.SpanID,
			attribute.String("task.id", task.ID),
			attribute.String("task.type", string(task.Type)),
			attribute.String("query.id", task.QueryID),
			attribute.String("worker.id", w.config.WorkerID),
		)
		defer span.End()
	}

	// Monitor for cancellation, publish per-task progress, and extend
	// JetStream AckWait — but only when the task is actually making
	// forward progress.
	//
	// Three timers run inside one select:
	//   - cancelTicker (500ms): poll for query-level cancellation.
	//   - progressTicker (2s): publish TaskProgress to coord with the
	//       latest row/byte counters, so coord-side awaitStageProgress
	//       can distinguish slow-but-healthy from wedged.
	//   - ackTicker (2min): extend JetStream AckWait via msg.InProgress
	//       — IFF the task has reported progress in the last
	//       progressIdleThreshold. A wedged task (worker process alive
	//       but stuck in a retry loop or syscall) eventually stops
	//       reporting progress; after that we stop extending and
	//       AckWait expires, JetStream redelivers to a healthy worker.
	//
	// progressIdleThreshold is a worker-side floor. Coord-side
	// per-task idle detection runs at a longer threshold (60s) so a
	// brief stall doesn't immediately cancel the task; the worker
	// just stops cementing AckWait so JetStream can decide.
	const (
		progressIdleThreshold   = 60 * time.Second
		maxInProgressExtensions = 5 // belt-and-suspenders if the progress signal stops working
	)
	go func() {
		cancelTicker := time.NewTicker(500 * time.Millisecond)
		defer cancelTicker.Stop()
		progressTicker := time.NewTicker(2 * time.Second)
		defer progressTicker.Stop()
		ackTicker := time.NewTicker(2 * time.Minute)
		defer ackTicker.Stop()
		var lastPublishedRows int64
		extensions := 0
		// Mark progress at task start so the first 60s aren't treated as idle
		// (operators may take time to start emitting batches; the task is alive).
		taskProgress.lastUpdateNano.Store(time.Now().UnixNano())
		for {
			select {
			case <-taskCtx.Done():
				return
			case <-cancelTicker.C:
				if w.isCancelled(task.QueryID) {
					taskCancel()
					return
				}
			case <-progressTicker.C:
				rows, bytesProc, _ := taskProgress.snapshot()
				if rows == lastPublishedRows {
					continue // no new progress since last publish; skip
				}
				lastPublishedRows = rows
				publishTaskProgress(w.nc, w.dpClient, distributed.TaskProgress{
					QueryID:        task.QueryID,
					StageID:        task.StageID,
					TaskID:         task.ID,
					WorkerID:       w.config.WorkerID,
					RowsProcessed:  rows,
					BytesProcessed: bytesProc,
					Timestamp:      time.Now(),
				})
			case <-ackTicker.C:
				_, _, lastUpdate := taskProgress.snapshot()
				if !lastUpdate.IsZero() && time.Since(lastUpdate) > progressIdleThreshold {
					w.logger.Warn("task progress idle; stopping AckWait extension",
						"task_id", task.ID, "query_id", task.QueryID,
						"idle_for", time.Since(lastUpdate).Round(time.Second))
					return
				}
				if extensions >= maxInProgressExtensions {
					w.logger.Warn("task InProgress cap reached; allowing JetStream redelivery",
						"task_id", task.ID, "query_id", task.QueryID,
						"extensions", extensions)
					return
				}
				msg.InProgress()
				extensions++
			}
		}
	}()

	// Recover from panics in task execution to prevent crashing
	// the entire worker process on schema mismatches or other bugs.
	var result distributed.ResultNotification
	func() {
		defer func() {
			if r := recover(); r != nil {
				stack := string(debug.Stack())
				w.logger.Error("task panicked",
					"task_id", task.ID,
					"query_id", task.QueryID,
					"panic", fmt.Sprintf("%v", r),
					"stack", stack,
				)
				result = distributed.ResultNotification{
					TaskID:    task.ID,
					QueryID:   task.QueryID,
					StageID:   task.StageID,
					WorkerID:  w.config.WorkerID,
					Error:     fmt.Sprintf("task panicked: %v\n%s", r, stack),
					Duration:  0,
					Timestamp: time.Now(),
				}
			}
		}()
		result = w.executor.Execute(taskCtx, task, w.config.WorkerID)
	}()

	// Propagate trace context to result notification
	result.TraceID = task.TraceID
	result.SpanID = task.SpanID

	// Publish result notification
	subject := distributed.ResultSubject(task.QueryID, task.StageID, task.ID)
	data, err := distributed.Marshal(result)
	if err != nil {
		w.logger.Error("failed to marshal result", "error", err)
		w.publishDLQ(task, "marshal_error", fmt.Sprintf("marshal result: %v", err))
		msg.Nak()
		return
	}

	// Request/reply: coordinator ACKs receipt so we know the result arrived.
	// Retry with backoff if no response — eliminates silent result loss that
	// previously required the stale-stage watchdog.
	var publishErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 2 * time.Second)
			w.logger.Warn("retrying result publish",
				"task_id", task.ID, "attempt", attempt+1)
		}
		_, publishErr = w.nc.Request(subject, data, 10*time.Second)
		if publishErr == nil {
			break
		}
	}
	if publishErr != nil {
		w.logger.Error("failed to publish result after retries",
			"error", publishErr, "task_id", task.ID)
		w.publishDLQ(task, "publish_error", fmt.Sprintf("publish result: %v", publishErr))
		msg.Nak()
		return
	}

	// Publish failed tasks to the DLQ for inspection
	if !result.Success {
		reason := "execution_error"
		if len(result.Error) >= 13 && result.Error[:13] == "task panicked" {
			reason = "panic"
		}
		w.publishDLQ(task, reason, result.Error)
	}

	msg.Ack()

	// Force GC after memory-intensive tasks to reclaim build/probe batches
	// and hash table memory before the next task allocates. Without this,
	// garbage from the previous task can push RSS past physical memory
	// before Go's GC cycle triggers (gogc=off + GoMemLimit means GC only
	// fires at the soft limit, by which time the OS may already OOM-kill).
	//
	// Native-DAG (TaskTypeStage) was missing from this match — Q18 SF10
	// 2026-04-25 confirmed num_gc=0 for the entire 72s run, heap climbed
	// monotonically to 19 GB then OS-killed the coord. Including TaskTypeStage
	// ensures the same per-task GC discipline that legacy task types get.
	shouldGC := false
	switch task.Type {
	case "join", "aggregate", "sort", "window":
		shouldGC = true
	case distributed.TaskTypeStage:
		switch task.StageType {
		case "hash_join", "broadcast_join", "sort_merge_join", "aggregate", "final_aggregate", "sort", "merge_sort", "window":
			shouldGC = true
		}
	}
	if shouldGC {
		runtime.GC()
	}

	logAttrsEnd := []any{
		"task_id", task.ID,
		"success", result.Success,
		"rows", result.NumRows,
		"duration", result.Duration,
	}
	if task.StageID != "" {
		logAttrsEnd = append(logAttrsEnd, "stage_id", task.StageID)
	}
	if task.StageType != "" {
		logAttrsEnd = append(logAttrsEnd, "stage_type", task.StageType)
	}
	// PeakHeapMB is the peak Go heap during this task — sampled at 50ms
	// cadence by taskPeakHeapTracker (executor.go:200). For Q18-class
	// memory-constrained-scaling investigations, this is the per-stage
	// signal that lets us locate which task's hash table / build cache /
	// scan output is the runaway.
	if result.TaskStats != nil && result.TaskStats.PeakHeapMB > 0 {
		logAttrsEnd = append(logAttrsEnd, "peak_heap_mb", result.TaskStats.PeakHeapMB)
	}
	// TrackerPeak is the peak of Reserve-tracked bytes for this task — a
	// narrower signal than PeakHeapMB (excludes non-tracked allocations like
	// ResultStore buffering). OperatorPeaks breaks that down per registered
	// Spillable, surfacing which HashJoin/HashAggregate/Sort owns the
	// high-water mark.
	if result.TaskStats != nil && result.TaskStats.TrackerPeak > 0 {
		logAttrsEnd = append(logAttrsEnd, "tracker_peak_mb", result.TaskStats.TrackerPeak/(1<<20))
	}
	if result.TaskStats != nil && len(result.TaskStats.OperatorPeaks) > 0 {
		parts := make([]string, len(result.TaskStats.OperatorPeaks))
		for i, op := range result.TaskStats.OperatorPeaks {
			// Keep the existing %s=%dMB(peak)/%dMB(now) shape (entries are
			// comma-joined, so intra-token extras use ';') for grep continuity;
			// append the Phase-2 owned/state breakdown.
			parts[i] = fmt.Sprintf("%s=%dMB(peak)/%dMB(now);owned=%dMB;state=%s",
				op.Name, op.Peak/(1<<20), op.Current/(1<<20), op.Owned/(1<<20), op.State)
		}
		logAttrsEnd = append(logAttrsEnd, "operator_peaks", strings.Join(parts, ","))
	}
	// Phase-4 per-task accounting drift + non-heap (mmap) resident working set.
	if result.TaskStats != nil && result.TaskStats.DriftMB != 0 {
		logAttrsEnd = append(logAttrsEnd, "drift_mb", result.TaskStats.DriftMB)
	}
	if result.TaskStats != nil && result.TaskStats.MmapRSSMB > 0 {
		logAttrsEnd = append(logAttrsEnd, "mmap_rss_mb", result.TaskStats.MmapRSSMB)
	}
	if task.TraceID != "" {
		logAttrsEnd = append(logAttrsEnd, "trace_id", task.TraceID)
	}
	if result.Error != "" {
		logAttrsEnd = append(logAttrsEnd, "error", result.Error)
	}
	// Late-materialization engagement marker (cumulative per process,
	// monotonic): lets an A/B arm prove from worker logs that view batches
	// were actually emitted, instead of inferring from wall-clock deltas.
	// Zero-valued attrs are omitted so flag-off logs are byte-identical.
	if emitted := exec.LateMatBatchesEmitted.Load(); emitted > 0 {
		logAttrsEnd = append(logAttrsEnd, "late_mat_batches", emitted,
			"late_mat_flattens", exec.LateMatFlattens.Load(),
			"late_mat_serialized_cols", exec.LateMatViewColumnsSerialized.Load())
	}
	w.logger.Info("task completed", logAttrsEnd...)
}

// publishDLQ sends a failed task to the dead-letter queue for later inspection.
func (w *Worker) publishDLQ(task distributed.Task, reason, errMsg string) {
	taskData, _ := json.Marshal(task)
	entry := distributed.DLQEntry{
		EntryID:   uuid.NewString(),
		TaskID:    task.ID,
		QueryID:   task.QueryID,
		StageID:   task.StageID,
		WorkerID:  w.config.WorkerID,
		TaskType:  task.Type,
		Error:     errMsg,
		Reason:    reason,
		TaskData:  taskData,
		Timestamp: time.Now(),
	}
	data, err := json.Marshal(entry)
	if err != nil {
		w.logger.Error("failed to marshal DLQ entry", "task_id", task.ID, "error", err)
		return
	}
	subj := distributed.DLQSubject(task.QueryID, task.ID)
	if err := w.nc.Publish(subj, data); err != nil {
		w.logger.Error("failed to publish to DLQ", "task_id", task.ID, "error", err)
	}
}

// statsCache holds the slow-to-collect process statistics that the
// heartbeat reports. A separate goroutine refreshes it on a coarser
// cadence so the hot heartbeat path never has to call runtime.ReadMemStats
// (which does a stop-the-world pause) or walk the spill directory while
// the worker is in heavy compute. Under SF10/SF100 with GOGC=off, in-line
// ReadMemStats can stall for minutes while back-to-back GCs run; coord
// then reaps the worker as stale even though it's alive and progressing.
type statsCache struct {
	mu            sync.RWMutex
	allocBytes    int64
	sysBytes      int64
	mallocs       uint64
	rss           int64
	numGoroutines int
	spillBytes    int64
	// GC pressure signals sampled alongside the existing fields. Used by the
	// stats logger so worker logs surface periods where the GC is taking
	// significant CPU — the SF100 Q17 hang investigation needed this signal
	// to confirm whether 90s heartbeat gaps were real GC stalls or NATS-side.
	numGC        uint32
	pauseTotalNs uint64
	// heapInuse is the in-use heap-span figure (Phase 4) used for the
	// accounting-drift and mmap-RSS gauges. Distinct from allocBytes (ms.Alloc,
	// which sawtooths with the GC cycle and feeds MemoryUsed/alloc_mb).
	heapInuse int64
}

func (s *statsCache) refresh(spillDir string) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms) // STW; only this goroutine pays the cost
	rss := distributed.ProcessRSS()
	ng := distributed.NumGoroutines()
	spill := distributed.DirDiskUsage(spillDir)
	s.mu.Lock()
	s.allocBytes = int64(ms.Alloc)
	s.sysBytes = int64(ms.Sys)
	s.mallocs = ms.Mallocs
	s.rss = rss
	s.numGoroutines = ng
	s.spillBytes = spill
	s.numGC = ms.NumGC
	s.pauseTotalNs = ms.PauseTotalNs
	s.heapInuse = int64(ms.HeapInuse)
	s.mu.Unlock()
}

func (s *statsCache) snapshot() (alloc, sys int64, mallocs uint64, rss int64, ng int, spill int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.allocBytes, s.sysBytes, s.mallocs, s.rss, s.numGoroutines, s.spillBytes
}

func (w *Worker) statsRefreshLoop(ctx context.Context, cache *statsCache) {
	// Initial snapshot so the first heartbeat after startup carries real
	// values rather than zeros.
	cache.refresh(w.config.SpillDir)
	var prevGC uint32
	var prevPauseNs uint64
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cache.refresh(w.config.SpillDir)
			// Surface GC pressure deltas in the worker log so the next
			// SF100 Q17-class hang can be diagnosed without instrumenting
			// the wire format. Logged every 30s alongside refresh.
			cache.mu.RLock()
			alloc, rss := cache.allocBytes, cache.rss
			heapInuse := cache.heapInuse
			ng := cache.numGoroutines
			gcDelta := cache.numGC - prevGC
			pauseDeltaNs := cache.pauseTotalNs - prevPauseNs
			prevGC = cache.numGC
			prevPauseNs = cache.pauseTotalNs
			cache.mu.RUnlock()

			// Phase 4 (observability only — drives no spill/relief decision):
			// accounting drift = HeapInuse − (operator owned + reservoir actual),
			// the unaccounted Go-heap residual; mmap RSS = RSS − HeapInuse, the
			// non-heap resident working set (parquet/shuffle page cache) Phase 5
			// will MADV-relieve.
			driftMB, driftPct, mmapMB := computeStatsGauges(heapInuse, rss, w.executor.HeapDrift(heapInuse))

			w.logger.Info("worker stats",
				"alloc_mb", alloc/1024/1024,
				"rss_mb", rss/1024/1024,
				"heap_inuse_mb", heapInuse/(1<<20),
				"mmap_rss_mb", mmapMB,
				"accounting_drift_mb", driftMB,
				"accounting_drift_pct", int64(driftPct*100),
				"goroutines", ng,
				"gc_delta", gcDelta,
				"gc_pause_delta_ms", pauseDeltaNs/1_000_000)

			// Phase-5 mmap relief (dormant unless --mmap-relief): when TOTAL
			// process RSS exceeds the configured ceiling, MADV_DONTNEED the
			// coldest mappings to bring RSS back to the ceiling. Only cold mmap
			// page cache is reclaimable, so we target (RSS − ceiling) bytes of it.
			//
			// The trigger is RSS-vs-ceiling, NOT heap pressure: mmap'd page cache
			// is not Go heap, so HeapAlloc/GOMEMLIMIT stays low even at multi-GB
			// mmap — gating on HeapPressureExceeded() meant relief never fired
			// (SF100 Run 2, 2026-06-01: worker hit 9.4 GB mmap_rss > threshold but
			// relief_events=0 because Go heap was nowhere near GOMEMLIMIT). RSS is
			// what counts toward the cgroup memory.max / OOM, so it is the right
			// pressure signal. The ceiling is deploy-tuned per worker envelope,
			// set below memory.max so relief has headroom to act.
			if mmapReliefEnabled.Load() {
				ceiling := mmapReliefThresholdBytes.Load()
				if ceiling > 0 && rss > ceiling {
					if freed := relieveMmap(rss - ceiling); freed > 0 {
						w.logger.Warn("mmap relief",
							"freed_mb", freed/(1<<20),
							"rss_mb", rss/(1<<20),
							"mmap_rss_mb", mmapMB,
							"ceiling_mb", ceiling/(1<<20))
					}
				}
			}

			// Drift WARN is greppable, not pageable (memo §3/C16). Critical at
			// >50%, high at >20%. Negative drift (sample skew) never trips these.
			if driftPct > 0.50 {
				w.logger.Warn("accounting drift critical",
					"accounting_drift_mb", driftMB,
					"accounting_drift_pct", int64(driftPct*100),
					"heap_inuse_mb", heapInuse/(1<<20))
			} else if driftPct > 0.20 {
				w.logger.Warn("accounting drift high",
					"accounting_drift_mb", driftMB,
					"accounting_drift_pct", int64(driftPct*100))
			}
		}
	}
}

// computeStatsGauges derives the Phase-4 log gauges from a stats sample.
// driftBytes is HeapInuse − accounted (from SpillManager.HeapDrift, supplied by
// the caller so this stays a pure, unit-testable function). driftPct is the
// raw fraction (caller logs int percent and uses the float for thresholds);
// mmapMB = max(0, RSS − HeapInuse) floored to MB.
func computeStatsGauges(heapInuse, rss, driftBytes int64) (driftMB int64, driftPct float64, mmapMB int64) {
	driftMB = driftBytes / (1 << 20)
	if heapInuse > 0 {
		driftPct = float64(driftBytes) / float64(heapInuse)
	}
	mmap := rss - heapInuse
	if mmap < 0 {
		mmap = 0
	}
	mmapMB = mmap / (1 << 20)
	return driftMB, driftPct, mmapMB
}

func (w *Worker) heartbeatLoop(ctx context.Context, sem chan struct{}) {
	cache := &statsCache{}
	w.bgWG.Add(1)
	go func() {
		defer w.bgWG.Done()
		w.statsRefreshLoop(ctx, cache)
	}()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	tickCounter := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tickCounter++

			// Read cached stats — never blocks on STW or filesystem walk,
			// so the hot heartbeat path keeps firing on its 10s cadence
			// even while the stats refresher is paused inside ReadMemStats
			// or the spill walker. Coord sees Timestamp tick forward and
			// keeps the worker marked alive.
			alloc, sys, mallocs, rss, ng, spill := cache.snapshot()

			// Snapshot active task IDs for progress reporting.
			w.activeTasksMu.RLock()
			taskIDs := make([]string, 0, len(w.activeTasks))
			for id := range w.activeTasks {
				taskIDs = append(taskIDs, id)
			}
			w.activeTasksMu.RUnlock()

			poolUsed, poolBudget := w.executor.SharedPoolStats()
			hb := distributed.WorkerHeartbeat{
				WorkerID:      w.config.WorkerID,
				ClusterID:     w.config.ClusterID,
				MaxConcurrent: w.config.MaxConcurrent,
				ActiveTasks:   len(sem),
				ActiveTaskIDs: taskIDs,
				MemoryUsed:    alloc,
				MemoryTotal:   sys,
				PoolUsed:      poolUsed,
				PoolBudget:    poolBudget,
				RSS:           rss,
				NumGoroutines: ng,
				Mallocs:       mallocs,
				SpillDiskUsed: spill,
				Draining:      w.Draining(),
				PeerAddr:      w.peerAdvertiseAddr(),
				Timestamp:     time.Now(),
			}

			data, err := distributed.Marshal(hb)
			if err != nil {
				continue
			}

			// Heartbeat uses the control-plane connection when set; the
			// dedicated connection isolates the publish path from
			// data-plane buffer pressure (gather chunks, large task
			// results) that previously left coord blind to a still-alive
			// worker.
			hbConn := w.controlNC
			if hbConn == nil {
				hbConn = w.nc
			}
			hbConn.Publish(distributed.SubjectHeartbeat, data)

			// Reap old cancellation entries every ~60s (6 heartbeat ticks)
			if tickCounter%6 == 0 {
				w.reapCancelled(10 * time.Minute)
			}
		}
	}
}

// peerAdvertiseAddr returns the peer-exchange address to carry in
// heartbeats, or "" when this worker doesn't serve peer fetches.
func (w *Worker) peerAdvertiseAddr() string {
	if w.peerServer == nil {
		return ""
	}
	return w.peerServer.AdvertiseAddr()
}

// PeerExchangeAddr exposes the advertised peer-exchange address ("" when
// peer serving is disabled or the listener isn't bound yet). Tests use it
// to bootstrap coordinator heartbeats ahead of the first real one.
func (w *Worker) PeerExchangeAddr() string {
	return w.peerAdvertiseAddr()
}

// PeerFetchStats returns the streaming-exchange read-tier counters: inputs
// served via peer fetch, and hinted fetches that fell through to S3.
func (w *Worker) PeerFetchStats() (hits, fallthroughs int64) {
	return w.executor.PeerFetchHits(), w.executor.PeerFetchFallthroughs()
}

// UploadStats returns the Phase-B background-upload counters: files landed,
// files cancelled by query completion, files abandoned after retries.
func (w *Worker) UploadStats() (completed, cancelled, failed int64) {
	return w.executor.uploads.UploadStats()
}

// ShuffleStreamStats returns the streaming-shuffle-read counters:
// streaming opens, staged fallbacks, batches skipped by fallbacks.
func (w *Worker) ShuffleStreamStats() (reads, fallbacks, skipResumes int64) {
	return w.executor.ShuffleStreamStats()
}

// reapCancelled removes cancellation entries older than maxAge.
func (w *Worker) reapCancelled(maxAge time.Duration) {
	w.cancelledMu.Lock()
	defer w.cancelledMu.Unlock()

	cutoff := time.Now().Add(-maxAge)
	for id, cancelledAt := range w.cancelled {
		if cancelledAt.Before(cutoff) {
			delete(w.cancelled, id)
		}
	}
}

// WorkerProfile is the JSON envelope for profile data sent over NATS.
type WorkerProfile struct {
	WorkerID string `json:"worker_id"`
	CPU      []byte `json:"cpu,omitempty"`   // pprof CPU profile (gzip-compressed)
	Heap     []byte `json:"heap,omitempty"`  // pprof heap profile (gzip-compressed)
	Block    []byte `json:"block,omitempty"` // pprof block profile; empty unless WADJET_BLOCK_PROFILE_RATE set
	Mutex    []byte `json:"mutex,omitempty"` // pprof mutex profile; empty unless WADJET_MUTEX_PROFILE_FRACTION set
}

// handleProfileStart begins CPU profiling to an in-memory buffer.
func (w *Worker) handleProfileStart(msg *nats.Msg) {
	w.profMu.Lock()
	defer w.profMu.Unlock()

	if w.profBuf != nil {
		// Already profiling — stop the old one first
		pprof.StopCPUProfile()
	}

	w.profBuf = &bytes.Buffer{}
	if err := pprof.StartCPUProfile(w.profBuf); err != nil {
		w.logger.Error("failed to start CPU profile", "error", err)
		w.profBuf = nil
		msg.Respond([]byte("error: " + err.Error()))
		return
	}
	w.logger.Info("CPU profiling started")
	msg.Respond([]byte("ok"))
}

// handleProfileCollect stops CPU profiling, collects a heap snapshot,
// and responds with both profiles as JSON.
func (w *Worker) handleProfileCollect(msg *nats.Msg) {
	resp := w.collectProfileEnvelope()
	data, err := json.Marshal(resp)
	if err != nil {
		msg.Respond([]byte("error: " + err.Error()))
		return
	}
	msg.Respond(data)
}

// collectProfileEnvelope stops any in-flight CPU profile and snapshots
// heap, block, and mutex profiles into a WorkerProfile envelope.
func (w *Worker) collectProfileEnvelope() WorkerProfile {
	w.profMu.Lock()
	defer w.profMu.Unlock()

	var cpuData []byte
	if w.profBuf != nil {
		pprof.StopCPUProfile()
		cpuData = w.profBuf.Bytes()
		w.profBuf = nil
		w.logger.Info("CPU profiling stopped", "bytes", len(cpuData))
	}

	// Heap snapshot
	runtime.GC()
	var heapBuf bytes.Buffer
	pprof.WriteHeapProfile(&heapBuf)

	// Block/mutex profiles carry records only when the env-gated samplers
	// (WADJET_BLOCK_PROFILE_RATE / WADJET_MUTEX_PROFILE_FRACTION) are on;
	// the Count gate keeps the envelope free of empty profiles otherwise.
	var blockBuf, mutexBuf bytes.Buffer
	if p := pprof.Lookup("block"); p != nil && p.Count() > 0 {
		p.WriteTo(&blockBuf, 0)
	}
	if p := pprof.Lookup("mutex"); p != nil && p.Count() > 0 {
		p.WriteTo(&mutexBuf, 0)
	}

	return WorkerProfile{
		WorkerID: w.config.WorkerID,
		CPU:      cpuData,
		Heap:     heapBuf.Bytes(),
		Block:    blockBuf.Bytes(),
		Mutex:    mutexBuf.Bytes(),
	}
}
