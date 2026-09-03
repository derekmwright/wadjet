package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	nethttppprof "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/peterh/liner"

	"github.com/derekmwright/wadjet/internal/alerts"
	"github.com/derekmwright/wadjet/internal/embedding"
	"github.com/derekmwright/wadjet/internal/engine/memory"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/config"
	"github.com/derekmwright/wadjet/internal/coordinator"
	"github.com/derekmwright/wadjet/internal/dataplane"
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/format"
	"github.com/derekmwright/wadjet/internal/geoip"
	"github.com/derekmwright/wadjet/internal/logio"
	"github.com/derekmwright/wadjet/internal/metrics"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/server"
	"github.com/derekmwright/wadjet/internal/server/mcp"
	"github.com/derekmwright/wadjet/internal/server/pgwire"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/compaction"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/telemetry"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
	"github.com/spf13/cobra"
)

var (
	mode                  string
	storageType           string
	dataDir               string
	endpoint              string
	accessKey             string
	secretKey             string
	bucket                string
	httpAddr              string
	natsPort              int
	natsURL               string
	configFile            string
	clusterID             string
	leafRemotes           []string
	grpcAddr              string
	memoryBudget          int64
	sharedPoolBudget      int64
	spillFloatingBudget   bool
	mmapRelief            bool
	mmapReliefThresholdMB int64
	boundedDirtyWrites    bool
	spillDir              string
	resultStoreBytes      int64
	circuitThreshold      int
	circuitResetTimeout   time.Duration
	circuitRequestTimeout time.Duration
	queryIntermediateTTL  time.Duration
	queryIntermediateGC   time.Duration
	pgAddr                string
	pgTLSCert             string
	pgTLSKey              string
	queryTimeout          string
	maxConcurrentQry      int
	natsStoreDir          string
	geoipCityDB           string
	geoipASNDB            string
	useSSL                bool
	s3Region              string
	maxConcurrent         int
	morselWorkers         int
	cacheBytes            int64
	logLevel              string
	natsTLSCert           string
	natsTLSKey            string
	natsTLSCA             string
	otelEndpoint          string
	otelInsecure          bool
	metricsAddr           string
	enableAlerts          bool
	backgroundCompaction  bool
	reclaimDroppedTables  bool
	dataPlane             string
	drainTimeout          time.Duration
	dataPlaneAddr         string
	coordDataPlane        string
	localFastPathBytes    int64
	sortMergeJoinBytes    int64
	lateMaterialization   bool
	skewSplit             bool
	aggPartialSplit       bool
	bushyJoinReorder      bool
	broadcastBytes        int64
	streamingExchange     bool
	eagerDispatch         bool
	shuffleDurability     string
	localityPlacement     bool
	peerExchangeAddr      string
	peerExchangeAdvertise string
	baseTableCacheBytes   int64
	decodedCacheBytes     int64
	baseTableCacheDir     string
	streamingShuffleRead  bool
	asyncScratchPurge     bool
	peerWireCompression   bool
	scanDecodeAhead       bool
	scanDecodeAheadBytes  int64
	shuffleDecodeAhead    bool
)

func main() {
	if err := newRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

// newRootCmd builds the root command with every persistent flag registered
// and the configuration loader installed.
//
// It is a function (rather than inline in main) so the precedence census can
// drive the REAL command with the REAL flag registrations through the REAL
// PersistentPreRunE — a census that ran against a model of the loader could
// pass while the binary disagreed with it. Building it also RESETS every
// bound package variable to its registered default, which is what makes one
// census cell independent of the last.
func newRootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "wadjet",
		Short: "Wadjet — lightweight distributed analytical query engine",
		Long:  "A distributed analytical query engine that uses embedded NATS for coordination and object storage for results.",
		// Runtime failures (S3 unreachable, query errors) print the error
		// alone — dumping the full flag listing after "context deadline
		// exceeded" buries the message. Flag/usage mistakes still show
		// usage via the FlagErrorFunc below.
		SilenceUsage: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			// One loader, two passes: defaults -> config file -> env, then
			// the flags the operator actually typed. Every command runs it,
			// so no code path can read a value the resolution disagrees
			// with (ADR-0029, #808).
			if err := resolveConfiguration(cmd); err != nil {
				return err
			}
			// Process-wide planner knobs (package-level state read at plan
			// time; the logical optimizer has no per-query config surface).
			logical.BushyJoinReorder.Store(bushyJoinReorder)
			return nil
		},
	}

	rootCmd.PersistentFlags().StringVar(&configFile, "config", "", "YAML config file path")
	rootCmd.PersistentFlags().StringVar(&mode, "mode", "standalone", "Run mode: standalone, coordinator, or worker")
	rootCmd.PersistentFlags().StringVar(&storageType, "storage-type", "s3", "Storage backend: s3 or file")
	rootCmd.PersistentFlags().StringVar(&dataDir, "data-dir", "", "Local data directory (for --storage-type=file)")
	rootCmd.PersistentFlags().StringVar(&endpoint, "endpoint", "localhost:9000", "S3-compatible endpoint")
	rootCmd.PersistentFlags().StringVar(&accessKey, "access-key", "", "S3 access key (empty = auto-detect from env/IAM)")
	rootCmd.PersistentFlags().StringVar(&secretKey, "secret-key", "", "S3 secret key (empty = auto-detect from env/IAM)")
	rootCmd.PersistentFlags().BoolVar(&useSSL, "ssl", false, "Use TLS for S3 connections")
	rootCmd.PersistentFlags().StringVar(&s3Region, "region", "", "S3 region (for IAM credential signing)")
	rootCmd.PersistentFlags().StringVar(&bucket, "bucket", "wadjet", "Storage bucket name")
	rootCmd.PersistentFlags().StringVar(&httpAddr, "http-addr", ":8080", "HTTP API listen address")
	rootCmd.PersistentFlags().StringVar(&grpcAddr, "grpc-addr", ":9090", "gRPC API listen address")
	rootCmd.PersistentFlags().IntVar(&natsPort, "nats-port", 4222, "Embedded NATS port")
	rootCmd.PersistentFlags().StringVar(&natsURL, "nats-url", "", "NATS URL (for worker mode)")
	rootCmd.PersistentFlags().StringVar(&natsStoreDir, "nats-store-dir", "", "NATS JetStream storage directory (default: ~/.wadjet/nats)")
	rootCmd.PersistentFlags().StringVar(&clusterID, "cluster-id", "local", "Cluster identifier for federation")
	rootCmd.PersistentFlags().StringSliceVar(&leafRemotes, "leaf-remote", nil, "Remote NATS URLs for leaf node connections (repeatable)")
	rootCmd.PersistentFlags().StringVar(&natsTLSCert, "nats-tls-cert", "", "TLS certificate file for NATS mTLS")
	rootCmd.PersistentFlags().StringVar(&natsTLSKey, "nats-tls-key", "", "TLS private key file for NATS mTLS")
	rootCmd.PersistentFlags().StringVar(&natsTLSCA, "nats-tls-ca", "", "CA certificate file for NATS mTLS peer verification")
	rootCmd.PersistentFlags().StringVar(&otelEndpoint, "otel-endpoint", "", "OTLP gRPC endpoint for tracing (e.g., localhost:4317)")
	rootCmd.PersistentFlags().BoolVar(&otelInsecure, "otel-insecure", false, "Use plaintext gRPC for OTLP exporter")
	rootCmd.PersistentFlags().Int64Var(&memoryBudget, "memory-budget", 0, "Per-task memory budget in bytes (0 = auto-detect from cgroup, or unlimited)")
	rootCmd.PersistentFlags().Int64Var(&sharedPoolBudget, "shared-pool-budget", 0, "Worker-wide shared memory pool in bytes (0 = auto-detect: envelope minus cache). All concurrent tasks Reserve against this pool.")
	rootCmd.PersistentFlags().BoolVar(&spillFloatingBudget, "spill-floating-budget", false, "Activate the floating-budget spill threshold (deploy-gated; requires Phase-4 mmap RSS accounting). Default false = tuned static 40%/90% thresholds.")
	rootCmd.PersistentFlags().BoolVar(&mmapRelief, "mmap-relief", true, "MADV_DONTNEED relief of cold mmap'd cache files when total RSS exceeds the ceiling. Default true (validated free-to-faster at SF100 and under 512MB-4GB edge caps, 2026-06-11); --mmap-relief=false restores the dormant path (no tracking, no syscall).")
	rootCmd.PersistentFlags().Int64Var(&mmapReliefThresholdMB, "mmap-relief-threshold-mb", 0, "Total process RSS ceiling in MB; when --mmap-relief is set, relieve the coldest mmap'd cache files to bring RSS back to this level. 0 = auto: 85% of the detected memory limit (the old absolute default of 16000 could never fire inside an edge-sized envelope). Tune below the worker cgroup memory.max so relief has headroom.")
	rootCmd.PersistentFlags().BoolVar(&boundedDirtyWrites, "bounded-dirty-writes", true, "Bound the dirty page-cache footprint of spill/cache/stage file writes via windowed sync_file_range, and drop spill-file pages from cache as they are written. Default true (validated suite-neutral at SF100, -15% on Q05, faster-than-off under edge caps); --bounded-dirty-writes=false restores kernel-writeback-only.")
	rootCmd.PersistentFlags().StringVar(&spillDir, "spill-dir", "", "Directory for spill files (default: OS temp dir)")
	rootCmd.PersistentFlags().Int64Var(&cacheBytes, "cache-bytes", 0, "LRU file cache size in bytes (0 = auto-detect: 10% of memory)")

	rootCmd.PersistentFlags().Int64Var(&resultStoreBytes, "result-store", 512*1024*1024, "In-memory result store capacity in bytes (0 = disabled, results pass through S3)")
	rootCmd.PersistentFlags().IntVar(&circuitThreshold, "storage-circuit-threshold", 5, "Consecutive object-store failures IN ONE OPERATION CLASS (read / write / delete) before that class's circuit breaker opens and its requests fast-fail. Classes are independent: a delete or upload burst failing never fast-fails a read (ADR-0028). 0 = use the default (5).")
	rootCmd.PersistentFlags().DurationVar(&circuitResetTimeout, "storage-circuit-reset", 30*time.Second, "How long an open object-store circuit breaker stays open before admitting one half-open probe. 0 = use the default (30s).")
	rootCmd.PersistentFlags().DurationVar(&circuitRequestTimeout, "storage-circuit-request-timeout", 10*time.Second, "Per-request object-store timeout applied by the circuit breaker to non-streaming operations (Head/List/Delete/BucketExists/MakeBucket). Streaming Get/GetReaderAt and Put are bounded by the transport and the caller's context instead. 0 = use the default (10s).")
	rootCmd.PersistentFlags().DurationVar(&queryIntermediateTTL, "query-intermediate-ttl", time.Hour, "Age after which the coordinator's periodic sweep reclaims a queries/<id>/* prefix that the per-query cleanup did not (in-flight queries are always skipped). 0 = use the default (1h).")
	rootCmd.PersistentFlags().DurationVar(&queryIntermediateGC, "query-intermediate-sweep", 10*time.Minute, "How often the coordinator sweeps queries/ for prefixes older than --query-intermediate-ttl. 0 = use the default (10m).")
	rootCmd.PersistentFlags().StringVar(&pgAddr, "pg-addr", ":5433", "PostgreSQL wire protocol listen address")
	rootCmd.PersistentFlags().StringVar(&pgTLSCert, "pg-tls-cert", "", "TLS certificate file for PostgreSQL wire protocol")
	rootCmd.PersistentFlags().StringVar(&pgTLSKey, "pg-tls-key", "", "TLS private key file for PostgreSQL wire protocol")
	rootCmd.PersistentFlags().StringVar(&queryTimeout, "query-timeout", "0", "Default query timeout (e.g. 30s, 5m, 0=unlimited)")
	rootCmd.PersistentFlags().IntVar(&maxConcurrentQry, "max-concurrent-queries", 0, "Maximum concurrent queries (0=unlimited)")
	rootCmd.PersistentFlags().StringVar(&metricsAddr, "metrics-addr", ":9100", "Prometheus metrics listen address (worker mode)")
	rootCmd.PersistentFlags().IntVar(&maxConcurrent, "max-concurrent", 4, "Maximum concurrent tasks per worker")
	rootCmd.PersistentFlags().IntVar(&morselWorkers, "morsel-workers", 0, "Intra-fragment parallel pipeline consumers per task (morsel-driven execution, docs/design/morsel-execution.md). 0 = auto (default since the 2026-07-08 SF100 flip pair: width adapts to fragment input size and idle CPU tokens), 1 = serial (kill switch), N>1 = fixed width of N (bypasses the size gate; testing/benchmark knob).")
	rootCmd.PersistentFlags().DurationVar(&drainTimeout, "drain-timeout", 0, "Bound on graceful worker drain (SIGTERM): time allowed for in-flight tasks to finish and pending stage-output uploads to flush before escalating to a hard stop. 0 = unbounded (the platform kill timeout, e.g. the Kubernetes termination grace period, is the backstop).")
	rootCmd.PersistentFlags().StringVar(&dataPlane, "data-plane", "nats", "Worker↔coord data-plane transport: nats (default) or grpc. See project_split_plane_design_2026-05-20.")
	rootCmd.PersistentFlags().StringVar(&dataPlaneAddr, "data-plane-addr", ":9091", "Data-plane gRPC listen address (coord/standalone)")
	rootCmd.PersistentFlags().StringVar(&coordDataPlane, "coord-data-plane", "", "Coord's data-plane host:port (worker only; defaults to coord-host + 9091)")
	rootCmd.PersistentFlags().Int64Var(&broadcastBytes, "broadcast-bytes", 0, "Override the broadcast-join threshold: joins whose estimated build side is under this many bytes replicate the build to every worker. 0 = derive from worker pool budget (default), <0 = never broadcast (every join takes the hash-shuffle/sort-merge path; benchmarking/debugging surface).")
	rootCmd.PersistentFlags().Int64Var(&sortMergeJoinBytes, "sort-merge-join-bytes", 0, "Inner equi-joins whose sides BOTH exceed this estimated size run as sort-merge joins (both sides sort to spill-friendly runs and stream a merge) instead of hash joins, bounding join memory at merge-cursor state instead of a resident build table. Applies to both the local single-process paths and the distributed stage DAG (the join stage swaps operator; its exchange children are identical). 0 = disabled (default). See docs/design/sort-merge-join.md.")
	rootCmd.PersistentFlags().BoolVar(&lateMaterialization, "late-materialization", true, "Emit inner/left hash-join output as view (dictionary) columns over the probe input and build batches, deferring the column gather to the first consumer that needs owned storage — join chains compose the indirection so a column is copied once, at its final consumer or the shuffle encode. Default true (validated 2026-07-09: SF10 −6.2%, SF100 −4.9% suite wall, Q08 −36%/−44%, row-identical both scales); --late-materialization=false restores eager join-output gather. See docs/design/late-materialization.md.")
	rootCmd.PersistentFlags().BoolVar(&skewSplit, "skew-split", true, "Adaptive skew-aware shuffle layout: when a shuffled hash join's per-partition input bytes (reported by the shuffle stages) show a hot partition group (over the absolute floor AND >=2x the mean group), split it into k sub-tasks that divide the group's probe files and replicate its build files, bounding the straggler task's input and memory footprint. Default true (validated 2026-07-11: SF10 hot-key fixture -41% straggler wall, row-identical; plan-identical on uniform workloads via the ratio gate); --skew-split=false is the kill switch. See docs/design/skew-aware-shuffle.md.")
	rootCmd.PersistentFlags().BoolVar(&aggPartialSplit, "agg-partial-split", true, "Fan out partial (pre-merge) aggregate stages over a non-trivial multi-file upstream into at most workerCount tasks aggregating disjoint file slices, instead of one task reading the entire upstream. Gated on the upstream's worker-reported output size so trivial aggregates stay single-task (per-task scheduling overhead otherwise dominates). --agg-partial-split=false is the kill switch.")
	rootCmd.PersistentFlags().BoolVar(&bushyJoinReorder, "bushy-join-reorder", false, "Let the cost-based join reorder emit BUSHY plans (joins of two composite intermediates — e.g. pre-joining a snowflake dimension chain before it meets the fact stream) when strictly cheaper than every left-deep order. Cost ties keep the left-deep shape. Process-wide, default false. See docs/design/bushy-join-cbo.md.")
	rootCmd.PersistentFlags().Int64Var(&localFastPathBytes, "local-fastpath-bytes", coordinator.DefaultLocalFastPathBytes, "Queries whose post-pruning catalog scan bytes stay under this threshold execute in-process on the coordinator (skipping the distributed stage DAG and its per-stage object-store round trips). 0 = disabled.")
	rootCmd.PersistentFlags().BoolVar(&streamingExchange, "streaming-exchange", true, "Streaming exchange: consumers fetch stage outputs from the producing workers' local disk over gRPC with async S3 upload; every failure falls through to the durable S3 path. Default true (validated 2026-07-02: SF10 −10%, SF100 −23% suite wall, row-identical, zero fault-tolerance events); --streaming-exchange=false restores synchronous S3-only shuffle. See docs/design/streaming-exchange.md.")
	rootCmd.PersistentFlags().BoolVar(&eagerDispatch, "eager-dispatch", false, "Eager consumer dispatch (Phase C1): eligible non-join consumer stages (aggregate/sort over a standalone repartition) start before their producer stage fully drains, consuming per-producer-task file manifests as tasks finish. Requires --streaming-exchange. Default false until SF100 validation (kill switch thereafter). See docs/design/eager-consumer-dispatch.md.")
	rootCmd.PersistentFlags().BoolVar(&localityPlacement, "locality-placement", true, "Input-locality task placement (docs/design/locality-placement.md, ADR-0008): a task whose peer-location hints all point at one connected worker is dispatched to that worker, so 1:1 stage chains (consumer task i reading producer task i's output) read via same-worker mmap instead of peer gRPC streams. A same-batch cap preserves fan-out anti-clumping. Requires --streaming-exchange and --data-plane=grpc (inert otherwise). Default true (SF100-validated in two windows 2026-07-24: read split 37->50%/32->49% local, Q18 steady -15.3% clean-window, spread uniform); =false is the kill switch.")
	rootCmd.PersistentFlags().StringVar(&shuffleDurability, "shuffle-durability", "eager", "Stage-output durability policy under --streaming-exchange (docs/design/shuffle-durability.md). eager: background S3 uploads start as outputs finalize (default). lazy: uploads queue unstarted on the workers and run only on demand (consumer missing-input retry against a live producer, coordinator-side stage read, or worker drain); scratch never demanded is elided — no S3 PUT. off: scratch never uploads; a producer lost mid-query degrades to the one-shot streaming-disabled re-execution, including graceful drains. Scalar-subquery producer stages always upload eagerly (the coordinator reads those from S3).")
	rootCmd.PersistentFlags().StringVar(&peerExchangeAddr, "peer-exchange-addr", ":0", "Peer-exchange (FetchShuffle) listen address. Default :0 picks a free port (the address reaches peers via heartbeats, and a fixed default would collide when multiple workers share a host); pin it when firewalls need a known port.")
	rootCmd.PersistentFlags().StringVar(&peerExchangeAdvertise, "peer-exchange-advertise", "", "Peer-exchange address advertised in heartbeats (default: derived from the bound listener)")
	rootCmd.PersistentFlags().BoolVar(&streamingShuffleRead, "streaming-shuffle-read", true, "Decode WSHF/WSHC exchange inputs directly from the peer/S3 byte stream instead of staging the whole file to NVMe + mmap first — the first chunk decodes as soon as its frames arrive. Any mid-stream failure falls back to a staged read of the durable copy, skipping already-delivered batches. Default true (SF100-validated); =false is the kill switch restoring the staged read path. See docs/design/exchange-streaming-consumption.md.")
	rootCmd.PersistentFlags().BoolVar(&asyncScratchPurge, "async-scratch-purge", true, "Defer per-query stage-cache scratch deletion to a paced background janitor instead of unlinking inline on the query-complete broadcast handler — at SF100 the inline unlink storm of a big query's multi-GB scratch stalls the NEXT query's first tasks (Q22/Q14/Q11 straggler tails). Worker-side. Default true; =false is the kill switch restoring inline deletion. See docs/design/async-scratch-purge.md.")
	rootCmd.PersistentFlags().BoolVar(&peerWireCompression, "peer-wire-compression", true, "s2-compress raw WSHF payloads on outgoing peer-exchange streams: the wire carries a standard WSHC envelope every consumer already decodes, cutting peer-stream bytes ~20% for ~1 core-GB/s of producer CPU per stream. Worker-side. Default true (SF100-validated 2026-08-09: rows identical, walls in-band, ENA out-throttle events -30% on network-allowance-bound c7gd.4xlarge); =false is the kill switch. See docs/design/peer-wire-compression.md.")
	rootCmd.PersistentFlags().BoolVar(&scanDecodeAhead, "scan-decode-ahead", true, "Decode parquet row groups ahead of scan consumption: k decode workers per scan source with in-order delivery and a decoded-bytes window bounded by the shared memory pool and the page-cache refault sensor. Worker scan path only. Default true (SF100-validated, steady-state -7.3%); =false is the kill switch restoring the serial row-group path. See docs/design/scan-decode-pipelining.md.")
	rootCmd.PersistentFlags().Int64Var(&scanDecodeAheadBytes, "scan-decode-ahead-bytes", 0, "Decoded-but-unconsumed byte window per scan source for --scan-decode-ahead. 0 = engine default (256 MiB).")
	rootCmd.PersistentFlags().BoolVar(&shuffleDecodeAhead, "shuffle-decode-ahead", true, "Decode WSHF shuffle chunks ahead of consumption: the streaming reader's scanner stages chunk bytes while CPU-token-budgeted workers decode them, with strict in-order delivery — the probe-input width-plateau fix (q08/q09 broadcast probe-split). Default true; =false is the kill switch restoring the serial streaming reader. See docs/design/shuffle-decode-ahead.md.")
	rootCmd.PersistentFlags().Int64Var(&decodedCacheBytes, "decoded-cache-bytes", 0, "Worker-lifetime in-memory cache of decoded base-table parquet column chunks: hits skip zstd decompress + decode kernels for re-reads of the same immutable objects across queries and runs. Registered as a hard system reservoir and evicted first under memory relief. 0 = disabled (default until SF100 validation). See docs/design/decoded-rowgroup-cache.md.")
	rootCmd.PersistentFlags().Int64Var(&baseTableCacheBytes, "base-table-cache-bytes", 0, "Cross-query disk cache for immutable base-table parquet objects: LRU byte budget on the cache volume. Hits are served from local disk without touching S3 (or the circuit breaker); misses tee the download into the cache. The cache survives restarts (index rebuilt from the directory). 0 = disabled (default until SF100 validation). See docs/design/base-table-nvme-cache.md.")
	rootCmd.PersistentFlags().StringVar(&baseTableCacheDir, "base-table-cache-dir", "", "Directory for the base-table cache (default: <spill-dir>/base-cache, inheriting the spill volume's NVMe mount)")
	rootCmd.PersistentFlags().StringVar(&geoipCityDB, "geoip-city", "", "Path to MaxMind GeoIP City database (GeoLite2-City.mmdb)")
	rootCmd.PersistentFlags().StringVar(&geoipASNDB, "geoip-asn", "", "Path to MaxMind GeoIP ASN database (GeoLite2-ASN.mmdb)")
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", "Log level: debug, info, warn, error")
	rootCmd.PersistentFlags().BoolVar(&enableAlerts, "enable-alerts", false, "enable CREATE ALERT DDL and scheduler (default: disabled)")
	rootCmd.PersistentFlags().BoolVar(&backgroundCompaction, "background-compaction", true, "Run the periodic small-file compaction sweep (5m interval). --background-compaction=false disables it — useful for benchmark comparability (compaction mid-suite shifts timings and doubles data-dir disk during the delete grace) and for read-only/pre-compacted datasets.")
	rootCmd.PersistentFlags().BoolVar(&reclaimDroppedTables, "reclaim-dropped-tables", false, "Physically delete a DROPped table's data files once catalog.DefaultDropTableGrace (30m) has elapsed. Only files WADJET ITSELF wrote are ever eligible — ingest, compaction and GC-rewrite output. Objects you staged and registered with AddFiles (every bench and harness loader) are never themselves marked eligible — but that is NOT the same as reclaim leaving a registered-only table's data alone: background compaction runs by default (--background-compaction) and merges a table's small registered files into an engine-written compacted copy, deleting the originals outright and unconditionally regardless of this flag. Once compaction has run once, that table's live data IS the engine-written copy, and DROP plus this flag reclaims it — a compacted bench or harness table's data leaves the bucket. Do not enable this on a catalog over a shared or do-not-wipe bucket unless background compaction is also disabled there (--background-compaction=false). On top of that, a path still referenced by any current table's manifest is never deleted (drop-then-re-register of the same object paths, or an Iceberg RefreshTable's drop+recreate), re-checked immediately before each delete. Default false: this process's *Catalog is not necessarily the only one a DROP can go through (standalone's pgwire server opens its own embedded wadjet.DB with a separate *Catalog from this compaction sweep's), so an operator who enables this on one process while another can also DROP against a different Catalog should understand the split. NOTE: --query-timeout defaults to 0 (unlimited); keep it at or below the drop grace, or a query running longer than the grace can have its files reclaimed underneath it. See docs/adr/0020-drop-table-reclaim-is-opt-in.md. #494.")

	rootCmd.AddCommand(serveCmd())
	rootCmd.AddCommand(queryCmd())
	rootCmd.AddCommand(tablesCmd())
	rootCmd.AddCommand(createTableCmd())
	rootCmd.AddCommand(dropTableCmd())
	rootCmd.AddCommand(compactCmd())
	rootCmd.AddCommand(shellCmd())
	rootCmd.AddCommand(clustersCmd())
	rootCmd.AddCommand(mcpCmd())
	rootCmd.AddCommand(catalogCmd())

	// Flag mistakes keep the usage dump (it answers "what should I have
	// typed"); SilenceUsage above scopes it away from runtime errors.
	rootCmd.SetFlagErrorFunc(func(c *cobra.Command, err error) error {
		c.Println(c.UsageString())
		return err
	})

	// Snapshot the flag DEFAULTS while they are still the bound variables'
	// values. They are the resolver's default tier for every key that has a
	// flag: the binary runs on the flag default today, and
	// config.DefaultConfig() is not always the same value (it sets
	// storage.access_key to "minioadmin" where --access-key defaults to ""
	// and means "auto-detect from env/IAM").
	snapshotConfigFlagDefaults()

	return rootCmd
}

func serveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the Wadjet server",
	}

	catalogSnapshotPrefix := cmd.Flags().String("catalog-snapshot-s3-prefix", "", "S3 URL prefix (s3://bucket/path/) for catalog snapshots. Unset disables.")
	catalogSnapshotInterval := cmd.Flags().Duration("catalog-snapshot-interval", 5*time.Minute, "Periodic catalog snapshot cadence. 0 disables periodic (explicit CREATE SNAPSHOT still works).")
	forceRestoreCatalog := cmd.Flags().String("force-restore-catalog", "", "Restore catalog from S3 regardless of KV state. Value: 'latest' or a specific timestamp.")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer cancel()

		// Async log sink: in deploys stderr is a pipe into journald, and a
		// stalled journald otherwise freezes the whole process via the
		// slog handler mutex (frozen-spin/quiet-stall family; see
		// internal/logio doc comment). WADJET_SYNC_LOG=1 restores direct
		// writes for debugging.
		var logSink io.Writer = os.Stderr
		if os.Getenv("WADJET_SYNC_LOG") != "1" {
			asyncSink := logio.NewAsyncWriter(os.Stderr, 8192)
			defer asyncSink.Close()
			logSink = asyncSink
		}
		logger := slog.New(slog.NewTextHandler(logSink, &slog.HandlerOptions{Level: parseLogLevel(logLevel)}))

		// Env-gated contention profiling for benchmark/profiling deploys.
		// Rates are the raw runtime knobs (block rate in ns, mutex 1-in-N);
		// unset or 0 keeps both samplers off — zero cost in production.
		if rate, err := strconv.Atoi(os.Getenv("WADJET_BLOCK_PROFILE_RATE")); err == nil && rate > 0 {
			runtime.SetBlockProfileRate(rate)
			logger.Info("block profiling enabled", "rate_ns", rate)
		}
		if frac, err := strconv.Atoi(os.Getenv("WADJET_MUTEX_PROFILE_FRACTION")); err == nil && frac > 0 {
			runtime.SetMutexProfileFraction(frac)
			logger.Info("mutex profiling enabled", "fraction", frac)
		}

		if memLimit := memory.DetectMemoryLimit(); memLimit > 0 {
			maxConc := int64(maxConcurrent)
			if maxConc < 1 {
				maxConc = 1
			}

			// Memory envelope: 75% of detected limit. Leaves 25% headroom
			// for OS page cache, kernel buffers, and non-Go allocations.
			// (Previous 90% left only 3 GB on 32 GB machines — not enough.)
			//
			// Honour an explicit GOMEMLIMIT env var when set — the harness
			// passes a tight per-process limit (4 GB / 8 GB) to reproduce
			// constrained-memory paths locally, and overriding it with the
			// 75%-of-physical default would silently mask the very
			// workloads we're testing.
			goMemLimit := memLimit * 3 / 4
			if envLim := os.Getenv("GOMEMLIMIT"); envLim != "" {
				if parsed, ok := parseGoMemLimit(envLim); ok {
					goMemLimit = parsed
				} else {
					logger.Warn("ignoring unparseable GOMEMLIMIT env",
						"value", envLim, "fallback_bytes", goMemLimit)
				}
			}
			debug.SetMemoryLimit(goMemLimit)
			// GC mode is overridable via WADJET_GOGC env var:
			//   unset (default): envelope-aware — "off" (GOMEMLIMIT-only) on
			//     big machines, where GC assist tax with GOGC=100 caused
			//     2-3x query slowdowns once the LRU cache was populated; but
			//     GOGC=100 below edgeGCEnvelope. With GC off, up to a full
			//     GOMEMLIMIT of garbage legally accumulates between cycles,
			//     and near small envelopes that slack IS the box: the 512 MiB
			//     edge validation died at Q21 from 20 queries of accumulated
			//     slack with GOGC=off and passed 25/25 with GOGC=100.
			//   "off": force GOMEMLIMIT-only at any size.
			//   "<int>" (e.g. "100"): set debug.SetGCPercent to that value —
			//     useful for catalog-priming-heavy workloads where transient
			//     garbage accumulates pre-query (Q18 SF10 baseline 11.5 GB
			//     before query starts on a freshly primed coord).
			const edgeGCEnvelope = 2 << 30 // 2 GiB GOMEMLIMIT
			gcMode := os.Getenv("WADJET_GOGC")
			if gcMode == "" && goMemLimit < edgeGCEnvelope {
				debug.SetGCPercent(100)
				gcMode = "100 (auto: edge envelope)"
			} else if gcMode == "" || strings.EqualFold(gcMode, "off") {
				debug.SetGCPercent(-1)
				gcMode = "off"
			} else if pct, perr := strconv.Atoi(gcMode); perr == nil && pct > 0 {
				debug.SetGCPercent(pct)
				gcMode = strconv.Itoa(pct)
			} else {
				debug.SetGCPercent(-1)
				gcMode = "off (invalid WADJET_GOGC)"
			}
			logger.Info("set GOMEMLIMIT", "detected_limit", memLimit, "go_mem_limit", goMemLimit, "gogc", gcMode)

			// mmap-relief RSS ceiling: auto-derive from the detected limit
			// when the flag is left at 0. The old absolute default (16000 MB)
			// was sized for the SF100 c7gd worker envelope and could never
			// fire inside an edge-sized cgroup — RSS hits the cap and the
			// kernel OOM-kills long before a 16 GB ceiling is approached.
			// 85% mirrors the validated SF100 ratio (16000/~19000).
			if mmapRelief && mmapReliefThresholdMB == 0 {
				mmapReliefThresholdMB = memLimit * 85 / 100 >> 20
				logger.Info("auto-derived mmap relief threshold",
					"threshold_mb", mmapReliefThresholdMB, "detected_limit", memLimit)
			}

			if cacheBytes == 0 {
				if memoryBudget > 0 {
					// With explicit memory budget, scale cache to fit alongside
					// task memory. Each task uses ~3x its tracked budget in total
					// RSS (tracked + hash table arenas + intermediate batches).
					taskFootprint := memoryBudget * maxConc * 3
					headroom := goMemLimit / 10
					available := goMemLimit - taskFootprint - headroom
					if available < 256*1024*1024 {
						available = 256 * 1024 * 1024
					}
					if maxCache := goMemLimit / 5; available > maxCache {
						available = maxCache
					}
					cacheBytes = available
				} else {
					// Default: 10% of envelope for cross-query S3 file cache.
					cacheBytes = goMemLimit / 10
				}
				logger.Info("auto-detected file cache size", "cache_bytes", cacheBytes)
			}
			if memoryBudget == 0 {
				// Per-task spill budget. Reduced from 5x to 4x: hash table
				// reconciliation now uses ForceReserve for accurate tracking,
				// and spill threshold lowered to 60%, enabling tighter budgets
				// without OOM risk on multi-join queries.
				// Formula: (envelope - cache) / (4 * maxConcurrent)
				//
				// Note: with the shared worker memory pool (Trino MemoryPool /
				// Spark ExecutionMemoryPool model), the per-task budget is only
				// consulted by the planner for operator sizing — actual
				// allocation tracking flows through `sharedPoolBudget` below.
				// We keep this calculation for planner sizing and as a fallback
				// for legacy callers, but spill triggers are pool-driven.
				memoryBudget = (goMemLimit - cacheBytes) / (4 * maxConc)

				// Auto-tune maxConc DOWN when the per-task budget would be
				// too small to fit a SF100-class join. The 4x factor models
				// task overhead but cannot rescue a worker whose hash tables
				// alone need 8 GB at SF100 from a 1.4 GB budget — at SF100
				// the original maxConc=4 / 30GB-machine combo gives each
				// task ~1.4 GB and the worker OOMs at 31 GB anon-rss when
				// multiple tasks pick up the same query in parallel.
				//
				// Minimum target: 2 GB per task. If we'd be under, reduce
				// maxConc until we hit it (or until maxConc=1). Each query
				// in distributed/probe-split mode dispatches one task per
				// worker, so maxConc above ~2 only helps with concurrent
				// queries from different sessions — which is rare in
				// benchmarks and tunable upward via --max-concurrent when
				// the workload actually needs it.
				const minBudgetPerTask int64 = 2 * 1024 * 1024 * 1024
				if memoryBudget < minBudgetPerTask && maxConc > 1 {
					origConc := maxConc
					for memoryBudget < minBudgetPerTask && maxConc > 1 {
						maxConc--
						memoryBudget = (goMemLimit - cacheBytes) / (4 * maxConc)
					}
					maxConcurrent = int(maxConc)
					logger.Info("auto-tuned max_concurrent down to fit memory budget",
						"orig_max_concurrent", origConc,
						"new_max_concurrent", maxConc,
						"budget_bytes", memoryBudget,
						"min_budget_target", minBudgetPerTask)
				}

				logger.Info("auto-detected memory budget", "budget_bytes", memoryBudget, "max_concurrent", maxConc)
			}
			// Result store: the flag default (512 MiB) predates edge-class
			// envelopes and is ABSOLUTE — on a 512 MiB box it alone exceeds
			// the whole GOMEMLIMIT and was the proximate OOM-kill in the
			// 2026-06-11 edge validation (the boot invariant flagged
			// Σcaps > GOMEMLIMIT, but it is advisory). When the operator
			// didn't set the result store ANYWHERE, clamp it to 15% of the
			// envelope; large results fall through to S3 as always. No
			// change on big machines (15% of a 24 GiB limit ≫ 512 MiB).
			//
			// The guard is the resolved SOURCE, not the flag alone: a
			// `worker.result_store_bytes` in the config file is as explicit
			// as typing --result-store, and clamping it would be a new way
			// to ignore the file (#808).
			if effectiveResolution().Source("worker.result_store_bytes") == config.SourceDefault {
				if maxStore := goMemLimit * 15 / 100; resultStoreBytes > maxStore {
					resultStoreBytes = maxStore
					logger.Info("auto-scaled result store to envelope",
						"result_store_bytes", resultStoreBytes, "go_mem_limit", goMemLimit)
				}
			}
			if sharedPoolBudget == 0 {
				// Pool is the full envelope minus the file cache. All
				// concurrent tasks Reserve from it; operators
				// cooperatively spill when the pool fills. NOT divided
				// by maxConc — the pool's whole point is to share a
				// single budget across tasks instead of statically
				// carving N slices.
				sharedPoolBudget = goMemLimit - cacheBytes
				if sharedPoolBudget < 256*1024*1024 {
					sharedPoolBudget = 256 * 1024 * 1024
				}
				logger.Info("auto-detected shared pool budget",
					"pool_bytes", sharedPoolBudget,
					"go_mem_limit", goMemLimit, "cache_bytes", cacheBytes)
			}
			// Phase 3 note: the system-reservoir registry is built per
			// run-function (buildReservoirs) and threaded into worker.Config
			// so it reaches a real worker's SpillManager. The Phase-1 prelude
			// that built+logged a throwaway registry here was removed.
		}

		store, err := newStore()
		if err != nil {
			return err
		}

		// Wrap store with circuit breaker for S3 resilience. The breaker is
		// scoped per operation class — a failing delete or upload burst must
		// never fast-fail a base-table read (ADR-0028). The thresholds are
		// resolved config keys: a `storage.circuit:` block in the YAML, a
		// WADJET_STORAGE_CIRCUIT_* variable and the flags all reach here, in
		// ADR-0029's order.
		circuit := effectiveConfig().Storage.Circuit
		circuitCfg := objstore.CircuitConfig{
			FailureThreshold: circuit.FailureThreshold,
			ResetTimeout:     circuit.ResetTimeout,
			RequestTimeout:   circuit.RequestTimeout,
		}
		store = objstore.NewCircuitStore(store, circuitCfg, logger)

		// Base-table NVMe cache sits ABOVE the breaker: hits never consult
		// it (a warm cache keeps serving through an S3 brownout), misses
		// keep full breaker protection. One process-wide seam serves every
		// scan path (docs/design/base-table-nvme-cache.md §3).
		if baseTableCacheBytes > 0 {
			cacheDir := baseTableCacheDir
			if cacheDir == "" && spillDir != "" {
				cacheDir = filepath.Join(spillDir, "base-cache")
			}
			if cacheDir == "" {
				logger.Warn("base-table cache disabled: set --base-table-cache-dir or --spill-dir to place it")
			} else {
				btc, err := objstore.NewBaseTableCache(store, cacheDir, baseTableCacheBytes, logger)
				if err != nil {
					return fmt.Errorf("initializing base-table cache: %w", err)
				}
				store = btc
				logger.Info("base-table cache enabled", "dir", cacheDir, "budget_bytes", baseTableCacheBytes)
				go func() {
					var last objstore.BaseTableCacheStats
					for range time.Tick(60 * time.Second) {
						if s := btc.Stats(); s != last {
							last = s
							btc.LogStats()
						}
					}
				}()
			}
		}

		// WADJET_ENABLE_ALERTS used to be read here, and it beat the flag
		// unconditionally — the opposite of every other tier. It is an
		// ordinary resolved key now (alerts.enabled), so `alerts:` in the
		// config file works and an explicit --enable-alerts=false wins
		// (ADR-0029).
		if v := os.Getenv("WADJET_CATALOG_SNAPSHOT_PREFIX"); v != "" {
			*catalogSnapshotPrefix = v
		}
		if v := os.Getenv("WADJET_CATALOG_SNAPSHOT_INTERVAL"); v != "" {
			if d, err := time.ParseDuration(v); err == nil {
				*catalogSnapshotInterval = d
			}
		}

		switch serveMode() {
		case "standalone":
			return runStandalone(ctx, store, logger, enableAlerts, *catalogSnapshotPrefix, *catalogSnapshotInterval, *forceRestoreCatalog)
		case "coordinator":
			return runCoordinator(ctx, store, logger, enableAlerts, *catalogSnapshotPrefix, *catalogSnapshotInterval, *forceRestoreCatalog)
		case "worker":
			return runWorker(ctx, store, logger)
		default:
			return fmt.Errorf("unknown mode: %s", serveMode())
		}
	}
	return cmd
}

func queryCmd() *cobra.Command {
	var outputFormat string

	cmd := &cobra.Command{
		Use:   "query [sql]",
		Short: "Execute a SQL query (standalone mode)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

			f, err := format.ParseFormat(outputFormat)
			if err != nil {
				return err
			}

			// Load GeoIP databases if configured
			if err := loadGeoIP(logger); err != nil {
				return fmt.Errorf("loading GeoIP: %w", err)
			}
			defer geoip.Close()

			// A statement whose every source is a catalog-free table
			// function (read_json / read_csv / read_parquet over a local
			// path or URL) needs no object store at all. Building one
			// against the default endpoint is what made the first command
			// a new user runs fail inside catalog init, before the SQL was
			// ever parsed (#303). Anything else keeps the existing path.
			var store objstore.Store
			if isCatalogFreeQuery(args[0]) {
				store = objstore.NewMemStore()
			} else {
				store, err = newStore()
				if err != nil {
					store = objstore.NewMemStore()
				}
			}

			db, err := wadjet.Open(ctx, wadjet.Config{
				Store:  store,
				Bucket: bucket,
			})
			if err != nil {
				return err
			}

			result, err := db.Query(ctx, args[0])
			if err != nil {
				return err
			}

			return format.WriteTyped(os.Stdout, f, result.Columns, columnTypes(result), result.Rows)
		},
	}

	cmd.Flags().StringVarP(&outputFormat, "format", "f", "json", "Output format: table, json, csv")
	return cmd
}

func tablesCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "tables",
		Short: "List all tables",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			store, err := newStore()
			if err != nil {
				return err
			}

			cat := catalog.NewWithStore(store, bucket)
			tables, err := cat.ListTables(ctx)
			if err != nil {
				return err
			}

			for _, t := range tables {
				fmt.Println(t)
			}
			return nil
		},
	}
}

func createTableCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "create-table [sql]",
		Short: "Create a table via SQL (e.g., CREATE TABLE events (id BIGINT, name VARCHAR) PARTITION BY (date))",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			store, err := newStore()
			if err != nil {
				store = objstore.NewMemStore()
			}

			db, err := wadjet.Open(ctx, wadjet.Config{
				Store:  store,
				Bucket: bucket,
			})
			if err != nil {
				return err
			}

			result, err := db.Query(ctx, args[0])
			if err != nil {
				return err
			}

			if len(result.Rows) > 0 {
				if r, ok := result.Rows[0]["result"]; ok {
					fmt.Println(r)
				}
			}
			return nil
		},
	}
}

func dropTableCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "drop-table [name]",
		Short: "Drop a table by name",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			store, err := newStore()
			if err != nil {
				store = objstore.NewMemStore()
			}

			db, err := wadjet.Open(ctx, wadjet.Config{
				Store:  store,
				Bucket: bucket,
			})
			if err != nil {
				return err
			}

			if err := db.DropTable(ctx, args[0]); err != nil {
				return err
			}
			fmt.Printf("Table %q dropped\n", args[0])
			return nil
		},
	}
}

// compactCmd is the maintenance surface for the storage layer's two
// read-and-rewrite modes.
//
// The default is an ordinary compaction pass: the background sweep's own
// thresholds, run once, on demand.
//
// --rewrite is the FORMAT MIGRATION mode, and it is the reason this command
// exists. Compaction's thresholds ask "is this partition worth merging" —
// two files or more, at least --min-files of them, average size under
// --max-file-size — and a table that is already healthy answers no to all
// three. So no compaction pass will ever touch a partition holding one large
// file, which is exactly the file that has to be rewritten when the FORMAT
// changed underneath it. --rewrite rewrites every file of every partition
// once, floors and all.
//
// The migration it was built for is ADR-0018's DECIMAL(p > 18): files written
// before #429 annotate a wide DECIMAL over an INT64 leaf, which no reader
// outside wadjet will open. One rewrite produces a FLBA(16) leaf with
// byte-identical unscaled values. Upgrade every reader in the cluster BEFORE
// running it — an old reader silently truncates a wide DECIMAL from a new
// file to its low 64 bits (#437).
func compactCmd() *cobra.Command {
	var rewrite bool
	var minFiles int
	var maxFileSize int64

	cmd := &cobra.Command{
		Use:   "compact [table]",
		Short: "Compact a table's small files, or rewrite every file through the current writer",
		Long: "Compact a table. With --rewrite, every file of every partition is rewritten " +
			"once through the current writer regardless of the compaction thresholds — the " +
			"format-migration mode (see docs/adr/0018-parquet-file-numbers-are-input.md).",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			table := args[0]

			natsAddr := natsURL
			if natsAddr == "" {
				natsAddr = fmt.Sprintf("nats://127.0.0.1:%d", natsPort)
			}
			nc, err := distributed.Connect(natsAddr, nil)
			if err != nil {
				return fmt.Errorf("connecting to NATS at %s: %w", natsAddr, err)
			}
			defer nc.Close()

			js, err := distributed.NewJetStream(nc)
			if err != nil {
				return fmt.Errorf("creating JetStream: %w", err)
			}
			kv, err := catalog.NewNATSKV(js)
			if err != nil {
				return fmt.Errorf("creating catalog KV: %w", err)
			}

			store, err := newStore()
			if err != nil {
				return fmt.Errorf("opening object store: %w", err)
			}

			cat := catalog.NewWithCluster(kv, store, bucket, clusterID)
			if _, err := cat.GetTable(ctx, table); err != nil {
				return err
			}

			logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
			cfg := compaction.DefaultConfig()
			if minFiles > 0 {
				cfg.MinFiles = minFiles
			}
			if maxFileSize > 0 {
				cfg.MaxFileSizeBytes = maxFileSize
			}
			c := compaction.New(cat, logger, cfg)

			var result *compaction.Result
			if rewrite {
				result, err = c.RewriteTable(ctx, table)
			} else {
				result, err = c.CompactTable(ctx, table)
			}
			if result != nil {
				fmt.Printf("table %s: %d merges, %d files removed, %d created, %d rows, %d -> %d bytes\n",
					result.Table, result.PartitionsCompacted, result.FilesRemoved,
					result.FilesCreated, result.RowsMerged, result.BytesBefore, result.BytesAfter)
				if result.PassLimitReached {
					fmt.Println("note: the pass limit was reached with work outstanding — run again")
				}
				for _, f := range result.Failed {
					fmt.Fprintf(os.Stderr, "partition %s FAILED: %v\n", f.Partition, f.Err)
				}
			}
			return err
		},
	}

	cmd.Flags().BoolVar(&rewrite, "rewrite", false,
		"rewrite EVERY file of every partition once, ignoring the compaction thresholds (format migration)")
	cmd.Flags().IntVar(&minFiles, "min-files", 0,
		"override the minimum file count that triggers compaction (ignored with --rewrite)")
	cmd.Flags().Int64Var(&maxFileSize, "max-file-size", 0,
		"override the average file size below which compaction triggers, in bytes (ignored with --rewrite)")
	return cmd
}

func clustersCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "clusters",
		Short: "List all federated clusters and their tables",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			natsAddr := natsURL
			if natsAddr == "" {
				natsAddr = fmt.Sprintf("nats://127.0.0.1:%d", natsPort)
			}

			nc, err := distributed.Connect(natsAddr, nil)
			if err != nil {
				return fmt.Errorf("connecting to NATS: %w", err)
			}
			defer nc.Close()

			js, err := distributed.NewJetStream(nc)
			if err != nil {
				return fmt.Errorf("creating JetStream: %w", err)
			}

			kv, err := catalog.NewNATSKV(js)
			if err != nil {
				return fmt.Errorf("creating catalog KV: %w", err)
			}

			store, storeErr := newStore()
			if storeErr != nil {
				store = objstore.NewMemStore()
			}

			cat := catalog.NewWithCluster(kv, store, bucket, clusterID)
			_ = cat.Init(ctx)

			clusters, err := cat.ListClusters()
			if err != nil {
				return err
			}

			if len(clusters) == 0 {
				fmt.Println("No clusters found.")
				return nil
			}

			for _, c := range clusters {
				marker := ""
				if c.ClusterID == clusterID {
					marker = " (local)"
				}
				fmt.Printf("Cluster: %s%s\n", c.ClusterID, marker)
				if len(c.Tables) == 0 {
					fmt.Println("  (no tables)")
				}
				for _, t := range c.Tables {
					fmt.Printf("  - %s\n", t)
				}
			}
			return nil
		},
	}
}

func shellCmd() *cobra.Command {
	var outputFormat string

	cmd := &cobra.Command{
		Use:   "shell",
		Short: "Interactive SQL shell (standalone mode)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

			f, err := format.ParseFormat(outputFormat)
			if err != nil {
				return err
			}

			// Load GeoIP databases if configured
			if err := loadGeoIP(logger); err != nil {
				return fmt.Errorf("loading GeoIP: %w", err)
			}
			defer geoip.Close()

			store, err := newStore()
			if err != nil {
				store = objstore.NewMemStore()
			}

			db, err := wadjet.Open(ctx, wadjet.Config{
				Store:  store,
				Bucket: bucket,
			})
			if err != nil {
				return err
			}

			return runShell(ctx, db, f)
		},
	}

	cmd.Flags().StringVarP(&outputFormat, "format", "f", "table", "Output format: table, json, csv")
	return cmd
}

func historyPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	dir := filepath.Join(home, ".wadjet")
	os.MkdirAll(dir, 0o700)
	return filepath.Join(dir, "history")
}

// columnTypes returns the declared type of each result column, positionally
// aligned with result.Columns, or nil when the query carried no typed
// metadata (introspection answers). The formatter needs it to render
// TIMESTAMP columns, which the engine boxes as epoch milliseconds.
func columnTypes(result *wadjet.QueryResult) []parquet.TypeID {
	if result == nil || len(result.ColumnMetas) == 0 {
		return nil
	}
	types := make([]parquet.TypeID, len(result.ColumnMetas))
	for i, m := range result.ColumnMetas {
		types[i] = m.TypeID
	}
	return types
}

func runShell(ctx context.Context, db *wadjet.DB, f format.Format) error {
	line := liner.NewLiner()
	defer line.Close()

	line.SetCtrlCAborts(true)

	// Load history
	if path := historyPath(); path != "" {
		if fh, err := os.Open(path); err == nil {
			line.ReadHistory(fh)
			fh.Close()
		}
	}

	// Save history on exit
	defer func() {
		if path := historyPath(); path != "" {
			if fh, err := os.Create(path); err == nil {
				line.WriteHistory(fh)
				fh.Close()
			}
		}
	}()

	fmt.Println("Wadjet SQL Shell. Type 'exit' to quit.")
	fmt.Println("  Supports: SELECT, EXPLAIN, DESCRIBE, SHOW COLUMNS FROM")
	fmt.Println()

	var buf strings.Builder
	prompt := "wadjet> "

	for {
		input, err := line.Prompt(prompt)
		if err == liner.ErrPromptAborted {
			// Ctrl-C: clear current buffer
			buf.Reset()
			prompt = "wadjet> "
			continue
		}
		if err == io.EOF {
			fmt.Println()
			break
		}
		if err != nil {
			return err
		}

		trimmed := strings.TrimSpace(input)

		// Exit commands (only when not in multi-line mode)
		if buf.Len() == 0 && (trimmed == "exit" || trimmed == "quit" || trimmed == `\q`) {
			break
		}

		if trimmed == "" {
			if buf.Len() > 0 {
				buf.WriteString("\n")
			}
			continue
		}

		// Accumulate multi-line input
		if buf.Len() > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString(trimmed)

		// Check if statement is complete (ends with ;)
		current := buf.String()
		if !strings.HasSuffix(strings.TrimSpace(current), ";") {
			prompt = "     -> "
			continue
		}

		// Strip trailing semicolon and execute
		sql := strings.TrimRight(strings.TrimSpace(current), ";")
		buf.Reset()
		prompt = "wadjet> "

		if sql == "" {
			continue
		}

		line.AppendHistory(current)

		result, err := db.Query(ctx, sql)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			continue
		}

		if len(result.Rows) == 0 {
			fmt.Println("(0 rows)")
			continue
		}

		format.WriteTyped(os.Stdout, f, result.Columns, columnTypes(result), result.Rows)
	}

	return nil
}

// newStore builds the object store from the RESOLVED storage configuration.
// Before #808's fix it read the flag variables, so the whole `storage:`
// section of the config file was parsed, validated and reported by the admin
// endpoint without ever reaching a connection.
func newStore() (objstore.Store, error) {
	st := effectiveConfig().Storage
	switch st.Type {
	case "file":
		dir := st.DataDir
		if dir == "" {
			dir = "/var/lib/wadjet/data"
		}
		return objstore.NewFileStore(dir)
	default:
		return objstore.NewMinIOStore(objstore.MinIOConfig{
			Endpoint:  st.Endpoint,
			AccessKey: st.AccessKey,
			SecretKey: st.SecretKey,
			UseSSL:    st.UseSSL,
			Region:    st.Region,
		})
	}
}

// natsServerConfig builds the embedded NATS server configuration from the
// RESOLVED `nats:` section. runStandalone and runCoordinator share it, which
// is also the seam the census asserts the file and environment tiers reach.
func natsServerConfig() distributed.NATSConfig {
	n := effectiveConfig().NATS
	cfg := distributed.DefaultNATSConfig()
	cfg.Port = n.Port
	cfg.ClusterID = n.ClusterID
	cfg.LeafRemotes = n.LeafRemotes
	if n.StoreDir != "" {
		cfg.StoreDir = n.StoreDir
	}
	return cfg
}

func runStandalone(ctx context.Context, store objstore.Store, logger *slog.Logger, alertsEnabled bool, snapshotPrefix string, snapshotInterval time.Duration, forceRestoreTS string) error {
	// Opt-in heap profile dumper for OOM debugging. No-op unless
	// WADJET_HEAP_DUMP_INTERVAL is set. See cmd/wadjet/heap_dumper.go.
	startHeapDumper(ctx, logger)

	// Periodic background GC: with the default gogc=off, large transient
	// garbage (catalog priming, NATS message buffers) accumulates and
	// can push baseline heap to 11+ GB before any query runs (Q18 SF10
	// 2026-04-25, project_q18_sf10_native_dag_oom_2026-04-24). One GC
	// every 30s reclaims this cheaply (~50ms per call) without the
	// per-allocation GC assist tax that GOGC=100 imposes. Override via
	// WADJET_BG_GC_INTERVAL ("off" to disable, "<duration>" otherwise).
	startBackgroundGC(ctx, logger)

	// Start embedded NATS (with optional leaf node connections)
	natsCfg := natsServerConfig()
	embeddedNATS, err := distributed.NewEmbeddedNATS(natsCfg, logger)
	if err != nil {
		return fmt.Errorf("starting NATS: %w", err)
	}
	defer embeddedNATS.Shutdown()

	// Connect to NATS via in-process (zero-copy, no TCP overhead)
	nc, err := distributed.ConnectInProcess(embeddedNATS.Server())
	if err != nil {
		return fmt.Errorf("connecting to NATS: %w", err)
	}
	defer nc.Close()

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		return fmt.Errorf("creating JetStream: %w", err)
	}

	if err := distributed.SetupStreams(ctx, js); err != nil {
		return fmt.Errorf("setting up streams: %w", err)
	}

	// Create catalog with NATS KV metadata store and cluster identity
	kv, err := catalog.NewNATSKV(js)
	if err != nil {
		return fmt.Errorf("creating catalog KV: %w", err)
	}
	cat := catalog.NewWithCluster(kv, store, bucket, clusterID)
	if err := cat.Init(ctx); err != nil {
		return fmt.Errorf("initializing catalog: %w", err)
	}

	// Restore persisted UDFs and wire persistence callback
	wireUDFPersistence(cat, logger)

	// Load GeoIP databases if configured. The two dropped-error
	// `config.Load(configFile)` reads that used to sit here are gone: the
	// file is loaded once, in the root command's PersistentPreRunE, and a
	// parse failure stops the process there instead of silently yielding a
	// nil config here (#802's doctrine, #808's loader).
	if err := loadGeoIP(logger); err != nil {
		return fmt.Errorf("loading GeoIP: %w", err)
	}
	defer geoip.Close()

	// Construct worker (deferred Start; the gRPC data-plane wiring below
	// must complete first so Start can see the dpClient and skip the
	// JetStream Fetch loop in --data-plane=grpc mode).
	w := worker.New(worker.Config{
		NATSUrl:               embeddedNATS.ClientURL(),
		ClusterID:             clusterID,
		MaxConcurrent:         maxConcurrent,
		MorselWorkers:         morselWorkersConfig(morselWorkers),
		DrainTimeout:          drainTimeout,
		CacheBytes:            cacheBytes,
		MemoryBudget:          memoryBudget,
		SharedPoolBudget:      sharedPoolBudget,
		SpillDir:              spillDir,
		ResultStoreBytes:      resultStoreBytes,
		Reservoirs:            memory.NewReservoirRegistry(),
		FloatingBudgetActive:  spillFloatingBudget,
		MmapRelief:            mmapRelief,
		MmapReliefThresholdMB: mmapReliefThresholdMB,
		BoundedDirtyWrites:    boundedDirtyWrites,
		PeerListenAddr:        peerListenAddr(),
		PeerAdvertiseAddr:     peerExchangeAdvertise,
		StreamingShuffleRead:  streamingShuffleRead,
		AsyncScratchPurge:     asyncScratchPurge,
		PeerWireCompression:   peerWireCompression,
		ScanDecodeAhead:       scanDecodeAhead,
		ScanDecodeAheadBytes:  scanDecodeAheadBytes,
		ShuffleDecodeAhead:    shuffleDecodeAhead,
		DecodedCacheBytes:     decodedCacheBytes,
	}, store, nc, js, logger)

	// Initialize Prometheus metrics (before worker.Start so spill metrics are wired)
	m := metrics.New()
	m.Registry.MustRegister(alerts.Collectors()...)
	w.SetMetrics(m)
	if cb := objstore.FindCircuitStore(store); cb != nil {
		cb.SetOnOpen(func(class objstore.OpClass) {
			m.CircuitBreakerOpened.WithLabelValues(class.String()).Inc()
		})
	}

	// Start coordinator
	durability, err := parseShuffleDurability(shuffleDurability)
	if err != nil {
		return err
	}
	coord := coordinator.New(coordinator.Config{
		NATSUrl:                embeddedNATS.ClientURL(),
		ResultBucket:           bucket,
		DynamicFilters:         dynamicFiltersFromEnv(),
		LocalFastPathBytes:     localFastPathBytes,
		IntermediateTTL:        queryIntermediateTTL,
		BroadcastBytesOverride: broadcastBytes,
		SortMergeJoinBytes:     sortMergeJoinBytes,
		LateMaterialization:    lateMaterialization,
		SkewSplit:              skewSplit,
		AggPartialSplit:        aggPartialSplit,
		StreamingExchange:      streamingExchange,
		EagerDispatch:          eagerDispatch,
		ShuffleDurability:      durability,
		LocalityPlacement:      localityPlacement,
	}, cat, nc, js, logger)

	// Phase A: same-process data-plane server + client when enabled.
	// Worker dials localhost:dataPlaneAddr. Phase B: coord registers
	// gather receivers; worker streams results via dpClient.
	// Phase C: coord pushes TaskDispatch over the gRPC stream; worker.Start
	// (below) sees a non-nil dpClient and skips its JetStream Fetch loop.
	var dpSrv *dataplane.Server
	var dpClient *dataplane.Client
	if dataPlane == "grpc" {
		dpSrv = dataplane.NewServer(dataplane.ServerConfig{
			Addr:      dataPlaneAddr,
			ClusterID: clusterID,
		}, logger)
		if err := dpSrv.Start(); err != nil {
			return fmt.Errorf("dataplane server: %w", err)
		}
		defer dpSrv.Stop(3 * time.Second)
		coord.SetDataPlaneServer(dpSrv)

		dpClient = dataplane.NewClient(dataplane.ClientConfig{
			CoordAddr: dpSrv.Addr(),
			WorkerID:  "standalone-worker",
			BuildSHA:  buildSHA(),
		}, logger)
		dpClient.Start(ctx)
		defer dpClient.Stop()
		w.SetDataPlaneClient(dpClient)
	}

	if err := w.Start(ctx); err != nil {
		return fmt.Errorf("starting worker: %w", err)
	}
	defer w.Stop()

	// Start heartbeat monitoring, query reaping, active check, and result cleanup
	coord.Workers().StartReaper(ctx)
	coord.Workers().StartSubStatsLogger(ctx)
	coord.StartQueryReaper(ctx)
	coord.StartQueryActiveHandler()
	coord.Cleaner(store, bucket).StartPeriodicCleanup(ctx, queryIntermediateGC)

	// OpenTelemetry. runStandalone did not call this at all, so the
	// `telemetry:` section and WADJET_OTEL_* reached nothing in the DEFAULT
	// run mode while working in the other two — "the config reaches runtime"
	// has to mean every mode that has the consumer, not two of three.
	if otelTP := initTelemetry(ctx, logger); otelTP != nil {
		coord.SetTelemetry(otelTP)
		defer otelTP.Shutdown(context.Background())
	}

	// Enable alerts feature flag; in standalone mode StartLeaderWatch is a
	// no-op so we start the scheduler directly here if enabled.
	coord.SetAlertsEnabled(alertsEnabled)
	if alertsEnabled {
		coord.StartAlertScheduler(ctx)
	}

	// Wire CLI-driven catalog snapshot options.
	if snapshotPrefix != "" {
		snapBucket, snapPath, err := parseS3URL(snapshotPrefix)
		if err != nil {
			return fmt.Errorf("parsing --catalog-snapshot-s3-prefix: %w", err)
		}
		coord.SetCatalogSnapshotOptions(catalog.SnapshotOptions{
			Store: store, Bucket: snapBucket, Prefix: snapPath,
		})
		coord.SetCatalogSnapshotInterval(snapshotInterval)
		if err := coord.MaybeRestoreCatalog(ctx, forceRestoreTS); err != nil {
			return fmt.Errorf("restoring catalog: %w", err)
		}
		// Standalone mode skips leader-election, so start the loop directly.
		coord.StartCatalogSnapshotLoop(ctx)
	}

	// Start background compaction
	compactor := compaction.NewBackgroundCompactor(cat, compaction.BackgroundConfig{
		Enabled:              backgroundCompaction,
		Compaction:           compaction.DefaultConfig(),
		ReclaimDroppedTables: reclaimDroppedTables,
	}, logger)
	compactor.Start(ctx)

	// Build config manager and auth provider for hot-reload
	srvCfg := server.Config{
		Addr:                httpAddr,
		Catalog:             cat,
		Coordinator:         coord,
		Metrics:             m,
		SortMergeJoinBytes:  sortMergeJoinBytes,
		LateMaterialization: lateMaterialization,
	}

	var cfgMgr *config.Manager
	var provider *auth.Provider
	// Hoisted out of the block below: the pgwire DB is opened further down
	// and needs the same limits.
	// The cost guard reaches every planner a served query can meet: the
	// HTTP server's own (embedded, no-coordinator) path, the coordinator's
	// four, and — below — the embedded DB that pgwire falls back to for any
	// statement its routing gate declines and for every statement when a
	// provider is present but disabled (#803).
	//
	// It is wired from the RESOLVED config and OUTSIDE the `--config` block.
	// Both assignments used to live inside it, so a deployment that exported
	// WADJET_QUERY_MAX_SCAN_BYTES and passed no config file resolved the key,
	// reported it through GET /v1/admin/config, and ran with no cost guard at
	// all — #808's own shape surviving in one corner. Per-role limits still
	// come from the file, because roles do.
	globalLimits, roleLimits := effectiveConfig().EffectiveQueryLimits()
	srvCfg.QueryLimits, srvCfg.RoleLimits = globalLimits, roleLimits
	coord.SetQueryLimits(globalLimits, roleLimits)

	if configFile != "" {
		fileCfg, mgr, prov, wireErr := wireAuthFromConfig(ctx, configFile, logger)
		if wireErr != nil {
			return wireErr
		}
		cfgMgr, provider = mgr, prov
		srvCfg.Provider = provider

		if fileCfg.Auth.MTLS.Enabled {
			tlsCfg, err := buildTLSConfig(fileCfg.Auth.MTLS)
			if err != nil {
				return fmt.Errorf("configuring mTLS: %w", err)
			}
			srvCfg.TLSConfig = tlsCfg
		}
	}

	// Configure embedding provider for embed() if one is requested.
	configureEmbeddingProvider(logger)

	// Start HTTP server
	srv := server.New(srvCfg, logger)

	// Register admin API if config manager is available
	// Coordinator-side ABAC: with the provider wired, ExecuteSQL enforces
	// table/row/column policies itself, which lets pgwire route authed
	// connections through the native-DAG executor and local fast path.
	if provider != nil {
		coord.SetAuthProvider(provider)
	}

	if cfgMgr != nil && provider != nil {
		admin := server.NewAdminAPI(cfgMgr, provider, logger)
		admin.RegisterRoutes(srv.Mux())
	}

	// Register ops API (workers, cleanup)
	ops := server.NewOpsAPI(coord)
	ops.RegisterRoutes(srv.Mux())

	// Start gRPC server
	grpcSrv := server.NewGRPCServer(server.GRPCConfig{
		Addr:         grpcAddr,
		Catalog:      cat,
		Coord:        coord,
		AuthProvider: provider,
	}, logger)

	// Start PostgreSQL wire protocol server
	pgDB, err := wadjet.Open(ctx, wadjet.Config{
		Store:        store,
		Bucket:       bucket,
		MetaKV:       kv,
		AuthProvider: provider,
		QueryLimits:  globalLimits,
		RoleLimits:   roleLimits,
	})
	if err != nil {
		return fmt.Errorf("opening DB for pgwire: %w", err)
	}
	pgQueryTimeout, _ := time.ParseDuration(queryTimeout)
	pgCfg := pgwire.Config{
		AuthProvider:     provider,
		QueryTimeout:     pgQueryTimeout,
		MaxConcurrentQry: maxConcurrentQry,
	}
	if pgTLSCert != "" && pgTLSKey != "" {
		cert, err := tls.LoadX509KeyPair(pgTLSCert, pgTLSKey)
		if err != nil {
			return fmt.Errorf("loading pgwire TLS cert: %w", err)
		}
		pgCfg.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}}
	}
	pgSrv := pgwire.NewServer(pgDB, pgCfg, logger)
	// Route SELECT/WITH through coord.ExecuteSQL (native-DAG executor) when
	// available — bypasses the legacy db.Query CollectSink materialization
	// path that OOMed on Q18 SF10 (project_q18_sf10_native_dag_oom_2026-04-24).
	pgSrv.SetCoordinator(coord)

	errCh := make(chan error, 3)
	go func() {
		errCh <- srv.Start()
	}()
	go func() {
		errCh <- grpcSrv.Start()
	}()
	go func() {
		if err := pgSrv.Start(pgAddr); err != nil {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutting down...")
		// Reap query intermediates before exit. In-flight queries are
		// dying with us, so their queries/<id>/* prefix on the data
		// store would otherwise leak — the per-query cleanupQuery hook
		// only fires on graceful completion. Best-effort with a 3 s cap
		// so SIGTERM->SIGKILL still completes within the harness's 5 s
		// shutdown deadline.
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 3*time.Second)
		if cleaner := coord.Cleaner(store, bucket); cleaner != nil {
			_, _ = cleaner.CleanAll(cleanupCtx)
		}
		cancelCleanup()
		coord.Workers().Close()
		pgSrv.Shutdown()
		grpcSrv.Shutdown()
		srv.Shutdown(context.Background())
		return nil
	case err := <-errCh:
		return err
	}
}

func runCoordinator(ctx context.Context, store objstore.Store, logger *slog.Logger, alertsEnabled bool, snapshotPrefix string, snapshotInterval time.Duration, forceRestoreTS string) error {
	// Start embedded NATS (with optional leaf node connections)
	// Bind to 0.0.0.0 so remote workers can connect.
	natsCfg := natsServerConfig()
	natsCfg.Host = "0.0.0.0"
	// Apply NATS mTLS config: CLI flag, then env var, then the config file
	// (#827). A config file that will not PARSE is a startup error, and
	// partially-specified material is a startup error.
	natsFileCfg, err := loadConfigForNATSTLS()
	if err != nil {
		return err
	}
	if err := applyNATSTLS(&natsCfg, natsFileCfg, logger); err != nil {
		return err
	}
	embeddedNATS, err := distributed.NewEmbeddedNATS(natsCfg, logger)
	if err != nil {
		return fmt.Errorf("starting NATS: %w", err)
	}
	defer embeddedNATS.Shutdown()

	nc, err := distributed.ConnectInProcess(embeddedNATS.Server())
	if err != nil {
		return fmt.Errorf("connecting to NATS: %w", err)
	}
	defer nc.Close()

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		return fmt.Errorf("creating JetStream: %w", err)
	}

	if err := distributed.SetupStreams(ctx, js); err != nil {
		return fmt.Errorf("setting up streams: %w", err)
	}

	// Create catalog with NATS KV metadata store and cluster identity
	kv, err := catalog.NewNATSKV(js)
	if err != nil {
		return fmt.Errorf("creating catalog KV: %w", err)
	}
	cat := catalog.NewWithCluster(kv, store, bucket, clusterID)
	if err := cat.Init(ctx); err != nil {
		return fmt.Errorf("initializing catalog: %w", err)
	}

	// Restore persisted UDFs and wire persistence callback
	wireUDFPersistence(cat, logger)

	durability, err := parseShuffleDurability(shuffleDurability)
	if err != nil {
		return err
	}
	coord := coordinator.New(coordinator.Config{
		NATSUrl:                embeddedNATS.ClientURL(),
		ResultBucket:           bucket,
		DynamicFilters:         dynamicFiltersFromEnv(),
		LocalFastPathBytes:     localFastPathBytes,
		IntermediateTTL:        queryIntermediateTTL,
		BroadcastBytesOverride: broadcastBytes,
		SortMergeJoinBytes:     sortMergeJoinBytes,
		LateMaterialization:    lateMaterialization,
		SkewSplit:              skewSplit,
		AggPartialSplit:        aggPartialSplit,
		StreamingExchange:      streamingExchange,
		EagerDispatch:          eagerDispatch,
		ShuffleDurability:      durability,
		LocalityPlacement:      localityPlacement,
	}, cat, nc, js, logger)

	// Phase A: start data-plane gRPC server alongside coord when enabled.
	// Phase B: coord registers gather receivers as ResultHandlers so
	// workers can stream results over gRPC instead of NATS.
	var dpSrv *dataplane.Server
	if dataPlane == "grpc" {
		dpSrv = dataplane.NewServer(dataplane.ServerConfig{
			Addr:      dataPlaneAddr,
			ClusterID: clusterID,
		}, logger)
		if err := dpSrv.Start(); err != nil {
			return fmt.Errorf("dataplane server: %w", err)
		}
		defer dpSrv.Stop(3 * time.Second)
		coord.SetDataPlaneServer(dpSrv)
	}

	// Initialize OTel tracing if configured
	otelTP := initTelemetry(ctx, logger)
	if otelTP != nil {
		coord.SetTelemetry(otelTP)
		defer otelTP.Shutdown(context.Background())
	}

	coord.Workers().StartReaper(ctx)
	coord.Workers().StartSubStatsLogger(ctx)
	coord.StartQueryReaper(ctx)
	coord.StartQueryActiveHandler()
	coord.Cleaner(store, bucket).StartPeriodicCleanup(ctx, queryIntermediateGC)

	// Enable alerts feature flag; in coordinator mode StartLeaderWatch manages
	// the scheduler lifecycle on leader transitions.
	coord.SetAlertsEnabled(alertsEnabled)

	// Wire CLI-driven catalog snapshot options.
	if snapshotPrefix != "" {
		snapBucket, snapPath, err := parseS3URL(snapshotPrefix)
		if err != nil {
			return fmt.Errorf("parsing --catalog-snapshot-s3-prefix: %w", err)
		}
		coord.SetCatalogSnapshotOptions(catalog.SnapshotOptions{
			Store: store, Bucket: snapBucket, Prefix: snapPath,
		})
		coord.SetCatalogSnapshotInterval(snapshotInterval)
		if err := coord.MaybeRestoreCatalog(ctx, forceRestoreTS); err != nil {
			return fmt.Errorf("restoring catalog: %w", err)
		}
		// In coordinator mode, StartLeaderWatch fires StartCatalogSnapshotLoop
		// on leader election, so we do NOT call it here.
	}

	// Start background compaction
	coordCompactor := compaction.NewBackgroundCompactor(cat, compaction.BackgroundConfig{
		Enabled:              backgroundCompaction,
		Compaction:           compaction.DefaultConfig(),
		ReclaimDroppedTables: reclaimDroppedTables,
	}, logger)
	coordCompactor.Start(ctx)

	m := metrics.New()
	m.Registry.MustRegister(alerts.Collectors()...)
	if cb := objstore.FindCircuitStore(store); cb != nil {
		cb.SetOnOpen(func(class objstore.OpClass) {
			m.CircuitBreakerOpened.WithLabelValues(class.String()).Inc()
		})
	}
	dlq := coordinator.NewDLQ(js)

	srvCfg := server.Config{
		Addr:                httpAddr,
		Catalog:             cat,
		Coordinator:         coord,
		DLQ:                 dlq,
		Metrics:             m,
		SortMergeJoinBytes:  sortMergeJoinBytes,
		LateMaterialization: lateMaterialization,
	}

	var cfgMgr *config.Manager
	var provider *auth.Provider

	// The cost guard, from the resolved config and outside the `--config`
	// block — see runStandalone. This mode serves no pgwire listener (#803).
	globalLimits, roleLimits := effectiveConfig().EffectiveQueryLimits()
	srvCfg.QueryLimits, srvCfg.RoleLimits = globalLimits, roleLimits
	coord.SetQueryLimits(globalLimits, roleLimits)

	if configFile != "" {
		fileCfg, mgr, prov, wireErr := wireAuthFromConfig(ctx, configFile, logger)
		if wireErr != nil {
			return wireErr
		}
		cfgMgr, provider = mgr, prov
		srvCfg.Provider = provider

		if fileCfg.Auth.MTLS.Enabled {
			tlsCfg, err := buildTLSConfig(fileCfg.Auth.MTLS)
			if err != nil {
				return fmt.Errorf("configuring mTLS: %w", err)
			}
			srvCfg.TLSConfig = tlsCfg
		}
	}

	// Configure embedding provider for coordinator mode
	configureEmbeddingProvider(logger)

	srv := server.New(srvCfg, logger)

	// Coordinator-side ABAC: with the provider wired, ExecuteSQL enforces
	// table/row/column policies itself, which lets pgwire route authed
	// connections through the native-DAG executor and local fast path.
	if provider != nil {
		coord.SetAuthProvider(provider)
	}

	if cfgMgr != nil && provider != nil {
		admin := server.NewAdminAPI(cfgMgr, provider, logger)
		admin.RegisterRoutes(srv.Mux())
	}

	// Register ops API
	ops := server.NewOpsAPI(coord)
	ops.RegisterRoutes(srv.Mux())

	// Start gRPC server
	grpcSrv := server.NewGRPCServer(server.GRPCConfig{
		Addr:         grpcAddr,
		Catalog:      cat,
		Coord:        coord,
		AuthProvider: provider,
	}, logger)

	errCh := make(chan error, 2)
	go func() {
		errCh <- srv.Start()
	}()
	go func() {
		errCh <- grpcSrv.Start()
	}()

	select {
	case <-ctx.Done():
		logger.Info("coordinator shutting down...")
		// Same shutdown reap as standalone (see standalone path comment).
		// In-flight queries die with us; their queries/<id>/* prefix would
		// otherwise leak.
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 3*time.Second)
		if cleaner := coord.Cleaner(store, bucket); cleaner != nil {
			_, _ = cleaner.CleanAll(cleanupCtx)
		}
		cancelCleanup()
		coord.Workers().Close()
		grpcSrv.Shutdown()
		srv.Shutdown(context.Background())
		return nil
	case err := <-errCh:
		return err
	}
}

func runWorker(ctx context.Context, store objstore.Store, logger *slog.Logger) error {
	natsAddr := natsURL
	if natsAddr == "" {
		natsAddr = fmt.Sprintf("nats://127.0.0.1:%d", natsPort)
	}

	// Build NATS client TLS config if mTLS is configured. The config file is
	// a tier here, and material named but unusable is a startup error rather
	// than a silent plaintext connection (#827).
	natsFileCfg, err := loadConfigForNATSTLS()
	if err != nil {
		return err
	}
	var natsTLSCfg *tls.Config
	tlsCert, tlsKey, tlsCA, err := resolveNATSTLSPaths(natsFileCfg)
	if err != nil {
		return err
	}
	if tlsCert != "" && tlsKey != "" && tlsCA != "" {
		natsTLSCfg, err = distributed.BuildNATSClientTLS(tlsCert, tlsKey, tlsCA)
		if err != nil {
			return fmt.Errorf("building NATS TLS config: %w", err)
		}
		logger.Info("NATS mTLS enabled", "ca", tlsCA, "cert", tlsCert)
	}

	nc, err := distributed.Connect(natsAddr, natsTLSCfg)
	if err != nil {
		return fmt.Errorf("connecting to NATS: %w", err)
	}
	defer nc.Close()

	// Dedicated control-plane connection for the heartbeat publish path.
	// Separate from `nc` (data plane) so heartbeat traffic can't share fate
	// with bursty gather/result publishes that wedged the data connection
	// past JetStream AckWait on the 2026-05-02 SF10 EC2 deploy.
	controlNC, err := distributed.Connect(natsAddr, natsTLSCfg)
	if err != nil {
		return fmt.Errorf("connecting control-plane NATS: %w", err)
	}
	defer controlNC.Close()

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		return fmt.Errorf("creating JetStream: %w", err)
	}

	// Load GeoIP databases if configured (from the resolved config; see
	// runStandalone).
	if err := loadGeoIP(logger); err != nil {
		return fmt.Errorf("loading GeoIP: %w", err)
	}
	defer geoip.Close()

	// Generate a worker id once so both the existing NATS worker code and
	// the new data-plane client share the same identity.
	workerID := "worker-" + uuid.New().String()[:8]

	w := worker.New(worker.Config{
		WorkerID:              workerID,
		NATSUrl:               natsAddr,
		ClusterID:             clusterID,
		MaxConcurrent:         maxConcurrent,
		MorselWorkers:         morselWorkersConfig(morselWorkers),
		DrainTimeout:          drainTimeout,
		CacheBytes:            cacheBytes,
		MemoryBudget:          memoryBudget,
		SharedPoolBudget:      sharedPoolBudget,
		SpillDir:              spillDir,
		ResultStoreBytes:      resultStoreBytes,
		Reservoirs:            memory.NewReservoirRegistry(),
		FloatingBudgetActive:  spillFloatingBudget,
		MmapRelief:            mmapRelief,
		MmapReliefThresholdMB: mmapReliefThresholdMB,
		BoundedDirtyWrites:    boundedDirtyWrites,
		PeerListenAddr:        peerListenAddr(),
		PeerAdvertiseAddr:     peerExchangeAdvertise,
		StreamingShuffleRead:  streamingShuffleRead,
		AsyncScratchPurge:     asyncScratchPurge,
		PeerWireCompression:   peerWireCompression,
		ScanDecodeAhead:       scanDecodeAhead,
		ScanDecodeAheadBytes:  scanDecodeAheadBytes,
		ShuffleDecodeAhead:    shuffleDecodeAhead,
		DecodedCacheBytes:     decodedCacheBytes,
	}, store, nc, js, logger)
	w.SetControlConn(controlNC)

	// Phase A: open the data-plane stream to coord when enabled. Heartbeats,
	// cancellation, KV stay on NATS. Phases B–E migrate task dispatch,
	// results, gather, progress onto this stream.
	var dpClient *dataplane.Client
	if dataPlane == "grpc" {
		addr := coordDataPlane
		if addr == "" {
			// Derive from natsAddr host: keep host, swap port to data-plane.
			addr = deriveDataPlaneAddr(natsAddr, dataPlaneAddr)
		}
		dpClient = dataplane.NewClient(dataplane.ClientConfig{
			CoordAddr: addr,
			WorkerID:  workerID,
			BuildSHA:  buildSHA(),
		}, logger)
		dpClient.Start(ctx)
		defer dpClient.Stop()
		w.SetDataPlaneClient(dpClient)
	}

	// Opt-in heap+goroutine pprof dumper (env-gated). Workers are silent
	// to journald under buffered cloud-init pipes, so disk-snapshot pprof
	// is the only signal that survives across a stall window.
	startHeapDumper(ctx, logger)

	// Initialize Prometheus metrics
	m := metrics.New()
	w.SetMetrics(m)
	if cb := objstore.FindCircuitStore(store); cb != nil {
		cb.SetOnOpen(func(class objstore.OpClass) {
			m.CircuitBreakerOpened.WithLabelValues(class.String()).Inc()
		})
	}

	// Start /metrics HTTP endpoint for Prometheus scraping, plus the
	// Kubernetes lifecycle surface: liveness, readiness (false once
	// draining, so the pod drops out of any Service while it finishes),
	// and a POST /drain admin hook (preStop-hook alternative to SIGTERM).
	metricsMux := http.NewServeMux()
	metricsMux.HandleFunc("/healthz", func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(http.StatusOK)
		rw.Write([]byte("ok"))
	})
	metricsMux.HandleFunc("/readyz", func(rw http.ResponseWriter, _ *http.Request) {
		if w.Draining() {
			rw.WriteHeader(http.StatusServiceUnavailable)
			rw.Write([]byte("draining"))
			return
		}
		rw.WriteHeader(http.StatusOK)
		rw.Write([]byte("ok"))
	})
	metricsMux.HandleFunc("/drain", func(rw http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			rw.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.BeginDrain()
		rw.WriteHeader(http.StatusAccepted)
		rw.Write([]byte("draining"))
	})
	metricsMux.Handle("/metrics", m.Handler())
	metricsMux.HandleFunc("/debug/pprof/", nethttppprof.Index)
	metricsMux.HandleFunc("/debug/pprof/cmdline", nethttppprof.Cmdline)
	metricsMux.HandleFunc("/debug/pprof/profile", nethttppprof.Profile)
	metricsMux.HandleFunc("/debug/pprof/symbol", nethttppprof.Symbol)
	metricsMux.HandleFunc("/debug/pprof/trace", nethttppprof.Trace)
	metricsMux.Handle("/debug/pprof/goroutine", nethttppprof.Handler("goroutine"))
	metricsMux.Handle("/debug/pprof/heap", nethttppprof.Handler("heap"))
	metricsMux.Handle("/debug/pprof/allocs", nethttppprof.Handler("allocs"))
	metricsSrv := &http.Server{Addr: metricsAddr, Handler: metricsMux}
	go func() {
		logger.Info("worker metrics server listening", "addr", metricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("metrics server error", "error", err)
		}
	}()
	defer metricsSrv.Shutdown(context.Background())

	// Initialize OTel tracing on worker
	workerOtelTP := initTelemetry(ctx, logger)
	if workerOtelTP != nil {
		w.SetTelemetry(workerOtelTP)
		defer workerOtelTP.Shutdown(context.Background())
	}

	if err := w.Start(ctx); err != nil {
		return fmt.Errorf("starting worker: %w", err)
	}

	// Signal contract for workers (Kubernetes-compatible):
	//   SIGTERM, SIGQUIT  -> graceful drain: stop taking tasks, finish
	//                        in-flight work, flush stage-output uploads,
	//                        exit. K8s sends SIGTERM on pod termination;
	//                        --drain-timeout (or the pod's grace period)
	//                        bounds it.
	//   SIGINT            -> hard stop (interactive Ctrl-C).
	// The serve command's NotifyContext claimed SIGINT+SIGTERM for a hard
	// cancel before we got here; detach both so SIGTERM cannot race into
	// the hard path and cancel in-flight task contexts mid-drain.
	signal.Reset(syscall.SIGINT, syscall.SIGTERM)
	hardCh := make(chan os.Signal, 1)
	signal.Notify(hardCh, syscall.SIGINT)
	drainSigCh := make(chan os.Signal, 1)
	signal.Notify(drainSigCh, syscall.SIGTERM, syscall.SIGQUIT)

	select {
	case <-hardCh:
		logger.Info("SIGINT received, stopping worker...")
		w.Stop()
	case sig := <-drainSigCh:
		logger.Info("drain signal received, draining worker...", "signal", sig.String())
		w.Drain()
	case <-w.DrainRequested():
		// NATS drain subject (coordinator reap) or POST /drain.
		logger.Info("drain requested via control plane, draining worker...")
		w.Drain()
	}
	return nil
}

// wireUDFPersistence loads persisted UDFs from the catalog and sets up
// a callback so future UDF changes are automatically saved to KV.
func catalogCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "catalog",
		Short: "Catalog management commands",
	}
	cmd.AddCommand(catalogSnapshotCmd())
	return cmd
}

func catalogSnapshotCmd() *cobra.Command {
	var coordAddr string
	cmd := &cobra.Command{
		Use:   "snapshot",
		Short: "Take a catalog snapshot via a running coordinator",
		RunE: func(cmd *cobra.Command, args []string) error {
			if coordAddr == "" {
				return fmt.Errorf("snapshots require a running coordinator; pass --coord-addr=host:port")
			}

			ctx := context.Background()
			conn, err := pgx.Connect(ctx, "postgres://wadjet@"+coordAddr+"/wadjet?sslmode=disable")
			if err != nil {
				return fmt.Errorf("connect to coordinator: %w", err)
			}
			defer conn.Close(ctx)

			rows, err := conn.Query(ctx, "CREATE SNAPSHOT")
			if err != nil {
				return fmt.Errorf("executing CREATE SNAPSHOT: %w", err)
			}
			defer rows.Close()

			for rows.Next() {
				vals, err := rows.Values()
				if err != nil {
					return err
				}
				fmt.Println(vals)
			}
			return rows.Err()
		},
	}
	cmd.Flags().StringVar(&coordAddr, "coord-addr", "", "coordinator pgwire address (host:port)")
	return cmd
}

// dynamicFiltersFromEnv reads WADJET_DYNAMIC_FILTERS and reports whether
// the Trino-style semi-join dynamic-filter optimization should be enabled
// on the coordinator. Accepts "1", "true" (case-insensitive). Off otherwise
// — v1 default until validated at SF10/SF100.
// parseShuffleDurability maps the --shuffle-durability flag value to the
// wire policy ("eager" is the zero value).
func parseShuffleDurability(s string) (distributed.UploadPolicy, error) {
	switch s {
	case "", "eager":
		return distributed.UploadEager, nil
	case "lazy":
		return distributed.UploadLazy, nil
	case "off":
		return distributed.UploadOff, nil
	default:
		return "", fmt.Errorf("invalid --shuffle-durability %q (want eager, lazy, or off)", s)
	}
}

func dynamicFiltersFromEnv() bool {
	v := os.Getenv("WADJET_DYNAMIC_FILTERS")
	return v == "1" || strings.EqualFold(v, "true")
}

// configureEmbeddingProvider wires the embed() SQL function to an embedding
// provider selected by WADJET_EMBED_PROVIDER: "openai" (default), "voyage", or
// "ollama". Each provider reads its own key/model env vars. If the selected
// provider has no credentials, embed() is left unregistered (returns NULL).
//
// Note: Anthropic has no native embeddings endpoint — "voyage" is Anthropic's
// officially recommended embeddings path.
func configureEmbeddingProvider(logger *slog.Logger) {
	provider := strings.ToLower(os.Getenv("WADJET_EMBED_PROVIDER"))
	model := os.Getenv("WADJET_EMBED_MODEL")
	cache := embedding.NewCache(50000)

	// Optional explicit output dimension. Required for any provider/model whose
	// true width isn't in the provider's built-in table (notably custom Ollama
	// models) — vecEmbed validates the returned width and NULLs mismatched rows,
	// so a wrong/missing dimension fails loudly rather than corrupting vectors.
	dim := 0
	if d := os.Getenv("WADJET_EMBED_DIM"); d != "" {
		if v, err := strconv.Atoi(d); err == nil && v > 0 {
			dim = v
		}
	}

	// Default to OpenAI when no provider is named (back-compat: the only knob
	// that previously existed was WADJET_OPENAI_API_KEY).
	if provider == "" {
		provider = "openai"
	}

	var p embedding.Provider
	switch provider {
	case "voyage":
		apiKey := os.Getenv("WADJET_VOYAGE_API_KEY")
		if apiKey == "" {
			return
		}
		p = embedding.NewVoyage(embedding.VoyageConfig{
			APIKey:     apiKey,
			Model:      model,
			Dimensions: dim,
			InputType:  os.Getenv("WADJET_VOYAGE_INPUT_TYPE"),
		}, cache)
	case "ollama":
		// Ollama is local and keyless; enable it only when explicitly selected.
		p = embedding.NewOllama(embedding.OllamaConfig{
			Model:      model,
			Dimensions: dim,
			BaseURL:    os.Getenv("WADJET_OLLAMA_URL"),
		}, cache)
	case "openai":
		apiKey := os.Getenv("WADJET_OPENAI_API_KEY")
		if apiKey == "" {
			return
		}
		if model == "" {
			model = "text-embedding-3-small"
		}
		p = embedding.NewOpenAI(embedding.OpenAIConfig{
			APIKey:     apiKey,
			Model:      model,
			Dimensions: dim,
		}, cache)
	default:
		logger.Warn("unknown WADJET_EMBED_PROVIDER, embed() disabled", "provider", provider)
		return
	}

	embedding.SetProvider(p)
	embedding.RegisterFunctions()
	logger.Info("embedding provider configured", "provider", provider, "model", p.Model(), "dim", p.Dimension())
}

func wireUDFPersistence(cat *catalog.Catalog, logger *slog.Logger) {
	// Load existing UDFs from KV
	kvDefs, err := cat.LoadUDFs()
	if err != nil {
		logger.Warn("failed to load persisted UDFs", "error", err)
	} else if len(kvDefs) > 0 {
		exprDefs := make([]expr.UDFDef, len(kvDefs))
		for i, d := range kvDefs {
			exprDefs[i] = expr.UDFDef{
				Name:   d.Name,
				Params: d.Params,
				Body:   d.Body,
				Owner:  d.Owner,
				Locked: d.Locked,
			}
		}
		loaded := expr.DefaultUDFs.LoadDefs(exprDefs)
		logger.Info("restored persisted UDFs", "count", loaded)
	}

	// Wire persistence callback for future mutations
	expr.DefaultUDFs.SetPersister(func(udfs []expr.UDFDef) error {
		catDefs := make([]catalog.UDFDef, len(udfs))
		for i, d := range udfs {
			catDefs[i] = catalog.UDFDef{
				Name:   d.Name,
				Params: d.Params,
				Body:   d.Body,
				Owner:  d.Owner,
				Locked: d.Locked,
			}
		}
		return cat.SaveUDFs(catDefs)
	})
}

// buildAuthConfig converts config.Auth to auth.Config without creating the Authenticator.
func buildAuthConfig(cfg config.Auth) auth.Config {
	authCfg := auth.Config{
		Enabled: cfg.Enabled,
		Roles:   make([]auth.RoleConfig, len(cfg.Roles)),
		APIKeys: make([]auth.APIKeyDef, len(cfg.APIKeys)),
	}
	for i, r := range cfg.Roles {
		authCfg.Roles[i] = auth.RoleConfig{Name: r.Name, Tables: r.Tables, Allow: r.Allow}
	}
	for i, k := range cfg.APIKeys {
		authCfg.APIKeys[i] = auth.APIKeyDef{Key: k.Key, Name: k.Name, Role: k.Role}
	}
	if cfg.JWT.Enabled {
		authCfg.JWT = auth.JWTConfig{
			Enabled:       true,
			Secret:        cfg.JWT.Secret,
			PublicKeyFile: cfg.JWT.PublicKeyFile,
			RoleClaim:     cfg.JWT.RoleClaim,
			Issuer:        cfg.JWT.Issuer,
		}
	}
	if cfg.MTLS.Enabled {
		authCfg.MTLS = auth.MTLSConfig{
			Enabled:     true,
			CAFile:      cfg.MTLS.CAFile,
			RoleMap:     cfg.MTLS.RoleMap,
			DefaultRole: cfg.MTLS.DefaultRole,
		}
	}
	return authCfg
}

// buildPolicyConfigs converts config.AuthPolicy to auth.PolicyConfig slices.
func buildPolicyConfigs(cfgs []config.AuthPolicy) []auth.PolicyConfig {
	policyCfgs := make([]auth.PolicyConfig, len(cfgs))
	for i, c := range cfgs {
		policyCfgs[i] = auth.PolicyConfig{
			Table:     c.Table,
			Role:      c.Role,
			Columns:   c.Columns,
			RowFilter: c.RowFilter,
		}
	}
	return policyCfgs
}

// wireAuthFromConfig loads the YAML config file and builds the hot-reloadable
// auth provider both serve modes share. It is the single place a config-borne
// security control comes into existence, so it is the single place one can be
// REFUSED.
//
// Nothing here degrades. Before #802 the two callers wrote `if cfg, loadErr :=
// config.Load(configFile); loadErr == nil { ... }` — an unreadable config file
// silently started a server with NO authentication at all — and the policy
// parse underneath turned an unrecognised `columns:` action into a grant. Both
// failures now stop the process with the reason.
//
// The hot-reload subscription refuses the same way: a reload whose policies do
// not parse is logged and dropped, and the provider keeps the state it already
// had rather than swapping in a weaker one.
func wireAuthFromConfig(ctx context.Context, configFile string, logger *slog.Logger) (*config.Config, *config.Manager, *auth.Provider, error) {
	// The file was read and resolved once, in the root command's
	// PersistentPreRunE, and an unparseable one stopped the process there.
	// Loading it a second time here would give the auth provider a
	// different view of the configuration from the one the rest of the
	// process runs on — which is the whole of #808 in miniature.
	res := effectiveResolution()
	cfg := res.Config()

	provider, err := buildProviderFromConfig(cfg, logger)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("loading config %q: %w", configFile, err)
	}

	// The manager carries the RESOLUTION, so GET /v1/admin/config reports
	// the effective value of every key with the tier it came from, instead
	// of a file-and-defaults view of a process running on something else
	// (#828).
	cfgMgr := config.NewManagerFromResolution(res, logger)
	// Declaring the keys is what makes them hot-reloadable: auth is the one
	// section with a subscriber that applies a change at runtime, so it is
	// the one section the admin API will accept a write for.
	cfgMgr.SubscribeKeys([]string{"auth"}, func(event config.ChangeEvent) {
		authCfg := buildAuthConfig(event.New.Auth)
		policyCfgs := buildPolicyConfigs(event.New.Auth.Policies)
		abacPolicies := buildABACPolicies(event.New.Auth.ABACPolicies)
		if err := provider.UpdateFromConfig(authCfg, policyCfgs, abacPolicies...); err != nil {
			logger.Error("auth hot-reload REFUSED — keeping the previous configuration",
				"path", configFile, "error", err)
			return
		}
		logger.Info("auth hot-reloaded",
			"enabled", event.New.Auth.Enabled,
			"api_keys", len(event.New.Auth.APIKeys),
			"roles", len(event.New.Auth.Roles),
			"policies", len(event.New.Auth.Policies),
			"abac_policies", len(event.New.Auth.ABACPolicies),
		)
	})

	logger.Info("authentication enabled (hot-reloadable)",
		"api_keys", len(cfg.Auth.APIKeys),
		"jwt", cfg.Auth.JWT.Enabled,
		"mtls", cfg.Auth.MTLS.Enabled,
		"roles", len(cfg.Auth.Roles),
		"policies", len(cfg.Auth.Policies),
	)

	watcher := config.NewWatcher(config.WatcherConfig{Path: configFile}, cfgMgr, logger)
	go watcher.Watch(ctx)

	return cfg, cfgMgr, provider, nil
}

// buildProviderFromConfig constructs an auth.Provider from a loaded config,
// mirroring the coordinator/standalone wiring but WITHOUT the hot-reload
// watcher (callers that need a one-shot provider, e.g. the mcp command). Auth
// disabled in config yields an enabled==false provider, which enforcement
// treats as a no-op. logger may be nil.
func buildProviderFromConfig(cfg *config.Config, logger *slog.Logger) (*auth.Provider, error) {
	authn, authz := buildAuth(cfg.Auth)
	var policies *auth.PolicySet
	if len(cfg.Auth.Policies) > 0 {
		var err error
		policies, err = buildPolicies(cfg.Auth.Policies)
		if err != nil {
			return nil, fmt.Errorf("auth policies: %w", err)
		}
	}
	provider := auth.NewProvider(authn, authz, policies, logger)
	if len(cfg.Auth.ABACPolicies) > 0 {
		abac := buildABACPolicies(cfg.Auth.ABACPolicies)
		if err := provider.UpdateFromConfig(buildAuthConfig(cfg.Auth), buildPolicyConfigs(cfg.Auth.Policies), abac...); err != nil {
			return nil, fmt.Errorf("auth policies: %w", err)
		}
	} else if len(cfg.Auth.Roles) > 0 {
		if err := provider.UpdateFromConfig(buildAuthConfig(cfg.Auth), buildPolicyConfigs(cfg.Auth.Policies)); err != nil {
			return nil, fmt.Errorf("auth policies: %w", err)
		}
	}
	return provider, nil
}

func buildAuth(cfg config.Auth) (*auth.Authenticator, *auth.Authorizer) {
	authCfg := auth.Config{
		Enabled: cfg.Enabled,
		Roles:   make([]auth.RoleConfig, len(cfg.Roles)),
		APIKeys: make([]auth.APIKeyDef, len(cfg.APIKeys)),
	}
	for i, r := range cfg.Roles {
		authCfg.Roles[i] = auth.RoleConfig{Name: r.Name, Tables: r.Tables, Allow: r.Allow}
	}
	for i, k := range cfg.APIKeys {
		authCfg.APIKeys[i] = auth.APIKeyDef{Key: k.Key, Name: k.Name, Role: k.Role}
	}
	if cfg.JWT.Enabled {
		authCfg.JWT = auth.JWTConfig{
			Enabled:       true,
			Secret:        cfg.JWT.Secret,
			PublicKeyFile: cfg.JWT.PublicKeyFile,
			RoleClaim:     cfg.JWT.RoleClaim,
			Issuer:        cfg.JWT.Issuer,
		}
	}
	if cfg.MTLS.Enabled {
		authCfg.MTLS = auth.MTLSConfig{
			Enabled:     true,
			CAFile:      cfg.MTLS.CAFile,
			RoleMap:     cfg.MTLS.RoleMap,
			DefaultRole: cfg.MTLS.DefaultRole,
		}
	}
	return auth.New(authCfg)
}

func buildPolicies(cfgs []config.AuthPolicy) (*auth.PolicySet, error) {
	return auth.ParsePolicies(buildPolicyConfigs(cfgs))
}

// buildABACPolicies converts config ABAC policies to auth ABAC policies.
func buildABACPolicies(cfgs []config.ABACPolicy) []auth.AccessControlPolicy {
	policies := make([]auth.AccessControlPolicy, len(cfgs))
	for i, p := range cfgs {
		enabled := true
		if p.Enabled != nil {
			enabled = *p.Enabled
		}
		rules := make([]auth.PolicyRule, len(p.Rules))
		for j, r := range p.Rules {
			effect := auth.EffectAllow
			if r.Effect == "deny" {
				effect = auth.EffectDeny
			}
			// Categorize conditions into subject/resource/environment
			var subjects, resources, envConds []auth.Condition
			for _, c := range r.Conditions {
				cond := auth.Condition{Attribute: c.Attribute, Op: c.Operator, Value: c.Value}
				switch {
				case len(c.Attribute) > 8 && c.Attribute[:8] == "subject.":
					cond.Attribute = c.Attribute[8:]
					subjects = append(subjects, cond)
				case len(c.Attribute) > 9 && c.Attribute[:9] == "resource.":
					cond.Attribute = c.Attribute[9:]
					resources = append(resources, cond)
				case len(c.Attribute) > 4 && c.Attribute[:4] == "env.":
					cond.Attribute = c.Attribute[4:]
					envConds = append(envConds, cond)
				default:
					// Put unprefixed conditions in subjects by default
					subjects = append(subjects, cond)
				}
			}
			obligs := make([]auth.Obligation, len(r.Obligations))
			for k, o := range r.Obligations {
				obligs[k] = auth.Obligation{
					Type:   o.Type,
					Target: o.Target,
					Value:  o.Value,
				}
			}
			rules[j] = auth.PolicyRule{
				Description: p.Description,
				EffectStr:   r.Effect,
				Effect:      effect,
				Priority:    p.Priority,
				Subjects:    subjects,
				Resources:   resources,
				Environment: envConds,
				Obligations: obligs,
			}
		}
		policies[i] = auth.AccessControlPolicy{
			Name:    p.Name,
			Enabled: enabled,
			Rules:   rules,
		}
	}
	return policies
}

// loadGeoIP loads the MaxMind GeoIP databases named by the RESOLVED
// configuration. The flag/file tie-break it used to do by hand is the
// loader's job now (ADR-0029), which is also how the environment tier
// (WADJET_GEOIP_CITY_DB / _ASN_DB) starts reaching it.
func loadGeoIP(logger *slog.Logger) error {
	g := effectiveConfig().GeoIP
	cityDB, asnDB := g.CityDB, g.ASNDB
	if cityDB == "" && asnDB == "" {
		return nil
	}
	if err := geoip.Load(cityDB, asnDB); err != nil {
		return err
	}
	logger.Info("GeoIP databases loaded", "city", cityDB, "asn", asnDB)
	return nil
}

func parseLogLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// parseGoMemLimit accepts the same humanized formats the Go runtime
// recognizes for GOMEMLIMIT (e.g. "2GiB", "2GB", "2G", "2147483648") and
// returns the value in bytes. Returns ok=false if the input doesn't parse.
// Plain integer fast path matches the historical behavior callers depended
// on (the harness writes raw bytes).
func parseGoMemLimit(s string) (int64, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		if n > 0 {
			return n, true
		}
		return 0, false
	}
	// Strip a trailing "B" — both "GiB" and "GB" end in "B" and the suffix
	// is informational; the multiplier is determined by the i-vs-no-i below.
	t := s
	if strings.HasSuffix(t, "B") || strings.HasSuffix(t, "b") {
		t = t[:len(t)-1]
	}
	binary := false
	if l := len(t); l >= 1 && (t[l-1] == 'i' || t[l-1] == 'I') {
		binary = true
		t = t[:l-1]
	}
	if len(t) == 0 {
		return 0, false
	}
	mult := int64(1)
	switch t[len(t)-1] {
	case 'K', 'k':
		if binary {
			mult = 1024
		} else {
			mult = 1000
		}
	case 'M', 'm':
		if binary {
			mult = 1024 * 1024
		} else {
			mult = 1000 * 1000
		}
	case 'G', 'g':
		if binary {
			mult = 1024 * 1024 * 1024
		} else {
			mult = 1000 * 1000 * 1000
		}
	case 'T', 't':
		if binary {
			mult = 1024 * 1024 * 1024 * 1024
		} else {
			mult = 1000 * 1000 * 1000 * 1000
		}
	default:
		return 0, false
	}
	num, err := strconv.ParseInt(t[:len(t)-1], 10, 64)
	if err != nil || num <= 0 {
		return 0, false
	}
	return num * mult, true
}

func buildTLSConfig(cfg config.AuthMTLS) (*tls.Config, error) {
	clientCA, err := auth.LoadClientCA(cfg.CAFile)
	if err != nil {
		return nil, err
	}
	return auth.NewTLSConfig(cfg.CertFile, cfg.KeyFile, clientCA)
}

// resolveNATSTLSPaths returns the NATS TLS cert/key/CA paths: CLI flag
// first, then environment variable, then the config file — resolved per
// field, so a deployment may take the certificate from a flag, the key from
// the environment and the CA from the file.
//
// Since #808's loader this is the SAME order the whole process resolves on
// (ADR-0029), and the three flag variables it reads first already hold the
// loader's answer, so the two cannot disagree —
// TestNATSTLSAgreesWithTheResolvedConfig drives all eight presence cells
// through the real command and asserts exactly that. What this function
// still owns, and the loader does not, is the REFUSAL below.
//
// Material that is NAMED and then not used is a startup error, not a
// silent downgrade. The connection is only secured when all three paths are
// present, so naming one or two of them used to disable TLS quietly; that
// now refuses to start (#827).
func resolveNATSTLSPaths(cfg *config.Config) (cert, key, ca string, err error) {
	pick := func(flag, env string, file func() string) string {
		if flag != "" {
			return flag
		}
		if v := os.Getenv(env); v != "" {
			return v
		}
		return file()
	}
	natsFile := func(get func(config.NATS) string) func() string {
		return func() string {
			if cfg == nil {
				return ""
			}
			return get(cfg.NATS)
		}
	}
	cert = pick(natsTLSCert, "WADJET_NATS_TLS_CERT", natsFile(func(n config.NATS) string { return n.TLSCert }))
	key = pick(natsTLSKey, "WADJET_NATS_TLS_KEY", natsFile(func(n config.NATS) string { return n.TLSKey }))
	ca = pick(natsTLSCA, "WADJET_NATS_TLS_CA", natsFile(func(n config.NATS) string { return n.TLSCA }))

	var missing []string
	if cert == "" {
		missing = append(missing, "certificate")
	}
	if key == "" {
		missing = append(missing, "private key")
	}
	if ca == "" {
		missing = append(missing, "CA")
	}
	if len(missing) > 0 && len(missing) < 3 {
		return "", "", "", fmt.Errorf(
			"NATS TLS is partially configured: no %s. All three of the certificate, "+
				"the private key and the CA are required, and a partial set would connect "+
				"to NATS WITHOUT TLS. Supply the rest, or remove the ones that are set",
			strings.Join(missing, " and "))
	}
	return cert, key, ca, nil
}

// initTelemetry creates an OTel TracerProvider if an OTLP endpoint is configured.
// Returns nil if no endpoint is set (tracing disabled).
func initTelemetry(ctx context.Context, logger *slog.Logger) *telemetry.Provider {
	// The three tiers used to be walked by hand here, in the OPPOSITE
	// convention to the rest of the process: a flag won only when non-empty,
	// the environment was consulted second, and the config file's
	// `telemetry:` section was never consulted at all. They are ordinary
	// resolved keys now (ADR-0029, #808).
	t := effectiveConfig().Telemetry
	if t.Endpoint == "" {
		return nil
	}
	sampleRate := t.SampleRate
	if sampleRate <= 0 {
		sampleRate = 1.0
	}

	tp, err := telemetry.Init(ctx, telemetry.Config{
		Endpoint:   t.Endpoint,
		Insecure:   t.Insecure,
		SampleRate: sampleRate,
	}, logger)
	if err != nil {
		logger.Error("failed to initialize OpenTelemetry", "error", err)
		return nil
	}
	return tp
}

// loadConfigForNATSTLS reads the config file for the NATS TLS tier and
// PROPAGATES a parse failure.
//
// Dropping that error is how the tier's own stated invariant gets
// falsified: an unparseable file that NAMES tls_cert, tls_key and tls_ca
// yields a nil config, resolveNATSTLSPaths then sees three empty strings —
// which is the legitimate "no TLS configured" shape — and the process
// connects to NATS in PLAINTEXT with no error and no warning. #802 settled
// exactly this doctrine for the auth block ("an unreadable config file
// silently started a server with NO authentication at all — that now stops
// the process with the reason"), and it applies on EVERY mode, not only the
// ones that happen to load the file again later for another reason: worker
// mode has no wireAuthFromConfig and would have run to completion.
//
// The root command's loader now refuses the same file earlier and for every
// command (resolveConfiguration), so in a real process this read is a
// second look at a file already known to parse. It stays because it is the
// security control's OWN guarantee: the tier does not depend on some other
// caller having checked first, and its gates hold it to that.
func loadConfigForNATSTLS() (*config.Config, error) {
	if configFile == "" {
		return nil, nil
	}
	cfg, err := config.Load(configFile)
	if err != nil {
		return nil, fmt.Errorf("loading config file %q: %w", configFile, err)
	}
	return cfg, nil
}

// applyNATSTLS sets TLS fields on a NATSConfig from the flag / env / config
// tiers. Used by runCoordinator to configure mTLS on the embedded NATS
// server. It returns an error when the material is partially specified,
// which would otherwise start a plaintext server (#827).
func applyNATSTLS(cfg *distributed.NATSConfig, fileCfg *config.Config, logger *slog.Logger) error {
	cert, key, ca, err := resolveNATSTLSPaths(fileCfg)
	if err != nil {
		return err
	}
	if cert != "" && key != "" && ca != "" {
		cfg.TLSCert = cert
		cfg.TLSKey = key
		cfg.TLSCA = ca
		logger.Info("NATS mTLS enabled on server", "ca", ca, "cert", cert)
	}
	return nil
}

// resolveMCPAuth decides the MCP session identity and enforces fail-closed
// behavior. When the provider is nil or auth is not enabled, it returns
// (nil, nil) — the caller runs unauthenticated (dev/embedded, no policy to
// enforce). When auth IS enabled, a valid credential is mandatory: an empty
// or unauthenticated token is a hard error, never a silent unauthenticated
// session. This is the guard that prevents MCP from bypassing ABAC.
func resolveMCPAuth(provider *auth.Provider, token string) (*auth.Identity, error) {
	if provider == nil || !provider.Enabled() {
		return nil, nil
	}
	if token == "" {
		return nil, fmt.Errorf("auth is enabled but no MCP credential provided: " +
			"pass --api-key or set WADJET_MCP_API_KEY (refusing to serve unauthenticated)")
	}
	id, err := provider.Authenticator().AuthenticateToken(token)
	if err != nil {
		return nil, fmt.Errorf("authenticating MCP credential: %w (refusing to serve)", err)
	}
	return id, nil
}

func mcpCmd() *cobra.Command {
	var mcpAPIKey string
	cmd := &cobra.Command{
		Use:   "mcp",
		Short: "Start MCP (Model Context Protocol) server on stdio for AI agent integration",
		Long: `Start a Model Context Protocol server that communicates over stdin/stdout.
This allows AI agents (Claude Desktop, Claude Code, Cursor, etc.) to discover
tables, inspect schemas, and execute SQL queries against Wadjet.

Transport is stdio only — there is deliberately no network listener.

Security: pass --config with an auth block to enforce ABAC (row filters,
column masks, table access) on MCP queries. When auth is configured you must
also supply a credential via --api-key (or the WADJET_MCP_API_KEY env var);
the resolved identity governs every query for the session. If auth is
configured but no valid credential is supplied, the server refuses to start
(fail closed) rather than serving unfiltered data.

Without --config (or with auth disabled), MCP runs unauthenticated against a
direct-to-store DB — appropriate only for local/dev use, where the operator
already holds the store credentials.

Configure in Claude Desktop's claude_desktop_config.json:

  {
    "mcpServers": {
      "wadjet": {
        "command": "wadjet",
        "args": ["mcp", "--config", "/etc/wadjet/config.yaml", "--api-key", "..."]
      }
    }
  }`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer cancel()

			logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

			// Resolve the auth provider and the operator's identity BEFORE
			// opening the DB, so a misconfigured secure deployment fails closed
			// without ever exposing a query surface.
			var provider *auth.Provider
			var identity *auth.Identity
			if configFile != "" {
				// The resolved config, not a second read of the file: the
				// root command already loaded it and refused an unparseable
				// one, and MCP must enforce the same auth block the rest of
				// the process runs on (#808).
				p, buildErr := buildProviderFromConfig(effectiveConfig(), logger)
				if buildErr != nil {
					return fmt.Errorf("loading config %q: %w", configFile, buildErr)
				}
				provider = p
			}

			token := mcpAPIKey
			if token == "" {
				token = os.Getenv("WADJET_MCP_API_KEY")
			}
			identity, err := resolveMCPAuth(provider, token)
			if err != nil {
				return err
			}
			if identity != nil {
				logger.Info("MCP auth enabled", "identity", identity.Name, "role", identity.Role, "method", identity.Method)
			} else {
				logger.Warn("MCP server running WITHOUT authentication — ABAC row/column security is not enforced; " +
					"use --config with an auth block for secured deployments")
			}

			store, err := newStore()
			if err != nil {
				return fmt.Errorf("initializing storage: %w", err)
			}

			db, err := wadjet.Open(ctx, wadjet.Config{
				Store:        store,
				Bucket:       bucket,
				Logger:       logger,
				AuthProvider: provider,
			})
			if err != nil {
				return fmt.Errorf("opening database: %w", err)
			}

			srv := mcp.NewServerWithIdentity(db, logger, identity)
			return srv.ServeStdio(ctx, os.Stdin, os.Stdout)
		},
	}
	cmd.Flags().StringVar(&mcpAPIKey, "api-key", "",
		"API key/bearer token establishing the MCP session identity (or set WADJET_MCP_API_KEY). Required when auth is configured.")
	return cmd
}

// parseS3URL splits "s3://bucket/path/..." into (bucket, path).
// path is empty or ends with "/".
func parseS3URL(s string) (bucket, path string, err error) {
	rest, ok := strings.CutPrefix(s, "s3://")
	if !ok {
		return "", "", fmt.Errorf("not an s3:// URL: %s", s)
	}
	slash := strings.Index(rest, "/")
	if slash < 0 {
		return rest, "", nil
	}
	bucket = rest[:slash]
	path = rest[slash+1:]
	if path != "" && !strings.HasSuffix(path, "/") {
		path += "/"
	}
	return bucket, path, nil
}

// deriveDataPlaneAddr builds host:port for the data plane from the
// NATS URL host and the data-plane listen port. natsAddr is something
// like "nats://10.0.1.2:4222" or "10.0.1.2:4222"; portFromFlag is the
// listen address used by coord (":9091" → port 9091).
// peerListenAddr resolves the worker's peer-exchange listen address:
// --peer-exchange-addr when --streaming-exchange is set, else "" (no peer
// server, no advertised address — the coordinator never hints at this
// worker).
func peerListenAddr() string {
	if !streamingExchange {
		return ""
	}
	return peerExchangeAddr
}

// morselWorkersConfig maps the --morsel-workers flag to worker.Config
// semantics: flag 0 (auto) becomes Config -1, because the Config zero value
// must stay serial/dormant for programmatic callers that never set it.
func morselWorkersConfig(flagVal int) int {
	if flagVal == 0 {
		return -1
	}
	return flagVal
}

func deriveDataPlaneAddr(natsAddr, portFromFlag string) string {
	host := natsAddr
	host = strings.TrimPrefix(host, "nats://")
	host = strings.TrimPrefix(host, "tls://")
	if i := strings.LastIndex(host, ":"); i >= 0 {
		host = host[:i]
	}
	port := strings.TrimPrefix(portFromFlag, ":")
	return host + ":" + port
}

// buildSHA returns the VCS revision short SHA from the build info, or
// "unknown" if unavailable.
func buildSHA() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, s := range info.Settings {
		if s.Key == "vcs.revision" {
			if len(s.Value) >= 7 {
				return s.Value[:7]
			}
			return s.Value
		}
	}
	return "unknown"
}
