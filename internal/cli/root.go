// SPDX-License-Identifier: MIT

package cli

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/peterh/liner"

	"github.com/derekmwright/wadjet/internal/embedding"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/config"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/format"
	"github.com/derekmwright/wadjet/internal/geoip"
	"github.com/derekmwright/wadjet/internal/natsconn"
	"github.com/derekmwright/wadjet/internal/server/mcp"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/compaction"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/telemetry"
	"github.com/derekmwright/wadjet/wadjet"
	"github.com/spf13/cobra"
)

// memoryEnvelopeNumerator/memoryEnvelopeDenominator and cacheBytesAutoDivisor
// together define the --cache-bytes auto-detect default computed in
// PersistentPreRunE: goMemLimit = detected memory *
// memoryEnvelopeNumerator / memoryEnvelopeDenominator, and (when
// --memory-budget is left at its 0 default) cacheBytes = goMemLimit /
// cacheBytesAutoDivisor. Named — rather than left as the literals `3`,
// `4`, `10` at their call sites — so the --cache-bytes flag's help string
// and TestCacheBytesHelpStringMatchesComputation cannot drift out of sync
// with the computation again the way the help string did: it said the
// auto-detect share was 20%, then (after fixing that) a still-wrong "10%
// of memory" with no envelope/detected distinction, before landing here.
const (
	memoryEnvelopeNumerator   = 3
	memoryEnvelopeDenominator = 4
	cacheBytesAutoDivisor     = 10
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

// NewRootCmd builds the root command with every persistent flag registered
// and the configuration loader installed.
//
// serve is the `serve` subcommand: EmbeddedServeCmd in the wadjet binary,
// clid.ServeCmd — the distributed run modes — in wadjetd. Everything else in
// the tree is the same in both, and so is every persistent flag: a flag is
// registered here whether or not the serve command this binary carries reads
// it, so the configuration precedence a deployment relies on does not change
// with the binary.
//
// It is a function (rather than inline in main) so the precedence census can
// drive the REAL command with the REAL flag registrations through the REAL
// PersistentPreRunE — a census that ran against a model of the loader could
// pass while the binary disagreed with it. Building it also RESETS every
// bound package variable to its registered default, which is what makes one
// census cell independent of the last.
func NewRootCmd(serve *cobra.Command) *cobra.Command {
	rootCmd := &cobra.Command{
		// The wadjetd binary overrides these three: one tree, two programs
		// (LICENSING.md), and usage text that names the wrong one sends a
		// user to the wrong binary.
		Use:   "wadjet",
		Short: "Wadjet — analytical query engine, embedded or distributed",
		Long: "An analytical query engine over Parquet on object storage, speaking the PostgreSQL " +
			"wire protocol.\n\n" +
			"`wadjet serve` runs it in one process; `wadjetd serve --mode=...` runs it as a " +
			"coordinator and workers.",
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
			return resolveConfiguration(cmd)
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
	rootCmd.PersistentFlags().Int64Var(&cacheBytes, "cache-bytes", 0, "LRU file cache size in bytes (0 = auto-detect: 10% of the Go memory limit, ~7.5% of detected memory)")

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
	rootCmd.PersistentFlags().BoolVar(&bushyJoinReorder, "bushy-join-reorder", false, "Let the cost-based join reorder emit BUSHY plans (joins of two composite intermediates — e.g. pre-joining a snowflake dimension chain before it meets the fact stream) when strictly cheaper than every left-deep order. Cost ties keep the left-deep shape. Default false. See docs/design/bushy-join-cbo.md.")
	rootCmd.PersistentFlags().Int64Var(&localFastPathBytes, "local-fastpath-bytes", config.DefaultLocalFastPathBytes, "Queries whose post-pruning catalog scan bytes stay under this threshold execute in-process on the coordinator (skipping the distributed stage DAG and its per-stage object-store round trips). 0 = disabled.")
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

	rootCmd.AddCommand(serve)
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
			if err := LoadGeoIP(logger); err != nil {
				return fmt.Errorf("loading GeoIP: %w", err)
			}
			defer geoip.Close()

			// A statement whose every source is a catalog-free table
			// function (read_json / read_csv / read_parquet over a local
			// path or URL) needs no object store at all — and no catalog
			// either. Building one against the default endpoint is what
			// made the first command a new user runs fail inside catalog
			// init, before the SQL was ever parsed (#303). Anything else
			// opens the SHARED catalog (#842), so a table `create-table`
			// or `serve` wrote is a table this query can read.
			if isCatalogFreeQuery(args[0]) {
				db, err := wadjet.Open(ctx, wadjet.Config{
					Store: objstore.NewMemStore(), Bucket: bucket,
					// Every planner/engine flag `serve` carries, so a
					// typed --memory-budget or --late-materialization
					// means here what it means there (#1223, #1226).
					SortMergeJoinBytes:  sortMergeJoinBytes,
					LateMaterialization: lateMaterialization,
					BushyJoinReorder:    bushyJoinReorder,
					MemoryBudget:        memoryBudget,
					SpillDir:            spillDir,
				})
				if err != nil {
					return err
				}
				defer db.Close()
				result, err := db.Query(ctx, args[0])
				if err != nil {
					return err
				}
				return format.WriteDeclared(os.Stdout, f, result.Columns, columnDecls(result), resultRows(result))
			}

			db, release, err := openSharedDB(ctx, logger)
			if err != nil {
				return err
			}
			defer release()

			result, err := db.Query(ctx, args[0])
			if err != nil {
				return err
			}

			return format.WriteDeclared(os.Stdout, f, result.Columns, columnDecls(result), resultRows(result))
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
			logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

			store, err := newStore()
			if err != nil {
				return err
			}

			// The SHARED catalog, Init'd. Both halves are #842: this built a
			// catalog.NewWithStore — an in-memory KV despite the name — and
			// never called Init, so `wadjet tables` answered "reading catalog
			// meta: key not found" on every invocation, including as the
			// verification step of the disaster-recovery runbook.
			cat, release, err := sharedCatalog(ctx, store, logger)
			if err != nil {
				return err
			}
			defer release()

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
			logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

			// The SHARED catalog, so the table survives this process (#842).
			db, release, err := openSharedDB(ctx, logger)
			if err != nil {
				return err
			}
			defer release()

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
			logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

			// The SHARED catalog: dropping a table out of a private in-memory
			// one dropped nothing anybody else could see (#842).
			db, release, err := openSharedDB(ctx, logger)
			if err != nil {
				return err
			}
			defer release()

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
			nc, err := natsconn.Connect(natsAddr, nil)
			if err != nil {
				return fmt.Errorf("connecting to NATS at %s: %w", natsAddr, err)
			}
			defer nc.Close()

			js, err := natsconn.NewJetStream(nc)
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
			printCompactResult(os.Stdout, os.Stderr, result)
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

// printCompactResult reports one compaction run. Every stdout line comes from
// Result.Summary, so a counter the Result reports cannot be dropped here by
// omission — which is how PublicationConflicts went unprinted when it was
// added. Failures go to stderr, one per partition.
func printCompactResult(out, errOut io.Writer, result *compaction.Result) {
	if result == nil {
		return
	}
	for _, line := range result.Summary() {
		fmt.Fprintln(out, line)
	}
	for _, f := range result.Failed {
		fmt.Fprintf(errOut, "partition %s FAILED: %v\n", f.Partition, f.Err)
	}
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

			nc, err := natsconn.Connect(natsAddr, nil)
			if err != nil {
				return fmt.Errorf("connecting to NATS: %w", err)
			}
			defer nc.Close()

			js, err := natsconn.NewJetStream(nc)
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
			if err := LoadGeoIP(logger); err != nil {
				return fmt.Errorf("loading GeoIP: %w", err)
			}
			defer geoip.Close()

			// The SHARED catalog: a session's CREATE TABLE used to live only
			// as long as the session did (#842).
			db, release, err := openSharedDB(ctx, logger)
			if err != nil {
				return err
			}
			defer release()

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

// resultRows returns the result POSITIONALLY — one slice per row, cells
// aligned with result.Columns — which is the only form a renderer can print.
//
// QueryResult.Rows is keyed by column NAME and a result may legally carry two
// output columns of one name: `SELECT abs(a), abs(b)` is two columns called
// `abs`, and a star over a `JOIN … USING` whose arms share a tail name
// publishes that name twice. The map holds the LAST of them, so rendering
// from it printed one value under both headings in the table and CSV forms
// and emitted a single key — dropping the other column — in JSON, while psql
// against the same server showed both (#1218). Cells reads the positional
// form the engine materialises for exactly this case and falls back to the
// map lookup when the names are unique, so an ordinary result renders
// byte-identically to before.
//
// The row count is the longer of the two forms, the way wadjet's own CTAS
// door reads it: neither is authoritative on its own once one of them is the
// populated one.
func resultRows(result *wadjet.QueryResult) [][]any {
	if result == nil {
		return nil
	}
	n := len(result.Rows)
	if len(result.RowValues) > n {
		n = len(result.RowValues)
	}
	rows := make([][]any, n)
	for i := range rows {
		cells := make([]any, len(result.Columns))
		copy(cells, result.Cells(i))
		rows[i] = cells
	}
	return rows
}

// columnDecls returns the declared column of each result column,
// positionally aligned with result.Columns, or nil when the query carried no
// typed metadata (introspection answers). The formatter needs the TYPE to
// render a TIMESTAMP column, which the engine boxes as epoch milliseconds,
// and a container's whole shape — its element, its fields — to render it as
// PostgreSQL does (arc CW).
func columnDecls(result *wadjet.QueryResult) []parquet.Column {
	if result == nil || len(result.ColumnMetas) == 0 {
		return nil
	}
	decls := make([]parquet.Column, len(result.ColumnMetas))
	for i, m := range result.ColumnMetas {
		decls[i] = parquet.Column{Name: m.Name, Type: m.TypeID, Precision: m.Precision, Scale: m.Scale,
			Fields: m.Fields, ElementType: m.ElementType}
	}
	return decls
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

		rows := resultRows(result)
		if len(rows) == 0 {
			fmt.Println("(0 rows)")
			continue
		}

		format.WriteDeclared(os.Stdout, f, result.Columns, columnDecls(result), rows)
	}

	return nil
}

// newStore builds the object store from the RESOLVED storage configuration.
// Before #808's fix it read the flag variables, so the whole `storage:`
// section of the config file was parsed, validated and reported by the admin
// endpoint without ever reaching a connection.
func newStore() (objstore.Store, error) {
	st := EffectiveConfig().Storage
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

// NATSServerConfig builds the embedded NATS server configuration from the
// RESOLVED `nats:` section. runStandalone and runCoordinator share it, which
// is also the seam the census asserts the file and environment tiers reach.
func NATSServerConfig() natsconn.NATSConfig {
	n := EffectiveConfig().NATS
	cfg := natsconn.DefaultNATSConfig()
	cfg.Port = n.Port
	cfg.ClusterID = n.ClusterID
	cfg.LeafRemotes = n.LeafRemotes
	if n.StoreDir != "" {
		cfg.StoreDir = n.StoreDir
	}
	if dir := derivedCatalogStoreDir(); dir != "" {
		cfg.StoreDir = dir
	}
	return cfg
}

// derivedCatalogStoreDir is the catalog store directory a LOCAL-FILE
// deployment gets when nobody named one, or "" when the default stands.
//
// The catalog and the data have to be one thing. `~/.wadjet/nats` does not
// vary with `--data-dir`, so two data directories shared one catalog: a table
// created against A was listed against B, `SELECT COUNT(*)` answered from A's
// metadata while the files were not in B's store, and `SELECT *` returned half
// the rows the same invocation's COUNT reported — the scan drops a file it
// cannot read unless every file fails. Before the CLI reached this catalog at
// all, B answered `relation "ta" does not exist`, loudly. That is loud →
// silent-wrong on the flow docs/getting-started.md documents, and it is
// round-1 B3.
//
// So a file-backed deployment keeps its catalog UNDER its data directory, one
// per `--data-dir`, and the two cannot drift apart. `_catalog` rather than
// `catalog`: an object-store bucket cannot begin with an underscore (S3 names
// must start alphanumeric), so this can never collide with `--bucket`.
//
// It applies only when nobody said otherwise: `--nats-store-dir` at any tier
// wins (Source is not the default), and the S3 flow keeps `~/.wadjet/nats`,
// where a shared machine-wide catalog beside a shared bucket is the right
// default. Both `serve` and the CLI commands resolve through here, so the two
// still meet — a lock only one side took would be no lock (#842).
func derivedCatalogStoreDir() string {
	res := effectiveResolution()
	if res.Source("nats.store_dir") != config.SourceDefault {
		return ""
	}
	cfg := res.Config()
	if cfg.Storage.Type != "file" || cfg.Storage.DataDir == "" {
		return ""
	}
	return filepath.Join(cfg.Storage.DataDir, "_catalog")
}

// WireUDFPersistence loads persisted UDFs from the catalog and sets up
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

// ConfigureEmbeddingProvider wires the embed() SQL function to an embedding
// provider selected by WADJET_EMBED_PROVIDER: "openai" (default), "voyage", or
// "ollama". Each provider reads its own key/model env vars. If the selected
// provider has no credentials, embed() is left unregistered (returns NULL).
//
// Note: Anthropic has no native embeddings endpoint — "voyage" is Anthropic's
// officially recommended embeddings path.
func ConfigureEmbeddingProvider(logger *slog.Logger) {
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

func WireUDFPersistence(cat *catalog.Catalog, logger *slog.Logger) {
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

// WireAuthFromConfig loads the YAML config file and builds the hot-reloadable
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
func WireAuthFromConfig(ctx context.Context, configFile string, logger *slog.Logger) (*config.Config, *config.Manager, *auth.Provider, error) {
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
		abacPolicies, err := buildABACPolicies(event.New.Auth.ABACPolicies)
		if err != nil {
			logger.Error("auth hot-reload REFUSED — keeping the previous configuration",
				"path", configFile, "error", err)
			return
		}
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
	authn, authz, err := buildAuth(cfg.Auth)
	if err != nil {
		// Refuse to START. An operator who asked for authentication and
		// misconfigured it must not get a server that serves everyone.
		return nil, fmt.Errorf("auth configuration: %w", err)
	}
	var policies *auth.PolicySet
	if len(cfg.Auth.Policies) > 0 {
		policies, err = buildPolicies(cfg.Auth.Policies)
		if err != nil {
			return nil, fmt.Errorf("auth policies: %w", err)
		}
	}
	provider := auth.NewProvider(authn, authz, policies, logger)
	if len(cfg.Auth.ABACPolicies) > 0 {
		abac, err := buildABACPolicies(cfg.Auth.ABACPolicies)
		if err != nil {
			return nil, fmt.Errorf("auth policies: %w", err)
		}
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

// buildAuth constructs the authenticator and authorizer from the resolved
// auth block, REPORTING a configuration it cannot honour. It used to swallow
// that error, so `jwt: enabled: true` with an unreadable key started the
// server with authentication OFF on every frontend (#931).
func buildAuth(cfg config.Auth) (*auth.Authenticator, *auth.Authorizer, error) {
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
	return auth.Build(authCfg)
}

func buildPolicies(cfgs []config.AuthPolicy) (*auth.PolicySet, error) {
	return auth.ParsePolicies(buildPolicyConfigs(cfgs))
}

// buildABACPolicies converts config ABAC policies to auth ABAC policies.
//
// A condition's ATTRIBUTE is stored exactly as the operator wrote it. The
// evaluator's attribute map is keyed by the namespaced spelling — `subject.`,
// `resource.`, `env.` (auth.PolicyEvaluator.buildAttrMap) — and that spelling
// is the only one there has ever been. This function used to slice the
// namespace OFF (`c.Attribute[8:]`), so every documented condition looked up
// an attribute that does not exist and no condition ever matched. A
// conditional ALLOW then failed closed (it simply never granted), but a
// conditional DENY beside a broad allow — which is what every `roles:`-to-ABAC
// migration emits — failed OPEN: the deny disappeared and the grant won
// (#930). The Subjects / Resources / Environment slice a condition lands in is
// a grouping for readability; it never restored the prefix and was never
// consulted for it.
//
// An UNPREFIXED attribute is an error at load rather than a subject attribute
// "by default": `attribute: hour` reads a subject attribute called `hour` that
// nothing populates, so the rule silently never matches — and a security rule
// that silently never matches is the defect this whole function had. Refusing
// tells the operator which line to fix.
func buildABACPolicies(cfgs []config.ABACPolicy) ([]auth.AccessControlPolicy, error) {
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
			// Group the conditions by namespace, keeping the attribute the
			// operator wrote — that IS the evaluator's key.
			var subjects, resources, envConds []auth.Condition
			for _, c := range r.Conditions {
				cond := auth.Condition{Attribute: c.Attribute, Op: c.Operator, Value: c.Value}
				switch {
				case strings.HasPrefix(c.Attribute, "subject."):
					subjects = append(subjects, cond)
				case strings.HasPrefix(c.Attribute, "resource."):
					resources = append(resources, cond)
				case strings.HasPrefix(c.Attribute, "env."):
					envConds = append(envConds, cond)
				default:
					return nil, fmt.Errorf("auth policy %q rule %d: condition attribute %q "+
						"has no namespace; write subject.<name>, resource.<name> or env.<name> "+
						"(an unnamespaced attribute matches nothing, and a rule that matches "+
						"nothing is a grant beside a broad allow)",
						p.Name, j+1, c.Attribute)
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
	return policies, nil
}

// LoadGeoIP loads the MaxMind GeoIP databases named by the RESOLVED
// configuration. The flag/file tie-break it used to do by hand is the
// loader's job now (ADR-0029), which is also how the environment tier
// (WADJET_GEOIP_CITY_DB / _ASN_DB) starts reaching it.
func LoadGeoIP(logger *slog.Logger) error {
	g := EffectiveConfig().GeoIP
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

func BuildTLSConfig(cfg config.AuthMTLS) (*tls.Config, error) {
	clientCA, err := auth.LoadClientCA(cfg.CAFile)
	if err != nil {
		return nil, err
	}
	return auth.NewTLSConfig(cfg.CertFile, cfg.KeyFile, clientCA)
}

// ResolveNATSTLSPaths returns the NATS TLS cert/key/CA paths: CLI flag
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
func ResolveNATSTLSPaths(cfg *config.Config) (cert, key, ca string, err error) {
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

// InitTelemetry creates an OTel TracerProvider if an OTLP endpoint is configured.
// Returns nil if no endpoint is set (tracing disabled).
func InitTelemetry(ctx context.Context, logger *slog.Logger) *telemetry.Provider {
	// The three tiers used to be walked by hand here, in the OPPOSITE
	// convention to the rest of the process: a flag won only when non-empty,
	// the environment was consulted second, and the config file's
	// `telemetry:` section was never consulted at all. They are ordinary
	// resolved keys now (ADR-0029, #808).
	t := EffectiveConfig().Telemetry
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

// LoadConfigForNATSTLS reads the config file for the NATS TLS tier and
// PROPAGATES a parse failure.
//
// Dropping that error is how the tier's own stated invariant gets
// falsified: an unparseable file that NAMES tls_cert, tls_key and tls_ca
// yields a nil config, ResolveNATSTLSPaths then sees three empty strings —
// which is the legitimate "no TLS configured" shape — and the process
// connects to NATS in PLAINTEXT with no error and no warning. #802 settled
// exactly this doctrine for the auth block ("an unreadable config file
// silently started a server with NO authentication at all — that now stops
// the process with the reason"), and it applies on EVERY mode, not only the
// ones that happen to load the file again later for another reason: worker
// mode has no WireAuthFromConfig and would have run to completion.
//
// The root command's loader now refuses the same file earlier and for every
// command (resolveConfiguration), so in a real process this read is a
// second look at a file already known to parse. It stays because it is the
// security control's OWN guarantee: the tier does not depend on some other
// caller having checked first, and its gates hold it to that.
func LoadConfigForNATSTLS() (*config.Config, error) {
	if configFile == "" {
		return nil, nil
	}
	cfg, err := config.Load(configFile)
	if err != nil {
		return nil, fmt.Errorf("loading config file %q: %w", configFile, err)
	}
	return cfg, nil
}

// ApplyNATSTLS sets TLS fields on a NATSConfig from the flag / env / config
// tiers. Used by runCoordinator to configure mTLS on the embedded NATS
// server. It returns an error when the material is partially specified,
// which would otherwise start a plaintext server (#827).
func ApplyNATSTLS(cfg *natsconn.NATSConfig, fileCfg *config.Config, logger *slog.Logger) error {
	cert, key, ca, err := ResolveNATSTLSPaths(fileCfg)
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

Without --config (or with auth disabled), MCP runs unauthenticated —
appropriate only for local/dev use, where the operator already holds the store
credentials.

The catalog is the SHARED one every other CLI command uses, so a table
create-table, shell or a running serve wrote is a table an agent can query.

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
				p, buildErr := buildProviderFromConfig(EffectiveConfig(), logger)
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

			// The SHARED catalog, the way `query`, `shell`, `create-table`
			// and `drop-table` open it (#842). Opening with no MetaKV gave
			// this command a private in-memory catalog that started EMPTY on
			// every invocation, so an agent saw no table `create-table` or
			// `serve` had written while their parquet files sat in the store
			// — and, since a policy set binds to the catalog it is attached
			// to (#882), a config carrying any policy could not start here at
			// all: the catalog it bound against held nothing.
			db, release, err := openSharedDB(ctx, logger)
			if err != nil {
				return err
			}
			defer release()
			// Attach AFTER the catalog is open, which is the order the rule
			// makes mandatory and docs/security.md states: the policy's names
			// are resolved against the relations the catalog holds.
			if err := db.SetAuthProvider(provider); err != nil {
				return fmt.Errorf("attaching the auth policy set: %w", err)
			}

			srv := mcp.NewServerWithIdentity(db, logger, identity)
			return srv.ServeStdio(ctx, os.Stdin, os.Stdout)
		},
	}
	cmd.Flags().StringVar(&mcpAPIKey, "api-key", "",
		"API key/bearer token establishing the MCP session identity (or set WADJET_MCP_API_KEY). Required when auth is configured.")
	return cmd
}

// ParseS3URL splits "s3://bucket/path/..." into (bucket, path).
// path is empty or ends with "/".
func ParseS3URL(s string) (bucket, path string, err error) {
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
