variable "profile" {
  description = "Benchmark profile name (e.g., sf100-distributed). Reads ../profiles/<profile>.yaml for benchmark config. Individual -var flags override profile values when explicitly set."
  type        = string
  default     = ""
}

variable "region" {
  description = "AWS region (null = use profile or default us-east-2)"
  type        = string
  default     = null
}

variable "scale_factor" {
  description = "TPC-H scale factor (1, 10, or 100). null = use profile or default 1."
  type        = number
  default     = null

  validation {
    condition     = var.scale_factor == null || contains([1, 10, 100], var.scale_factor)
    error_message = "scale_factor must be 1, 10, or 100"
  }
}

variable "mode" {
  description = "Benchmark mode: standalone or distributed. null = use profile or default standalone."
  type        = string
  default     = null

  validation {
    condition     = var.mode == null || contains(["standalone", "distributed"], var.mode)
    error_message = "mode must be standalone or distributed"
  }
}

variable "coordinator_instance_type" {
  description = "EC2 instance type for coordinator (distributed mode). null = use profile or default c7g.2xlarge."
  type        = string
  default     = null
}

variable "worker_instance_type" {
  description = "EC2 instance type for standalone or worker nodes. null = use profile or default c7g.2xlarge."
  type        = string
  default     = null
}

variable "worker_count" {
  description = "Number of workers (distributed mode only). null = use profile or default 3."
  type        = number
  default     = null
}

variable "go_version" {
  description = "Go version to install"
  type        = string
  default     = "1.24.1"
}

variable "memory_budget" {
  description = "Per-task memory budget in bytes (0 = unlimited)"
  type        = number
  default     = 0
}

variable "benchmark_runs" {
  description = "Number of benchmark runs per query. null = use profile or default 1."
  type        = number
  default     = null
}

variable "use_spot" {
  description = "Use spot instances. null = use profile or default true."
  type        = bool
  default     = null
}

variable "data_bucket" {
  description = "Existing S3 bucket with TPC-H data. null = use profile; empty string = create ephemeral bucket."
  type        = string
  default     = null
}

variable "bin_version" {
  description = "Pre-built binary version to pull from S3 (git short SHA or 'latest'). If empty, builds from source."
  type        = string
  default     = "latest"
}

variable "generate_data" {
  description = "Set to true to regenerate TPC-H data instead of using pre-seeded bucket. null = use profile."
  type        = bool
  default     = null
}

variable "run_duckdb_comparison" {
  description = "Run DuckDB TPC-H comparison after the Wadjet benchmark"
  type        = bool
  default     = false
}

variable "skip_queries" {
  description = "Comma-separated query numbers to skip (e.g. '2,17'). null = use profile."
  type        = string
  default     = null
}

variable "query_timeout" {
  description = "Per-query timeout (Go duration, e.g. '10m', '30m'). null = use profile or default 10m."
  type        = string
  default     = null
}

variable "max_concurrent" {
  description = "Maximum concurrent tasks per worker process (lower = less memory, slower execution). With workers_per_node > 1 the effective cluster concurrency is workers_per_node × max_concurrent × node_count."
  type        = number
  default     = 4
}

variable "morsel_workers" {
  description = "Worker --morsel-workers: intra-fragment parallel pipeline consumers per task (docs/design/morsel-execution.md). 0 = auto (matches the binary default since the 2026-07-08 flip), 1 = serial (kill switch; use to reproduce pre-flip baselines), N>1 = fixed width."
  type        = number
  default     = 0
}

variable "mmap_relief" {
  description = "Worker --mmap-relief: MADV_DONTNEED relief of cold mmap'd cache files when RSS exceeds the ceiling. Default true (matches the binary default; validated at SF100 + edge 2026-06-11). Set false for flags-off A/B baselines."
  type        = bool
  default     = true
}

variable "mmap_relief_threshold_mb" {
  description = "--mmap-relief-threshold-mb: TOTAL process RSS ceiling in MB; relieve cold mmap to bring RSS back to this level. Set below the worker memory.max (e.g. ~16000 on the ~20 GB SF100 c7gd per-proc envelope). Only used when mmap_relief = true."
  type        = number
  default     = 16000
}

variable "spill_floating_budget" {
  description = "Pass --spill-floating-budget to workers: activate the floating-budget spill threshold (Phase 3a). Default false = static 40%/90%. Requires mmap relief to be safe."
  type        = bool
  default     = false
}

variable "bounded_dirty_writes" {
  description = "Worker --bounded-dirty-writes: windowed sync_file_range writeback + FADV_DONTNEED on spill-class files (PR #113). Default true (matches the binary default). Set false for flags-off A/B baselines."
  type        = bool
  default     = true
}

variable "catalog_snapshot_prefix" {
  description = "S3 prefix (within the data bucket) for catalog snapshots, e.g. 'catalog/'. Non-empty: tpch-bench restores the post-discovery catalog instead of running discovery+ANALYZE (~15 min/deploy), writing a snapshot on the first boot. Empty = disabled. Delete s3://<bucket>/<prefix> after changing table data."
  type        = string
  default     = ""
}

variable "workers_per_node" {
  description = "Number of independent wadjet worker processes to launch per node. >1 gives crash isolation: when one process OOMs, sibling workers on the same node keep running. Each process gets a per-process memory envelope (auto-derived from /N of total) and registers separately with the coord."
  type        = number
  default     = 1
}

variable "cache_bytes" {
  description = "Worker LRU file cache size in bytes (0 = auto-detect based on memory and budget)"
  type        = number
  default     = 0
}

variable "data_prefix" {
  description = "S3 prefix for table data (e.g. 'tables/' or '' for root-level paths). null = use profile."
  type        = string
  default     = null
}

variable "benchmark_type" {
  description = "Benchmark suite to run: tpch or security. null = use profile or default tpch."
  type        = string
  default     = null

  validation {
    condition     = var.benchmark_type == null || contains(["tpch", "security"], var.benchmark_type)
    error_message = "benchmark_type must be tpch or security"
  }
}

variable "arch" {
  description = "CPU architecture: arm64 (Graviton) or x86_64 (Intel/AMD). null = use profile or default arm64."
  type        = string
  default     = null

  validation {
    condition     = var.arch == null || contains(["arm64", "x86_64"], var.arch)
    error_message = "arch must be arm64 or x86_64"
  }
}

variable "reverse_bloom_inner_threshold" {
  description = "Build-side row count above which inner-join reverse-bloom fires. 0 = use code default (50M). Set to a huge number (e.g. 999999999999) to disable the optimization for hunting bugs."
  type        = number
  default     = 0
}

variable "join_debug" {
  description = "Set to 1 to enable HashJoin diagnostic prints (DBG HashJoin.Build, DBG HashJoinProbe.Close)."
  type        = string
  default     = ""
}

variable "sort_merge_join_bytes" {
  description = "Sort-merge join gate (docs/design/sort-merge-join.md): inner equi-joins whose sides BOTH exceed this many estimated bytes run as sort-merge joins instead of hash joins. 0 = disabled (default, dormant)."
  type        = number
  default     = 0
}

variable "late_materialization" {
  description = "View-column join output (docs/design/late-materialization.md). Default on (empty/1 = on, matching the engine default); set \"0\" as the A/B kill switch to restore eager join-output gather."
  type        = string
  default     = ""
}

variable "skew_split" {
  description = "Adaptive skew-aware shuffle layout (docs/design/skew-aware-shuffle.md): hot partition groups split into sub-tasks that divide probe files and replicate build files. Default on (empty/1 = on, matching the engine default since 2026-07-11); set \"0\" as the A/B kill switch."
  type        = string
  default     = ""
}

variable "agg_over_exchange" {
  description = "Aggregate-over-exchange rewrite (rewireAggOverRawExchange): grouped finals consume a sibling raw exchange's partitions directly, dropping duplicate fused scan-agg legs (Q18). Default on (empty/1 = on, matching the engine default); set \"0\" as the A/B kill switch."
  type        = string
  default     = ""
}

variable "stage_fusion" {
  description = "1:1 stage-chain fusion (fuseStageChains, docs/design/stage-chain-fusion.md): same-distribution join chains run as one fragment, eliding the per-link materialization (Q18's join-class scratch). Default on (empty/1 = on, matching the engine default); set \"0\" as the A/B kill switch."
  type        = string
  default     = ""
}

variable "stage_fusion_agg" {
  description = "join→partial-aggregate absorb (fuseStageChains step 2) on top of join→join fusion. Default on (empty/1 = on); set \"0\" as the A/B sub-switch isolating step 2 from step 1 (stage_fusion=0 kills both)."
  type        = string
  default     = ""
}

variable "skew_suite" {
  description = "Set to 1 to run the hot-key skew fixture (benchmarks/skew) instead of TPC-H — the Phase 3 skew-split A/B. Pair with data_prefix=tables-skew/ and generate_data=true on the first arm to stage the fixture."
  type        = string
  default     = ""
}

variable "block_profile_rate" {
  description = "runtime.SetBlockProfileRate value in ns for coordinator + workers (profiling runs only). Empty/0 = sampler off."
  type        = string
  default     = ""
}

variable "mutex_profile_fraction" {
  description = "runtime.SetMutexProfileFraction 1-in-N value for coordinator + workers (profiling runs only). Empty/0 = sampler off."
  type        = string
  default     = ""
}

variable "dynamic_filters" {
  description = "Set to 1 to enable Trino-style semi-join dynamic-filter pushdown on the coordinator. Off by default for v1 rollout."
  type        = string
  default     = ""
}

variable "bushy_join_reorder" {
  description = "Set to 1 to let the CBO emit bushy join orders when strictly cheaper than left-deep (docs/design/bushy-join-cbo.md). Off by default."
  type        = string
  default     = ""
}

variable "use_native_dag" {
  description = "Route distributed queries through the Phase 3 native-DAG executor (feat/distribution-property-phase-3)."
  type        = bool
  default     = false
}

variable "data_plane" {
  description = "Worker↔coord data-plane transport. Empty or 'nats' uses the legacy NATS reply-subject path; 'grpc' enables the bidi gRPC stream (Phases C+D+E). See project_split_plane_design_2026-05-20."
  type        = string
  default     = ""
}

variable "streaming_exchange" {
  description = "Streaming exchange (docs/design/streaming-exchange.md): consumers fetch stage outputs from producing workers' local disk over gRPC (peer-exchange port below) with async S3 upload; every failure falls through to the S3 path. Wires the coordinator (TPCH_STREAMING_EXCHANGE) and worker --streaming-exchange flags together. Default true, matching the engine default since 2026-07-02 (this var defaulted false until 2026-07-12, silently benchmarking the synchronous S3 shuffle path; SF100 A/B: −27.7%). Set false as the kill switch."
  type        = bool
  default     = true
}

variable "eager_dispatch" {
  description = "Eager consumer dispatch (docs/design/eager-consumer-dispatch.md, Phase C1): eligible non-join consumer stages start before their producer stage fully drains, consuming per-producer-task manifests. Coordinator-side only (workers act on the task spec; no worker flag). Requires streaming_exchange. Default false pending SF100 validation — set true for the treatment arm of the C3 pair."
  type        = bool
  default     = false
}

variable "eager_min_tail_seconds" {
  description = "WADJET_EAGER_MIN_TAIL_SECONDS for the coordinator: projected-tail clearance floor for eager dispatch (eager-consumer-dispatch.md §11/§14). Default matches the in-binary default (12); treatment arms set ~3 because current-world producer spreads sit below the legacy calibration. Inert when eager_dispatch=false."
  type        = number
  default     = 12
}

variable "rowgroup_touch" {
  description = "WADJET_ROWGROUP_TOUCH for the workers: row-group touch-ahead — a per-mmap goroutine that physically pages-in advised column-chunk ranges behind MADV_WILLNEED, off the CPU-token budget (rowgroup_touch.go). Default \"1\" matches the in-binary default; \"0\" is the same-binary A/B off arm / kill switch. Inert when WADJET_ROWGROUP_READAHEAD=0 or scan_decode_ahead=false."
  type        = string
  default     = "1"
}

variable "df_late_group_attach" {
  description = "WADJET_DF_LATE_GROUP_ATTACH for the workers: full-layer delivery of attach-on-arrival dynamic filters — resolved deferred blooms/ranges reach the row-group iterator layer (pruning + prune-aware advises) and the shuffle-task path, not just row-level ops (attach-on-arrival-dynamic-filters.md §Full-layer delivery). Default \"1\" matches the in-binary default; \"0\" is the same-binary A/B off arm / kill switch."
  type        = string
  default     = "1"
}

variable "base_table_cache_bytes" {
  description = "Base-table NVMe cache LRU budget in bytes per worker process (docs/design/base-table-nvme-cache.md): cross-query disk cache for immutable base-table parquet under <spill-dir>/base-cache, so repeat scans skip the S3 GET. Worker-side flag only. null = take the profile's benchmark.base_table_cache_bytes (sf100-distributed sets 150 GB — SF100-validated 2026-08-09: suite -37.8% cold / -35.5% steady, cluster NIC rx -85%, rows identical), else 0 = disabled. Explicit -var (including 0) overrides the profile."
  type        = number
  default     = null
}

variable "streaming_shuffle_read" {
  description = "Streaming shuffle read (docs/design/exchange-streaming-consumption.md): workers decode WSHF/WSHC exchange inputs directly from the peer/S3 byte stream (D1) and prefetch peer-hinted shuffle inputs ahead of consumption (D2), instead of staging whole files before the first chunk decodes. Worker-side flag only. Default true (SF100-validated, 2026-07-14 pair) — set false for a control arm / kill switch."
  type        = bool
  default     = true
}

variable "refault_pressure_rate" {
  description = "WADJET_REFAULT_PRESSURE_RATE for workers: page-cache pressure sensor activation threshold in refaulted pages/sec (docs/design/scan-decode-pipelining.md §9). Empty = binary default (1000); 0 = sensor OFF (the §9.4 sensor-off A/B arm)."
  type        = string
  default     = ""
}

variable "scan_decode_ahead" {
  description = "Scan decode pipelining (docs/design/scan-decode-pipelining.md): workers decode parquet row groups ahead of scan consumption — k decode workers with in-order delivery, pool-ledger-charged byte window, refault-sensor collapse, cross-file continuation. Worker-side flag only. Default true (SF100 pair 2026-07-17: steady-state -7.3%, 0/44 mismatches); set false as the kill switch."
  type        = bool
  default     = true
}

variable "shuffle_durability" {
  description = "WADJET_SHUFFLE_DURABILITY for the coordinator (docs/design/shuffle-durability.md): stage-output upload policy stamped on dispatched tasks. eager = uploads start as outputs finalize (default, pre-knob behavior); lazy = uploads queue unstarted and run only on demand (consumer missing-input retry, coordinator read, worker drain) with the rest elided at query end; off = scratch never uploads. Coordinator-side only — workers act on Task.UploadPolicy."
  type        = string
  default     = "eager"
}

variable "peer_wire_compression" {
  description = "Worker --peer-wire-compression (docs/design/peer-wire-compression.md): s2-compress raw WSHF payloads on outgoing peer-exchange streams (WSHC envelope, consumers already decode it). Default true (SF100-validated 2026-08-09); false is the kill switch."
  type        = bool
  default     = true
}

variable "async_scratch_purge" {
  description = "Worker --async-scratch-purge (docs/design/async-scratch-purge.md): defer per-query stage-cache deletion to a paced background janitor instead of unlinking inline on the query-complete broadcast handler. Default true; false = inline-deletion control arm / kill switch."
  type        = bool
  default     = true
}

variable "locality_placement" {
  description = "WADJET_LOCALITY_PLACEMENT for the coordinator (docs/design/locality-placement.md): dispatch a task whose peer-location hints all point at one worker onto that worker, converting 1:1 stage-chain reads from peer gRPC streams into same-worker mmaps. Coordinator-side only. Default false pending SF100 validation."
  type        = bool
  default     = false
}

variable "peer_exchange_port" {
  description = "Worker↔worker peer-exchange (FetchShuffle) port; SG opens it intra-cluster whenever streaming_exchange is set."
  type        = number
  default     = 9095
}

variable "data_plane_port" {
  description = "TCP port for the gRPC data-plane listener on the coordinator. Workers dial coord on this port. Only used when data_plane=grpc."
  type        = number
  default     = 9091
}

# Recommended instance types per scale factor:
#   Graviton3 ARM (c7g):
#     SF1:   c7g.2xlarge  (8 vCPU, 16 GB) — $0.29/hr
#     SF10:  c7g.4xlarge  (16 vCPU, 32 GB) — $0.58/hr
#     SF100: c7g.8xlarge  (32 vCPU, 64 GB) — $1.15/hr
#   Intel Sapphire Rapids (c7i):
#     SF10:  c7i.4xlarge  (16 vCPU, 32 GB) — $0.71/hr
#     SF10 coordinator: c7i.2xlarge (8 vCPU, 16 GB) — $0.36/hr

variable "scan_output_prune" {
  description = "WADJET_SCAN_OUTPUT_PRUNE (coordinator-side): scan stages ship only consumer-declared columns instead of their full read set (pruneScanOutputColumns, 03b1a10 — Q13 leak fix). Default true; false = A/B kill-switch arm."
  type        = bool
  default     = true
}

variable "fuse_scan_shuffle" {
  description = "WADJET_FUSE_SCAN_SHUFFLE (coordinator-side): fuse dispatched scan→exchange-repartition pairs so scan tasks hash-partition their filtered output directly, deleting the unpartitioned WSHF write+read cycle (fuseScanShuffle, 765ce81 — ~20 GB duplicated per SF100 cold run on Q03/Q21/Q13-class legs). Default true; false = A/B kill-switch arm."
  type        = bool
  default     = true
}

variable "fuse_join_shuffle" {
  description = "WADJET_FUSE_JOIN_SHUFFLE (coordinator-side): fuse hash_join→exchange-repartition pairs so join fragments partition output directly (fuseJoinShuffle, c400307 — Q18 7.6GB/Q05 2.96GB dup legs). Default true; false = A/B kill-switch arm."
  type        = bool
  default     = true
}

variable "refault_episode_cap_seconds" {
  description = "WADJET_REFAULT_EPISODE_CAP (worker-side): seconds one page-cache pressure episode may throttle decode-ahead before being declared non-causal (executor_fragment.go). In-binary default 10; the window-variance memo's tuning candidate is ~3 (causal relief measured ~2s in #260). 0 = unbounded v2 semantics."
  type        = number
  default     = 10
}

variable "gogc" {
  description = "WADJET_GOGC for coordinator + workers: 'off' (GOMEMLIMIT-only), an integer percent, or empty for the profile default of 100. Steady-residual GC-discipline A/B knob."
  type        = string
  default     = "100"
}

variable "task_gc" {
  description = "WADJET_TASK_GC: set '0' to disable the per-task forced runtime.GC() after join/aggregate/sort/window tasks (worker.go). Default on (empty). Steady-residual GC-discipline A/B knob."
  type        = string
  default     = ""
}

