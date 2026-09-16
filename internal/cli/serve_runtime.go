package cli

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/derekmwright/wadjet/internal/config"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/logio"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// The prologue every `serve` shares, whichever binary carries it.
//
// `wadjet serve` runs the embedded server and `wadjetd serve` runs the
// distributed modes, but a server process is a server process: the same log
// sink, the same memory envelope, the same object store with the same
// circuit breaker and base-table cache. The three functions here are that
// prologue, called in this order — logger, envelope, store — because the
// envelope resolves the flag values the store and every later consumer read.

// ServeLogger builds the server's logger and returns it with the function
// that closes its sink.
//
// In deploys stderr is a pipe into journald, and a stalled journald freezes
// the whole process through the slog handler mutex (frozen-spin/quiet-stall
// family; see internal/logio). WADJET_SYNC_LOG=1 restores direct writes for
// debugging.
func ServeLogger() (*slog.Logger, func()) {
	var logSink io.Writer = os.Stderr
	closeSink := func() {}
	if os.Getenv("WADJET_SYNC_LOG") != "1" {
		asyncSink := logio.NewAsyncWriter(os.Stderr, 8192)
		closeSink = func() { asyncSink.Close() }
		logSink = asyncSink
	}
	return slog.New(slog.NewTextHandler(logSink, &slog.HandlerOptions{Level: parseLogLevel(logLevel)})), closeSink
}

// ApplyServeRuntimeEnvelope sets the process memory envelope and the opt-in
// profiling samplers from the resolved flags, and RESOLVES the flag values
// that were left at their auto-detect defaults — the file cache, the
// per-task budget and max concurrency, the shared pool, the result store and
// the mmap relief ceiling. Call it before ServeOptionsNow: those are the
// values a server is actually run with.
func ApplyServeRuntimeEnvelope(logger *slog.Logger) {
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
		goMemLimit := memLimit * memoryEnvelopeNumerator / memoryEnvelopeDenominator
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
				// Default: 10% of the envelope for cross-query S3 file
				// cache (~7.5% of detected memory, since goMemLimit
				// itself is 75% of detected — see cacheBytesAutoDivisor).
				cacheBytes = goMemLimit / cacheBytesAutoDivisor
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
}

// OpenServeStore opens the object store the storage flags name and wraps it
// the way a server wants it: an operation-class circuit breaker, and the
// base-table NVMe cache above it when one is configured.
func OpenServeStore(logger *slog.Logger) (objstore.Store, error) {
	store, err := newStore()
	if err != nil {
		return nil, err
	}

	// Wrap store with circuit breaker for S3 resilience. The breaker is
	// scoped per operation class — a failing delete or upload burst must
	// never fast-fail a base-table read (ADR-0028). The thresholds are
	// resolved config keys: a `storage.circuit:` block in the YAML, a
	// WADJET_STORAGE_CIRCUIT_* variable and the flags all reach here, in
	// ADR-0029's order.
	circuit := EffectiveConfig().Storage.Circuit
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
				return nil, fmt.Errorf("initializing base-table cache: %w", err)
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
	return store, nil
}
