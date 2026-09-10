// This file holds catalog scan for the physical planner, governed by ADR-0026 and ADR-0027.
package physical

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func (p *Planner) newScanner(ctx context.Context, tableName string, partFilter map[string]string, requiredCols []string, scanPreds []logical.Predicate) exec.Source {
	// Get table schema
	tableMeta, err := p.catalog.GetTable(ctx, tableName)
	if err != nil {
		return &exec.SliceSource{}
	}
	_ = tableMeta

	// Create a scanner source that reads from the catalog
	src := &catalogScanSource{
		catalog:          p.catalog,
		tableName:        tableName,
		partitionFilter:  partFilter,
		requiredCols:     requiredCols,
		scanPreds:        scanPreds,
		manifestSnapshot: p.ManifestSnapshot,
	}
	// Attach scan cache if this table is scanned multiple times in this query.
	if p.scanCache != nil {
		if cached, ok := p.scanCache[tableName]; ok {
			src.cache = cached
		}
	}
	// Wire per-query memory tracker so parquet pooled buffers are accounted for.
	if sm := p.getSpillManager(); sm != nil {
		src.memTracker = sm.Tracker()
		src.spillMgr = sm
	}
	return src
}

// catalogScanSource adapts the scan.Scanner to exec.Source.
//
// Note: Pipeline.runParallel calls Source.Next() concurrently from multiple
// worker goroutines on a single source instance, so replayIdx must be atomic.
// Previously it was a plain int and the race detector caught it producing
// non-deterministic Q02 row counts (4/5/6 rows depending on which goroutine
// won the increment).
type catalogScanSource struct {
	catalog         *catalog.Catalog
	tableName       string
	partitionFilter map[string]string
	requiredCols    []string
	scanPreds       []logical.Predicate
	allowedFiles    []string // probe-split: only scan these files (nil = all)
	inner           exec.Source
	cache           *scanCached      // non-nil when this table is scanned multiple times
	replayIdx       atomic.Int64     // position in cache replay (atomic for parallel pipeline)
	projOnce        sync.Once        // guards projIdx/projSchema init (replay Next is concurrent)
	projIdx         []int            // cache-batch column indices for this consumer; nil = no projection
	projSchema      []parquet.Column // this consumer's projected schema
	isReplay        bool             // true when reading from cache instead of scanning; written once in Init before runParallel starts, so no synchronization needed
	// claimedCache is true when THIS source created cache.ready, i.e. it owes
	// every other consumer a release. Written once in Init, before any worker
	// goroutine exists, for the same reason isReplay is.
	claimedCache     bool
	bloomFilter      *exec.BloomScanFilter // bloom filter pushdown from hash join build side
	dynamicFilter    []exec.DynamicRange   // dynamic min/max range filter from hash join build side
	rowLimit         int64                 // LIMIT pushdown: enables lazy file downloading (0 = eager)
	memTracker       *memory.Tracker       // per-query memory tracker; wired at construction when budget>0
	spillMgr         *memory.SpillManager  // for pre-emptive relief on file-load reservations; nil-safe
	emitRowLoc       bool                  // top-N late materialization: stamp __row_loc on scan batches
	rowPreds         []scan.RowPred        // scan-level filter conjuncts (scan_filter_pushdown.go)
	shapeOnlyCols    map[string]bool       // byte-array columns decoded as lengths only (logical/shape_only_columns.go)
	manifestSnapshot *ManifestSnapshot     // pins this table's manifest to one read per statement (#502); nil-safe
}

// RefetchRows re-reads the full-width rows named by __row_loc values (see
// topn_late_mat.go), in locs order. Only valid on an emitRowLoc scan whose
// narrow phase has completed and whose source has not been closed.
func (s *catalogScanSource) RefetchRows(ctx context.Context, locs []int64) (*batch.RecordBatch, error) {
	ses, ok := s.inner.(*scannerExecSource)
	if !ok || ses.scanner == nil {
		return nil, fmt.Errorf("refetch: scan source is not a row-loc scan")
	}
	return ses.scanner.RefetchRows(ctx, locs)
}

// SetBloomFilter attaches a bloom filter for scan-level row group pruning.
func (s *catalogScanSource) SetBloomFilter(bf *exec.BloomScanFilter) {
	s.bloomFilter = bf
}

// SetDynamicFilter attaches a dynamic min/max range filter for row group pruning.
func (s *catalogScanSource) SetDynamicFilter(ranges []exec.DynamicRange) {
	s.dynamicFilter = ranges
}

func (s *catalogScanSource) Init(ctx context.Context) error {
	if s.cache != nil {
		s.cache.mu.Lock()
		if s.cache.done {
			s.cache.mu.Unlock()
			// Scan already complete — replay from cache.
			s.isReplay = true
			s.replayIdx.Store(0)
			return nil
		}
		if s.cache.ready != nil {
			// Another goroutine is populating the cache. Wait for it.
			s.cache.mu.Unlock()
			select {
			case <-s.cache.ready:
			case <-ctx.Done():
				return ctx.Err()
			}
			// The claim is released on EVERY exit, not only on success, so
			// waking up says the claiming scan is FINISHED — not that it
			// filled the cache. Replaying an abandoned cache would answer
			// from a truncated table, so this fails loudly with the reason
			// the claiming scan stopped.
			s.cache.mu.Lock()
			if !s.cache.done {
				err := s.cache.err
				s.cache.mu.Unlock()
				if err == nil {
					err = errors.New("scan ended before the table did")
				}
				return &abandonedClaimError{table: s.tableName, cause: err}
			}
			s.cache.mu.Unlock()
			s.isReplay = true
			s.replayIdx.Store(0)
			return nil
		}
		// First scan claims the cache. Whoever claims it OWES every other
		// consumer a release — see abandonCache.
		s.cache.ready = make(chan struct{})
		s.claimedCache = true
		s.cache.mu.Unlock()
	}
	// First scan (or no cache) — scan from storage. A cache-populating
	// scan reads the UNION of all consumers' columns so the cache can
	// serve every consumer; each consumer (this one included) projects
	// back down to its own columns in Next.
	scanCols := s.requiredCols
	if s.cache != nil {
		scanCols = s.cache.unionCols
	}
	sc := newScannerSource(s.catalog, s.tableName, s.partitionFilter, scanCols, s.scanPreds, s.manifestSnapshot)
	if ses, ok := sc.(*scannerExecSource); ok {
		if s.bloomFilter != nil {
			ses.bloomFilter = s.bloomFilter
		}
		if s.dynamicFilter != nil {
			ses.dynamicFilter = s.dynamicFilter
		}
		if s.allowedFiles != nil {
			ses.allowedFiles = s.allowedFiles
		}
		if s.rowLimit > 0 {
			ses.rowLimit = s.rowLimit
		}
		if s.memTracker != nil {
			ses.memTracker = s.memTracker
			ses.spillMgr = s.spillMgr
		}
		ses.emitRowLoc = s.emitRowLoc
		ses.rowPreds = s.rowPreds
		ses.shapeOnlyCols = s.shapeOnlyCols
	}
	s.inner = sc
	if err := s.inner.Init(ctx); err != nil {
		s.abandonCache(err)
		return err
	}
	return nil
}

// abandonedClaimError is what a waiter gets when the scan that claimed the
// shared cache finished without filling it.
//
// It is never a ROOT CAUSE. The claiming scan stopped because something else
// went wrong — its own read failed, or the query was already being torn down —
// so this error is always downstream of the reason the client actually needs.
// It carries that reason where the claiming scan knew it (Unwrap), and callers
// that hold BOTH this and the real failure prefer the real one; see the probe
// side of buildJoin.
type abandonedClaimError struct {
	table string
	cause error
}

func (e *abandonedClaimError) Error() string {
	return fmt.Sprintf("shared scan of %s did not complete: %v", e.table, e.cause)
}

func (e *abandonedClaimError) Unwrap() error { return e.cause }

// isAbandonedClaim reports whether err is (or wraps) a waiter's abandoned-claim
// failure — that is, whether it is a CONSEQUENCE of some other failure rather
// than a reason of its own.
func isAbandonedClaim(err error) bool {
	var a *abandonedClaimError
	return errors.As(err, &a)
}

// abandonCache releases a claim this source took but will not fill, so the
// consumers waiting on it fail with err instead of blocking forever.
//
// The claim used to be released in exactly one place — Next's end-of-table
// branch — which made every other exit a permanent block: a scan that FAILED,
// or was closed before the end of the table, left `ready` open and every other
// reader of that table's cache waited on a channel nobody would ever close.
// That is the deadlock #616 reports and it is reachable from any plan with two
// readers of one table where the first one fails (measured: two LATERALs over
// the same table, the second carrying a residual the scan cannot compile —
// 6 ms to the error on the single-process arm, an unbounded block on both DAG
// arms).
//
// Releasing is not enough on its own: a waiter that wakes on an abandoned
// claim must NOT replay, because the cache holds only what was read before the
// scan stopped. Init returns the error instead.
func (s *catalogScanSource) abandonCache(err error) {
	if s.cache == nil || !s.claimedCache {
		return
	}
	s.cache.mu.Lock()
	defer s.cache.mu.Unlock()
	s.abandonCacheLocked(err)
}

// abandonCacheLocked is abandonCache for callers already holding cache.mu.
func (s *catalogScanSource) abandonCacheLocked(err error) {
	if s.cache.done || s.cache.abandoned {
		return
	}
	s.cache.abandoned = true
	s.cache.err = err
	if s.cache.ready != nil {
		close(s.cache.ready)
	}
}

func (s *catalogScanSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	if s.isReplay {
		// cache.batches is stable (read-only) after cache.done=true.
		// replayIdx is atomic because Pipeline.runParallel may call Next()
		// from multiple worker goroutines on this same source instance.
		idx := s.replayIdx.Add(1) - 1
		if idx >= int64(len(s.cache.batches)) {
			return nil, nil
		}
		cached := s.cache.batches[idx]
		// Return a shallow copy: shared column vectors (read-only), independent Sel.
		// This prevents downstream operators from corrupting cached data via in-place
		// Sel mutation. Sel itself is carried over (slice header copy —
		// downstream filters replace Sel rather than mutating in place):
		// dropping it, as this used to, resurrected delete-marker-filtered
		// rows on replay.
		clone := &batch.RecordBatch{
			Schema:  cached.Schema,
			Columns: make([]*batch.Vector, len(cached.Columns)),
			Len:     cached.Len,
			Sel:     cached.Sel,
		}
		copy(clone.Columns, cached.Columns)
		return s.projectForConsumer(clone), nil
	}

	// When this scan is populating a shared cache, the entire pull-and-cache
	// step must run under cache.mu — otherwise the parallel pipeline races
	// where one worker pulls nil and sets cache.done=true while OTHER workers
	// still hold real batches that they've pulled but not yet appended. Those
	// late workers see done==true and skip the append, silently dropping
	// rows that the second (replay) scanner of this same table needs. This
	// surfaced as Q02's intermittent 4-rows-instead-of-5 result at SF0.01.
	if s.cache != nil {
		s.cache.mu.Lock()
		defer s.cache.mu.Unlock()
		if s.cache.done {
			// Another worker finished the scan while we were waiting on the
			// lock. Tell our caller "no more batches" so they fall through.
			return nil, nil
		}
		b, err := s.inner.Next(ctx)
		if err != nil {
			// This scan owns the claim and is not going to fill the cache.
			s.abandonCacheLocked(err)
			return nil, err
		}
		if b == nil {
			// An ABANDONED claim is already released and its batches are a
			// partial read. Pipeline.runParallel calls Next from every worker
			// on this same source, so "worker A failed, worker B then reached
			// the end of the table" is a real interleaving — and running this
			// branch after it would close an already-closed channel (a panic)
			// and, worse, mark a TRUNCATED cache done for the next waiter to
			// replay as if it were the whole table.
			if s.cache.abandoned {
				return nil, s.cache.err
			}
			s.cache.done = true
			if s.cache.ready != nil {
				close(s.cache.ready)
			}
			return nil, nil
		}
		// Detach from pool so the pipeline's b.Release() is a no-op.
		// Without this, the pool recycles the batch and the scanner
		// overwrites the Vectors that the cache references.
		b.Detach()
		// Cache a shallow copy so the first consumer's operators don't
		// corrupt cached data by setting Sel in-place. Sel is preserved
		// (delete markers arrive from the scan as Sel) — see the replay
		// branch.
		cached := &batch.RecordBatch{
			Schema:  b.Schema,
			Columns: make([]*batch.Vector, len(b.Columns)),
			Len:     b.Len,
			Sel:     b.Sel,
		}
		copy(cached.Columns, b.Columns)
		// NOT charged to the memory tracker, deliberately. The cache's
		// vectors are SHARED with its consumers — hash-join builds
		// Reserve hashBuildBytes for these same vectors, and the scan
		// source charges them transiently in flight. Reserving them
		// again here (tried 2026-07-06) triple-counted the same physical
		// memory: the ledger hit the budget while RSS was fine, every
		// append stalled in ReserveOrForce's relief wait, and the forced
		// build spills turned SF10 Q21 from 1m28s into 8m35s on EC2
		// (CPU profile: 4.76% utilization — pure stall). Honest cache
		// accounting needs the cache to OWN spillable bytes
		// (SpillableBatchCollector, like the CTE cache) — not a second
		// charge for memory the ledger already sees.
		s.cache.batches = append(s.cache.batches, cached)
		return s.projectForConsumer(b), nil
	}

	// No cache: inner.Next() is thread-safe for channel-based scan sources.
	return s.inner.Next(ctx)
}

// projectForConsumer narrows a union-column cache batch down to this
// consumer's RequiredColumns. Shallow: shares vectors, no copies. The
// no-cache path, SELECT-* consumers (empty requiredCols), and batches
// already matching the consumer's set pass through untouched. Also
// defensive: any required column missing from the batch schema (e.g.,
// synthetic columns) disables projection rather than dropping data.
func (s *catalogScanSource) projectForConsumer(b *batch.RecordBatch) *batch.RecordBatch {
	if s.cache == nil || len(s.requiredCols) == 0 || b == nil {
		return b
	}
	s.projOnce.Do(func() {
		if len(s.requiredCols) >= len(b.Schema) {
			return
		}
		want := make(map[string]bool, len(s.requiredCols))
		for _, name := range s.requiredCols {
			want[name] = true
		}
		// Keep BATCH-SCHEMA (table) order, matching what a standalone
		// scan of this node would emit via buildReadSchema — downstream
		// operators may have bound positions against that shape.
		idx := make([]int, 0, len(s.requiredCols))
		schema := make([]parquet.Column, 0, len(s.requiredCols))
		found := 0
		for i, col := range b.Schema {
			if want[col.Name] || batch.NameSetNames(want, col.Name) {
				idx = append(idx, i)
				schema = append(schema, col)
				found++
			}
		}
		if found < len(want) {
			return // some required column missing — pass through unprojected
		}
		s.projIdx = idx
		s.projSchema = schema
	})
	if s.projIdx == nil {
		return b
	}
	nb := &batch.RecordBatch{
		Schema:  s.projSchema,
		Columns: make([]*batch.Vector, len(s.projIdx)),
		Len:     b.Len,
		Sel:     b.Sel,
	}
	for i, ci := range s.projIdx {
		nb.Columns[i] = b.Columns[ci]
	}
	return nb
}

func (s *catalogScanSource) Close() error {
	if s.isReplay {
		return nil
	}
	// A claiming scan torn down before the end of the table (a LIMIT upstream,
	// a cancelled query, an operator that failed) owes the release just as
	// much as one that errored — otherwise every other reader of this table
	// waits on a channel nobody will close.
	s.abandonCache(errors.New("scan closed before the end of the table"))
	if s.inner != nil {
		return s.inner.Close()
	}
	return nil
}

func (s *catalogScanSource) RowsScanned() int64 {
	if s.isReplay {
		var total int64
		for _, b := range s.cache.batches {
			total += int64(b.Len)
		}
		return total
	}
	if sp, ok := s.inner.(exec.ScanStatsProvider); ok {
		return sp.RowsScanned()
	}
	return 0
}
