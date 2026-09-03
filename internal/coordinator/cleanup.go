package coordinator

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// ResultCleaner manages cleanup of query result files in object storage.
type ResultCleaner struct {
	store         objstore.Store
	bucket        string
	ttl           time.Duration
	logger        *slog.Logger
	activeQueries func() map[string]struct{} // returns IDs of in-flight queries
}

// NewResultCleaner creates a result cleaner.
func NewResultCleaner(store objstore.Store, bucket string, ttl time.Duration, logger *slog.Logger) *ResultCleaner {
	if ttl == 0 {
		ttl = 1 * time.Hour
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &ResultCleaner{
		store:  store,
		bucket: bucket,
		ttl:    ttl,
		logger: logger,
	}
}

// SetActiveQueriesFunc registers a callback that returns the set of
// query IDs currently in-flight. CleanStale will skip files belonging
// to these queries regardless of age.
func (rc *ResultCleaner) SetActiveQueriesFunc(fn func() map[string]struct{}) {
	rc.activeQueries = fn
}

// CleanQuery removes all result files for a specific query.
//
// It reports what it actually reclaimed. The loop used to log every failure
// and continue with a nil return, so a caller was told the cleanup succeeded
// after deleting nothing (measured: deleted=0, err=<nil> while the store's
// breaker was open) and nothing ever retried. It also kept issuing deletes
// after the caller's deadline had expired, manufacturing one instant
// DeadlineExceeded per remaining object — a burst of consecutive failures
// that opened the breaker by itself (#820, and the producer half of #798).
// The first ctx error ends the loop; anything left is named in the error.
func (rc *ResultCleaner) CleanQuery(ctx context.Context, queryID string) (int, error) {
	prefix := fmt.Sprintf("queries/%s/", queryID)
	objects, err := rc.store.List(ctx, rc.bucket, objstore.ListOptions{Prefix: prefix})
	if err != nil {
		return 0, fmt.Errorf("listing query results: %w", err)
	}

	deleted := 0
	var firstErr error
	for _, obj := range objects {
		if cerr := ctx.Err(); cerr != nil {
			return deleted, fmt.Errorf("cleaning query %s: stopped after %d of %d objects: %w",
				queryID, deleted, len(objects), cerr)
		}
		if err := rc.store.Delete(ctx, rc.bucket, obj.Key); err != nil {
			rc.logger.Warn("failed to delete result file", "path", obj.Key, "error", err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		deleted++
	}

	if firstErr != nil {
		return deleted, fmt.Errorf("cleaning query %s: %d of %d objects remain: %w",
			queryID, len(objects)-deleted, len(objects), firstErr)
	}
	rc.logger.Info("cleaned query results", "query_id", queryID, "deleted", deleted)
	return deleted, nil
}

// CleanAll removes every queries/ object regardless of TTL or active set.
// Intended for shutdown — when the coordinator is exiting, in-flight queries
// are already dying, so leftover intermediates would never be read again
// and just leak. A normal-running coordinator should use CleanQuery for
// individual completions and CleanStale for periodic GC.
func (rc *ResultCleaner) CleanAll(ctx context.Context) (int, error) {
	objects, err := rc.store.List(ctx, rc.bucket, objstore.ListOptions{Prefix: "queries/"})
	if err != nil {
		return 0, fmt.Errorf("listing results: %w", err)
	}
	deleted := 0
	var firstErr error
	for _, obj := range objects {
		if cerr := ctx.Err(); cerr != nil {
			return deleted, fmt.Errorf("cleaning all query intermediates: stopped after %d of %d objects: %w",
				deleted, len(objects), cerr)
		}
		if err := rc.store.Delete(ctx, rc.bucket, obj.Key); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		deleted++
	}
	if deleted > 0 {
		rc.logger.Info("cleaned all query intermediates on shutdown",
			"objects_deleted", deleted)
	}
	if firstErr != nil {
		return deleted, fmt.Errorf("cleaning all query intermediates: %d of %d objects remain: %w",
			len(objects)-deleted, len(objects), firstErr)
	}
	return deleted, nil
}

// CleanStale removes result files older than the TTL, skipping files
// that belong to queries currently in-flight.
func (rc *ResultCleaner) CleanStale(ctx context.Context) (int, error) {
	objects, err := rc.store.List(ctx, rc.bucket, objstore.ListOptions{Prefix: "queries/"})
	if err != nil {
		return 0, fmt.Errorf("listing results: %w", err)
	}

	// Build the set of active query IDs to protect.
	var active map[string]struct{}
	if rc.activeQueries != nil {
		active = rc.activeQueries()
	}

	cutoff := time.Now().Add(-rc.ttl)
	deleted := 0
	skipped := 0
	stale := 0
	var firstErr error
	for _, obj := range objects {
		if obj.LastModified.Before(cutoff) {
			if active != nil {
				qid := QueryIDFromPath(obj.Key)
				if _, ok := active[qid]; ok {
					skipped++
					continue
				}
			}
			stale++
			if cerr := ctx.Err(); cerr != nil {
				return deleted, fmt.Errorf("cleaning stale results: stopped after %d objects: %w", deleted, cerr)
			}
			if err := rc.store.Delete(ctx, rc.bucket, obj.Key); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			deleted++
		}
	}

	if deleted > 0 || skipped > 0 {
		rc.logger.Info("cleaned stale results", "deleted", deleted, "skipped_active", skipped, "ttl", rc.ttl)
	}
	if firstErr != nil {
		return deleted, fmt.Errorf("cleaning stale results: %d of %d stale objects remain: %w",
			stale-deleted, stale, firstErr)
	}
	return deleted, nil
}

// StartPeriodicCleanup runs cleanup on a schedule.
func (rc *ResultCleaner) StartPeriodicCleanup(ctx context.Context, interval time.Duration) {
	if interval == 0 {
		interval = 10 * time.Minute
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				rc.CleanStale(ctx)
			}
		}
	}()
}

// QueryIDFromPath extracts the query ID from a result file path.
func QueryIDFromPath(path string) string {
	// paths like: queries/{queryID}/stage-0/task-0001.parquet
	parts := strings.Split(path, "/")
	if len(parts) >= 2 && parts[0] == "queries" {
		return parts[1]
	}
	return ""
}
