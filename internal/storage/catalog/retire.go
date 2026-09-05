package catalog

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// ErrPathRetiring reports that a file path cannot be registered into a
// manifest right now because a retirement sweep is in the middle of deciding
// whether to physically delete the object at that path.
//
// It is the second half of #896's fix, and it is the half that does not
// depend on ordering luck. The first half — "no live manifest references this
// object" — is a READ, and a read cannot exclude a registration that lands
// after it. Marking the candidate paths before that read, and refusing a
// registration that names a marked path until the sweep is done with it,
// closes the window from the other side: a registration either completes
// before the mark (and the sweep's read sees it, and preserves the bytes) or
// arrives after it (and is refused, loudly, with nothing written).
//
// It is retryable and brief: the mark lives only for the duration of one
// retirement batch. Callers that register operator-staged objects — the
// harness loaders, `iceberg.CatalogIntegration` — should retry.
var ErrPathRetiring = errors.New("this path is being retired by a cleanup sweep: retry the registration")

// RetireRequest is one object proposed for physical retirement.
type RetireRequest struct {
	// Path is the object key, in this catalog's own bucket.
	Path string
	// NotModifiedAfter is the instant the retirement was scheduled. An
	// object written since then is not the object that was scheduled —
	// something recreated the path — and is preserved. Zero disables the
	// check.
	NotModifiedAfter time.Time
}

// RetireOutcome is what RetireObjects decided about one path.
type RetireOutcome int

const (
	// Retired: the object was deleted. The caller drops its queue entry.
	Retired RetireOutcome = iota
	// RetireReferenced: a live manifest in this catalog references the
	// object, or its bytes were replaced since the retirement was
	// scheduled. It must never be deleted on this schedule; the caller
	// drops its queue entry, because the reference is not going to
	// disappear because we waited.
	RetireReferenced
	// RetireUnproven: eligibility could not be established — the catalog
	// could not be read, or a registration naming this path was in flight.
	// Nothing was deleted. The caller requeues and tries again later.
	RetireUnproven
)

func (o RetireOutcome) String() string {
	switch o {
	case Retired:
		return "retired"
	case RetireReferenced:
		return "referenced"
	default:
		return "unproven"
	}
}

// RetireObjects physically deletes objects that NO live manifest in this
// catalog references, and preserves the bytes of every object it cannot prove
// that about.
//
// It is the one place an object is allowed to leave the bucket on a
// compaction schedule, and it exists because "this table stopped referencing
// the file" is not the same claim as "nothing references the file". #896 is
// the difference: compaction removed a source from `events`'s manifest and
// queued its bytes; a still-live `archive` registered the very same object
// through AddFiles during the grace; the queue's only guard was the object's
// LastModified, which registering unchanged bytes does not move. The queue
// deleted a file a live table's manifest still names.
//
// Three things stand between a queued path and the Delete call, and the order
// they run in is the point:
//
//  1. **The retirement mark, taken first.** Every candidate path is marked
//     before anything is read. A registration naming a marked path is refused
//     with ErrPathRetiring until the mark is released. A path with a
//     registration already IN FLIGHT is not marked at all — it comes back
//     RetireUnproven, and the caller tries again once that registration has
//     landed and can be observed.
//  2. **The live-manifest reference check**, over EVERY table in the catalog
//     (`liveCatalogState`, shared with DROP reclaim). A path any current
//     manifest names is RetireReferenced and is never deleted. Because the
//     mark is already held, a registration that could invalidate this read
//     cannot be running: it either finished before the mark (and this read
//     sees it) or is refused.
//  3. **The recreated-object guard**: an object written since the retirement
//     was scheduled is not the object that was scheduled.
//
// A catalog read that fails yields RetireUnproven for every path rather than
// a delete against an incomplete picture. Doubt preserves bytes.
//
// The residual, stated plainly: the mark is IN-PROCESS. It excludes a
// registration through this same *Catalog — which is what #896 reproduced,
// and what an embedder running a BackgroundCompactor beside its own AddFiles
// calls reaches — and it does not exclude a DIFFERENT process registering the
// path into a shared catalog. Closing that needs a catalog-side lease, which
// the deferred-delete queue could not use anyway: the queue itself is
// process-local, so another process's compactor never sees these paths.
func (c *Catalog) RetireObjects(ctx context.Context, reqs []RetireRequest) map[string]RetireOutcome {
	out := make(map[string]RetireOutcome, len(reqs))
	if len(reqs) == 0 {
		return out
	}
	log := slog.Default().With("component", "object_retire")

	paths := make([]string, 0, len(reqs))
	for _, r := range reqs {
		paths = append(paths, r.Path)
	}

	// (1) Mark. A path with a registration in flight is not marked and is
	// not a candidate this round.
	marked, busy := c.markRetiring(paths)
	defer c.unmarkRetiring(marked)
	for _, p := range busy {
		log.Warn("retirement deferred: a registration naming this path is in flight", "path", p)
		out[p] = RetireUnproven
	}
	if len(marked) == 0 {
		return out
	}

	// (2) The live-manifest reference check, taken AFTER the mark.
	livePaths, _, err := c.liveCatalogState(ctx)
	if err != nil {
		log.Warn("retirement deferred: cannot read the live catalog state",
			"error", err, "paths", len(marked))
		for _, p := range marked {
			out[p] = RetireUnproven
		}
		return out
	}

	byPath := make(map[string]RetireRequest, len(reqs))
	for _, r := range reqs {
		byPath[r.Path] = r
	}
	for _, p := range marked {
		if livePaths[p] {
			// The guard earning its keep: some table's manifest names this
			// object right now. A signal, not routine.
			log.Warn("retirement skipped: the object is still referenced by a live table's manifest", "path", p)
			out[p] = RetireReferenced
			continue
		}
		req := byPath[p]
		if !req.NotModifiedAfter.IsZero() {
			if info, hErr := c.store.Head(ctx, c.bucket, p); hErr == nil && info.LastModified.After(req.NotModifiedAfter) {
				log.Warn("retirement skipped: the object was written after the retirement was scheduled",
					"path", p, "scheduled_at", req.NotModifiedAfter, "object_modified", info.LastModified)
				out[p] = RetireReferenced
				continue
			}
		}
		if dErr := c.store.Delete(ctx, c.bucket, p); dErr != nil {
			log.Warn("retirement failed to delete an object", "path", p, "error", dErr)
			out[p] = RetireUnproven
			continue
		}
		out[p] = Retired
	}
	return out
}

// markRetiring marks every path with no registration in flight, and reports
// the ones it declined to mark.
func (c *Catalog) markRetiring(paths []string) (marked, busy []string) {
	c.retireMu.Lock()
	defer c.retireMu.Unlock()
	if c.retiring == nil {
		c.retiring = make(map[string]int)
	}
	for _, p := range paths {
		if c.registering[p] > 0 {
			busy = append(busy, p)
			continue
		}
		c.retiring[p]++
		marked = append(marked, p)
	}
	return marked, busy
}

func (c *Catalog) unmarkRetiring(paths []string) {
	c.retireMu.Lock()
	defer c.retireMu.Unlock()
	for _, p := range paths {
		if c.retiring[p] <= 1 {
			delete(c.retiring, p)
			continue
		}
		c.retiring[p]--
	}
}

// beginRegistration declares that the caller is about to write these paths
// into a manifest, and refuses if a retirement sweep is deciding whether to
// delete one of them. Every manifest write that ADDS a file path takes it,
// and every one of them pairs it with endRegistration in a defer.
//
// The in-flight count is what lets a retirement sweep tell "nothing is
// registering this path" from "a registration is running right now and its
// manifest write may not be observable yet".
func (c *Catalog) beginRegistration(paths []string) error {
	if len(paths) == 0 {
		return nil
	}
	c.retireMu.Lock()
	defer c.retireMu.Unlock()
	for _, p := range paths {
		if c.retiring[p] > 0 {
			return fmt.Errorf("registering %q: %w", p, ErrPathRetiring)
		}
	}
	if c.registering == nil {
		c.registering = make(map[string]int)
	}
	for _, p := range paths {
		c.registering[p]++
	}
	return nil
}

func (c *Catalog) endRegistration(paths []string) {
	if len(paths) == 0 {
		return
	}
	c.retireMu.Lock()
	defer c.retireMu.Unlock()
	for _, p := range paths {
		if c.registering[p] <= 1 {
			delete(c.registering, p)
			continue
		}
		c.registering[p]--
	}
}

// filePathsOf lists the paths a set of entries registers.
func filePathsOf(files []FileEntry) []string {
	if len(files) == 0 {
		return nil
	}
	out := make([]string, len(files))
	for i, f := range files {
		out[i] = f.Path
	}
	return out
}
