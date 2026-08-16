package coordinator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
)

// peerRegistryTTL bounds how long file locations and fetch tokens survive
// without an explicit CleanupQuery — a backstop for groups whose cleanup
// signal never fires (e.g. fanout-phase synthetic query IDs). Generously
// above any query lifetime; entries are tiny (key strings).
const peerRegistryTTL = 2 * time.Hour

// peerRegistrySweepEvery rate-limits the lazy TTL sweep piggybacked on
// Record/TokenFor calls.
const peerRegistrySweepEvery = 10 * time.Minute

// peerFileRegistry is the coordinator's streaming-exchange bookkeeping
// (Phase A): which worker produced (and still locally holds) each stage-
// output file, and the per-query fetch tokens. Populated centrally from
// noteTaskResult (every dispatcher's result path); consumed by the
// scheduler's task annotator, which turns entries into Task.InputLocations
// hints and Task.FetchToken.
//
// Locations are hints, never authoritative — a stale entry (worker died,
// file evicted) costs the consumer one failed peer fetch before the S3
// fallthrough. Grouping is by QueryIDFromPath for file keys and by task
// QueryID for tokens, both dropped in cleanupQuery.
type peerFileRegistry struct {
	mu        sync.Mutex
	files     map[string]string         // file key → producing worker ID
	groups    map[string]*peerFileGroup // QueryIDFromPath(key) → its keys
	tokens    map[string]*peerToken     // task QueryID → fetch token
	lastSweep time.Time
}

type peerFileGroup struct {
	keys      []string
	durable   map[string]bool // key → background upload landed (Phase B); absent = not durable
	pending   map[string]bool // key → worker reported its background upload still in flight
	createdAt time.Time
}

type peerToken struct {
	token     string
	createdAt time.Time
}

func newPeerFileRegistry() *peerFileRegistry {
	return &peerFileRegistry{
		files:  make(map[string]string),
		groups: make(map[string]*peerFileGroup),
		tokens: make(map[string]*peerToken),
	}
}

// Record notes that workerID produced the given result files and holds them
// on local disk. Retried tasks record the winning attempt's worker — the
// notification that carried the files is the one whose worker wrote them.
func (r *peerFileRegistry) Record(files []string, workerID string) {
	if r == nil || workerID == "" || len(files) == 0 {
		return
	}
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sweepLocked(now)
	for _, f := range files {
		qid := QueryIDFromPath(f)
		if qid == "" {
			continue // not query scratch (e.g. table data) — never peer-served
		}
		r.files[f] = workerID
		g := r.groups[qid]
		if g == nil {
			g = &peerFileGroup{createdAt: now}
			r.groups[qid] = g
		}
		g.keys = append(g.keys, f)
	}
}

// RecordPending marks keys the producing worker reported as
// background-upload-in-flight (ResultNotification.UploadPendingKeys).
// Call after Record — only already-recorded keys are marked. A key stops
// counting as pending once MarkDurable flips its durable bit; the pair of
// bits feeds PendingNonDurableFor (reap grace).
func (r *peerFileRegistry) RecordPending(keys []string) {
	if r == nil || len(keys) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, k := range keys {
		g := r.groups[QueryIDFromPath(k)]
		if g == nil {
			continue
		}
		if g.pending == nil {
			g.pending = make(map[string]bool)
		}
		g.pending[k] = true
	}
}

// PendingNonDurableFor counts the worker's recorded output keys whose
// background upload is still in flight (pending, not yet durable). The
// reaper grants a bounded reap grace while this is non-zero: reaping a
// producer that holds the only copy of stage outputs invalidates them and
// forces a producer-stage re-run, a far costlier recovery than waiting
// out a transient network silence. See docs/design/reap-grace.md.
func (r *peerFileRegistry) PendingNonDurableFor(workerID string) int {
	if r == nil || workerID == "" {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for key, w := range r.files {
		if w != workerID {
			continue
		}
		g := r.groups[QueryIDFromPath(key)]
		if g != nil && g.pending[key] && !g.durable[key] {
			n++
		}
	}
	return n
}

// Lookup returns the producing worker ID for a file key, or "".
func (r *peerFileRegistry) Lookup(key string) string {
	if r == nil {
		return ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.files[key]
}

// MarkDurable flips the durability bit for keys whose background upload
// landed (worker UploadComplete). Unknown keys are recorded durable-only —
// message ordering vs the result notification is not guaranteed.
func (r *peerFileRegistry) MarkDurable(keys []string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, k := range keys {
		qid := QueryIDFromPath(k)
		if qid == "" {
			continue
		}
		g := r.groups[qid]
		if g == nil {
			g = &peerFileGroup{createdAt: time.Now()}
			r.groups[qid] = g
		}
		if g.durable == nil {
			g.durable = make(map[string]bool)
		}
		g.durable[k] = true
	}
}

// IsDurable reports whether the key's durable copy is known to exist.
// Conservative: false for unknown keys and for keys produced by
// synchronous-upload tasks that never send UploadComplete — callers treat
// "not durable" as "don't conclude anything", never as "safe to fail".
func (r *peerFileRegistry) IsDurable(key string) bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	g := r.groups[QueryIDFromPath(key)]
	return g != nil && g.durable[key]
}

// TokenFor returns the fetch token for a query ID, minting one on first
// use. Producers and consumers of the same stage boundary share a QueryID,
// so both sides of a fetch present the same token.
func (r *peerFileRegistry) TokenFor(queryID string) string {
	if r == nil || queryID == "" {
		return ""
	}
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sweepLocked(now)
	if t, ok := r.tokens[queryID]; ok {
		return t.token
	}
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "" // no token → no peer serving for this query; S3 path unaffected
	}
	tok := hex.EncodeToString(raw[:])
	r.tokens[queryID] = &peerToken{token: tok, createdAt: now}
	return tok
}

// CleanupQuery drops the query's file locations and token. Called from
// cleanupQuery for every terminal query.
func (r *peerFileRegistry) CleanupQuery(queryID string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if g, ok := r.groups[queryID]; ok {
		for _, k := range g.keys {
			delete(r.files, k)
		}
		delete(r.groups, queryID)
	}
	delete(r.tokens, queryID)
}

// annotateTaskPeerLocations is the scheduler's pre-dispatch hook when
// streaming exchange is enabled: attach the query's fetch token (producers
// validate incoming fetches against it; consumers present it) and, for
// every input file a live worker still holds locally, a peer-location hint.
// Hints resolve worker → address at dispatch time, so a retried task —
// re-annotated on its way through PublishTasks — sees current liveness.
//
// The token is minted per ROOT query ID (derived from the task's scratch
// paths), NOT per task QueryID: task QueryIDs are stage-scoped, and the
// producer serving a fetch and the consumer issuing it must present the
// same token. Tasks with no scratch anchors (legacy pipeline SQL) get
// nothing — they never read stage outputs.
func (c *Coordinator) annotateTaskPeerLocations(t *distributed.Task) {
	if c.peerFiles == nil {
		return
	}
	root := distributed.TaskRootQueryID(t)
	if root == "" {
		return
	}
	if c.streamingDisabledFor(root) {
		return // ErrInputLost re-execution: pure Phase-A-less S3 semantics
	}
	t.FetchToken = c.peerFiles.TokenFor(root)
	// Phase B: native-DAG stage/shuffle outputs are consumed by workers
	// (peer tier + bounded S3 re-poll), so their uploads can run in the
	// background. Pipeline/gather tasks stay synchronous — their outputs
	// feed coordinator-side reads (legacy result retrieval) or stream
	// directly.
	if t.Type == distributed.TaskTypeStage || t.Type == distributed.TaskTypeShuffle {
		t.AsyncUpload = true
		// Durability policy (docs/design/shuffle-durability.md): defer or
		// skip the background upload, except for stages whose outputs the
		// coordinator reads itself — those have no peer tier to fall back
		// on. Retried tasks are re-annotated on their way through
		// PublishTasks, so a retry recomputes the same policy.
		switch c.config.ShuffleDurability {
		case distributed.UploadLazy, distributed.UploadOff:
			if !c.stageReadByCoordinator(root, t.StageID) {
				t.UploadPolicy = c.config.ShuffleDurability
			}
		}
	}
	var locs map[string]string
	addAll := func(files []string) {
		for _, f := range files {
			workerID := c.peerFiles.Lookup(f)
			if workerID == "" {
				continue
			}
			addr := c.workers.PeerAddr(workerID)
			if addr == "" {
				continue // worker gone or not serving — S3 covers it
			}
			if locs == nil {
				locs = make(map[string]string)
			}
			locs[f] = addr
		}
	}
	addAll(t.Files)
	addAll(t.InputFiles)
	addAll(t.BuildFiles)
	for _, fs := range t.Inputs {
		addAll(fs)
	}
	for _, fs := range t.PreScannedInputs {
		addAll(fs)
	}
	for _, fs := range t.ScanFileFilter {
		addAll(fs)
	}
	for i := range t.FusedJoins {
		addAll(t.FusedJoins[i].BuildFiles)
	}
	// Chained-join builds (stage-chain fusion) exist only as fragment op
	// specs; walk Operators so their build reads get peer hints too.
	for i := range t.Operators {
		addAll(t.Operators[i].BuildFiles)
	}
	t.InputLocations = locs
}

// stageReadByCoordinator reports whether the coordinator itself reads the
// given stage's outputs (scalar-subquery producer registered by
// executeStageDAG). Those stages' uploads must stay eager.
func (c *Coordinator) stageReadByCoordinator(root, stageID string) bool {
	if stageID == "" {
		return false
	}
	v, ok := c.coordReadStages.Load(root)
	if !ok {
		return false
	}
	_, hit := v.(map[string]struct{})[stageID]
	return hit
}

// releaseDeferredUploads broadcasts SubjectUploadRelease for a root query:
// every worker holding queued lazy uploads for it starts them. Published
// from the paths that discover a durable copy is needed after all (a
// consumer's missing-input retry against a live producer, a coordinator-
// side stage read that missed). Cheap and idempotent — a released or
// unknown root is a no-op on the workers — so no rate limiting.
func (c *Coordinator) releaseDeferredUploads(root string) {
	if c.nc == nil || root == "" || c.config.ShuffleDurability != distributed.UploadLazy {
		return
	}
	if err := c.nc.Publish(distributed.SubjectUploadRelease, []byte(root)); err != nil {
		c.logger.Debug("upload-release publish failed; retry rounds re-trigger it",
			"root", root, "error", err)
	}
}

// classifyFatalResult is the taskRetrier's fatal hook (nil-safe when
// streaming exchange is off): a missing-input failure is unrecoverable by
// task retry exactly when the producing worker is dead AND the key's
// durable copy never landed (Phase-B async upload died with the worker).
// Everything else returns false — retries either hit the peer again
// (transient fetch failure) or S3 once the in-flight upload lands.
func (c *Coordinator) classifyFatalResult(r distributed.ResultNotification) bool {
	if r.MissingInputKey == "" || c.peerFiles == nil {
		return false
	}
	if c.peerFiles.IsDurable(r.MissingInputKey) {
		return false // durable copy exists now; a retry will read it
	}
	producer := c.peerFiles.Lookup(r.MissingInputKey)
	if producer == "" {
		return false // unknown producer — can't conclude, let retries run
	}
	if c.workers.MayRecover(producer) {
		// Producer alive OR inside the reap-grace window: its upload will
		// land, the peer serves the retry, or the grace expires and a
		// later failure classifies fatal. Grace-deferred producers MUST
		// stay retryable here — otherwise this fast path declares input
		// lost during the very window the reaper is holding open for the
		// producer's uploads to land (docs/design/reap-grace.md). Under
		// lazy durability "will land" needs a nudge — the upload is
		// queued unstarted on the producer, so release it; the retry
		// then converges on the S3 copy even if the peer path stays
		// broken.
		c.releaseDeferredUploads(distributed.ScratchQueryID(r.MissingInputKey))
		return false
	}
	c.logger.Error("streaming-exchange input lost: producer dead before upload landed",
		"key", r.MissingInputKey, "producer", producer, "task_id", r.TaskID)
	return true
}

// fetchStageOutputData reads one stage-output object for coordinator-side
// consumption (scalar-subquery extraction). With streaming exchange off it
// is a plain fetchResultData. With it on, a miss may just mean the
// producer's Phase-B background upload hasn't landed — bounded re-poll,
// with a fast input-lost exit when the producer is dead and the key never
// went durable (the coordinator has no peer tier; S3 is its only read
// path).
func (c *Coordinator) fetchStageOutputData(ctx context.Context, path string) ([]byte, error) {
	data, err := c.fetchResultData(ctx, "", path)
	if err == nil || c.peerFiles == nil {
		return data, err
	}
	// Belt-and-braces under lazy durability: scalar-producer stages are
	// annotated eager via coordReadStages, but if this key's upload was
	// deferred anyway (registration gap, plan edge), release it so the
	// re-poll below can converge instead of timing out.
	c.releaseDeferredUploads(distributed.ScratchQueryID(path))
	deadline := time.Now().Add(15 * time.Second)
	for {
		if !c.peerFiles.IsDurable(path) {
			// MayRecover, not IsAlive: a grace-deferred producer keeps the
			// re-poll running — its upload may still land within this
			// poll's budget (docs/design/reap-grace.md).
			if producer := c.peerFiles.Lookup(path); producer != "" && !c.workers.MayRecover(producer) {
				return nil, fmt.Errorf("%s (%s): producer dead before upload landed", inputLostMarker, path)
			}
		}
		if time.Now().After(deadline) {
			// Budget exhausted with the producer silent past the stale
			// TTL (grace-deferred or not): classify input-lost so the
			// caller gets the streaming-disabled re-execution instead of
			// an opaque fetch failure.
			if !c.peerFiles.IsDurable(path) {
				if producer := c.peerFiles.Lookup(path); producer != "" && !c.workers.IsAlive(producer) {
					return nil, fmt.Errorf("%s (%s): producer dead before upload landed", inputLostMarker, path)
				}
			}
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
		if data, err = c.fetchResultData(ctx, "", path); err == nil {
			return data, nil
		}
	}
}

// streamingDisabledFor reports whether the root query runs with streaming
// exchange disabled (the one-shot ErrInputLost re-execution).
func (c *Coordinator) streamingDisabledFor(root string) bool {
	_, disabled := c.streamingDisabled.Load(root)
	return disabled
}

// streamingDisabledCtxKey marks a context as belonging to a
// streaming-disabled re-execution; ExecuteSQL propagates it onto the fresh
// query ID before dispatch and uses it to cap re-execution depth at one.
type streamingDisabledCtxKey struct{}

func withStreamingExchangeDisabled(ctx context.Context) context.Context {
	return context.WithValue(ctx, streamingDisabledCtxKey{}, true)
}

func streamingExchangeDisabled(ctx context.Context) bool {
	v, _ := ctx.Value(streamingDisabledCtxKey{}).(bool)
	return v
}

// sweepLocked drops groups and tokens past peerRegistryTTL, at most once
// per peerRegistrySweepEvery. Caller must hold r.mu.
func (r *peerFileRegistry) sweepLocked(now time.Time) {
	if now.Sub(r.lastSweep) < peerRegistrySweepEvery {
		return
	}
	r.lastSweep = now
	cutoff := now.Add(-peerRegistryTTL)
	for qid, g := range r.groups {
		if g.createdAt.Before(cutoff) {
			for _, k := range g.keys {
				delete(r.files, k)
			}
			delete(r.groups, qid)
		}
	}
	for qid, t := range r.tokens {
		if t.createdAt.Before(cutoff) {
			delete(r.tokens, qid)
		}
	}
}
