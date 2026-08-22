package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/derekmwright/wadjet/internal/dataplane"
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/optswitch"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// peerExchange holds the worker's streaming-exchange state (Phase A,
// docs/design/streaming-exchange.md): per-query fetch tokens and per-file
// peer-location hints, both carried in by task specs, plus the shared
// PeerClient used for outbound fetches.
//
// Everything is keyed by the ROOT query ID derived from object keys
// ("queries/<id>/..."), never by task QueryIDs — those are stage-scoped and
// differ between the producer that serves a file and the consumer that
// fetches it. State is populated at task start (Executor.Execute) and
// dropped by CleanupQuery on the same root-ID query-complete/cancel signals
// that clean the LocalStageCache.
type peerExchange struct {
	mu       sync.RWMutex
	tokens   map[string]string   // root query ID → fetch token
	hints    map[string]string   // file key → producer peer address
	rootKeys map[string][]string // root query ID → hinted keys (for cleanup)

	client *dataplane.PeerClient

	// Tier read counters (observability + test assertions).
	fetchHits        atomic.Int64 // peer fetch served the file
	fetchFallthrough atomic.Int64 // hint existed but the fetch failed → S3
}

func newPeerExchange() *peerExchange {
	return &peerExchange{
		tokens:   make(map[string]string),
		hints:    make(map[string]string),
		rootKeys: make(map[string][]string),
	}
}

// registerTask records the task's fetch token and location hints. Multiple
// tasks of one query write identical entries. No-op for tasks without a
// token (streaming exchange disabled) or with no query-scratch anchors.
func (p *peerExchange) registerTask(task *distributed.Task) {
	if p == nil || task.FetchToken == "" {
		return
	}
	root := distributed.TaskRootQueryID(task)
	p.mu.Lock()
	if root != "" {
		p.tokens[root] = task.FetchToken
	}
	for key, addr := range task.InputLocations {
		keyRoot := distributed.ScratchQueryID(key)
		if keyRoot == "" {
			continue // hints only make sense for query scratch
		}
		if _, seen := p.hints[key]; !seen {
			p.rootKeys[keyRoot] = append(p.rootKeys[keyRoot], key)
		}
		p.hints[key] = addr
	}
	p.mu.Unlock()
}

// addHint records one runtime-discovered file→peer-address hint (eager
// consumer dispatch: manifests carry the producer's PeerAddr). Same
// bookkeeping as registerTask so CleanupQuery drops it with the query.
func (p *peerExchange) addHint(key, addr string) {
	if p == nil || key == "" || addr == "" {
		return
	}
	keyRoot := distributed.ScratchQueryID(key)
	if keyRoot == "" {
		return
	}
	p.mu.Lock()
	if _, seen := p.hints[key]; !seen {
		p.rootKeys[keyRoot] = append(p.rootKeys[keyRoot], key)
	}
	p.hints[key] = addr
	p.mu.Unlock()
}

// hintFor returns the peer address hint and fetch token for one input file,
// or empty strings when no hint exists.
func (p *peerExchange) hintFor(key string) (addr, token string) {
	if p == nil {
		return "", ""
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.hints[key], p.tokens[distributed.ScratchQueryID(key)]
}

// CleanupQuery drops the root query's token and hints.
func (p *peerExchange) CleanupQuery(rootID string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	delete(p.tokens, rootID)
	for _, k := range p.rootKeys[rootID] {
		delete(p.hints, k)
	}
	delete(p.rootKeys, rootID)
	p.mu.Unlock()
}

// SetPeerClient attaches the outbound fetch client used by the Tier-1.5
// peer read path. nil (default) disables peer fetches — hints are ignored.
func (e *Executor) SetPeerClient(c *dataplane.PeerClient) {
	if e.peers == nil {
		e.peers = newPeerExchange() // struct-literal Executors (tests)
	}
	e.peers.client = c
}

// ResolveShuffleFile implements dataplane.ShuffleFileResolver: validate the
// fetch token against the one this worker's own tasks for the root query
// carried, then look the key up in the LocalStageCache. Called by the
// PeerServer for every incoming FetchShuffle. The request's queryID is
// advisory — the key's "queries/<id>/" prefix is the identity.
//
// A worker that never executed a task for the root query has no token
// recorded and denies the fetch — it also could not hold the file, so the
// consumer loses nothing by falling through to S3.
func (e *Executor) ResolveShuffleFile(ctx context.Context, _, key, token string) (string, error) {
	if bucket, obj, ok := distributed.CutBaseTablePeerKey(key); ok {
		return e.resolveBaseTableFile(ctx, bucket, obj, token)
	}
	root := distributed.ScratchQueryID(key)
	if token == "" || root == "" {
		return "", dataplane.ErrPeerDenied
	}
	if want := e.peers.tokenFor(root); want == "" || want != token {
		return "", dataplane.ErrPeerDenied
	}
	path := e.localCache.Get(root, key)
	if path == "" {
		return "", dataplane.ErrPeerNotFound
	}
	return path, nil
}

// resolveBaseTableFile serves a base-table peer fetch (base_table_peer.go)
// from this worker's base-table cache. Unlike query-scratch keys there is
// no per-query capability token — the owner may never have run a task of
// the consumer's query — so serving is gated by the tier kill switch, the
// optional cluster-wide WADJET_PEER_SECRET, and residency-or-ownership:
// bytes already admitted to the cache are served via its own index lookup
// (no path construction from request input), and a not-yet-resident file
// this worker OWNS (same rendezvous hash both sides use for placement) is
// read through from the inner store first — first-touch single-flight,
// docs/design/scan-affinity.md. Non-owned misses stay NotFound: a
// divergent membership view must not let a peer make this worker fetch
// and cache arbitrary S3 objects it will never own.
func (e *Executor) resolveBaseTableFile(ctx context.Context, bucket, key, token string) (string, error) {
	if e.baseTableCache == nil || !basePeerTierEnabled {
		return "", dataplane.ErrPeerNotFound
	}
	if basePeerSecret != "" && token != basePeerSecret {
		return "", dataplane.ErrPeerDenied
	}
	path, ok := e.baseTableCache.PeerLocalPath(bucket, key)
	if !ok {
		if !basePeerReadThroughEnabled || e.baseTableOwns == nil || !e.baseTableOwns(key) {
			return "", dataplane.ErrPeerNotFound
		}
		if err := e.baseTableCache.ReadThrough(ctx, bucket, key); err != nil {
			if e.logger != nil {
				e.logger.Debug("base-table read-through failed; peer goes durable",
					"bucket", bucket, "key", key, "error", err)
			}
			return "", dataplane.ErrPeerNotFound
		}
		if path, ok = e.baseTableCache.PeerLocalPath(bucket, key); !ok {
			return "", dataplane.ErrPeerNotFound // admitted then instantly evicted
		}
	}
	return path, nil
}

// SetBaseTableCache attaches the store stack's base-table cache layer so
// incoming peer fetches can be served from it. nil (default) rejects
// base-table fetches with NotFound.
func (e *Executor) SetBaseTableCache(c *objstore.BaseTableCache) {
	e.baseTableCache = c
}

// SetBaseTableOwnership wires the rendezvous-ownership check the owner
// read-through path consults (worker wiring, pre-Start — read without
// synchronization on every base-table fetch). owns receives the bare
// object key, matching the hash the coordinator and the consumer tier
// use for placement. nil (default) disables read-through.
func (e *Executor) SetBaseTableOwnership(owns func(key string) bool) {
	e.baseTableOwns = owns
}

// tokenFor returns the recorded fetch token for a root query ID ("" when
// unknown).
func (p *peerExchange) tokenFor(rootID string) string {
	if p == nil {
		return ""
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.tokens[rootID]
}

// openShuffleFromPeer streams one shuffle file from a producing worker's
// PeerExchange endpoint into the standard NVMe-temp + mmap read path. Only
// WSHF/WSHC payloads are valid — peers serve stage outputs, never parquet.
// Errors before openShuffleFile leave no state; openShuffleFile itself cleans
// its temp file on failure — either way the caller falls through to S3.
func (s *cachedFileStreamSource) openShuffleFromPeer(ctx context.Context, key, addr, token string) error {
	prc, err := s.executor.peers.client.FetchShuffle(ctx, addr, s.queryID, key, token)
	if err != nil {
		return err
	}
	// Per-tier ledger: count the gRPC stream's wire bytes as peer-tier
	// transfer (WSHC stays compressed through the wrapper).
	rc := io.ReadCloser(&countingReadCloser{rc: prc, n: &s.executor.shuffleIO.peerBytes})
	ownedByReader := false
	defer func() {
		if !ownedByReader {
			rc.Close()
		}
	}()
	var magic [4]byte
	if _, err := io.ReadFull(rc, magic[:]); err != nil {
		return fmt.Errorf("reading magic from peer %s: %w", addr, err)
	}
	codec, isShuffle := codecForMagic(magic)
	if !isShuffle {
		return fmt.Errorf("peer %s returned non-shuffle payload for %s (magic %q)", addr, key, magic[:])
	}
	// Streaming decode (memo §3 D1): hand the gRPC stream straight to the
	// chunk decoder — no NVMe staging hop. A failed streaming open has
	// partially consumed rc, so it cannot fall through to the staged path
	// here; returning the error sends the caller to the durable S3 tier,
	// same as any other peer-fetch failure.
	if s.executor.streamingShuffleRead {
		if err := s.openShuffleStreaming(ctx, key, rc, codec); err != nil {
			return fmt.Errorf("streaming decode from peer %s: %w", addr, err)
		}
		ownedByReader = true
		return nil
	}
	return s.openShuffleFile(ctx, key, magic[:], rc, codec)
}

// durableWaitTotal bounds how long a consumer re-polls S3 for a
// streaming-query key whose peer fetch failed (or had no hint) while the
// durable copy hasn't landed yet (Phase-B async upload in flight).
// Failure-recovery path, not a hot loop — past the bound the task fails
// with MissingInputKey and the coordinator classifies. var so tests can
// shrink the window.
var durableWaitTotal = 15 * time.Second

// durableWaitPoll caps the S3 re-poll cadence within durableWaitTotal —
// the ceiling of the geometric ramp that starts at durableWaitPollMin.
// var so tests can shrink it.
var durableWaitPoll = 500 * time.Millisecond

// durableWaitPollMin is the FIRST re-poll delay, and the base the ramp
// doubles from. Derivation, not a tuning knob:
//
// The wait is for an upload the producer has already FINALIZED locally —
// entering the wait publishes SubjectUploadRelease, which makes the
// producer's queued or foreground-yielded job urgent (upload_manager.go).
// The consumer therefore cannot learn anything faster than the rate at
// which the producer's own upload state can change, and that rate is
// uploadSlotPollMs = 50ms: the interval at which a released job re-checks
// for an admission slot. 25ms is half of it — the coarsest cadence that
// still observes every state the producer can enter between two polls.
// Anything finer only spends S3 GETs on a state that cannot have moved.
//
// What the old flat 500ms cost: it quantized every wait to a multiple of
// itself. SF100 window 2 (docs/benchmarks/sf100-window2-analysis-2026-08-22.md
// §7.1) measured the 4-row gather-merge tasks completing at
// 0.74 / 1.25 / 1.75 / 2.25 / 2.77 / 3.34 / 3.83 / 4.30 / 4.81 s — a
// 500ms grid sitting on the critical path of every aggregate query, worth
// 12–14.5 s per suite run. The ramp keeps the same 15s budget, the same
// ceiling and the same MissingInputKey failure semantics, and pays at most
// one interval of overshoot: ~25ms for a copy that is already landing,
// 500ms only once the wait is already seconds long.
var durableWaitPollMin = 25 * time.Millisecond

// durableWaitPollGrowth is the ramp's multiplier. Binary: 25, 50, 100,
// 200, 400, 500, 500… — the ceiling is reached after 775ms of waiting,
// i.e. 5 probes inside the window where the flat cadence managed 1.
const durableWaitPollGrowth = 2

// durableWaitJitterNum/Den spread each delay over ±25% of its nominal
// value (mean preserved) so the tasks of one stage boundary — 14+ of them
// can enter the wait in the same millisecond — do not re-poll S3 as a
// synchronized burst.
const (
	durableWaitJitterNum = 1
	durableWaitJitterDen = 4
)

// durableWaitBackoff gates the ramp. Off = the pre-2026-08-22 flat
// durableWaitPoll cadence, unjittered. Registered here rather than in a
// planner package because it changes only WHEN a consumer re-reads an
// object it is already entitled to read, never which rows it gets — but
// the invariance oracle enumerates it all the same, and it is bisectable
// on EC2 by env alone.
var durableWaitBackoff = optswitch.Register("durable-wait-backoff", "WADJET_DURABLE_WAIT_BACKOFF",
	"geometric+jittered re-poll ramp (25ms → 500ms) in the durable-object wait; off = flat 500ms polling")

// nextDurableWaitPoll returns the delay to sleep before the next re-poll
// and the nominal delay the one after that ramps to.
func nextDurableWaitPoll(cur time.Duration) (sleep, next time.Duration) {
	if !durableWaitBackoff.On() {
		return durableWaitPoll, durableWaitPoll
	}
	next = cur * durableWaitPollGrowth
	if next > durableWaitPoll {
		next = durableWaitPoll
	}
	span := cur * durableWaitJitterNum / durableWaitJitterDen
	sleep = cur - span
	if span > 0 {
		sleep += rand.N(2 * span)
	}
	return sleep, next
}

// missingInputError marks a task failure caused by an unresolvable hinted
// input: the peer fetch failed and the durable copy stayed absent past the
// bounded wait. Surfaces as ResultNotification.MissingInputKey so the
// coordinator can distinguish "producer died before its upload landed"
// (ErrInputLost, unrecoverable by task retry) from ordinary failures.
type missingInputError struct {
	key   string
	cause error
}

func (e *missingInputError) Error() string {
	return fmt.Sprintf("input %s unavailable: peer fetch failed and no durable copy after %s: %v", e.key, durableWaitTotal, e.cause)
}

func (e *missingInputError) Unwrap() error { return e.cause }

// awaitDurableObject re-polls the store for a hinted key whose first read
// missed — the producing worker reported the file, so either its background
// upload is in flight (it will land) or the producer died with it (it
// won't). Returns the opened reader on success.
//
// Entering this wait IS a demand signal: broadcast SubjectUploadRelease
// for the key's root query so the producing worker's pending uploads jump
// the foreground-yield gate (upload_manager.go urgency) instead of racing
// the re-poll at the throttled width. Idempotent, cheap, best-effort.
func (s *cachedFileStreamSource) awaitDurableObject(ctx context.Context, key string) (io.ReadCloser, error) {
	if root := rootQueryFromKey(key); root != "" && s.executor.nc != nil {
		if err := s.executor.nc.Publish(distributed.SubjectUploadRelease, []byte(root)); err != nil && s.executor.logger != nil {
			s.executor.logger.Debug("upload-release publish failed; re-poll continues",
				"key", key, "error", err)
		}
	}
	start := time.Now()
	deadline := start.Add(durableWaitTotal)
	delay := min(durableWaitPollMin, durableWaitPoll)
	polls := 0
	defer func() { s.noteDurableWait(time.Since(start), polls) }()
	// Not-found is the state this wait was entered in: a budget that
	// expires before the first poll must still report why, not a nil cause.
	lastErr := error(objstore.ErrNotFound)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, lastErr
		}
		sleep, next := nextDurableWaitPoll(delay)
		delay = next
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(min(sleep, remaining)):
		}
		polls++
		rc, _, err := s.executor.store.Get(ctx, s.bucket, key)
		if err == nil {
			return rc, nil
		}
		lastErr = err
		if !errors.Is(err, objstore.ErrNotFound) {
			return nil, err // real store error — don't spin on it
		}
	}
}

// noteDurableWait folds one completed durable wait into the task's
// acquisition tally, so the "fragment task phases" line carries
// durable_waits / durable_wait_polls / durable_wait_ms. Producer
// goroutine only, like every other srcAcqStats write.
func (s *cachedFileStreamSource) noteDurableWait(d time.Duration, polls int) {
	if s.acq == nil {
		s.acq = &srcAcqStats{}
	}
	s.acq.noteDurableWait(d, polls)
}

// rootQueryFromKey extracts the root query id from a stage-output key of
// the form "queries/<root>/<stage>/<file>". Empty for other layouts.
func rootQueryFromKey(key string) string {
	const p = "queries/"
	if !strings.HasPrefix(key, p) {
		return ""
	}
	rest := key[len(p):]
	if i := strings.IndexByte(rest, '/'); i > 0 {
		return rest[:i]
	}
	return ""
}

// PeerFetchHits returns how many input files were served via peer fetch.
func (e *Executor) PeerFetchHits() int64 { return e.peers.fetchHits.Load() }

// PeerFetchFallthroughs returns how many hinted fetches failed over to S3.
func (e *Executor) PeerFetchFallthroughs() int64 { return e.peers.fetchFallthrough.Load() }
