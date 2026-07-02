package coordinator

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
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

// Lookup returns the producing worker ID for a file key, or "".
func (r *peerFileRegistry) Lookup(key string) string {
	if r == nil {
		return ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.files[key]
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
	t.FetchToken = c.peerFiles.TokenFor(root)
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
	t.InputLocations = locs
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
