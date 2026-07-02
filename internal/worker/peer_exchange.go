package worker

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/citc-tech/wadjet/internal/dataplane"
	"github.com/citc-tech/wadjet/internal/distributed"
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
func (e *Executor) ResolveShuffleFile(_, key, token string) (string, error) {
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
	rc, err := s.executor.peers.client.FetchShuffle(ctx, addr, s.queryID, key, token)
	if err != nil {
		return err
	}
	defer rc.Close()
	var magic [4]byte
	if _, err := io.ReadFull(rc, magic[:]); err != nil {
		return fmt.Errorf("reading magic from peer %s: %w", addr, err)
	}
	wshf := magic == shuffleMagic
	wshc := magic == compressedMagic
	if !wshf && !wshc {
		return fmt.Errorf("peer %s returned non-shuffle payload for %s (magic %q)", addr, key, magic[:])
	}
	return s.openShuffleFile(ctx, key, magic[:], rc, wshc)
}

// PeerFetchHits returns how many input files were served via peer fetch.
func (e *Executor) PeerFetchHits() int64 { return e.peers.fetchHits.Load() }

// PeerFetchFallthroughs returns how many hinted fetches failed over to S3.
func (e *Executor) PeerFetchFallthroughs() int64 { return e.peers.fetchFallthrough.Load() }
