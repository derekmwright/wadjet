package coordinator

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/dataplane"
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// getCountingStore counts Get calls so a test can prove a coordinator-side
// read never reached the durable store.
type getCountingStore struct {
	objstore.Store
	gets atomic.Int64
}

func (s *getCountingStore) Get(ctx context.Context, bucket, key string) (io.ReadCloser, objstore.ObjectInfo, error) {
	s.gets.Add(1)
	return s.Store.Get(ctx, bucket, key)
}

// stageOutputResolver is the producing worker's side of a peer fetch: the
// token must match the one the coordinator minted for the root query, and
// the key must be one this worker still holds locally. Mirrors
// worker.Executor.ResolveShuffleFile's contract without importing worker.
type stageOutputResolver struct {
	token  string
	files  map[string]string // key → local path
	serves atomic.Int64
}

func (r *stageOutputResolver) ResolveShuffleFile(_ context.Context, _, key, token string) (string, error) {
	if token == "" || token != r.token {
		return "", dataplane.ErrPeerDenied
	}
	path, ok := r.files[key]
	if !ok {
		return "", dataplane.ErrPeerNotFound
	}
	r.serves.Add(1)
	return path, nil
}

// wshfFloatPayload builds a valid one-row / one-column WSHF buffer holding
// v — the shape a scalar-subquery producer emits (see
// internal/worker/shuffle_format.go for the layout).
func wshfFloatPayload(v float64) []byte {
	var buf []byte
	buf = append(buf, 'W', 'S', 'H', 'F')
	buf = binary.LittleEndian.AppendUint32(buf, 1) // 1 chunk
	buf = binary.LittleEndian.AppendUint16(buf, 1) // 1 column
	buf = binary.LittleEndian.AppendUint16(buf, 1) // name length
	buf = append(buf, 'x')
	buf = append(buf, byte(parquet.TypeFloat64))
	buf = binary.LittleEndian.AppendUint32(buf, 1) // 1 row
	buf = binary.LittleEndian.AppendUint32(buf, 1) // 1 null-bitmap word
	buf = binary.LittleEndian.AppendUint64(buf, 1) // row 0 valid
	buf = binary.LittleEndian.AppendUint32(buf, 8) // 8 data bytes
	buf = binary.LittleEndian.AppendUint64(buf, math.Float64bits(v))
	return buf
}

const (
	stageReadRoot   = "q-stage-read"
	stageReadKey    = "queries/q-stage-read/final_aggregate-6/t7.wshf"
	stageReadBucket = "results"
)

// stageReadHarness is a coordinator whose scalar producer's output exists
// BOTH on the producing worker's local disk (served by a live in-process
// PeerServer) and in the durable store — the steady state at the moment the
// coordinator reads a scalar: the local copy is written before the result
// notification, the S3 copy lands whenever the background upload gets to it.
// Counting durable Gets is what separates the two tiers.
type stageReadHarness struct {
	c        *Coordinator
	store    *getCountingStore
	resolver *stageOutputResolver
	payload  []byte
}

// newStageReadHarness wires the coordinator. producerLive registers the
// worker heartbeat that carries its peer address; workerToken is the token
// the producer will accept (mismatch = PermissionDenied, the "stale hint"
// case).
func newStageReadHarness(t *testing.T, producerLive bool, workerToken string) *stageReadHarness {
	t.Helper()
	ctx := context.Background()
	payload := wshfFloatPayload(42.5)

	local := filepath.Join(t.TempDir(), "t7.wshf")
	if err := os.WriteFile(local, payload, 0o644); err != nil {
		t.Fatal(err)
	}
	resolver := &stageOutputResolver{token: workerToken, files: map[string]string{stageReadKey: local}}
	srv := dataplane.NewPeerServer(dataplane.PeerServerConfig{Addr: "127.0.0.1:0"}, resolver, nil)
	if err := srv.Start(); err != nil {
		t.Fatalf("starting peer server: %v", err)
	}
	t.Cleanup(srv.Stop)

	store := &getCountingStore{Store: objstore.NewMemStore()}
	if err := store.MakeBucket(ctx, stageReadBucket); err != nil {
		t.Fatal(err)
	}
	// The durable copy exists too: every tier must be able to answer, so a
	// zero Get count proves a choice, not an accident.
	if _, err := store.Put(ctx, stageReadBucket, stageReadKey, bytes.NewReader(payload), int64(len(payload)), ""); err != nil {
		t.Fatal(err)
	}

	c := &Coordinator{
		config:     Config{StreamingExchange: true, ResultBucket: stageReadBucket},
		catalog:    catalog.NewWithStore(store, stageReadBucket),
		peerFiles:  newPeerFileRegistry(),
		peerClient: dataplane.NewPeerClient(nil),
		workers: &WorkerRegistry{
			workers: make(map[string]*WorkerInfo),
			stale:   time.Minute,
			logger:  slog.Default(),
		},
		logger: slog.Default(),
	}
	t.Cleanup(c.peerClient.Close)

	// Registry state the real coordinator builds: the token it minted for
	// the root query at dispatch, and the producer that reported the file.
	c.peerFiles.TokenFor(stageReadRoot)
	c.peerFiles.Record([]string{stageReadKey}, "w1")
	if producerLive {
		c.workers.record(distributed.WorkerHeartbeat{WorkerID: "w1", PeerAddr: srv.AdvertiseAddr()})
	}
	return &stageReadHarness{c: c, store: store, resolver: resolver, payload: payload}
}

// TestCoordinatorStageRead_PeerTierServesWithoutDurableGet is the
// regression test for the scalar-substitution barrier (SF100 window 4 §7):
// the coordinator must read a producer's stage output from the worker that
// still holds it, not from the durable copy it may still be waiting on.
func TestCoordinatorStageRead_PeerTierServesWithoutDurableGet(t *testing.T) {
	h := newStageReadHarness(t, true, "")
	// Producer accepts the coordinator's own token.
	h.resolver.token = h.c.peerFiles.ExistingTokenFor(stageReadRoot)

	data, tier, err := h.c.fetchStageOutputData(context.Background(), stageReadKey)
	if err != nil {
		t.Fatalf("fetchStageOutputData: %v", err)
	}
	if tier != coordReadPeer {
		t.Fatalf("tier = %s, want peer", tier)
	}
	if !bytes.Equal(data, h.payload) {
		t.Fatalf("payload mismatch: got %d bytes, want %d", len(data), len(h.payload))
	}
	if got := h.store.gets.Load(); got != 0 {
		t.Fatalf("durable Gets = %d, want 0 (the peer held the file)", got)
	}
	if got := h.resolver.serves.Load(); got != 1 {
		t.Fatalf("peer serves = %d, want 1", got)
	}
	kv, peer, s3, misses := h.c.StageReadTierCounts()
	if kv != 0 || peer != 1 || s3 != 0 || misses != 0 {
		t.Fatalf("tier counts kv=%d peer=%d s3=%d misses=%d, want 0/1/0/0", kv, peer, s3, misses)
	}
}

// TestCoordinatorStageRead_SwitchOffUsesDurableCopy pins the kill switch:
// WADJET_COORD_PEER_READS=0 restores the KV→S3 path exactly.
func TestCoordinatorStageRead_SwitchOffUsesDurableCopy(t *testing.T) {
	h := newStageReadHarness(t, true, "")
	h.resolver.token = h.c.peerFiles.ExistingTokenFor(stageReadRoot)
	prev := coordPeerReads.Set(false)
	t.Cleanup(func() { coordPeerReads.Set(prev) })

	data, tier, err := h.c.fetchStageOutputData(context.Background(), stageReadKey)
	if err != nil {
		t.Fatalf("fetchStageOutputData: %v", err)
	}
	if tier != coordReadS3 {
		t.Fatalf("tier = %s, want s3", tier)
	}
	if !bytes.Equal(data, h.payload) {
		t.Fatal("payload mismatch on the durable path")
	}
	if got := h.store.gets.Load(); got != 1 {
		t.Fatalf("durable Gets = %d, want 1", got)
	}
	if got := h.resolver.serves.Load(); got != 0 {
		t.Fatalf("peer serves = %d, want 0 with the switch off", got)
	}
}

// TestCoordinatorStageRead_FallthroughToDurable covers every way the tier
// declines or fails. In each the read must still succeed off the durable
// copy — hints are advisory, and nothing here may turn a readable object
// into an error.
func TestCoordinatorStageRead_FallthroughToDurable(t *testing.T) {
	cases := []struct {
		name       string
		setup      func(t *testing.T) *stageReadHarness
		wantMisses int64
	}{
		{
			name: "producer not live",
			// No heartbeat: PeerAddr is "", so the tier declines before dialing.
			setup: func(t *testing.T) *stageReadHarness { return newStageReadHarness(t, false, "tok") },
		},
		{
			name: "producer unknown to the registry",
			setup: func(t *testing.T) *stageReadHarness {
				h := newStageReadHarness(t, true, "tok")
				h.c.peerFiles.CleanupQuery(stageReadRoot)
				h.c.peerFiles.TokenFor(stageReadRoot)
				return h
			},
		},
		{
			name: "no token minted for the query",
			setup: func(t *testing.T) *stageReadHarness {
				h := newStageReadHarness(t, true, "tok")
				h.c.peerFiles.mu.Lock()
				delete(h.c.peerFiles.tokens, stageReadRoot)
				h.c.peerFiles.mu.Unlock()
				return h
			},
		},
		{
			name: "streaming disabled for the query (ErrInputLost re-execution)",
			setup: func(t *testing.T) *stageReadHarness {
				h := newStageReadHarness(t, true, "tok")
				h.c.streamingDisabled.Store(stageReadRoot, true)
				return h
			},
		},
		{
			name: "producer rejects the token",
			// Stale/mismatched capability: the fetch is attempted and fails,
			// which must count as a miss and fall through.
			setup:      func(t *testing.T) *stageReadHarness { return newStageReadHarness(t, true, "some-other-token") },
			wantMisses: 1,
		},
		{
			name: "producer no longer holds the file",
			setup: func(t *testing.T) *stageReadHarness {
				h := newStageReadHarness(t, true, "tok")
				h.resolver.token = h.c.peerFiles.ExistingTokenFor(stageReadRoot)
				h.resolver.files = map[string]string{}
				return h
			},
			wantMisses: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := tc.setup(t)
			data, tier, err := h.c.fetchStageOutputData(context.Background(), stageReadKey)
			if err != nil {
				t.Fatalf("fetchStageOutputData: %v", err)
			}
			if tier != coordReadS3 {
				t.Fatalf("tier = %s, want s3", tier)
			}
			if !bytes.Equal(data, h.payload) {
				t.Fatal("payload mismatch on the durable path")
			}
			if got := h.store.gets.Load(); got != 1 {
				t.Fatalf("durable Gets = %d, want 1", got)
			}
			if _, _, _, misses := h.c.StageReadTierCounts(); misses != tc.wantMisses {
				t.Fatalf("peer misses = %d, want %d", misses, tc.wantMisses)
			}
		})
	}
}

// TestScalarSubstitution_ServedByPeerTier is the end-to-end shape of the
// lever: the scalar-subquery substitution the whole cluster waits on
// resolves off the producing worker's local copy, with zero durable Gets.
func TestScalarSubstitution_ServedByPeerTier(t *testing.T) {
	h := newStageReadHarness(t, true, "")
	h.resolver.token = h.c.peerFiles.ExistingTokenFor(stageReadRoot)

	consumer := physical.Stage{
		ID:                 "join-4",
		ScalarDependencies: map[string]string{"scalar_1": "final_aggregate-6"},
		FilterExprs:        []string{"total > :scalar_1"},
	}
	producers := map[string]StageOutput{
		"final_aggregate-6": {Files: [][]string{{stageReadKey}}},
	}
	out, err := h.c.substituteScalarDependencies(context.Background(), consumer, producers,
		map[string]physical.Stage{"final_aggregate-6": {ID: "final_aggregate-6"}})
	if err != nil {
		t.Fatalf("substituteScalarDependencies: %v", err)
	}
	if got, want := out.FilterExprs[0], "total > 42.5"; got != want {
		t.Fatalf("substituted filter = %q, want %q", got, want)
	}
	if got := h.store.gets.Load(); got != 0 {
		t.Fatalf("durable Gets = %d, want 0 — the scalar read must not wait on the upload", got)
	}
	if _, peer, _, _ := h.c.StageReadTierCounts(); peer != 1 {
		t.Fatalf("peer-tier reads = %d, want 1", peer)
	}
}
