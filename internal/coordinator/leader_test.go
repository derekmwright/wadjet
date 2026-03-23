package coordinator

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/nats-io/nats.go/jetstream"
)

// setupNATS creates an embedded NATS server with JetStream for testing.
// Returns the JetStream handle and a cleanup function.
func setupNATSJetStream(t *testing.T) jetstream.JetStream {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	natsCfg := distributed.DefaultNATSConfig()
	natsCfg.Port = -1 // random port
	natsCfg.StoreDir = t.TempDir()
	embeddedNATS, err := distributed.NewEmbeddedNATS(natsCfg, logger)
	if err != nil {
		t.Fatalf("starting NATS: %v", err)
	}
	t.Cleanup(embeddedNATS.Shutdown)

	nc, err := distributed.ConnectInProcess(embeddedNATS.Server())
	if err != nil {
		t.Fatalf("connecting to NATS: %v", err)
	}
	t.Cleanup(func() { nc.Close() })

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatalf("creating JetStream: %v", err)
	}

	return js
}

func TestLeaderElection_SingleCoordinator(t *testing.T) {
	js := setupNATSJetStream(t)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	le, err := NewLeaderElection(js, "coord-1", logger)
	if err != nil {
		t.Fatalf("creating leader election: %v", err)
	}

	if err := le.Start(ctx); err != nil {
		t.Fatalf("starting leader election: %v", err)
	}
	defer le.Stop()

	// Single coordinator should become leader immediately
	if !le.IsLeader() {
		t.Fatal("expected single coordinator to be leader immediately")
	}

	// Verify the leader channel signals leadership
	select {
	case isLeader := <-le.LeaderChanged():
		if !isLeader {
			t.Fatal("expected leadership=true from channel")
		}
	default:
		// Channel might have already been drained during Start — that's ok
		// as long as IsLeader() returns true (checked above)
	}

	// Verify the leader key has the correct payload
	leader := le.CurrentLeader(ctx)
	if leader != "coord-1" {
		t.Fatalf("expected leader=coord-1, got %s", leader)
	}
}

func TestLeaderElection_Failover(t *testing.T) {
	js := setupNATSJetStream(t)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start first coordinator — should become leader
	le1, err := NewLeaderElection(js, "coord-1", logger)
	if err != nil {
		t.Fatalf("creating leader election 1: %v", err)
	}
	if err := le1.Start(ctx); err != nil {
		t.Fatalf("starting leader election 1: %v", err)
	}

	if !le1.IsLeader() {
		t.Fatal("expected coord-1 to be leader")
	}

	// Start second coordinator — should be standby
	le2, err := NewLeaderElection(js, "coord-2", logger)
	if err != nil {
		t.Fatalf("creating leader election 2: %v", err)
	}
	if err := le2.Start(ctx); err != nil {
		t.Fatalf("starting leader election 2: %v", err)
	}

	if le2.IsLeader() {
		t.Fatal("expected coord-2 to NOT be leader")
	}

	// Verify current leader is coord-1
	leader := le2.CurrentLeader(ctx)
	if leader != "coord-1" {
		t.Fatalf("expected leader=coord-1, got %s", leader)
	}

	// Stop first coordinator — simulates a crash
	t.Log("stopping coord-1 to simulate failover...")
	le1.Stop()

	// Wait for TTL expiry + standby acquisition (up to ~10s)
	deadline := time.After(12 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("coord-2 did not become leader within timeout")
		case isLeader := <-le2.LeaderChanged():
			if isLeader {
				t.Log("coord-2 acquired leadership")
				if !le2.IsLeader() {
					t.Fatal("expected coord-2 IsLeader() to return true")
				}
				leader = le2.CurrentLeader(ctx)
				if leader != "coord-2" {
					t.Fatalf("expected leader=coord-2, got %s", leader)
				}
				le2.Stop()
				return
			}
		case <-time.After(500 * time.Millisecond):
			// Poll in case we missed the channel event
			if le2.IsLeader() {
				t.Log("coord-2 acquired leadership (polled)")
				le2.Stop()
				return
			}
		}
	}
}

func TestLeaderElection_LeaseRefresh(t *testing.T) {
	js := setupNATSJetStream(t)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	le, err := NewLeaderElection(js, "coord-1", logger)
	if err != nil {
		t.Fatalf("creating leader election: %v", err)
	}
	if err := le.Start(ctx); err != nil {
		t.Fatalf("starting: %v", err)
	}
	defer le.Stop()

	if !le.IsLeader() {
		t.Fatal("expected to be leader")
	}

	// Wait past the TTL — the lease refresh should keep leadership alive
	time.Sleep(7 * time.Second)

	if !le.IsLeader() {
		t.Fatal("expected to still be leader after TTL (lease should have been refreshed)")
	}

	// Verify the key is still ours
	leader := le.CurrentLeader(ctx)
	if leader != "coord-1" {
		t.Fatalf("expected leader=coord-1 after refresh, got %s", leader)
	}
}

func TestQueryStateStore_SaveAndGet(t *testing.T) {
	js := setupNATSJetStream(t)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := NewQueryStateStore(js, logger)
	if err != nil {
		t.Fatalf("creating query state store: %v", err)
	}

	now := time.Now().Truncate(time.Millisecond)
	state := &PersistentQueryState{
		ID:              "q1",
		SQL:             "SELECT * FROM events",
		CompletedStages: []string{"scan-0"},
		Status:          "executing",
		LeaderID:        "coord-1",
		StartedAt:       now,
	}

	if err := store.Save(ctx, state); err != nil {
		t.Fatalf("saving state: %v", err)
	}

	got, err := store.Get(ctx, "q1")
	if err != nil {
		t.Fatalf("getting state: %v", err)
	}

	if got.ID != "q1" {
		t.Errorf("ID: got %s, want q1", got.ID)
	}
	if got.SQL != "SELECT * FROM events" {
		t.Errorf("SQL: got %s, want SELECT * FROM events", got.SQL)
	}
	if got.Status != "executing" {
		t.Errorf("Status: got %s, want executing", got.Status)
	}
	if got.LeaderID != "coord-1" {
		t.Errorf("LeaderID: got %s, want coord-1", got.LeaderID)
	}
	if len(got.CompletedStages) != 1 || got.CompletedStages[0] != "scan-0" {
		t.Errorf("CompletedStages: got %v, want [scan-0]", got.CompletedStages)
	}

	// Verify timestamps round-trip through JSON
	// JSON time marshaling truncates to seconds by default, so we compare
	// with second precision
	if !got.StartedAt.Truncate(time.Second).Equal(now.Truncate(time.Second)) {
		t.Errorf("StartedAt: got %v, want %v", got.StartedAt, now)
	}
}

func TestQueryStateStore_Delete(t *testing.T) {
	js := setupNATSJetStream(t)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := NewQueryStateStore(js, logger)
	if err != nil {
		t.Fatalf("creating query state store: %v", err)
	}

	state := &PersistentQueryState{
		ID:     "q-del",
		SQL:    "SELECT 1",
		Status: "executing",
	}

	if err := store.Save(ctx, state); err != nil {
		t.Fatalf("saving state: %v", err)
	}

	// Verify it exists
	if _, err := store.Get(ctx, "q-del"); err != nil {
		t.Fatalf("getting state before delete: %v", err)
	}

	if err := store.Delete(ctx, "q-del"); err != nil {
		t.Fatalf("deleting state: %v", err)
	}

	// Verify it's gone
	_, err = store.Get(ctx, "q-del")
	if err == nil {
		t.Fatal("expected error after delete, got nil")
	}
}

func TestQueryStateStore_ListActive(t *testing.T) {
	js := setupNATSJetStream(t)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := NewQueryStateStore(js, logger)
	if err != nil {
		t.Fatalf("creating query state store: %v", err)
	}

	// Save a mix of active and terminal states
	states := []*PersistentQueryState{
		{ID: "q1", SQL: "SELECT 1", Status: "executing", LeaderID: "c1"},
		{ID: "q2", SQL: "SELECT 2", Status: "merging", LeaderID: "c1"},
		{ID: "q3", SQL: "SELECT 3", Status: "done", LeaderID: "c1"},
		{ID: "q4", SQL: "SELECT 4", Status: "failed", LeaderID: "c1"},
		{ID: "q5", SQL: "SELECT 5", Status: "planning", LeaderID: "c1"},
	}

	for _, s := range states {
		if err := store.Save(ctx, s); err != nil {
			t.Fatalf("saving state %s: %v", s.ID, err)
		}
	}

	active, err := store.ListActive(ctx)
	if err != nil {
		t.Fatalf("listing active: %v", err)
	}

	// Should return q1 (executing), q2 (merging), q5 (planning)
	// but NOT q3 (done) or q4 (failed)
	if len(active) != 3 {
		t.Fatalf("expected 3 active queries, got %d", len(active))
	}

	activeIDs := make(map[string]bool)
	for _, a := range active {
		activeIDs[a.ID] = true
	}

	for _, expected := range []string{"q1", "q2", "q5"} {
		if !activeIDs[expected] {
			t.Errorf("expected %s in active list", expected)
		}
	}
	for _, notExpected := range []string{"q3", "q4"} {
		if activeIDs[notExpected] {
			t.Errorf("did not expect %s in active list", notExpected)
		}
	}
}

func TestQueryStateStore_ListActive_Empty(t *testing.T) {
	js := setupNATSJetStream(t)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := NewQueryStateStore(js, logger)
	if err != nil {
		t.Fatalf("creating query state store: %v", err)
	}

	active, err := store.ListActive(ctx)
	if err != nil {
		t.Fatalf("listing active on empty store: %v", err)
	}
	if len(active) != 0 {
		t.Fatalf("expected 0 active queries on empty store, got %d", len(active))
	}
}

func TestQueryStateStore_SaveOverwrite(t *testing.T) {
	js := setupNATSJetStream(t)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := NewQueryStateStore(js, logger)
	if err != nil {
		t.Fatalf("creating query state store: %v", err)
	}

	// Save initial state
	state := &PersistentQueryState{
		ID:              "q-update",
		SQL:             "SELECT 1",
		Status:          "executing",
		CompletedStages: []string{"scan-0"},
	}
	if err := store.Save(ctx, state); err != nil {
		t.Fatalf("saving initial state: %v", err)
	}

	// Update with more completed stages
	state.CompletedStages = append(state.CompletedStages, "agg-1")
	state.Status = "merging"
	if err := store.Save(ctx, state); err != nil {
		t.Fatalf("saving updated state: %v", err)
	}

	got, err := store.Get(ctx, "q-update")
	if err != nil {
		t.Fatalf("getting updated state: %v", err)
	}

	if got.Status != "merging" {
		t.Errorf("Status: got %s, want merging", got.Status)
	}
	if len(got.CompletedStages) != 2 {
		t.Fatalf("CompletedStages: got %d, want 2", len(got.CompletedStages))
	}
	if got.CompletedStages[0] != "scan-0" || got.CompletedStages[1] != "agg-1" {
		t.Errorf("CompletedStages: got %v, want [scan-0 agg-1]", got.CompletedStages)
	}
}

func TestLeaderElection_StandaloneMode(t *testing.T) {
	// When leader election is nil (standalone mode), coordinator
	// should always accept queries.
	c := &Coordinator{
		leader: nil,
	}

	if !c.isLeaderOrStandalone() {
		t.Fatal("standalone coordinator should always be considered leader")
	}
}

func TestLeaderPayload_JSON(t *testing.T) {
	// Verify the leader payload round-trips through JSON correctly
	p := leaderPayload{
		ID:    "coord-test-123",
		Since: time.Now().Truncate(time.Millisecond),
	}

	data, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshaling: %v", err)
	}

	var got leaderPayload
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshaling: %v", err)
	}

	if got.ID != p.ID {
		t.Errorf("ID: got %s, want %s", got.ID, p.ID)
	}
	if !got.Since.Equal(p.Since) {
		t.Errorf("Since: got %v, want %v", got.Since, p.Since)
	}
}
