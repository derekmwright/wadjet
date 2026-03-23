package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	// queryStateBucket is the NATS KV bucket for persisting in-flight query state.
	queryStateBucket = "query-state"

	// queryStateTTL auto-cleans stale queries that were never properly removed.
	queryStateTTL = 5 * time.Minute

	// queryStateKeyPrefix is prepended to query IDs for the KV key.
	queryStateKeyPrefix = "q."
)

// PersistentQueryState represents the persisted state of an in-flight query.
// This is stored in NATS KV so that a standby coordinator can recover
// queries after a failover.
type PersistentQueryState struct {
	ID              string   `json:"id"`
	SQL             string   `json:"sql"`
	CompletedStages []string `json:"completed_stages"`
	Status          string   `json:"status"` // "planning", "executing", "merging", "done", "failed"
	LeaderID        string   `json:"leader_id"`
	StartedAt       time.Time `json:"started_at"`
}

// QueryStateStore persists in-flight query state to NATS KV for HA recovery.
// When a coordinator fails, the new leader can read active query states and
// resume or clean up in-flight queries.
type QueryStateStore struct {
	kv     jetstream.KeyValue
	logger *slog.Logger
}

// NewQueryStateStore creates a query state store backed by NATS KV.
func NewQueryStateStore(js jetstream.JetStream, logger *slog.Logger) (*QueryStateStore, error) {
	if logger == nil {
		logger = slog.Default()
	}

	kv, err := js.CreateOrUpdateKeyValue(context.Background(), jetstream.KeyValueConfig{
		Bucket: queryStateBucket,
		TTL:    queryStateTTL,
	})
	if err != nil {
		return nil, fmt.Errorf("creating query state KV bucket: %w", err)
	}

	return &QueryStateStore{
		kv:     kv,
		logger: logger,
	}, nil
}

// Save persists a query state to NATS KV. Overwrites any existing entry
// for the same query ID.
func (s *QueryStateStore) Save(ctx context.Context, state *PersistentQueryState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshaling query state: %w", err)
	}

	key := queryStateKeyPrefix + state.ID
	if _, err := s.kv.Put(ctx, key, data); err != nil {
		return fmt.Errorf("saving query state %s: %w", state.ID, err)
	}

	s.logger.Debug("saved query state", "query_id", state.ID, "status", state.Status)
	return nil
}

// Get retrieves a query state by ID. Returns nil if not found.
func (s *QueryStateStore) Get(ctx context.Context, queryID string) (*PersistentQueryState, error) {
	key := queryStateKeyPrefix + queryID
	entry, err := s.kv.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("getting query state %s: %w", queryID, err)
	}

	var state PersistentQueryState
	if err := json.Unmarshal(entry.Value(), &state); err != nil {
		return nil, fmt.Errorf("unmarshaling query state %s: %w", queryID, err)
	}

	return &state, nil
}

// Delete removes a query state. Called when a query completes successfully.
func (s *QueryStateStore) Delete(ctx context.Context, queryID string) error {
	key := queryStateKeyPrefix + queryID
	if err := s.kv.Delete(ctx, key); err != nil {
		return fmt.Errorf("deleting query state %s: %w", queryID, err)
	}

	s.logger.Debug("deleted query state", "query_id", queryID)
	return nil
}

// ListActive returns all query states that are not in a terminal state
// (i.e., not "done" or "failed"). Used during failover recovery.
func (s *QueryStateStore) ListActive(ctx context.Context) ([]*PersistentQueryState, error) {
	keys, err := s.kv.Keys(ctx)
	if err != nil {
		// "nats: no keys found" is fine — no active queries
		if strings.Contains(err.Error(), "no keys found") {
			return nil, nil
		}
		return nil, fmt.Errorf("listing query state keys: %w", err)
	}

	var active []*PersistentQueryState
	for _, key := range keys {
		entry, err := s.kv.Get(ctx, key)
		if err != nil {
			s.logger.Warn("failed to read query state", "key", key, "error", err)
			continue
		}

		var state PersistentQueryState
		if err := json.Unmarshal(entry.Value(), &state); err != nil {
			s.logger.Warn("failed to unmarshal query state", "key", key, "error", err)
			continue
		}

		// Only return active (non-terminal) states
		if state.Status != "done" && state.Status != "failed" {
			active = append(active, &state)
		}
	}

	return active, nil
}
