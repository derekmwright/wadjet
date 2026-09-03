package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	// leaderBucket is the NATS KV bucket name for leader election.
	leaderBucket = "coordinator-leader"

	// leaderKey is the single key used for leader election.
	leaderKey = "leader"

	// leaderTTL is how long a leader entry lives before expiring.
	// If the leader crashes without refreshing, the key expires after this duration
	// and standbys can acquire leadership.
	leaderTTL = 5 * time.Second

	// leaderRefreshInterval is how often the leader refreshes its lease.
	// Must be well under leaderTTL to prevent accidental expiry.
	leaderRefreshInterval = 2 * time.Second

	// standbyPollInterval is how frequently standbys check whether the leader key
	// has expired. Polling is used because NATS KV TTL expiry does not always
	// generate watcher events reliably.
	standbyPollInterval = 1 * time.Second
)

// leaderPayload is the JSON structure stored in the leader KV entry.
type leaderPayload struct {
	ID    string    `json:"id"`
	Since time.Time `json:"since"`
}

// LeaderElection implements active-passive leader election for coordinators
// using NATS KV with TTL-based leasing. One coordinator is leader (accepts
// queries), the other is standby (warm, ready to take over).
//
// The leader writes its ID + timestamp, refreshing every 2s with a 5s TTL.
// CAS (revision-based) updates prevent split-brain. Standbys poll the key
// and attempt acquisition when it expires.
//
// NOT YET WIRED INTO ANY SERVER MODE. `NewLeaderElection` has no non-test
// caller and `Coordinator.SetLeaderElection` has no caller at all, so no
// `wadjet serve` path instantiates this and no deployment has ever run it.
// The behaviour described here — and every defect fixed in it — is therefore
// forward-looking: it is what will be true of the first mode that wires it
// up, not a description of anything a running server does today.
type LeaderElection struct {
	kv       jetstream.KeyValue
	id       string // unique coordinator ID
	isLeader atomic.Bool
	leaderCh chan bool
	rev      uint64 // last known revision for CAS updates
	logger   *slog.Logger
	cancel   context.CancelFunc
	done     chan struct{}
	// leaseReclaims counts refreshes that FAILED their CAS and were resolved
	// by reclaimLease without standing down.
	leaseReclaims atomic.Int64
}

// NewLeaderElection creates a new leader election instance.
// The id should be unique across coordinators (e.g., hostname + pid or UUID).
func NewLeaderElection(js jetstream.JetStream, id string, logger *slog.Logger) (*LeaderElection, error) {
	if logger == nil {
		logger = slog.Default()
	}

	kv, err := js.CreateOrUpdateKeyValue(context.Background(), jetstream.KeyValueConfig{
		Bucket: leaderBucket,
		TTL:    leaderTTL,
	})
	if err != nil {
		return nil, fmt.Errorf("creating leader KV bucket: %w", err)
	}

	return &LeaderElection{
		kv:       kv,
		id:       id,
		leaderCh: make(chan bool, 1),
		logger:   logger,
		done:     make(chan struct{}),
	}, nil
}

// Start begins the leader election process. A single run loop goroutine
// handles both leader and standby states, switching between them as needed.
func (le *LeaderElection) Start(ctx context.Context) error {
	ctx, le.cancel = context.WithCancel(ctx)

	// Try to become leader immediately
	acquired := le.tryAcquire(ctx)
	if acquired {
		le.becomeLeader()
	}

	go le.runLoop(ctx, acquired)

	return nil
}

// Stop gracefully stops the leader election. If this instance is leader,
// the key will expire after TTL, allowing another coordinator to take over.
func (le *LeaderElection) Stop() {
	if le.cancel != nil {
		le.cancel()
	}
	<-le.done
}

// IsLeader returns true if this coordinator is the current leader.
func (le *LeaderElection) IsLeader() bool {
	return le.isLeader.Load()
}

// LeaderChanged returns a channel that receives true when this instance
// becomes leader, or false when it loses leadership.
func (le *LeaderElection) LeaderChanged() <-chan bool {
	return le.leaderCh
}

// tryAcquire attempts to create the leader key (only succeeds if key doesn't exist).
func (le *LeaderElection) tryAcquire(ctx context.Context) bool {
	payload, err := json.Marshal(leaderPayload{
		ID:    le.id,
		Since: time.Now(),
	})
	if err != nil {
		le.logger.Error("marshaling leader payload", "error", err)
		return false
	}

	rev, err := le.kv.Create(ctx, leaderKey, payload)
	if err != nil {
		// Key already exists — another coordinator is leader
		return false
	}
	le.rev = rev
	return true
}

// becomeLeader transitions this instance to leader state.
func (le *LeaderElection) becomeLeader() {
	le.isLeader.Store(true)
	le.logger.Info("became leader", "id", le.id)
	// Non-blocking send to notify listeners
	select {
	case le.leaderCh <- true:
	default:
	}
}

// loseLeadership transitions this instance to standby state.
func (le *LeaderElection) loseLeadership() {
	le.isLeader.Store(false)
	le.logger.Warn("lost leadership", "id", le.id)
	select {
	case le.leaderCh <- false:
	default:
	}
}

// runLoop is the single goroutine managing the leader election lifecycle.
// It switches between leader mode (refreshing the lease) and standby mode
// (polling for expiry) within one goroutine, avoiding channel lifecycle issues.
func (le *LeaderElection) runLoop(ctx context.Context, startAsLeader bool) {
	defer close(le.done)

	amLeader := startAsLeader
	for {
		if amLeader {
			// Leader mode: refresh lease periodically
			lost := le.runLeader(ctx)
			if !lost {
				return // context cancelled
			}
			// Lost leadership — transition to standby
			le.loseLeadership()
			amLeader = false
		} else {
			// Standby mode: poll for key expiry
			acquired := le.runStandby(ctx)
			if !acquired {
				return // context cancelled
			}
			// Acquired leadership — transition to leader
			le.becomeLeader()
			amLeader = true
		}
	}
}

// runLeader runs the leader refresh loop. Returns true if leadership was lost
// (should transition to standby), false if the context was cancelled.
func (le *LeaderElection) runLeader(ctx context.Context) (lost bool) {
	ticker := time.NewTicker(leaderRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			if !le.refreshLease(ctx) {
				return true // lost leadership
			}
		}
	}
}

// refreshLease updates the leader key with CAS to prove this instance is
// still alive. Returns false if the lease was lost.
func (le *LeaderElection) refreshLease(ctx context.Context) bool {
	payload, err := json.Marshal(leaderPayload{
		ID:    le.id,
		Since: time.Now(),
	})
	if err != nil {
		le.logger.Error("marshaling leader payload", "error", err)
		return false
	}

	rev, err := le.kv.Update(ctx, leaderKey, payload, le.rev)
	if err == nil {
		le.rev = rev
		return true
	}
	le.logger.Warn("leader lease refresh failed", "error", err)
	return le.reclaimLease(ctx, payload, err)
}

// reclaimLease decides what a FAILED refresh actually MEANS by asking the
// store who holds the lease now. A CAS rejection says only that the key is
// not at the revision this instance recorded, and there are three states
// behind that, which are not the same thing:
//
//   - The key is GONE. The KV bucket's TTL is 5s and the refresh ticks every
//     2s, so a tick the runtime delivers late — a GC pause, a busy host, a
//     slow round trip — lets the entry age out. The CAS then reports
//     `wrong last sequence: 0`: the subject holds no message at all. Nobody
//     else holds the lease either, because there is nothing to hold. Treating
//     that as "lost" resigned leadership over an empty key and then waited a
//     full standby poll before even LOOKING at it (#559). The right move is to
//     race the standbys for it immediately: Create is atomic, so exactly one
//     instance wins and split-brain is impossible.
//   - Someone else holds it. Then leadership really is lost, and the standby
//     path is correct.
//   - WE hold it, at a revision this instance did not record — the update
//     landed but its acknowledgement did not come back. Adopting the store's
//     revision is the whole repair; resigning would have been a flap over a
//     lease that was never in doubt.
//
// Any other error leaves the question unanswered, and an unanswered question
// about who is leader is answered by standing down.
func (le *LeaderElection) reclaimLease(ctx context.Context, payload []byte, cause error) bool {
	entry, err := le.kv.Get(ctx, leaderKey)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		rev, cerr := le.kv.Create(ctx, leaderKey, payload)
		if cerr != nil {
			le.logger.Warn("leader lease expired and another coordinator took it",
				"id", le.id, "cause", cause, "error", cerr)
			return false
		}
		le.rev = rev
		le.leaseReclaims.Add(1)
		le.logger.Warn("leader lease had expired; reacquired it without standing down",
			"id", le.id, "cause", cause)
		return true
	}
	if err != nil {
		le.logger.Warn("leader lease refresh failed and the holder could not be read",
			"id", le.id, "cause", cause, "error", err)
		return false
	}

	var p leaderPayload
	if uerr := json.Unmarshal(entry.Value(), &p); uerr != nil {
		le.logger.Warn("leader key is unreadable, standing down",
			"id", le.id, "error", uerr)
		return false
	}
	if p.ID != le.id {
		le.logger.Warn("leader lease is held by another coordinator",
			"id", le.id, "holder", p.ID, "cause", cause)
		return false
	}
	le.rev = entry.Revision()
	le.leaseReclaims.Add(1)
	le.logger.Warn("leader lease is still ours at a revision we had not recorded; adopting it",
		"id", le.id, "revision", entry.Revision(), "cause", cause)
	return true
}

// LeaseReclaims reports how many times a failed refresh was resolved by
// consulting the store instead of standing down. Exposed so a gate can tell
// "the refresh simply succeeded" from "the recovery path ran and held", which
// the leadership flag alone cannot.
func (le *LeaderElection) LeaseReclaims() int64 { return le.leaseReclaims.Load() }

// runStandby polls for leader key expiry and attempts acquisition.
// Returns true if leadership was acquired (should transition to leader),
// false if the context was cancelled.
func (le *LeaderElection) runStandby(ctx context.Context) (acquired bool) {
	le.logger.Info("entering standby mode", "id", le.id)

	ticker := time.NewTicker(standbyPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			// Check if the leader key still exists
			_, err := le.kv.Get(ctx, leaderKey)
			if err != nil {
				// Key expired or doesn't exist — try to acquire
				le.logger.Info("leader key expired, attempting acquisition", "id", le.id)
				if le.tryAcquire(ctx) {
					return true
				}
				// Another standby got it first — keep polling
				le.logger.Debug("acquisition failed, another coordinator took over", "id", le.id)
			}
		}
	}
}

// CurrentLeader returns the ID of the current leader, or empty string if none.
func (le *LeaderElection) CurrentLeader(ctx context.Context) string {
	entry, err := le.kv.Get(ctx, leaderKey)
	if err != nil {
		return ""
	}
	var p leaderPayload
	if err := json.Unmarshal(entry.Value(), &p); err != nil {
		return ""
	}
	return p.ID
}
