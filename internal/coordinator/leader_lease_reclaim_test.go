package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// errOnGetKV answers Get with a transport-shaped error while every other
// operation goes to the real store — the "the who-holds question cannot be
// answered" state reclaimLease must resolve by standing down.
type errOnGetKV struct {
	jetstream.KeyValue
	err error
}

func (k errOnGetKV) Get(_ context.Context, _ string) (jetstream.KeyValueEntry, error) {
	return nil, k.err
}

// A failed lease refresh is a QUESTION — the key is not at the revision this
// coordinator recorded — and standing down is only one of its three answers.
//
// #559 caught the wrong one under load: the bucket's TTL is 5s and the refresh
// ticks at 2s, so a tick the runtime delivered late let the entry age out, the
// CAS came back `wrong last sequence: 0` (the subject holds no message at all),
// and the coordinator resigned over a key NOBODY held. It then waited a whole
// standby poll before so much as looking at it. Filed as a flake because a busy
// machine is what makes a 2s tick miss a 5s deadline; the logic was simply
// wrong, and the load only decided how often it was reached.
//
// This is FORWARD-LOOKING, not a fixed outage: LeaderElection has no non-test
// caller (see its type doc), so no shipped server mode has ever run it. What is
// fixed is the logic the first mode to wire it up will inherit.
//
// The three answers are asserted here against a live embedded NATS, with no
// sleeps and no dependence on scheduling: the store is put into each state
// directly and refreshLease is called once. LeaseReclaims separates "the
// refresh simply succeeded" from "the recovery ran and held", which the
// leadership flag alone cannot.
//
// Reverting reclaimLease — returning false on any Update error — fails the
// Expired and OurEntryAtAnotherRevision cells on every run.
func TestFailedLeaseRefreshAsksWhoHoldsTheLeaseBeforeStandingDown(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	// newLeader stands up an election that HOLDS the lease, without the run
	// loop: refreshLease is called by hand so the test never races a ticker.
	newLeader := func(t *testing.T, js jetstream.JetStream, id string) *LeaderElection {
		t.Helper()
		le, err := NewLeaderElection(js, id, logger)
		if err != nil {
			t.Fatalf("creating leader election: %v", err)
		}
		return le
	}

	// dropTheKey removes the leader entry the way the bucket's TTL does —
	// a stream purge of the subject, leaving NO message and therefore a last
	// sequence of 0, which is the exact CAS rejection #559 reported. A KV
	// Purge would leave a tombstone at a nonzero sequence instead.
	dropTheKey := func(t *testing.T, ctx context.Context, js jetstream.JetStream) {
		t.Helper()
		st, err := js.Stream(ctx, "KV_"+leaderBucket)
		if err != nil {
			t.Fatalf("opening the leader KV stream: %v", err)
		}
		if err := st.Purge(ctx, jetstream.WithPurgeSubject("$KV."+leaderBucket+"."+leaderKey)); err != nil {
			t.Fatalf("purging the leader key: %v", err)
		}
	}

	t.Run("ExpiredAndUnclaimedIsReacquiredNotResigned", func(t *testing.T) {
		js := setupNATSJetStream(t)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		le := newLeader(t, js, "coord-1")
		if !le.tryAcquire(ctx) {
			t.Fatal("could not acquire the lease to begin with")
		}
		dropTheKey(t, ctx, js)

		if !le.refreshLease(ctx) {
			t.Fatal("the coordinator resigned over a lease NOBODY held: an expired key is not another coordinator")
		}
		if got := le.LeaseReclaims(); got != 1 {
			t.Fatalf("LeaseReclaims = %d, want 1 — the CAS must have failed and the recovery path must have run", got)
		}
		if holder := le.CurrentLeader(ctx); holder != "coord-1" {
			t.Fatalf("leader key names %q after reacquisition, want coord-1", holder)
		}
		// And the adopted revision must be usable: the NEXT refresh has to
		// succeed on its own, without another reclaim.
		if !le.refreshLease(ctx) {
			t.Fatal("the refresh after a reclaim failed: the adopted revision is wrong")
		}
		if got := le.LeaseReclaims(); got != 1 {
			t.Fatalf("LeaseReclaims = %d after a clean refresh, want 1", got)
		}
	})

	t.Run("ExpiredAndTakenIsResigned", func(t *testing.T) {
		js := setupNATSJetStream(t)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		le := newLeader(t, js, "coord-1")
		if !le.tryAcquire(ctx) {
			t.Fatal("could not acquire the lease to begin with")
		}
		dropTheKey(t, ctx, js)

		// A standby wins the empty key first.
		other := newLeader(t, js, "coord-2")
		if !other.tryAcquire(ctx) {
			t.Fatal("the standby could not take the expired key")
		}

		if le.refreshLease(ctx) {
			t.Fatal("the coordinator kept leadership while another coordinator holds the key — split brain")
		}
		if got := le.LeaseReclaims(); got != 0 {
			t.Fatalf("LeaseReclaims = %d, want 0 — nothing was reclaimed", got)
		}
		if holder := le.CurrentLeader(ctx); holder != "coord-2" {
			t.Fatalf("leader key names %q, want coord-2", holder)
		}
	})

	t.Run("OurEntryAtAnotherRevisionIsAdopted", func(t *testing.T) {
		js := setupNATSJetStream(t)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		le := newLeader(t, js, "coord-1")
		if !le.tryAcquire(ctx) {
			t.Fatal("could not acquire the lease to begin with")
		}
		// The update landed; its acknowledgement did not come back, so the
		// store is one revision ahead of what this instance recorded.
		payload, err := json.Marshal(leaderPayload{ID: "coord-1", Since: time.Now()})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		newRev, err := le.kv.Put(ctx, leaderKey, payload)
		if err != nil {
			t.Fatalf("put: %v", err)
		}
		if newRev == le.rev {
			t.Fatal("the fixture did not move the revision: this cell tests nothing")
		}

		if !le.refreshLease(ctx) {
			t.Fatal("the coordinator resigned a lease it still holds itself")
		}
		if got := le.LeaseReclaims(); got != 1 {
			t.Fatalf("LeaseReclaims = %d, want 1", got)
		}
		if !le.refreshLease(ctx) {
			t.Fatal("the refresh after adoption failed: the adopted revision is wrong")
		}
	})

	// The who-holds question is not always answerable. Both cells below are
	// branches of reclaimLease that stand down correctly today and are each one
	// `if` away from returning true in a future edit, which is the whole reason
	// they are gated rather than left to review.

	t.Run("UnanswerableWhoHoldsStandsDown", func(t *testing.T) {
		js := setupNATSJetStream(t)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		le := newLeader(t, js, "coord-1")
		if !le.tryAcquire(ctx) {
			t.Fatal("could not acquire the lease to begin with")
		}
		dropTheKey(t, ctx, js)
		// Every other operation still reaches the real store; only the
		// who-holds Get cannot be answered.
		le.kv = errOnGetKV{KeyValue: le.kv, err: errors.New("nats: connection closed")}

		if le.refreshLease(ctx) {
			t.Fatal("the coordinator kept leadership although the store could not say who holds the lease")
		}
		if got := le.LeaseReclaims(); got != 0 {
			t.Fatalf("LeaseReclaims = %d after an unanswerable question, want 0", got)
		}
	})

	t.Run("UnreadableLeaderPayloadStandsDown", func(t *testing.T) {
		js := setupNATSJetStream(t)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		le := newLeader(t, js, "coord-1")
		if !le.tryAcquire(ctx) {
			t.Fatal("could not acquire the lease to begin with")
		}
		// A key whose payload cannot be parsed says nothing about who holds it.
		if _, err := le.kv.Put(ctx, leaderKey, []byte("{not json")); err != nil {
			t.Fatalf("put: %v", err)
		}

		if le.refreshLease(ctx) {
			t.Fatal("the coordinator kept leadership over a key it cannot read")
		}
		if got := le.LeaseReclaims(); got != 0 {
			t.Fatalf("LeaseReclaims = %d, want 0", got)
		}
	})

	t.Run("AnotherCoordinatorAtOurRevisionIsResigned", func(t *testing.T) {
		js := setupNATSJetStream(t)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		le := newLeader(t, js, "coord-1")
		if !le.tryAcquire(ctx) {
			t.Fatal("could not acquire the lease to begin with")
		}
		payload, err := json.Marshal(leaderPayload{ID: "coord-2", Since: time.Now()})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if _, err := le.kv.Put(ctx, leaderKey, payload); err != nil {
			t.Fatalf("put: %v", err)
		}

		if le.refreshLease(ctx) {
			t.Fatal("the coordinator kept leadership over a key another coordinator wrote")
		}
		if got := le.LeaseReclaims(); got != 0 {
			t.Fatalf("LeaseReclaims = %d, want 0", got)
		}
	})
}
