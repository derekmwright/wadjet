package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// A cancel is a terminal exit, and every terminal exit reclaims the same
// things (ADR-0028).
//
// Coordinator.CancelQuery published CancelSubject, marked the tracker and
// returned. It never called cleanupQuery — the single site that deletes
// queries/<id>/*, drops the peer-location hints, purges the NATS-KV result
// keys and publishes CompleteSubject. Every other terminal path (stage
// failure, normal completion, the async watchdog, the native DAG's defer)
// went through cleanupQuery; the user-facing cancel API was the one that
// did not, so a cancelled query's stage prefix survived to the 1-hour TTL
// sweep, and the worker never got the message that lets it drop its
// ResultStore entry (#817, and #818's trigger).
func TestCancelQueryRunsTheSameCleanupAsACompletion(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS server")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	infra := tmdInfra(t, ctx)
	coord := New(Config{NATSUrl: infra.clientURL, ResultBucket: "test"},
		infra.cat, infra.nc, infra.js, infra.logger)
	coord.Cleaner(infra.store, "test")

	// The message a worker needs in order to drop its ResultStore entry.
	// A cancel published only CancelSubject, and the worker's CANCEL
	// handler deliberately frees four caches and not that one — so without
	// this message a cancelled query's result bytes were resident for the
	// worker's whole process lifetime (#818).
	completed := make(chan string, 4)
	sub, err := infra.nc.Subscribe(distributed.CompleteSubjectAll(), func(m *nats.Msg) {
		select {
		case completed <- string(m.Data):
		default:
		}
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	t.Cleanup(func() { sub.Unsubscribe() })

	const queryID = "cancel-reclaims-1"
	stages := map[string]*StageInfo{"scan-0": {StageID: "scan-0", Type: "scan"}}
	coord.tracker.Register(queryID, "SELECT 1", stages, []string{"scan-0"})
	coord.tracker.Start(queryID)

	// The stage output a cancelled query leaves behind.
	prefix := fmt.Sprintf("queries/%s/scan-0/", queryID)
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("%stask-%04d.wshf", prefix, i)
		payload := bytes.Repeat([]byte("x"), 1024)
		if _, err := infra.store.Put(ctx, "test", key, bytes.NewReader(payload), int64(len(payload)), ""); err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
	}

	if err := coord.CancelQuery(queryID); err != nil {
		t.Fatalf("CancelQuery: %v", err)
	}

	select {
	case got := <-completed:
		if got != queryID {
			t.Fatalf("CompleteSubject carried %q, want %q", got, queryID)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a cancel never published CompleteSubject; the worker's ResultStore " +
			"entry for this query is only ever freed by that message (#818)")
	}

	// cleanupQuery deletes asynchronously (it must not block the caller),
	// so the assertion is over a bounded settle, not an instant.
	deadline := time.Now().Add(20 * time.Second)
	var left []objstore.ObjectInfo
	for time.Now().Before(deadline) {
		left, err = infra.store.List(ctx, "test", objstore.ListOptions{Prefix: fmt.Sprintf("queries/%s/", queryID)})
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		if len(left) == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("a CANCELLED query left %d objects under queries/%s/ after a 20s settle; "+
		"a cancel must run the same reclamation as a completion (ADR-0028)", len(left), queryID)
}
