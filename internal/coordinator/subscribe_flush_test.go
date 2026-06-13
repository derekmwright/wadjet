package coordinator

import (
	"log/slog"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/citc-tech/wadjet/internal/distributed"
)

// Regression test for issue #143: result-subject subscriptions were
// installed with bare nc.Subscribe — the interest only reaches the NATS
// server on the next flush, so a worker completing a task faster than the
// coordinator's background flusher could publish its result to a subject
// with no registered subscriber. The result was dropped, the task was too
// fast to ever enter heartbeat liveness, and the stage idled out (the
// TestTPCHNativeDAG_SF01 Q04 hang under -race). subscribeTaskResults must
// flush before returning, so a publish from ANOTHER connection issued
// immediately after it returns is always delivered.
func TestSubscribeTaskResults_InterestRegisteredBeforeReturn(t *testing.T) {
	logger := slog.Default()
	natsCfg := distributed.DefaultNATSConfig()
	natsCfg.Port = -1
	natsCfg.StoreDir = t.TempDir()
	srv, err := distributed.NewEmbeddedNATS(natsCfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)

	coordConn, err := distributed.ConnectInProcess(srv.Server())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(coordConn.Close)
	workerConn, err := distributed.ConnectInProcess(srv.Server())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(workerConn.Close)

	c := &Coordinator{nc: coordConn, logger: logger}

	// Tight loop: subscribe-then-immediately-publish from the other
	// connection. Without the flush inside subscribeTaskResults this
	// drops messages whenever the publish beats interest propagation
	// (reliably, under -race scheduling).
	const rounds = 50
	for i := 0; i < rounds; i++ {
		subject := distributed.QueryResultSubject(t.Name() + string(rune('a'+i%26)))
		got := make(chan struct{}, 1)
		sub, err := c.subscribeTaskResults(subject, func(_ *nats.Msg) {
			select {
			case got <- struct{}{}:
			default:
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		// Worker-side publish the instant the coordinator's subscribe
		// returns — the lost-result window this test pins shut.
		if err := workerConn.Publish(subject, []byte("r")); err != nil {
			t.Fatal(err)
		}
		if err := workerConn.Flush(); err != nil {
			t.Fatal(err)
		}
		select {
		case <-got:
		case <-time.After(5 * time.Second):
			t.Fatalf("round %d: result published immediately after subscribeTaskResults was dropped — interest not flushed", i)
		}
		sub.Unsubscribe()
	}
}
