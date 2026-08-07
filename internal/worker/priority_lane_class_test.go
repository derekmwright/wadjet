package worker

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/citc-tech/wadjet/internal/distributed"
)

// Probe: two non-overlapping filtered consumers on the pritasks WorkQueue
// stream must BOTH deliver. Pins the lane-split's core NATS assumption.
func TestPriLaneClassConsumersBothDeliver(t *testing.T) {
	cfg := distributed.NATSConfig{StoreDir: t.TempDir()}
	en, err := distributed.NewEmbeddedNATS(cfg, nil)
	if err != nil {
		t.Fatalf("embed: %v", err)
	}
	t.Cleanup(en.Shutdown)
	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatalf("setup: %v", err)
	}
	mk := func(name, filter string) jetstream.Consumer {
		c, err := js.CreateOrUpdateConsumer(ctx, distributed.StreamPriTasks, jetstream.ConsumerConfig{
			Durable: name, FilterSubject: filter,
			AckPolicy: jetstream.AckExplicitPolicy,
		})
		if err != nil {
			t.Fatalf("consumer %s: %v", name, err)
		}
		return c
	}
	leaf := mk("pritasks-leaf", "wadjet.pritasks.leaf.>")
	deep := mk("pritasks-deep", "wadjet.pritasks.deep.>")
	if _, err := js.Publish(ctx, "wadjet.pritasks.leaf.stage.q.s1", []byte("a")); err != nil {
		t.Fatalf("pub leaf: %v", err)
	}
	if _, err := js.Publish(ctx, "wadjet.pritasks.deep.stage.q.s2", []byte("b")); err != nil {
		t.Fatalf("pub deep: %v", err)
	}
	for name, c := range map[string]jetstream.Consumer{"leaf": leaf, "deep": deep} {
		fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		m, err := c.Next(jetstream.FetchMaxWait(4 * time.Second))
		cancel()
		_ = fctx
		if err != nil {
			t.Fatalf("%s lane delivered nothing: %v", name, err)
		}
		m.Ack()
	}
}

// Probe 2: the SCHEDULER path — a CORE NATS publish (nc.Publish, not
// js.Publish) to a class subject must be captured by the pri stream and
// delivered through the class consumer. Mimics coordinator
// scheduler.PublishTasks exactly.
func TestPriLaneCorePublishDelivers(t *testing.T) {
	en, err := distributed.NewEmbeddedNATS(distributed.NATSConfig{StoreDir: t.TempDir()}, nil)
	if err != nil {
		t.Fatalf("embed: %v", err)
	}
	t.Cleanup(en.Shutdown)
	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatalf("setup: %v", err)
	}
	leaf, err := js.CreateOrUpdateConsumer(ctx, distributed.StreamPriTasks, jetstream.ConsumerConfig{
		Durable: "pritasks-leaf", FilterSubject: "wadjet.pritasks.leaf.>",
		AckPolicy: jetstream.AckExplicitPolicy, AckWait: 10 * time.Minute, MaxDeliver: 3,
	})
	if err != nil {
		t.Fatalf("leaf consumer: %v", err)
	}
	if _, err := js.CreateOrUpdateConsumer(ctx, distributed.StreamPriTasks, jetstream.ConsumerConfig{
		Durable: "pritasks-deep", FilterSubject: "wadjet.pritasks.deep.>",
		AckPolicy: jetstream.AckExplicitPolicy, AckWait: 10 * time.Minute, MaxDeliver: 3,
	}); err != nil {
		t.Fatalf("deep consumer: %v", err)
	}
	subject := distributed.PriTaskSubject("leaf", "stage", "st-scan-3-abc123", "scan-3")
	if err := nc.Publish(subject, []byte("task-blob")); err != nil {
		t.Fatalf("core publish: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	m, err := leaf.Next(jetstream.FetchMaxWait(4 * time.Second))
	if err != nil {
		t.Fatalf("leaf lane did not deliver a core-published task: %v", err)
	}
	m.Ack()
}

// A non-cluster worker's class filter must ALSO match cluster-tagged
// subjects (cluster nests UNDER the class token) — the pre-split
// "wadjet.pritasks.>" filter had this property and TestDistributedTPCH/Q07
// hung when the split lost it.
func TestPriLaneClusterSubjectMatchesClasslessFilter(t *testing.T) {
	en, err := distributed.NewEmbeddedNATS(distributed.NATSConfig{StoreDir: t.TempDir()}, nil)
	if err != nil {
		t.Fatalf("embed: %v", err)
	}
	t.Cleanup(en.Shutdown)
	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatalf("setup: %v", err)
	}
	leaf, err := js.CreateOrUpdateConsumer(ctx, distributed.StreamPriTasks, jetstream.ConsumerConfig{
		Durable: "pritasks-leaf", FilterSubject: "wadjet.pritasks.leaf.>",
		AckPolicy: jetstream.AckExplicitPolicy, AckWait: 10 * time.Minute, MaxDeliver: 3,
	})
	if err != nil {
		t.Fatalf("leaf consumer: %v", err)
	}
	subject := distributed.ClusterPriTaskSubject("test-cluster", "leaf", "stage", "st-scan-3-x", "scan-3")
	if err := nc.Publish(subject, []byte("task-blob")); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	m, err := leaf.Next(jetstream.FetchMaxWait(4 * time.Second))
	if err != nil {
		t.Fatalf("cluster-tagged leaf task not delivered to classless-cluster filter: %v", err)
	}
	m.Ack()
}
