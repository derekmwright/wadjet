package coordinator

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
)

func setupNATS(t *testing.T) (*distributed.EmbeddedNATS, *context.CancelFunc) {
	t.Helper()
	cfg := distributed.DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	en, err := distributed.NewEmbeddedNATS(cfg, logger)
	if err != nil {
		t.Fatalf("NewEmbeddedNATS: %v", err)
	}
	t.Cleanup(en.Shutdown)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatalf("ConnectInProcess: %v", err)
	}
	t.Cleanup(func() { nc.Close() })

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatalf("NewJetStream: %v", err)
	}

	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatalf("SetupStreams: %v", err)
	}
	_ = ctx
	return en, &cancel
}

func TestNewSchedulerNilLogger(t *testing.T) {
	cfg := distributed.DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()

	en, err := distributed.NewEmbeddedNATS(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer en.Shutdown()

	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	s := NewScheduler(nc, nil)
	if s == nil {
		t.Fatal("expected non-nil scheduler")
	}
}

func TestSchedulerPublishTasksEmpty(t *testing.T) {
	cfg := distributed.DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()

	en, err := distributed.NewEmbeddedNATS(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer en.Shutdown()

	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	s := NewScheduler(nc, nil)
	err = s.PublishTasks(context.Background(), nil)
	if err != nil {
		t.Fatalf("PublishTasks with nil tasks: %v", err)
	}
}

func TestSchedulerPublishTasks(t *testing.T) {
	cfg := distributed.DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	en, err := distributed.NewEmbeddedNATS(cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer en.Shutdown()

	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatal(err)
	}

	s := NewScheduler(nc, logger)

	tasks := []distributed.Task{
		{
			ID:           "t1",
			QueryID:      "q1",
			StageID:      "s0",
			Type:         distributed.TaskType("scan"),
			ResultBucket: "results",
			CreatedAt:    time.Now(),
		},
		{
			ID:           "t2",
			QueryID:      "q1",
			StageID:      "s0",
			Type:         distributed.TaskType("aggregate"),
			ResultBucket: "results",
			CreatedAt:    time.Now(),
		},
	}

	err = s.PublishTasks(ctx, tasks)
	if err != nil {
		t.Fatalf("PublishTasks: %v", err)
	}
}

func TestSchedulerPublishTasksWithClusterID(t *testing.T) {
	cfg := distributed.DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	en, err := distributed.NewEmbeddedNATS(cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer en.Shutdown()

	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Need to create stream that accepts cluster-scoped subjects
	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatal(err)
	}

	s := NewScheduler(nc, logger)

	tasks := []distributed.Task{
		{
			ID:           "t1",
			QueryID:      "q1",
			StageID:      "s0",
			Type:         distributed.TaskType("scan"),
			ClusterID:    "afb-east",
			ResultBucket: "results",
			CreatedAt:    time.Now(),
		},
	}

	err = s.PublishTasks(ctx, tasks)
	if err != nil {
		t.Fatalf("PublishTasks with cluster: %v", err)
	}
}
