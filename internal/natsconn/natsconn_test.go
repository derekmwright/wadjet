// SPDX-License-Identifier: MIT

package natsconn

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// recordHandler captures slog records with their attributes.
type recordHandler struct {
	mu      sync.Mutex
	records []map[string]string
}

func (h *recordHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *recordHandler) Handle(_ context.Context, r slog.Record) error {
	m := map[string]string{"msg": r.Message, "level": r.Level.String()}
	r.Attrs(func(a slog.Attr) bool {
		m[a.Key] = a.Value.String()
		return true
	})
	h.mu.Lock()
	h.records = append(h.records, m)
	h.mu.Unlock()
	return nil
}
func (h *recordHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordHandler) WithGroup(string) slog.Handler      { return h }

func (h *recordHandler) matching(msg string) []map[string]string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []map[string]string
	for _, r := range h.records {
		if r["msg"] == msg {
			out = append(out, r)
		}
	}
	return out
}

// captureDefaultLogger routes slog's default logger into a recordHandler for
// the test's duration. The handler under test logs through the default.
func captureDefaultLogger(t *testing.T) *recordHandler {
	t.Helper()
	h := &recordHandler{}
	prev := slog.Default()
	slog.SetDefault(slog.New(h))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return h
}

func waitRecords(t *testing.T, h *recordHandler, msg string) []map[string]string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if rs := h.matching(msg); len(rs) > 0 {
			return rs
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no %q log record within 10s", msg)
	return nil
}

func startEmbedded(t *testing.T) *EmbeddedNATS {
	t.Helper()
	cfg := DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()
	en, err := NewEmbeddedNATS(cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("embedded NATS: %v", err)
	}
	t.Cleanup(en.Shutdown)
	return en
}

// TestSlowConsumerIsLoggedWithSubjectAndDroppedCount forces one slow-consumer
// episode on a test subscription (a channel subscription whose one-slot
// channel is never read) on both connection constructors, and checks that
// the episode is logged once, at Error level, with the subscription's subject,
// a dropped count and the connection id. The burst drops 4 of 5 messages but
// is ONE episode: the line is per episode, not per message (#857e).
func TestSlowConsumerIsLoggedWithSubjectAndDroppedCount(t *testing.T) {
	for _, tc := range []struct {
		name    string
		connect func(en *EmbeddedNATS) (*nats.Conn, error)
	}{
		{"tcp", func(en *EmbeddedNATS) (*nats.Conn, error) { return Connect(en.ClientURL(), nil) }},
		{"in-process", func(en *EmbeddedNATS) (*nats.Conn, error) { return ConnectInProcess(en.Server()) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureDefaultLogger(t)
			en := startEmbedded(t)
			nc, err := tc.connect(en)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(nc.Close)
			pub, err := Connect(en.ClientURL(), nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(pub.Close)

			const subject = "test.slow"
			sub, err := nc.ChanSubscribe(subject, make(chan *nats.Msg, 1))
			if err != nil {
				t.Fatal(err)
			}
			if err := nc.Flush(); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 5; i++ {
				if err := pub.Publish(subject, []byte(fmt.Sprint(i))); err != nil {
					t.Fatal(err)
				}
			}
			if err := pub.Flush(); err != nil {
				t.Fatal(err)
			}
			rs := waitRecords(t, logs, "NATS slow consumer: messages dropped client-side")
			r := rs[0]
			if r["subject"] != subject {
				t.Errorf("subject = %q, want %q (record %v)", r["subject"], subject, r)
			}
			if r["level"] != "ERROR" {
				t.Errorf("level = %q, want ERROR", r["level"])
			}
			if d := r["dropped_total"]; d == "" || d == "0" {
				t.Errorf("dropped_total = %q, want a positive count (record %v)", d, r)
			}
			if r["connection_id"] == "" || r["connection_id"] == "0" {
				t.Errorf("connection_id = %q, want the server-assigned id", r["connection_id"])
			}
			if !strings.Contains(r["error"], "slow consumer") {
				t.Errorf("error = %q", r["error"])
			}
			for _, k := range []string{"reported", "dropped_counts", "attribution"} {
				if r[k] == "" {
					t.Errorf("limit attribute %q missing (record %v)", k, r)
				}
			}
			// Every message after the first was dropped, in one episode.
			if dropped, _ := sub.Dropped(); dropped != 4 {
				t.Errorf("subscription dropped %d, want 4", dropped)
			}
			time.Sleep(200 * time.Millisecond)
			if n := len(logs.matching("NATS slow consumer: messages dropped client-side")); n != 1 {
				t.Errorf("%d slow-consumer lines for one episode, want 1", n)
			}
		})
	}
}

// TestOtherAsyncErrorsAreStillReported: installing a handler replaces
// nats.go's default, which writes EVERY asynchronous error to stderr. A
// non-slow-consumer asynchronous error (a publish permissions violation the
// server reports with -ERR) must still be reported, with the error text and
// the connection id.
func TestOtherAsyncErrorsAreStillReported(t *testing.T) {
	logs := captureDefaultLogger(t)
	ns, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
		Users: []*natsserver.User{{
			Username: "u", Password: "p",
			Permissions: &natsserver.Permissions{Publish: &natsserver.SubjectPermission{Deny: []string{"forbidden.>"}}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	ns.Start()
	t.Cleanup(ns.Shutdown)
	if !ns.ReadyForConnections(10 * time.Second) {
		t.Fatal("server not ready")
	}
	url := strings.Replace(ns.ClientURL(), "nats://", "nats://u:p@", 1)
	nc, err := Connect(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	if err := nc.Publish("forbidden.x", []byte("x")); err != nil {
		t.Fatal(err)
	}
	_ = nc.Flush()
	rs := waitRecords(t, logs, "NATS asynchronous error")
	r := rs[0]
	if !strings.Contains(strings.ToLower(r["error"]), "permissions violation") {
		t.Errorf("error = %q, want the permissions violation", r["error"])
	}
	if r["level"] != "ERROR" {
		t.Errorf("level = %q, want ERROR", r["level"])
	}
	if r["connection_id"] == "" || r["connection_id"] == "0" {
		t.Errorf("connection_id = %q, want the server-assigned id", r["connection_id"])
	}
	if len(logs.matching("NATS slow consumer: messages dropped client-side")) != 0 {
		t.Error("a permissions violation was logged as a slow consumer")
	}
}
