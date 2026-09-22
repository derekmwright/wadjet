// SPDX-License-Identifier: MIT

package catalogdir

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/derekmwright/wadjet/internal/natsconn"
)

// The lock is the whole point of the package: one holder per directory, the
// holder readable by a loser, and a released or dead holder's file not a
// lock. Open/OpenLocked are exercised end to end by wadjet's restart gates
// and internal/cli's e2e gates; this is the seam under them.

func TestOneHolderPerDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "_catalog")
	l, err := TakeLock(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := ReadHolder(dir); ok {
		t.Fatal("a fresh lock publishes a holder before Publish")
	}
	if err := l.Publish("nats://127.0.0.1:41234"); err != nil {
		t.Fatal(err)
	}
	h, ok := ReadHolder(dir)
	if !ok || h.PID != os.Getpid() || h.URL != "nats://127.0.0.1:41234" {
		t.Fatalf("ReadHolder = %+v, %v", h, ok)
	}
	if _, err := TakeLock(dir); err == nil {
		t.Fatal("a second TakeLock on a held directory succeeded")
	}
	l.Release()
	if _, ok := ReadHolder(dir); ok {
		t.Fatal("a released lock still publishes a holder")
	}
	l2, err := TakeLock(dir)
	if err != nil {
		t.Fatalf("TakeLock after Release: %v", err)
	}
	l2.Release()
}

func TestOpenRefusesAHeldDirectoryNamingTheHolder(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "_catalog")
	cfg := natsconn.DefaultNATSConfig()
	cfg.StoreDir = dir
	cfg.Port = -1
	h, err := Open(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if got, ok := ReadHolder(dir); !ok || got.URL != h.ClientURL() {
		t.Fatalf("the holder published %+v, want its own client URL %s", got, h.ClientURL())
	}
	_, err = Open(cfg, nil)
	if !errors.Is(err, ErrHeld) {
		t.Fatalf("second Open: %v, want ErrHeld", err)
	}
	h.Close()
	h.Close() // idempotent
	again, err := Open(cfg, nil)
	if err != nil {
		t.Fatalf("Open after Close: %v", err)
	}
	again.Close()
}
