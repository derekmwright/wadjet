// SPDX-License-Identifier: MIT

package cli

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// The embedded server, end to end: `wadjet serve` answers the PostgreSQL
// wire protocol from the engine in its own process.
//
// It is the binary's whole reason to exist — pgwire over a wadjet.DB with no
// coordinator, no worker and no task queues — so the gate is the thing a
// client does: connect, write, read back, and find the write still there
// after the server is signalled and a later command opens the same catalog.
//
// The declared type is asserted alongside the value. A server that answers
// the right rows under OID 25 (text) for every column is a server BI clients
// misread, and the routed path and this one have disagreed about exactly
// that before (#396, #305) — which is why pgwire derives its metas from the
// result's declared schema on both.
func TestTheEmbeddedServerAnswersOnTheWire(t *testing.T) {
	bin := e2eBin(t)
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	pgPort := freePort(t)

	serve := exec.Command(bin,
		"--storage-type=file", "--data-dir="+dataDir, "--bucket=wadjet",
		"--nats-port="+fmt.Sprint(freePort(t)),
		"serve", "--pg-addr=127.0.0.1:"+fmt.Sprint(pgPort))
	serve.Env = append(os.Environ(), "HOME="+filepath.Join(root, "home"))
	out, err := serve.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	serve.Stderr = serve.Stdout
	if err := serve.Start(); err != nil {
		t.Fatalf("starting the embedded server: %v", err)
	}
	stopped := false
	defer func() {
		if !stopped {
			serve.Process.Kill()
			serve.Wait()
		}
	}()
	go func() {
		buf := make([]byte, 4096)
		for {
			if _, err := out.Read(buf); err != nil {
				return
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	dsn := fmt.Sprintf("postgres://wadjet@127.0.0.1:%d/wadjet?sslmode=disable", pgPort)
	var conn *pgx.Conn
	deadline := time.Now().Add(60 * time.Second)
	for {
		c, dialErr := pgx.Connect(ctx, dsn)
		if dialErr == nil {
			conn = c
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the embedded server never accepted a connection on %d: %v", pgPort, dialErr)
		}
		time.Sleep(200 * time.Millisecond)
	}
	defer conn.Close(context.Background())

	if _, err := conn.Exec(ctx, "CREATE TABLE embedded_probe (id BIGINT, note VARCHAR)"); err != nil {
		t.Fatalf("CREATE TABLE over the wire: %v", err)
	}
	if _, err := conn.Exec(ctx, "INSERT INTO embedded_probe VALUES (7, 'seven'), (8, 'eight')"); err != nil {
		t.Fatalf("INSERT over the wire: %v", err)
	}

	rows, err := conn.Query(ctx, "SELECT id, note FROM embedded_probe ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT over the wire: %v", err)
	}
	// The RowDescription, before the rows: a value under the wrong OID is a
	// wrong answer to a typed client.
	fields := rows.FieldDescriptions()
	if len(fields) != 2 {
		rows.Close()
		t.Fatalf("RowDescription carries %d fields, want 2", len(fields))
	}
	const oidInt8, oidText = 20, 25
	if fields[0].DataTypeOID != oidInt8 {
		t.Errorf("id declared OID %d, want %d (bigint)", fields[0].DataTypeOID, oidInt8)
	}
	if fields[1].DataTypeOID != oidText {
		t.Errorf("note declared OID %d, want %d (text)", fields[1].DataTypeOID, oidText)
	}
	var got []string
	for rows.Next() {
		var id int64
		var note string
		if err := rows.Scan(&id, &note); err != nil {
			rows.Close()
			t.Fatalf("scanning: %v", err)
		}
		got = append(got, fmt.Sprintf("%d=%s", id, note))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("reading rows: %v", err)
	}
	if want := "7=seven,8=eight"; strings.Join(got, ",") != want {
		t.Fatalf("the embedded server answered %q, want %q", strings.Join(got, ","), want)
	}
	conn.Close(context.Background())

	// A signalled server shuts down rather than being killed by the signal,
	// and what it wrote is on disk for the next process.
	if err := serve.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signalling the server: %v", err)
	}
	waited := make(chan error, 1)
	go func() { waited <- serve.Wait() }()
	select {
	case <-waited:
		stopped = true
	case <-time.After(30 * time.Second):
		t.Fatal("the embedded server did not exit within 30s of SIGTERM")
	}

	listed, err := e2eRunIn(t, bin, root, dataDir, "tables")
	if err != nil {
		t.Fatalf("`tables` after the server exited: %v\n%s", err, listed)
	}
	if !strings.Contains(listed, "embedded_probe") {
		t.Fatalf("the table the embedded server created is not in the catalog it left behind:\n%s", listed)
	}
}

// freePort returns a port nothing is listening on. Racy by nature — the
// listener is closed before the server binds — but the alternative is a
// hard-coded port, which collides with whatever a developer is running.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
