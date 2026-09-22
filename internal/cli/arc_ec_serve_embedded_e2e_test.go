// SPDX-License-Identifier: MIT

package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/derekmwright/wadjet/wadjet"
)

// Arc EC (#1255): an embedded program and `wadjet serve` over ONE data
// directory hold one set of tables, because wadjet.Config.DataDir opens the
// directory exactly the way `--storage-type=file --data-dir` does — the
// same file store, bucket, catalog directory, lock and KV bucket.
//
// Sequentially: the program creates and fills a table and closes; the
// server answers its rows on the wire and creates a second table; the
// program reopens and sees both. And the two never hold the directory at
// once: a `serve` started while the program holds it is refused, naming the
// lock, and a program opened while the server holds it gets ErrCatalogHeld.
//
// The CLI commands are the third door, and they DIAL a holder rather than
// refuse: `wadjet tables --data-dir=D` beside the running program lists the
// program's table live, through the URL the program published in the lock
// file.
func TestAnEmbeddedProgramAndServeShareOneDataDir(t *testing.T) {
	bin := e2eBin(t)
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	home := filepath.Join(root, "home")
	if err := os.MkdirAll(home, 0o755); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	schema := wadjet.Schema{Columns: []wadjet.Column{
		{Name: "id", Type: wadjet.TypeInt64},
		{Name: "ip", Type: wadjet.TypeIPv4},
	}}

	// 1. The program's first life.
	db, err := wadjet.Open(ctx, wadjet.Config{DataDir: dataDir})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateTable(ctx, "from_program", schema, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, "INSERT INTO from_program VALUES (1, '10.0.0.1'), (2, '10.0.0.2')"); err != nil {
		t.Fatal(err)
	}

	// 1a. A CLI command beside the running program reaches ITS catalog.
	out, err := e2eRun(t, bin, root, "tables")
	if err != nil || !strings.Contains(out, "from_program") {
		t.Fatalf("`wadjet tables` beside the running program: %v\n%s", err, out)
	}

	// 1b. A `serve` beside the running program is refused, naming the lock.
	refused := exec.Command(bin, "--storage-type=file", "--data-dir="+dataDir, "--bucket=wadjet",
		"--nats-port="+fmt.Sprint(freePort(t)), "serve", "--pg-addr=127.0.0.1:"+fmt.Sprint(freePort(t)))
	refused.Env = append(os.Environ(), "HOME="+home)
	rout, rerr := refused.CombinedOutput()
	if rerr == nil {
		t.Fatalf("`wadjet serve` started over a directory an embedded program holds:\n%s", rout)
	}
	// One refusal on every door: serve names the holder's pid exactly as
	// wadjet.ErrCatalogHeld does (review B4).
	if !strings.Contains(string(rout), fmt.Sprintf("held by process %d", os.Getpid())) {
		t.Fatalf("the serve refusal does not name the holder's pid %d:\n%s", os.Getpid(), rout)
	}
	db.Close()

	// 2. The server's life over the same directory.
	pgPort := freePort(t)
	serve := exec.Command(bin, "--storage-type=file", "--data-dir="+dataDir, "--bucket=wadjet",
		"--nats-port="+fmt.Sprint(freePort(t)), "serve", "--pg-addr=127.0.0.1:"+fmt.Sprint(pgPort))
	serve.Env = append(os.Environ(), "HOME="+home)
	serve.Stdout, serve.Stderr = nil, nil
	if err := serve.Start(); err != nil {
		t.Fatal(err)
	}
	stopped := false
	defer func() {
		if !stopped {
			serve.Process.Kill()
			serve.Wait()
		}
	}()
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
			t.Fatalf("serve never accepted a connection: %v", dialErr)
		}
		time.Sleep(200 * time.Millisecond)
	}

	rows, err := conn.Query(ctx, "SELECT id, ip::text FROM from_program ORDER BY id")
	if err != nil {
		t.Fatalf("serve does not see the program's table: %v", err)
	}
	var got []string
	for rows.Next() {
		var id int64
		var ip string
		if err := rows.Scan(&id, &ip); err != nil {
			t.Fatal(err)
		}
		got = append(got, fmt.Sprintf("%d=%s", id, ip))
	}
	rows.Close()
	if want := "1=10.0.0.1,2=10.0.0.2"; strings.Join(got, ",") != want {
		t.Fatalf("serve answered %q for the program's rows, want %q", strings.Join(got, ","), want)
	}
	if _, err := conn.Exec(ctx, "CREATE TABLE from_serve (id BIGINT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "INSERT INTO from_serve VALUES (7)"); err != nil {
		t.Fatal(err)
	}
	conn.Close(ctx)

	// 2a. The program cannot open the directory while the server holds it.
	if held, err := wadjet.Open(ctx, wadjet.Config{DataDir: dataDir}); err == nil {
		held.Close()
		t.Fatal("wadjet.Open succeeded over a directory `serve` holds")
	} else if !errors.Is(err, wadjet.ErrCatalogHeld) || !strings.Contains(err.Error(), fmt.Sprintf("process %d", serve.Process.Pid)) {
		t.Fatalf("the refusal is not ErrCatalogHeld naming serve's pid %d: %v", serve.Process.Pid, err)
	}

	if err := serve.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	waited := make(chan error, 1)
	go func() { waited <- serve.Wait() }()
	select {
	case <-waited:
		stopped = true
	case <-time.After(30 * time.Second):
		t.Fatal("serve did not stop on SIGTERM")
	}

	// 3. The program's second life sees both tables.
	db, err = wadjet.Open(ctx, wadjet.Config{DataDir: dataDir})
	if err != nil {
		t.Fatalf("reopening after serve released the directory: %v", err)
	}
	defer db.Close()
	names, err := db.ListTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(names, ",") != "from_program,from_serve" {
		t.Fatalf("after serve: ListTables = %v, want [from_program from_serve]", names)
	}
	res, err := db.Query(ctx, "SELECT (SELECT count(*) FROM from_program) AS p, (SELECT sum(id) FROM from_serve) AS s")
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprintf("%v/%v", res.Rows[0]["p"], res.Rows[0]["s"]); got != "2/7" {
		t.Fatalf("after serve: program reads %s, want 2/7", got)
	}
}
