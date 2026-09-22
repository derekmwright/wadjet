// SPDX-License-Identifier: MIT

// Command embedcheck is docs/getting-started.md's "Your First Table (Embedded
// Go)" program, in a module that is NOT github.com/derekmwright/wadjet.
//
// It exists to be COMPILED. `go get github.com/derekmwright/wadjet/wadjet`
// used to succeed and then fail at the first line of this program, because
// every type the guide named lived under internal/ (#805). Nothing here may
// import github.com/derekmwright/wadjet/internal/... — Go refuses to build it
// if anything tries, which is exactly the property under test.
//
// It is also the guide's RESTART: the program opens its data directory,
// writes a table, closes, and opens the same directory again — the second
// Open has nothing but the directory, and the table is there (#1255). It
// uses a temporary directory rather than the guide's ./wadjet-data so it
// leaves nothing behind.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/derekmwright/wadjet/wadjet"
)

func main() {
	ctx := context.Background()

	dir, err := os.MkdirTemp("", "embedcheck")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// One local directory holds the data AND the catalog, so the tables
	// survive a restart. wadjet.NewS3Store + Config.CatalogDir is the same
	// program over an S3-compatible store.
	db, err := wadjet.Open(ctx, wadjet.Config{DataDir: dir})
	if err != nil {
		log.Fatal(err)
	}

	// A schema for network flow logs. Every type the engine has is nameable
	// from here; these are the ones the guide uses.
	schema := wadjet.Schema{
		Columns: []wadjet.Column{
			{Name: "timestamp", Type: wadjet.TypeTimestamp},
			{Name: "src_ip", Type: wadjet.TypeIPv4},
			{Name: "dst_ip", Type: wadjet.TypeIPv4},
			{Name: "src_port", Type: wadjet.TypeInt32},
			{Name: "dst_port", Type: wadjet.TypeInt32},
			{Name: "protocol", Type: wadjet.TypeString},
			{Name: "bytes_in", Type: wadjet.TypeInt64},
			{Name: "bytes_out", Type: wadjet.TypeInt64},
			{Name: "amount", Type: wadjet.TypeDecimal, Precision: 9, Scale: 2},
			{Name: "date", Type: wadjet.TypeDate},
		},
	}

	if err := db.CreateTable(ctx, "flow_logs", schema, []string{"date"}); err != nil {
		log.Fatal(err)
	}

	ingester := db.NewIngester("flow_logs", schema, []string{"date"}, wadjet.IngestConfig{
		FlushInterval: 10 * time.Second,
		MaxBufferRows: 100000,
	})
	ingester.Start()

	now := time.Now()
	err = ingester.Ingest(ctx, []map[string]any{{
		"timestamp": now,
		"src_ip":    "10.0.1.50",
		"dst_ip":    "10.0.2.100",
		"src_port":  int32(54321),
		"dst_port":  int32(443),
		"protocol":  "TCP",
		"bytes_in":  int64(2048),
		"bytes_out": int64(512),
		"amount":    "12.34",
		"date":      now.Format("2006-01-02"),
	}})
	if err != nil {
		log.Fatal(err)
	}
	if err := ingester.Stop(ctx); err != nil {
		log.Fatal(err)
	}

	// Close releases the directory; the "restart" is the next Open of it.
	db.Close()
	db, err = wadjet.Open(ctx, wadjet.Config{DataDir: dir})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	result, err := db.Query(ctx, "SELECT src_ip, bytes_in, amount FROM flow_logs")
	if err != nil {
		log.Fatal(err)
	}
	for _, row := range result.Rows {
		fmt.Println(row["src_ip"], row["bytes_in"], row["amount"])
	}

	tables, err := db.ListTables(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("tables after the restart:", tables)
}
