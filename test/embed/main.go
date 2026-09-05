// Command embedcheck is docs/getting-started.md's "Your First Table (Embedded
// Go)" program, in a module that is NOT github.com/derekmwright/wadjet.
//
// It exists to be COMPILED. `go get github.com/derekmwright/wadjet/wadjet`
// used to succeed and then fail at the first line of this program, because
// every type the guide named lived under internal/ (#805). Nothing here may
// import github.com/derekmwright/wadjet/internal/... — Go refuses to build it
// if anything tries, which is exactly the property under test.
//
// It uses the in-memory store rather than the guide's ./wadjet-data directory
// so it leaves nothing behind; the guide's own line is one call away
// (wadjet.NewFileStore) and is exercised below too.
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

	// Local disk needs no server; NewS3Store points at an S3-compatible store
	// instead, and the rest of this program is unchanged either way.
	dir, err := os.MkdirTemp("", "embedcheck")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	if _, err := wadjet.NewFileStore(dir); err != nil {
		log.Fatal(err)
	}
	store := wadjet.NewMemStore()

	db, err := wadjet.Open(ctx, wadjet.Config{
		Store:  store,
		Bucket: "wadjet",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

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
		"date":      now,
	}})
	if err != nil {
		log.Fatal(err)
	}
	if err := ingester.FlushAll(ctx); err != nil {
		log.Fatal(err)
	}
	ingester.Stop(ctx)

	result, err := db.Query(ctx,
		"SELECT src_ip, dst_ip, bytes_in, amount FROM flow_logs LIMIT 10")
	if err != nil {
		log.Fatal(err)
	}
	for _, row := range result.Rows {
		fmt.Printf("%v %v %v %v\n", row["src_ip"], row["dst_ip"], row["bytes_in"], row["amount"])
	}

	tables, err := db.ListTables(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("tables:", tables)

	// The defaults, for a caller that wants to change one of them.
	fmt.Println("default flush rows:", wadjet.DefaultIngestConfig().MaxBufferRows)
}
