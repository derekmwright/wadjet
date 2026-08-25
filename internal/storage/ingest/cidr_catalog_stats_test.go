package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"unicode/utf8"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// cidrStatsSchema is a two-column table whose CIDR column is the one under
// test; the id column keeps the schema from being degenerate.
var cidrStatsSchema = parquet.Schema{Columns: []parquet.Column{
	{Name: "id", Type: parquet.TypeInt64},
	{Name: "c_cidr", Type: parquet.TypeCIDR},
}}

const cidrStatsTable = "cidr_events"

// TestIngestPersistsCidrStatsAsText is the #523 follow-up regression: what
// reaches the CATALOG for a CIDR column must be the winning row's address
// TEXT, not the inet-order SORT KEY parquet.RowGroupStats compares on.
//
// RowGroupStats boxes a confirmed CIDR bound as parquet.CidrInetBound, whose
// Key is a BINARY string (family byte, masked address bytes, mask length,
// full address bytes). extractColumnStats copies MinValue/MaxValue straight
// into catalog.FileColumnStats, which is JSON-tagged and persisted in NATS
// KV — and encoding/json replaces every byte that is not valid UTF-8 with
// U+FFFD, irreversibly. A 10.x address keys to bytes 0x04 0x0a ... , so the
// persisted "min" for this table came back as a run of replacement
// characters: permanent bad metadata, written by the ordinary ingest path,
// with nothing that reads it well enough to notice.
//
// The file deliberately holds MORE THAN ONE ROW GROUP, because that is the
// second half of the same defect: merging row groups goes through
// parquet.CompareNative, which had no arm for the boxed type and answered 0
// for every pair, so the file's recorded bound was whichever row group came
// first rather than the file's true extreme.
func TestIngestPersistsCidrStatsAsText(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, testBucket)
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("catalog init: %v", err)
	}
	if err := cat.CreateTable(ctx, cidrStatsTable, cidrStatsSchema, nil); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Two row groups of four rows each. The true inet-order minimum
	// ("9.0.0.0/8") lives in the SECOND row group and the true maximum
	// ("192.168.188.190/24") in the first, so a merge that cannot compare
	// the two groups' bounds keeps the wrong pair. Text order would pick
	// "10.0.0.0/24" as the minimum ('1' < '9').
	cfg := DefaultConfig()
	cfg.RowGroupSize = 4
	ing := New(cat, cidrStatsTable, cidrStatsSchema, nil, cfg)
	vals := []string{
		"10.0.0.0/24", "192.168.188.190/24", "172.16.0.5/16", "10.255.255.255/8",
		"9.0.0.0/8", "11.0.0.0/8", "172.16.2.187", "192.168.1.0/24",
	}
	rows := make([]map[string]any, len(vals))
	for i, v := range vals {
		rows[i] = map[string]any{"id": int64(i), "c_cidr": v}
	}
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	manifest, err := cat.GetManifest(ctx, cidrStatsTable)
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}
	var files []catalog.FileEntry
	for _, part := range manifest.Partitions {
		files = append(files, part.Files...)
	}
	if len(files) != 1 {
		t.Fatalf("manifest holds %d files, want exactly 1", len(files))
	}
	cs, ok := files[0].ColumnStats["c_cidr"]
	if !ok {
		t.Fatal("the catalog recorded no stats at all for c_cidr")
	}

	minStr, ok := cs.MinValue.(string)
	if !ok {
		t.Fatalf("catalog MinValue for c_cidr is %#v (%T), want the winning row's address TEXT as a plain string",
			cs.MinValue, cs.MinValue)
	}
	maxStr, ok := cs.MaxValue.(string)
	if !ok {
		t.Fatalf("catalog MaxValue for c_cidr is %#v (%T), want the winning row's address TEXT as a plain string",
			cs.MaxValue, cs.MaxValue)
	}
	if !utf8.ValidString(minStr) || !utf8.ValidString(maxStr) {
		t.Fatalf("catalog CIDR bounds are not valid UTF-8 (min %q, max %q) — "+
			"they will not survive the manifest's JSON encoding", minStr, maxStr)
	}
	if minStr != "9.0.0.0/8" {
		t.Errorf("catalog MinValue for c_cidr = %q, want %q (the file's true inet-order minimum, "+
			"which lives in the SECOND row group)", minStr, "9.0.0.0/8")
	}
	if maxStr != "192.168.188.190/24" {
		t.Errorf("catalog MaxValue for c_cidr = %q, want %q (the file's true inet-order maximum)",
			maxStr, "192.168.188.190/24")
	}

	// The manifest really is persisted as JSON, so the bounds must survive a
	// round trip through it byte for byte. This is the step that destroyed
	// the sort key: json.Marshal replaces every invalid UTF-8 byte with
	// U+FFFD and json.Unmarshal cannot put them back.
	blob, err := json.Marshal(cs)
	if err != nil {
		t.Fatalf("marshalling the persisted stats: %v", err)
	}
	var back catalog.FileColumnStats
	if err := json.Unmarshal(blob, &back); err != nil {
		t.Fatalf("unmarshalling the persisted stats: %v", err)
	}
	if fmt.Sprint(back.MinValue) != minStr || fmt.Sprint(back.MaxValue) != maxStr {
		t.Errorf("the CIDR bounds did not survive the manifest's JSON round trip:\n  before: %q / %q\n  after:  %q / %q",
			minStr, maxStr, fmt.Sprint(back.MinValue), fmt.Sprint(back.MaxValue))
	}
}
