// SPDX-License-Identifier: MIT

package sysrows

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/syscatalog"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// natsLikeKV is the store a LIVE server reads its catalog from, as far as a
// catalog scan can tell: NATS KV has no value-free revision probe (MemKV's
// Revision), so every per-table revalidation is a full Get — and a listing
// is a keys consumer. It has a write GENERATION (the bucket's stream
// sequence), which is what it forwards here, and it counts the reads.
type natsLikeKV struct {
	inner       *catalog.MemKV
	gets, lists atomic.Int64
	gen         atomic.Uint64 // every write and delete, as a stream sequence
}

func (k *natsLikeKV) Get(key string) ([]byte, uint64, error) {
	k.gets.Add(1)
	return k.inner.Get(key)
}
func (k *natsLikeKV) Put(key string, v []byte) (uint64, error) {
	k.gen.Add(1)
	return k.inner.Put(key, v)
}
func (k *natsLikeKV) Update(key string, v []byte, rev uint64) (uint64, error) {
	k.gen.Add(1)
	return k.inner.Update(key, v, rev)
}
func (k *natsLikeKV) Delete(key string) error {
	k.gen.Add(1)
	return k.inner.Delete(key)
}
func (k *natsLikeKV) List(prefix string) ([]string, error) {
	k.lists.Add(1)
	return k.inner.List(prefix)
}
func (k *natsLikeKV) Generation() (uint64, error) { return k.gen.Load(), nil }

// TestACatalogScanReadsNoKeyWhileTheCatalogIsUnchanged is P1's mechanism gate
// (arc PC round 3). On a live file-backed server over 1,000 tables `\d t`
// took 1.7 s against PostgreSQL's 36 ms, because every catalog scan listed
// the tables and re-read every table's definition: the per-table revision
// cache needs MemKV's value-free probe, which NATS KV does not have. A scan
// at an unchanged catalog generation must read NO key; a DDL must be seen by
// the next scan; and a policy (which columns an identity is denied) is
// applied per scan, never cached, so it needs no invalidation at all.
func TestACatalogScanReadsNoKeyWhileTheCatalogIsUnchanged(t *testing.T) {
	ctx := context.Background()
	kv := &natsLikeKV{inner: catalog.NewMemKV()}
	cat := catalog.New(kv, objstore.NewMemStore(), "wadjet")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}
	var cols []parquet.Column
	for j := 0; j < 20; j++ {
		cols = append(cols, parquet.Column{Name: fmt.Sprintf("c%02d", j), Type: parquet.TypeInt64, Nullable: true})
	}
	for i := 0; i < 200; i++ {
		if err := cat.CreateTable(ctx, fmt.Sprintf("perf_%04d", i), parquet.Schema{Columns: cols}, nil); err != nil {
			t.Fatal(err)
		}
	}
	denied := map[string]bool{}
	actx := syscatalog.WithAccess(ctx, syscatalog.Access{
		User: "wadjet",
		DeniedColumns: func(_ context.Context, table string) map[string]bool {
			if table == "perf_0007" {
				return denied
			}
			return nil
		},
	})
	rel, ok := syscatalog.ByFuncName("pg_catalog.pg_attribute")
	if !ok {
		t.Fatal("pg_catalog.pg_attribute is not registered")
	}
	count := func(rows []map[string]any, table string) int {
		n := 0
		oid := syscatalog.ObjectOID(table)
		for _, r := range rows {
			if fmt.Sprint(r["attrelid"]) == fmt.Sprint(oid) {
				n++
			}
		}
		return n
	}
	scan := func() []map[string]any {
		t.Helper()
		rows, err := rowsFor(actx, cat, rel)
		if err != nil {
			t.Fatal(err)
		}
		return rows
	}

	first := scan()
	if got := count(first, "perf_0007"); got != 20 {
		t.Fatalf("perf_0007 has %d pg_attribute rows, want 20", got)
	}
	kv.gets.Store(0)
	kv.lists.Store(0)
	for i := 0; i < 3; i++ {
		scan()
	}
	if g, l := kv.gets.Load(), kv.lists.Load(); g != 0 || l != 0 {
		t.Errorf("three scans of an UNCHANGED catalog read %d keys and listed %d times; want 0 and 0 "+
			"(a scan re-reading every definition is what made `\\d t` 1.7 s on a live server)", g, l)
	}

	// A policy change is the identity's view, applied per scan.
	denied["c03"] = true
	if got := count(scan(), "perf_0007"); got != 19 {
		t.Errorf("after denying c03, perf_0007 has %d pg_attribute rows, want 19", got)
	}

	// A DDL moves the generation, and the next scan sees it.
	if err := cat.CreateTable(ctx, "perf_new", parquet.Schema{Columns: cols[:3]}, nil); err != nil {
		t.Fatal(err)
	}
	if got := count(scan(), "perf_new"); got != 3 {
		t.Errorf("after CREATE TABLE perf_new, it has %d pg_attribute rows, want 3", got)
	}
	if err := cat.DropTable(ctx, "perf_0007"); err != nil {
		t.Fatal(err)
	}
	if got := count(scan(), "perf_0007"); got != 0 {
		t.Errorf("after DROP TABLE perf_0007, it still has %d pg_attribute rows", got)
	}
}
