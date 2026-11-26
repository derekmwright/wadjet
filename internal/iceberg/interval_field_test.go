// SPDX-License-Identifier: MIT

package iceberg

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// Iceberg has no interval type; a field spelled so fell to
// icebergTypeToWadjet's STRING fallback and read as text. It is refused
// (#1713d); the same table with the field a string reads as before.
func TestTableReaderRefusesAnIntervalField(t *testing.T) {
	for _, c := range []struct{ typ, state string }{{"interval", "0A000"}, {"string", ""}} {
		store := objstore.NewMemStore()
		ctx := context.Background()
		store.MakeBucket(ctx, "b")
		putObj(t, store, "b", "w/t/metadata/v1.metadata.json", TableMetadata{
			FormatVersion: 1, TableUUID: "t", Location: "w/t",
			Schema:    Schema{Fields: []Field{{ID: 1, Name: "d", Type: c.typ}}},
			Snapshots: []Snapshot{},
		})
		_, err := NewTableReader(store, "b").ReadTable(ctx, "w/t/metadata/v1.metadata.json")
		if got := sqlerr.StateOf(err); got != c.state || (c.state == "" && err != nil) {
			t.Errorf("field of type %s: %v, want SQLSTATE %q", c.typ, err, c.state)
		}
	}
}
