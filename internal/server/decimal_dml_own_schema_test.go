package server

import (
	"context"
	"io"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// srvMultiRowsUnderEachFilesOwnSchema reads every manifest file EXCEPT
// originalFile through the schema that file itself declares.
//
// It exists for one fixture:
// TestServerFailedUpdateAcrossFilesLeavesEveryFileIntact writes a file whose
// DECIMAL leaf is declared wider than the table's, holding a value the table's
// type cannot express, directly to the store — bypassing every write door,
// because a REFUSED update needs something to be refused over.
//
// The reader holds a file's DECIMAL values to the CATALOG's declared band
// (ADR-0018 §9): a `DECIMAL(9,2)` column promises an absolute value below
// 10^7, PostgreSQL cannot reach any other state (`…::numeric(9,2)` is 22003),
// and both read paths now refuse one. So the catalog's type is exactly the
// wrong lens for observing that deliberately out-of-band row. What this test
// needs to know is that the ROW IS STILL THERE, and the file's own declaration
// is what can say so.
func srvMultiRowsUnderEachFilesOwnSchema(t *testing.T, cat *catalog.Catalog,
	tableName, originalFile string,
) []map[string]any {
	t.Helper()
	ctx := context.Background()
	manifest, err := cat.GetManifest(ctx, tableName)
	if err != nil {
		t.Fatal(err)
	}
	var out []map[string]any
	for _, part := range manifest.Partitions {
		for _, f := range part.Files {
			if f.Path == originalFile {
				continue
			}
			rc, _, err := cat.Store().Get(ctx, cat.Bucket(), f.Path)
			if err != nil {
				t.Fatalf("get %s: %v", f.Path, err)
			}
			data, err := io.ReadAll(rc)
			rc.Close()
			if err != nil {
				t.Fatalf("read %s: %v", f.Path, err)
			}
			r, err := parquet.NewReaderFromBytes(data)
			if err != nil {
				t.Fatalf("open %s: %v", f.Path, err)
			}
			rows, err := r.ReadRows(nil)
			if err != nil {
				t.Fatalf("read %s on its own terms: %v", f.Path, err)
			}
			out = append(out, rows...)
		}
	}
	return out
}
