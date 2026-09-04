package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle/collide"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// Two output columns of one NAME keep two VALUES (#556, #844).
//
// These are the entries of the colliding-bare-name fixture that a gate reading
// rows BY NAME cannot judge: both project two columns called `c0`, and
// `map[string]any` holds one of them. That losing map is not an accident of a
// harness — it is the mistake the engine itself made. The single-process
// set-operation lowering ran its arms through exactly such a map, so a UNION
// ALL branch projecting two same-named columns bound the first to the LAST
// one's value, on every row, in silence. `QueryResult.Cells` is positional and
// is what this reads.
//
// Every Want measured on live postgres:17-alpine over the same rows.
func TestCollidingDuplicateOutputNamesKeepBothValues(t *testing.T) {
	ctx := context.Background()
	db := collideDB(t, ctx)
	for _, c := range collide.DuplicateNameCorpus() {
		t.Run(c.Name, func(t *testing.T) {
			res, err := db.Query(ctx, c.SQL)
			if err != nil {
				t.Fatalf("%s: %v", c.SQL, err)
			}
			got := make([]string, 0, len(res.Rows))
			for i := range res.Rows {
				cells := res.Cells(i)
				parts := make([]string, len(cells))
				for j, v := range cells {
					parts[j] = fmt.Sprint(v)
				}
				got = append(got, strings.Join(parts, "|"))
			}
			if len(got) != len(c.Want) {
				t.Fatalf("%d rows, want %d\n  got  %v\n  want %v\n  SQL: %s",
					len(got), len(c.Want), got, c.Want, c.SQL)
			}
			for i := range got {
				if got[i] != c.Want[i] {
					t.Errorf("row %d = %s, PostgreSQL answers %s\n  SQL: %s",
						i, got[i], c.Want[i], c.SQL)
				}
			}
			// The NAMES are duplicated, which is what makes the map lossy and
			// what PostgreSQL publishes. Asserted as a PROPERTY rather than a
			// literal list: every entry of this corpus is here because two of
			// its output columns share a name, and a gate that stopped seeing
			// one would stop testing what it exists for.
			dupNames := false
			for i := range res.Columns {
				for j := i + 1; j < len(res.Columns); j++ {
					if res.Columns[i] == res.Columns[j] {
						dupNames = true
					}
				}
			}
			if !dupNames {
				t.Errorf("published %v, which carries no duplicate name — this entry is in the "+
					"DUPLICATE-name corpus\n  SQL: %s", res.Columns, c.SQL)
			}
			if res.RowValues == nil && len(res.Rows) > 0 {
				t.Errorf("no positional form for a result with duplicate names; "+
					"a caller reading Rows by name would silently get one column twice\n  SQL: %s",
					c.SQL)
			}
		})
	}
}

func collideDB(t *testing.T, ctx context.Context) *DB {
	t.Helper()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, tbl := range collide.Tables() {
		if err := db.CreateTable(ctx, tbl.Name, tbl.Schema, nil); err != nil {
			t.Fatal(err)
		}
		ing := db.NewIngester(tbl.Name, tbl.Schema, nil,
			ingest.Config{MaxBufferRows: len(tbl.Rows) + 1, RowGroupSize: 8})
		if err := ing.Ingest(ctx, tbl.Rows); err != nil {
			t.Fatal(err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	return db
}
