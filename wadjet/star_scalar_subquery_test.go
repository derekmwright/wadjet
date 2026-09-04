package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Round-1 P2. The star's declared output schema is NOT advisory: it also
// reaches the ROW path.
//
// physical.declaredOutputSchema feeds subqueryOutputColumn (#696), which picks
// the COMPARISON RULE for a scalar subquery — how the outer column's value and
// the subquery's value are made comparable before they are compared. A bare
// star as the subquery had no declaration at all, so the rule was chosen
// without one and `d = (SELECT * FROM one_row)` over two DECIMALs of different
// scale answered ZERO rows where PostgreSQL 17 answers one. Declaring the star
// hands that call the same column the named spelling has always handed it.
//
// So this is a silent-wrong → right move on a query that returns rows, and the
// first pass's census called the non-empty path "unchanged". It was not, and
// nothing gated it. The values are chosen so the two arms cannot agree by
// accident: 1.25 at scale 2 against 1.2500 at scale 4 is the same NUMBER under
// two encodings, which is precisely what the comparison rule has to reconcile.
func TestStarScalarSubqueryComparesLikeItsNamedSpelling(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := Open(ctx, Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	main := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 9, Scale: 2},
	}}
	one := parquet.Schema{Columns: []parquet.Column{
		{Name: "v", Type: parquet.TypeDecimal, Precision: 18, Scale: 4},
	}}
	if err := db.CreateTable(ctx, "ssmain", main, nil); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateTable(ctx, "ssone", one, nil); err != nil {
		t.Fatal(err)
	}
	ingMain := db.NewIngester("ssmain", main, nil, ingest.Config{MaxBufferRows: 8, RowGroupSize: 8})
	if err := ingMain.Ingest(ctx, []map[string]any{
		{"id": int64(1), "d": "1.25"}, {"id": int64(2), "d": "9.99"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ingMain.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	ingOne := db.NewIngester("ssone", one, nil, ingest.Config{MaxBufferRows: 8, RowGroupSize: 8})
	if err := ingOne.Ingest(ctx, []map[string]any{{"v": "1.2500"}}); err != nil {
		t.Fatal(err)
	}
	if err := ingOne.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	// The named spelling is the reference: it has always had a declaration,
	// and PostgreSQL 17 answers one row for both.
	named, err := db.Query(ctx, "SELECT id FROM ssmain WHERE d = (SELECT v FROM ssone)")
	if err != nil {
		t.Fatalf("named spelling: %v", err)
	}
	if len(named.Rows) != 1 {
		t.Fatalf("the REFERENCE spelling returned %d rows, want 1; the comparison is meaningless",
			len(named.Rows))
	}

	star, err := db.Query(ctx, "SELECT id FROM ssmain WHERE d = (SELECT * FROM ssone)")
	if err != nil {
		t.Fatalf("star spelling: %v", err)
	}
	if len(star.Rows) != len(named.Rows) {
		t.Fatalf("`d = (SELECT * FROM ssone)` returned %d rows and `d = (SELECT v FROM ssone)` "+
			"returned %d. PostgreSQL 17 answers 1 for both. The two spell the same subquery, "+
			"so the scalar comparison rule must not depend on which one was typed (#696, "+
			"round-1 P2).", len(star.Rows), len(named.Rows))
	}
	if got := star.Rows[0]["id"]; got != int64(1) {
		t.Errorf("star spelling matched id %v, want 1", got)
	}
}
