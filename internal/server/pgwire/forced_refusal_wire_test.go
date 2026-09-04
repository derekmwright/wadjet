package pgwire

// A memory refusal names what was charged PAST the budget, and the WIRE carries
// that sentence.
//
// The engine builds the suffix in `memory.Tracker.Reserve` and
// `wadjet.TestAQueryThatRefusesNamesWhatWasForced` proves it survives the
// join's wrapping and the planner's through the embedded door. This is the
// remaining leg: the error then travels the pgwire error path before it becomes
// an ErrorResponse field, and a client that never sees the sentence cannot act
// on it. #789's investigation is the argument for the sentence existing at all —
// `used=465738` with no way to say whose bytes those were — and the operator
// reading it is on the other end of a socket, not in a unit test.

import (
	"context"
	"regexp"
	"strconv"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

var wireForcedSuffix = regexp.MustCompile(
	`of which forced=\d+ (by "[a-z ]+"|\(largest: \d+ by "[a-z ]+"\))`)

func TestAForcedRefusalReachesTheWire(t *testing.T) {
	srv := setupForcedRefusalServer(t)
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, "postgres://wadjet@"+srv.Addr()+"/wadjet?sslmode=disable")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	// pgx defers a query's error to the row iteration, so drain before asking.
	rows, err := conn.Query(ctx, `SELECT a.id, b.pad FROM refuse a JOIN refuse b ON a.id = b.id`)
	if err == nil {
		for rows.Next() {
		}
		err = rows.Err()
		rows.Close()
	}
	if err == nil {
		t.Fatal("the query answered over the wire at 256 KiB; this cell needs a refusal " +
			"to read, so the fixture has stopped meeting its own condition")
	}
	if !wireForcedSuffix.MatchString(err.Error()) {
		t.Errorf("the refusal reached the client without saying what was forced:\n  %v", err)
	}
	t.Logf("wire refusal: %v", err)
}

func setupForcedRefusalServer(t *testing.T) *Server {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{
		Store: objstore.NewMemStore(), Bucket: "test",
		MemoryBudget: 256 << 10, SpillDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "pad", Type: parquet.TypeString},
	}}
	if err := db.CreateTable(ctx, "refuse", schema, nil); err != nil {
		t.Fatal(err)
	}
	const rows = 60000
	data := make([]map[string]any, rows)
	for i := range data {
		data[i] = map[string]any{
			"id":  int64(i),
			"pad": "padpadpadpadpadpadpadpadpadpadpad" + strconv.Itoa(i),
		}
	}
	ing := db.NewIngester("refuse", schema, nil, ingest.Config{
		MaxBufferRows: rows + 1, RowGroupSize: 4096,
	})
	if err := ing.Ingest(ctx, data); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	srv := NewServer(db, Config{}, nil)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	return srv
}
