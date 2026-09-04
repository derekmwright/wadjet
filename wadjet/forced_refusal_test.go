package wadjet

import (
	"context"
	"regexp"
	"strconv"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A refusal names what was charged PAST the budget, and it does so where
// somebody reads it: in the error a query returns.
//
// `memory.TestARefusalNamesWhatWasForced` asserts the tracker's own error
// string, which is where the sentence is built. That is not the same claim as
// "a query that refuses says it": the engine's error travels through the join's
// wrapping and the planner's before anything sees it, and a suffix lost
// anywhere on that route is invisible to a unit test at the source. This is
// that route through the EMBEDDED door;
// `pgwire.TestAForcedRefusalReachesTheWire` is the same claim through the WIRE.
//
// #789's investigation had `used=465738` and no way to say whose bytes those
// were. The suffix exists so the next reader does not have to ask.
var forcedSuffix = regexp.MustCompile(
	`of which forced=\d+ (by "[a-z ]+"|\(largest: \d+ by "[a-z ]+"\))`)

// forcedRefusalSQL is a self-join whose build cannot fit the budget below.
const forcedRefusalSQL = `SELECT a.id, b.pad FROM refuse a JOIN refuse b ON a.id = b.id`

// forcedRefusalBudget refuses on this fixture; 512 MiB below answers. Measured
// across 1 MiB / 768 / 512 / 384 / 256 / 192 / 128 KiB: the first three answer,
// the rest refuse, all four refusals carrying the suffix.
const forcedRefusalBudget = 256 << 10

func TestAQueryThatRefusesNamesWhatWasForced(t *testing.T) {
	db := refuseOpen(t, forcedRefusalBudget)
	_, err := db.Query(context.Background(), forcedRefusalSQL)
	if err == nil {
		t.Fatal("the query answered at 256 KiB; this cell needs a refusal to read, so the " +
			"fixture has stopped meeting its own condition")
	}
	if !forcedSuffix.MatchString(err.Error()) {
		t.Errorf("a refusal on a ledger the scan and the join both forced past does not "+
			"say what was forced:\n  %v", err)
	}
	t.Logf("refusal: %v", err)

	// The contrast: with room, the same query answers and there is no refusal
	// to decorate. (That the message is UNCHANGED when nothing was forced is
	// `memory.TestARefusalWithNothingForcedIsUnchanged`, which can construct a
	// refusal on a clean ledger — end to end, a refusal always has something
	// forced under it, which is the point.)
	if _, err := refuseOpen(t, 512<<20).Query(context.Background(), forcedRefusalSQL); err != nil {
		t.Fatalf("the query refused at 512 MiB: %v", err)
	}
}

func refuseOpen(t *testing.T, budget int64) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{
		Store: objstore.NewMemStore(), Bucket: "test",
		MemoryBudget: budget, SpillDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "pad", Type: parquet.TypeString},
	}}
	if err := db.CreateTable(ctx, "refuse", schema, nil); err != nil {
		t.Fatalf("create: %v", err)
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
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return db
}
