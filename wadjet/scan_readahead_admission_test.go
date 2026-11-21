// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/optswitch"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// #1706: at a fixed budget, on fixed data, a wide self-join answered or
// refused depending on which of two goroutines reached the query's tracker
// first — the scan's read-ahead worker, charging a freshly decoded row group
// (≈251 KB of a 512 KiB budget for a 22-column `SELECT *` over the type
// matrix), or the hash join build, reserving for the batch it had just taken.
// Measured at the base over 30 runs per configuration: 0 to 7 refusals per
// statement depending on GOMAXPROCS and load, and the order alone decided it.
//
// A scan's read-ahead is now admitted against what its decode will hold, and a
// row group that does not fit is decoded on the consumer's demand instead
// (planner/physical/scan_decode_admission.go, ADR-0006's 2026-11-21
// amendment). This gate puts each order on EVERY run, with no sleep, through
// physical.ForceScanReadAheadOrder:
//
//   - DecodeFirst: before every arrival reservation of the grace build, every
//     scan worker has run ahead as far as it can. This is the order that
//     refused; the contrast arm below shows it still refuses with the rule
//     switched off, so the hook is known to be ordering something.
//   - ConsumerFirst: a worker decodes only what its consumer has asked for —
//     the order that answered at the base.
//
// and asserts ONE verdict, the unbudgeted engine's rows, for every statement
// in both orders and with the scheduler free.

const readAheadBudget = 512 * 1024

// readAheadStatements are the shapes in which one decoded row group is a large
// part of the budget: #1706's two statements (TestM1AMergedOrderIsTheQuerysOrderAtScale),
// a wide self-join under a LIMIT, a wide scan feeding a hash aggregate, and a
// wide sort. The last two never refused at the base — neither consumer refuses
// a reservation — and are here so the rule is seen not to break them.
var readAheadStatements = []struct {
	name, sql string
	joins     bool // a grace build is the consumer, so the base's race reaches it
}{
	{"M1A star DISTINCT self-join, ORDER BY id DESC LIMIT 3000",
		"SELECT DISTINCT * FROM typemx a JOIN typemx b ON b.id = a.id ORDER BY a.id DESC LIMIT 3000", true},
	{"M1A star DISTINCT self-join, ORDER BY a nullable STRING key",
		"SELECT DISTINCT * FROM typemx a JOIN typemx b ON b.id = a.id ORDER BY a.c_str, a.id", true},
	{"wide SELECT * self-join under a LIMIT",
		"SELECT * FROM typemx a JOIN typemx b ON b.id = a.id ORDER BY a.id LIMIT 5", true},
	{"wide scan feeding a hash aggregate",
		"SELECT COUNT(*) FROM (SELECT DISTINCT * FROM typemx) x", false},
	{"wide sort",
		"SELECT * FROM typemx ORDER BY c_str, id LIMIT 5", false},
}

// readAheadDB is the flat type matrix under budget (0: no budget).
func readAheadDB(t *testing.T, budget int64) *DB {
	t.Helper()
	ctx := context.Background()
	cfg := Config{Store: objstore.NewMemStore(), Bucket: "test", MemoryBudget: budget}
	if budget > 0 {
		cfg.SpillDir = t.TempDir()
	}
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	schema := typematrix.Schema()
	if err := db.CreateTable(ctx, typematrix.Table, schema, nil); err != nil {
		t.Fatalf("create %s: %v", typematrix.Table, err)
	}
	ing := db.NewIngester(typematrix.Table, schema, nil, ingest.Config{
		MaxBufferRows: typematrix.Rows + 1, RowGroupSize: typematrix.RowGroup,
	})
	if err := ing.Ingest(ctx, typematrix.Data(typematrix.Rows)); err != nil {
		t.Fatalf("ingest %s: %v", typematrix.Table, err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush %s: %v", typematrix.Table, err)
	}
	return db
}

// withReadAheadAdmission sets the rule's kill switch for the length of a test.
func withReadAheadAdmission(t *testing.T, on bool) {
	t.Helper()
	for _, tg := range optswitch.All() {
		if tg.Name == "scan-readahead-admission" {
			prev := tg.Set(on)
			t.Cleanup(func() { tg.Set(prev) })
			return
		}
	}
	t.Fatal("no kill switch named scan-readahead-admission")
}

// rowsText is a result's rows in order, as one comparable string.
func rowsText(t *testing.T, db *DB, sql string) (string, error) {
	t.Helper()
	out, err := db.Query(context.Background(), sql)
	if err != nil {
		return "", err
	}
	return fmt.Sprint(out.Rows), nil
}

func TestAWideScansVerdictAtAFixedBudgetDoesNotFollowTheReadAheadOrder(t *testing.T) {
	if testing.Short() {
		t.Skip("about 60 budgeted queries")
	}
	ref := readAheadDB(t, 0)
	db := readAheadDB(t, readAheadBudget)
	withReadAheadAdmission(t, true)

	want := make([]string, len(readAheadStatements))
	for i, st := range readAheadStatements {
		w, err := rowsText(t, ref, st.sql)
		if err != nil {
			t.Fatalf("%s: unbudgeted: %v", st.name, err)
		}
		want[i] = w
	}

	for _, arm := range []struct {
		name  string
		order int32
		runs  int
	}{
		{"decode first", physical.ReadAheadOrderDecodeFirst, 3},
		{"consumer first", physical.ReadAheadOrderConsumerFirst, 3},
		{"scheduler free", physical.ReadAheadOrderFree, 5},
	} {
		restore := physical.ForceScanReadAheadOrder(arm.order)
		for i, st := range readAheadStatements {
			for run := 0; run < arm.runs; run++ {
				got, err := rowsText(t, db, st.sql)
				if err != nil {
					t.Errorf("%s, %s, run %d: refused at %d bytes where the unbudgeted engine answers: %v",
						st.name, arm.name, run, readAheadBudget, err)
					continue
				}
				if got != want[i] {
					t.Errorf("%s, %s, run %d: answered differently from the unbudgeted engine", st.name, arm.name, run)
				}
			}
		}
		restore()
	}
}

// The contrast arm: with the rule switched off, the DecodeFirst order still
// refuses every join statement on every run. It is what makes the gate above
// mean something — an ordering hook that stopped ordering would leave the gate
// green and prove nothing — and it is the base's verdict for the record.
func TestWithoutReadAheadAdmissionTheDecodeFirstOrderRefuses(t *testing.T) {
	if testing.Short() {
		t.Skip("budgeted queries")
	}
	db := readAheadDB(t, readAheadBudget)
	withReadAheadAdmission(t, false)
	defer physical.ForceScanReadAheadOrder(physical.ReadAheadOrderDecodeFirst)()
	for _, st := range readAheadStatements {
		if !st.joins {
			continue
		}
		_, err := rowsText(t, db, st.sql)
		if err == nil {
			t.Errorf("%s answered with the rule off under DecodeFirst: the hook no longer puts the read-ahead's "+
				"charge before the build's reservation, so the gate beside this one proves nothing", st.name)
			continue
		}
		if !strings.Contains(err.Error(), "memory budget exceeded") || !strings.Contains(err.Error(), "scan decoded batch") {
			t.Errorf("%s refused for another reason than the read-ahead's decoded charge: %v", st.name, err)
		}
	}
}
