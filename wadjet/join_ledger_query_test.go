package wadjet

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #789 asked what a query's memory floor is made of, because `used` differed
// run to run for one query on one fixture: 465738, 447345, 449423, 482816,
// 492138, 660967. Two mechanisms produced that, and only one of them is a
// defect.
//
// The LIVE one is not. A scan holds the row groups it has in flight and
// releases each when it is decoded (ADR-0006's 2026-09-03 amendment), so what a
// scan holds at any instant follows how far it has run ahead of its consumer.
// At `GOMAXPROCS=1` this fixture's floor is one number on every run; with the
// scheduler free it takes several. Those bytes are resident and charged, the
// read is bounded by the budget, and serializing it is what S2a implemented
// twice and refused on measurement. It is ADR-0013's legal nondeterminism, and
// `TestAJoinAtAFixedBudgetIsDecidedByTheQueryNotTheScheduler` already gates the
// thing that must not vary: the VERDICT.
//
// The DEFECT was the LEDGER. A grace build released either more or less than it
// reserved on every arrival batch — the spilled branch by 1.22x too much, the
// in-memory branch a little too little — so which way a query's `used` drifted
// followed how many partitions had spilled by then. This gate is that half, end
// to end: no query may release bytes it never reserved.
//
// The instrument is the tracker's own WARN, which fires once per tracker the
// first time a release drives `used` below zero. It is a condition no correct
// query can reach, so counting it needs no threshold and no tolerance.
//
// It is a NON-REGRESSION claim, not the revert proof, and the difference is
// stated rather than assumed: the drift is per ARRIVAL BATCH, and a query's
// arrival batches are 2,048 rows, so 60,000 rows is only ~30 of them and about
// 163 KB of drift — not enough to cross zero against these budgets. Reverting
// the fix therefore leaves this gate GREEN. The revert proof is
// `exec.TestAGraceBuildsLedgerIsConserved`, which drives the operator directly
// with 256-row arrivals and reaches `used = -867,561`. What this gate adds is
// the end-to-end direction: any future over-release large enough to cross zero
// in a real query fails here, and the engagement assert below keeps it honest
// about reaching the already-spilled write path at all.
//
// The AGGREGATE was excluded from this gate until #862 was fixed, and it is a
// cell now. The symptom read as `HashAggregate.Close` over-releasing on a
// morsel-parallel emit clone (`emitDrain.produce -> HashAggregate.Close ->
// ReleaseTracking`, `released=189416 resulting=-151298`), and the producer was
// one line away: the raw-row buffer accumulated `spillBufferBytes` at its
// append site and never CHARGED them, while three sites released them. The
// aggregate cells below carry an engagement assert of their own — the parallel
// emit must actually run, or they are not the shape the filing named.
func TestNoQueryOverReleasesItsMemoryLedger(t *testing.T) {
	var underflows atomic.Int64
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	// Forward to a handler of our OWN over stderr, never to the previous
	// default. slog's default handler writes through log.Default(), and
	// SetDefault points log.Default() back at slog — so a wrapper that forwards
	// to it re-enters the log package's mutex and deadlocks on the first line
	// anything logs.
	inner := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})
	slog.SetDefault(slog.New(&ledgerUnderflowCounter{inner: inner, n: &underflows}))

	queries := []string{
		`SELECT a.id, b.pad FROM e5led a JOIN e5led b ON a.id = b.id`,
		`SELECT a.pad, b.id FROM e5led a JOIN e5led b ON a.pad = b.pad`,
		// #862's shape: a GROUP BY above a join, which is what adopts
		// partitions and fans the emission out across clones.
		`SELECT b.pad AS k, COUNT(*) AS n FROM e5led a JOIN e5led b ON a.id = b.id GROUP BY b.pad`,
		// ...and one that reaches the RAW-ROW buffer, whose unpaired
		// `spillBufferBytes` was the producer: COUNT(DISTINCT) is non-simple,
		// so canUseExternalMerge is false and a grouped shape under pressure
		// buffers its input rows.
		`SELECT b.id % 97 AS k, COUNT(DISTINCT b.pad) AS n FROM e5led a JOIN e5led b ON a.id = b.id GROUP BY b.id % 97`,
	}
	evicted, emits := int64(0), int64(0)
	for _, budget := range []int64{4 << 20, 2 << 20} {
		for _, q := range queries {
			db := ledgerOpen(t, budget)
			before := exec.JoinPartitionsEvicted.Load()
			beforeEmit := exec.ParallelEmitRuns.Load()
			// A refusal is a legal outcome at these budgets; an over-release is
			// not, and it is what this gate measures.
			_, _ = db.Query(context.Background(), q)
			evicted += exec.JoinPartitionsEvicted.Load() - before
			emits += exec.ParallelEmitRuns.Load() - beforeEmit
		}
	}
	if emits == 0 {
		t.Error("no query took the parallel emit path, so the aggregate cells did not " +
			"reach the morsel-clone shape #862 was filed on")
	}
	if evicted == 0 {
		t.Fatal("no grace partition was evicted by any of these queries, so none of them " +
			"reached the already-spilled write path this gate exists for; the fixture " +
			"stopped meeting its own condition")
	}
	if n := underflows.Load(); n != 0 {
		t.Errorf("%d query trackers were driven below zero by a release (%d partitions "+
			"evicted). Bytes given back that were never taken make every later admission "+
			"measure against a floor lower than the memory that exists — see "+
			"exec.TestAGraceBuildsLedgerIsConserved for the operator-level figure",
			n, evicted)
	}
}

// ledgerUnderflowCounter counts the tracker's over-release WARN and forwards
// everything unchanged.
type ledgerUnderflowCounter struct {
	inner slog.Handler
	n     *atomic.Int64
}

func (h *ledgerUnderflowCounter) Enabled(ctx context.Context, l slog.Level) bool {
	return h.inner.Enabled(ctx, l)
}

func (h *ledgerUnderflowCounter) Handle(ctx context.Context, r slog.Record) error {
	if strings.Contains(r.Message, "released more than was reserved") {
		h.n.Add(1)
	}
	return h.inner.Handle(ctx, r)
}

func (h *ledgerUnderflowCounter) WithAttrs(a []slog.Attr) slog.Handler {
	return &ledgerUnderflowCounter{inner: h.inner.WithAttrs(a), n: h.n}
}

func (h *ledgerUnderflowCounter) WithGroup(name string) slog.Handler {
	return &ledgerUnderflowCounter{inner: h.inner.WithGroup(name), n: h.n}
}

const ledgerRows = 60000

func ledgerOpen(t *testing.T, budget int64) *DB {
	t.Helper()
	ctx := context.Background()
	cfg := Config{Store: objstore.NewMemStore(), Bucket: "test", MemoryBudget: budget}
	if budget > 0 {
		cfg.SpillDir = t.TempDir()
	}
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("open (budget %d): %v", budget, err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "pad", Type: parquet.TypeString},
	}}
	if err := db.CreateTable(ctx, "e5led", schema, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	rows := make([]map[string]any, ledgerRows)
	for i := range rows {
		// The pad is UNIQUE per row. A pad with few distinct values makes the
		// string-key self-join a 60,000-way fan-out, which is a different test
		// (and a 275-second one).
		rows[i] = map[string]any{
			"id":  int64(i),
			"pad": "padpadpadpadpadpadpadpadpadpadpad" + strconv.Itoa(i),
		}
	}
	ing := db.NewIngester("e5led", schema, nil, ingest.Config{
		MaxBufferRows: ledgerRows + 1, RowGroupSize: 4096,
	})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return db
}
