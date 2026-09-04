package coordinator

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// arcD5CountingStore counts the object-store GETs whose key names one table.
//
// This is the timing-free way to ask "did the inner relation get read once, or
// once per outer row". A wall-clock assertion on the same question would be a
// flake; a fetch count is exact and it is the quantity the cost is linear in.
type arcD5CountingStore struct {
	objstore.Store
	needle string
	gets   atomic.Int64
}

func (s *arcD5CountingStore) Get(ctx context.Context, bucket, key string) (io.ReadCloser, objstore.ObjectInfo, error) {
	if strings.Contains(key, s.needle) {
		s.gets.Add(1)
	}
	return s.Store.Get(ctx, bucket, key)
}

// arcD5ForcedCounter counts memory.ReserveOrForce's give-up warning and keeps
// the last reservation size it reported.
type arcD5ForcedCounter struct {
	forced atomic.Int64
	bytes  atomic.Int64
}

func (h *arcD5ForcedCounter) Enabled(context.Context, slog.Level) bool { return true }
func (h *arcD5ForcedCounter) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *arcD5ForcedCounter) WithGroup(string) slog.Handler            { return h }
func (h *arcD5ForcedCounter) Handle(_ context.Context, r slog.Record) error {
	if r.Message != "memory reservation forced past budget" {
		return nil
	}
	h.forced.Add(1)
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "bytes" {
			h.bytes.Store(a.Value.Int64())
		}
		return true
	})
	return nil
}

// arcD5RerunFixture builds outer(k) and inner(k, pad) in a fresh database whose
// store counts reads of the inner table's files. padWidth sets the inner file's
// size, which is what decides whether a single load fits the budget.
func arcD5RerunFixture(t *testing.T, ctx context.Context, budget int64, outerRows, innerRows, padWidth int) (*wadjet.DB, *arcD5CountingStore) {
	t.Helper()
	store := &arcD5CountingStore{Store: objstore.NewMemStore(), needle: "d5_inner"}
	db, err := wadjet.Open(ctx, wadjet.Config{
		Store: store, Bucket: "test",
		MemoryBudget: budget, SpillDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	outerSchema := parquet.Schema{Columns: []parquet.Column{{Name: "k", Type: parquet.TypeInt64}}}
	innerSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "pad", Type: parquet.TypeString},
	}}
	outerRowsData := make([]map[string]any, outerRows)
	for i := range outerRowsData {
		outerRowsData[i] = map[string]any{"k": int64(i)}
	}
	pad := strings.Repeat("x", padWidth)
	innerRowsData := make([]map[string]any, innerRows)
	for i := range innerRowsData {
		innerRowsData[i] = map[string]any{"k": int64(i % 97), "pad": fmt.Sprintf("%s-%d", pad, i)}
	}
	for _, spec := range []struct {
		name   string
		schema parquet.Schema
		rows   []map[string]any
	}{
		{"d5_outer", outerSchema, outerRowsData},
		{"d5_inner", innerSchema, innerRowsData},
	} {
		if err := db.CreateTable(ctx, spec.name, spec.schema, nil); err != nil {
			t.Fatalf("create %s: %v", spec.name, err)
		}
		ing := db.NewIngester(spec.name, spec.schema, nil, ingest.Config{MaxBufferRows: len(spec.rows) + 1})
		if err := ing.Ingest(ctx, spec.rows); err != nil {
			t.Fatalf("ingest %s: %v", spec.name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", spec.name, err)
		}
	}
	store.gets.Store(0) // ingest's own reads are not the measurement
	return db, store
}

// The three spellings of one question: "is there a row of d5_inner matching
// this outer row". Only the FROM differs.
const (
	arcD5RerunOverBase = `SELECT count(*) FROM d5_outer o
	                        WHERE EXISTS (SELECT 1 FROM d5_inner i WHERE i.k = o.k)`
	arcD5RerunOverDerived = `SELECT count(*) FROM d5_outer o
	                           WHERE EXISTS (SELECT 1 FROM (SELECT k FROM d5_inner) d WHERE d.k = o.k)`
	arcD5RerunOverCTE = `WITH d AS (SELECT k FROM d5_inner)
	                     SELECT count(*) FROM d5_outer o
	                       WHERE EXISTS (SELECT 1 FROM d WHERE d.k = o.k)`
	// The same question in the one position that is still not a decorrelation
	// site: an aggregate ARGUMENT (deferral D1). It answers the same number
	// and it is re-run once per outer row, which is what
	// TestCorrelatedRerunPaysTheFullReserveWaitPerOuterRow needs to reach the
	// memory defect it is really about.
	arcD5RerunInAnAggregateArgument = `SELECT SUM(CASE WHEN EXISTS (
	                                     SELECT 1 FROM d5_inner i WHERE i.k = o.k)
	                                   THEN 1 ELSE 0 END) FROM d5_outer o`
)

// TestEveryCorrelatedInnerIsReadOnce is the DELETED pin of deferral D2, kept
// as its inverse — which is the fix's proof (#852).
//
// It used to assert the defect: the decorrelators lowered a correlated EXISTS
// whose FROM was a BASE TABLE and only that, so the same subquery over a
// DERIVED TABLE or a CTE REFERENCE fell back to the per-row re-run and read
// the whole inner relation once for every outer row — measured at 2N+1 reads
// for N outer rows against a flat 3 for the spelling that lowered. All three
// answers were right, which is why no answer-comparing gate could see it; a
// fetch count is exact and it is the quantity the cost is linear in.
//
// The build side is now the subquery's OWN PLAN (logical.decorrelatedInnerPlan
// over the builder's buildFromClause), so all three spellings read the inner
// ONCE for the join build, whatever the outer row count. Revert that and the
// derived and CTE arms go back to one read per outer row and this fails.
func TestEveryCorrelatedInnerIsReadOnce(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"base table inner", arcD5RerunOverBase},
		{"derived table inner", arcD5RerunOverDerived},
		{"cte reference inner", arcD5RerunOverCTE},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Two outer sizes, because the SHAPE of the growth is the claim.
			// A single count cannot tell a constant from a multiple.
			counts := map[int]int64{}
			for _, outerRows := range []int{8, 16} {
				db, store := arcD5RerunFixture(t, ctx, 0, outerRows, 400, 8)
				res, err := db.Query(ctx, tc.sql)
				if err != nil {
					t.Fatalf("outer=%d: %v", outerRows, err)
				}
				if got := fmt.Sprint(res.Rows[0][res.Columns[0]]); got != fmt.Sprint(outerRows) {
					t.Fatalf("outer=%d: count(*) = %s, want %d", outerRows, got, outerRows)
				}
				counts[outerRows] = store.gets.Load()
			}
			t.Logf("inner-file reads: outer=8 -> %d, outer=16 -> %d", counts[8], counts[16])

			if counts[8] != counts[16] {
				t.Errorf("read the inner %d times for 8 outer rows and %d for 16; a "+
					"decorrelated correlated subquery reads its inner ONCE, however many "+
					"outer rows probe it. A count that grows with the outer rows means the "+
					"rewrite declined and the per-row re-run answered (#852).",
					counts[8], counts[16])
			}
			// Measured at this tip: 3, and 3 for either outer size. The bound
			// is deliberately a small constant rather than the exact number —
			// what this pin defends is that the count is not a MULTIPLE of the
			// outer rows, and a scan that splits its reads differently is not
			// this defect coming back.
			if counts[8] > 4 {
				t.Errorf("read the inner %d times for 8 outer rows; want a small constant "+
					"(3 at the time of writing), not a per-row re-read", counts[8])
			}
		})
	}
}

// TestCorrelatedRerunPaysTheFullReserveWaitPerOuterRow reaches, end to end, the
// condition the round-0 census hit as a 30-minute timeout, at the smallest size
// that reaches it (protocol rule 10: a documented cost needs a fixture that
// attempts it, or it is a story).
//
// The condition is the CONJUNCTION of two independent defects, and neither one
// alone is expensive:
//
//  1. planner — a correlated subquery the decorrelators do not lower is re-run
//     from the top for every outer row, so the inner file is loaded once per
//     row. The shape that reached it used to be a DERIVED-TABLE inner; that
//     one now lowers (#852), so this pin drives the re-run through the
//     position that still does not lower — an aggregate ARGUMENT, which is not
//     a decorrelation site at all (deferral D1, #734's residual). The two are
//     the same re-run and the same cost; only the reason it is reached moved.
//  2. memory — internal/engine/memory.ReserveOrForce waits the caller's full
//     relief timeout (physical.fileLoadReserveWait, 2s) for a reservation of n
//     bytes against a budget SMALLER THAN n. That reservation cannot succeed:
//     no amount of spilling makes a 912 KiB file fit a 512 KiB budget, so the
//     wait is spent to reach the ForceReserve that was inevitable on entry.
//
// Together they cost 2 seconds per outer row. The measured shape at fd679ae9
// and at this tip: forced == outer rows, one reservation of 933732 bytes
// against a 524288-byte budget, wall == 2s x outer rows.
//
// What flips this pin: fixing EITHER defect. Making the aggregate argument a
// decorrelation site drops forced to 1; short-circuiting ReserveOrForce when
// n > budget drops the wall to ~0 while forced stays at the outer row count.
// The pin asserts the two separately so the failure names which one moved.
//
// Deliberately two outer rows. The defect is per-row and constant per row, so
// two rows prove the multiplication that twenty would only prove more slowly.
func TestCorrelatedRerunPaysTheFullReserveWaitPerOuterRow(t *testing.T) {
	if testing.Short() {
		t.Skip("costs 2s per outer row by construction; that is the finding")
	}
	ctx := context.Background()
	const budget = 512 * 1024
	const outerRows = 2

	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	counter := &arcD5ForcedCounter{}

	// A 60000-row inner with a 120-byte pad makes ONE file that is larger than
	// the whole budget. That is the trigger for defect 2 and it is a property
	// of the file, not of the query.
	db, store := arcD5RerunFixture(t, ctx, budget, outerRows, 60000, 120)
	slog.SetDefault(slog.New(counter))
	res, err := db.Query(ctx, arcD5RerunInAnAggregateArgument)
	slog.SetDefault(prev)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if got := fmt.Sprint(res.Rows[0][res.Columns[0]]); got != fmt.Sprint(outerRows) {
		t.Fatalf("count(*) = %s, want %d", got, outerRows)
	}

	reads, forced, bytes := store.gets.Load(), counter.forced.Load(), counter.bytes.Load()
	t.Logf("inner reads=%d forced reservations=%d reservation bytes=%d budget=%d",
		reads, forced, bytes, budget)

	// Defect 1: the inner file is read once per outer row.
	if reads < outerRows {
		t.Errorf("inner read %d times for %d outer rows; want one read per row "+
			"(if the decorrelator now covers this shape, see the pin above)", reads, outerRows)
	}
	// Defect 2: each of those reads reserves more than the entire budget, so
	// it burns the relief wait before forcing.
	if forced < outerRows {
		t.Errorf("%d forced reservations for %d per-row reads; want one per read", forced, outerRows)
	}
	if bytes <= budget {
		t.Errorf("reservation was %d bytes against a %d-byte budget; this pin needs a "+
			"single file LARGER than the budget to reach the futile wait — the "+
			"fixture stopped reaching its own condition", bytes, budget)
	}
}

// TestScalarSubqueryOverTheSameTableAsAnEnclosingBuildHangs pins the hang the
// #616 deferral's mechanism did not cover, with the discriminator that says
// what the trigger actually is.
//
// #616's record attributed this hang class to TPC-H Q2's comma spelling and to
// a shared scan cache reached through a comma join. It is broader than that,
// and it is SHARPER: this repro has NO comma join and NO correlation. What it
// has is a scalar subquery reading THE SAME TABLE that the enclosing IN's
// semi-join is at that moment scanning to build its hash table. The build
// waits on the scan, the scan's slot is held for the build, `source init`
// never returns, and the query hangs until something cancels it.
//
// The control is the whole argument. Change ONLY the scalar subquery's table —
// same nesting, same operators, same shapes — and it answers in milliseconds.
// Each level ALONE also answers in milliseconds. So the trigger is neither the
// nesting nor either subquery: it is the RE-ENTRANT read of a table from
// inside a build that the same table's scan is feeding.
//
// Not fixed here. It belongs to the scan cache and the join's build, which is
// where #616's remaining half already sits (ADR-0021 §1i); this pin exists so
// the deferral's mechanism names the real condition instead of a spelling.
//
// The deadline is short and deliberate: a hang pinned with a long timeout is a
// slow test, and a hang pinned with a short one is a fact. The day this shape
// answers, the deadline error stops arriving and the pin fails.
func TestScalarSubqueryOverTheSameTableAsAnEnclosingBuildHangs(t *testing.T) {
	ctx := context.Background()
	db, _ := arcD5RerunFixture(t, ctx, 0, 10, 10, 4)

	const hangs = `SELECT COUNT(*) FROM d5_inner a WHERE a.k IN (
	                 SELECT b.k FROM d5_inner b
	                  WHERE b.k > (SELECT AVG(c.k) FROM d5_inner c))`
	// Identical but for the scalar subquery's table.
	const control = `SELECT COUNT(*) FROM d5_inner a WHERE a.k IN (
	                   SELECT b.k FROM d5_inner b
	                    WHERE b.k > (SELECT AVG(o.k) FROM d5_outer o))`

	deadline, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if _, err := db.Query(deadline, hangs); err == nil {
		t.Errorf("the same-table nesting answered; it hung at fd679ae9 and at the "+
			"tip of this arc. If the scan cache no longer deadlocks against a build "+
			"reading the same table, delete this pin and say so (#616).\n  SQL: %s", hangs)
	} else if deadline.Err() == nil {
		t.Errorf("wanted the query to hang until the deadline; it failed early with %v", err)
	}

	// The control must answer, and quickly, or the pin is measuring the
	// fixture rather than the condition.
	quick, cancel2 := context.WithTimeout(ctx, 30*time.Second)
	defer cancel2()
	start := time.Now()
	res, err := db.Query(quick, control)
	if err != nil {
		t.Fatalf("control (scalar subquery over the OTHER table) failed: %v", err)
	}
	if el := time.Since(start); el > 5*time.Second {
		t.Errorf("control took %v; it answers in milliseconds when the hang is the "+
			"same-table read, so the discriminator has stopped discriminating", el)
	}
	if got := fmt.Sprint(res.Rows[0][res.Columns[0]]); got != "5" {
		t.Errorf("control answered %s, want 5", got)
	}
}

// TestAFailedSharedScanDoesNotStrandItsOtherReaders is the gate for the second
// deadlock this arc found, and the one it fixed.
//
// The shared scan cache is claimed in catalogScanSource.Init — the first
// reader of a table creates `cache.ready` — and it USED to be released in
// exactly one place: Next's end-of-table branch. So a claiming scan that
// FAILED, or that was closed before the end of the table, left `ready` open
// and every other reader of that table waited on a channel nobody would ever
// close. Not slowly: forever.
//
// Reaching it needs two readers of one table where the first one fails, and
// the LATERAL repair made that shape reachable — which is how this arc found
// it. Two LATERALs over the same table, the second carrying a residual the
// scan cannot compile:
//
//	fd679ae9   error in 0.9 s on every arm (the residual defect, older than
//	           this arc, and not what is being tested here)
//	this arc   single: the same error in 6 ms
//	           DAG:    an unbounded block — measured at 20 s, 180 s, 360 s
//	fixed      DAG: the same error in 4 ms
//
// A hang is worse than the error it replaced, so this asserts a BOUND on the
// failure, not just its text: the query must fail, and it must fail fast. The
// deadline is what makes it a gate rather than a description — revert the
// release in abandonCache and this test does not fail with a wrong message, it
// stops finishing.
//
// It also asserts that the client is told the REASON. Releasing the claim
// makes the waiter fail, and for a while that failure was reported instead of
// the real one: under `-race` this query answered "shared scan of lat_item did
// not complete: scan closed before the end of the table", which names the
// teardown rather than the uncompilable residual that caused it. The build and
// the probe are prepared concurrently, so which of the two errors arrived
// first was a scheduling race — and a masked root cause is a diagnosis defect
// even when the query correctly fails. An abandoned claim is never a root
// cause (abandonedClaimError says so in its own doc), so buildJoin prefers the
// build's error over a probe that only failed on the claim, and BOTH arms now
// report the residual deterministically. The invariant asserted below is that
// the release's message never appears WITHOUT the reason.
//
// The second cell is the reason the fix is a release and not a decline. That
// shape — the same two LATERALs, both compilable — ANSWERS PostgreSQL's values
// including Carol's defaulted 0. Declining the repair whenever two LATERALs
// appear would have traded this right answer for a wrong one to avoid a hang
// in a query that is already broken by an unrelated defect.
//
// What this does NOT fix, and the difference is worth stating: a claiming scan
// that is still RUNNING, blocked on a build that is itself blocked on that
// scan's output, is a genuine CYCLE. Releasing on exit cannot help — nothing
// has exited. That is #616's deadlock and
// TestScalarSubqueryOverTheSameTableAsAnEnclosingBuildHangs still pins it,
// still hanging, deliberately.
func TestAFailedSharedScanDoesNotStrandItsOtherReaders(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)

	const twoReadersFirstFails = `SELECT o.customer AS c, s.n AS n, s2.m AS m FROM lat_ord o ` +
		`JOIN LATERAL (SELECT COUNT(*) AS n FROM lat_item WHERE order_id = o.id) s ON true ` +
		`JOIN LATERAL (SELECT COUNT(*) AS m FROM lat_item ` +
		`WHERE order_id = o.id AND amount > 60) s2 ON true ORDER BY c`
	const twoReadersBothClean = `SELECT o.customer AS c, s.n AS n, s2.m AS m FROM lat_ord o ` +
		`JOIN LATERAL (SELECT COUNT(*) AS n FROM lat_item WHERE order_id = o.id) s ON true ` +
		`JOIN LATERAL (SELECT SUM(amount) AS m FROM lat_item WHERE order_id = o.id) s2 ON true ` +
		`ORDER BY c`

	// Generous enough that a slow machine does not flake it, and short enough
	// that a re-stranded claim cannot be mistaken for slowness: the failure
	// arrives in single-digit milliseconds and the block was unbounded.
	const bound = 45 * time.Second

	for _, tc := range []struct {
		name string
		sql  string
		// wantErr: non-empty means the arm must FAIL inside bound, naming the
		// query's REAL defect — the same substring on both arms, because the
		// invariant is that the client is told the reason and never the
		// consequence. See the release-message note above the table.
		wantErr string
		want    []string // otherwise: must ANSWER this
	}{
		{
			name:    "the failing reader releases its claim, and the cause survives",
			sql:     twoReadersFirstFails,
			wantErr: `filter column "amount" does not exist`,
		},
		{
			name: "two clean readers of one table still answer",
			sql:  twoReadersBothClean,
			want: []string{
				"c=Alice|n=int64:2|m=float:150",
				"c=Bob|n=int64:2|m=float:200",
				"c=Carol|n=int64:0|m=NULL",
			},
		},
	} {
		for _, arm := range []struct {
			name string
			run  func(context.Context, string) ([]string, error)
		}{
			{"single", func(c context.Context, q string) ([]string, error) {
				return na2Run(tmdRunSingle(c, single, q))
			}},
			{"dag", func(c context.Context, q string) ([]string, error) {
				return na2Run(tmdRunDAG(c, coord, q))
			}},
		} {
			wantErr := tc.wantErr
			t.Run(tc.name+"/"+arm.name, func(t *testing.T) {
				type outcome struct {
					rows []string
					err  error
				}
				done := make(chan outcome, 1)
				cctx, cancel := context.WithTimeout(context.Background(), bound)
				defer cancel()
				start := time.Now()
				go func() {
					rows, err := arm.run(cctx, tc.sql)
					done <- outcome{rows, err}
				}()

				var got outcome
				select {
				case got = <-done:
				case <-time.After(bound + 10*time.Second):
					t.Fatalf("query did not finish within %v — a shared scan claim is "+
						"stranded again. The claiming scan must release cache.ready on "+
						"EVERY exit (catalogScanSource.abandonCache), not only at the end "+
						"of the table.\n  SQL: %s", bound, tc.sql)
				}
				elapsed := time.Since(start).Round(time.Millisecond)

				if wantErr != "" {
					if got.err == nil {
						t.Fatalf("want a failure naming %q, got rows %v", wantErr, got.rows)
					}
					if !strings.Contains(got.err.Error(), wantErr) {
						t.Errorf("error does not name %q: %v", wantErr, got.err)
					}
					// The invariant that makes this a diagnosis gate and not
					// a string check: the release's own message is a
					// CONSEQUENCE of the failure, so it must never be all the
					// client is told. It may appear in the chain; it may not
					// appear alone.
					if msg := got.err.Error(); strings.Contains(msg, "did not complete") &&
						!strings.Contains(msg, wantErr) {
						t.Errorf("the abandoned-claim message MASKED the real cause — the "+
							"client is told the teardown, not the reason:\n  %v", msg)
					}
					if cctx.Err() != nil {
						t.Errorf("the query only ended because its own deadline expired "+
							"after %v; that is a timeout, not a refusal", elapsed)
					}
					t.Logf("failed in %v: %v", elapsed, got.err)
					return
				}
				if got.err != nil {
					t.Fatalf("want rows, got error after %v: %v", elapsed, got.err)
				}
				sort.Strings(got.rows)
				want := append([]string(nil), tc.want...)
				sort.Strings(want)
				if len(got.rows) != len(want) {
					t.Fatalf("got %d rows, want %d\n  got:  %v\n  want: %v",
						len(got.rows), len(want), got.rows, want)
				}
				for i := range want {
					if got.rows[i] != want[i] {
						t.Errorf("row %d: got %q, want %q", i, got.rows[i], want[i])
					}
				}
			})
		}
	}
}
