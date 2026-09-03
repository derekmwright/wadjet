package wadjet

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// The scan reads its parquet files one ROW GROUP at a time, out of ONE object
// GET per file. These are the two gates that property has to hold: the query's
// disposition at a fixed budget stops following the scheduler (#789), and the
// object store still sees exactly one whole-body request per file per scan,
// which is the recorded decision the design had to keep
// (docs/design/scan-pread-reads.md, docs/performance-bottlenecks.md).

// scanDeterminismRuns is the replication count. One passing run proves nothing
// about a defect whose whole shape is "the same query sometimes answers": at
// base, `c_cidr` refused 20 of 20 runs with the scheduler free and answered 20
// of 20 with GOMAXPROCS=1.
const scanDeterminismRuns = 20

// TestAJoinAtAFixedBudgetIsDecidedByTheQueryNotTheScheduler.
//
// The base measurement this replaces (fresh database per run, 512 KiB, 20 runs
// per column, all 18 flat type-matrix columns):
//
//	                 GOMAXPROCS=24        GOMAXPROCS=1
//	c_str            answered  2/20       answered 20/20
//	c_bytes          answered  1/20       answered 20/20
//	c_cidr           answered  0/20       answered 20/20
//	the other 15     answered 20/20       answered 20/20
//
// `c_cidr` is the sharpest cell: both arms were deterministic and they
// DISAGREED, so "it fits" and "it does not fit" were both reproducible for one
// query at one budget on one fixture. The scan held its whole 412,074-byte
// file — 79% of the budget — from the first row group's decode to the last, so
// what decided the join was how far the scan had run ahead.
//
// The gate asserts the property, not a disposition: every run of an arm agrees
// with every other, and the two arms agree with each other. A future change
// that makes this shape refuse deterministically on both arms is a sizing
// decision to argue about; a change that makes the arms disagree is #789 back.
func TestAJoinAtAFixedBudgetIsDecidedByTheQueryNotTheScheduler(t *testing.T) {
	runs := scanDeterminismRuns
	if testing.Short() {
		runs = 3
	}
	const col = "c_cidr"
	sql := fmt.Sprintf(
		`SELECT z.%[1]s AS k, COUNT(*) AS n FROM %[2]s x JOIN %[2]s z ON x.id = z.id GROUP BY z.%[1]s`,
		col, typematrix.Table)

	// Engagement is asserted LAST, after the dispositions. The order matters
	// for the revert: with WADJET_SCAN_RG_BUFFERS=0 the arms disagree, and the
	// disagreement is what this gate is for — a test that bailed out on the
	// engagement check first would report the switch, not the defect.
	engaged := true
	answersAt := func(t *testing.T, procs, runs int) (answered, refused int) {
		t.Helper()
		prev := runtime.GOMAXPROCS(procs)
		defer runtime.GOMAXPROCS(prev)
		ctx := context.Background()
		// One database per arm, the way the spill sweep runs its own
		// replications. A fresh database PER RUN gives the same census —
		// measured on this fixture, both arms, 20 runs each — and costs an
		// ingest per run.
		db := spillMxOpen(t, spillMxBudget)
		rgBefore, _ := physical.RowGroupLoadStats()
		for i := 0; i < runs; i++ {
			_, err := tmRun(ctx, db, sql)
			switch {
			case err == nil:
				answered++
			case strings.Contains(err.Error(), "memory budget exceeded"):
				refused++
			default:
				t.Fatalf("GOMAXPROCS=%d run %d: %v", procs, i, err)
			}
		}
		if rgAfter, _ := physical.RowGroupLoadStats(); rgAfter == rgBefore {
			engaged = false
		}
		return answered, refused
	}

	parA, parR := answersAt(t, runtime.NumCPU(), runs)
	// The control arm runs fewer times on purpose: what it has to catch is a
	// DISAGREEMENT with the arm above, and at base that disagreement was
	// total (0 of 20 answered free, 20 of 20 at GOMAXPROCS=1). The many-run
	// arm is the one that has to catch a minority — `c_str` answered 2 of 20.
	// A serial run of this shape costs about a second.
	serialRuns := runs / 4
	if serialRuns < 3 {
		serialRuns = 3
	}
	if parA != 0 && parR != 0 {
		t.Fatalf("the same join at %d KiB reached BOTH dispositions with the scheduler free: %d of %d "+
			"runs answered and %d refused. At a fixed budget on fixed data a query has one outcome; "+
			"this is #789", spillMxBudget/1024, parA, parA+parR, parR)
	}
	serA, serR := answersAt(t, 1, serialRuns)
	if serA != 0 && serR != 0 {
		t.Fatalf("the same join at %d KiB reached BOTH dispositions at GOMAXPROCS=1: %d answered, "+
			"%d refused", spillMxBudget/1024, serA, serR)
	}
	if (parA > 0) != (serA > 0) {
		t.Fatalf("the parallel arm and the GOMAXPROCS=1 control DISAGREE at %d KiB: parallel answered "+
			"%d of %d, control answered %d of %d. The query's outcome is being decided by the "+
			"scheduler, which is #789", spillMxBudget/1024, parA, parA+parR, serA, serA+serR)
	}
	if !engaged {
		t.Fatal("no scan read row group at a time — the dispositions above agreed, but not because of " +
			"the property under test")
	}
	t.Logf("%s at %d KiB: %d/%d runs answered with the scheduler free, %d/%d at GOMAXPROCS=1",
		col, spillMxBudget/1024, parA, runs, serA, serialRuns)
}

// countingStore counts what the scan asks the object store for. Not a mock —
// it wraps a real MemStore and forwards every call; only the tally is new.
type countingStore struct {
	objstore.Store
	mu      sync.Mutex
	gets    map[string]int
	readers map[string]int
}

func newCountingStore() *countingStore {
	return &countingStore{Store: objstore.NewMemStore(), gets: map[string]int{}, readers: map[string]int{}}
}

func (s *countingStore) Get(ctx context.Context, bucket, key string) (io.ReadCloser, objstore.ObjectInfo, error) {
	s.mu.Lock()
	s.gets[key]++
	s.mu.Unlock()
	return s.Store.Get(ctx, bucket, key)
}

func (s *countingStore) GetReaderAt(ctx context.Context, bucket, key string) (objstore.ReaderAtCloser, int64, error) {
	ras, ok := s.Store.(objstore.ReaderAtStore)
	if !ok {
		return nil, 0, fmt.Errorf("no ReaderAt")
	}
	s.mu.Lock()
	s.readers[key]++
	s.mu.Unlock()
	return ras.GetReaderAt(ctx, bucket, key)
}

func (s *countingStore) StoreID() string { return objstore.StoreID(s.Store) }

func (s *countingStore) count(key string) (gets, readers int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets[key], s.readers[key]
}

func (s *countingStore) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets, s.readers = map[string]int{}, map[string]int{}
}

func (s *countingStore) dataFile(t *testing.T) string {
	t.Helper()
	objs, err := s.Store.List(context.Background(), "test", objstore.ListOptions{Prefix: "tables/"})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	for _, o := range objs {
		if strings.HasSuffix(o.Key, ".parquet") {
			return o.Key
		}
	}
	t.Fatal("no parquet file in the fixture")
	return ""
}

// TestAScanIssuesOneObjectGetPerFile.
//
// Reading a file one row group at a time is worth nothing if it costs one
// request per row group. Non-local stores keep the whole-file GET because one
// object GET beats per-chunk ranged GETs (docs/design/scan-pread-reads.md),
// and at SF100 request COUNT is a first-class metric, not a rounding error —
// so the row-group design streams ONE body and cuts it up, and this is what
// says so. A per-row-group or per-column-chunk read would show up here as five
// GETs for a five-row-group file.
func TestAScanIssuesOneObjectGetPerFile(t *testing.T) {
	ctx := context.Background()
	st := newCountingStore()
	db, err := Open(ctx, Config{Store: st, Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	schema := typematrix.Schema()
	if err := db.CreateTable(ctx, typematrix.Table, schema, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	ing := db.NewIngester(typematrix.Table, schema, nil, ingest.Config{
		MaxBufferRows: typematrix.Rows + 1, RowGroupSize: typematrix.RowGroup,
	})
	if err := ing.Ingest(ctx, typematrix.Data(typematrix.Rows)); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	key := st.dataFile(t)

	for _, q := range []struct{ name, sql string }{
		{"full scan", `SELECT COUNT(*) AS n, MIN(c_str) AS m FROM ` + typematrix.Table},
		{"projected scan", `SELECT SUM(id) AS s FROM ` + typematrix.Table},
	} {
		t.Run(q.name, func(t *testing.T) {
			st.reset()
			rgBefore, _ := physical.RowGroupLoadStats()
			if _, err := tmRun(ctx, db, q.sql); err != nil {
				t.Fatalf("%s: %v", q.name, err)
			}
			if rgAfter, _ := physical.RowGroupLoadStats(); rgAfter == rgBefore {
				t.Fatal("the scan did not read row group at a time — this gate proved nothing")
			}
			gets, _ := st.count(key)
			if gets != 1 {
				t.Fatalf("%s issued %d Get calls for one file, want exactly 1. The row-group read "+
					"streams ONE body and cuts it up; one request per row group would reverse the "+
					"recorded whole-file-GET decision (docs/design/scan-pread-reads.md)", q.name, gets)
			}
		})
	}
}
