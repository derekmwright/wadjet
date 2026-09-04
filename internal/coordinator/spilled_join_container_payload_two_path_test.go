package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

// The budgets this gate runs its spilled arms at. Two of them, because #865
// was BUDGET-GRADED: at 512 KiB the ROW column was lost and ARRAY and MAP
// survived; at 256 KiB all three were lost. A single pin records one point on
// a curve, so both points are cells.
const (
	sjcBudgetHi = 512 * 1024
	sjcBudgetLo = 256 * 1024
	// Five, per ADR-0027: a single passing spilled run proves nothing. Which
	// probe rows reach the replay follows how many partitions were evicted,
	// and that follows tracker timing.
	sjcReplicas = 5
)

// sjcBudgetedStandalone is the single-process engine at `budget`, over the
// fixture tmdStandalone loads.
func sjcBudgetedStandalone(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
	t.Helper()
	db, err := wadjet.Open(ctx, wadjet.Config{
		Store: objstore.NewMemStore(), Bucket: "test",
		MemoryBudget: budget, SpillDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("open budgeted standalone: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	for _, tbl := range tmdTables() {
		if tmdStoresAReservedName(tbl) {
			continue // the DDL door refuses its schema; no cell here names it
		}
		if err := db.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
			t.Fatalf("create %s: %v", tbl.name, err)
		}
		ing := db.NewIngester(tbl.name, tbl.schema, nil, ingest.Config{
			MaxBufferRows: len(tbl.rows) + 1, RowGroupSize: typematrix.RowGroup,
		})
		if err := ing.Ingest(ctx, tbl.rows); err != nil {
			t.Fatalf("ingest %s: %v", tbl.name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", tbl.name, err)
		}
	}
	return db
}

// A join under a memory budget answers a container column the way the
// unbudgeted one does — on the padded side, on the preserved side, and for
// every container carrier (#865).
//
// The defect: a spilled build's batches are written to a columnar run file
// whose per-column header carried NAME, TYPE, NULLABLE and — for DECIMAL only
// — (p,s). `readColumnarBatch` rebuilt a `parquet.Column` from exactly that,
// so a ROW came back declared with no Fields, an ARRAY with no ElementType, a
// MAP with no entry ROW and a VECTOR with no Dimension.
// `buildTempJoinFromBatches` adopts that as the replay join's `buildSchema`
// and `joinOutputSchemaWithMapping` puts it on the output batch, so every
// operator ABOVE the join that allocates from the schema
// (`batch.NewColumnVector`) minted a container vector with no children and
// dropped the column. The VALUES were never lost — `Vector.CopyValueFrom`
// mints children lazily — which is why a gate reading the operator's own rows
// saw nothing, and why the symptom was a MATCHED row reading back NULL.
//
// It is budget-graded because what reaches the replay is what the eviction
// routed to disk: more evictions, more shapes through the broken declaration.
// So the ENGAGEMENT number is asserted per cell, not assumed (ADR-0027
// decision 5) — a cell whose build never evicts is comparing two in-memory
// answers and says so.
//
// RIGHT and FULL over a build this size do not reach the replay HERE: a RIGHT
// join's build reserves its whole hinted size up front and refuses loudly at
// both budgets (a legal disposition — ADR-0006 never guesses, it refuses), and
// an INNER join on an expression key is executed as a CROSS join, whose build
// cannot be grace-partitioned at all. Their replay DECLARATION is gated at the
// operator instead, where the spill can be forced for every join type:
// `exec.TestASpilledJoinDeclaresItsContainerColumns`.
func TestASpilledJoinAnswersItsContainerColumns(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up two embedded NATS clusters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	arms := dajArms(t, ctx)
	hi := sjcBudgetedStandalone(t, ctx, sjcBudgetHi)
	lo := sjcBudgetedStandalone(t, ctx, sjcBudgetLo)

	nested := typematrix.Nested
	// Every container carrier the type system has, in one select list: ROW
	// (Fields), ARRAY (ElementType), MAP (an entry ROW inside ElementType)
	// and VECTOR (Dimension). A cell that named only ROW would have passed
	// the day the ARRAY carrier broke.
	cols := func(q string) string {
		return q + ".c_row, " + q + ".c_arr, " + q + ".c_map, " + q + ".c_vec"
	}
	for _, tc := range []struct {
		name string
		sql  string
		cols []string
		want string
		// mustEvict says this cell's build is big enough that a grace
		// eviction has to happen at both budgets. A cell that cannot evict
		// names why instead.
		mustEvict bool
		whyNot    string
	}{
		{
			name: "left/container-on-the-padded-side",
			sql: "SELECT d.id AS did, " + cols("n") + " FROM decpair d LEFT JOIN " + nested +
				" n ON d.id = n.id WHERE d.id < 4 ORDER BY d.id",
			cols:      []string{"did", "c_row", "c_arr", "c_map", "c_vec"},
			mustEvict: true,
		},
		{
			name: "left/container-on-the-preserved-side",
			sql: "SELECT x.id AS xid, " + cols("x") + " FROM " + nested +
				" x LEFT JOIN decpair d ON x.id = d.id WHERE x.id < 4 ORDER BY x.id",
			cols:   []string{"xid", "c_row", "c_arr", "c_map", "c_vec"},
			whyNot: "the build is decpair's nine rows, which no budget here evicts",
		},
		{
			name: "left/self-join-carries-both-sides",
			sql: "SELECT x.id AS xid, x.c_row AS xrow, y.c_row AS yrow, y.c_arr AS yarr FROM " +
				nested + " x LEFT JOIN " + nested + " y ON x.id = y.id + 1 WHERE x.id < 4 ORDER BY x.id",
			cols:      []string{"xid", "xrow", "yrow", "yarr"},
			mustEvict: true,
		},
		{
			name: "full/self-join-carries-both-sides",
			sql: "SELECT COALESCE(x.id, y.id + 1) AS k, x.c_row AS xrow, y.c_row AS yrow, " +
				"y.c_vec AS yvec FROM " + nested + " x FULL JOIN " + nested +
				" y ON x.id = y.id + 1 WHERE COALESCE(x.id, y.id + 1) < 4 ORDER BY COALESCE(x.id, y.id + 1)",
			cols:      []string{"k", "xrow", "yrow", "yvec"},
			mustEvict: true,
		},
		{
			name: "right/container-on-the-padded-side",
			sql: "SELECT d.id AS did, " + cols("x") + " FROM " + nested +
				" x RIGHT JOIN decpair d ON x.id = d.id WHERE d.id < 4 ORDER BY d.id",
			cols:   []string{"did", "c_row", "c_arr", "c_map", "c_vec"},
			whyNot: "the build is decpair's nine rows, which no budget here evicts",
		},
		{
			name: "full/both-sides-padded",
			sql: "SELECT COALESCE(x.id, d.id) AS k, " + cols("x") + " FROM " + nested +
				" x FULL JOIN decpair d ON x.id = d.id WHERE COALESCE(x.id, d.id) < 4 " +
				"ORDER BY COALESCE(x.id, d.id)",
			cols:   []string{"k", "c_row", "c_arr", "c_map", "c_vec"},
			whyNot: "the build is decpair's nine rows, which no budget here evicts",
		},
		{
			// THE BOUNDING CONTROL. An INNER join agrees on every arm at base
			// too — the defect is the OUTER payload's — so this cell is here
			// to catch a fix that moves a right answer.
			name: "inner/ctl-the-same-projection-under-an-inner-join",
			sql: "SELECT d.id AS did, " + cols("n") + " FROM decpair d JOIN " + nested +
				" n ON d.id = n.id WHERE d.id < 4 ORDER BY d.id",
			cols:   []string{"did", "c_row", "c_arr", "c_map", "c_vec"},
			whyNot: "an inner join may build on decpair's nine rows, which no budget here evicts",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			// The ORACLE for this arc is the in-memory arm: a container's
			// rendering has no PostgreSQL spelling to compare against, and
			// every disposition here is either the unbudgeted rows or a loud
			// refusal (E5's report records the same reasoning). The digest is
			// pinned as a literal below so a regression shared by every arm
			// cannot make them agree wrongly.
			ref, err := arms[0].run(tc.sql)
			if err != nil {
				t.Fatalf("the unbudgeted single arm refused: %v\n  SQL: %s", err, tc.sql)
			}
			want := dajDigest(ref, tc.cols)
			if tc.want != "" && want != tc.want {
				t.Fatalf("the unbudgeted arm answers\n  %s\nthis gate records\n  %s\n  SQL: %s",
					want, tc.want, tc.sql)
			}
			if len(ref.Rows) == 0 {
				t.Fatalf("the reference answer is empty, so this cell asserts nothing\n  SQL: %s", tc.sql)
			}

			for _, arm := range arms[1:] {
				got, err := arm.run(tc.sql)
				if err != nil {
					t.Errorf("the %s arm refused: %v\n  SQL: %s", arm.name, err, tc.sql)
					continue
				}
				if d := dajDigest(got, tc.cols); d != want {
					t.Errorf("the %s arm answered\n  %s\nthe unbudgeted arm answers\n  %s\n  SQL: %s",
						arm.name, d, want, tc.sql)
				}
			}

			for _, b := range []struct {
				name string
				db   *wadjet.DB
			}{{"spilled (512 KiB)", hi}, {"spilled (256 KiB)", lo}} {
				evicted := int64(0)
				for i := 0; i < sjcReplicas; i++ {
					before := exec.JoinPartitionsEvicted.Load()
					var got *oracle.Result
					got, err := tmdRunSingle(ctx, b.db, tc.sql)
					evicted += exec.JoinPartitionsEvicted.Load() - before
					if err != nil {
						t.Errorf("the %s arm refused on run %d: %v\n  SQL: %s",
							b.name, i, err, tc.sql)
						continue
					}
					if d := dajDigest(got, tc.cols); d != want {
						t.Errorf("the %s arm answered, on run %d of %d,\n  %s\nthe unbudgeted "+
							"arm answers\n  %s\n  SQL: %s", b.name, i, sjcReplicas, d, want, tc.sql)
					}
				}
				switch {
				case tc.mustEvict && evicted == 0:
					t.Errorf("the %s arm evicted NO grace partition over %d runs, so this cell "+
						"compared two in-memory answers (ADR-0027 decision 5)\n  SQL: %s",
						b.name, sjcReplicas, tc.sql)
				case !tc.mustEvict:
					t.Logf("%s: %d evictions over %d runs (%s)", b.name, evicted, sjcReplicas, tc.whyNot)
				default:
					t.Logf("%s: %d evictions over %d runs", b.name, evicted, sjcReplicas)
				}
			}
		})
	}
	_ = fmt.Sprint
}
