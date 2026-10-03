// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// sjcTables is the five-arm fixture for #1359: sjc_p the preserved side
// (40 rows; every key NULL on every seventh row and repeating every 17, so
// NULL keys and duplicate keys both reach the build), sjc_e the empty table,
// sjc_f the table the filtered and LIMIT 0 empty sides read. sjcPGFixture in
// the arc's gen_pg_cells_coord.py is the same rows as PostgreSQL DDL.
func sjcTables() []tmdTable {
	col := func(n string, ty parquet.TypeID) parquet.Column {
		return parquet.Column{Name: n, Type: ty, Nullable: true}
	}
	p := tmdTable{name: "sjc_p", schema: parquet.Schema{Columns: []parquet.Column{
		col("id", parquet.TypeInt64), col("k_int", parquet.TypeInt32), col("k_big", parquet.TypeInt64),
		col("k_txt", parquet.TypeString), col("k_date", parquet.TypeDate),
		{Name: "v_dec", Type: parquet.TypeDecimal, Nullable: true, Precision: 18, Scale: 4},
		{Name: "v_arr", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
	}}}
	for i := 1; i <= 40; i++ {
		r := map[string]any{"id": int64(i), "v_dec": float64(i) + 0.25,
			"v_arr": []any{fmt.Sprintf("a%d", i), fmt.Sprintf("b%d", i)}}
		if i%7 == 3 {
			r["k_int"], r["k_big"], r["k_txt"], r["k_date"] = nil, nil, nil, nil
		} else {
			k := i % 17
			r["k_int"], r["k_big"] = int32(k), int64(k)+10_000_000_000
			r["k_txt"], r["k_date"] = fmt.Sprintf("k%d", k), fmt.Sprintf("2000-01-%02d", k+1)
		}
		p.rows = append(p.rows, r)
	}
	eSchema := parquet.Schema{Columns: []parquet.Column{
		col("k_int", parquet.TypeInt32), col("k_big", parquet.TypeInt64), col("k_txt", parquet.TypeString),
		col("k_date", parquet.TypeDate), col("ey", parquet.TypeInt32),
	}}
	e := tmdTable{name: "sjc_e", schema: eSchema}
	f := tmdTable{name: "sjc_f", schema: eSchema}
	for i := 1; i <= 10; i++ {
		f.rows = append(f.rows, map[string]any{"k_int": int32(-i), "k_big": int64(-i),
			"k_txt": fmt.Sprintf("f%d", i), "k_date": fmt.Sprintf("1990-01-%02d", i), "ey": int32(i)})
	}
	return []tmdTable{p, e, f}
}

type sjcCell struct{ name, sql, want string }

func sjcCells(t *testing.T) []sjcCell {
	t.Helper()
	f, err := os.Open("testdata/arc_sj_empty_side_join_arms_pg17.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []sjcCell
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		p := strings.SplitN(line, "\t", 3)
		if len(p) != 3 {
			t.Fatalf("malformed cell line %q", line)
		}
		out = append(out, sjcCell{p[0], p[1], p[2]})
	}
	if len(out) != 128 {
		t.Fatalf("read %d cells, want 128", len(out))
	}
	return out
}

// sjcRender is the arc's one rendering (gen_pg_cells_coord.py renders
// PostgreSQL's the same way): each row's text cells joined by '|', NULL
// spelled NULL, rows sorted, then the count and the MD5 of the lines.
func sjcRender(res *oracle.Result) string {
	var rows []string
	for _, r := range brCells(res) {
		parts := make([]string, len(r))
		for i, v := range r {
			if v == nil {
				parts[i] = "NULL"
			} else {
				parts[i] = fmt.Sprint(v)
			}
		}
		rows = append(rows, strings.Join(parts, "|"))
	}
	sort.Strings(rows)
	sum := md5.Sum([]byte(strings.Join(rows, "\n")))
	return fmt.Sprintf("rows=%d %s", len(rows), hex.EncodeToString(sum[:]))
}

func sjcStandalone(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
	t.Helper()
	cfg := wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"}
	if budget > 0 {
		cfg.MemoryBudget = budget
		cfg.SpillDir = t.TempDir()
	}
	db, err := wadjet.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("open standalone: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	for _, tbl := range sjcTables() {
		if err := db.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
			t.Fatalf("create %s: %v", tbl.name, err)
		}
		if len(tbl.rows) == 0 {
			continue
		}
		ing := db.NewIngester(tbl.name, tbl.schema, nil, ingest.Config{MaxBufferRows: len(tbl.rows) + 1, RowGroupSize: 8})
		if err := ing.Ingest(ctx, tbl.rows); err != nil {
			t.Fatalf("ingest %s: %v", tbl.name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", tbl.name, err)
		}
	}
	return db
}

// A SPILLED RIGHT / FULL JOIN OVER AN EMPTY SIDE KEEPS THE PRESERVED SIDE'S
// VALUES, ON EVERY ARM (#1359). {RIGHT, FULL, LEFT, INNER} × the side written
// first (`ep` builds the preserved side) × the empty side {table, filtered,
// LIMIT 0 subquery, VALUES under WHERE false} × the key {INTEGER, BIGINT,
// VARCHAR, DATE}, every row's cells rendered as text by the query, against
// PostgreSQL 17.11 (testdata/arc_sj_empty_side_join_arms_pg17.tsv).
//
// The spilled arm forces a build-partition eviction on every arriving build
// batch (ADR-0027 decision 6) and ASSERTS it on every outer cell that builds
// the preserved side. At the arc's base that arm answered every RIGHT and
// FULL `ep` cell with the preserved side's values NULL or shifted into the
// empty side's columns: the partition replay had no probe-side schema when no
// probe row ever arrived. The DAG arms are the control (the stage DAG's
// partitioned join runs per task; its workers keep the knob unarmed here).
func TestArcSJEmptySideJoinEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the empty-side join table")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	cells := sjcCells(t)

	stand := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, sjcTables())
		if wcfg != nil {
			return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
		}
		return tmdCoordinator(t, ctx, infra, opts...)
	}
	single, spilled := sjcStandalone(t, ctx, 0), sjcStandalone(t, ctx, 512*1024)
	coord := stand(nil)
	coordB := stand(nil, func(c *Config) { c.BroadcastBytesOverride = 1 })
	coordM := stand(func(w *worker.Config) { w.MorselWorkers = 4 })
	type arm struct {
		name   string
		run    func(string) (*oracle.Result, error)
		forced bool
	}
	arms := []arm{
		{"single", func(s string) (*oracle.Result, error) { return tmdRunSingle(ctx, single, s) }, false},
		{"spilled512k-forced", func(s string) (*oracle.Result, error) { return tmdRunSingle(ctx, spilled, s) }, true},
		{"dag", func(s string) (*oracle.Result, error) { return tmdRunDAG(ctx, coord, s) }, false},
		{"dag-shuffled", func(s string) (*oracle.Result, error) { return tmdRunDAG(ctx, coordB, s) }, false},
		{"dag-morsel4", func(s string) (*oracle.Result, error) { return tmdRunDAG(ctx, coordM, s) }, false},
	}
	nonEmpty, engaged := 0, 0
	for _, c := range cells {
		if !strings.HasPrefix(c.want, "rows=0 ") {
			nonEmpty++
		}
		t.Run(c.name, func(t *testing.T) {
			for _, a := range arms {
				var before int64
				if a.forced {
					prev := exec.ForceJoinPartitionEvictEvery(1)
					before = exec.JoinPartitionsEvicted.Load()
					defer exec.ForceJoinPartitionEvictEvery(prev)
				}
				res, err := a.run(c.sql)
				if a.forced {
					exec.ForceJoinPartitionEvictEvery(0)
					parts := strings.Split(c.name, "/")
					if parts[1] == "ep" && parts[0] != "inner" {
						if exec.JoinPartitionsEvicted.Load() == before {
							t.Errorf("%s: no build partition was evicted — the cell compared two in-memory runs", a.name)
						} else {
							engaged++
						}
					}
				}
				if err != nil {
					t.Errorf("%s\n  arm  %s\n  refused: %v\n  want %s (PostgreSQL 17.11)", c.sql, a.name, err, c.want)
					continue
				}
				if got := sjcRender(res); got != c.want {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.sql, a.name, got, c.want)
				}
			}
		})
	}
	if nonEmpty != 64 || engaged != 48 {
		t.Fatalf("%d cells with rows (want 64), %d spill-engaged outer cells (want 48): the table must discriminate", nonEmpty, engaged)
	}
}
