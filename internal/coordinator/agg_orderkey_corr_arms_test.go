// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// MIN_BY/MAX_BY'S ORDERING KEY AND CORR ON DEGENERATE INPUT, ON ALL FIVE ARMS
// (#1694, round 2).
//
// Two kernel answers moved for BARE columns, so both are held on the single
// and spilled embedded arms and on the three stage-DAG arms (whose CORR merges
// partial covariance states, and whose MIN_BY runs whole-input on one task):
//
//   - MIN_BY/MAX_BY compare a key through a float64 (exec.orderKeyFloat64).
//     TEXT, BYTEA, IPV6, CIDR, UUID and the containers have no such reading,
//     and every row was skipped: the call answered NULL (ROW was 42883). They
//     are now refused 0A000 at plan time, bare or computed, ungrouped and
//     grouped. The thirteen types it orders answer as `ORDER BY key LIMIT 1`
//     over the rows whose value and key are not NULL does, NaN greatest and
//     -Inf least (PostgreSQL's float order); ties are not held to one row.
//     A BIGINT or DECIMAL key is compared exactly (exec.orderKeyExact): two
//     BIGINT keys past 2^53 rounded to one double and tied.
//   - CORR answered 0 where PostgreSQL's float8_corr answers NULL: an
//     argument whose squared deviations sum to zero (all x equal, all y equal,
//     or both, including after NULL pairs are dropped). COVAR_SAMP and
//     COVAR_POP agreed with PostgreSQL 17.11 on every degenerate input at base
//     and do now; each want below is PostgreSQL's (the st table is loaded
//     there as `st`).
//
// A computed key or argument is a residual on the stage-DAG arms: the worker's
// pre-aggregate projection does not materialize a later argument, so those
// cells are refused there (`is not a column of its input`) at base and here.
// A refused key type is refused at plan time, before the DAG, on every arm.
func ocTables() []tmdTable {
	ranks := []int{5, 9, 1, 7, 3} // ids 1..5; id 6 carries NULL in every key
	keyCols := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "g", Type: parquet.TypeInt32},
		{Name: "k_bool", Type: parquet.TypeBool, Nullable: true},
		{Name: "k_i32", Type: parquet.TypeInt32, Nullable: true},
		{Name: "k_i64", Type: parquet.TypeInt64, Nullable: true},
		{Name: "k_f32", Type: parquet.TypeFloat32, Nullable: true},
		{Name: "k_f64", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "k_str", Type: parquet.TypeString, Nullable: true},
		{Name: "k_bytes", Type: parquet.TypeBytes, Nullable: true},
		{Name: "k_ts", Type: parquet.TypeTimestamp, Nullable: true},
		{Name: "k_ipv4", Type: parquet.TypeIPv4, Nullable: true},
		{Name: "k_ipv6", Type: parquet.TypeIPv6, Nullable: true},
		{Name: "k_cidr", Type: parquet.TypeCIDR, Nullable: true},
		{Name: "k_mac", Type: parquet.TypeMAC, Nullable: true},
		{Name: "k_port", Type: parquet.TypePort, Nullable: true},
		{Name: "k_proto", Type: parquet.TypeProtocol, Nullable: true},
		{Name: "k_dur", Type: parquet.TypeDuration, Nullable: true},
		{Name: "k_uuid", Type: parquet.TypeUUID, Nullable: true},
		{Name: "k_date", Type: parquet.TypeDate, Nullable: true},
		{Name: "k_dec", Type: parquet.TypeDecimal, Nullable: true, Precision: 18, Scale: 4},
		{Name: "k_vec", Type: parquet.TypeVector, Nullable: true, Dimension: 2},
		{Name: "k_arr", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeInt64, Nullable: true}},
		{Name: "k_row", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
			{Name: "a", Type: parquet.TypeInt64, Nullable: true},
		}},
		{Name: "k_map", Type: parquet.TypeMap, Nullable: true,
			ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "key", Type: parquet.TypeString},
				{Name: "value", Type: parquet.TypeInt64, Nullable: true},
			}}},
	}
	var kt []map[string]any
	for i := 1; i <= 6; i++ {
		r := map[string]any{"id": int64(i), "g": int32(i % 2)}
		for _, c := range keyCols[2:] {
			r[c.Name] = nil
		}
		if i <= 5 {
			k := ranks[i-1]
			r["k_i32"] = int32(k)
			r["k_i64"] = int64(k) * 1_000_000_000_000
			r["k_f32"] = float32(k) / 2
			r["k_f64"] = float64(k) / 3
			r["k_str"] = fmt.Sprintf("s%d", k)
			r["k_bytes"] = []byte(fmt.Sprintf("b%d", k))
			r["k_ts"] = int64(1_700_000_000_000 + int64(k)*61_000)
			r["k_ipv4"] = fmt.Sprintf("10.0.%d.%d", k, k)
			r["k_ipv6"] = fmt.Sprintf("2001:db8::%x", k)
			r["k_cidr"] = fmt.Sprintf("192.168.%d.0/24", k)
			r["k_mac"] = fmt.Sprintf("aa:bb:cc:00:%02x:%02x", k, k)
			r["k_port"] = int32(1000 + k)
			r["k_proto"] = int32(k)
			r["k_dur"] = int64(k) * 1_000_000
			r["k_uuid"] = fmt.Sprintf("00000000-0000-4000-8000-%012x", k)
			r["k_date"] = fmt.Sprintf("2024-01-%02d", k)
			r["k_dec"] = float64(k) + 0.25
			r["k_vec"] = []float32{float32(k), 1}
			r["k_arr"] = []any{int64(k)}
			r["k_row"] = map[string]any{"a": int64(k)}
			r["k_map"] = map[string]any{"a": int64(k)}
		}
		// BOOL has two values, so only two rows carry one: no tie.
		switch i {
		case 2:
			r["k_bool"] = true
		case 3:
			r["k_bool"] = false
		}
		kt = append(kt, r)
	}
	f := func(v float64) any { return v }
	fvals := []any{f(0), f(math.NaN()), f(-1), f(math.Copysign(0, -1)), nil, f(0), f(math.Inf(-1))}
	var kf []map[string]any
	for i, v := range fvals {
		r := map[string]any{"id": int64(i + 1), "g": int32((i + 1) % 2), "f": v, "r": nil}
		if v != nil {
			r["r"] = float32(v.(float64))
		}
		kf = append(kf, r)
	}
	// The degenerate scenarios: 1 one row; 2 all x equal; 3 all y equal; 4
	// both; 5 a NULL in one argument of some rows; 6 NULLs leave one complete
	// pair; 7 NULLs leave x equal; 8 a non-degenerate control.
	sc := map[int][][2]any{
		1: {{1, 2}},
		2: {{5, 1}, {5, 2}, {5, 4}},
		3: {{1, 7}, {2, 7}, {4, 7}},
		4: {{5, 7}, {5, 7}, {5, 7}},
		5: {{1, 2}, {2, nil}, {nil, 5}, {3, 6}, {4, 9}},
		6: {{1, 2}, {2, nil}, {nil, 4}},
		7: {{5, 1}, {nil, 2}, {5, 4}, {6, nil}},
		8: {{1, 2}, {2, 4}, {3, 7}},
	}
	var st []map[string]any
	id := 0
	for s := 1; s <= 8; s++ {
		for _, p := range sc[s] {
			id++
			r := map[string]any{"id": int32(id), "sc": int32(s), "x": nil, "y": nil}
			if p[0] != nil {
				r["x"] = int32(p[0].(int))
			}
			if p[1] != nil {
				r["y"] = int32(p[1].(int))
			}
			st = append(st, r)
		}
	}
	return []tmdTable{
		{name: "kp1694", schema: parquet.Schema{Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "k", Type: parquet.TypeInt64},
		}}, rows: []map[string]any{
			{"id": int64(1), "k": int64(1 << 53)},
			{"id": int64(2), "k": int64(1<<53 + 1)},
			{"id": int64(3), "k": int64(1<<53 - 1)},
		}},
		{name: "kt1694", schema: parquet.Schema{Columns: keyCols}, rows: kt},
		{name: "kf1694", schema: parquet.Schema{Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "g", Type: parquet.TypeInt32},
			{Name: "f", Type: parquet.TypeFloat64, Nullable: true},
			{Name: "r", Type: parquet.TypeFloat32, Nullable: true},
		}}, rows: kf},
		{name: "st1694", schema: parquet.Schema{Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt32},
			{Name: "sc", Type: parquet.TypeInt32},
			{Name: "x", Type: parquet.TypeInt32, Nullable: true},
			{Name: "y", Type: parquet.TypeInt32, Nullable: true},
		}}, rows: st},
	}
}

func ocArms(t *testing.T, ctx context.Context) []acArm {
	t.Helper()
	tables := ocTables()
	open := func(budget int64) *wadjet.DB {
		db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test",
			MemoryBudget: budget, SpillDir: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		for _, tbl := range tables {
			if err := db.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
				t.Fatalf("create %s: %v", tbl.name, err)
			}
			ing := db.NewIngester(tbl.name, tbl.schema, nil, ingest.Config{
				MaxBufferRows: len(tbl.rows) + 1, RowGroupSize: 3,
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
	single, spilled := open(0), open(512*1024)
	dag := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, tables)
		return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
	}
	coord := dag(nil)
	coordB := dag(nil, func(c *Config) { c.BroadcastBytesOverride = 1 })
	coordM := dag(func(w *worker.Config) { w.MorselWorkers = 4 })
	runSingle := func(db *wadjet.DB, drain bool) func(string) (*oracle.Result, []parquet.Column, error) {
		return func(sql string) (*oracle.Result, []parquet.Column, error) {
			if drain {
				prev := exec.ForceAggDrainEvery(1)
				defer exec.ForceAggDrainEvery(prev)
			}
			res, err := tmdRunSingle(ctx, db, sql)
			if err != nil {
				return nil, nil, err
			}
			cols, err := tmdRunSingleDeclAll(ctx, db, sql)
			return res, cols, err
		}
	}
	runDAG := func(c *Coordinator) func(string) (*oracle.Result, []parquet.Column, error) {
		return func(sql string) (*oracle.Result, []parquet.Column, error) {
			res, err := tmdRunDAG(ctx, c, sql)
			if err != nil {
				return nil, nil, err
			}
			cols, err := tmdRunDAGDeclAll(ctx, c, sql)
			return res, cols, err
		}
	}
	return []acArm{
		{"single", runSingle(single, false)},
		{"spilled", runSingle(spilled, true)},
		{"dag", runDAG(coord)},
		{"dag-shuffled", runDAG(coordB)},
		{"dag-morsel4", runDAG(coordM)},
	}
}

type ocCell struct {
	name, sql string
	// wants are the answers any arm may give (more than one only where the
	// call's tie is not held to one row); "ERR <state> <words>" is a refusal.
	wants []string
	// dagRefuses: the stage DAG refuses this computed later argument
	// (`is not a column of its input`), at base and here.
	dagRefuses bool
}

func ocCells() []ocCell {
	var cells []ocCell
	ordered := []string{"k_i32", "k_i64", "k_f32", "k_f64", "k_ts", "k_ipv4", "k_mac",
		"k_port", "k_proto", "k_dur", "k_date", "k_dec"}
	refused := map[string]string{"k_str": "text", "k_bytes": "bytea", "k_ipv6": "inet",
		"k_cidr": "inet", "k_uuid": "uuid", "k_vec": "vector", "k_arr": "array",
		"k_row": "record", "k_map": "map"}
	for _, fn := range []string{"min_by", "max_by"} {
		// ids 1..5 rank 5, 9, 1, 7, 3; id 6 is NULL. g = id % 2.
		u, g := "rows=1 3", "rows=2 0,4 | 1,3"
		if fn == "max_by" {
			u, g = "rows=1 2", "rows=2 0,2 | 1,1"
		}
		for _, c := range ordered {
			for _, k := range []struct {
				form string
				dag  bool
			}{{c, false}, {"CASE WHEN id > 0 THEN " + c + " END", true}} {
				cells = append(cells,
					ocCell{fn + "/" + c + "/U/" + fmt.Sprint(k.dag), fmt.Sprintf("SELECT %s(id, %s) FROM kt1694", fn, k.form), []string{u}, k.dag},
					ocCell{fn + "/" + c + "/G/" + fmt.Sprint(k.dag), fmt.Sprintf("SELECT g, %s(id, %s) FROM kt1694 GROUP BY g", fn, k.form), []string{g}, k.dag})
			}
		}
		bu, bg := "rows=1 3", "rows=2 0,2 | 1,3"
		if fn == "max_by" {
			bu = "rows=1 2"
		}
		cells = append(cells,
			ocCell{fn + "/k_bool/U", fmt.Sprintf("SELECT %s(id, k_bool) FROM kt1694", fn), []string{bu}, false},
			ocCell{fn + "/k_bool/G", fmt.Sprintf("SELECT g, %s(id, k_bool) FROM kt1694 GROUP BY g", fn), []string{bg}, false})
		for c, ty := range refused {
			want := []string{"ERR 0A000 " + fn + " over an ordering key of type " + ty}
			for _, k := range []string{c, "CASE WHEN id > 0 THEN " + c + " END"} {
				cells = append(cells,
					ocCell{fn + "/" + c + "/U/" + k, fmt.Sprintf("SELECT %s(id, %s) FROM kt1694", fn, k), want, false},
					ocCell{fn + "/" + c + "/G/" + k, fmt.Sprintf("SELECT g, %s(id, %s) FROM kt1694 GROUP BY g", fn, k), want, false})
			}
		}
		for _, k := range []string{"upper(k_str)", "'k'", "CAST(k_i32 AS TEXT)"} {
			want := []string{"ERR 0A000 " + fn + " over an ordering key of type text"}
			cells = append(cells,
				ocCell{fn + "/text/U/" + k, fmt.Sprintf("SELECT %s(id, %s) FROM kt1694", fn, k), want, false},
				ocCell{fn + "/text/G/" + k, fmt.Sprintf("SELECT g, %s(id, %s) FROM kt1694 GROUP BY g", fn, k), want, false})
		}
		// Floats: 1 0.0, 2 NaN, 3 -1, 4 -0.0, 5 NULL, 6 0.0, 7 -Inf. NaN is
		// greatest and -Inf least; -0.0 and 0.0 are one value, so g = 0's
		// minimum is either.
		for _, c := range []string{"f", "r"} {
			fu, fg := []string{"rows=1 7"}, []string{"rows=2 0,4 | 1,7", "rows=2 0,6 | 1,7"}
			if fn == "max_by" {
				fu, fg = []string{"rows=1 2"}, []string{"rows=2 0,2 | 1,1"}
			}
			cells = append(cells,
				ocCell{fn + "/float/U/" + c, fmt.Sprintf("SELECT %s(id, %s) FROM kf1694", fn, c), fu, false},
				ocCell{fn + "/float/G/" + c, fmt.Sprintf("SELECT g, %s(id, %s) FROM kf1694 GROUP BY g", fn, c), fg, false})
		}
	}
	// BIGINT keys past 2^53 round to one double: compared as a float64, 2^53
	// and 2^53 + 1 tied and max_by answered 1, the first row read (at base,
	// and computed keys at round 1). The key is compared exactly now.
	cells = append(cells,
		ocCell{"exact/max_by/int64-past-2^53", "SELECT max_by(id, k) FROM kp1694", []string{"rows=1 2"}, false},
		ocCell{"exact/min_by/int64-past-2^53", "SELECT min_by(id, k) FROM kp1694", []string{"rows=1 3"}, false},
		ocCell{"exact/max_by/int64-past-2^53/computed", "SELECT max_by(id, k + 0) FROM kp1694", []string{"rows=1 2"}, true})
	// CORR and its family on degenerate input; every want is PostgreSQL
	// 17.11's over the same rows.
	pg := map[string][]string{
		"corr":       {"NULL", "NULL", "NULL", "NULL", "0.9941916256", "NULL", "NULL", "0.9933992678"},
		"covar_samp": {"NULL", "0", "0", "0", "5.333333333", "NULL", "0", "2.5"},
		"covar_pop":  {"0", "0", "0", "0", "3.555555556", "0", "0", "1.666666667"},
	}
	for fn, by := range pg {
		var grouped []string
		for s := 1; s <= 8; s++ {
			grouped = append(grouped, fmt.Sprintf("%d,%s", s, by[s-1]))
			for _, a := range []struct {
				args string
				dag  bool
			}{{"y, x", false}, {"y + 0, x * 1", true}} {
				cells = append(cells, ocCell{fmt.Sprintf("%s/sc%d/%v", fn, s, a.dag),
					fmt.Sprintf("SELECT %s(%s) FROM st1694 WHERE sc = %d", fn, a.args, s),
					[]string{"rows=1 " + by[s-1]}, a.dag})
			}
		}
		for _, a := range []struct {
			args string
			dag  bool
		}{{"y, x", false}, {"y + 0, x * 1", true}} {
			cells = append(cells,
				ocCell{fn + "/grouped/" + fmt.Sprint(a.dag), fmt.Sprintf("SELECT sc, %s(%s) FROM st1694 GROUP BY sc", fn, a.args),
					[]string{"rows=8 " + strings.Join(grouped, " | ")}, a.dag},
				ocCell{fn + "/zero/" + fmt.Sprint(a.dag), fmt.Sprintf("SELECT %s(%s) FROM st1694 WHERE sc = 0", fn, a.args),
					// Over zero rows the DAG never reaches the projection
					// that refuses a computed later argument: it answers.
					[]string{"rows=1 NULL"}, false},
				ocCell{fn + "/zeroGrouped/" + fmt.Sprint(a.dag), fmt.Sprintf("SELECT sc, %s(%s) FROM st1694 WHERE sc = 0 GROUP BY sc", fn, a.args),
					[]string{"rows=0 "}, false})
		}
	}
	return cells
}

func TestAggOrderKeyAndCorrDegenerateFiveArms(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := ocArms(t, ctx)
	const notAColumn = "is not a column of its input"
	for _, tc := range ocCells() {
		for _, arm := range arms {
			dag := strings.HasPrefix(arm.name, "dag")
			res, _, err := arm.run(tc.sql)
			if dag && tc.dagRefuses {
				if err == nil || !strings.Contains(err.Error(), notAColumn) {
					t.Errorf("%s\n  arm %s: want the DAG's refusal %q, got %v %s", tc.sql, arm.name, notAColumn, err, renderOrNil(res))
				}
				continue
			}
			got := ""
			if err != nil {
				got = "ERR " + sqlerr.StateOf(err) + " " + err.Error()
			} else {
				got = afRender(res)
			}
			ok := false
			for _, w := range tc.wants {
				if got == w || (strings.HasPrefix(w, "ERR ") && strings.HasPrefix(got, w)) {
					ok = true
				}
			}
			if !ok {
				t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s", tc.sql, arm.name, got, strings.Join(tc.wants, "  or  "))
			}
		}
	}
}

func renderOrNil(res *oracle.Result) string {
	if res == nil {
		return ""
	}
	return afRender(res)
}
