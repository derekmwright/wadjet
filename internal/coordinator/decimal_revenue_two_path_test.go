package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// TPC-H Q1 and Q6's revenue expressions over spec-conformant DECIMAL(15,2),
// under a filter that leaves a partial task with no rows.
//
// This is the shape #685's review named as the one that matters beyond the
// synthetic corpus: `SUM(l_extendedprice * (1 - l_discount))` is a COMPUTED
// DECIMAL aggregate argument, and until aggOutputFromInputDecl it had no
// declared output — so a partial whose filter matched nothing wrote its
// identity row as FLOAT64 beside DECIMAL siblings. On main at eed973a9 that was
// a silent 10^scale answer; with the reader guard it was a refused read.
//
// The fixture is DECIMAL(15,2) because TPC-H v3 §1.3 declares those eight
// columns that way (ADR-0024). It is a nine-row stand-in written HERE rather
// than the phase-3 benchmarks/tpch DECIMAL variant, which is landing on its own
// branch and which this one must not edit: nine rows in three files over three
// workers is what makes `l_orderkey < 5` empty a whole partial, which the SF0.01
// fixture does not do for any natural predicate. When the two branches meet,
// the benchmark's own Q1/Q6 run the same expressions at scale.
//
// Every expectation is live PostgreSQL 17.11 over the identical nine rows.
const revTable = "lineitem_dec"

func revSchema() parquet.Schema {
	dec := func(name string) parquet.Column {
		// The specification's DECIMAL(15,2), the same (p,s) the phase-3
		// benchmark fixture declares for these columns.
		return parquet.Column{Name: name, Type: parquet.TypeDecimal, Precision: 15, Scale: 2, Nullable: true}
	}
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "l_orderkey", Type: parquet.TypeInt64},
		{Name: "l_returnflag", Type: parquet.TypeString, Nullable: true},
		{Name: "l_linestatus", Type: parquet.TypeString, Nullable: true},
		dec("l_extendedprice"), dec("l_discount"), dec("l_tax"),
	}}
}

func revData() []map[string]any {
	src := []struct {
		key          int64
		flag, status string
		price, disc  int64 // unscaled at scale 2
		tax          int64
	}{
		{1, "A", "F", 105025, 5, 8},
		{2, "B", "O", 210050, 10, 0},
		{3, "A", "F", 99999, 0, 5},
		{4, "B", "O", 1234567, 7, 6},
		{5, "A", "F", 50000, 2, 7},
		{6, "B", "O", 777777, 15, 1},
		{7, "A", "F", 1, 9, 2},
		{8, "B", "O", 8888888, 3, 4},
		{9, "A", "F", 432100, 11, 3},
	}
	rows := make([]map[string]any, 0, len(src))
	for _, r := range src {
		rows = append(rows, map[string]any{
			"l_orderkey": r.key, "l_returnflag": r.flag, "l_linestatus": r.status,
			"l_extendedprice": dbpDec(r.price), "l_discount": dbpDec(r.disc), "l_tax": dbpDec(r.tax),
		})
	}
	return rows
}

func TestTPCHRevenueExpressionsTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	infra := tmdInfra(t, ctx)
	revWriteTable(t, ctx, infra)
	coord := tmdCoordinator(t, ctx, infra)
	single := revStandalone(t, ctx)

	const (
		q6      = `SUM(l_extendedprice * l_discount)`
		q1disc  = `SUM(l_extendedprice * (1 - l_discount))`
		q1charg = `SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax))`
	)

	// Ungrouped: the shape that owes an identity row.
	for _, tc := range []struct {
		name, expr string
		bound      int64
		want       string
	}{
		// Q6's revenue. The bound selects how many partial tasks see a row:
		// <5 leaves ONE with none, <4 leaves TWO, <100 leaves none empty.
		{"q6_some_tasks_empty", q6, 5, "1126.7594"},
		{"q6_one_task_only", q6, 4, "262.5625"},
		{"q6_all_match", q6, 100, "5445.4022"},
		{"q6_no_task_matches", q6, 0, ""},
		// Q1's discounted revenue.
		{"q1_disc_some_tasks_empty", q1disc, 5, "15369.6506"},
		{"q1_disc_one_task_only", q1disc, 4, "3888.1775"},
		{"q1_disc_all_match", q1disc, 100, "112538.6678"},
		{"q1_disc_no_task_matches", q1disc, 0, ""},
		// Q1's charge, which nests the revenue expression one deeper.
		{"q1_charge_some_tasks_empty", q1charg, 5, "16188.357486"},
		{"q1_charge_one_task_only", q1charg, 4, "4017.996000"},
		{"q1_charge_all_match", q1charg, 100, "117022.045157"},
		// The bare-column neighbours Q1 selects beside them.
		{"q1_sum_price", `SUM(l_extendedprice)`, 5, "16496.41"},
		// MIN/MAX over the revenue expression: the (p,s)-preserving arm.
		{"q1_min_revenue", `MIN(l_extendedprice * (1 - l_discount))`, 5, "997.7375"},
		{"q1_max_revenue", `MAX(l_extendedprice * (1 - l_discount))`, 5, "11481.4731"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := fmt.Sprintf("SELECT %s AS v FROM %s WHERE l_orderkey < %d", tc.expr, revTable, tc.bound)
			for _, arm := range []struct {
				name string
				dag  bool
			}{{"single", false}, {"dag", true}} {
				rows := dtpRun(t, ctx, single, coord, sql, arm.dag)
				if len(rows) != 1 {
					t.Fatalf("%s: %d rows, want 1", arm.name, len(rows))
				}
				if tc.want == "" {
					if rows[0]["v"] != nil {
						t.Errorf("%s %s = %#v, want NULL", arm.name, sql, rows[0]["v"])
					}
					continue
				}
				dtpCell(t, arm.name+" "+sql, rows[0]["v"], tc.want)
			}
		})
	}

	// AVG's two Q1 columns, at wadjet's scale+4 contract rather than
	// PostgreSQL's ≥16-significant-digit rule (ADR-0012 item 9). PostgreSQL
	// answers 4124.1025000000000000 and 0.05500000000000000000 for these; the
	// two agree to min(scale), which is the contract.
	for _, tc := range []struct{ name, expr, want string }{
		{"q1_avg_price", `AVG(l_extendedprice)`, "4124.102500"},
		{"q1_avg_disc", `AVG(l_discount)`, "0.055000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := fmt.Sprintf("SELECT %s AS v FROM %s WHERE l_orderkey < 5", tc.expr, revTable)
			for _, arm := range []struct {
				name string
				dag  bool
			}{{"single", false}, {"dag", true}} {
				rows := dtpRun(t, ctx, single, coord, sql, arm.dag)
				dtpCell(t, arm.name+" "+sql, rows[0]["v"], tc.want)
			}
		})
	}

	// Q1's real shape: grouped by returnflag/linestatus, every revenue column
	// at once, under the same partial-emptying filter. Grouped partials emit no
	// rows when they match nothing, so this was correct throughout — it is here
	// because Q1 is a GROUP BY and a gate for it that only ran the ungrouped
	// form would not be a gate for Q1.
	t.Run("q1_grouped", func(t *testing.T) {
		sql := `SELECT l_returnflag AS rf, l_linestatus AS ls,
			SUM(l_extendedprice) AS sum_price,
			SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
			SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
			COUNT(*) AS n
			FROM ` + revTable + ` WHERE l_orderkey < 5
			GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus`
		want := []map[string]string{
			{"rf": "A", "ls": "F", "sum_price": "2050.24", "sum_disc_price": "1997.7275", "sum_charge": "2127.546000"},
			{"rf": "B", "ls": "O", "sum_price": "14446.17", "sum_disc_price": "13371.9231", "sum_charge": "14060.811486"},
		}
		for _, arm := range []struct {
			name string
			dag  bool
		}{{"single", false}, {"dag", true}} {
			rows := dtpRun(t, ctx, single, coord, sql, arm.dag)
			if len(rows) != len(want) {
				t.Fatalf("%s: %d groups, want %d", arm.name, len(rows), len(want))
			}
			for i, w := range want {
				for k, v := range w {
					if k == "rf" || k == "ls" {
						if got, _ := rows[i][k].(string); got != v {
							t.Errorf("%s group %d %s = %#v, want %q", arm.name, i, k, rows[i][k], v)
						}
						continue
					}
					dtpCell(t, fmt.Sprintf("%s group %d %s", arm.name, i, k), rows[i][k], v)
				}
				if n := toInt64(rows[i]["n"]); n != 2 {
					t.Errorf("%s group %d n = %v, want 2", arm.name, i, rows[i]["n"])
				}
			}
		}
	})
}

// revWriteTable writes the fixture as three parquet files of three rows.
func revWriteTable(t *testing.T, ctx context.Context, infra tmdInfraT) {
	t.Helper()
	schema, rows := revSchema(), revData()
	if err := infra.cat.CreateTable(ctx, revTable, schema, nil); err != nil {
		t.Fatalf("create %s: %v", revTable, err)
	}
	var entries []catalog.FileEntry
	for c := 0; c*3 < len(rows); c++ {
		chunk := rows[c*3 : c*3+3]
		var buf bytes.Buffer
		pw, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
		if err != nil {
			t.Fatalf("parquet writer: %v", err)
		}
		if err := pw.WriteRows(chunk); err != nil {
			t.Fatalf("write rows: %v", err)
		}
		if err := pw.Close(); err != nil {
			t.Fatalf("close writer: %v", err)
		}
		path := fmt.Sprintf("tables/%s/chunk_%04d.parquet", revTable, c)
		payload := buf.Bytes()
		if _, err := infra.store.Put(ctx, "test", path, bytes.NewReader(payload),
			int64(len(payload)), "application/octet-stream"); err != nil {
			t.Fatalf("put %s: %v", path, err)
		}
		entries = append(entries, catalog.FileEntry{
			Path: path, SizeBytes: int64(len(payload)),
			NumRows: int64(len(chunk)), CreatedAt: time.Now(),
		})
	}
	if len(entries) != 3 {
		t.Fatalf("the fixture wrote %d files, not 3 — `l_orderkey < 5` no longer empties a "+
			"partial task and this gate proves nothing", len(entries))
	}
	if err := infra.cat.AddFiles(ctx, revTable, map[string]string{}, "tables/"+revTable+"/", entries); err != nil {
		t.Fatalf("add files: %v", err)
	}
}

func revStandalone(t *testing.T, ctx context.Context) *wadjet.DB {
	t.Helper()
	return tmdStandaloneWith(t, ctx, revTable, revSchema(), revData())
}

// tmdStandaloneWith is the embedded single-process arm over one ad-hoc table.
// The fixtures that ride in tmdTables() share one DB; this is for the ones that
// stand up their own infra so a gate can read the object store.
func tmdStandaloneWith(t *testing.T, ctx context.Context, name string,
	schema parquet.Schema, rows []map[string]any,
) *wadjet.DB {
	t.Helper()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open standalone: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.CreateTable(ctx, name, schema, nil); err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	ing := db.NewIngester(name, schema, nil, ingest.Config{
		MaxBufferRows: len(rows) + 1, RowGroupSize: 3,
	})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatalf("ingest %s: %v", name, err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush %s: %v", name, err)
	}
	return db
}
