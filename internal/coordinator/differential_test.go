package coordinator

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"testing"

	"github.com/derekmwright/wadjet/benchmarks/tpch"
	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/oracle/sqlgen"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// tpchGenSchema builds the sqlgen universe from the real TPC-H schema:
// column kinds derive from the parquet types, join edges are the TPC-H FK
// pairs, and literal pools come from the spec domains so predicates are
// selective without being always-empty.
func tpchGenSchema() *sqlgen.Schema {
	lits := map[string][]string{
		"r_regionkey":     {"0", "2", "4"},
		"r_name":          {"'ASIA'", "'EUROPE'", "'AMERICA'"},
		"n_nationkey":     {"3", "10", "20"},
		"n_name":          {"'FRANCE'", "'CHINA'", "'BRAZIL'"},
		"n_regionkey":     {"0", "1", "3"},
		"s_suppkey":       {"3", "50", "99"},
		"s_nationkey":     {"1", "12", "23"},
		"s_acctbal":       {"100.0", "4500.0"},
		"p_partkey":       {"5", "500", "1999"},
		"p_size":          {"5", "15", "40"},
		"p_retailprice":   {"950.0", "1500.0"},
		"p_type":          {"'PROMO BURNISHED COPPER'", "'STANDARD ANODIZED TIN'"},
		"p_brand":         {"'Brand#13'", "'Brand#42'"},
		"ps_availqty":     {"100", "5000"},
		"ps_supplycost":   {"50.0", "500.0"},
		"c_custkey":       {"10", "150", "1499"},
		"c_nationkey":     {"2", "9", "21"},
		"c_acctbal":       {"0.0", "3000.0"},
		"c_mktsegment":    {"'BUILDING'", "'AUTOMOBILE'", "'MACHINERY'"},
		"o_orderkey":      {"100", "5000", "12000"},
		"o_custkey":       {"15", "700", "1400"},
		"o_orderstatus":   {"'F'", "'O'"},
		"o_totalprice":    {"50000.0", "200000.0"},
		"o_orderdate":     {"'1993-01-01'", "'1994-06-30'", "'1996-12-31'"},
		"o_orderpriority": {"'1-URGENT'", "'3-MEDIUM'"},
		"l_orderkey":      {"100", "5000", "12000"},
		"l_partkey":       {"5", "500", "1999"},
		"l_suppkey":       {"3", "50", "99"},
		"l_quantity":      {"10.0", "25.0", "45.0"},
		"l_extendedprice": {"10000.0", "40000.0"},
		"l_discount":      {"0.02", "0.05", "0.08"},
		"l_tax":           {"0.02", "0.06"},
		"l_returnflag":    {"'R'", "'N'", "'A'"},
		"l_linestatus":    {"'F'", "'O'"},
		"l_shipdate":      {"'1993-06-01'", "'1994-09-15'", "'1996-03-31'"},
		"l_shipmode":      {"'AIR'", "'MAIL'", "'SHIP'"},
	}
	s := &sqlgen.Schema{}
	// Deterministic table order — AllTables is a map.
	names := make([]string, 0, len(tpch.AllTables))
	for name := range tpch.AllTables {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		schema := tpch.AllTables[name]
		t := sqlgen.Table{Name: name}
		for _, c := range schema.Columns {
			var kind sqlgen.Kind
			switch c.Type {
			case parquet.TypeInt64, parquet.TypeInt32:
				kind = sqlgen.KindInt
			case parquet.TypeFloat64, parquet.TypeFloat32:
				kind = sqlgen.KindFloat
			case parquet.TypeString:
				kind = sqlgen.KindString
			default:
				continue // types the generator doesn't produce predicates/exprs for
			}
			t.Cols = append(t.Cols, sqlgen.Column{Name: c.Name, Kind: kind, Lits: lits[c.Name]})
		}
		s.Tables = append(s.Tables, t)
	}
	s.Edges = []sqlgen.Edge{
		{LeftTable: "lineitem", LeftCol: "l_orderkey", RightTable: "orders", RightCol: "o_orderkey"},
		{LeftTable: "lineitem", LeftCol: "l_partkey", RightTable: "part", RightCol: "p_partkey"},
		{LeftTable: "lineitem", LeftCol: "l_suppkey", RightTable: "supplier", RightCol: "s_suppkey"},
		{LeftTable: "orders", LeftCol: "o_custkey", RightTable: "customer", RightCol: "c_custkey"},
		{LeftTable: "customer", LeftCol: "c_nationkey", RightTable: "nation", RightCol: "n_nationkey"},
		{LeftTable: "supplier", LeftCol: "s_nationkey", RightTable: "nation", RightCol: "n_nationkey"},
		{LeftTable: "nation", LeftCol: "n_regionkey", RightTable: "region", RightCol: "r_regionkey"},
		{LeftTable: "partsupp", LeftCol: "ps_partkey", RightTable: "part", RightCol: "p_partkey"},
		{LeftTable: "partsupp", LeftCol: "ps_suppkey", RightTable: "supplier", RightCol: "s_suppkey"},
	}
	return s
}

// TestSQLGenParsesAgainstRealSchema pins that generated SQL is valid for
// our parser across a wide seed range — a generator emitting unparseable
// SQL would silently turn the differential into a no-op.
func TestSQLGenParsesAgainstRealSchema(t *testing.T) {
	s := tpchGenSchema()
	for seed := int64(1); seed <= 500; seed++ {
		sql := sqlgen.New(seed, s).Query().SQL()
		if _, err := plansql.Parse(sql); err != nil {
			t.Fatalf("seed %d generates unparseable SQL: %v\n%s", seed, err, sql)
		}
	}
}

// standaloneTPCH opens a single-process wadjet.DB over its own MemStore
// with the identical (deterministic) TPC-H SF0.01 dataset the distributed
// cluster loads.
func standaloneTPCH(t *testing.T, ctx context.Context) *wadjet.DB {
	t.Helper()
	store := objstore.NewMemStore()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "tpch"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	data := tpch.Generate(tpch.SF001)
	for tableName, schema := range tpch.AllTables {
		if err := db.CreateTable(ctx, tableName, schema, nil); err != nil {
			t.Fatalf("creating table %s: %v", tableName, err)
		}
		rows := data[tableName]
		if len(rows) == 0 {
			continue
		}
		ing := db.NewIngester(tableName, schema, nil, ingest.Config{
			MaxBufferRows: len(rows) + 1,
			RowGroupSize:  max(100, len(rows)/4),
		})
		if err := ing.Ingest(ctx, rows); err != nil {
			t.Fatalf("ingesting %s: %v", tableName, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flushing %s: %v", tableName, err)
		}
	}
	return db
}

// TestStandaloneVsDistributedDifferential is the randomized
// standalone-vs-distributed oracle (#288): seeded random queries run in a
// single-process wadjet.DB and in a 3-worker distributed cluster (8 chunks
// per table so probe-split and shuffle paths engage), and results must be
// identical. On divergence the query is shrunk to a minimal repro and
// reported with its seed.
//
// Seed range: WADJET_DIFF_SEED_START / WADJET_DIFF_SEED_COUNT (defaults
// 1 / 25). CI runs the bounded default; longer campaigns raise the count.
func TestStandaloneVsDistributedDifferential(t *testing.T) {
	if testing.Short() {
		t.Skip("distributed differential skipped in -short mode")
	}
	seedStart := envInt("WADJET_DIFF_SEED_START", 1)
	seedCount := envInt("WADJET_DIFF_SEED_COUNT", 25)

	ctx, coord := setupTPCHDistributedAtScale(t, tpch.SF001)
	sdb := standaloneTPCH(t, ctx)
	schema := tpchGenSchema()

	diverges := func(q *sqlgen.Query) (bool, string) {
		return diffOneQuery(ctx, t, sdb, coord, q)
	}

	for seed := seedStart; seed < seedStart+seedCount; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed%04d", seed), func(t *testing.T) {
			q := sqlgen.New(int64(seed), schema).Query()
			bad, detail := diverges(q)
			if !bad {
				return
			}
			min := sqlgen.Shrink(schema, q, func(c *sqlgen.Query) bool {
				b, _ := diverges(c)
				return b
			})
			_, minDetail := diverges(min)
			t.Errorf("standalone and distributed diverge (seed %d)\noriginal: %s\n%s\nminimal repro: %s\n%s",
				seed, q.SQL(), detail, min.SQL(), minDetail)
		})
	}
}

// diffOneQuery runs q on both arms. Returns (true, detail) on divergence.
// Both-arms-reject is not a divergence (the generator can produce queries
// the engine rejects; rejecting consistently is fine) — but one arm
// rejecting what the other executes is.
func diffOneQuery(ctx context.Context, t *testing.T, sdb *wadjet.DB, coord *Coordinator, q *sqlgen.Query) (bool, string) {
	t.Helper()
	sql := q.SQL()

	sres, serr := sdb.Query(ctx, sql)
	dres, derr := coord.ExecuteSQL(ctx, sql)
	if derr == nil && dres.Error != "" {
		derr = fmt.Errorf("%s", dres.Error)
	}
	if serr != nil && derr != nil {
		return false, "" // both reject
	}
	if (serr == nil) != (derr == nil) {
		return true, fmt.Sprintf("error mismatch: standalone err=%v, distributed err=%v", serr, derr)
	}

	dRows, err := dres.Rows()
	if err != nil {
		return true, fmt.Sprintf("distributed result rows: %v", err)
	}

	// Canonical column order shared by both arms: sorted standalone names.
	cols := append([]string(nil), sres.Columns...)
	sort.Strings(cols)
	sCanon := oracle.Canonicalize(&oracle.Result{Columns: cols, Rows: sres.Rows})
	dCanon := oracle.Canonicalize(&oracle.Result{Columns: cols, Rows: dRows})

	// LIMIT boundary ties make limited output legitimately nondeterministic
	// (any tie-group member is admissible), so a LIMIT-ed query compares by
	// row count and its stripped form compares the full multiset — the same
	// scheme as the kill-switch oracle's ExpandLimits.
	oq := oracle.Query{Name: "gen", CountOnly: q.Limit > 0}
	if diff := sCanon.Diff(dCanon, oq); diff != "" {
		return true, fmt.Sprintf("(%d standalone rows vs %d distributed)\n%s", sCanon.Rows(), dCanon.Rows(), diff)
	}
	if q.Limit > 0 {
		stripped := q.Clone()
		stripped.Limit = 0
		stripped.OrderBy = nil
		return diffOneQuery(ctx, t, sdb, coord, stripped)
	}
	return false, ""
}

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}
