package tpch

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/derekmwright/wadjet/internal/oracle/multikey"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// This file is the shared harness for the PostgreSQL differential oracle. The
// two arms that use it live in postgres_compare_test.go (engine semantics) and
// postgres_wire_test.go (protocol).
//
// # WHY A SECOND ORACLE EXISTS, given the DuckDB one
//
// DuckDB is the performance goal and a correctness oracle, and it stays that.
// It cannot be the authority on SEMANTICS, because Wadjet ships the PostgreSQL
// WIRE PROTOCOL: the question a psql / DataGrip / Superset / JDBC client asks
// is "does this behave the way PostgreSQL behaves", and DuckDB's answer to that
// is only a coincidence when it agrees. The DuckDB arm already had to be
// CONFIGURED to answer PostgreSQL's question once — default_null_order, guarded
// by TestOracleIsConfiguredForPostgresSemantics — which is the tell: the ground
// truth being consulted was not the ground truth being claimed.
//
// So: PostgreSQL is the authority on semantics; DuckDB remains the performance
// goal and a second correctness oracle. Where the two disagree with each other,
// this arm is the one that decides.
//
// # NO STORED BASELINE, deliberately
//
// The DuckDB arm commits a fingerprint file so CI can gate without the binary.
// This one does not, and must not: its second arm compares WIRE BYTES from a
// live server, which no file can stand in for, and a stored file is exactly the
// artefact that made a wrong answer look like ground truth (the Q05 baseline of
// 2026-08-17, and baseline-local-small.json of 2026-08-18). Every number here
// is read from a running PostgreSQL in the same process invocation that reads
// Wadjet's. With no server the suite SKIPS — see postgresOracleDSN.
//
// # SCALE IS A PARAMETER
//
// The default tier is the committed SF0.01 parquet under duckdb-data/, which
// keeps the suite at seconds. WADJET_PG_SCALE=0.1 (or 1) selects a generated
// tier instead. Whatever the tier, both engines are fed from ONE source — the
// same parquet rows, or the same GenerateChunked stream — because two
// generations of "the same" data compare two different questions.
//
// What the larger tier bought, measured rather than assumed: at SF0.1 (865,543
// rows, ~10× the committed fixture) the corpus found NOTHING the SF0.01 tier
// had not already found — the same 14 entries diverged and every other entry
// agreed. That is the expected result and worth stating plainly: semantics
// divergences are visible at 25 rows, and the larger tier buys value-
// distribution coverage (ties, boundaries, wider NULL patterns) rather than new
// rules. Run the cheap tier constantly; reach for the larger one when a change
// is about accumulation, partitioning or a threshold, not about meaning.
const (
	// postgresDSNEnv points the oracle at a server the caller already has.
	postgresDSNEnv = "WADJET_PG_DSN"
	// postgresScaleEnv selects the larger tier, e.g. WADJET_PG_SCALE=1.
	// Unset (or "0.01") uses the committed SF0.01 parquet fixture.
	postgresScaleEnv = "WADJET_PG_SCALE"
	// postgresImage is the container the harness starts when no DSN is given.
	// It is never PULLED — see startPostgresContainer.
	postgresImage = "postgres:17-alpine"
	// postgresOracleDB is the database the harness loads the fixture into.
	postgresOracleDB = "wadjet_oracle"

	// postgresCollation is the whole of the string-ordering decision, and the
	// reason it is a constant rather than a comment.
	//
	// Wadjet compares strings by BYTES. PostgreSQL compares them by the
	// database's collation, which on a default install is a locale collation
	// (en_US.UTF-8) that ignores case and punctuation at the primary level:
	// there 'a' < 'B', and by bytes 'B' < 'a'. Neither engine is wrong. The
	// oracle's job is to compare the same question, so it asks PostgreSQL for
	// the byte ordering it can actually produce — the C collation.
	//
	// Two levers, because the harness does not always own the database:
	//   - the container it starts is initdb'd --locale=C, so EVERY comparison
	//     in it is byte ordering, including ones no column collation reaches;
	//   - every text column of the fixture is declared COLLATE "C", which
	//     carries byte ordering through a caller's own en_US database too
	//     (a column's implicit collation beats a literal's default one).
	// Exempting the string-ordering queries instead would blind the arm to
	// every real ordering bug in the same queries, which is the mistake
	// default_null_order already taught this project once.
	postgresCollation = `"C"`
)

// postgresOracle is one live differential run: a PostgreSQL holding the
// fixture, and a Wadjet holding the identical rows from the identical source.
type postgresOracle struct {
	dsn  string
	tier string // human label for the loaded tier, e.g. "SF0.01 (committed parquet fixture)"
	conn *pgx.Conn
	db   *wadjet.DB
	// counts is rows loaded per table, asserted equal on both sides.
	counts map[string]int
}

// newPostgresOracle brings up the server, loads both engines from one source,
// and returns the pair. It SKIPS the test when no PostgreSQL is reachable and
// none can be started — CI has neither, exactly as it has no /tmp/duckdb.
func newPostgresOracle(t *testing.T, ctx context.Context) *postgresOracle {
	t.Helper()
	dsn := postgresOracleDSN(t)

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connecting to the oracle at %s: %v", redactDSN(dsn), err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })

	o := &postgresOracle{dsn: dsn, conn: conn, counts: map[string]int{}}
	o.reportCollation(t, ctx)
	o.load(t, ctx)
	return o
}

// postgresOracleDSN resolves a server, or skips.
//
// Order: an explicit DSN wins; otherwise a container is started IF the image is
// already on the machine. The image is never pulled — a 400 MB download is not
// something a `go test` should decide to do, and in CI it would happen on every
// run only to be thrown away.
func postgresOracleDSN(t *testing.T) string {
	t.Helper()
	if dsn := strings.TrimSpace(os.Getenv(postgresDSNEnv)); dsn != "" {
		return dsn
	}
	if dsn, ok := startPostgresContainer(t); ok {
		return dsn
	}
	t.Skipf(`no PostgreSQL oracle available, skipping.

PostgreSQL is the authority on SEMANTICS for this engine (it ships the pg wire
protocol), so this suite needs a real server. Any of:

  task pg-oracle:up        # starts %s, prints the DSN
  %s=postgres://user:pw@host:port/db?sslmode=disable go test ./benchmarks/tpch/ -run TestPostgresOracle

or make the image available and the harness will start and tear down its own:

  docker pull %s

Larger tier (loads a generated fixture into BOTH engines from one source):

  %s=0.1 go test ./benchmarks/tpch/ -run TestPostgresOracle -timeout 30m`,
		postgresImage, postgresDSNEnv, postgresImage, postgresScaleEnv)
	return ""
}

// startPostgresContainer runs a throwaway server and tears it down with the
// test. Reports false — never fails — when Docker or the image is absent, so
// the caller can skip.
func startPostgresContainer(t *testing.T) (string, bool) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		return "", false
	}
	if err := exec.Command("docker", "image", "inspect", postgresImage).Run(); err != nil {
		return "", false
	}

	name := fmt.Sprintf("wadjet-pg-oracle-%d", time.Now().UnixNano())
	out, err := exec.Command("docker", "run", "-d", "--name", name,
		"-e", "POSTGRES_USER=wadjet",
		"-e", "POSTGRES_PASSWORD=wadjet",
		"-e", "POSTGRES_DB="+postgresOracleDB,
		// initdb --locale=C: see postgresCollation. This makes byte ordering
		// the database-wide default, not only the fixture columns'.
		"-e", "POSTGRES_INITDB_ARGS=--locale=C --encoding=UTF8",
		"-p", "127.0.0.1::5432",
		postgresImage,
		// Durability off: this database exists for the length of one test and
		// is thrown away. It makes the SF1 load minutes rather than tens of
		// minutes and changes no answer.
		"-c", "fsync=off", "-c", "full_page_writes=off", "-c", "synchronous_commit=off",
	).CombinedOutput()
	if err != nil {
		t.Logf("docker run %s failed, skipping the PostgreSQL oracle: %v\n%s", postgresImage, err, out)
		return "", false
	}
	t.Cleanup(func() {
		if err := exec.Command("docker", "rm", "-f", name).Run(); err != nil {
			t.Logf("removing container %s: %v", name, err)
		}
	})

	portOut, err := exec.Command("docker", "port", name, "5432/tcp").Output()
	if err != nil {
		t.Logf("docker port %s: %v", name, err)
		return "", false
	}
	// "127.0.0.1:49154\n", possibly several lines (v4 + v6).
	line := strings.TrimSpace(strings.SplitN(string(portOut), "\n", 2)[0])
	colon := strings.LastIndexByte(line, ':')
	if colon < 0 {
		t.Logf("docker port %s returned %q", name, line)
		return "", false
	}
	port := line[colon+1:]
	dsn := fmt.Sprintf("postgres://wadjet:wadjet@127.0.0.1:%s/%s?sslmode=disable", port, postgresOracleDB)

	// Readiness: the container reports "database system is ready" once during
	// initdb and again for real, so poll the protocol rather than the log.
	deadline := time.Now().Add(90 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		conn, err := pgx.Connect(ctx, dsn)
		cancel()
		if err == nil {
			conn.Close(context.Background())
			t.Logf("PostgreSQL oracle: started %s as %s on port %s", postgresImage, name, port)
			return dsn, true
		}
		if time.Now().After(deadline) {
			t.Logf("PostgreSQL container %s never became ready: %v", name, err)
			return "", false
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// reportCollation logs what the server will actually compare strings with, and
// fails when it is neither C nor one the column-level COLLATE "C" can rescue.
// Silence here is what would let a locale-collated caller-supplied database
// report every string ORDER BY as a Wadjet divergence.
func (o *postgresOracle) reportCollation(t *testing.T, ctx context.Context) {
	t.Helper()
	var collate, ctype, version string
	err := o.conn.QueryRow(ctx,
		`SELECT datcollate, datctype, current_setting('server_version') FROM pg_database WHERE datname = current_database()`).
		Scan(&collate, &ctype, &version)
	if err != nil {
		t.Fatalf("reading the oracle's collation: %v", err)
	}
	// The property that actually matters, asked of the server rather than
	// inferred from its settings: does a C-collated comparison order by bytes?
	var byteOrder bool
	if err := o.conn.QueryRow(ctx, `SELECT ('B' COLLATE `+postgresCollation+`) < ('a' COLLATE `+postgresCollation+`)`).Scan(&byteOrder); err != nil {
		t.Fatalf("probing collation %s: %v", postgresCollation, err)
	}
	if !byteOrder {
		t.Fatalf("the oracle's COLLATE %s does not order by bytes ('B' < 'a' is false). "+
			"Wadjet compares strings by bytes; without a byte-ordering collation this arm compares "+
			"two different questions and every string ORDER BY reads as a divergence.", postgresCollation)
	}
	t.Logf("PostgreSQL oracle: %s, database collation %s/%s, fixture text columns declared COLLATE %s",
		version, collate, ctype, postgresCollation)
}

// postgresScale reads the tier. Returns the scale and whether the committed
// parquet fixture is the source (the default), rather than generated data.
func postgresScale(t *testing.T) (ScaleFactor, bool) {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv(postgresScaleEnv))
	if raw == "" {
		return SF001, true
	}
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil || f <= 0 {
		t.Fatalf("%s=%q is not a positive scale factor", postgresScaleEnv, raw)
	}
	// SF0.01 asked for explicitly still means the committed fixture: it is the
	// same scale and it is the data the DuckDB arm's fingerprints describe, so
	// the two oracles stay comparable.
	if ScaleFactor(f) == SF001 {
		return SF001, true
	}
	return ScaleFactor(f), false
}

// load fills both engines from one source and asserts they agree on how many
// rows arrived. The assertion is not a formality: a differential oracle over
// two different datasets reports divergences that are only the load's.
func (o *postgresOracle) load(t *testing.T, ctx context.Context) {
	t.Helper()
	scale, committed := postgresScale(t)

	store, err := newOracleStore(t, committed)
	if err != nil {
		t.Fatalf("object store: %v", err)
	}
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "tpch"})
	if err != nil {
		t.Fatalf("wadjet.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	o.db = db

	o.createPostgresSchema(t, ctx)

	tables := oracleTables()
	ingesters := make(map[string]*ingest.Ingester, len(tables))
	for name, schema := range tables {
		rowGroup := 50_000
		if name == pgDecimalTable {
			// Small row groups so the DECIMAL entries actually cross a
			// zonemap boundary: a single row group has bounds wide enough to
			// contain every literal, and #438 was a PRUNE.
			rowGroup = pgDecimalRowGroup
		}
		if err := db.CreateTable(ctx, name, schema, nil); err != nil {
			t.Fatalf("wadjet create table %s: %v", name, err)
		}
		ingesters[name] = db.NewIngester(name, schema, nil, ingest.Config{
			MaxBufferRows: 200_000,
			RowGroupSize:  rowGroup,
		})
	}

	// One source, two sinks. Every row that reaches PostgreSQL is the same Go
	// map that reaches Wadjet's ingester.
	sink := func(table string, rows []map[string]any) error {
		if err := ingesters[table].Ingest(ctx, rows); err != nil {
			return fmt.Errorf("wadjet ingest %s: %w", table, err)
		}
		if err := o.copyInto(ctx, table, rows); err != nil {
			return fmt.Errorf("postgres copy %s: %w", table, err)
		}
		o.counts[table] += len(rows)
		return nil
	}

	start := time.Now()
	if committed {
		o.tier = "SF0.01 (committed parquet fixture, duckdb-data/)"
		for table, rows := range duckdbFixtureRows(t) {
			if err := sink(table, rows); err != nil {
				t.Fatalf("%v", err)
			}
		}
	} else {
		o.tier = fmt.Sprintf("SF%g (generated, %s=%g)", float64(scale), postgresScaleEnv, float64(scale))
		if err := GenerateChunked(scale, 50_000, sink); err != nil {
			t.Fatalf("generating SF%g: %v", float64(scale), err)
		}
	}
	if err := sink(pgDecimalTable, pgDecimalRows()); err != nil {
		t.Fatalf("%v", err)
	}
	if err := sink(pgBytesTable, pgBytesRows()); err != nil {
		t.Fatalf("%v", err)
	}
	for _, tbl := range multikey.Tables() {
		if err := sink(tbl.Name, tbl.Rows); err != nil {
			t.Fatalf("%v", err)
		}
	}
	for name, ing := range ingesters {
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("wadjet flush %s: %v", name, err)
		}
	}

	o.createPostgresIndexes(t, ctx)

	// ANALYZE so the oracle plans its own queries sanely at the larger tiers;
	// it changes no answer, only how long the arm takes to get one.
	if _, err := o.conn.Exec(ctx, "ANALYZE"); err != nil {
		t.Fatalf("ANALYZE: %v", err)
	}

	total := 0
	for table, n := range o.counts {
		total += n
		var pgCount int
		if err := o.conn.QueryRow(ctx, "SELECT COUNT(*) FROM "+table).Scan(&pgCount); err != nil {
			t.Fatalf("counting %s: %v", table, err)
		}
		if pgCount != n {
			t.Fatalf("%s: %d rows loaded, PostgreSQL holds %d — the two engines are not looking at "+
				"the same data and every divergence this run reports would be the load's", table, n, pgCount)
		}
	}
	t.Logf("PostgreSQL oracle: loaded %s — %d rows into both engines from one source in %s",
		o.tier, total, time.Since(start).Round(time.Millisecond))
}

// newOracleStore picks the object store for the Wadjet side. MemStore for the
// committed tier (it is a few MB); a temp FileStore for a generated one, where
// SF1's parquet would otherwise sit in the test process's heap next to
// PostgreSQL's answer.
func newOracleStore(t *testing.T, committed bool) (objstore.Store, error) {
	t.Helper()
	if committed {
		return objstore.NewMemStore(), nil
	}
	return objstore.NewFileStore(t.TempDir())
}

// pgWideStep is the unscaled increment between consecutive d_wide values: 23
// digits, so |unscaled| runs to about 9.9e24 across the fixture and even the
// SMALLEST non-zero value needs 77 bits. int64 tops out at 19 digits, so no
// value in this column is representable the way every DECIMAL wadjet wrote
// before #429 was.
const pgWideStep = "98765432109876543210987"

// pgWideUnscaled builds row i's d_wide value, monotonic in i and spanning both
// signs for the same reason the columns above are (see pgDecimalRows).
func pgWideUnscaled(i int) parquet.Decimal128 {
	step, ok := new(big.Int).SetString(pgWideStep, 10)
	if !ok {
		panic("pgWideStep is not an integer")
	}
	n := step.Mul(step, big.NewInt(int64(i-100)))
	return decimal128FromBigInt(n)
}

// decimal128FromBigInt renders a signed big.Int as the two 64-bit halves
// parquet.Decimal128 carries, two's complement.
func decimal128FromBigInt(n *big.Int) parquet.Decimal128 {
	m := new(big.Int).Set(n)
	if m.Sign() < 0 {
		m.Add(m, new(big.Int).Lsh(big.NewInt(1), 128))
	}
	var b [16]byte
	m.FillBytes(b[:])
	return parquet.Decimal128{
		Hi: int64(binary.BigEndian.Uint64(b[0:8])),
		Lo: binary.BigEndian.Uint64(b[8:16]),
	}
}

// pgNumericOf converts a Decimal128 box at a column's scale into the exact
// pgtype.Numeric the PostgreSQL sink needs.
//
// This is a RENDERING decision of the harness in the same class as
// normalizeTemporal, not a second source: the number is the one Decimal128
// holds, and pgx's COPY is binary-only (a Go string for a numeric column has
// no binary encode plan), so the box has to be spelled in the driver's type.
// The conversion is exact — it moves 128 bits of integer and a scale.
func pgNumericOf(d parquet.Decimal128, scale int32) pgtype.Numeric {
	n, ok := new(big.Int).SetString(d.String(), 10)
	if !ok {
		panic("Decimal128.String is not an integer: " + d.String())
	}
	return pgtype.Numeric{Int: n, Exp: -scale, Valid: true}
}

// createPostgresSchema declares the fixture with the PostgreSQL types that
// MIRROR Wadjet's declared types, so a type OID the wire arm compares is a
// comparison of the two engines and not of two schema-writing conventions.
//
// The date columns are TEXT on both sides on purpose: benchmarks/tpch/schema.go
// stores o_orderdate and the l_*date columns as YYYY-MM-DD strings, and the
// corpus compares them as strings. Declaring them `date` here would make
// PostgreSQL answer a different query.
func (o *postgresOracle) createPostgresSchema(t *testing.T, ctx context.Context) {
	t.Helper()
	for name, schema := range oracleTables() {
		if _, err := o.conn.Exec(ctx, "DROP TABLE IF EXISTS "+name+" CASCADE"); err != nil {
			t.Fatalf("drop %s: %v", name, err)
		}
		cols := make([]string, 0, len(schema.Columns))
		for _, c := range schema.Columns {
			cols = append(cols, c.Name+" "+postgresColumnType(t, c))
		}
		ddl := fmt.Sprintf("CREATE TABLE %s (%s)", name, strings.Join(cols, ", "))
		if _, err := o.conn.Exec(ctx, ddl); err != nil {
			t.Fatalf("create %s: %v\n%s", name, err, ddl)
		}
	}
}

// createPostgresIndexes builds the TPC-H key indexes AFTER the load.
//
// They change no answer — an index cannot alter a result set — and without
// them the larger tier is unusable: at SF0.1, Q20's correlated subquery over an
// unindexed lineitem ran for more than seven minutes in PostgreSQL, against a
// whole-suite budget of a few. Wadjet has no indexes at all and does not need
// them; it scans columnar files with predicate pushdown. Giving the ORACLE the
// access paths its own planner expects is the same kind of configuration as the
// C collation: it makes the reference engine able to answer the question, and
// it is the only reason the larger tier is affordable.
func (o *postgresOracle) createPostgresIndexes(t *testing.T, ctx context.Context) {
	t.Helper()
	// Key and foreign-key columns. Deliberately NOT primary keys: a PRIMARY
	// KEY asserts uniqueness, the generator makes no such promise at every
	// scale, and a failed constraint would abort the run over something that
	// has nothing to do with the comparison. A plain index gives the planner
	// the same access paths.
	for _, ddl := range []string{
		`CREATE INDEX ON region (r_regionkey)`,
		`CREATE INDEX ON nation (n_nationkey)`,
		`CREATE INDEX ON supplier (s_suppkey)`,
		`CREATE INDEX ON part (p_partkey)`,
		`CREATE INDEX ON partsupp (ps_partkey, ps_suppkey)`,
		`CREATE INDEX ON customer (c_custkey)`,
		`CREATE INDEX ON orders (o_orderkey)`,
		`CREATE INDEX ON nation (n_regionkey)`,
		`CREATE INDEX ON supplier (s_nationkey)`,
		`CREATE INDEX ON partsupp (ps_suppkey)`,
		`CREATE INDEX ON customer (c_nationkey)`,
		`CREATE INDEX ON orders (o_custkey)`,
		`CREATE INDEX ON orders (o_orderdate)`,
		`CREATE INDEX ON lineitem (l_orderkey)`,
		`CREATE INDEX ON lineitem (l_partkey, l_suppkey)`,
		`CREATE INDEX ON lineitem (l_suppkey)`,
		`CREATE INDEX ON lineitem (l_shipdate)`,
	} {
		if _, err := o.conn.Exec(ctx, ddl); err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
	}
}

// pgDecimalTable is a fixture the TPC-H schema cannot provide. TPC-H models
// every monetary column as FLOAT64 (benchmarks/tpch/schema.go: "Monetary
// values use FLOAT64"), so no arm of this oracle ever put a predicate on a
// DECIMAL column — and #438 was `WHERE v = 0.25` on a DECIMAL(9,2) returning
// no rows at all. PostgreSQL's `numeric` is the authority on what those
// predicates mean (ADR-0012), so the fixture gains a table rather than the
// question going unasked.
//
// It is DELIBERATELY not in AllTables: that map drives the harness, the
// seeders and both data tiers, and this table has no place in any of them.
const (
	pgDecimalTable    = "dec_probe"
	pgDecimalRows_    = 200
	pgDecimalRowGroup = 50
)

// pgBytesTable is the second fixture the TPC-H schema cannot provide, for the
// same reason dec_probe is the first: `bytea` appeared NOWHERE in benchmarks/
// or internal/oracle/ before #570, so neither arm of this oracle had ever
// compared a BYTES value or the OID it is declared under — and the wire was
// answering OID 25 (text) with Go's %v of the byte slice ("[255 254 0 65]").
//
// It is DELIBERATELY not in AllTables, exactly as dec_probe is not: that map
// drives the harness, the seeders and both data tiers.
const pgBytesTable = "bytea_probe"

// oracleTables is AllTables plus the fixtures that exist only for this oracle.
func oracleTables() map[string]parquet.Schema {
	out := make(map[string]parquet.Schema, len(AllTables)+1)
	for name, schema := range AllTables {
		out[name] = schema
	}
	out[pgDecimalTable] = parquet.Schema{Columns: []parquet.Column{
		{Name: "d_key", Type: parquet.TypeInt64},
		{Name: "d_2", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
		{Name: "d_4", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true},
		// The WIDE arm. Precision 38 is the other side of ADR-0018 §4's
		// encoding rule — an INT32 leaf to 9 digits, INT64 to 18, and
		// FIXED_LEN_BYTE_ARRAY beyond — and it is the arm the fixture had no
		// column for, so every DECIMAL entry above was exercising the INT64
		// path only. Its values need more than 64 bits, which is exactly the
		// range #429 could not write and #437's old reader silently
		// truncates.
		{Name: "d_wide", Type: parquet.TypeDecimal, Precision: 38, Scale: 10, Nullable: true},
	}}
	out[pgBytesTable] = parquet.Schema{Columns: []parquet.Column{
		{Name: "b_key", Type: parquet.TypeInt64},
		{Name: "b_val", Type: parquet.TypeBytes, Nullable: true},
		// A second BYTES column so column-to-column comparison, and a join
		// key, have something to be compared against.
		{Name: "b_other", Type: parquet.TypeBytes, Nullable: true},
	}}
	// The multi-key correlated-subquery fixture (#562). It is here rather than
	// in a second oracle because it needs exactly what this one already has —
	// one source feeding both engines — and because the shapes it carries are
	// ones only PostgreSQL can arbitrate: a correlated EXISTS keyed on two
	// columns, over DATE, CIDR and UUID columns the TPC-H schema does not have
	// at all.
	for _, t := range multikey.Tables() {
		out[t.Name] = t.Schema
	}
	return out
}

// pgBytesRows is the BYTES fixture: the values a rendering can go wrong on,
// not a distribution.
//
//	empty          zero bytes, which must stay distinguishable from NULL —
//	               `\x` on the wire, a zero LENGTH, never a negative one
//	all-zero       four NULs, which no text-format PostgreSQL field can carry
//	               and which libpq's strlen-based reader truncates at
//	high bytes    0xff 0xfe 0x00 0x41: invalid UTF-8 AND an embedded NUL, the
//	               exact value #570 was filed with
//	ASCII text     the case that looks fine under every wrong rendering
//	NULL           the one a wrong NULL representation collapses into ""
//
// b_other repeats the set shifted by one row, so b_val = b_other holds for
// exactly one row and the column-to-column comparisons are not all-or-nothing.
func pgBytesRows() []map[string]any {
	vals := [][]byte{
		{},
		{0x00, 0x00, 0x00, 0x00},
		{0xff, 0xfe, 0x00, 0x41},
		[]byte("hi"),
		[]byte("Hi"),
		{0x41},
		[]byte("wadjet"),
		{0xff},
		{0x00},
		[]byte("hi there"),
	}
	rows := make([]map[string]any, 0, len(vals)+1)
	for i, v := range vals {
		r := map[string]any{"b_key": int64(i), "b_val": v}
		r["b_other"] = vals[(i+1)%len(vals)]
		rows = append(rows, r)
	}
	// A row NULL in both columns, and one NULL in only b_val, so IS NULL and
	// the three-valued comparisons have something to answer about — plus one
	// row where the two columns hold the SAME bytes, so `b_val = b_other` is
	// not vacuously empty.
	rows = append(rows,
		map[string]any{"b_key": int64(len(vals))},
		map[string]any{"b_key": int64(len(vals) + 1), "b_other": []byte("hi")},
		map[string]any{"b_key": int64(len(vals) + 2), "b_val": []byte("hi"), "b_other": []byte("hi")},
	)
	return rows
}

// pgBytesDuckDBSetup spells pgBytesRows as DuckDB DDL, so the DuckDB arm and
// the PostgreSQL arm look at the SAME rows rather than at two hand-kept
// copies of "the same" fixture. unhex() is DuckDB's BLOB constructor and
// takes the hex form on both sides of the comparison, so no value here
// depends on how a byte is escaped inside a string literal.
func pgBytesDuckDBSetup() string {
	schema := oracleTables()[pgBytesTable]
	cols := make([]string, 0, len(schema.Columns))
	for _, c := range schema.Columns {
		typ := "BIGINT"
		if c.Type == parquet.TypeBytes {
			typ = "BLOB"
		}
		cols = append(cols, c.Name+" "+typ)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE %s (%s);\n", pgBytesTable, strings.Join(cols, ", "))
	for _, r := range pgBytesRows() {
		vals := make([]string, 0, len(schema.Columns))
		for _, c := range schema.Columns {
			switch v := r[c.Name].(type) {
			case nil:
				vals = append(vals, "NULL")
			case int64:
				vals = append(vals, strconv.FormatInt(v, 10))
			case []byte:
				vals = append(vals, fmt.Sprintf("unhex('%x')", v))
			default:
				panic(fmt.Sprintf("pgBytesDuckDBSetup: %s holds %T", c.Name, v))
			}
		}
		fmt.Fprintf(&b, "INSERT INTO %s VALUES (%s);\n", pgBytesTable, strings.Join(vals, ", "))
	}
	return b.String()
}

// pgDecimalRows is MONOTONIC in both decimal columns and spans both signs, so
// each row group's bounds are narrow and a literal in the middle of one is
// outside every other's — the only shape a wrong row-group prune is visible
// through. Every value is exact at its column's scale.
func pgDecimalRows() []map[string]any {
	rows := make([]map[string]any, pgDecimalRows_)
	for i := range rows {
		r := map[string]any{"d_key": int64(i)}
		// Every 17th row is NULL in d_2 and every 23rd in d_4, on strides that
		// do not line up with the row-group boundary.
		if i%17 != 0 {
			r["d_2"] = float64(i-100) * 0.25
		}
		if i%23 != 0 {
			r["d_4"] = float64(i-100) * 0.0625
		}
		// d_wide is boxed as parquet.Decimal128 — the UNSCALED 128-bit
		// integer, which ADR-0018 §4 makes the writer's verbatim input. A
		// float64 box would be scaled on the way in and cannot hold 25
		// significant digits, and a numeric STRING box goes through
		// strconv.ParseFloat in decimalUnscaledInt64 and loses them the same
		// way. Every non-zero value here needs at least 77 bits.
		if i%13 != 0 {
			r["d_wide"] = pgWideUnscaled(i)
		}
		rows[i] = r
	}
	return rows
}

// postgresColumnType maps one Wadjet column type to the PostgreSQL type that
// carries the same values and declares the same wire OID.
func postgresColumnType(t *testing.T, c parquet.Column) string {
	t.Helper()
	switch c.Type {
	case parquet.TypeBool:
		return "boolean"
	case parquet.TypeInt32:
		return "integer"
	case parquet.TypeInt64:
		return "bigint"
	case parquet.TypeFloat32:
		return "real"
	case parquet.TypeFloat64:
		return "double precision"
	case parquet.TypeString:
		// COLLATE "C": see postgresCollation.
		return "text COLLATE " + postgresCollation
	case parquet.TypeDate:
		return "date"
	case parquet.TypeTimestamp:
		return "timestamp"
	case parquet.TypeDecimal:
		return fmt.Sprintf("numeric(%d,%d)", c.Precision, c.Scale)
	case parquet.TypeBytes:
		// bytea, which is byte-ordered and byte-compared in every collation:
		// no COLLATE clause applies to it and none is needed (#570).
		return "bytea"
	case parquet.TypeCIDR:
		// PostgreSQL's own network type, not text: its equality compares the
		// address AND the mask length, which is the comparison a CIDR key has
		// to make. Wadjet stores a CIDR as unvalidated text, so the fixture's
		// values are canonical networks — `cidr` rejects a set host bit, and
		// a fixture PostgreSQL refuses to load is not an oracle.
		return "cidr"
	case parquet.TypeUUID:
		return "uuid"
	default:
		t.Fatalf("column %s has type %s, which the PostgreSQL oracle does not map yet", c.Name, c.Type)
		return ""
	}
}

// copyInto loads one chunk through COPY, which is what keeps the larger tier a
// couple of minutes rather than an afternoon of INSERTs.
func (o *postgresOracle) copyInto(ctx context.Context, table string, rows []map[string]any) error {
	schema, ok := oracleTables()[table]
	if !ok {
		return fmt.Errorf("no schema for table %s", table)
	}
	cols := make([]string, len(schema.Columns))
	for i, c := range schema.Columns {
		cols[i] = c.Name
	}
	values := make([][]any, len(rows))
	for i, row := range rows {
		v := make([]any, len(cols))
		for j, col := range cols {
			val := row[col]
			if d, ok := val.(parquet.Decimal128); ok {
				val = pgNumericOf(d, int32(schema.Columns[j].Scale))
			}
			v[j] = val
		}
		values[i] = v
	}
	_, err := o.conn.CopyFrom(ctx, pgx.Identifier{table}, cols, pgx.CopyFromRows(values))
	return err
}

// runPostgres executes sql on the oracle and returns rows keyed by column name,
// in the shape oracle.FingerprintOf consumes.
//
// Values arrive through pgx's default (binary where it can) decoding, which is
// deliberate: it is the same path a Go client takes, so a value this arm
// compares is a value a client would actually see.
func (o *postgresOracle) runPostgres(ctx context.Context, sql string) ([]map[string]any, []string, error) {
	return o.runPostgresBoxed(ctx, sql, false)
}

// runPostgresExact is runPostgres with NUMERIC values kept as their exact
// decimal text instead of flattened to float64. Used by the corpus entries
// that compare a DECIMAL aggregate as a VALUE (pgCase.exactNumeric): a
// float64 rendering cannot hold what a numeric(38,10) carries, which is the
// whole of #455 — an oracle that rounds both sides to six significant digits
// can only report that the two agree about the first six.
func (o *postgresOracle) runPostgresExact(ctx context.Context, sql string) ([]map[string]any, []string, error) {
	return o.runPostgresBoxed(ctx, sql, true)
}

func (o *postgresOracle) runPostgresBoxed(ctx context.Context, sql string, exact bool) ([]map[string]any, []string, error) {
	rows, err := o.conn.Query(ctx, sql)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	cols := make([]string, len(fields))
	for i, f := range fields {
		cols[i] = f.Name
	}
	out := make([]map[string]any, 0, 64)
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, nil, err
		}
		m := make(map[string]any, len(cols))
		for i, col := range cols {
			v := normalizeTemporal(fields[i].DataTypeOID, vals[i])
			if exact {
				if text, ok := exactNumericText(v); ok {
					m[col] = text
					continue
				}
			}
			m[col] = normalizePostgresValue(v)
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return out, cols, nil
}

// normalizePostgresValue flattens the driver's typing to what the fingerprint
// digests. Only shapes that are not a divergence are flattened here:
//
//   - pgtype.Numeric: PostgreSQL's AVG/SUM over exact types returns NUMERIC,
//     where Wadjet returns a float. That is a real and DELIBERATE difference of
//     type, not of value, and it is reported by the wire arm where it belongs
//     (OIDs). Comparing the VALUE requires one rendering, so numerics render as
//     float64 and the fingerprint's dual precision covers the last digits.
//   - []byte for text: bytes and string digest identically already; the
//     conversion only keeps the diff output readable.
func normalizePostgresValue(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case []byte:
		return string(x)
	case bool, string, int16, int32, int64, float32, float64:
		return x
	default:
		// pgtype.Numeric and friends implement Float64Value(); anything that
		// can say what number it is, says so.
		if f, ok := numericFloat(v); ok {
			return f
		}
		return fmt.Sprint(v)
	}
}

// normalizeTemporal renders a PostgreSQL DATE or TIMESTAMP as the canonical
// string both engines can be compared on.
//
// This is a RENDERING decision of the harness, not an exemption. Wadjet carries
// a date as its YYYY-MM-DD text (benchmarks/tpch/schema.go stores the fixture's
// date columns as strings, and CAST(x AS DATE) keeps that boxing); PostgreSQL
// hands the driver a time.Time. The two describe the SAME DAY, and a comparison
// of "1996-01-09" against Go's default time.Time formatting would report a
// divergence about neither engine. The TYPE difference this papers over is not
// dropped — it is exactly what the wire arm reports as a type_oids divergence
// on DateColumn and TimestampColumn, which is where a client actually meets it.
func normalizeTemporal(oid uint32, v any) any {
	t, ok := v.(time.Time)
	if !ok {
		return v
	}
	switch oid {
	case 1082: // date
		return t.Format("2006-01-02")
	case 1114, 1184: // timestamp, timestamptz
		return t.UTC().Format("2006-01-02 15:04:05")
	case 1083, 1266: // time, timetz
		return t.Format("15:04:05")
	default:
		return v
	}
}

// exactNumericText renders a PostgreSQL NUMERIC as its exact decimal digits.
// pgtype.Numeric carries the value as (Int, Exp) — Int x 10^Exp — which is
// exactly the unscaled-integer-plus-scale form a DECIMAL column holds, so no
// float64 is involved anywhere on this path.
//
// ok=false for anything that is not a finite numeric; the caller falls back
// to the ordinary float flattening.
func exactNumericText(v any) (string, bool) {
	n, ok := v.(pgtype.Numeric)
	if !ok || !n.Valid || n.NaN || n.Int == nil || n.InfinityModifier != pgtype.Finite {
		return "", false
	}
	unscaled := new(big.Int).Set(n.Int)
	scale := 0
	if n.Exp < 0 {
		scale = int(-n.Exp)
	} else if n.Exp > 0 {
		unscaled.Mul(unscaled, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n.Exp)), nil))
	}
	return canonicalDecimalText(unscaled, scale), true
}

func numericFloat(v any) (float64, bool) {
	if fv, ok := v.(pgtype.Float64Valuer); ok {
		f, err := fv.Float64Value()
		if err == nil && f.Valid {
			return f.Float64, true
		}
	}
	return 0, false
}

// redactDSN hides the password when a DSN reaches a failure message.
func redactDSN(dsn string) string {
	at := strings.LastIndexByte(dsn, '@')
	scheme := strings.Index(dsn, "://")
	if at < 0 || scheme < 0 || at < scheme {
		return dsn
	}
	return dsn[:scheme+3] + "***" + dsn[at:]
}
