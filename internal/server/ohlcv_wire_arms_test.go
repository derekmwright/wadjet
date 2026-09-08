package server

// THE BAR'S DECLARATION ON THE WIRE, ON BOTH DOORS — #965 round 2, B1.
//
// `coordinator.TestTheBarsDeclaredTypeIsTheSameOnEveryArm` holds five
// EXECUTION arms to one (type, precision, scale). This is the other half: what
// a CLIENT reads. A PostgreSQL client binds by `RowDescription` — the type OID
// and the typmod beside it — so `(b).open` going out as numeric(18,4) from a
// standalone server and numeric(0,4) from a coordinator is two different
// columns to pgx, pgJDBC, Superset and DataGrip, and it is invisible to every
// value oracle (ADR-0012: the wire arm is the one DuckDB cannot provide).
//
// That divergence was real at round 1 and this is the gate that would have
// caught it: numeric's typmod carries the precision, and DECIMAL(0,s) is not
// a type at all (ADR-0024, #685). The EMPTY-input half is the sharper one —
// over no rows the values cannot show a wrong declaration, and the round-1
// defect made the DAG's fold invent FLOAT64 for a column of empty states, so
// the same statement declared OID 1700 on one door and 701 on the other.
//
// Both doors are real pgwire servers over the same rows; the DAG door differs
// only in `SetCoordinator`. The expected declarations are the census's own
// list, parsed into (OID, typmod) here so the two gates cannot drift apart.

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/coordinator"
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/server/pgwire"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
	"github.com/jackc/pgx/v5"
)

// owaCell is one price × volume pairing and the declaration each bar field
// owes. `decls` is in the census's own spelling — INT32, FLOAT64,
// DECIMAL(p,s) — because these ARE the census's cells.
type owaCell struct {
	name   string
	price  string
	volume string
	decls  []string
}

func owaCells() []owaCell {
	return []owaCell{
		{"int32_price_int32_volume", "px_i32", "vol_i32",
			[]string{"INT32", "INT32", "INT32", "INT32", "INT64", "DECIMAL(38,4)"}},
		{"int64_price_int64_volume", "px_i64", "vol_i64",
			[]string{"INT64", "INT64", "INT64", "INT64", "DECIMAL(38,0)", "DECIMAL(38,4)"}},
		{"float64_price_float64_volume", "px_f64", "vol_f64",
			[]string{"FLOAT64", "FLOAT64", "FLOAT64", "FLOAT64", "FLOAT64", "FLOAT64"}},
		{"float32_price_int32_volume", "px_f32", "vol_i32",
			[]string{"FLOAT32", "FLOAT32", "FLOAT32", "FLOAT32", "INT64", "FLOAT64"}},
		{"decimal92_price_int32_volume", "px_d92", "vol_i32",
			[]string{"DECIMAL(9,2)", "DECIMAL(9,2)", "DECIMAL(9,2)", "DECIMAL(9,2)", "INT64", "DECIMAL(38,6)"}},
		{"decimal184_price_int64_volume", "px_d184", "vol_i64",
			[]string{"DECIMAL(18,4)", "DECIMAL(18,4)", "DECIMAL(18,4)", "DECIMAL(18,4)", "DECIMAL(38,0)", "DECIMAL(38,8)"}},
		{"decimal184_price_decimal92_volume", "px_d184", "vol_d92",
			[]string{"DECIMAL(18,4)", "DECIMAL(18,4)", "DECIMAL(18,4)", "DECIMAL(18,4)", "DECIMAL(38,2)", "DECIMAL(38,8)"}},
		{"decimal3810_price_int32_volume", "px_d3810", "vol_i32",
			[]string{"DECIMAL(38,10)", "DECIMAL(38,10)", "DECIMAL(38,10)", "DECIMAL(38,10)", "INT64", "DECIMAL(38,14)"}},
		{"decimal184_price_float64_volume", "px_d184", "vol_f64",
			[]string{"DECIMAL(18,4)", "DECIMAL(18,4)", "DECIMAL(18,4)", "DECIMAL(18,4)", "FLOAT64", "FLOAT64"}},
		{"float64_price_int64_volume", "px_f64", "vol_i64",
			[]string{"FLOAT64", "FLOAT64", "FLOAT64", "FLOAT64", "DECIMAL(38,0)", "FLOAT64"}},
		{"computed_decimal_price", "px_d184*2", "vol_i64",
			[]string{"DECIMAL(20,4)", "DECIMAL(20,4)", "DECIMAL(20,4)", "DECIMAL(20,4)", "DECIMAL(38,0)", "DECIMAL(38,8)"}},
	}
}

func (c owaCell) sql(empty bool) string {
	where := ""
	if empty {
		where = " WHERE id < 0"
	}
	return fmt.Sprintf(
		`SELECT (b).open AS o, (b).high AS h, (b).low AS l, (b).close AS c,
		        (b).volume AS v, (b).vwap AS w
		 FROM (SELECT ohlcv(ts, %s, %s) AS b FROM owabars%s) t`,
		c.price, c.volume, where)
}

// owaWire is the wire face of one declared type: what PostgreSQL 17 sends for
// the same column. numeric's typmod is ((precision<<16)|scale) + VARHDRSZ,
// which is the only place on the wire the precision appears at all.
func owaWire(t *testing.T, decl string) (uint32, int32) {
	t.Helper()
	switch decl {
	case "INT32":
		return 23, -1
	case "INT64":
		return 20, -1
	case "FLOAT32":
		return 700, -1
	case "FLOAT64":
		return 701, -1
	}
	if !strings.HasPrefix(decl, "DECIMAL(") || !strings.HasSuffix(decl, ")") {
		t.Fatalf("owaWire has no wire face for %q", decl)
	}
	parts := strings.SplitN(decl[len("DECIMAL("):len(decl)-1], ",", 2)
	if len(parts) != 2 {
		t.Fatalf("malformed declaration %q", decl)
	}
	p, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil {
		t.Fatal(err)
	}
	s, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil {
		t.Fatal(err)
	}
	return 1700, int32((p<<16)|(s&0xFFFF)) + 4
}

func TestTheBarDeclaresTheSameThingOnBothWireDoors(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster and three workers")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	single, dag, coord := owaDoors(t, ctx)

	for _, c := range owaCells() {
		t.Run(c.name, func(t *testing.T) {
			for _, empty := range []bool{false, true} {
				label, sql := "rows", c.sql(false)
				if empty {
					label, sql = "empty", c.sql(true)
				}
				want := make([]string, len(c.decls))
				for i, d := range c.decls {
					oid, tm := owaWire(t, d)
					want[i] = fmt.Sprintf("%s(oid=%d,typmod=%d)", d, oid, tm)
				}
				for _, door := range []struct {
					name, addr string
				}{{"pgwire/single", single}, {"pgwire/dag", dag}} {
					got, err := owaDescribe(ctx, door.addr, sql)
					if err != nil {
						t.Errorf("%s (%s): %v\n  SQL: %s", door.name, label, err, sql)
						continue
					}
					if len(got) != len(want) {
						t.Errorf("%s (%s): RowDescription has %d fields, want %d: %v",
							door.name, label, len(got), len(want), got)
						continue
					}
					for i := range want {
						if got[i] != want[i] {
							t.Errorf("%s (%s) field %d\n  got  %s\n  want %s\n  SQL: %s",
								door.name, label, i, got[i], want[i], sql)
						}
					}
				}
			}
		})
	}

	// THE DAG DOOR ASSERTS ITS OWN ENGAGEMENT. Two green doors are worth
	// nothing if the second one quietly answered in process: that is a second
	// copy of `pgwire/single` reporting a divergence it cannot see. Every bar
	// above crosses the coordinator's partial/final decomposition, which is
	// the counter this arc added for exactly this purpose (ADR-0035).
	if n := coord.OhlcvStateRoutes(); n == 0 {
		t.Errorf("the DAG door decomposed NO bar into partial states — it answered "+
			"in process, so `pgwire/dag` is a duplicate of `pgwire/single` and this "+
			"gate proved nothing. LocalFastPathBytes is 0 and %d workers are "+
			"registered; check that SetCoordinator still routes SELECT.",
			coord.Workers().Count())
	}
}

// owaDescribe returns one statement's RowDescription rendered as the DECLARED
// type each field carries, read back through pgx — the same lens a client
// has. The rows are drained because a RowDescription that is right until the
// first DataRow arrives is still wrong.
func owaDescribe(ctx context.Context, addr, sql string) ([]string, error) {
	conn, err := pgx.Connect(ctx, fmt.Sprintf(
		"postgres://wadjet:none@%s/wadjet?sslmode=disable", addr))
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)
	rows, err := conn.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]string, 0, 6)
	for _, fd := range rows.FieldDescriptions() {
		out = append(out, fmt.Sprintf("%s(oid=%d,typmod=%d)",
			owaDeclName(fd.DataTypeOID, fd.TypeModifier), fd.DataTypeOID, fd.TypeModifier))
	}
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// owaDeclName reads the wire back the other way, so a mismatch names the type
// a client would actually see rather than two integers.
func owaDeclName(oid uint32, typmod int32) string {
	switch oid {
	case 23:
		return "INT32"
	case 20:
		return "INT64"
	case 700:
		return "FLOAT32"
	case 701:
		return "FLOAT64"
	case 1114:
		return "TIMESTAMP"
	case 1700:
		if typmod < 0 {
			return "DECIMAL(unconstrained)"
		}
		m := typmod - 4
		return fmt.Sprintf("DECIMAL(%d,%d)", m>>16, m&0xFFFF)
	}
	return fmt.Sprintf("oid%d", oid)
}

// owaDoors stands up two pgwire servers over ONE set of rows: a single-process
// door and a coordinator (DAG) door. They differ in `SetCoordinator` and in
// nothing else, which is what makes a divergence between them a defect rather
// than a difference in fixture.
func owaDoors(t *testing.T, ctx context.Context) (singleAddr, dagAddr string, coord *coordinator.Coordinator) {
	t.Helper()
	natsCfg := distributed.DefaultNATSConfig()
	natsCfg.Port = -1
	natsCfg.StoreDir = t.TempDir()
	embedded, err := distributed.NewEmbeddedNATS(natsCfg, nil)
	if err != nil {
		t.Fatalf("embedded nats: %v", err)
	}
	t.Cleanup(embedded.Shutdown)
	nc, err := distributed.ConnectInProcess(embedded.Server())
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	t.Cleanup(func() { nc.Close() })
	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatal(err)
	}
	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatal(err)
	}
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	kv, err := catalog.NewNATSKV(js)
	if err != nil {
		t.Fatal(err)
	}
	cat := catalog.New(kv, store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}

	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test", MetaKV: kv})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	owaWriteBars(t, ctx, db)

	ids := make([]string, 3)
	for i := range ids {
		ids[i] = fmt.Sprintf("owa-worker-%d", i)
		w := worker.New(worker.Config{
			WorkerID: ids[i], NATSUrl: embedded.ClientURL(),
			MaxConcurrent: 4, CacheBytes: 64 << 20, SpillDir: t.TempDir(),
		}, store, nc, js, nil)
		wctx, wcancel := context.WithCancel(context.Background())
		t.Cleanup(wcancel)
		if err := w.Start(wctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(w.Stop)
	}
	coord = coordinator.New(coordinator.Config{
		NATSUrl: embedded.ClientURL(), ResultBucket: "test", LocalFastPathBytes: 0,
	}, cat, nc, js, nil)
	deadline := time.Now().Add(30 * time.Second)
	for coord.Workers().Count() < 3 {
		for _, id := range ids {
			hb, err := distributed.Marshal(distributed.WorkerHeartbeat{
				WorkerID: id, MaxConcurrent: 4, Timestamp: time.Now(),
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := nc.Publish(distributed.SubjectHeartbeat, hb); err != nil {
				t.Fatal(err)
			}
		}
		nc.Flush()
		if time.Now().After(deadline) {
			t.Fatalf("workers did not register: %d of 3", coord.Workers().Count())
		}
		time.Sleep(50 * time.Millisecond)
	}

	pgSingle := pgwire.NewServer(db, pgwire.Config{}, nil)
	if err := pgSingle.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pgSingle.Shutdown)
	pgDAG := pgwire.NewServer(db, pgwire.Config{}, nil)
	pgDAG.SetCoordinator(coord)
	if err := pgDAG.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pgDAG.Shutdown)
	return pgSingle.Addr(), pgDAG.Addr(), coord
}

func owaWriteBars(t *testing.T, ctx context.Context, db *wadjet.DB) {
	t.Helper()
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "ts", Type: parquet.TypeTimestamp},
		{Name: "px_f64", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "px_f32", Type: parquet.TypeFloat32, Nullable: true},
		{Name: "px_i64", Type: parquet.TypeInt64, Nullable: true},
		{Name: "px_i32", Type: parquet.TypeInt32, Nullable: true},
		{Name: "px_d92", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
		{Name: "px_d184", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true},
		{Name: "px_d3810", Type: parquet.TypeDecimal, Precision: 38, Scale: 10, Nullable: true},
		{Name: "vol_i64", Type: parquet.TypeInt64, Nullable: true},
		{Name: "vol_i32", Type: parquet.TypeInt32, Nullable: true},
		{Name: "vol_f64", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "vol_d92", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "owabars", schema, nil); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2020, 9, 13, 12, 0, 0, 0, time.UTC).UnixMilli()
	rows := make([]map[string]any, 0, 8)
	for i := 0; i < 8; i++ {
		px := float64(10 + 3*i)
		vol := int64(1 + i)
		rows = append(rows, map[string]any{
			"id": int32(i + 1), "ts": base + int64(i)*10_000,
			"px_f64": px, "px_f32": float32(px), "px_i64": int64(px), "px_i32": int32(px),
			"px_d92":  fmt.Sprintf("%.2f", px),
			"px_d184": fmt.Sprintf("%.4f", px), "px_d3810": fmt.Sprintf("%.10f", px),
			"vol_i64": vol, "vol_i32": int32(vol), "vol_f64": float64(vol),
			"vol_d92": fmt.Sprintf("%d.00", vol),
		})
	}
	ing := db.NewIngester("owabars", schema, nil, ingest.Config{MaxBufferRows: 16, RowGroupSize: 16})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
}

// time_bucket DECLARES timestamp ON BOTH DOORS — #965 round 2, P2.
//
// A value oracle cannot see this: a right instant under OID 25 is a bucket
// every client reads as text, and one that declares `text` sorts lexically in
// Superset and refuses to bind to a `time.Time` in pgx. The function is
// registered beside `date_trunc`, which declares timestamp (OID 1114) with no
// typmod — PostgreSQL sends -1 for an unqualified `timestamp` — so this holds
// `time_bucket` to the same thing on the single-process door and through the
// coordinator, with rows, over an EMPTY input, with an explicit origin, and as
// a GROUP BY key, which is the spelling the function exists for.
func TestTimeBucketDeclaresTimestampOnBothWireDoors(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster and three workers")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	single, dag, coord := owaDoors(t, ctx)
	// Snapshot every way a statement can decide to run IN PROCESS after
	// reaching the coordinator. `time_bucket` has no state to route, so the
	// DAG door's engagement is proven negatively (nothing fell back) and
	// positively (the door itself dispatches — the bar probe below).
	local0 := owaLocalRoutes(coord)

	for _, c := range []struct {
		name, sql string
		want      []string
	}{
		{"bare", `SELECT time_bucket(INTERVAL '1' MINUTE, ts) AS bkt FROM owabars`,
			[]string{"TIMESTAMP(oid=1114,typmod=-1)"}},
		{"empty", `SELECT time_bucket(INTERVAL '1' MINUTE, ts) AS bkt FROM owabars WHERE id < 0`,
			[]string{"TIMESTAMP(oid=1114,typmod=-1)"}},
		{"grouped", `SELECT time_bucket(INTERVAL '1' MINUTE, ts) AS bkt, COUNT(*) AS n
		             FROM owabars GROUP BY 1 ORDER BY 1`,
			[]string{"TIMESTAMP(oid=1114,typmod=-1)", "INT64(oid=20,typmod=-1)"}},
		{"with_an_origin", `SELECT time_bucket(INTERVAL '1' MINUTE, ts,
		                      TIMESTAMP '2020-09-13 12:00:07') AS bkt FROM owabars`,
			[]string{"TIMESTAMP(oid=1114,typmod=-1)"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			for _, door := range []struct{ name, addr string }{
				{"pgwire/single", single}, {"pgwire/dag", dag},
			} {
				got, err := owaDescribe(ctx, door.addr, c.sql)
				if err != nil {
					t.Errorf("%s: %v\n  SQL: %s", door.name, err, c.sql)
					continue
				}
				if strings.Join(got, " | ") != strings.Join(c.want, " | ") {
					t.Errorf("%s\n  got  %s\n  want %s\n  SQL: %s", door.name,
						strings.Join(got, " | "), strings.Join(c.want, " | "), c.sql)
				}
			}
		})
	}

	// ENGAGEMENT, both signs. Two green doors are worth nothing if the second
	// one quietly answered in process — that is a duplicate of `pgwire/single`
	// reporting a divergence it cannot see.
	if got, want := owaLocalRoutes(coord), local0; got != want {
		t.Errorf("a time_bucket statement fell back to an in-process route: %s\n"+
			"before the cells it was %s. Every cell above must reach the DAG or "+
			"`pgwire/dag` is not a second door.", got, want)
	}
	before := coord.OhlcvStateRoutes()
	if _, err := owaDescribe(ctx, dag, `SELECT (b).open AS o
		FROM (SELECT ohlcv(ts, px_d184, vol_i64) AS b FROM owabars) t`); err != nil {
		t.Fatalf("bar probe on the DAG door: %v", err)
	}
	if coord.OhlcvStateRoutes() == before {
		t.Errorf("the DAG door dispatched nothing — `pgwire/dag` answered in "+
			"process. LocalFastPathBytes is 0 and %d workers are registered; check "+
			"that SetCoordinator still routes SELECT.", coord.Workers().Count())
	}
}

// owaLocalRoutes renders every counter that says "this statement reached the
// coordinator and then ran in process anyway", so a fallback shows up as a
// changed string rather than as a silently identical answer.
func owaLocalRoutes(c *coordinator.Coordinator) string {
	return fmt.Sprintf("fastpath=%d groupkey=%d tableless=%d unbuildable=%d "+
		"unreachable=%d scalarproj=%d distinct=%d groupingsets=%d",
		c.LocalFastPathHits(), c.GroupKeyLocalRoutes(), c.TableLessLocalRoutes(),
		c.UnbuildableStageLocalRoutes(), c.UnreachableOutputLocalRoutes(),
		c.ScalarProjectionLocalRoutes(), c.DistinctLocalRoutes(),
		c.GroupingSetsLocalRoutes())
}
