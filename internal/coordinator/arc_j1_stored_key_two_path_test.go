package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// A STORED COLUMN IN THE RESERVED NAMESPACE SURVIVES A LATERAL JOIN — the
// drop is by IDENTITY, not by name (arc J1 round 2, ADR-0026 §3c).
//
// Reading is not minting, so a table may already store a column called
// `__key_0` (ADR-0012, `TestStoredReservedColumnStaysReadable`). The first cut
// of the correlation-key drop matched the exclusion by BARE NAME over the
// join's whole output, and the lateral's slot allocator was seeded only from
// the lateral's own text — so a `__key_0` on the OUTER relation was dropped by
// the join that minted a slot of that name:
//
//	SELECT * FROM jko o JOIN LATERAL (SELECT MAX(amount) AS mx FROM jki
//	  WHERE order_id = o.id) s ON true
//	  base      id,__key_0,order_id,mx   (the phantom key, and the user's column)
//	  first cut id,mx                    ← the USER's column, gone
//	  PG 17     id,__key_0,mx
//
// and `SELECT o.__key_0` read NULL where PostgreSQL reads its values: right →
// silently wrong, which is the blocker class by itself.
//
// Two rules put it back. The join drops what it MINTED — a column is excluded
// only where its name is a JOIN KEY OF ITS OWN SIDE, so a stored one on the
// other side is a different column and stays. And the allocator is seeded with
// the OUTER side's names as well, so where this layer can see the stored name
// at all the mint steps past it.
//
// Every Want below is live PostgreSQL 17 over the same rows.
func TestArcJ1AStoredReservedColumnSurvivesALateral(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })
	spilled := e3BudgetedStandalone(t, ctx)

	jkWriteTables(t, ctx, infra, infraB)
	jkIngestTables(t, ctx, single)
	jkIngestTables(t, ctx, spilled)

	arms := []e3Arm{
		{"single", func(sql string) ([]string, [][]any, error) { return e3PosSingle(ctx, single, sql) }, nil},
		{spilledArm, func(sql string) ([]string, [][]any, error) { return e3PosSingle(ctx, spilled, sql) }, nil},
		{"dag", func(sql string) ([]string, [][]any, error) { return e3PosDAG(ctx, coord, sql) }, coord},
		{"dagshuf", func(sql string) ([]string, [][]any, error) { return e3PosDAG(ctx, coordB, sql) }, coordB},
	}

	const lat = `JOIN LATERAL (SELECT MAX(amount) AS mx FROM jki WHERE order_id = o.id) s ON true`

	for _, tc := range []struct {
		name, sql, want string
		wantDAG         string
		pgSays          string
	}{
		// THE REGRESSION, from both sides of the join.
		{name: "star-keeps-the-outer-stored-key",
			sql: `SELECT * FROM jko o ` + lat + ` ORDER BY o.id`,
			want: `id,__key_0,__key_1,__sortkey_0,mx | 1,mine-1,k1-1,s-1,100 | ` +
				`2,mine-2,k1-2,s-2,75`},
		{name: "the-outer-stored-key-by-name",
			sql:  `SELECT o.__key_0 AS mine, s.mx AS mx FROM jko o ` + lat + ` ORDER BY o.id`,
			want: `mine,mx | mine-1,100 | mine-2,75`},
		{name: "every-stored-family-by-name",
			sql: `SELECT o.__key_0 AS a, o.__key_1 AS b, o.__sortkey_0 AS c FROM jko o ` +
				lat + ` ORDER BY o.id`,
			want: `a,b,c | mine-1,k1-1,s-1 | mine-2,k1-2,s-2`},
		// A `__`-PREFIXED NAME read through a DERIVED STAR over a lateral is
		// LOUD on both DAG arms, stored or not, and the control two cells
		// down says it is the NAME and not the shape. PRE-EXISTING and not
		// this arc's: `logical.sanitizeScanNeeds` keeps every `__`-prefixed
		// reference in a scan's required columns (a fused scan-aggregate
		// fragment really does emit `__having_N` and `__gb_expr_N`), the
		// reference reaches the LATERAL's scan, that scan cannot provide it,
		// and the worker's all-or-nothing projection guard takes the whole
		// list down with it — `column "order_id" does not exist in the input
		// schema`. Measured identical with this arc's own scan-needs rule
		// disabled, so it is that keep and not the correlation family.
		//
		// Loud, never a wrong value, and the single-process arms answer.
		{name: "pinned-a-reserved-name-through-a-derived-star-is-loud-on-the-dag",
			sql: `SELECT x.__key_0 AS mine FROM (SELECT * FROM jko o JOIN LATERAL (` +
				`SELECT MAX(amount) AS mx FROM lat_item WHERE order_id = o.id) s ON true) x ` +
				`ORDER BY 1`,
			want:    `mine | mine-1 | mine-2`,
			wantDAG: "ERR",
			pgSays:  "mine-1 | mine-2 on every arm"},
		{name: "pinned-the-same-when-the-inner-relation-stores-the-name-too",
			sql:     `SELECT x.__key_0 AS mine FROM (SELECT * FROM jko o ` + lat + `) x ORDER BY 1`,
			want:    `mine | mine-1 | mine-2`,
			wantDAG: "ERR",
			pgSays:  "mine-1 | mine-2 on every arm"},
		{name: "ctl-an-ORDINARY-column-through-the-same-derived-star-answers",
			sql: `SELECT x.id AS i FROM (SELECT * FROM jko o JOIN LATERAL (` +
				`SELECT MAX(amount) AS mx FROM lat_item WHERE order_id = o.id) s ON true) x ` +
				`ORDER BY 1`,
			want: `i | 1 | 2`},
		{name: "through-a-cte-star",
			sql:  `WITH c AS (SELECT * FROM jko o ` + lat + `) SELECT c.__key_1 AS b FROM c ORDER BY 1`,
			want: `b | k1-1 | k1-2`},

		// THE INNER RELATION stores one too, and the lateral's own list names
		// it — which is what puts it in the allocator's seed, so the mint
		// steps to a free slot and both columns survive.
		{name: "the-inner-stored-key-is-published",
			sql: `SELECT o.id AS oid, s.k AS k FROM jko o JOIN LATERAL (` +
				`SELECT __key_0 AS k FROM jki WHERE order_id = o.id) s ON true ORDER BY 1, 2`,
			want: `oid,k | 1,inner-1 | 1,inner-2 | 2,inner-3`},
		{name: "the-inner-stored-key-through-a-star",
			sql: `SELECT o.id AS oid, s.__key_0 AS k FROM jko o JOIN LATERAL (` +
				`SELECT * FROM jki WHERE order_id = o.id) s ON true ORDER BY 1, 2`,
			want: `oid,k | 1,inner-1 | 1,inner-2 | 2,inner-3`},

		// THE CONTROLS: the same tables with no lateral at all, and the
		// minted slot still absent from a star that has one.
		{name: "ctl-no-lateral", sql: `SELECT * FROM jko ORDER BY id`,
			want: `id,__key_0,__key_1,__sortkey_0 | 1,mine-1,k1-1,s-1 | 2,mine-2,k1-2,s-2`},
		{name: "ctl-the-lateral-still-hides-what-it-minted",
			sql: `SELECT * FROM lat_ord o JOIN LATERAL (SELECT MAX(amount) AS mx ` +
				`FROM lat_item WHERE order_id = o.id) s ON true ORDER BY o.id`,
			want: `id,customer,total,mx | 1,Alice,150,100 | 2,Bob,200,125 | 3,Carol,0,NULL`},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				want := tc.want
				if tc.wantDAG != "" && arm.coord != nil {
					want = tc.wantDAG
				}
				cols, rows, err := arm.run(tc.sql)
				if err != nil {
					if want == "ERR" {
						continue
					}
					t.Fatalf("%s arm: %v\n  want %s (live PostgreSQL 17)\n  SQL: %s",
						arm.name, err, want, tc.sql)
				}
				if want == "ERR" {
					t.Fatalf("%s arm ANSWERED %s where the pin says it fails — the "+
						"same-side slot collision is closed, so delete the pin\n  SQL: %s",
						arm.name, e3Render(cols, rows), tc.sql)
				}
				if got := e3Render(cols, rows); got != want {
					t.Fatalf("%s arm: %s\n  want %s (live PostgreSQL 17)\n  PostgreSQL: %s\n  SQL: %s",
						arm.name, got, want, tc.pgSays, tc.sql)
				}
			}
		})
	}
}

const (
	jkOuter = "jko"
	jkInner = "jki"
)

func jkOuterSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "__key_0", Type: parquet.TypeString, Nullable: true},
		{Name: "__key_1", Type: parquet.TypeString, Nullable: true},
		{Name: "__sortkey_0", Type: parquet.TypeString, Nullable: true},
	}}
}

func jkInnerSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "order_id", Type: parquet.TypeInt64},
		{Name: "amount", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "__key_0", Type: parquet.TypeString, Nullable: true},
	}}
}

func jkOuterRows() []map[string]any {
	return []map[string]any{
		{"id": int64(1), "__key_0": "mine-1", "__key_1": "k1-1", "__sortkey_0": "s-1"},
		{"id": int64(2), "__key_0": "mine-2", "__key_1": "k1-2", "__sortkey_0": "s-2"},
	}
}

func jkInnerRows() []map[string]any {
	return []map[string]any{
		{"order_id": int64(1), "amount": 50.0, "__key_0": "inner-1"},
		{"order_id": int64(1), "amount": 100.0, "__key_0": "inner-2"},
		{"order_id": int64(2), "amount": 75.0, "__key_0": "inner-3"},
	}
}

// jkWriteTables writes both fixtures straight through the CATALOG — the way a
// binary that predates the reservation did, and the only way to produce such a
// schema now that the DDL, API and ingest doors refuse it (42939).
func jkWriteTables(t *testing.T, ctx context.Context, infras ...tmdInfraT) {
	t.Helper()
	for _, tbl := range []struct {
		name   string
		schema parquet.Schema
		rows   []map[string]any
	}{
		{jkOuter, jkOuterSchema(), jkOuterRows()},
		{jkInner, jkInnerSchema(), jkInnerRows()},
	} {
		for _, infra := range infras {
			if err := infra.cat.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
				t.Fatalf("create %s: %v", tbl.name, err)
			}
			var buf bytes.Buffer
			pw, err := parquet.NewWriter(&buf, tbl.schema, parquet.DefaultWriterConfig())
			if err != nil {
				t.Fatalf("parquet writer: %v", err)
			}
			if err := pw.WriteRows(tbl.rows); err != nil {
				t.Fatalf("write rows: %v", err)
			}
			if err := pw.Close(); err != nil {
				t.Fatalf("close writer: %v", err)
			}
			path := fmt.Sprintf("tables/%s/chunk_0000.parquet", tbl.name)
			payload := buf.Bytes()
			if _, err := infra.store.Put(ctx, "test", path, bytes.NewReader(payload),
				int64(len(payload)), "application/octet-stream"); err != nil {
				t.Fatalf("put: %v", err)
			}
			if err := infra.cat.AddFiles(ctx, tbl.name, map[string]string{},
				"tables/"+tbl.name+"/", []catalog.FileEntry{{Path: path,
					SizeBytes: int64(len(payload)), NumRows: int64(len(tbl.rows)),
					CreatedAt: time.Now()}}); err != nil {
				t.Fatalf("add files: %v", err)
			}
		}
	}
}

// jkIngestTables is jkWriteTables for a single-process arm, through the
// ingester primitive rather than the wadjet API, for the same reason.
func jkIngestTables(t *testing.T, ctx context.Context, db *wadjet.DB) {
	t.Helper()
	for _, tbl := range []struct {
		name   string
		schema parquet.Schema
		rows   []map[string]any
	}{
		{jkOuter, jkOuterSchema(), jkOuterRows()},
		{jkInner, jkInnerSchema(), jkInnerRows()},
	} {
		if err := db.Catalog().CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
			t.Fatalf("create %s: %v", tbl.name, err)
		}
		ing := ingest.New(db.Catalog(), tbl.name, tbl.schema, nil,
			ingest.Config{MaxBufferRows: len(tbl.rows) + 1, RowGroupSize: 128})
		if err := ing.Ingest(ctx, tbl.rows); err != nil {
			t.Fatalf("ingest %s: %v", tbl.name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", tbl.name, err)
		}
	}
}

// A READING cell for every family the reservation covers, which is the half
// `TestArcJ1TheReservedNamespaceRefusesOnlyMinting`'s name claims and its
// first draft did not have: all four of its cells MINTED.
func TestArcJ1TheReservedNamespaceIsReadableWhereItIsStored(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	jkWriteTables(t, ctx, infra)
	jkIngestTables(t, ctx, single)

	for _, tc := range []struct{ name, sql, want string }{
		{"star", `SELECT * FROM jko ORDER BY id`,
			`id,__key_0,__key_1,__sortkey_0 | 1,mine-1,k1-1,s-1 | 2,mine-2,k1-2,s-2`},
		{"by-name", `SELECT __key_0 FROM jko ORDER BY id`, `__key_0 | mine-1 | mine-2`},
		{"under-a-user-alias", `SELECT __key_1 AS k FROM jko ORDER BY id`, `k | k1-1 | k1-2`},
		{"in-a-predicate", `SELECT id FROM jko WHERE __sortkey_0 = 's-2'`, `id | 2`},
		{"in-a-group-by", `SELECT __key_0 AS k, COUNT(*) AS n FROM jko GROUP BY __key_0 ORDER BY 1`,
			`k,n | mine-1,1 | mine-2,1`},
		{"in-an-order-by", `SELECT id FROM jko ORDER BY __key_0 DESC`, `id | 2 | 1`},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range []e3Arm{
				{"single", func(sql string) ([]string, [][]any, error) {
					return e3PosSingle(ctx, single, sql)
				}, nil},
				{"dag", func(sql string) ([]string, [][]any, error) {
					return e3PosDAG(ctx, coord, sql)
				}, coord},
			} {
				cols, rows, err := arm.run(tc.sql)
				if err != nil {
					t.Fatalf("%s arm REFUSED a read of a stored column: %v\n  SQL: %s\n"+
						"reading is not minting — a table an older binary wrote stays "+
						"readable (ADR-0012)", arm.name, err, tc.sql)
				}
				if got := e3Render(cols, rows); got != strings.TrimSpace(tc.want) {
					t.Fatalf("%s arm: %s\n  want %s\n  SQL: %s", arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}
}
