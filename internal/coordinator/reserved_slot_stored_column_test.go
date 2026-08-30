package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// A table that ALREADY STORES a column in a reserved slot family stays fully
// readable, and the planner's own slot moves out of its way.
//
// The reservation's first draft refused a stored column at READ time, and that
// was a trap rather than a guard rail: `oldtab(id, __winkey_1, __win_0,
// plain)` became unreadable by every query — `SELECT *`, `SELECT id, plain`,
// `SELECT COUNT(*)`, an aliased scan — while CREATE TABLE, the CreateTable Go
// API, an Ingester and INSERT all still SUCCEEDED. The trap closed behind the
// user, DROP TABLE was the only exit, and the message told them to alias a
// column they could not select. It also breaks the direction ADR-0018 sets:
// a table written by an older binary must stay readable.
//
// The rule now: reading is never refused; MINTING is (the DDL and ingest
// doors, and an explicit `AS <alias>`); and where a stored name and the
// planner's slot meet in one query, the SLOT renumbers, because the stored
// column is the one the user can see and name.
func TestStoredReservedColumnStaysReadable(t *testing.T) {
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
	coordB := tmdCoordinator(t, ctx, infraB)
	coordB.config.BroadcastBytesOverride = 1

	rsWriteOldTable(t, ctx, infra, infraB)
	rsIngestOldTable(t, ctx, single)

	arms := []struct {
		name string
		run  func(string) (cols []string, rows []map[string]any, err error)
	}{
		{"single", func(sql string) ([]string, []map[string]any, error) {
			r, err := tmdRunSingle(ctx, single, sql)
			if err != nil {
				return nil, nil, err
			}
			return r.Columns, r.Rows, nil
		}},
		{"dag", func(sql string) ([]string, []map[string]any, error) {
			r, err := tmdRunDAG(ctx, coord, sql)
			if err != nil {
				return nil, nil, err
			}
			return r.Columns, r.Rows, nil
		}},
		{"dag-shuffled", func(sql string) ([]string, []map[string]any, error) {
			r, err := tmdRunDAG(ctx, coordB, sql)
			if err != nil {
				return nil, nil, err
			}
			return r.Columns, r.Rows, nil
		}},
	}

	for _, tc := range []struct {
		name string
		sql  string
		// cols is the expected output column list, and want the value of
		// check on each of the four rows.
		cols  []string
		check string
		want  []string
	}{
		{"SELECT star", "SELECT * FROM " + rsOldTab + " ORDER BY id",
			[]string{"id", "__winkey_1", "__win_0", "plain"},
			"__win_0", []string{"100", "200", "300", "400"}},
		{"a projection that omits the reserved columns",
			"SELECT id, plain FROM " + rsOldTab + " ORDER BY id",
			[]string{"id", "plain"}, "plain", []string{"p1", "p2", "p3", "p4"}},
		{"COUNT star", "SELECT COUNT(*) AS n FROM " + rsOldTab,
			[]string{"n"}, "n", []string{"4"}},
		{"through a table alias", "SELECT id FROM " + rsOldTab + " AS t2 ORDER BY id",
			[]string{"id"}, "id", []string{"1", "2", "3", "4"}},
		{"the reserved column BY NAME",
			"SELECT __win_0 FROM " + rsOldTab + " ORDER BY id",
			[]string{"__win_0"}, "__win_0", []string{"100", "200", "300", "400"}},
		{"the reserved column under a user alias",
			"SELECT __winkey_1 AS k FROM " + rsOldTab + " ORDER BY id",
			[]string{"k"}, "k", []string{"10", "20", "30", "40"}},
		{
			// The one the whole mechanism is for: the stored column and the
			// planner's slot of the SAME NAME in one query. The stored column
			// keeps its values and the window keeps its own.
			name:  "the reserved column BESIDE a window that mints its name",
			sql:   "SELECT id, __win_0, SUM(id) OVER () AS w FROM " + rsOldTab + " ORDER BY id",
			cols:  []string{"id", "__win_0", "w"},
			check: "__win_0", want: []string{"100", "200", "300", "400"},
		},
		{"and the window's own value in that query",
			"SELECT id, __win_0, SUM(id) OVER () AS w FROM " + rsOldTab + " ORDER BY id",
			[]string{"id", "__win_0", "w"}, "w", []string{"10", "10", "10", "10"}},
		{"a stored window-KEY name beside a window over an expression",
			"SELECT id, __winkey_1, SUM(id * 2) OVER () AS w FROM " + rsOldTab + " ORDER BY id",
			[]string{"id", "__winkey_1", "w"}, "__winkey_1", []string{"10", "20", "30", "40"}},
		{"an UNALIASED window beside the stored column it would have collided with",
			"SELECT id, __win_0, SUM(id) OVER () FROM " + rsOldTab + " ORDER BY id",
			[]string{"id", "__win_0", "sum"}, "sum", []string{"10", "10", "10", "10"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				cols, rows, err := arm.run(tc.sql)
				if err != nil {
					t.Fatalf("%s arm REFUSED a read of a stored column: %v\n  SQL: %s\n"+
						"A table an older binary wrote has to stay readable; the reservation "+
						"belongs at the DDL and ingest doors, not here", arm.name, err, tc.sql)
				}
				if strings.Join(cols, ",") != strings.Join(tc.cols, ",") {
					t.Errorf("%s arm answered columns %v, want %v\n  SQL: %s",
						arm.name, cols, tc.cols, tc.sql)
				}
				if len(rows) != len(tc.want) {
					t.Fatalf("%s arm returned %d rows, want %d\n  SQL: %s",
						arm.name, len(rows), len(tc.want), tc.sql)
				}
				for i, r := range rows {
					if got := fmt.Sprintf("%v", r[tc.check]); got != tc.want[i] {
						t.Errorf("%s arm row %d: %s = %q, want %q\n  SQL: %s",
							arm.name, i, tc.check, got, tc.want[i], tc.sql)
					}
				}
			}
		})
	}
}

const rsOldTab = "oldtab"

func rsOldSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "__winkey_1", Type: parquet.TypeInt64, Nullable: true},
		{Name: "__win_0", Type: parquet.TypeInt64, Nullable: true},
		{Name: "plain", Type: parquet.TypeString, Nullable: true},
	}}
}

func rsOldRows() []map[string]any {
	out := make([]map[string]any, 4)
	for i := range out {
		out[i] = map[string]any{
			"id": int64(i + 1), "__winkey_1": int64((i + 1) * 10),
			"__win_0": int64((i + 1) * 100), "plain": fmt.Sprintf("p%d", i+1),
		}
	}
	return out
}

// rsWriteOldTable writes the table straight through the CATALOG — the way a
// binary that predates the reservation did, and the only way to produce one
// now that the doors refuse the schema.
func rsWriteOldTable(t *testing.T, ctx context.Context, infras ...tmdInfraT) {
	t.Helper()
	schema, rows := rsOldSchema(), rsOldRows()
	for _, infra := range infras {
		if err := infra.cat.CreateTable(ctx, rsOldTab, schema, nil); err != nil {
			t.Fatalf("create %s: %v", rsOldTab, err)
		}
		var buf bytes.Buffer
		pw, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
		if err != nil {
			t.Fatalf("parquet writer: %v", err)
		}
		if err := pw.WriteRows(rows); err != nil {
			t.Fatalf("write rows: %v", err)
		}
		if err := pw.Close(); err != nil {
			t.Fatalf("close writer: %v", err)
		}
		path := fmt.Sprintf("tables/%s/chunk_0000.parquet", rsOldTab)
		payload := buf.Bytes()
		if _, err := infra.store.Put(ctx, "test", path, bytes.NewReader(payload),
			int64(len(payload)), "application/octet-stream"); err != nil {
			t.Fatalf("put: %v", err)
		}
		if err := infra.cat.AddFiles(ctx, rsOldTab, map[string]string{}, "tables/"+rsOldTab+"/",
			[]catalog.FileEntry{{Path: path, SizeBytes: int64(len(payload)),
				NumRows: int64(len(rows)), CreatedAt: time.Now()}}); err != nil {
			t.Fatalf("add files: %v", err)
		}
	}
}

// rsIngestOldTable puts the same rows into the single-process arm's own
// catalog, through the ingester primitive rather than the wadjet API, for the
// reason rsWriteOldTable bypasses the doors.
func rsIngestOldTable(t *testing.T, ctx context.Context, db *wadjet.DB) {
	t.Helper()
	schema, rows := rsOldSchema(), rsOldRows()
	if err := db.Catalog().CreateTable(ctx, rsOldTab, schema, nil); err != nil {
		t.Fatalf("single create: %v", err)
	}
	ing := ingest.New(db.Catalog(), rsOldTab, schema, nil,
		ingest.Config{MaxBufferRows: len(rows) + 1, RowGroupSize: 128})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatalf("single ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("single flush: %v", err)
	}
}

// TestStoredReservedColumnWithSeveralWindows is Blocker 2's gate: renumbering
// one window's slot past a stored column must not land it on ANOTHER window's
// slot.
//
// With `__win_0` stored, the first repair took the first name not in the
// STORED set — `__win_1` — which the query's SECOND window already held. The
// second was not renamed, because `__win_1` is not stored, so both wrote it and
// the by-name projection handed w2 the value of w1. Silent, and on the
// single-process path only, so it was a two-path divergence as well as a wrong
// number. The free-name search now runs through plansql.SlotAllocator, which
// excludes the names in scope AND every slot it has itself issued.
func TestStoredReservedColumnWithSeveralWindows(t *testing.T) {
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
	coordB := tmdCoordinator(t, ctx, infraB)
	coordB.config.BroadcastBytesOverride = 1

	// wintab0 rides in the SHARED corpus (tmdTables) now, so both cluster arms
	// and the standalone arm already carry it — which is also what puts the
	// stored-reserved-name shape on every suite built on that corpus.

	arms := []struct {
		name string
		run  func(string) (*oracle.Result, error)
	}{
		{"single", func(q string) (*oracle.Result, error) { return tmdRunSingle(ctx, single, q) }},
		{"dag", func(q string) (*oracle.Result, error) { return tmdRunDAG(ctx, coord, q) }},
		{"dag-shuffled", func(q string) (*oracle.Result, error) { return tmdRunDAG(ctx, coordB, q) }},
	}

	// PostgreSQL 17 over the four rows: SUM(id) = 10, SUM(plain) = 10000,
	// MIN(id) = 1, SUM(__win_0) = 1000. The two window values are deliberately
	// DIFFERENT — with equal values a slot collision is invisible.
	for _, tc := range []struct {
		name  string
		sql   string
		check map[string]string
	}{
		{
			name: "two windows beside a stored slot name",
			sql: "SELECT __win_0, SUM(id) OVER () AS w1, SUM(plain) OVER () AS w2 FROM " +
				rsWinTab + " ORDER BY __win_0",
			check: map[string]string{"w1": "10", "w2": "10000"},
		},
		{
			name: "three windows beside a stored slot name",
			sql: "SELECT __win_0, SUM(id) OVER () AS w1, SUM(plain) OVER () AS w2, " +
				"MIN(id) OVER () AS w3 FROM " + rsWinTab + " ORDER BY __win_0",
			check: map[string]string{"w1": "10", "w2": "10000", "w3": "1"},
		},
		{
			// The stored slot column as a window ARGUMENT as well.
			name: "a window OVER the stored slot column, beside another window",
			sql: "SELECT __win_0, SUM(__win_0) OVER () AS w1, SUM(plain) OVER () AS w2 FROM " +
				rsWinTab + " ORDER BY __win_0",
			check: map[string]string{"w1": "1000", "w2": "10000"},
		},
		{
			// The control: the same two windows with NO stored collision, so
			// no renumbering happens at all.
			name: "control, two windows with no stored collision",
			sql: "SELECT id, SUM(id) OVER () AS w1, SUM(plain) OVER () AS w2 FROM " +
				rsWinTab + " ORDER BY id",
			check: map[string]string{"w1": "10", "w2": "10000"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(tc.sql)
				if err != nil {
					t.Fatalf("%s arm refused %q: %v", arm.name, tc.sql, err)
				}
				if len(res.Rows) != 4 {
					t.Fatalf("%s arm returned %d rows, want 4", arm.name, len(res.Rows))
				}
				for col, want := range tc.check {
					for i, r := range res.Rows {
						if got := fmt.Sprintf("%v", r[col]); got != want {
							t.Errorf("%s arm row %d: %s = %q, want %q — two windows sharing a "+
								"slot hand the second one the first's value\n  SQL: %s",
								arm.name, i, col, got, want, tc.sql)
						}
					}
				}
			}
		})
	}
}

const rsWinTab = "wintab0"

func rsWinSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "__win_0", Type: parquet.TypeInt64, Nullable: true},
		{Name: "plain", Type: parquet.TypeInt64, Nullable: true},
	}}
}

func rsWinRows() []map[string]any {
	out := make([]map[string]any, 4)
	for i := range out {
		out[i] = map[string]any{
			"id": int64(i + 1), "__win_0": int64((i + 1) * 100), "plain": int64((i + 1) * 1000),
		}
	}
	return out
}

// TestSiblingWindowsAcrossAJoinKeepTheirOwnSlots is the scope boundary of the
// slot renamer, and it is the fixture the design's impossibility claim needs
// (correctness protocol, method 10): the claim is "a rename map is scoped to
// the block that minted the slot", so the corpus carries the shapes that
// ATTEMPT to cross that scope.
//
// Two sibling subqueries each mint `__win_0`. With a stored `__win_0` column in
// scope the renamer moves both, and a single global map keyed by the old name
// has room for ONE mapping: it rewrote the other block's projection to a slot
// its own window never wrote. The single-process path failed with `column
// "__win_2" does not exist in the input schema` and the DAG handed both outputs
// one window's value.
//
// wintab0 is in the shared corpus (tmdTables), so these run against a catalog
// that really stores the reserved name — without which the renamer's `hot`
// guard exits early and none of this code is reached at all.
func TestSiblingWindowsAcrossAJoinKeepTheirOwnSlots(t *testing.T) {
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
	coordB := tmdCoordinator(t, ctx, infraB)
	coordB.config.BroadcastBytesOverride = 1

	arms := []struct {
		name string
		run  func(string) (*oracle.Result, error)
	}{
		{"single", func(q string) (*oracle.Result, error) { return tmdRunSingle(ctx, single, q) }},
		{"dag", func(q string) (*oracle.Result, error) { return tmdRunDAG(ctx, coord, q) }},
		{"dag-shuffled", func(q string) (*oracle.Result, error) { return tmdRunDAG(ctx, coordB, q) }},
	}

	// wintab0: SUM(plain) = 10000, SUM(id) = 10. Deliberately DIFFERENT, since
	// two equal window values make a slot collision invisible.
	const pBlock = "(SELECT id, SUM(plain) OVER () AS w FROM " + rsWinTab + ") p"
	const qBlock = "(SELECT id, SUM(id) OVER () AS w FROM " + rsWinTab + ") q"

	for _, tc := range []struct {
		name  string
		sql   string
		check map[string]string
		rows  int
	}{
		{
			name: "two siblings under an INNER join",
			sql: "SELECT p.w AS pw, q.w AS qw, p.id FROM " + pBlock + " JOIN " + qBlock +
				" ON p.id = q.id ORDER BY p.id",
			check: map[string]string{"pw": "10000", "qw": "10"}, rows: 4,
		},
		{
			name: "two siblings under a LEFT join",
			sql: "SELECT p.w AS pw, q.w AS qw, p.id FROM " + pBlock + " LEFT JOIN " + qBlock +
				" ON p.id = q.id ORDER BY p.id",
			check: map[string]string{"pw": "10000", "qw": "10"}, rows: 4,
		},
		{
			name: "two siblings under a SEMI join",
			sql: "SELECT p.id, p.w FROM " + pBlock + " WHERE p.id IN " +
				"(SELECT id FROM (SELECT id, SUM(id) OVER () AS w FROM " + rsWinTab + ") q) " +
				"ORDER BY p.id",
			check: map[string]string{"w": "10000"}, rows: 4,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(tc.sql)
				if err != nil {
					t.Fatalf("%s arm refused %q: %v\nA sibling's rename reached across the "+
						"join and left a projection reading a slot nothing wrote", arm.name,
						tc.sql, err)
				}
				if len(res.Rows) != tc.rows {
					t.Fatalf("%s arm returned %d rows, want %d\n  SQL: %s",
						arm.name, len(res.Rows), tc.rows, tc.sql)
				}
				for col, want := range tc.check {
					for i, r := range res.Rows {
						if got := fmt.Sprintf("%v", r[col]); got != want {
							t.Errorf("%s arm row %d: %s = %q, want %q — two sibling blocks "+
								"sharing a rename hand one of them the other's window\n  SQL: %s",
								arm.name, i, col, got, want, tc.sql)
						}
					}
				}
			}
		})
	}
}

// TestSiblingWindowsInUnionArmsKeepTheirOwnSlots: a set operation is the other
// multi-child boundary, and its arms mint slots independently too.
func TestSiblingWindowsInUnionArmsKeepTheirOwnSlots(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)

	sql := "SELECT w FROM (SELECT SUM(plain) OVER () AS w FROM " + rsWinTab + ") p " +
		"UNION ALL SELECT w FROM (SELECT SUM(id) OVER () AS w FROM " + rsWinTab + ") q"
	for _, arm := range []struct {
		name string
		run  func(string) (*oracle.Result, error)
	}{
		{"single", func(x string) (*oracle.Result, error) { return tmdRunSingle(ctx, single, x) }},
		{"dag", func(x string) (*oracle.Result, error) { return tmdRunDAG(ctx, coord, x) }},
	} {
		res, err := arm.run(sql)
		if err != nil {
			t.Fatalf("%s arm refused %q: %v", arm.name, sql, err)
		}
		if len(res.Rows) != 8 {
			t.Fatalf("%s arm returned %d rows, want 8", arm.name, len(res.Rows))
		}
		var tens, tenThousands int
		for _, r := range res.Rows {
			switch fmt.Sprintf("%v", r["w"]) {
			case "10":
				tens++
			case "10000":
				tenThousands++
			}
		}
		if tens != 4 || tenThousands != 4 {
			t.Errorf("%s arm answered %d tens and %d ten-thousands, want 4 and 4 — the arms' "+
				"slots collided\n  SQL: %s", arm.name, tens, tenThousands, sql)
		}
	}
}
