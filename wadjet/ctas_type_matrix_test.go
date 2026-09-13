package wadjet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A query's result becomes a table without changing a single value, for every
// type this engine has (#1024).
//
// This is the compaction gate's rule applied to a second writer: `CREATE TABLE
// … AS SELECT` reads a table through the QUERY path — which boxes every cell
// through Vector.GetValue — and writes it back through the INGEST path, which
// validates every box and hands it to the parquet writer. Every read→write
// asymmetry in that round trip is silent data loss, and no shape corpus finds
// one, because a shape corpus compares a query's answer with a query's answer
// and both sides are the read half.
//
// Both fixture tables are copied on purpose: `typemx` is flat and is read by
// the native columnar decoder, `typemx_nested` carries the four container
// types and routes to the row reader (readBatchDirect decides on the WHOLE
// table schema, not on the projected columns), so the pair is the two read
// paths the compaction gate also names.
func TestACreatedTableHoldsExactlyWhatTheQueryAnswered(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db := ctasTypeMatrixDB(t, store)

	for _, src := range []string{typematrix.Table, typematrix.Nested} {
		t.Run(src, func(t *testing.T) {
			dst := src + "_ctas"
			res, err := db.Query(ctx, fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM %s", dst, src))
			if err != nil {
				t.Fatalf("CTAS over %s: %v", src, err)
			}
			// PostgreSQL tags a CTAS that ran its query `SELECT <n>`.
			if got, want := res.Rows[0]["result"], fmt.Sprintf("SELECT %d", typematrix.Rows); got != want {
				t.Errorf("command tag %v, want %q", got, want)
			}

			// (1) The DECLARATION. The new table's schema is the query's
			// declared output: same names, same types, same DECIMAL (p, s),
			// same VECTOR dimension, same container shape — and every column
			// nullable, because PostgreSQL never infers NOT NULL for a CTAS.
			srcMeta, err := db.catalog.GetTable(ctx, src)
			if err != nil {
				t.Fatal(err)
			}
			dstMeta, err := db.catalog.GetTable(ctx, dst)
			if err != nil {
				t.Fatal(err)
			}
			ctasCompareSchemas(t, srcMeta.Schema.Columns, dstMeta.Schema.Columns)

			// (2) The VALUES, cell for cell, in one order.
			ctasCompareTables(t, ctx, db, src, dst)

			// (3) A third party reads the file this statement wrote and sees
			// what it sees in the file the fixture wrote. Both sides go
			// through one renderer, so a difference is a difference in the
			// DATA. (The same check the compaction gate makes of its own
			// writer — gatePyArrowCrossCheck.)
			ctasPyArrowCrossCheck(t, ctx, store,
				ctasTableObjects(t, ctx, store, src),
				ctasTableObjects(t, ctx, store, dst))

			// (4) And the APPEND half of the same seam: the identical rows
			// again, through INSERT INTO … SELECT, doubling every value.
			ins, err := db.Query(ctx, fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", dst, src))
			if err != nil {
				t.Fatalf("INSERT INTO %s SELECT: %v", dst, err)
			}
			if got, want := ins.Rows[0]["result"], fmt.Sprintf("INSERT 0 %d", typematrix.Rows); got != want {
				t.Errorf("command tag %v, want %q", got, want)
			}
			n := ctasScalar(t, ctx, db, fmt.Sprintf("SELECT COUNT(*) AS c FROM %s", dst))
			if n != int64(2*typematrix.Rows) {
				t.Errorf("%s holds %v rows after the append, want %d", dst, n, 2*typematrix.Rows)
			}
		})
	}
}

// TestACreatedTableHoldsEveryColumnOneAtATime narrows the failure. The
// whole-star copy above proves the round trip; this one names the TYPE when it
// breaks, which a 23-column diff cannot.
func TestACreatedTableHoldsEveryColumnOneAtATime(t *testing.T) {
	ctx := context.Background()
	db := ctasTypeMatrixDB(t, objstore.NewMemStore())

	for _, col := range typematrix.Columns() {
		t.Run(col.Name, func(t *testing.T) {
			src, dst := col.TableOf(), "one_"+col.Name
			if _, err := db.Query(ctx, fmt.Sprintf(
				"CREATE TABLE %s AS SELECT id, %s FROM %s", dst, col.Name, src)); err != nil {
				t.Fatalf("CTAS: %v", err)
			}
			meta, err := db.catalog.GetTable(ctx, dst)
			if err != nil {
				t.Fatal(err)
			}
			if len(meta.Schema.Columns) != 2 || meta.Schema.Columns[1].Name != col.Name {
				t.Fatalf("declared %v, want [id %s]", meta.Schema.ColumnNames(), col.Name)
			}
			if got := meta.Schema.Columns[1].Type; got != col.Type {
				t.Errorf("%s declared %s, want %s", col.Name, got, col.Type)
			}
			ctasCompareQueries(t, ctx, db,
				fmt.Sprintf("SELECT id, %s FROM %s ORDER BY id", col.Name, src),
				fmt.Sprintf("SELECT id, %s FROM %s ORDER BY id", col.Name, dst))
		})
	}
}

// ctasTypeMatrixDB is tmOpen over a store the caller keeps, so the gate can
// read the parquet objects the statement wrote.
func ctasTypeMatrixDB(t *testing.T, store objstore.Store) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	load := func(name string, schema parquet.Schema, rows []map[string]any) {
		t.Helper()
		if err := db.CreateTable(ctx, name, schema, nil); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		ing := db.NewIngester(name, schema, nil, ingest.Config{
			MaxBufferRows: typematrix.Rows + 1, RowGroupSize: typematrix.RowGroup,
		})
		if err := ing.Ingest(ctx, rows); err != nil {
			t.Fatalf("ingest %s: %v", name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", name, err)
		}
	}
	load(typematrix.Table, typematrix.Schema(), typematrix.Data(typematrix.Rows))
	load(typematrix.Nested, typematrix.NestedSchema(), typematrix.NestedData(typematrix.Rows))
	return db
}

func ctasCompareSchemas(t *testing.T, want, got []parquet.Column) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("declared %d columns, want %d", len(got), len(want))
	}
	for i := range want {
		w, g := want[i], got[i]
		if g.Name != w.Name {
			t.Errorf("column %d named %q, want %q", i, g.Name, w.Name)
		}
		if g.Type != w.Type {
			t.Errorf("column %q declared %s, want %s", w.Name, g.Type, w.Type)
		}
		if w.Type == parquet.TypeDecimal && (g.Precision != w.Precision || g.Scale != w.Scale) {
			t.Errorf("column %q DECIMAL(%d,%d), want (%d,%d)", w.Name, g.Precision, g.Scale, w.Precision, w.Scale)
		}
		if w.Type == parquet.TypeVector && g.Dimension != w.Dimension {
			t.Errorf("column %q VECTOR(%d), want VECTOR(%d)", w.Name, g.Dimension, w.Dimension)
		}
		if !g.Nullable {
			t.Errorf("column %q is NOT NULL; a CTAS never infers one (PostgreSQL 17.11: is_nullable = YES)", w.Name)
		}
	}
}

func ctasCompareTables(t *testing.T, ctx context.Context, db *DB, src, dst string) {
	t.Helper()
	ctasCompareQueries(t, ctx, db,
		"SELECT * FROM "+src+" ORDER BY id",
		"SELECT * FROM "+dst+" ORDER BY id")
}

// ctasCompareQueries compares two result sets POSITIONALLY, through the JSON
// rendering both sides share, so a difference is a difference in the value and
// not in how one side was boxed.
func ctasCompareQueries(t *testing.T, ctx context.Context, db *DB, wantSQL, gotSQL string) {
	t.Helper()
	want, err := db.Query(ctx, wantSQL)
	if err != nil {
		t.Fatalf("%s: %v", wantSQL, err)
	}
	got, err := db.Query(ctx, gotSQL)
	if err != nil {
		t.Fatalf("%s: %v", gotSQL, err)
	}
	if len(got.Columns) != len(want.Columns) {
		t.Fatalf("%d columns, want %d (%v vs %v)", len(got.Columns), len(want.Columns), got.Columns, want.Columns)
	}
	if len(got.Rows) != len(want.Rows) {
		t.Fatalf("%d rows, want %d", len(got.Rows), len(want.Rows))
	}
	for i := range want.Rows {
		g, _ := json.Marshal(got.Cells(i))
		w, _ := json.Marshal(want.Cells(i))
		if !bytes.Equal(g, w) {
			t.Fatalf("row %d differs:\n  written %s\n  queried %s", i, g, w)
		}
	}
}

func ctasScalar(t *testing.T, ctx context.Context, db *DB, sql string) int64 {
	t.Helper()
	res, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	switch v := res.Cells(0)[0].(type) {
	case int64:
		return v
	case int32:
		return int64(v)
	default:
		t.Fatalf("%s answered %T", sql, v)
		return 0
	}
}

// ctasTableObjects returns the parquet objects a table's files hold, in
// manifest order.
func ctasTableObjects(t *testing.T, ctx context.Context, store objstore.Store, table string) [][]byte {
	t.Helper()
	infos, err := store.List(ctx, "test", objstore.ListOptions{Prefix: "tables/" + table + "/"})
	if err != nil {
		t.Fatalf("listing %s: %v", table, err)
	}
	var out [][]byte
	for _, info := range infos {
		rc, _, err := store.Get(ctx, "test", info.Key)
		if err != nil {
			t.Fatalf("reading %s: %v", info.Key, err)
		}
		data, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("reading %s: %v", info.Key, err)
		}
		out = append(out, data)
	}
	if len(out) == 0 {
		t.Fatalf("table %s has no parquet objects", table)
	}
	return out
}

// ctasPyArrowCrossCheck is gatePyArrowCrossCheck's sibling for this writer:
// PyArrow reads both sides and the rows must be identical. It skips where
// python3 with pyarrow is not importable, which is the same concession the
// compaction gate makes.
func ctasPyArrowCrossCheck(t *testing.T, ctx context.Context, store objstore.Store, before, after [][]byte) {
	t.Helper()
	_ = ctx
	_ = store
	if exec.Command("python3", "-c", "import pyarrow").Run() != nil {
		t.Log("python3 with pyarrow is not importable here — skipping the PyArrow cross-check")
		return
	}
	got := ctasPyArrowDump(t, after)
	want := ctasPyArrowDump(t, before)
	if len(got) != len(want) {
		t.Fatalf("pyarrow read %d rows from the created table, want %d", len(got), len(want))
	}
	for i := range want {
		g, _ := json.Marshal(got[i])
		w, _ := json.Marshal(want[i])
		if !bytes.Equal(g, w) {
			t.Fatalf("pyarrow row %d differs:\n  created %s\n  source  %s", i, g, w)
		}
	}
}

func ctasPyArrowDump(t *testing.T, files [][]byte) []map[string]any {
	t.Helper()
	dir := t.TempDir()
	args := []string{"-c", ctasPyArrowScript}
	for i, data := range files {
		p := filepath.Join(dir, fmt.Sprintf("f%03d.parquet", i))
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatal(err)
		}
		args = append(args, p)
	}
	out, err := exec.Command("python3", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("pyarrow read failed: %v\n%s", err, out)
	}
	var rows []map[string]any
	if err := json.Unmarshal(out, &rows); err != nil {
		t.Fatalf("decoding pyarrow output: %v\n%s", err, out)
	}
	return rows
}

// ctasPyArrowScript renders every row of every file it is given as JSON,
// sorted by id, with binary as hex and DECIMAL as its decimal string.
const ctasPyArrowScript = `
import datetime, json, sys, pyarrow as pa, pyarrow.parquet as pq

def conv(ty, v):
    if v is None:
        return None
    if pa.types.is_map(ty):
        return {str(k): conv(ty.item_type, x) for k, x in v}
    if pa.types.is_list(ty) or pa.types.is_large_list(ty):
        return [conv(ty.value_type, e) for e in v]
    if pa.types.is_struct(ty):
        return {ty.field(i).name: conv(ty.field(i).type, v[ty.field(i).name])
                for i in range(ty.num_fields)}
    if isinstance(v, (bytes, bytearray)):
        return v.hex()
    if pa.types.is_decimal(ty):
        return str(v)
    if isinstance(v, (datetime.date, datetime.datetime, datetime.timedelta)):
        return str(v)
    return v

rows = []
for path in sys.argv[1:]:
    tbl = pq.read_table(path)
    cols = {f.name: (f.type, tbl.column(f.name).to_pylist()) for f in tbl.schema}
    for i in range(tbl.num_rows):
        rows.append({n: conv(ty, vals[i]) for n, (ty, vals) in cols.items()})
rows.sort(key=lambda r: r.get("id"))
json.dump(rows, sys.stdout)
`
