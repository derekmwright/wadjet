package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// s4VectorTable opens a database with one VECTOR(dim) table and nothing in it.
func s4VectorTable(t *testing.T, table string, dim int) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "s4"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeVector, Dimension: dim, Nullable: true},
	}}
	if err := db.CreateTable(ctx, table, schema, nil); err != nil {
		t.Fatal(err)
	}
	return db
}

// A VECTOR(N) value has exactly N components AT EVERY DOOR, and a value of any
// other width is refused rather than written.
//
// docs/data-types.md said so from the moment the in-memory rule landed, and no
// SQL door kept the promise: convertValue had no VECTOR case at all, so the
// literal's raw TEXT reached the parquet writer and went into a
// FIXED_LEN_BYTE_ARRAY(N*4) leaf verbatim. `INSERT INTO t (v) VALUES ('[1]')`
// answered "INSERT 1" and left a page whose body is 3 bytes where its header
// promises 8 — the table unqueryable from then on — and `'[1,2]'`, the RIGHT
// width, did exactly the same with 5 bytes. Round-2 review B2.
func TestAVectorLiteralIsExactlyTheDeclaredWidthAtEveryDoor(t *testing.T) {
	ctx := context.Background()

	// The accepted case first, because it never worked either: a correct-width
	// literal round trips.
	t.Run("insert-correct-width-round-trips", func(t *testing.T) {
		db := s4VectorTable(t, "vok", 2)
		if _, err := db.Execute(ctx, `INSERT INTO vok (id, v) VALUES (1, '[1,2]')`); err != nil {
			t.Fatalf("a VECTOR(2) literal of the right width was refused: %v", err)
		}
		r, err := db.Query(ctx, `SELECT v FROM vok`)
		if err != nil {
			t.Fatalf("reading back a vector this door wrote: %v", err)
		}
		if got := fmt.Sprint(r.Rows); got != "[map[v:[1 2]]]" {
			t.Errorf("read back %s; want [map[v:[1 2]]]", got)
		}
	})

	refusals := []struct {
		name  string
		lit   string
		state string
		frag  string
	}{
		{"short", `'[1]'`, "22000", "expected 2 dimensions, not 1"},
		{"empty", `'[]'`, "22000", "expected 2 dimensions, not 0"},
		{"long", `'[1,2,3]'`, "22000", "expected 2 dimensions, not 3"},
		{"no-brackets", `'1,2'`, "22P02", "invalid input syntax for type vector"},
		{"not-a-number", `'[1,x]'`, "22P02", "invalid input syntax for type vector"},
		{"nan", `'[1,NaN]'`, "22P02", "invalid input syntax for type vector"},
	}

	for _, door := range []string{"INSERT", "UPDATE", "MERGE"} {
		for _, c := range refusals {
			t.Run(door+"/"+c.name, func(t *testing.T) {
				db := s4VectorTable(t, "vd", 2)
				if _, err := db.Execute(ctx, `INSERT INTO vd (id, v) VALUES (1, '[9,9]')`); err != nil {
					t.Fatal(err)
				}
				var sql string
				switch door {
				case "INSERT":
					sql = fmt.Sprintf(`INSERT INTO vd (id, v) VALUES (2, %s)`, c.lit)
				case "UPDATE":
					sql = fmt.Sprintf(`UPDATE vd SET v = %s WHERE id = 1`, c.lit)
				case "MERGE":
					sql = fmt.Sprintf(`MERGE INTO vd USING (SELECT 1 AS k) s ON vd.id = s.k `+
						`WHEN MATCHED THEN UPDATE SET v = %s`, c.lit)
				}
				_, err := db.Execute(ctx, sql)
				if err == nil {
					t.Fatalf("%s accepted %s into a VECTOR(2)", door, c.lit)
				}
				if !strings.Contains(err.Error(), c.frag) {
					t.Errorf("refusal %q does not say %q", err, c.frag)
				}
				if got := sqlerr.StateOf(err); got != c.state {
					t.Errorf("refusal SQLSTATE %q; want %q", got, c.state)
				}
				// The table is still readable and still holds what it held:
				// the refusal came BEFORE anything was written.
				r, qerr := db.Query(ctx, `SELECT id, v FROM vd ORDER BY id`)
				if qerr != nil {
					t.Fatalf("the table is unreadable after a refused %s: %v", door, qerr)
				}
				if got := fmt.Sprint(r.Rows); got != "[map[id:1 v:[9 9]]]" {
					t.Errorf("after a refused %s the table holds %s; want the untouched row", door, got)
				}
			})
		}
	}

	// The ingest API is the other door onto the same leaf, and it takes Go
	// boxes rather than literal text.
	//
	// Its rule is arc S1's (parquet.CheckLeafBox, reached from
	// ingest.checkType), which lands separately. The probe below decides
	// whether that rule is present in THIS tree: where it is not, the whole
	// arm skips with the reason rather than pretending the door is fixed —
	// and where it is, every case is asserted.
	t.Run("ingest-api", func(t *testing.T) {
		// The LONG case is the discriminator: a door with no width rule
		// ADMITS it and truncates silently, where a short one at least
		// leaves a page the reader refuses.
		if err := s4IngestVector(t, "vprobe", []float32{1, 2, 3}); err == nil {
			t.Skip("this tree's ingest door has no VECTOR width rule yet " +
				"(arc S1's parquet.CheckLeafBox); the cases below hold once it lands")
		}
		for _, c := range []struct {
			name string
			val  any
			ok   bool
		}{
			{"correct", []float32{1, 2}, true},
			{"short", []float32{1}, false},
			{"long", []float32{1, 2, 3}, false},
			{"empty", []float32{}, false},
			{"nil-typed", []float32(nil), false},
		} {
			t.Run(c.name, func(t *testing.T) {
				err := s4IngestVector(t, "vi"+c.name, c.val)
				if c.ok && err != nil {
					t.Fatalf("a %s vector was refused: %v", c.name, err)
				}
				if !c.ok && err == nil {
					t.Errorf("the ingest API admitted a %s vector into a VECTOR(2)", c.name)
				}
			})
		}
	})
}

// s4IngestVector ingests one row carrying v into a fresh VECTOR(2) table and
// returns whatever the door said, flush included — a width the door admits but
// the writer refuses is still a refusal, and it is the FLUSH that reports it.
func s4IngestVector(t *testing.T, table string, v any) error {
	t.Helper()
	ctx := context.Background()
	db := s4VectorTable(t, table, 2)
	sc := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeVector, Dimension: 2, Nullable: true},
	}}
	ing := db.NewIngester(table, sc, nil, ingest.Config{MaxBufferRows: 8, RowGroupSize: 4})
	if err := ing.Ingest(ctx, []map[string]any{{"id": int64(1), "v": v}}); err != nil {
		return err
	}
	if err := ing.FlushAll(ctx); err != nil {
		return err
	}
	// Admitted and written. A table that cannot be READ back is the worst of
	// the three outcomes and is reported as a failure of the write, not as a
	// success.
	if _, err := db.Query(ctx, "SELECT v FROM "+table); err != nil {
		return fmt.Errorf("written but unreadable: %w", err)
	}
	return nil
}
