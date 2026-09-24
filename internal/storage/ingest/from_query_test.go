// SPDX-License-Identifier: MIT

package ingest

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func fqCol(name string, t parquet.TypeID) parquet.Column {
	return parquet.Column{Name: name, Type: t, Nullable: false}
}

// TableSchemaForQuery is where a CTAS's declaration is decided, and every rule
// in it was measured on PostgreSQL 17.11 first.
func TestTableSchemaForQuery(t *testing.T) {
	declared := []parquet.Column{
		fqCol("id", parquet.TypeInt64),
		{Name: "d", Type: parquet.TypeDecimal, Precision: 12, Scale: 3},
		{Name: "v", Type: parquet.TypeVector, Dimension: 4},
	}

	t.Run("TheDeclarationTravels", func(t *testing.T) {
		got, err := TableSchemaForQuery(declared, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Columns) != 3 {
			t.Fatalf("%d columns", len(got.Columns))
		}
		if c := got.Columns[1]; c.Precision != 12 || c.Scale != 3 {
			t.Errorf("DECIMAL(%d,%d), want (12,3)", c.Precision, c.Scale)
		}
		if c := got.Columns[2]; c.Dimension != 4 {
			t.Errorf("VECTOR(%d), want VECTOR(4)", c.Dimension)
		}
		for _, c := range got.Columns {
			if !c.Nullable {
				t.Errorf("column %q is NOT NULL; PostgreSQL never infers one for a CTAS "+
					"(is_nullable = YES for every column of every measured shape)", c.Name)
			}
		}
	})

	t.Run("RenamesArePositionalAndMayBeShort", func(t *testing.T) {
		// `CREATE TABLE t (a) AS SELECT id, n FROM src` is accepted on
		// PostgreSQL 17.11 and gives (a, n) — measured.
		got, err := TableSchemaForQuery(declared, []string{"a"})
		if err != nil {
			t.Fatal(err)
		}
		if names := got.ColumnNames(); names[0] != "a" || names[1] != "d" || names[2] != "v" {
			t.Errorf("renamed to %v, want [a d v]", names)
		}
	})

	t.Run("TooManyRenames", func(t *testing.T) {
		_, err := TableSchemaForQuery(declared, []string{"a", "b", "c", "d"})
		if err == nil {
			t.Fatal("a rename list longer than the query's output was accepted")
		}
		if got := sqlerr.StateOf(err); got != "42601" {
			t.Errorf("SQLSTATE %q, want 42601", got)
		}
		if !strings.Contains(err.Error(), "too many column names") {
			t.Errorf("message: %v", err)
		}
	})

	t.Run("DuplicateNames", func(t *testing.T) {
		// PostgreSQL: `column "?column?" specified more than once`, 42701.
		dup := []parquet.Column{fqCol("?column?", parquet.TypeInt64), fqCol("?column?", parquet.TypeInt64)}
		_, err := TableSchemaForQuery(dup, nil)
		if err == nil {
			t.Fatal("two columns of one name were accepted")
		}
		if got := sqlerr.StateOf(err); got != "42701" {
			t.Errorf("SQLSTATE %q, want 42701", got)
		}
	})

	t.Run("DuplicateNamesTheRenameListFIXES", func(t *testing.T) {
		// The boundary: PostgreSQL accepts the same query under an explicit
		// column list, because the rename happens first (measured — a 14-name
		// list over a star self-join).
		dup := []parquet.Column{fqCol("?column?", parquet.TypeInt64), fqCol("?column?", parquet.TypeInt64)}
		if _, err := TableSchemaForQuery(dup, []string{"a", "b"}); err != nil {
			t.Errorf("the rename list did not rescue the duplicate: %v", err)
		}
	})

	t.Run("AnEmptyDeclarationRefuses", func(t *testing.T) {
		if _, err := TableSchemaForQuery(nil, nil); err == nil {
			t.Fatal("a query with no declared output was accepted")
		}
	})

	t.Run("AnUnwritableTypeRefusesAtCreate", func(t *testing.T) {
		// ADR-0018 §14: the writer refuses what it cannot write, and a CTAS
		// asks it at CREATE rather than at the first flush.
		bad := []parquet.Column{{Name: "m", Type: parquet.TypeMap}}
		if _, err := TableSchemaForQuery(bad, nil); err == nil {
			t.Fatal("a MAP with no ElementType was accepted as a table declaration")
		}
	})
}

// The assignment rule for INSERT INTO … SELECT: which query column may feed
// which target column. Each pair was measured on PostgreSQL 17.11.
func TestAssignableToColumn(t *testing.T) {
	dec123 := parquet.Column{Name: "d", Type: parquet.TypeDecimal, Precision: 12, Scale: 3}
	dec184 := parquet.Column{Name: "d", Type: parquet.TypeDecimal, Precision: 18, Scale: 4}
	row1 := parquet.Column{Name: "r", Type: parquet.TypeRow, Fields: []parquet.Column{
		fqCol("a", parquet.TypeInt64), fqCol("b", parquet.TypeString)}}
	row2 := parquet.Column{Name: "r", Type: parquet.TypeRow, Fields: []parquet.Column{
		fqCol("x", parquet.TypeInt64), fqCol("b", parquet.TypeString)}}

	cases := []struct {
		name     string
		from, to parquet.Column
		ok       bool
	}{
		{"SameType", fqCol("a", parquet.TypeInt64), fqCol("b", parquet.TypeInt64), true},
		{"Int32IntoInt64", fqCol("a", parquet.TypeInt32), fqCol("b", parquet.TypeInt64), true},
		{"Int64IntoInt32", fqCol("a", parquet.TypeInt64), fqCol("b", parquet.TypeInt32), true},
		{"PortIntoInt64", fqCol("a", parquet.TypePort), fqCol("b", parquet.TypeInt64), true},
		{"IntIntoFloat", fqCol("a", parquet.TypeInt64), fqCol("b", parquet.TypeFloat64), true},
		{"Float32IntoFloat64", fqCol("a", parquet.TypeFloat32), fqCol("b", parquet.TypeFloat64), true},
		{"IntIntoDecimal", fqCol("a", parquet.TypeInt64), dec123, true},
		{"SameDecimal", dec123, dec123, true},
		{"SameRow", row1, row1, true},
		// The whole NUMERIC family assigns, in every direction, because the
		// statement door converts every cell through the engine's one
		// assignment converter before the writer sees it — the same converter
		// INSERT … VALUES uses (round-2 review B1/B2). PostgreSQL assigns all
		// of these too, and `wadjet.TestBothWriteDoorsStoreTheSameNumber` and
		// `TestADecimalSourceIsAssignedAtItsValue` compare the VALUE both
		// doors store, pair by pair, against its measured answer.
		{"DecimalScaleDiffers", dec123, dec184, true},
		{"DecimalIntoInt", dec123, fqCol("b", parquet.TypeInt64), true},
		{"DecimalIntoFloat", dec123, fqCol("b", parquet.TypeFloat64), true},
		{"FloatIntoInt", fqCol("a", parquet.TypeFloat64), fqCol("b", parquet.TypeInt64), true},
		{"FloatIntoDecimal", fqCol("a", parquet.TypeFloat64), dec123, true},
		{"IntIntoPort", fqCol("a", parquet.TypeInt64), fqCol("b", parquet.TypePort), true},

		// PostgreSQL's assignment casts into TEXT and across DATE/TIMESTAMP
		// (arc VL round 3). These two were pinned as ADR-0012's recorded
		// divergence (42804 here) and now agree with PostgreSQL.
		{"IntIntoString", fqCol("a", parquet.TypeInt64), fqCol("b", parquet.TypeString), true},
		{"IPv4IntoString", fqCol("a", parquet.TypeIPv4), fqCol("b", parquet.TypeString), true},
		{"BoolIntoString", fqCol("a", parquet.TypeBool), fqCol("b", parquet.TypeString), true},
		{"UUIDIntoString", fqCol("a", parquet.TypeUUID), fqCol("b", parquet.TypeString), true},
		{"DateIntoTimestamp", fqCol("a", parquet.TypeDate), fqCol("b", parquet.TypeTimestamp), true},
		{"TimestampIntoDate", fqCol("a", parquet.TypeTimestamp), fqCol("b", parquet.TypeDate), true},

		// The refusals: PostgreSQL has no assignment cast for these pairs.
		{"StringIntoInt", fqCol("a", parquet.TypeString), fqCol("b", parquet.TypeInt64), false},
		{"StringIntoDate", fqCol("a", parquet.TypeString), fqCol("b", parquet.TypeDate), false},
		{"StringIntoUUID", fqCol("a", parquet.TypeString), fqCol("b", parquet.TypeUUID), false},
		{"BoolIntoInt", fqCol("a", parquet.TypeBool), fqCol("b", parquet.TypeInt64), false},
		{"IntIntoBool", fqCol("a", parquet.TypeInt64), fqCol("b", parquet.TypeBool), false},
		{"IntIntoDate", fqCol("a", parquet.TypeInt32), fqCol("b", parquet.TypeDate), false},
		{"TimestampIntoInt", fqCol("a", parquet.TypeTimestamp), fqCol("b", parquet.TypeInt64), false},
		{"IntIntoIPv4", fqCol("a", parquet.TypeInt64), fqCol("b", parquet.TypeIPv4), false},
		// BYTES into TEXT is an assignment cast in PostgreSQL (its \x text);
		// this engine refuses it — the refusing direction, ADR-0012.
		{"BytesIntoString", fqCol("a", parquet.TypeBytes), fqCol("b", parquet.TypeString), false},
		{"RowFieldNameDiffers", row1, row2, false},
		{"VectorWidthDiffers",
			parquet.Column{Name: "v", Type: parquet.TypeVector, Dimension: 4},
			parquet.Column{Name: "v", Type: parquet.TypeVector, Dimension: 8}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := AssignableToColumn(tc.from, tc.to)
			if tc.ok && err != nil {
				t.Errorf("refused: %v", err)
			}
			if !tc.ok {
				if err == nil {
					t.Fatal("accepted")
				}
				if got := sqlerr.StateOf(err); got != "42804" {
					t.Errorf("SQLSTATE %q, want 42804 (PostgreSQL's class for this refusal)", got)
				}
			}
		})
	}
}

// The RECLAIM half of WriteQueryRows, driven directly, because it is the half
// no door-level fixture reaches deterministically: the objects must be
// UPLOADED and the COMMIT must then fail.
func TestARefusedCommitReclaimsWhatItUploaded(t *testing.T) {
	ctx := context.Background()

	t.Run("ACreateOntoATakenName", func(t *testing.T) {
		cat, store := fqCatalog(t)
		schema := parquet.Schema{Columns: []parquet.Column{{Name: "a", Type: parquet.TypeInt64, Nullable: true}}}
		if err := cat.CreateTable(ctx, "taken", schema, nil); err != nil {
			t.Fatal(err)
		}
		before := fqKeys(t, ctx, store)

		_, err := WriteQueryRows(ctx, cat, QueryWrite{
			Table: "taken", Schema: schema, Columns: []string{"a"}, Create: true,
		}, sliceRows([][]any{{int64(1)}, {int64(2)}}))
		if err == nil {
			t.Fatal("a create onto a taken name succeeded")
		}
		if got := sqlerr.StateOf(err); got != "42P07" {
			t.Errorf("SQLSTATE %q, want 42P07: %v", got, err)
		}
		fqAssertNoNewObjects(t, ctx, store, before)
	})

	t.Run("AnAppendAgainstTheWrongIncarnation", func(t *testing.T) {
		cat, store := fqCatalog(t)
		schema := parquet.Schema{Columns: []parquet.Column{{Name: "a", Type: parquet.TypeInt64, Nullable: true}}}
		if err := cat.CreateTable(ctx, "app", schema, nil); err != nil {
			t.Fatal(err)
		}
		before := fqKeys(t, ctx, store)

		_, err := WriteQueryRows(ctx, cat, QueryWrite{
			Table: "app", Schema: schema, Columns: []string{"a"},
			Incarnation: "not-the-one-this-table-has",
		}, sliceRows([][]any{{int64(1)}}))
		if err == nil {
			t.Fatal("an append against a foreign incarnation succeeded")
		}
		if !errors.Is(err, catalog.ErrTableIncarnationChanged) {
			t.Errorf("refused with %v, want ErrTableIncarnationChanged", err)
		}
		fqAssertNoNewObjects(t, ctx, store, before)
		// And the table is still empty: nothing was published.
		m, err := cat.GetManifest(ctx, "app")
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range m.Partitions {
			if len(p.Files) != 0 {
				t.Errorf("the manifest holds %d files after a refused append", len(p.Files))
			}
		}
	})
}

// A create publishes its files WITH the table: there is never an instant at
// which the name resolves to an empty table.
func TestACreateWithFilesPublishesThemTogether(t *testing.T) {
	ctx := context.Background()
	cat, _ := fqCatalog(t)
	schema := parquet.Schema{Columns: []parquet.Column{{Name: "a", Type: parquet.TypeInt64, Nullable: true}}}

	n, err := WriteQueryRows(ctx, cat, QueryWrite{
		Table: "made", Schema: schema, Columns: []string{"a"}, Create: true,
	}, sliceRows([][]any{{int64(1)}, {int64(2)}, {int64(3)}}))
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("wrote %d rows, want 3", n)
	}
	m, err := cat.GetManifest(ctx, "made")
	if err != nil {
		t.Fatal(err)
	}
	rows := int64(0)
	for _, p := range m.Partitions {
		for _, f := range p.Files {
			rows += f.NumRows
			if !f.EngineWritten {
				t.Errorf("file %q is not marked engine-written; the ownership marker is what the "+
					"reclaim layer reads (ADR-0020 layer 0)", f.Path)
			}
		}
	}
	if rows != 3 {
		t.Errorf("the table's first manifest names %d rows, want 3", rows)
	}
}

// The strongest atomicity cell: a statement that writes SEVERAL files and
// fails on a later one has published NONE of them, and the earlier ones are
// reclaimed.
//
// It lives here rather than at a door because the buffer bound is what makes
// the statement write more than one file, and only this seam takes a Config.
// Without it every door-level fixture writes exactly one file, and a
// per-flush manifest commit — the shape this rule replaces — passes.
func TestAPartlyWrittenStatementPublishesNoneOfIt(t *testing.T) {
	ctx := context.Background()
	mem := objstore.NewMemStore()
	failing := &fqFailAfter{Store: mem, after: 1}
	cat := catalog.NewWithStore(failing, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "a", Type: parquet.TypeInt64, Nullable: true},
		{Name: "p", Type: parquet.TypeString, Nullable: true},
	}}
	if err := cat.CreateTable(ctx, "multi", schema, []string{"p"}); err != nil {
		t.Fatal(err)
	}
	inc, err := cat.TableIncarnation(ctx, "multi")
	if err != nil {
		t.Fatal(err)
	}
	before := fqKeys(t, ctx, failing)

	// Two partitions, so the flush writes two files; the second Put fails.
	_, err = WriteQueryRows(ctx, cat, QueryWrite{
		Table: "multi", Schema: schema, Columns: []string{"a", "p"},
		PartitionKeys: []string{"p"}, Incarnation: inc,
	}, sliceRows([][]any{{int64(1), "x"}, {int64(2), "y"}}))
	if err == nil {
		t.Fatal("the statement succeeded over a store that refused its second file")
	}
	fqAssertNoNewObjects(t, ctx, failing, before)

	m, err := cat.GetManifest(ctx, "multi")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range m.Partitions {
		if len(p.Files) != 0 {
			t.Errorf("partition %q holds %d files; a statement that failed halfway "+
				"must have published none", p.Path, len(p.Files))
		}
	}
}

// fqFailAfter fails every data-object Put after the first n.
type fqFailAfter struct {
	objstore.Store
	writes atomic.Int64
	after  int64
}

func (s *fqFailAfter) Put(ctx context.Context, bucket, key string, r io.Reader, size int64, ct string) (string, error) {
	if strings.HasPrefix(key, "tables/") && s.writes.Add(1) > s.after {
		return "", errors.New("simulated object-store failure")
	}
	return s.Store.Put(ctx, bucket, key, r, size, ct)
}

func fqCatalog(t *testing.T) (*catalog.Catalog, objstore.Store) {
	t.Helper()
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	return cat, store
}

func fqKeys(t *testing.T, ctx context.Context, store objstore.Store) map[string]bool {
	t.Helper()
	infos, err := store.List(ctx, "test", objstore.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]bool, len(infos))
	for _, i := range infos {
		out[i.Key] = true
	}
	return out
}

func fqAssertNoNewObjects(t *testing.T, ctx context.Context, store objstore.Store, before map[string]bool) {
	t.Helper()
	for key := range fqKeys(t, ctx, store) {
		if !before[key] {
			t.Errorf("the refused statement left the object %q behind", key)
		}
	}
}
