package wadjet

import (
	"context"
	"reflect"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// #589 at the engine, which is where the defect was visible.
//
// An IPv6 or a UUID inside a ROW, an ARRAY or a MAP read back as the EMPTY
// STRING while the identical value in a top-level column read back correctly.
// Parquet has no annotation for either type, so the reader restores them from
// the footer's declared-schema blob — and that overlay stopped at the top
// level. The nested leaf recovered as STRING, the row reader boxed sixteen
// intact bytes as a Go string, batch.Vector.SetValue handed it to
// net.ParseIP, and the value was gone. Nothing errored.
//
// The fixture (internal/oracle/typematrix.NestedDeclared*) writes the same
// value to a top-level column AND into every container position, so the
// assertion here is FLAT == NESTED rather than nested == some literal. The
// coordinator's two-path gate runs the same corpus across the stage DAG;
// this one is the fast arm and the one that can fail with both engines
// agreeing on the wrong answer.

// ndeclOpen loads the #589 fixture into an embedded DB.
func ndeclOpen(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	schema := typematrix.NestedDeclaredSchema()
	if err := db.CreateTable(ctx, typematrix.NestedDeclared, schema, nil); err != nil {
		t.Fatalf("create %s: %v", typematrix.NestedDeclared, err)
	}
	ing := db.NewIngester(typematrix.NestedDeclared, schema, nil, ingest.Config{
		MaxBufferRows: typematrix.NestedDeclaredRows + 1, RowGroupSize: typematrix.RowGroup,
	})
	if err := ing.Ingest(ctx, typematrix.NestedDeclaredData(typematrix.NestedDeclaredRows)); err != nil {
		t.Fatalf("ingest %s: %v", typematrix.NestedDeclared, err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush %s: %v", typematrix.NestedDeclared, err)
	}
	return db
}

// TestNestedDeclaredTypesReadBackTheirValues is the value anchor: the flat
// column and every container position were written the SAME value, so they
// must READ BACK the same. Before the fix the flat column answered
// "2001:db8::11" and every container answered "".
func TestNestedDeclaredTypesReadBackTheirValues(t *testing.T) {
	ctx := context.Background()
	db := ndeclOpen(t)

	res, err := db.Query(ctx, `SELECT id, flat_ipv6, flat_uuid, flat_cidr,
		rw, rw.f_ipv6, rw.f_uuid, rw.f_cidr, arr_ipv6, arr_uuid, m_ipv6
		FROM `+typematrix.NestedDeclared+` ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != typematrix.NestedDeclaredRows {
		t.Fatalf("read %d rows, wrote %d", len(res.Rows), typematrix.NestedDeclaredRows)
	}
	empties, checked := 0, 0
	for _, r := range res.Rows {
		id := r["id"].(int64)
		anchors := map[string]any{
			"ipv6": r["flat_ipv6"], "uuid": r["flat_uuid"], "cidr": r["flat_cidr"],
		}
		eq := func(where, anchor string, have any) {
			t.Helper()
			checked++
			want := anchors[anchor]
			if !reflect.DeepEqual(have, want) {
				t.Errorf("id=%d %s read back %#v; the same value in flat_%s reads back %#v",
					id, where, have, anchor, want)
			}
			if s, ok := have.(string); ok && s == "" && want != nil {
				empties++
			}
		}

		row, _ := r["rw"].(map[string]any)
		eq("rw.f_ipv6 (whole ROW)", "ipv6", ndeclField(row, "f_ipv6"))
		eq("rw.f_uuid (whole ROW)", "uuid", ndeclField(row, "f_uuid"))
		eq("rw.f_cidr (whole ROW)", "cidr", ndeclField(row, "f_cidr"))
		nested, _ := ndeclField(row, "f_nested").(map[string]any)
		eq("rw.f_nested.n_ipv6", "ipv6", ndeclField(nested, "n_ipv6"))
		eq("rw.f_nested.n_uuid", "uuid", ndeclField(nested, "n_uuid"))
		eq("rw.f_ipv6 (field path)", "ipv6", r["f_ipv6"])
		eq("rw.f_uuid (field path)", "uuid", r["f_uuid"])
		eq("rw.f_cidr (field path)", "cidr", r["f_cidr"])
		eq("arr_ipv6[0]", "ipv6", ndeclFirst(r["arr_ipv6"]))
		eq("arr_uuid[0]", "uuid", ndeclFirst(r["arr_uuid"]))
		eq("m_ipv6[k]", "ipv6", ndeclMapValue(r["m_ipv6"]))
	}
	if empties > 0 {
		t.Errorf("%d container positions read back the empty string — #589's signature", empties)
	}
	t.Logf("%d container positions compared against their flat anchor", checked)
}

// ndeclField reads a field out of a ROW value, tolerating a NULL row: the
// fixture NULLs whole containers on purpose, and the flat column is NULL on
// those same rows, so nil is the right answer on both sides.
func ndeclField(row map[string]any, field string) any {
	if row == nil {
		return nil
	}
	return row[field]
}

func ndeclFirst(v any) any {
	a, ok := v.([]any)
	if !ok || len(a) == 0 {
		return nil
	}
	return a[0]
}

// ndeclMapValue pulls the single entry's value out of a materialized MAP,
// which the engine hands back as a list of {key, value} ROWs.
func ndeclMapValue(v any) any {
	a, ok := v.([]any)
	if !ok || len(a) == 0 {
		return nil
	}
	e, ok := a[0].(map[string]any)
	if !ok {
		return nil
	}
	return e["value"]
}

// TestNestedDeclaredTypesCorpusAnswers runs the shape corpus single-process:
// every entry must answer without error and return rows. It is the half the
// coordinator's two-path gate compares against, so a shape that stops parsing
// or stops matching fails here — in the fast suite — rather than only in the
// gate that stands up NATS.
func TestNestedDeclaredTypesCorpusAnswers(t *testing.T) {
	ctx := context.Background()
	db := ndeclOpen(t)
	for _, q := range typematrix.NestedDeclaredCorpus() {
		t.Run(q.Name, func(t *testing.T) {
			res, err := db.Query(ctx, q.SQL)
			if err != nil {
				t.Fatalf("%s: %v", q.SQL, err)
			}
			if len(res.Rows) == 0 {
				t.Fatalf("%s returned no rows — this entry can no longer see anything", q.SQL)
			}
		})
	}
}
