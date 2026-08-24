package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
)

// TestScanProjectionAttachArithmeticAndBoolean is the direct regression for
// #443/#445: attachScanSelectProjections (the DAG's scan-projection-attach
// path, #169) miscomputed two classes of SELECT-list expression on the stage
// DAG route only.
//
//  1. Arithmetic over a strict-int column (`id + 1`) declared, and computed,
//     FLOAT64 — the integer-preserving-arithmetic rule (#297) resolves the
//     single-process path via strictIntArithCols, but attachScanSelectProjections
//     passed that hint as a hardcoded nil.
//  2. A computed BOOLEAN expression (a comparison, LIKE) came back as the
//     STRING "true"/"false" — parquet.TypeBool's zero value collided with
//     ProjectSpec.Type's "not set" sentinel, so buildSelectProjection could
//     not tell a declared BOOL from a never-declared type and defaulted to
//     STRING.
//
// The embedded single-process engine answered both correctly for the
// identical SQL (values agreed; only the DAG's declared/computed TYPE was
// wrong), which is what made this silent rather than a hard failure: a
// pgwire client asking for the correct OID got a boxed "true"/"false" string
// or a float8 where an int8/bool belonged.
func TestScanProjectionAttachArithmeticAndBoolean(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	coord := tmdCluster(t, ctx)

	res, err := coord.ExecuteSQL(ctx,
		`SELECT id, id > 3 AS gt, c_str LIKE 'str%' AS lk, id + 1 AS pi, id * 2 AS mi `+
			`FROM `+typematrix.Table+` WHERE id < 6 ORDER BY id`)
	if err != nil {
		t.Fatalf("ExecuteSQL: %v", err)
	}

	const wantSchema = "id:INT64,gt:BOOL,lk:BOOL,pi:INT64,mi:INT64"
	if got := describeSchema(res.OutputSchema()); got != wantSchema {
		t.Errorf("declared schema = %q, want %q", got, wantSchema)
	}

	rows, err := res.Rows()
	if err != nil {
		t.Fatalf("Rows: %v", err)
	}
	if len(rows) == 0 {
		t.Fatal("query returned no rows; the assertions below are meaningless")
	}
	for _, r := range rows {
		id, ok := r["id"].(int64)
		if !ok {
			t.Fatalf("row %v: id is %T, want int64", r, r["id"])
		}
		if gt, ok := r["gt"].(bool); !ok {
			t.Errorf("id=%d: gt = %#v (%T), want a bool — a comparison expression came back "+
				"as a STRING on the DAG (#445)", id, r["gt"], r["gt"])
		} else if want := id > 3; gt != want {
			t.Errorf("id=%d: gt = %v, want %v", id, gt, want)
		}
		if lk, ok := r["lk"].(bool); !ok {
			t.Errorf("id=%d: lk = %#v (%T), want a bool — a LIKE expression came back "+
				"as a STRING on the DAG (#445)", id, r["lk"], r["lk"])
		} else if lk {
			// No c_str value in this fixture matches 'str%'.
			t.Errorf("id=%d: lk = true, want false", id)
		}
		if pi, ok := r["pi"].(int64); !ok {
			t.Errorf("id=%d: pi = %#v (%T), want an int64 — arithmetic over a strict-int "+
				"column declared (and computed) FLOAT64 on the DAG (#443, #445)", id, r["pi"], r["pi"])
		} else if pi != id+1 {
			t.Errorf("id=%d: pi = %d, want %d", id, pi, id+1)
		}
		if mi, ok := r["mi"].(int64); !ok {
			t.Errorf("id=%d: mi = %#v (%T), want an int64", id, r["mi"], r["mi"])
		} else if mi != id*2 {
			t.Errorf("id=%d: mi = %d, want %d", id, mi, id*2)
		}
	}
}
