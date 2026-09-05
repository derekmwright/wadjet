package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A MAP KEY is corrupted by the parquet round trip, and the axis is the KEY
// POSITION rather than the DECIMAL type (rescoped 2026-09-05 after review, P5).
//
//	MAP(DECIMAL(18,4), STRING) key 12.75        -> "127500.0000"  scaled twice
//	MAP(DECIMAL(9,2),  STRING) key 12.75        -> "1275.00"      the same, x10^2
//	MAP(DECIMAL(18,0), STRING) key 13           -> "13"           right (10^0)
//
// The DATE and TIMESTAMP faces of "a map key is handed to the child as text"
// are NOT gated here and this file makes no claim about them: a probe found
// `MAP(DATE, STRING)` losing its key, but it is lost in `batch.FromRows`,
// BEFORE parquet — a different layer from the one this test pins, and one
// whose in-memory control is the thing that fails. Naming them here without a
// cell would be a claim with no fixture; they are in the arc report as a lead.
//
// while the DECIMAL map VALUE, a ROW field, an ARRAY-of-ROW field and an ARRAY
// of ARRAY all round-trip correctly. The defect is every family whose map key
// is handed to the child as its own TEXT (batch.mapKeyValue: "every other
// family parses its own string form already") and something below reads that
// text as an already-scaled carrier.
//
// Found while fixing #669, whose decimal-keyed-map lookup had to be gated at
// the expr layer over a constructed batch because of this — an end-to-end
// fixture cannot tell a lookup defect from a storage one when the stored value
// is already wrong.
//
// The defect is SPECIFIC to a map KEY, which is what makes it locatable: the
// in-memory path is right, a DECIMAL map VALUE is right, and a DECIMAL ARRAY
// element is right. All four are asserted below, so a fix that moves the wrong
// one is caught here.
//
// `batch.mapKeyValue` hands a map key's own TEXT to the DECIMAL child (every
// non-numeric family "parses its own string form already", as its doc says),
// and something below reads that text as an already-scaled carrier — the
// ADR-0018 §4 hand-off, applied to a value that has not been through it.
//
// TODO(parquet): delete this pin when a DECIMAL map key round-trips. The
// residual cell FAILS when it starts agreeing, which is what makes deleting it
// the proof.
func TestDecimalMapKeySurvivesTheParquetRoundTrip(t *testing.T) {
	ctx := context.Background()

	// The IN-MEMORY path, which is the control: no parquet, same schema, same
	// row. It is right, so the defect is at the storage boundary.
	keyCol := parquet.Column{Name: "mk", Type: parquet.TypeMap, Nullable: true,
		ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
			{Name: "key", Type: parquet.TypeDecimal, Precision: 18, Scale: 4},
			{Name: "value", Type: parquet.TypeString, Nullable: true},
		}}}
	b := batch.FromRows([]parquet.Column{keyCol},
		[]map[string]any{{"mk": map[string]any{"12.75": "twelve"}}})
	entries, ok := b.Columns[0].GetValue(0).([]any)
	if !ok || len(entries) != 1 {
		t.Fatalf("in-memory MAP came back as %#v", b.Columns[0].GetValue(0))
	}
	row, _ := entries[0].(map[string]any)
	if got := row["key"]; got != "12.7500" {
		t.Errorf("in-memory DECIMAL map key = %#v, want \"12.7500\" — the CONTROL moved, "+
			"so this is no longer a storage-only defect", got)
	}

	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	sc := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		keyCol,
		{Name: "mv", Type: parquet.TypeMap, Nullable: true,
			ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "key", Type: parquet.TypeString},
				{Name: "value", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true},
			}}},
		{Name: "ad", Type: parquet.TypeArray, Nullable: true, ElementType: &parquet.Column{
			Name: "element", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true}},
	}}
	if err := db.CreateTable(ctx, "decmapkey", sc, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	ing := db.NewIngester("decmapkey", sc, nil, ingest.Config{MaxBufferRows: 8, RowGroupSize: 4})
	if err := ing.Ingest(ctx, []map[string]any{{
		"id": int64(1),
		"mk": map[string]any{"12.75": "twelve"},
		"mv": map[string]any{"a": 12.75},
		"ad": []any{12.75},
	}}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// The two siblings that are RIGHT, and must stay right: a DECIMAL map
	// VALUE and a DECIMAL ARRAY element through the same writer and reader.
	for _, c := range []struct{ name, sql, want string }{
		{"map_value", `SELECT ELEMENT_AT(mv, 'a') AS v FROM decmapkey WHERE id = 1`, "12.7500"},
		{"array_element", `SELECT ELEMENT_AT(ad, 1) AS v FROM decmapkey WHERE id = 1`, "12.7500"},
	} {
		t.Run(c.name, func(t *testing.T) {
			res, err := db.Query(ctx, c.sql)
			if err != nil {
				t.Fatalf("%v\n  SQL: %s", err, c.sql)
			}
			if len(res.Rows) != 1 || res.Rows[0]["v"] != c.want {
				t.Errorf("= %#v, want %q", res.Rows, c.want)
			}
		})
	}

	// The SCALE is what the corruption is proportional to, so a second scale
	// is a second cell: at (9,2) the same 12.75 reads back 1275.00, and at
	// (18,0) — where 10^0 is 1 — it is right. Two of the three would agree
	// with a fix that only moved the (18,4) case.
	t.Run("residual_scale_is_the_multiplier", func(t *testing.T) {
		sc2 := parquet.Schema{Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "mk92", Type: parquet.TypeMap, Nullable: true,
				ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
					{Name: "key", Type: parquet.TypeDecimal, Precision: 9, Scale: 2},
					{Name: "value", Type: parquet.TypeString, Nullable: true},
				}}},
			{Name: "mk180", Type: parquet.TypeMap, Nullable: true,
				ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
					{Name: "key", Type: parquet.TypeDecimal, Precision: 18, Scale: 0},
					{Name: "value", Type: parquet.TypeString, Nullable: true},
				}}},
		}}
		if err := db.CreateTable(ctx, "decmapkey2", sc2, nil); err != nil {
			t.Fatalf("create: %v", err)
		}
		ing := db.NewIngester("decmapkey2", sc2, nil, ingest.Config{MaxBufferRows: 8, RowGroupSize: 4})
		if err := ing.Ingest(ctx, []map[string]any{{
			"id":    int64(1),
			"mk92":  map[string]any{"12.75": "a"},
			"mk180": map[string]any{"13": "b"},
		}}); err != nil {
			t.Fatalf("ingest: %v", err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
		for _, c := range []struct{ col, want string }{
			{"mk92", "1275.00"}, // 12.75 x 10^2 — the pin
			{"mk180", "13"},     // scale 0, so the multiplier is 1 — right
		} {
			res, err := db.Query(ctx, `SELECT `+c.col+` AS v FROM decmapkey2 WHERE id = 1`)
			if err != nil {
				t.Fatalf("%v", err)
			}
			got, _ := res.Rows[0]["v"].([]any)
			if len(got) != 1 {
				t.Fatalf("%s came back as %#v", c.col, res.Rows[0]["v"])
			}
			entry, _ := got[0].(map[string]any)
			if key, _ := entry["key"].(string); key != c.want {
				t.Errorf("%s key reads back %q, this pin records %q — the multiplier is the "+
					"column's SCALE, so re-measure the whole family", c.col, key, c.want)
			}
		}
	})

	// The RESIDUAL, pinned fail-on-agree.
	t.Run("residual_map_key_is_scaled_twice", func(t *testing.T) {
		res, err := db.Query(ctx, `SELECT mk AS v FROM decmapkey WHERE id = 1`)
		if err != nil {
			t.Fatalf("%v", err)
		}
		got, _ := res.Rows[0]["v"].([]any)
		if len(got) != 1 {
			t.Fatalf("MAP came back as %#v", res.Rows[0]["v"])
		}
		entry, _ := got[0].(map[string]any)
		key, _ := entry["key"].(string)
		if key != "127500.0000" {
			t.Errorf("the DECIMAL map key reads back %q; this pin records \"127500.0000\", "+
				"the value scaled a second time, and \"12.7500\" is what was written. If it "+
				"has moved, the parquet round trip carries a DECIMAL map key correctly now: "+
				"delete this pin and give #669's decimal-key lookup an end-to-end cell", key)
		}
	})
}
