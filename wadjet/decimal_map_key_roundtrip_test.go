package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A MAP KEY must survive the parquet round trip for every family whose key is
// carried as its own text — the axis is the KEY POSITION, not the type (#883).
//
//	MAP(DECIMAL(18,4), STRING) key 12.75        -> "12.7500"    (was "127500.0000")
//	MAP(DECIMAL(9,2),  STRING) key 12.75        -> "12.75"      (was "1275.00")
//	MAP(DECIMAL(18,0), STRING) key 13           -> "13"         (always right, scale 0)
//	MAP(DATE, STRING)          key 2023-11-14   -> "2023-11-14" (was NULL, key lost)
//	MAP(TIMESTAMP, STRING)     key <ts>         -> epoch millis (right on both paths)
//	MAP(IPv4, STRING)          key 192.168.1.10 -> same         (was 0.0.0.0)
//	MAP(MAC, STRING)           key aa:bb:..:ff  -> same         (was 00:00:..:00)
//	MAP(IPv6, STRING)          key 2001:db8::1  -> same         (was lost)
//	MAP(UUID, STRING)          key <uuid>       -> same         (was "")
//	MAP(CIDR, STRING)          key 10.0.0.0/8   -> same         (always right, text carrier)
//	MAP(BYTES, STRING)         key "hello"      -> []byte("hello") (was "[104 101 ...]")
//
// Root cause (fixed): on read, a nested-map key leaf decodes to its CARRIER,
// not its display value — an UNSCALED DECIMAL integer, a DATE day count, an
// int64 for IPv4/MAC, raw bytes for IPv6/UUID/BYTES (parquet.StorageClassOf).
// The record assembler printed that carrier with fmt.Sprint and re-ingested it,
// so the DECIMAL child re-scaled "127500" a SECOND time and the DATE/IPv4/MAC/
// IPv6/UUID/BYTES children could not parse "19675" / "3232235786" / "[10 0 0 5]"
// and dropped the key to a zero value. The assembler now renders EVERY map-key
// carrier through one canonical, parseable path (parquet.MapKeyCarrierText,
// exhaustive over the 22 TypeIDs — see TestMapKeyCarrierTextCoversEveryType), so
// the child reconstructs the value and GetValue re-renders its display form. A
// map VALUE was always right because it stays the typed box; only the key is
// forced through text because a Go map's key must be a string.
//
// The in-memory batch.FromRows path is the CONTROL and was already right for
// DECIMAL/DATE; a TIMESTAMP key given as wall-clock text was dropped there
// (batch.mapKeyValue ParseInt'd it) and is now handed to the child's own
// timestamp-text parse.
//
// The right-position siblings — DECIMAL map VALUE, DECIMAL ARRAY element — are
// asserted beside the keys so a fix that moved the wrong one is caught.
func TestDecimalMapKeySurvivesTheParquetRoundTrip(t *testing.T) {
	ctx := context.Background()

	// The IN-MEMORY path, the control: no parquet, same schema, same row.
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
		t.Errorf("in-memory DECIMAL map key = %#v, want \"12.7500\"", got)
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

	// The DECIMAL map KEY now round-trips: 12.7500, not the double-scaled
	// 127500.0000 the writer stored before the assembler rendered it at scale.
	t.Run("decimal_map_key_round_trips", func(t *testing.T) {
		res, err := db.Query(ctx, `SELECT mk AS v FROM decmapkey WHERE id = 1`)
		if err != nil {
			t.Fatalf("%v", err)
		}
		got, _ := res.Rows[0]["v"].([]any)
		if len(got) != 1 {
			t.Fatalf("MAP came back as %#v", res.Rows[0]["v"])
		}
		entry, _ := got[0].(map[string]any)
		if key, _ := entry["key"].(string); key != "12.7500" {
			t.Errorf("DECIMAL map key reads back %q, want \"12.7500\"", key)
		}
	})

	// The two siblings that were always RIGHT and must stay right.
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

	// The SCALE is the multiplier the corruption was proportional to, so a
	// second scale is a second cell: (9,2), and the (18,0) control where
	// 10^0 = 1 was right all along. Both must now read back the written value.
	t.Run("scale_is_the_multiplier", func(t *testing.T) {
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
			{"mk92", "12.75"},
			{"mk180", "13"},
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
				t.Errorf("%s key reads back %q, want %q", c.col, key, c.want)
			}
		}
	})

	// The DATE and TIMESTAMP faces of the same defect (#883). A DATE key was
	// LOST — its day-count carrier is not a date string — and now round-trips;
	// a TIMESTAMP key crosses as its epoch-millis carrier on both the parquet
	// and the (previously key-dropping) in-memory path.
	t.Run("date_and_timestamp_map_keys", func(t *testing.T) {
		sc3 := parquet.Schema{Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "md", Type: parquet.TypeMap, Nullable: true,
				ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
					{Name: "key", Type: parquet.TypeDate},
					{Name: "value", Type: parquet.TypeString, Nullable: true},
				}}},
			{Name: "mts", Type: parquet.TypeMap, Nullable: true,
				ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
					{Name: "key", Type: parquet.TypeTimestamp},
					{Name: "value", Type: parquet.TypeString, Nullable: true},
				}}},
		}}
		if err := db.CreateTable(ctx, "dtmapkey", sc3, nil); err != nil {
			t.Fatalf("create: %v", err)
		}
		ing := db.NewIngester("dtmapkey", sc3, nil, ingest.Config{MaxBufferRows: 8, RowGroupSize: 4})
		if err := ing.Ingest(ctx, []map[string]any{{
			"id":  int64(1),
			"md":  map[string]any{"2023-11-14": "b"},
			"mts": map[string]any{"2023-11-14 00:00:00": "c"},
		}}); err != nil {
			t.Fatalf("ingest: %v", err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
		for _, c := range []struct {
			col  string
			want any
		}{
			{"md", "2023-11-14"},
			{"mts", int64(1699920000000)},
		} {
			res, err := db.Query(ctx, `SELECT `+c.col+` AS v FROM dtmapkey WHERE id = 1`)
			if err != nil {
				t.Fatalf("%v", err)
			}
			got, _ := res.Rows[0]["v"].([]any)
			if len(got) != 1 {
				t.Fatalf("%s came back as %#v", c.col, res.Rows[0]["v"])
			}
			entry, _ := got[0].(map[string]any)
			if entry["key"] != c.want {
				t.Errorf("%s key reads back %#v, want %#v", c.col, entry["key"], c.want)
			}
		}

		// The in-memory control for the TIMESTAMP key, which was dropped
		// before (batch.mapKeyValue ParseInt on wall-clock text).
		tsCol := parquet.Column{Name: "mts", Type: parquet.TypeMap, Nullable: true,
			ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "key", Type: parquet.TypeTimestamp},
				{Name: "value", Type: parquet.TypeString, Nullable: true},
			}}}
		bb := batch.FromRows([]parquet.Column{tsCol},
			[]map[string]any{{"mts": map[string]any{"2023-11-14 00:00:00": "c"}}})
		ents, _ := bb.Columns[0].GetValue(0).([]any)
		if len(ents) != 1 {
			t.Fatalf("in-memory TIMESTAMP map came back as %#v", bb.Columns[0].GetValue(0))
		}
		e, _ := ents[0].(map[string]any)
		if e["key"] != int64(1699920000000) {
			t.Errorf("in-memory TIMESTAMP map key = %#v, want epoch 1699920000000 (was dropped)", e["key"])
		}
	})

	// The network families: the same defect, one per StorageClassOf carrier.
	// A leaf that decodes to an int64 (IPv4, MAC) or raw bytes (IPv6, UUID)
	// was fmt.Sprint'd to "3232235786" / "[10 0 0 5]" and lost to a zero value
	// on the round trip; the assembler now renders every carrier canonically.
	// CIDR was always right (its carrier is already text) and is kept as the
	// control. The read-out re-renders each key with GetValue, so the asserted
	// text is the canonical display form regardless of the parseable spelling
	// the assembler produced.
	t.Run("network_map_keys", func(t *testing.T) {
		netMap := func(name string, kt parquet.TypeID) parquet.Column {
			return parquet.Column{Name: name, Type: parquet.TypeMap, Nullable: true,
				ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
					{Name: "key", Type: kt},
					{Name: "value", Type: parquet.TypeString, Nullable: true},
				}}}
		}
		sc4 := parquet.Schema{Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			netMap("m4", parquet.TypeIPv4),
			netMap("m6", parquet.TypeIPv6),
			netMap("mmac", parquet.TypeMAC),
			netMap("muuid", parquet.TypeUUID),
			netMap("mcidr", parquet.TypeCIDR),
			netMap("mbytes", parquet.TypeBytes),
		}}
		if err := db.CreateTable(ctx, "netmapkey", sc4, nil); err != nil {
			t.Fatalf("create: %v", err)
		}
		ing := db.NewIngester("netmapkey", sc4, nil, ingest.Config{MaxBufferRows: 8, RowGroupSize: 4})
		if err := ing.Ingest(ctx, []map[string]any{{
			"id":     int64(1),
			"m4":     map[string]any{"192.168.1.10": "a"},
			"m6":     map[string]any{"2001:db8::1": "b"},
			"mmac":   map[string]any{"aa:bb:cc:dd:ee:ff": "c"},
			"muuid":  map[string]any{"12345678-1234-1234-1234-1234567890ab": "d"},
			"mcidr":  map[string]any{"10.0.0.0/8": "e"},
			"mbytes": map[string]any{"hello": "f"},
		}}); err != nil {
			t.Fatalf("ingest: %v", err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
		for _, c := range []struct{ col, want string }{
			{"m4", "192.168.1.10"},
			{"m6", "2001:db8::1"},
			{"mmac", "aa:bb:cc:dd:ee:ff"},
			{"muuid", "12345678-1234-1234-1234-1234567890ab"},
			{"mcidr", "10.0.0.0/8"},
		} {
			res, err := db.Query(ctx, `SELECT `+c.col+` AS v FROM netmapkey WHERE id = 1`)
			if err != nil {
				t.Fatalf("%s: %v", c.col, err)
			}
			got, _ := res.Rows[0]["v"].([]any)
			if len(got) != 1 {
				t.Fatalf("%s came back as %#v", c.col, res.Rows[0]["v"])
			}
			entry, _ := got[0].(map[string]any)
			if key, _ := entry["key"].(string); key != c.want {
				t.Errorf("%s key reads back %q, want %q", c.col, key, c.want)
			}
		}

		// A BYTES key reads back as the raw []byte a scalar BYTES column gives
		// (not a string), so it is asserted apart from the string-keyed families
		// above. On revert (fmt.Sprint of the carrier) it read back the bytes of
		// "[104 101 108 108 111]".
		res, err := db.Query(ctx, `SELECT mbytes AS v FROM netmapkey WHERE id = 1`)
		if err != nil {
			t.Fatalf("mbytes: %v", err)
		}
		got, _ := res.Rows[0]["v"].([]any)
		if len(got) != 1 {
			t.Fatalf("mbytes came back as %#v", res.Rows[0]["v"])
		}
		entry, _ := got[0].(map[string]any)
		key, _ := entry["key"].([]byte)
		if string(key) != "hello" {
			t.Errorf("BYTES map key reads back %#v, want []byte(\"hello\")", entry["key"])
		}
	})
}
