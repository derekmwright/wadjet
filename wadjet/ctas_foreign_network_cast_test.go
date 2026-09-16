// SPDX-License-Identifier: MIT

package wadjet

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestACtasOverAForeignFileMintsANativeColumn is #1092's consequence, and the
// reason the issue was filed: FOREIGN DATA COULD NOT REACH A NATIVE TYPE.
//
// Parquet has no IPV4, MAC or PROTOCOL, so a file written by any other tool
// carries strings — which is the honest declaration for those bytes. The only
// way to convert them was to re-ingest through Wadjet's own writer, because
// `CAST(col AS IPV4)` handed the string back unchanged and
// `physical.inferCastType` declared STRING for it, so the CREATE minted a
// STRING column that agreed with the cast about being wrong.
//
// Three readers, one assertion each: the created table's DECLARED type is the
// native one, the value read back is the type's own text (canonicalized, which
// is the proof the cast parsed rather than copied), and a predicate over the
// new column answers — the round trip through the writer and the catalog the
// issue asks for.
func TestACtasOverAForeignFileMintsANativeColumn(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// A foreign PARQUET file: every column a string, exactly as another
	// writer would leave it, and the addresses in spellings that CHANGE when
	// parsed so a pass-through cannot pass this test.
	pqPath := filepath.Join(dir, "foreign.parquet")
	var buf bytes.Buffer
	w, err := parquet.NewWriter(&buf, parquet.Schema{Columns: []parquet.Column{
		{Name: "src_ip", Type: parquet.TypeString},
		{Name: "src_mac", Type: parquet.TypeString},
		{Name: "proto", Type: parquet.TypeString},
	}}, parquet.DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRows([]map[string]any{
		{"src_ip": "010.1.2.3", "src_mac": "AA-BB-CC-DD-EE-FF", "proto": "udp"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pqPath, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}

	csvPath := filepath.Join(dir, "foreign.csv")
	if err := os.WriteFile(csvPath,
		[]byte("src_ip,src_mac,proto\n010.1.2.3,AA-BB-CC-DD-EE-FF,udp\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	jsonPath := filepath.Join(dir, "foreign.json")
	if err := os.WriteFile(jsonPath,
		[]byte(`{"src_ip":"010.1.2.3","src_mac":"AA-BB-CC-DD-EE-FF","proto":"udp"}`+"\n"),
		0o644); err != nil {
		t.Fatal(err)
	}

	for _, r := range []struct {
		name string
		from string
	}{
		{"read_parquet", "read_parquet('" + pqPath + "')"},
		{"read_csv", "read_csv('" + csvPath + "')"},
		{"read_json", "read_json('" + jsonPath + "')"},
	} {
		t.Run(r.name, func(t *testing.T) {
			db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.Query(ctx, `CREATE TABLE typed AS SELECT `+
				`CAST(src_ip AS IPV4) AS ip, `+
				`CAST(src_mac AS MACADDR) AS mac, `+
				`CAST(proto AS PROTOCOL) AS proto `+
				`FROM `+r.from); err != nil {
				t.Fatalf("CTAS over %s: %v", r.name, err)
			}

			// The CATALOG's declaration, which is what makes the column
			// native rather than a string that looks like one.
			meta, err := db.catalog.GetTable(ctx, "typed")
			if err != nil {
				t.Fatal(err)
			}
			want := map[string]parquet.TypeID{
				"ip": parquet.TypeIPv4, "mac": parquet.TypeMAC, "proto": parquet.TypeProtocol,
			}
			for _, col := range meta.Schema.Columns {
				if w, ok := want[col.Name]; ok && col.Type != w {
					t.Errorf("column %q declared %v, want %v — the CREATE minted a column "+
						"that cannot hold what the cast produces", col.Name, col.Type, w)
				}
			}

			// The VALUES, canonicalized: a pass-through would read back the
			// spellings the file holds.
			res, err := db.Query(ctx, `SELECT ip, mac, proto FROM typed`)
			if err != nil {
				t.Fatal(err)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("read back %d rows, want 1", len(res.Rows))
			}
			row := res.Rows[0]
			if row["ip"] != "10.1.2.3" {
				t.Errorf("ip = %#v, want %q", row["ip"], "10.1.2.3")
			}
			if row["mac"] != "aa:bb:cc:dd:ee:ff" {
				t.Errorf("mac = %#v, want %q", row["mac"], "aa:bb:cc:dd:ee:ff")
			}
			if row["proto"] != int32(17) {
				t.Errorf("proto = %#v, want int32(17)", row["proto"])
			}

			// And a predicate over the NEW column answers, which is the round
			// trip through the writer, the catalog and the scan.
			res, err = db.Query(ctx,
				`SELECT count(*) AS n FROM typed WHERE ip = '10.1.2.3' AND mac = 'aa:bb:cc:dd:ee:ff'`)
			if err != nil {
				t.Fatal(err)
			}
			if len(res.Rows) != 1 || res.Rows[0]["n"] != int64(1) {
				t.Errorf("the native columns filter as %#v, want one row", res.Rows)
			}
		})
	}
}
