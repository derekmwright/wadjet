package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Regression test for BUG 2 (README verification): "INSERT ... VALUES into
// PORT/PROTOCOL columns always fails". convertValue (wadjet/dml.go) had no
// case for parquet.TypePort or parquet.TypeProtocol, so its default arm
// handed the literal's raw STRING to ingest — but both types are INT32-
// backed and ingest.checkType's integer check rejects a string, so every
// such INSERT failed with "expected integer, got string" regardless of the
// literal.
//
// The convertValue sweep for the bug's other 20 types found one more
// mechanical gap of the same shape: TypeDuration had no case either, and
// unlike Port/Protocol its failure was SILENT — checkType accepts a string
// for DURATION (time.Duration, int64, OR string are all listed as valid),
// so validateRow let it through, but the writer's toInt64/
// convertStringToInt64 (file_writer.go) has no string case for TypeDuration
// and silently wrote int64(0) for every row. This table carries all three
// alongside a table's worth of the other network-native types (already
// correctly handled by convertValue's string pass-through default, per the
// sweep — see the comment on convertValue) as a readback regression guard.
func TestInsertNetworkTypedColumns(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "src_ip", Type: parquet.TypeIPv4, Nullable: true},
		{Name: "src_ip6", Type: parquet.TypeIPv6, Nullable: true},
		{Name: "subnet", Type: parquet.TypeCIDR, Nullable: true},
		{Name: "src_mac", Type: parquet.TypeMAC, Nullable: true},
		{Name: "dst_port", Type: parquet.TypePort, Nullable: true},
		{Name: "proto", Type: parquet.TypeProtocol, Nullable: true},
		{Name: "elapsed", Type: parquet.TypeDuration, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "flow_logs", schema, nil); err != nil {
		t.Fatal(err)
	}

	insertSQL := `INSERT INTO flow_logs
		(id, src_ip, src_ip6, subnet, src_mac, dst_port, proto, elapsed)
		VALUES (1, '10.1.2.3', '2001:db8::1', '192.168.1.0/24', 'aa:bb:cc:dd:ee:ff', 443, 6, 1500000)`
	res, err := db.Execute(ctx, insertSQL)
	if err != nil {
		t.Fatalf("INSERT into flow_logs failed: %v", err)
	}
	if res.RowsAffected != 1 {
		t.Fatalf("RowsAffected = %d, want 1", res.RowsAffected)
	}

	qres, err := db.Query(ctx, "SELECT src_ip, src_ip6, subnet, src_mac, dst_port, proto, elapsed FROM flow_logs WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT after insert failed: %v", err)
	}
	if len(qres.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(qres.Rows))
	}
	row := qres.Rows[0]

	want := map[string]any{
		"src_ip":   "10.1.2.3",
		"src_ip6":  "2001:db8::1",
		"subnet":   "192.168.1.0/24",
		"src_mac":  "aa:bb:cc:dd:ee:ff",
		"dst_port": int32(443),
		"proto":    int32(6),
		"elapsed":  int64(1_500_000),
	}
	for col, w := range want {
		if got := row[col]; got != w {
			t.Errorf("column %q = %#v, want %#v", col, got, w)
		}
	}
}

// TestConvertValuePortAndProtocol pins convertValue's TypePort/TypeProtocol
// parsing directly: an integer literal (quoted or not, matching how the
// INSERT VALUES parser hands numeric text through — see
// dml_parser.go:insertValueText) converts to int32, and a non-numeric
// string is refused rather than silently accepted as some other shape.
//
// The shared literal is "6" (TCP), not PORT's more typical "443": both
// columns run through this same loop, and 443 overflows PROTOCOL's uint8
// range (0-255) — TestConvertValuePortAndProtocolRejectOutOfRange below is
// what pins that boundary; this test is only about the parse/reject shape.
func TestConvertValuePortAndProtocol(t *testing.T) {
	for _, typ := range []parquet.TypeID{parquet.TypePort, parquet.TypeProtocol} {
		v, err := ConvertValue("6", typ)
		if err != nil {
			t.Fatalf("ConvertValue(%q, %s) error: %v", "6", typ, err)
		}
		if v != int32(6) {
			t.Errorf("ConvertValue(%q, %s) = %#v, want int32(6)", "6", typ, v)
		}
		if _, err := ConvertValue("not-a-number", typ); err == nil {
			t.Errorf("ConvertValue(%q, %s) expected an error, got nil", "not-a-number", typ)
		}
	}
}

// TestConvertValuePortAndProtocolRejectOutOfRange pins the range check added
// alongside the PORT/PROTOCOL parsing fix: ParseInt(s, 10, 32) alone accepts
// anything an int32 holds, but PORT is a uint16 and PROTOCOL a uint8
// (parquet/schema.go), so 99999 and -1 both fit in an int32 and used to
// convert and write back verbatim — a value no real port or protocol number
// can be, accepted silently where main errored loudly on the type mismatch.
func TestConvertValuePortAndProtocolRejectOutOfRange(t *testing.T) {
	cases := []struct {
		typ parquet.TypeID
		lit string
	}{
		{parquet.TypePort, "65536"}, // one past uint16 max
		{parquet.TypePort, "99999"},
		{parquet.TypePort, "-1"},
		{parquet.TypeProtocol, "256"}, // one past uint8 max
		{parquet.TypeProtocol, "99999"},
		{parquet.TypeProtocol, "-1"},
	}
	for _, c := range cases {
		if v, err := ConvertValue(c.lit, c.typ); err == nil {
			t.Errorf("ConvertValue(%q, %s) = %#v, want an out-of-range error", c.lit, c.typ, v)
		}
	}
	// The boundary values themselves must still be accepted.
	if v, err := ConvertValue("65535", parquet.TypePort); err != nil || v != int32(65535) {
		t.Errorf("ConvertValue(%q, TypePort) = %#v, %v, want int32(65535), nil", "65535", v, err)
	}
	if v, err := ConvertValue("0", parquet.TypePort); err != nil || v != int32(0) {
		t.Errorf("ConvertValue(%q, TypePort) = %#v, %v, want int32(0), nil", "0", v, err)
	}
	if v, err := ConvertValue("255", parquet.TypeProtocol); err != nil || v != int32(255) {
		t.Errorf("ConvertValue(%q, TypeProtocol) = %#v, %v, want int32(255), nil", "255", v, err)
	}
	if v, err := ConvertValue("0", parquet.TypeProtocol); err != nil || v != int32(0) {
		t.Errorf("ConvertValue(%q, TypeProtocol) = %#v, %v, want int32(0), nil", "0", v, err)
	}
}

// TestInsertRejectsOutOfRangePortAndProtocol is the end-to-end guard for the
// same bug: an INSERT ... VALUES with a PORT/PROTOCOL literal outside the
// stored type's range must fail the statement, not succeed and read back a
// value the column's declared type cannot actually hold.
func TestInsertRejectsOutOfRangePortAndProtocol(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "dst_port", Type: parquet.TypePort, Nullable: true},
		{Name: "proto", Type: parquet.TypeProtocol, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "range_check", schema, nil); err != nil {
		t.Fatal(err)
	}

	for _, sql := range []string{
		`INSERT INTO range_check (id, dst_port, proto) VALUES (1, 99999, 6)`,
		`INSERT INTO range_check (id, dst_port, proto) VALUES (2, -1, 6)`,
		`INSERT INTO range_check (id, dst_port, proto) VALUES (3, 443, 999)`,
		`INSERT INTO range_check (id, dst_port, proto) VALUES (4, 443, -1)`,
	} {
		if _, err := db.Execute(ctx, sql); err == nil {
			t.Errorf("Execute(%q) succeeded, want an out-of-range error", sql)
		}
	}

	qres, err := db.Query(ctx, "SELECT id FROM range_check")
	if err != nil {
		t.Fatalf("SELECT after rejected inserts failed: %v", err)
	}
	if len(qres.Rows) != 0 {
		t.Fatalf("expected 0 rows (every INSERT above should have been rejected), got %d", len(qres.Rows))
	}
}

// TestConvertValueDuration pins convertValue's TypeDuration parsing: a bare
// integer literal (nanoseconds, schema.go's declared unit) converts to
// int64 unchanged, so it reaches the writer as the same type
// batch.Vector.SetValue's own TypeDuration case accepts — never as the
// string that used to silently zero it out.
func TestConvertValueDuration(t *testing.T) {
	v, err := ConvertValue("1500000", parquet.TypeDuration)
	if err != nil {
		t.Fatalf("ConvertValue error: %v", err)
	}
	if v != int64(1_500_000) {
		t.Errorf("ConvertValue(%q, TypeDuration) = %#v, want int64(1500000)", "1500000", v)
	}
	if _, err := ConvertValue("5s", parquet.TypeDuration); err == nil {
		t.Errorf("ConvertValue(%q, TypeDuration) expected an error (no Go-duration-syntax support), got nil", "5s")
	}
}
