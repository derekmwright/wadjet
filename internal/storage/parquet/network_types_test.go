package parquet

import (
	"bytes"
	"net"
	"strings"
	"testing"
)

func TestNetworkTypesRoundTrip(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "src_ip", Type: TypeIPv4},
			{Name: "dst_ip", Type: TypeIPv6, Nullable: true},
			{Name: "cidr", Type: TypeCIDR},
			{Name: "mac", Type: TypeMAC, Nullable: true},
		},
	}

	rows := []map[string]any{
		{"src_ip": "192.168.1.1", "dst_ip": "::1", "cidr": "10.0.0.0/8", "mac": "aa:bb:cc:dd:ee:ff"},
		{"src_ip": "10.0.0.1", "dst_ip": nil, "cidr": "172.16.0.0/12", "mac": "11:22:33:44:55:66"},
		{"src_ip": "172.16.0.1", "dst_ip": "2001:db8::1", "cidr": "192.168.0.0/24", "mac": nil},
	}

	var buf bytes.Buffer
	pw, err := NewWriter(&buf, schema, DefaultWriterConfig())
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := pw.WriteRows(rows); err != nil {
		t.Fatalf("WriteRows: %v", err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data := buf.Bytes()
	reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	if reader.NumRows() != 3 {
		t.Fatalf("expected 3 rows, got %d", reader.NumRows())
	}

	s := reader.Schema()
	t.Logf("Schema: %v", s.ColumnNames())
	t.Logf("File size: %d bytes for %d rows with network types", len(data), reader.NumRows())

	readBack, err := reader.ReadRows(nil)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	for _, row := range readBack {
		t.Logf("  %v", row)
	}

}

func TestNewSIEMTypesRoundTrip(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "src_port", Type: TypePort},
			{Name: "protocol", Type: TypeProtocol},
			{Name: "latency_ns", Type: TypeDuration},
			{Name: "src_ip", Type: TypeIPv4},
		},
	}

	rows := []map[string]any{
		{"src_port": int32(443), "protocol": int32(6), "latency_ns": int64(1500000), "src_ip": "192.168.1.1"},
		{"src_port": int32(80), "protocol": int32(17), "latency_ns": int64(250000), "src_ip": "10.0.0.1"},
		{"src_port": int32(8080), "protocol": int32(6), "latency_ns": int64(5000000000), "src_ip": "172.16.0.1"},
	}

	var buf bytes.Buffer
	pw, err := NewWriter(&buf, schema, DefaultWriterConfig())
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := pw.WriteRows(rows); err != nil {
		t.Fatalf("WriteRows: %v", err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data := buf.Bytes()
	reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	if reader.NumRows() != 3 {
		t.Fatalf("expected 3 rows, got %d", reader.NumRows())
	}

	readBack, err := reader.ReadRows(nil)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	for _, row := range readBack {
		t.Logf("  %v", row)
	}
}

func TestUUIDTypeRoundTrip(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "id", Type: TypeUUID},
			{Name: "label", Type: TypeString},
		},
	}

	rows := []map[string]any{
		{"id": "550e8400-e29b-41d4-a716-446655440000", "label": "first"},
		{"id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8", "label": "second"},
		{"id": "f47ac10b-58cc-4372-a567-0e02b2c3d479", "label": "third"},
	}

	var buf bytes.Buffer
	pw, err := NewWriter(&buf, schema, DefaultWriterConfig())
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := pw.WriteRows(rows); err != nil {
		t.Fatalf("WriteRows: %v", err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data := buf.Bytes()
	reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	if reader.NumRows() != 3 {
		t.Fatalf("expected 3 rows, got %d", reader.NumRows())
	}

	readBack, err := reader.ReadRows(nil)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	for _, row := range readBack {
		t.Logf("  %v", row)
	}
}

func TestDateTypeRoundTrip(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "event_date", Type: TypeDate},
			{Name: "count", Type: TypeInt64},
		},
	}

	rows := []map[string]any{
		{"event_date": "2026-03-15", "count": int64(100)},
		{"event_date": "2026-01-01", "count": int64(50)},
		{"event_date": "1970-01-01", "count": int64(0)},
	}

	var buf bytes.Buffer
	pw, err := NewWriter(&buf, schema, DefaultWriterConfig())
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := pw.WriteRows(rows); err != nil {
		t.Fatalf("WriteRows: %v", err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data := buf.Bytes()
	reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	if reader.NumRows() != 3 {
		t.Fatalf("expected 3 rows, got %d", reader.NumRows())
	}

	readBack, err := reader.ReadRows(nil)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	for _, row := range readBack {
		t.Logf("  %v", row)
	}
}

func TestParseTypeIDNewTypes(t *testing.T) {
	tests := []struct {
		input string
		want  TypeID
		err   bool
	}{
		{"PORT", TypePort, false},
		{"PROTOCOL", TypeProtocol, false},
		{"PROTO", TypeProtocol, false},
		{"DURATION", TypeDuration, false},
		{"INTERVAL", TypeDuration, false},
		{"IPV4", TypeIPv4, false},
		{"IP", TypeIPv4, false},
		{"IPV6", TypeIPv6, false},
		{"CIDR", TypeCIDR, false},
		{"MAC", TypeMAC, false},
		{"MACADDR", TypeMAC, false},
		{"UUID", TypeUUID, false},
		{"GUID", TypeUUID, false},
		{"DATE", TypeDate, false},
	}
	for _, tt := range tests {
		got, err := ParseTypeID(tt.input)
		if tt.err {
			if err == nil {
				t.Errorf("ParseTypeID(%q): expected error", tt.input)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseTypeID(%q): %v", tt.input, err)
			continue
		}
		if got != tt.want {
			t.Errorf("ParseTypeID(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

// TestNativeWriterParsesIPv6Literals covers the direct NativeWriter API,
// which Writer.prepareRows does not sit in front of.
//
// ipv6StringToBytes used to store the literal's TEXT: 11 bytes where an IPV6
// column's contract is exactly 16. Nothing complained, because the reader
// reported the column as STRING until the declared type started coming back
// (#396) — and the engine renders a non-16-byte IPV6 value as the EMPTY
// STRING. Every row written through this path read back as "".
//
// The empty literal is now written as NULL rather than as a zero-length
// value, and an unparseable one is refused; the two cases are split into
// TestNetworkLiteralAbsenceIsNull and TestUnparseableNetworkLiteralIsRefused.
func TestNativeWriterParsesIPv6Literals(t *testing.T) {
	schema := Schema{Columns: []Column{{Name: "addr", Type: TypeIPv6, Nullable: true}}}

	preconverted := net.ParseIP("2001:db8::7").To16()
	cases := []struct {
		name string
		in   any
		want net.IP // nil means "no value stored"
	}{
		{"full literal", "2001:db8::5", net.ParseIP("2001:db8::5")},
		{"loopback", "::1", net.ParseIP("::1")},
		{"ipv4 literal maps into 16 bytes", "10.0.0.5", net.ParseIP("10.0.0.5").To16()},
		{"already storage bytes", []byte(preconverted), preconverted},
		{"empty literal stores nothing", "", nil},
	}

	rows := make([]map[string]any, len(cases))
	for i, tc := range cases {
		rows[i] = map[string]any{"addr": tc.in}
	}

	var buf bytes.Buffer
	w := NewNativeWriter(&buf, schema, DefaultWriterConfig())
	if err := w.WriteMapRows(rows); err != nil {
		t.Fatalf("WriteMapRows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data := buf.Bytes()
	r, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if got := r.Schema().Columns[0].Type; got != TypeIPv6 {
		t.Fatalf("column read back as %s, want IPV6", got)
	}
	readBack, err := r.ReadRows(nil)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	if len(readBack) != len(cases) {
		t.Fatalf("read %d rows, want %d", len(readBack), len(cases))
	}
	for i, tc := range cases {
		raw, _ := readBack[i]["addr"].([]byte)
		if tc.want == nil {
			if _, present := readBack[i]["addr"]; present {
				t.Errorf("%s: stored %d bytes (%x), want NULL", tc.name, len(raw), raw)
			}
			continue
		}
		if len(raw) != 16 {
			t.Errorf("%s: stored %d bytes (%q), want the 16-byte address form",
				tc.name, len(raw), raw)
			continue
		}
		if !net.IP(raw).Equal(tc.want) {
			t.Errorf("%s: stored %s, want %s", tc.name, net.IP(raw), tc.want)
		}
	}
}

// TestUnparseableNetworkLiteralIsRefused: a literal that is not an address,
// a MAC or a UUID is not one, and the writer says so instead of storing
// something address-shaped.
//
// Every converter used to answer garbage with a zero value. "zz" became
// 00:00:00:00:00:00 in a MAC column, indistinguishable from a real address
// somebody meant. "not-a-uuid" was stored as THE RAW STRING BYTES — ten bytes
// in a column whose entries are sixteen — which the row reader then refused
// ("UUID is 16 bytes per value but row 2 holds 10") on a file wadjet itself
// had written, while the native reader read it.
//
// PostgreSQL decides (ADR-0012): `invalid input syntax for type uuid` is an
// error, so this is one, and it names the column, the row and the literal.
func TestUnparseableNetworkLiteralIsRefused(t *testing.T) {
	cases := []struct {
		typ TypeID
		bad string
	}{
		{TypeIPv6, "not an address"},
		{TypeIPv6, "2001:db8::zz"},
		{TypeUUID, "not-a-uuid"},
		{TypeUUID, "550e8400"},
		{TypeUUID, "GGGGGGGG-GGGG-GGGG-GGGG-GGGGGGGGGGGG"},
		{TypeMAC, "zz"},
		{TypeMAC, "00:11:22:33:44:5"},
		{TypeMAC, "00:11:22:33:44:zz"},
		{TypeIPv4, "not an address"},
		{TypeIPv4, "10.0.0"},
		{TypeIPv4, "10.0.0.256"},
		{TypeIPv4, "10.0.0.1.2"},
	}
	for _, tc := range cases {
		t.Run(tc.typ.String()+"/"+tc.bad, func(t *testing.T) {
			schema := Schema{Columns: []Column{{Name: "c", Type: tc.typ, Nullable: true}}}
			var buf bytes.Buffer
			w := NewNativeWriter(&buf, schema, DefaultWriterConfig())
			err := w.WriteMapRows([]map[string]any{
				{"c": nil},
				{"c": tc.bad},
			})
			if err == nil {
				err = w.Close()
			}
			if err == nil {
				t.Fatalf("wrote %q into a %s column without complaint", tc.bad, tc.typ)
			}
			for _, want := range []string{`column "c"`, "row 1", tc.bad} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not mention %q", err, want)
				}
			}
		})
	}

	// And the valid forms of each still write.
	for _, tc := range []struct {
		typ  TypeID
		good string
	}{
		{TypeIPv6, "2001:db8::1"},
		{TypeUUID, "550e8400-e29b-41d4-a716-446655440000"},
		{TypeUUID, "550e8400e29b41d4a716446655440000"},
		{TypeMAC, "00:11:22:33:44:55"},
		{TypeIPv4, "10.0.0.1"},
		{TypeIPv4, "0.0.0.0"},
		{TypeIPv4, "255.255.255.255"},
	} {
		t.Run("valid/"+tc.typ.String()+"/"+tc.good, func(t *testing.T) {
			schema := Schema{Columns: []Column{{Name: "c", Type: tc.typ, Nullable: true}}}
			var buf bytes.Buffer
			w := NewNativeWriter(&buf, schema, DefaultWriterConfig())
			if err := w.WriteMapRows([]map[string]any{{"c": tc.good}}); err != nil {
				t.Fatalf("WriteMapRows: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		})
	}
}
