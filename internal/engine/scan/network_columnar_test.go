package scan

import (
	"bytes"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	pqt "github.com/citc-tech/wadjet/internal/storage/parquet"
)

func TestColumnarReadNetworkTypes(t *testing.T) {
	schema := pqt.Schema{
		Columns: []pqt.Column{
			{Name: "src_ip", Type: pqt.TypeIPv4},
			{Name: "dst_ip", Type: pqt.TypeIPv6, Nullable: true},
			{Name: "cidr", Type: pqt.TypeCIDR},
			{Name: "mac", Type: pqt.TypeMAC, Nullable: true},
		},
	}

	rows := []map[string]any{
		{"src_ip": "192.168.1.1", "dst_ip": "::1", "cidr": "10.0.0.0/8", "mac": "aa:bb:cc:dd:ee:ff"},
		{"src_ip": "10.0.0.1", "dst_ip": nil, "cidr": "172.16.0.0/12", "mac": "11:22:33:44:55:66"},
		{"src_ip": "172.16.0.1", "dst_ip": "2001:db8::1", "cidr": "192.168.0.0/24", "mac": nil},
	}

	// Write parquet
	var buf bytes.Buffer
	pw, err := pqt.NewWriter(&buf, schema, pqt.DefaultWriterConfig())
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := pw.WriteRows(rows); err != nil {
		t.Fatalf("WriteRows: %v", err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Read via columnar path
	data := buf.Bytes()
	reader, err := pqt.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	pqFile := reader.File()
	rgs := pqFile.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	rb, err := readRowGroupColumnar(rgs[0], schema.Columns, pqFile)
	if err != nil {
		t.Fatalf("readRowGroupColumnar: %v", err)
	}
	if rb == nil {
		t.Fatal("got nil RecordBatch")
	}
	if rb.Len != 3 {
		t.Fatalf("expected 3 rows, got %d", rb.Len)
	}

	// Verify IPv4 values
	ipCol := rb.Columns[0]
	if ipCol.Type != batch.TypeIPv4 {
		t.Fatalf("expected TypeIPv4, got %v", ipCol.Type)
	}
	v0 := ipCol.GetValue(0)
	if v0 != "192.168.1.1" {
		t.Errorf("row 0 src_ip: expected 192.168.1.1, got %v", v0)
	}
	v1 := ipCol.GetValue(1)
	if v1 != "10.0.0.1" {
		t.Errorf("row 1 src_ip: expected 10.0.0.1, got %v", v1)
	}

	// Verify IPv6 values (nullable)
	ip6Col := rb.Columns[1]
	if ip6Col.Type != batch.TypeIPv6 {
		t.Fatalf("expected TypeIPv6, got %v", ip6Col.Type)
	}
	v6_0 := ip6Col.GetValue(0)
	if v6_0 != "::1" {
		t.Errorf("row 0 dst_ip: expected ::1, got %v", v6_0)
	}
	v6_1 := ip6Col.GetValue(1)
	if v6_1 != nil {
		t.Errorf("row 1 dst_ip: expected nil, got %v", v6_1)
	}

	// Verify CIDR values
	cidrCol := rb.Columns[2]
	cidr0 := cidrCol.GetValue(0)
	if cidr0 != "10.0.0.0/8" {
		t.Errorf("row 0 cidr: expected 10.0.0.0/8, got %v", cidr0)
	}

	// Verify MAC values (nullable)
	macCol := rb.Columns[3]
	mac0 := macCol.GetValue(0)
	if mac0 != "aa:bb:cc:dd:ee:ff" {
		t.Errorf("row 0 mac: expected aa:bb:cc:dd:ee:ff, got %v", mac0)
	}
	mac2 := macCol.GetValue(2)
	if mac2 != nil {
		t.Errorf("row 2 mac: expected nil, got %v", mac2)
	}
}

func TestColumnarReadUUIDAndDate(t *testing.T) {
	schema := pqt.Schema{
		Columns: []pqt.Column{
			{Name: "id", Type: pqt.TypeUUID},
			{Name: "event_date", Type: pqt.TypeDate},
		},
	}

	rows := []map[string]any{
		{"id": "550e8400-e29b-41d4-a716-446655440000", "event_date": "2026-03-15"},
		{"id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8", "event_date": "2026-01-01"},
	}

	var buf bytes.Buffer
	pw, err := pqt.NewWriter(&buf, schema, pqt.DefaultWriterConfig())
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
	reader, err := pqt.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	pqFile := reader.File()
	rgs := pqFile.RowGroups()
	rb, err := readRowGroupColumnar(rgs[0], schema.Columns, pqFile)
	if err != nil {
		t.Fatalf("readRowGroupColumnar: %v", err)
	}
	if rb.Len != 2 {
		t.Fatalf("expected 2 rows, got %d", rb.Len)
	}

	// Verify UUID values
	uuidCol := rb.Columns[0]
	if uuidCol.Type != batch.TypeUUID {
		t.Fatalf("expected TypeUUID, got %v", uuidCol.Type)
	}
	v0 := uuidCol.GetValue(0)
	if v0 != "550e8400-e29b-41d4-a716-446655440000" {
		t.Errorf("row 0 id: expected 550e8400-e29b-41d4-a716-446655440000, got %v", v0)
	}
	v1 := uuidCol.GetValue(1)
	if v1 != "6ba7b810-9dad-11d1-80b4-00c04fd430c8" {
		t.Errorf("row 1 id: expected 6ba7b810-9dad-11d1-80b4-00c04fd430c8, got %v", v1)
	}

	// Verify Date values
	dateCol := rb.Columns[1]
	if dateCol.Type != batch.TypeDate {
		t.Fatalf("expected TypeDate, got %v", dateCol.Type)
	}
	d0 := dateCol.GetValue(0)
	if d0 != "2026-03-15" {
		t.Errorf("row 0 event_date: expected 2026-03-15, got %v", d0)
	}
	d1 := dateCol.GetValue(1)
	if d1 != "2026-01-01" {
		t.Errorf("row 1 event_date: expected 2026-01-01, got %v", d1)
	}
}

func TestColumnarReadSIEMTypes(t *testing.T) {
	schema := pqt.Schema{
		Columns: []pqt.Column{
			{Name: "port", Type: pqt.TypePort},
			{Name: "proto", Type: pqt.TypeProtocol},
			{Name: "latency", Type: pqt.TypeDuration},
		},
	}

	rows := []map[string]any{
		{"port": int32(443), "proto": int32(6), "latency": int64(1500000)},
		{"port": int32(80), "proto": int32(17), "latency": int64(250000)},
	}

	var buf bytes.Buffer
	pw, err := pqt.NewWriter(&buf, schema, pqt.DefaultWriterConfig())
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
	reader, err := pqt.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	pqFile := reader.File()
	rgs := pqFile.RowGroups()
	rb, err := readRowGroupColumnar(rgs[0], schema.Columns, pqFile)
	if err != nil {
		t.Fatalf("readRowGroupColumnar: %v", err)
	}

	// Verify Port
	portVal := rb.Columns[0].GetValue(0)
	if portVal != int32(443) {
		t.Errorf("port row 0: expected 443, got %v", portVal)
	}

	// Verify Protocol
	protoVal := rb.Columns[1].GetValue(1)
	if protoVal != int32(17) {
		t.Errorf("proto row 1: expected 17, got %v", protoVal)
	}

	// Verify Duration
	latVal := rb.Columns[2].GetValue(0)
	if latVal != int64(1500000) {
		t.Errorf("latency row 0: expected 1500000, got %v", latVal)
	}
}
