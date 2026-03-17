package kernel

import (
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

func TestFilterIPv4(t *testing.T) {
	schema := []parquet.Column{
		{Name: "src_ip", Type: parquet.TypeIPv4},
	}
	b := batch.NewRecordBatch(schema, 4)
	b.Columns[0].SetValue(0, "192.168.1.1")
	b.Columns[0].SetValue(1, "192.168.1.2")
	b.Columns[0].SetValue(2, "10.0.0.1")
	b.Columns[0].SetValue(3, "192.168.1.1")

	kern := ResolveFilterKernel(batch.TypeIPv4, OpEq, "192.168.1.1")
	if kern == nil {
		t.Fatal("ResolveFilterKernel returned nil for TypeIPv4")
	}
	outSel := make([]uint16, 0, 4)
	sel := kern(b.Columns[0], nil, 4, outSel)
	if len(sel) != 2 {
		t.Fatalf("expected 2 matches, got %d", len(sel))
	}
	if sel[0] != 0 || sel[1] != 3 {
		t.Fatalf("expected indices [0, 3], got %v", sel)
	}
}

func TestFilterIPv4LessThan(t *testing.T) {
	schema := []parquet.Column{
		{Name: "src_ip", Type: parquet.TypeIPv4},
	}
	b := batch.NewRecordBatch(schema, 3)
	b.Columns[0].SetValue(0, "10.0.0.1")
	b.Columns[0].SetValue(1, "172.16.0.1")
	b.Columns[0].SetValue(2, "192.168.1.1")

	kern := ResolveFilterKernel(batch.TypeIPv4, OpLt, "172.16.0.1")
	if kern == nil {
		t.Fatal("ResolveFilterKernel returned nil for TypeIPv4 OpLt")
	}
	outSel := make([]uint16, 0, 3)
	sel := kern(b.Columns[0], nil, 3, outSel)
	if len(sel) != 1 || sel[0] != 0 {
		t.Fatalf("expected [0] (10.0.0.1 < 172.16.0.1), got %v", sel)
	}
}

func TestFilterMAC(t *testing.T) {
	schema := []parquet.Column{
		{Name: "mac", Type: parquet.TypeMAC},
	}
	b := batch.NewRecordBatch(schema, 3)
	b.Columns[0].SetValue(0, "aa:bb:cc:dd:ee:ff")
	b.Columns[0].SetValue(1, "11:22:33:44:55:66")
	b.Columns[0].SetValue(2, "aa:bb:cc:dd:ee:ff")

	kern := ResolveFilterKernel(batch.TypeMAC, OpEq, "aa:bb:cc:dd:ee:ff")
	if kern == nil {
		t.Fatal("ResolveFilterKernel returned nil for TypeMAC")
	}
	outSel := make([]uint16, 0, 3)
	sel := kern(b.Columns[0], nil, 3, outSel)
	if len(sel) != 2 {
		t.Fatalf("expected 2 matches, got %d", len(sel))
	}
}

func TestFilterIPv6(t *testing.T) {
	schema := []parquet.Column{
		{Name: "dst_ip", Type: parquet.TypeIPv6},
	}
	b := batch.NewRecordBatch(schema, 3)
	b.Columns[0].SetValue(0, "::1")
	b.Columns[0].SetValue(1, "2001:db8::1")
	b.Columns[0].SetValue(2, "::1")

	kern := ResolveFilterKernel(batch.TypeIPv6, OpEq, "::1")
	if kern == nil {
		t.Fatal("ResolveFilterKernel returned nil for TypeIPv6")
	}
	outSel := make([]uint16, 0, 3)
	sel := kern(b.Columns[0], nil, 3, outSel)
	if len(sel) != 2 {
		t.Fatalf("expected 2 matches, got %d", len(sel))
	}
	if sel[0] != 0 || sel[1] != 2 {
		t.Fatalf("expected indices [0, 2], got %v", sel)
	}
}

func TestFilterCIDR(t *testing.T) {
	schema := []parquet.Column{
		{Name: "cidr", Type: parquet.TypeCIDR},
	}
	b := batch.NewRecordBatch(schema, 3)
	b.Columns[0].SetValue(0, "10.0.0.0/8")
	b.Columns[0].SetValue(1, "172.16.0.0/12")
	b.Columns[0].SetValue(2, "10.0.0.0/8")

	kern := ResolveFilterKernel(batch.TypeCIDR, OpEq, "10.0.0.0/8")
	if kern == nil {
		t.Fatal("ResolveFilterKernel returned nil for TypeCIDR")
	}
	outSel := make([]uint16, 0, 3)
	sel := kern(b.Columns[0], nil, 3, outSel)
	if len(sel) != 2 {
		t.Fatalf("expected 2 matches, got %d", len(sel))
	}
}

func TestFilterPort(t *testing.T) {
	schema := []parquet.Column{
		{Name: "port", Type: parquet.TypePort},
	}
	b := batch.NewRecordBatch(schema, 4)
	b.Columns[0].SetValue(0, int32(443))
	b.Columns[0].SetValue(1, int32(80))
	b.Columns[0].SetValue(2, int32(8080))
	b.Columns[0].SetValue(3, int32(443))

	kern := ResolveFilterKernel(batch.TypePort, OpEq, int64(443))
	if kern == nil {
		t.Fatal("ResolveFilterKernel returned nil for TypePort")
	}
	outSel := make([]uint16, 0, 4)
	sel := kern(b.Columns[0], nil, 4, outSel)
	if len(sel) != 2 {
		t.Fatalf("expected 2 matches, got %d", len(sel))
	}
}

func TestFilterDuration(t *testing.T) {
	schema := []parquet.Column{
		{Name: "latency_ns", Type: parquet.TypeDuration},
	}
	b := batch.NewRecordBatch(schema, 3)
	b.Columns[0].SetValue(0, int64(1000000))   // 1ms
	b.Columns[0].SetValue(1, int64(5000000000)) // 5s
	b.Columns[0].SetValue(2, int64(500000))     // 0.5ms

	// Filter: latency > 1ms
	kern := ResolveFilterKernel(batch.TypeDuration, OpGt, int64(1000000))
	if kern == nil {
		t.Fatal("ResolveFilterKernel returned nil for TypeDuration")
	}
	outSel := make([]uint16, 0, 3)
	sel := kern(b.Columns[0], nil, 3, outSel)
	if len(sel) != 1 || sel[0] != 1 {
		t.Fatalf("expected [1] (5s > 1ms), got %v", sel)
	}
}

func TestFilterUUID(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeUUID},
	}
	b := batch.NewRecordBatch(schema, 3)
	b.Columns[0].SetValue(0, "550e8400-e29b-41d4-a716-446655440000")
	b.Columns[0].SetValue(1, "6ba7b810-9dad-11d1-80b4-00c04fd430c8")
	b.Columns[0].SetValue(2, "550e8400-e29b-41d4-a716-446655440000")

	// UUID comparison is on raw bytes stored in BytesData, so compare the raw string form
	// The kernel uses string comparison on the raw byte representation
	rawTarget := b.Columns[0].BytesData.StringValue(0) // get the raw form of the first UUID
	kern := ResolveFilterKernel(batch.TypeUUID, OpEq, rawTarget)
	if kern == nil {
		t.Fatal("ResolveFilterKernel returned nil for TypeUUID")
	}
	outSel := make([]uint16, 0, 3)
	sel := kern(b.Columns[0], nil, 3, outSel)
	if len(sel) != 2 {
		t.Fatalf("expected 2 matches, got %d", len(sel))
	}
	if sel[0] != 0 || sel[1] != 2 {
		t.Fatalf("expected indices [0, 2], got %v", sel)
	}
}

func TestFilterDate(t *testing.T) {
	schema := []parquet.Column{
		{Name: "event_date", Type: parquet.TypeDate},
	}
	b := batch.NewRecordBatch(schema, 3)
	b.Columns[0].SetValue(0, "2026-03-15")
	b.Columns[0].SetValue(1, "2026-01-01")
	b.Columns[0].SetValue(2, "2026-06-01")

	// Filter: date > 2026-01-01 (days since epoch)
	// 2026-01-01 = 20454 days from epoch
	targetDays := b.Columns[0].Int32Data[1] // get the int32 representation of 2026-01-01
	kern := ResolveFilterKernel(batch.TypeDate, OpGt, int64(targetDays))
	if kern == nil {
		t.Fatal("ResolveFilterKernel returned nil for TypeDate")
	}
	outSel := make([]uint16, 0, 3)
	sel := kern(b.Columns[0], nil, 3, outSel)
	if len(sel) != 2 {
		t.Fatalf("expected 2 matches (2026-03-15 and 2026-06-01 > 2026-01-01), got %d: %v", len(sel), sel)
	}
}

func TestFilterIPv4WithNulls(t *testing.T) {
	schema := []parquet.Column{
		{Name: "src_ip", Type: parquet.TypeIPv4, Nullable: true},
	}
	b := batch.NewRecordBatch(schema, 4)
	b.Columns[0].SetValue(0, "192.168.1.1")
	b.Columns[0].SetValue(1, nil)
	b.Columns[0].SetValue(2, "192.168.1.1")
	b.Columns[0].SetValue(3, nil)

	kern := ResolveFilterKernel(batch.TypeIPv4, OpEq, "192.168.1.1")
	if kern == nil {
		t.Fatal("ResolveFilterKernel returned nil for TypeIPv4")
	}
	outSel := make([]uint16, 0, 4)
	sel := kern(b.Columns[0], nil, 4, outSel)
	if len(sel) != 2 {
		t.Fatalf("expected 2 matches (nulls excluded), got %d: %v", len(sel), sel)
	}
}
