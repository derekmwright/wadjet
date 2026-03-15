package batch

import (
	"testing"

	"github.com/derekmwright/caelum/internal/storage/parquet"
)

func TestBitmap(t *testing.T) {
	bm := NewBitmap(100)
	if bm.NullCount() != 0 {
		t.Fatalf("expected 0 nulls, got %d", bm.NullCount())
	}

	bm.SetNull(5)
	bm.SetNull(99)
	if bm.NullCount() != 2 {
		t.Fatalf("expected 2 nulls, got %d", bm.NullCount())
	}
	if !bm.IsNull(5) {
		t.Fatal("expected bit 5 to be null")
	}
	if bm.IsNull(6) {
		t.Fatal("expected bit 6 to be non-null")
	}

	bm.SetValid(5)
	if bm.IsNull(5) {
		t.Fatal("expected bit 5 to be valid after SetValid")
	}
}

func TestBytesColumn(t *testing.T) {
	bc := NewBytesColumn(3)
	bc.Set(0, []byte("hello"))
	bc.Set(1, []byte("world"))
	bc.Set(2, []byte("!"))

	if bc.Len() != 3 {
		t.Fatalf("expected len 3, got %d", bc.Len())
	}
	if bc.StringValue(0) != "hello" {
		t.Fatalf("expected 'hello', got %q", bc.StringValue(0))
	}
	if bc.StringValue(1) != "world" {
		t.Fatalf("expected 'world', got %q", bc.StringValue(1))
	}
}

func TestRecordBatch_FromRows(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString},
		{Name: "score", Type: parquet.TypeFloat64},
	}

	rows := []map[string]any{
		{"id": int64(1), "name": "alice", "score": 95.5},
		{"id": int64(2), "name": "bob", "score": 87.0},
		{"id": int64(3), "name": "carol", "score": nil},
	}

	b := FromRows(schema, rows)
	if b.Len != 3 {
		t.Fatalf("expected 3 rows, got %d", b.Len)
	}
	if b.ActiveLen() != 3 {
		t.Fatalf("expected 3 active rows, got %d", b.ActiveLen())
	}

	// Test column access
	idVec := b.ColumnByName("id")
	if idVec == nil {
		t.Fatal("expected 'id' column")
	}
	if idVec.GetValue(0).(int64) != 1 {
		t.Fatalf("expected id=1, got %v", idVec.GetValue(0))
	}

	// Test ToRows round-trip
	back := b.ToRows()
	if len(back) != 3 {
		t.Fatalf("expected 3 rows back, got %d", len(back))
	}
}

func TestRecordBatch_SelectionVector(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeInt64},
	}

	rows := []map[string]any{
		{"val": int64(10)},
		{"val": int64(20)},
		{"val": int64(30)},
		{"val": int64(40)},
	}

	b := FromRows(schema, rows)
	b.Sel = []uint16{1, 3} // select only rows 1 and 3

	if b.ActiveLen() != 2 {
		t.Fatalf("expected 2 active rows, got %d", b.ActiveLen())
	}

	back := b.ToRows()
	if len(back) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(back))
	}
	if back[0]["val"].(int64) != 20 {
		t.Fatalf("expected val=20, got %v", back[0]["val"])
	}
	if back[1]["val"].(int64) != 40 {
		t.Fatalf("expected val=40, got %v", back[1]["val"])
	}
}

func TestBatchPool(t *testing.T) {
	schema := []parquet.Column{
		{Name: "x", Type: parquet.TypeInt64},
	}

	pool := NewBatchPool(schema, DefaultBatchSize)

	b1 := pool.Get()
	if b1.Len != DefaultBatchSize {
		t.Fatalf("expected batch size %d, got %d", DefaultBatchSize, b1.Len)
	}

	pool.Put(b1)
	b2 := pool.Get()
	if b2 != b1 {
		t.Fatal("expected to get same batch from pool")
	}
}

func TestVectorNetworkTypes(t *testing.T) {
	// Test TypeIPv4
	t.Run("IPv4", func(t *testing.T) {
		v := NewVector(TypeIPv4, 3)
		v.SetValue(0, "192.168.1.1")
		v.SetValue(1, "10.0.0.1")
		v.SetValue(2, "255.255.255.255")

		if got := v.GetValue(0); got != "192.168.1.1" {
			t.Fatalf("expected 192.168.1.1, got %v", got)
		}
		if got := v.GetValue(1); got != "10.0.0.1" {
			t.Fatalf("expected 10.0.0.1, got %v", got)
		}
		if got := v.GetValue(2); got != "255.255.255.255" {
			t.Fatalf("expected 255.255.255.255, got %v", got)
		}
	})

	// Test TypeCIDR
	t.Run("CIDR", func(t *testing.T) {
		v := NewVector(TypeCIDR, 2)
		v.SetValue(0, "192.168.1.0/24")
		v.SetValue(1, "10.0.0.0/8")

		if got := v.GetValue(0); got != "192.168.1.0/24" {
			t.Fatalf("expected 192.168.1.0/24, got %v", got)
		}
		if got := v.GetValue(1); got != "10.0.0.0/8" {
			t.Fatalf("expected 10.0.0.0/8, got %v", got)
		}
	})

	// Test TypeMAC
	t.Run("MAC", func(t *testing.T) {
		v := NewVector(TypeMAC, 2)
		v.SetValue(0, "aa:bb:cc:dd:ee:ff")
		v.SetValue(1, "00:11:22:33:44:55")

		if got := v.GetValue(0); got != "aa:bb:cc:dd:ee:ff" {
			t.Fatalf("expected aa:bb:cc:dd:ee:ff, got %v", got)
		}
		if got := v.GetValue(1); got != "00:11:22:33:44:55" {
			t.Fatalf("expected 00:11:22:33:44:55, got %v", got)
		}
	})

	// Test null handling
	t.Run("Null", func(t *testing.T) {
		v := NewVector(TypeIPv4, 2)
		v.SetValue(0, nil)
		v.SetValue(1, "1.2.3.4")

		if got := v.GetValue(0); got != nil {
			t.Fatalf("expected nil, got %v", got)
		}
		if got := v.GetValue(1); got != "1.2.3.4" {
			t.Fatalf("expected 1.2.3.4, got %v", got)
		}
	})
}
