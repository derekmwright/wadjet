package batch

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
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
	b.Sel = []uint32{1, 3} // select only rows 1 and 3

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

func TestBatchPoolConcurrency(t *testing.T) {
	schema := []parquet.Column{
		{Name: "x", Type: parquet.TypeInt64},
		{Name: "y", Type: parquet.TypeFloat64},
	}

	pool := NewBatchPool(schema, 128)

	// Hammer Get/Put from multiple goroutines
	const goroutines = 8
	const iterations = 100
	done := make(chan struct{}, goroutines)

	for g := 0; g < goroutines; g++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for i := 0; i < iterations; i++ {
				b := pool.Get()
				if b == nil {
					t.Error("Get returned nil")
					return
				}
				if b.Len != 128 {
					t.Errorf("expected batch size 128, got %d", b.Len)
					return
				}
				// Simulate some work
				pool.Put(b)
			}
		}()
	}

	for i := 0; i < goroutines; i++ {
		<-done
	}
}

func TestGlobalPool(t *testing.T) {
	gp := NewGlobalPool()

	schema1 := []parquet.Column{
		{Name: "a", Type: parquet.TypeInt64},
	}
	schema2 := []parquet.Column{
		{Name: "b", Type: parquet.TypeString},
	}

	// Same schema and batch size should return the same pool
	pool1a := gp.ForSchema(schema1, 1024)
	pool1b := gp.ForSchema(schema1, 1024)
	if pool1a != pool1b {
		t.Fatal("ForSchema should return same pool for identical schema+size")
	}

	// Different schema should return a different pool
	pool2 := gp.ForSchema(schema2, 1024)
	if pool1a == pool2 {
		t.Fatal("ForSchema should return different pool for different schema")
	}

	// Different batch size should return a different pool
	pool1c := gp.ForSchema(schema1, 512)
	if pool1a == pool1c {
		t.Fatal("ForSchema should return different pool for different batch size")
	}

	// Verify the pool actually works
	b := pool1a.Get()
	if b == nil {
		t.Fatal("Get from global pool returned nil")
	}
	if b.Len != 1024 {
		t.Fatalf("expected batch size 1024, got %d", b.Len)
	}
	pool1a.Put(b)

	b2 := pool1a.Get()
	if b2 != b {
		t.Fatal("expected to get same batch back from pool")
	}
}

func TestGlobalPoolConcurrency(t *testing.T) {
	gp := NewGlobalPool()

	schemas := [][]parquet.Column{
		{{Name: "x", Type: parquet.TypeInt64}},
		{{Name: "y", Type: parquet.TypeFloat64}},
		{{Name: "z", Type: parquet.TypeString}},
	}

	const goroutines = 12
	const iterations = 50
	done := make(chan struct{}, goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer func() { done <- struct{}{} }()
			schema := schemas[id%len(schemas)]
			for i := 0; i < iterations; i++ {
				pool := gp.ForSchema(schema, 256)
				b := pool.Get()
				if b == nil {
					t.Error("Get returned nil")
					return
				}
				pool.Put(b)
			}
		}(g)
	}

	for i := 0; i < goroutines; i++ {
		<-done
	}
}

func TestBitmapGrow(t *testing.T) {
	bm := NewBitmap(4)
	bm.SetNull(1)

	bm = bm.Grow(8)
	if bm.Len() != 8 {
		t.Fatalf("expected len 8, got %d", bm.Len())
	}
	// Original null should be preserved
	if !bm.IsNull(1) {
		t.Fatal("expected bit 1 to still be null after grow")
	}
	// Original valid bits preserved
	if bm.IsNull(0) {
		t.Fatal("expected bit 0 to be valid after grow")
	}
	// New bits should be valid
	if bm.IsNull(5) {
		t.Fatal("expected new bit 5 to be valid after grow")
	}
}

func TestArrayVector(t *testing.T) {
	schema := []parquet.Column{
		{
			Name: "tags",
			Type: parquet.TypeArray,
			ElementType: &parquet.Column{
				Name: "element", Type: parquet.TypeString,
			},
		},
	}

	b := NewRecordBatch(schema, 3)
	v := b.Columns[0]

	// Set array values
	v.SetValue(0, []any{"hello", "world"})
	v.SetValue(1, []any{"foo"})
	v.SetValue(2, []any{})

	// Verify
	got0 := v.GetValue(0).([]any)
	if len(got0) != 2 || got0[0] != "hello" || got0[1] != "world" {
		t.Fatalf("row 0: expected [hello, world], got %v", got0)
	}

	got1 := v.GetValue(1).([]any)
	if len(got1) != 1 || got1[0] != "foo" {
		t.Fatalf("row 1: expected [foo], got %v", got1)
	}

	got2 := v.GetValue(2).([]any)
	if len(got2) != 0 {
		t.Fatalf("row 2: expected [], got %v", got2)
	}
}

func TestRowVector(t *testing.T) {
	schema := []parquet.Column{
		{
			Name: "person",
			Type: parquet.TypeRow,
			Fields: []parquet.Column{
				{Name: "name", Type: parquet.TypeString},
				{Name: "age", Type: parquet.TypeInt64},
			},
		},
	}

	b := NewRecordBatch(schema, 2)
	v := b.Columns[0]

	v.SetValue(0, map[string]any{"name": "Alice", "age": int64(30)})
	v.SetValue(1, map[string]any{"name": "Bob", "age": int64(25)})

	got0 := v.GetValue(0).(map[string]any)
	if got0["name"] != "Alice" || got0["age"] != int64(30) {
		t.Fatalf("row 0: expected {name:Alice, age:30}, got %v", got0)
	}

	got1 := v.GetValue(1).(map[string]any)
	if got1["name"] != "Bob" || got1["age"] != int64(25) {
		t.Fatalf("row 1: expected {name:Bob, age:25}, got %v", got1)
	}
}

func TestArrayVectorNull(t *testing.T) {
	v := NewArrayVector(3, TypeInt64)
	v.SetValue(0, []any{int64(1), int64(2)})
	v.SetValue(1, nil) // null array
	v.SetValue(2, []any{int64(3)})

	if v.GetValue(1) != nil {
		t.Fatalf("expected nil for null array, got %v", v.GetValue(1))
	}

	got0 := v.GetValue(0).([]any)
	if len(got0) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(got0))
	}
}

func TestResolveColumn(t *testing.T) {
	tests := []struct {
		typeStr string
		wantType parquet.TypeID
		wantElem bool
		wantFields int
	}{
		{"INT64", parquet.TypeInt64, false, 0},
		{"STRING", parquet.TypeString, false, 0},
		{"ARRAY(INT64)", parquet.TypeArray, true, 0},
		{"ARRAY(STRING)", parquet.TypeArray, true, 0},
		{"ROW(name STRING, age INT64)", parquet.TypeRow, false, 2},
		{"MAP(STRING, INT64)", parquet.TypeMap, true, 0},
	}

	for _, tt := range tests {
		t.Run(tt.typeStr, func(t *testing.T) {
			col, err := parquet.ResolveColumn("test", tt.typeStr)
			if err != nil {
				t.Fatalf("ResolveColumn(%q) error: %v", tt.typeStr, err)
			}
			if col.Type != tt.wantType {
				t.Fatalf("type = %v, want %v", col.Type, tt.wantType)
			}
			if tt.wantElem && col.ElementType == nil {
				t.Fatal("expected ElementType to be set")
			}
			if tt.wantFields > 0 && len(col.Fields) != tt.wantFields {
				t.Fatalf("fields = %d, want %d", len(col.Fields), tt.wantFields)
			}
		})
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

	// Test int64 input (Parquet round-trip: values read back from storage are int64, not strings)
	t.Run("IPv4_Int64Roundtrip", func(t *testing.T) {
		v := NewVector(TypeIPv4, 2)
		v.SetValue(0, "192.168.1.100")
		raw := v.Int64Data[0] // capture the encoded int64
		v2 := NewVector(TypeIPv4, 1)
		v2.SetValue(0, raw) // set from int64, as Parquet reader would
		if got := v2.GetValue(0); got != "192.168.1.100" {
			t.Fatalf("IPv4 int64 roundtrip: expected 192.168.1.100, got %v", got)
		}
	})

	t.Run("MAC_Int64Roundtrip", func(t *testing.T) {
		v := NewVector(TypeMAC, 2)
		v.SetValue(0, "aa:bb:cc:dd:ee:ff")
		raw := v.Int64Data[0] // capture the encoded int64
		v2 := NewVector(TypeMAC, 1)
		v2.SetValue(0, raw) // set from int64, as Parquet reader would
		if got := v2.GetValue(0); got != "aa:bb:cc:dd:ee:ff" {
			t.Fatalf("MAC int64 roundtrip: expected aa:bb:cc:dd:ee:ff, got %v", got)
		}
	})

	t.Run("FromRows_Int64NetworkTypes", func(t *testing.T) {
		// Simulate what happens when Parquet reader returns int64 values
		// for MAC/IPv4 columns (the actual round-trip scenario)
		schema := []parquet.Column{
			{Name: "ip", Type: TypeIPv4},
			{Name: "mac", Type: TypeMAC},
		}
		// Encode known values
		v := NewVector(TypeIPv4, 1)
		v.SetValue(0, "10.0.0.1")
		ipEncoded := v.Int64Data[0]

		vm := NewVector(TypeMAC, 1)
		vm.SetValue(0, "de:ad:be:ef:ca:fe")
		macEncoded := vm.Int64Data[0]

		// FromRows with int64 values (as Parquet reader would provide)
		rows := []map[string]any{
			{"ip": ipEncoded, "mac": macEncoded},
		}
		b := FromRows(schema, rows)
		result := b.ToRows()
		if len(result) != 1 {
			t.Fatalf("expected 1 row, got %d", len(result))
		}
		if result[0]["ip"] != "10.0.0.1" {
			t.Errorf("expected ip 10.0.0.1, got %v", result[0]["ip"])
		}
		if result[0]["mac"] != "de:ad:be:ef:ca:fe" {
			t.Errorf("expected mac de:ad:be:ef:ca:fe, got %v", result[0]["mac"])
		}
	})
}
