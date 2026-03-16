package worker

import (
	"testing"
)

func TestResultStore_PutGet(t *testing.T) {
	rs := NewResultStore(1024)

	data := []byte("parquet-bytes-here")
	if !rs.Put("q1", "queries/q1/scan-0/task-1.parquet", data) {
		t.Fatal("expected Put to succeed")
	}

	got, ok := rs.Get("queries/q1/scan-0/task-1.parquet")
	if !ok {
		t.Fatal("expected Get to find result")
	}
	if string(got) != string(data) {
		t.Fatalf("got %q, want %q", got, data)
	}

	if rs.Count() != 1 {
		t.Fatalf("expected 1 result, got %d", rs.Count())
	}
	if rs.UsedBytes() != int64(len(data)) {
		t.Fatalf("expected %d used bytes, got %d", len(data), rs.UsedBytes())
	}
}

func TestResultStore_CapacityLimit(t *testing.T) {
	rs := NewResultStore(50)

	data1 := make([]byte, 30)
	data2 := make([]byte, 30)

	if !rs.Put("q1", "path1", data1) {
		t.Fatal("first Put should succeed")
	}
	if rs.Put("q1", "path2", data2) {
		t.Fatal("second Put should fail (exceeds capacity)")
	}

	// path1 should still be accessible
	if _, ok := rs.Get("path1"); !ok {
		t.Fatal("path1 should still be accessible")
	}
	// path2 should not
	if _, ok := rs.Get("path2"); ok {
		t.Fatal("path2 should not be stored")
	}
}

func TestResultStore_CleanupQuery(t *testing.T) {
	rs := NewResultStore(1024)

	rs.Put("q1", "path1", []byte("aaa"))
	rs.Put("q1", "path2", []byte("bbb"))
	rs.Put("q2", "path3", []byte("ccc"))

	if rs.Count() != 3 {
		t.Fatalf("expected 3 results, got %d", rs.Count())
	}

	rs.CleanupQuery("q1")

	if rs.Count() != 1 {
		t.Fatalf("expected 1 result after cleanup, got %d", rs.Count())
	}
	if _, ok := rs.Get("path1"); ok {
		t.Fatal("path1 should be cleaned up")
	}
	if _, ok := rs.Get("path3"); !ok {
		t.Fatal("path3 (q2) should still exist")
	}
	if rs.UsedBytes() != 3 {
		t.Fatalf("expected 3 used bytes, got %d", rs.UsedBytes())
	}
}

func TestResultStore_Disabled(t *testing.T) {
	rs := NewResultStore(0)

	if rs.Put("q1", "path", []byte("data")) {
		t.Fatal("Put should fail when maxBytes=0 (disabled)")
	}
}

func TestResultStore_DataIsolation(t *testing.T) {
	rs := NewResultStore(1024)

	original := []byte("original")
	rs.Put("q1", "path", original)

	// Mutate the original — stored copy should be unaffected
	original[0] = 'X'

	got, _ := rs.Get("path")
	if got[0] == 'X' {
		t.Fatal("result store should copy data, not hold reference")
	}
}
