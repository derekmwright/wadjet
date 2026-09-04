package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func TestHashJoin_DualIntKey(t *testing.T) {
	buildSchema := []parquet.Column{
		{Name: "ps_partkey", Type: parquet.TypeInt64},
		{Name: "ps_suppkey", Type: parquet.TypeInt64},
		{Name: "ps_value", Type: parquet.TypeFloat64},
	}
	buildRows := []map[string]any{
		{"ps_partkey": int64(1), "ps_suppkey": int64(10), "ps_value": 100.0},
		{"ps_partkey": int64(2), "ps_suppkey": int64(20), "ps_value": 200.0},
		{"ps_partkey": int64(3), "ps_suppkey": int64(30), "ps_value": 300.0},
	}

	probeSchema := []parquet.Column{
		{Name: "l_partkey", Type: parquet.TypeInt64},
		{Name: "l_suppkey", Type: parquet.TypeInt64},
		{Name: "l_qty", Type: parquet.TypeInt64},
	}
	probeRows := []map[string]any{
		{"l_partkey": int64(1), "l_suppkey": int64(10), "l_qty": int64(5)},
		{"l_partkey": int64(2), "l_suppkey": int64(20), "l_qty": int64(10)},
		{"l_partkey": int64(99), "l_suppkey": int64(99), "l_qty": int64(1)},
	}

	hj := NewHashJoin(InnerJoin, []string{"l_partkey", "l_suppkey"}, []string{"ps_partkey", "ps_suppkey"})
	buildSrc := NewSliceSource(buildSchema, buildRows)
	if err := hj.Build(context.Background(), buildSrc); err != nil {
		t.Fatal(err)
	}
	hj.FixKeyAssignment()

	if !hj.useDualIntKey {
		t.Fatal("expected useDualIntKey=true")
	}
	keys := 0
	for i := range hj.parts {
		if t := hj.parts[i].ints; t != nil {
			keys += t.Len()
		}
	}
	if keys != 3 {
		t.Fatalf("expected 3 index entries, got %d", keys)
	}

	probe := hj.Probe()
	probe.Init(context.Background())

	probeBatch := batch.FromRows(probeSchema, probeRows)

	out, err := probe.Execute(context.Background(), probeBatch)
	if err != nil {
		t.Fatal(err)
	}
	if out == nil {
		t.Fatal("expected output batch, got nil")
	}
	rows := out.ToRows()
	if len(rows) != 2 {
		t.Fatalf("expected 2 matches, got %d", len(rows))
	}

	// Verify matched keys
	for i, row := range rows {
		pk := row["l_partkey"].(int64)
		sk := row["l_suppkey"].(int64)
		bpk := row["ps_partkey"].(int64)
		bsk := row["ps_suppkey"].(int64)
		if pk != bpk || sk != bsk {
			t.Errorf("row %d: probe keys (%d,%d) != build keys (%d,%d)", i, pk, sk, bpk, bsk)
		}
	}
}

func TestHashJoin_DualIntKey_SemiAnti(t *testing.T) {
	buildSchema := []parquet.Column{
		{Name: "ps_partkey", Type: parquet.TypeInt64},
		{Name: "ps_suppkey", Type: parquet.TypeInt64},
	}
	buildRows := []map[string]any{
		{"ps_partkey": int64(1), "ps_suppkey": int64(10)},
		{"ps_partkey": int64(2), "ps_suppkey": int64(20)},
	}

	probeSchema := []parquet.Column{
		{Name: "l_partkey", Type: parquet.TypeInt64},
		{Name: "l_suppkey", Type: parquet.TypeInt64},
	}
	probeRows := []map[string]any{
		{"l_partkey": int64(1), "l_suppkey": int64(10)},
		{"l_partkey": int64(3), "l_suppkey": int64(30)},
		{"l_partkey": int64(2), "l_suppkey": int64(20)},
	}

	t.Run("semi", func(t *testing.T) {
		hj := NewHashJoin(SemiJoin, []string{"l_partkey", "l_suppkey"}, []string{"ps_partkey", "ps_suppkey"})
		hj.SemiAntiKeyOnly = true
		buildSrc := NewSliceSource(buildSchema, buildRows)
		if err := hj.Build(context.Background(), buildSrc); err != nil {
			t.Fatal(err)
		}
		probe := hj.Probe()
		probe.Init(context.Background())
		out, err := probe.Execute(context.Background(), batch.FromRows(probeSchema, probeRows))
		if err != nil {
			t.Fatal(err)
		}
		if out == nil {
			t.Fatal("expected output for semi join")
		}
		if out.ActiveLen() != 2 {
			t.Fatalf("semi join: expected 2 rows, got %d", out.ActiveLen())
		}
	})

	t.Run("anti", func(t *testing.T) {
		hj := NewHashJoin(AntiJoin, []string{"l_partkey", "l_suppkey"}, []string{"ps_partkey", "ps_suppkey"})
		hj.SemiAntiKeyOnly = true
		buildSrc := NewSliceSource(buildSchema, buildRows)
		if err := hj.Build(context.Background(), buildSrc); err != nil {
			t.Fatal(err)
		}
		probe := hj.Probe()
		probe.Init(context.Background())
		out, err := probe.Execute(context.Background(), batch.FromRows(probeSchema, probeRows))
		if err != nil {
			t.Fatal(err)
		}
		if out == nil {
			t.Fatal("expected output for anti join")
		}
		if out.ActiveLen() != 1 {
			t.Fatalf("anti join: expected 1 row, got %d", out.ActiveLen())
		}
	})
}
