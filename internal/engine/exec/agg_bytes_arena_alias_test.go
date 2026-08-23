package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// recycleBacking replays what every output-backing reuse in the engine does
// to a batch a producer is about to write again: Project's BatchPool handing
// a batch back after Pipeline.runSerial's Release, the join emit path's
// vector reuse (69aecbb / ADR-0016) and scan.BackingPool.get for a row group
// (docs/design/scan-output-backing-reuse.md). ResetForWrite empties the BYTES
// arena while RETAINING its capacity, so the next writes of the same total
// length land on the very same bytes — which is the whole point of the reuse
// and the reason an alias handed out earlier is not merely stale but
// rewritten.
func recycleBacking(tb testing.TB, b *batch.RecordBatch, n int) {
	tb.Helper()
	for _, c := range b.Columns {
		c.ResetForWrite(n)
	}
	b.Len = n
	b.Sel = nil
}

// TestMinByOverBytesSurvivesBackingReuse is the regression for
// Vector.GetValue's TypeBytes arm returning bc.Data[start:end] — a live alias
// into the producer's arena — where every sibling arm returns a value that
// owns its storage. minMaxByState.bestVal keeps that box for the lifetime of
// the aggregate and neither Detaches the batch nor Claims the column, so the
// producer is entitled to write over it: MIN_BY over a BYTES column answered
// with the LAST batch's bytes at the min's row position instead of the min's.
func TestMinByOverBytesSurvivesBackingReuse(t *testing.T) {
	schema := []parquet.Column{
		{Name: "g", Type: parquet.TypeString},
		{Name: "payload", Type: parquet.TypeBytes},
		{Name: "k", Type: parquet.TypeFloat64},
	}
	// Equal lengths so the second write lands exactly on the first's bytes.
	const (
		best  = "AAAABBBB"
		later = "ZZZZYYYY"
	)

	agg := NewHashAggregate([]string{"g"}, []AggColumn{
		{Func: AggMinBy, InputCol: "payload", InputCol2: "k", OutputCol: "best", OutputType: parquet.TypeBytes},
		{Func: AggMaxBy, InputCol: "payload", InputCol2: "k", OutputCol: "worst", OutputType: parquet.TypeBytes},
	})
	ctx := context.Background()
	if err := agg.Init(ctx); err != nil {
		t.Fatal(err)
	}
	defer agg.Close()

	b := batch.NewRecordBatch(schema, 2)
	fill := func(payload0, payload1 string, k0, k1 float64) {
		b.Columns[0].SetValue(0, "a")
		b.Columns[0].SetValue(1, "a")
		b.Columns[1].SetValue(0, []byte(payload0))
		b.Columns[1].SetValue(1, []byte(payload1))
		b.Columns[2].SetValue(0, k0)
		b.Columns[2].SetValue(1, k1)
	}

	// Row group 1 carries both the min (k=1) and the max (k=4) of the query.
	fill(best, "MMMMMMMM", 1, 4)
	if err := agg.Consume(ctx, b); err != nil {
		t.Fatal(err)
	}

	// Row group 2 reuses the same backing and cannot displace either extreme
	// (2 and 3 sit strictly between 1 and 4), so both answers must still be
	// the bytes row group 1 held.
	recycleBacking(t, b, 2)
	fill(later, later, 2, 3)
	if err := agg.Consume(ctx, b); err != nil {
		t.Fatal(err)
	}

	if err := agg.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	out, err := agg.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if out == nil || out.Len != 1 {
		t.Fatalf("expected 1 group, got %v", out)
	}
	rows := out.ToRows()
	if got := textOf(t, rows[0]["best"]); got != best {
		t.Errorf("MIN_BY over BYTES = %q, want %q — GetValue handed out an alias into the producer's arena and the next write went over it", got, best)
	}
	if got := textOf(t, rows[0]["worst"]); got != "MMMMMMMM" {
		t.Errorf("MAX_BY over BYTES = %q, want %q", got, "MMMMMMMM")
	}
}

// textOf reads a BYTES-derived output value. MIN_BY/MAX_BY over a BYTES
// column declare a STRING output (minMaxOutputType), while a BYTES group key
// keeps its own type, so the boxed value is one or the other.
func textOf(tb testing.TB, v any) string {
	tb.Helper()
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	}
	tb.Fatalf("value is %T, want string or []byte", v)
	return ""
}

// TestBytesGroupKeySurvivesBackingReuse is the second retention site of the
// same alias: groupStateExtras.keyValues (and h.keys) box the group key with
// GetValue and emit it at Finalize. The hash side is safe — appendColumnValue
// copies into keyBuf and compactKeys stores a string — so the groups stay
// DISTINCT while the emitted key COLUMN reads whatever the arena holds at
// finalize time: two rows, both labelled with the last row group's bytes.
func TestBytesGroupKeySurvivesBackingReuse(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeBytes},
		{Name: "v", Type: parquet.TypeInt64},
	}
	// COUNT(DISTINCT) forces the generic per-row path (processRow), which is
	// where the group key is BOXED at consume into extras.keyValues. The
	// vectorized paths defer boxing and reconstruct the key from
	// serializedKeys at emit, so they never hold the alias.
	agg := NewHashAggregate([]string{"k"}, []AggColumn{
		{Func: AggCountDistinct, InputCol: "v", OutputCol: "total", OutputType: parquet.TypeInt64},
	})
	ctx := context.Background()
	if err := agg.Init(ctx); err != nil {
		t.Fatal(err)
	}
	defer agg.Close()

	b := batch.NewRecordBatch(schema, 1)
	b.Columns[0].SetValue(0, []byte("AAAAAAAAAAAA"))
	b.Columns[1].SetValue(0, int64(1))
	if err := agg.Consume(ctx, b); err != nil {
		t.Fatal(err)
	}

	recycleBacking(t, b, 1)
	b.Columns[0].SetValue(0, []byte("ZZZZZZZZZZZZ"))
	b.Columns[1].SetValue(0, int64(2))
	if err := agg.Consume(ctx, b); err != nil {
		t.Fatal(err)
	}

	if err := agg.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	seen := map[string]int64{}
	for {
		out, err := agg.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if out == nil || out.Len == 0 {
			break
		}
		for _, row := range out.ToRows() {
			seen[textOf(t, row["k"])] = row["total"].(int64)
		}
	}
	// Both groups hold one row, so both counts are 1; the KEYS are what the
	// alias corrupted — pre-fix the first group emitted the second row
	// group's bytes and the two groups collapsed into one output label.
	if len(seen) != 2 || seen["AAAAAAAAAAAA"] != 1 || seen["ZZZZZZZZZZZZ"] != 1 {
		t.Errorf("BYTES group keys after backing reuse = %v, want map[AAAAAAAAAAAA:1 ZZZZZZZZZZZZ:1] — keyValues kept an arena alias", seen)
	}
}
