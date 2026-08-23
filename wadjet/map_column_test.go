package wadjet

import (
	"context"
	"fmt"
	"testing"
)

// TestMapColumnIsReadable is #393's gate: three shapes of read that each
// killed the process, plus the values they must return.
func TestMapColumnIsReadable(t *testing.T) {
	ctx := context.Background()
	db := mbOpen(t)
	src := mbData(mbRows)

	t.Run("projection", func(t *testing.T) {
		res, err := db.Query(ctx, "SELECT id, c_map AS v FROM mbtypes WHERE id % 331 = 7 ORDER BY id")
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if len(res.Rows) == 0 {
			t.Fatal("no rows")
		}
		for _, r := range res.Rows {
			id := int(r["id"].(int64))
			mbAssertEqual(t, fmt.Sprintf("c_map at id=%d", id), r["v"], mbWantMap(src[id]["c_map"]))
		}
	})

	t.Run("select_star", func(t *testing.T) {
		res, err := db.Query(ctx, "SELECT * FROM mbtypes WHERE id % 743 = 5 ORDER BY id")
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if len(res.Rows) == 0 {
			t.Fatal("no rows")
		}
		for _, r := range res.Rows {
			id := int(r["id"].(int64))
			mbAssertEqual(t, fmt.Sprintf("c_map at id=%d", id), r["c_map"], mbWantMap(src[id]["c_map"]))
		}
	})

	// Every MAP shape, checked where it lands rather than where it is
	// convenient: rows 0..3 carry the empty map, the single entry, the two
	// entries and the NULL value in that order.
	t.Run("every_shape", func(t *testing.T) {
		res, err := db.Query(ctx, "SELECT id, c_map AS v FROM mbtypes WHERE id < 8 ORDER BY id")
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if len(res.Rows) != 8 {
			t.Fatalf("rows = %d, want 8", len(res.Rows))
		}
		for _, r := range res.Rows {
			id := int(r["id"].(int64))
			mbAssertEqual(t, fmt.Sprintf("c_map at id=%d", id), r["v"], mbWantMap(src[id]["c_map"]))
		}
	})
}

// mbWantMap is the vector's MAP shape for a source row's Go map: the entry
// list GetValue hands back, in key order. NULL stays NULL and an EMPTY map
// stays an empty list — the two must not collapse into each other.
func mbWantMap(v any) any {
	m, ok := v.(map[string]any)
	if !ok || v == nil {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	// insertion-order-independent: sort
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	out := make([]any, 0, len(keys))
	for _, k := range keys {
		out = append(out, map[string]any{"key": k, "value": m[k]})
	}
	return out
}
