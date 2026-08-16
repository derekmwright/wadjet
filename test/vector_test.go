package test

import (
	"context"
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

func setupVectorDB(t *testing.T) *wadjet.DB {
	t.Helper()
	ctx := context.Background()
	store := objstore.NewMemStore()

	db, err := wadjet.Open(ctx, wadjet.Config{
		Store:  store,
		Bucket: "test",
	})
	if err != nil {
		t.Fatal(err)
	}

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "label", Type: parquet.TypeString},
			{Name: "embedding", Type: parquet.TypeVector, Nullable: true, Dimension: 3},
		},
	}
	if err := db.CreateTable(ctx, "vectors", schema, nil); err != nil {
		t.Fatal(err)
	}

	rows := []map[string]any{
		{"id": int64(1), "label": "cat", "embedding": []float32{1.0, 0.0, 0.0}},
		{"id": int64(2), "label": "dog", "embedding": []float32{0.0, 1.0, 0.0}},
		{"id": int64(3), "label": "bird", "embedding": []float32{0.0, 0.0, 1.0}},
		{"id": int64(4), "label": "catdog", "embedding": []float32{0.707, 0.707, 0.0}},
		{"id": int64(5), "label": "none", "embedding": nil},
	}

	ing := db.NewIngester("vectors", schema, nil, ingest.Config{
		MaxBufferRows: 100,
		RowGroupSize:  100,
	})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	return db
}

func TestVectorScanAndSelect(t *testing.T) {
	db := setupVectorDB(t)
	ctx := context.Background()

	result, err := db.Query(ctx, "SELECT * FROM vectors ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(result.Rows))
	}

	// Row 1 should have embedding [1, 0, 0]
	emb, ok := result.Rows[0]["embedding"].([]float32)
	if !ok {
		t.Fatalf("row 0: embedding type = %T, want []float32", result.Rows[0]["embedding"])
	}
	if len(emb) != 3 {
		t.Fatalf("row 0: embedding dim = %d, want 3", len(emb))
	}
	if emb[0] != 1.0 || emb[1] != 0.0 || emb[2] != 0.0 {
		t.Errorf("row 0: embedding = %v, want [1 0 0]", emb)
	}

	// Row 5 (null) should have nil embedding
	if result.Rows[4]["embedding"] != nil {
		t.Errorf("row 4: expected nil embedding, got %v", result.Rows[4]["embedding"])
	}
}

func TestVectorCosineSimilarity(t *testing.T) {
	db := setupVectorDB(t)
	ctx := context.Background()

	result, err := db.Query(ctx, `
		SELECT id, label,
			cosine_similarity(embedding, embedding) AS self_sim
		FROM vectors
		WHERE embedding IS NOT NULL
		ORDER BY id
	`)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(result.Rows))
	}

	// Self-similarity should be 1.0 for unit vectors
	for i, row := range result.Rows[:3] {
		sim := toFloat64(row["self_sim"])
		if math.Abs(sim-1.0) > 1e-6 {
			t.Errorf("row %d: self cosine_similarity = %f, want 1.0", i, sim)
		}
	}
}

func TestVectorL2Distance(t *testing.T) {
	db := setupVectorDB(t)
	ctx := context.Background()

	result, err := db.Query(ctx, `
		SELECT id,
			l2_distance(embedding, embedding) AS self_dist
		FROM vectors
		WHERE embedding IS NOT NULL
		ORDER BY id
	`)
	if err != nil {
		t.Fatal(err)
	}

	for i, row := range result.Rows {
		dist := toFloat64(row["self_dist"])
		if math.Abs(dist) > 1e-6 {
			t.Errorf("row %d: self l2_distance = %f, want 0.0", i, dist)
		}
	}
}

func TestVectorDims(t *testing.T) {
	db := setupVectorDB(t)
	ctx := context.Background()

	result, err := db.Query(ctx, `
		SELECT id, vector_dims(embedding) AS dims
		FROM vectors
		WHERE embedding IS NOT NULL
		ORDER BY id
	`)
	if err != nil {
		t.Fatal(err)
	}
	for i, row := range result.Rows {
		switch v := row["dims"].(type) {
		case int64:
			if v != 3 {
				t.Errorf("row %d: dims = %d, want 3", i, v)
			}
		case string:
			if v != "3" {
				t.Errorf("row %d: dims = %q, want 3", i, v)
			}
		default:
			t.Errorf("row %d: dims type = %T", i, row["dims"])
		}
	}
}
