package embedding

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestVoyageEmbed(t *testing.T) {
	var lastReq voyageRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("bad auth header: %s", r.Header.Get("Authorization"))
		}
		json.NewDecoder(r.Body).Decode(&lastReq)

		resp := voyageResponse{Data: make([]voyageEmbedding, len(lastReq.Input))}
		for i := range lastReq.Input {
			vec := make([]float64, 4)
			for j := range vec {
				vec[j] = float64(i)*0.1 + float64(j)*0.01
			}
			resp.Data[i] = voyageEmbedding{Index: i, Embedding: vec}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	cache := NewCache(100)
	provider := NewVoyage(VoyageConfig{
		APIKey:    "test-key",
		Model:     "voyage-3.5",
		InputType: "document",
		BaseURL:   server.URL,
	}, cache)

	if provider.Dimension() != 1024 {
		t.Errorf("default voyage-3.5 dim = %d, want 1024", provider.Dimension())
	}

	// Batch of 3 → single API call.
	vecs, err := provider.Embed([]string{"a", "b", "c"})
	if err != nil {
		t.Fatal(err)
	}
	if len(vecs) != 3 || len(vecs[0]) != 4 {
		t.Fatalf("got %d vecs of dim %d, want 3 of 4", len(vecs), len(vecs[0]))
	}
	if lastReq.InputType != "document" {
		t.Errorf("input_type not propagated: %q", lastReq.InputType)
	}

	// Cache hit: re-embedding "a" must not change the value.
	again, err := provider.Embed([]string{"a"})
	if err != nil {
		t.Fatal(err)
	}
	if again[0][1] != vecs[0][1] {
		t.Errorf("cache returned %v, want %v", again[0], vecs[0])
	}
}

func TestVoyageAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(voyageResponse{
			Error: &voyageError{Message: "invalid api key", Type: "auth"},
		})
	}))
	defer server.Close()

	provider := NewVoyage(VoyageConfig{APIKey: "bad", BaseURL: server.URL}, NewCache(10))
	_, err := provider.Embed([]string{"hello"})
	if err == nil {
		t.Fatal("expected error for bad API key")
	}
	if err.Error() != "Voyage API error (401): invalid api key" {
		t.Errorf("unexpected error: %v", err)
	}
}
