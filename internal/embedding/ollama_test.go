package embedding

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestOllamaEmbed(t *testing.T) {
	var lastReq ollamaRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/embed" {
			t.Errorf("expected /api/embed, got %s", r.URL.Path)
		}
		json.NewDecoder(r.Body).Decode(&lastReq)

		resp := ollamaResponse{
			Model:      lastReq.Model,
			Embeddings: make([][]float32, len(lastReq.Input)),
		}
		for i := range lastReq.Input {
			vec := make([]float32, 3)
			for j := range vec {
				vec[j] = float32(i)*0.1 + float32(j)*0.01
			}
			resp.Embeddings[i] = vec
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	provider := NewOllama(OllamaConfig{Model: "nomic-embed-text", BaseURL: server.URL}, NewCache(100))

	if provider.Dimension() != 768 {
		t.Errorf("default nomic-embed-text dim = %d, want 768", provider.Dimension())
	}

	vecs, err := provider.Embed([]string{"a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	if len(vecs) != 2 || len(vecs[0]) != 3 {
		t.Fatalf("got %d vecs of dim %d, want 2 of 3", len(vecs), len(vecs[0]))
	}

	// Dimension self-syncs to what the model actually returned.
	if provider.Dimension() != 3 {
		t.Errorf("dim after call = %d, want 3 (synced from response)", provider.Dimension())
	}
}

func TestOllamaAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(ollamaResponse{Error: "model not found"})
	}))
	defer server.Close()

	provider := NewOllama(OllamaConfig{Model: "missing", BaseURL: server.URL}, NewCache(10))
	_, err := provider.Embed([]string{"hello"})
	if err == nil {
		t.Fatal("expected error for missing model")
	}
	if err.Error() != "Ollama API error (404): model not found" {
		t.Errorf("unexpected error: %v", err)
	}
}
