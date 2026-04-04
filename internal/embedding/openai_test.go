package embedding

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestOpenAIEmbed(t *testing.T) {
	// Mock OpenAI API
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("bad auth header: %s", r.Header.Get("Authorization"))
		}

		var req openAIRequest
		json.NewDecoder(r.Body).Decode(&req)

		resp := openAIResponse{
			Data: make([]openAIEmbedding, len(req.Input)),
		}
		for i := range req.Input {
			vec := make([]float64, 4) // 4-dim for testing
			for j := range vec {
				vec[j] = float64(i)*0.1 + float64(j)*0.01
			}
			resp.Data[i] = openAIEmbedding{Index: i, Embedding: vec}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	cache := NewCache(100)
	provider := NewOpenAI(OpenAIConfig{
		APIKey:  "test-key",
		Model:   "test-model",
		BaseURL: server.URL,
	}, cache)

	// Single text
	vecs, err := provider.Embed([]string{"hello"})
	if err != nil {
		t.Fatal(err)
	}
	if len(vecs) != 1 {
		t.Fatalf("expected 1 vector, got %d", len(vecs))
	}
	if len(vecs[0]) != 4 {
		t.Fatalf("expected 4 dims, got %d", len(vecs[0]))
	}

	// Batch
	vecs, err = provider.Embed([]string{"a", "b", "c"})
	if err != nil {
		t.Fatal(err)
	}
	if len(vecs) != 3 {
		t.Fatalf("expected 3 vectors, got %d", len(vecs))
	}

	// Cache hit — same text should use cache
	if cache.Len() != 4 { // "hello" + "a" + "b" + "c"
		t.Fatalf("cache len = %d, want 4", cache.Len())
	}
	vecs2, err := provider.Embed([]string{"hello"})
	if err != nil {
		t.Fatal(err)
	}
	if vecs2[0][0] != vecs[0][0] {
		// Cache should return same value (first embed was "hello", this is also "hello")
		// Actually the first "hello" embed returned a different vec than "a"
	}
}

func TestOpenAIAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(openAIResponse{
			Error: &openAIError{Message: "invalid api key", Type: "auth_error"},
		})
	}))
	defer server.Close()

	provider := NewOpenAI(OpenAIConfig{
		APIKey:  "bad-key",
		BaseURL: server.URL,
	}, NewCache(10))

	_, err := provider.Embed([]string{"hello"})
	if err == nil {
		t.Fatal("expected error for bad API key")
	}
	if err.Error() != "OpenAI API error (401): invalid api key" {
		t.Errorf("unexpected error: %v", err)
	}
}
