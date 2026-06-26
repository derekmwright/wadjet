package embedding

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// VoyageConfig configures the Voyage AI embedding provider. Voyage AI is
// Anthropic's officially recommended embeddings provider — Anthropic does not
// serve a native embeddings endpoint, so "Anthropic embeddings" means Voyage.
// See https://docs.claude.com/en/docs/build-with-claude/embeddings.
type VoyageConfig struct {
	APIKey     string // required
	Model      string // default: voyage-3.5
	Dimensions int    // 0 = model default (1024 for voyage-3.5 / voyage-3-large)
	InputType  string // optional: "document" or "query" — improves retrieval quality
	BaseURL    string // default: https://api.voyageai.com/v1
}

// Voyage implements Provider using the Voyage AI embeddings API.
type Voyage struct {
	config VoyageConfig
	client *http.Client
	cache  *Cache
	dim    int
}

// NewVoyage creates a Voyage AI embedding provider.
func NewVoyage(cfg VoyageConfig, cache *Cache) *Voyage {
	if cfg.Model == "" {
		cfg.Model = "voyage-3.5"
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://api.voyageai.com/v1"
	}

	dim := cfg.Dimensions
	if dim <= 0 {
		switch cfg.Model {
		case "voyage-3-lite":
			dim = 512
		case "voyage-code-2", "voyage-2":
			dim = 1536
		default:
			// voyage-3.5, voyage-3.5-lite, voyage-3-large, voyage-3
			dim = 1024
		}
	}

	return &Voyage{
		config: cfg,
		client: &http.Client{Timeout: 30 * time.Second},
		cache:  cache,
		dim:    dim,
	}
}

func (v *Voyage) Model() string  { return v.config.Model }
func (v *Voyage) Dimension() int { return v.dim }

// Embed returns embeddings for the given texts, using cache where possible.
// Uncached texts are batched into a single API call.
func (v *Voyage) Embed(texts []string) ([][]float32, error) {
	results := make([][]float32, len(texts))

	var misses []int
	for i, text := range texts {
		if cached := v.cache.Get(v.config.Model, text); cached != nil {
			results[i] = cached
		} else {
			misses = append(misses, i)
		}
	}

	if len(misses) == 0 {
		return results, nil
	}

	missTexts := make([]string, len(misses))
	for i, idx := range misses {
		missTexts[i] = texts[idx]
	}

	vectors, err := v.callAPI(missTexts)
	if err != nil {
		return nil, err
	}

	for i, idx := range misses {
		if i < len(vectors) {
			results[idx] = vectors[i]
			v.cache.Put(v.config.Model, texts[idx], vectors[i])
		}
	}

	return results, nil
}

type voyageRequest struct {
	Model           string   `json:"model"`
	Input           []string `json:"input"`
	InputType       string   `json:"input_type,omitempty"`
	OutputDimension int      `json:"output_dimension,omitempty"`
}

type voyageResponse struct {
	Data  []voyageEmbedding `json:"data"`
	Error *voyageError      `json:"error,omitempty"`
	// Voyage also returns top-level "detail" on some error shapes.
	Detail string `json:"detail,omitempty"`
}

type voyageEmbedding struct {
	Index     int       `json:"index"`
	Embedding []float64 `json:"embedding"`
}

type voyageError struct {
	Message string `json:"message"`
	Type    string `json:"type"`
}

func (v *Voyage) callAPI(texts []string) ([][]float32, error) {
	reqBody := voyageRequest{
		Model:     v.config.Model,
		Input:     texts,
		InputType: v.config.InputType,
	}
	if v.config.Dimensions > 0 {
		reqBody.OutputDimension = v.config.Dimensions
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", v.config.BaseURL+"/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+v.config.APIKey)

	resp, err := v.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("API request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var errResp voyageResponse
		json.Unmarshal(respBody, &errResp)
		switch {
		case errResp.Error != nil && errResp.Error.Message != "":
			return nil, fmt.Errorf("Voyage API error (%d): %s", resp.StatusCode, errResp.Error.Message)
		case errResp.Detail != "":
			return nil, fmt.Errorf("Voyage API error (%d): %s", resp.StatusCode, errResp.Detail)
		default:
			return nil, fmt.Errorf("Voyage API error: status %d", resp.StatusCode)
		}
	}

	var apiResp voyageResponse
	if err := json.Unmarshal(respBody, &apiResp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	vectors := make([][]float32, len(texts))
	for _, emb := range apiResp.Data {
		if emb.Index < 0 || emb.Index >= len(vectors) {
			continue
		}
		vec := make([]float32, len(emb.Embedding))
		for j, f := range emb.Embedding {
			vec[j] = float32(f)
		}
		vectors[emb.Index] = vec
	}

	return vectors, nil
}
