package embedding

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// OpenAIConfig configures the OpenAI embedding provider.
type OpenAIConfig struct {
	APIKey     string // required
	Model      string // default: text-embedding-3-small
	Dimensions int    // 0 = model default (1536 for small, 3072 for large)
	BaseURL    string // default: https://api.openai.com/v1
}

// OpenAI implements Provider using the OpenAI embeddings API.
type OpenAI struct {
	config OpenAIConfig
	client *http.Client
	cache  *Cache
	dim    int
}

// NewOpenAI creates an OpenAI embedding provider.
func NewOpenAI(cfg OpenAIConfig, cache *Cache) *OpenAI {
	if cfg.Model == "" {
		cfg.Model = "text-embedding-3-small"
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://api.openai.com/v1"
	}

	dim := cfg.Dimensions
	if dim <= 0 {
		switch cfg.Model {
		case "text-embedding-3-large":
			dim = 3072
		default:
			dim = 1536
		}
	}

	return &OpenAI{
		config: cfg,
		client: &http.Client{Timeout: 30 * time.Second},
		cache:  cache,
		dim:    dim,
	}
}

func (o *OpenAI) Model() string    { return o.config.Model }
func (o *OpenAI) Dimension() int   { return o.dim }

// Embed returns embeddings for the given texts, using cache where possible.
// Uncached texts are batched into a single API call.
func (o *OpenAI) Embed(texts []string) ([][]float32, error) {
	results := make([][]float32, len(texts))

	// Check cache first, collect misses
	var misses []int
	for i, text := range texts {
		if cached := o.cache.Get(o.config.Model, text); cached != nil {
			results[i] = cached
		} else {
			misses = append(misses, i)
		}
	}

	if len(misses) == 0 {
		return results, nil
	}

	// Batch API call for cache misses
	missTexts := make([]string, len(misses))
	for i, idx := range misses {
		missTexts[i] = texts[idx]
	}

	vectors, err := o.callAPI(missTexts)
	if err != nil {
		return nil, err
	}

	// Store results and update cache
	for i, idx := range misses {
		if i < len(vectors) {
			results[idx] = vectors[i]
			o.cache.Put(o.config.Model, texts[idx], vectors[i])
		}
	}

	return results, nil
}

type openAIRequest struct {
	Model      string   `json:"model"`
	Input      []string `json:"input"`
	Dimensions int      `json:"dimensions,omitempty"`
}

type openAIResponse struct {
	Data  []openAIEmbedding `json:"data"`
	Error *openAIError      `json:"error,omitempty"`
}

type openAIEmbedding struct {
	Index     int       `json:"index"`
	Embedding []float64 `json:"embedding"`
}

type openAIError struct {
	Message string `json:"message"`
	Type    string `json:"type"`
}

func (o *OpenAI) callAPI(texts []string) ([][]float32, error) {
	reqBody := openAIRequest{
		Model: o.config.Model,
		Input: texts,
	}
	if o.config.Dimensions > 0 {
		reqBody.Dimensions = o.config.Dimensions
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", o.config.BaseURL+"/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+o.config.APIKey)

	resp, err := o.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("API request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var errResp openAIResponse
		json.Unmarshal(respBody, &errResp)
		if errResp.Error != nil {
			return nil, fmt.Errorf("OpenAI API error (%d): %s", resp.StatusCode, errResp.Error.Message)
		}
		return nil, fmt.Errorf("OpenAI API error: status %d", resp.StatusCode)
	}

	var apiResp openAIResponse
	if err := json.Unmarshal(respBody, &apiResp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	// Sort by index and convert float64 → float32
	vectors := make([][]float32, len(texts))
	for _, emb := range apiResp.Data {
		if emb.Index >= len(vectors) {
			continue
		}
		vec := make([]float32, len(emb.Embedding))
		for j, v := range emb.Embedding {
			vec[j] = float32(v)
		}
		vectors[emb.Index] = vec
	}

	return vectors, nil
}
