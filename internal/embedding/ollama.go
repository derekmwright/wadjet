package embedding

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// OllamaConfig configures the Ollama embedding provider for local, self-hosted
// embedding models (no API key, runs on the user's machine).
type OllamaConfig struct {
	Model      string // default: nomic-embed-text
	Dimensions int    // 0 = inferred from the model name (falls back to first response)
	BaseURL    string // default: http://localhost:11434
}

// Ollama implements Provider using a local Ollama server's /api/embed endpoint.
type Ollama struct {
	config OllamaConfig
	client *http.Client
	cache  *Cache
	dim    int
}

// NewOllama creates an Ollama embedding provider.
func NewOllama(cfg OllamaConfig, cache *Cache) *Ollama {
	if cfg.Model == "" {
		cfg.Model = "nomic-embed-text"
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = "http://localhost:11434"
	}

	dim := cfg.Dimensions
	if dim <= 0 {
		switch cfg.Model {
		case "mxbai-embed-large":
			dim = 1024
		case "all-minilm":
			dim = 384
		case "snowflake-arctic-embed":
			dim = 1024
		default:
			// nomic-embed-text and most general-purpose models
			dim = 768
		}
	}

	return &Ollama{
		config: cfg,
		client: &http.Client{Timeout: 60 * time.Second},
		cache:  cache,
		dim:    dim,
	}
}

func (o *Ollama) Model() string  { return o.config.Model }
func (o *Ollama) Dimension() int { return o.dim }

// Embed returns embeddings for the given texts, using cache where possible.
// Uncached texts are batched into a single /api/embed call.
func (o *Ollama) Embed(texts []string) ([][]float32, error) {
	return embedWithCache(o.cache, o.config.Model, texts, o.callAPI)
}

type ollamaRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type ollamaResponse struct {
	Model      string      `json:"model"`
	Embeddings [][]float32 `json:"embeddings"`
	Error      string      `json:"error,omitempty"`
}

func (o *Ollama) callAPI(texts []string) ([][]float32, error) {
	reqBody := ollamaRequest{
		Model: o.config.Model,
		Input: texts,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", o.config.BaseURL+"/api/embed", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

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
		var errResp ollamaResponse
		json.Unmarshal(respBody, &errResp)
		if errResp.Error != "" {
			return nil, fmt.Errorf("Ollama API error (%d): %s", resp.StatusCode, errResp.Error)
		}
		return nil, fmt.Errorf("Ollama API error: status %d", resp.StatusCode)
	}

	var apiResp ollamaResponse
	if err := json.Unmarshal(respBody, &apiResp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}
	if apiResp.Error != "" {
		return nil, fmt.Errorf("Ollama API error: %s", apiResp.Error)
	}

	// NOTE: we deliberately do NOT mutate o.dim from the response here. The
	// output VECTOR width is fixed at plan time from Dimension(), so a runtime
	// update would (a) race with concurrent Dimension() reads on this shared
	// singleton and (b) arrive too late to affect the query that triggered it.
	// For a model whose true dimension isn't in the table above, set it
	// explicitly via OllamaConfig.Dimensions (WADJET_EMBED_DIM); vecEmbed
	// validates the returned width and NULLs any row that doesn't match.
	return apiResp.Embeddings, nil
}
