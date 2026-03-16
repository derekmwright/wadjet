package objstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// HTTPConfig holds configuration for the HTTP object store.
type HTTPConfig struct {
	Headers map[string]string // auth headers, API keys, etc.
	Timeout time.Duration
}

// HTTPStore implements a read-only Store backed by HTTP/HTTPS URLs.
// The "bucket" parameter is the base URL (scheme + host, e.g. "https://example.com")
// and "key" is the path component appended after a slash.
type HTTPStore struct {
	client  *http.Client
	headers map[string]string
}

// NewHTTPStore creates a read-only HTTP-backed object store with connection pooling.
func NewHTTPStore(cfg HTTPConfig) *HTTPStore {
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: timeout,
		DisableCompression:    true,
	}
	return &HTTPStore{
		client: &http.Client{
			Transport: transport,
			Timeout:   timeout,
		},
		headers: cfg.Headers,
	}
}

// url builds the full URL from bucket (base URL) and key (path).
func (s *HTTPStore) url(bucket, key string) string {
	return strings.TrimRight(bucket, "/") + "/" + strings.TrimLeft(key, "/")
}

// newRequest creates an http.Request with the configured headers applied.
func (s *HTTPStore) newRequest(ctx context.Context, method, url string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range s.headers {
		req.Header.Set(k, v)
	}
	return req, nil
}

// Get retrieves the object body and metadata via HTTP GET.
func (s *HTTPStore) Get(ctx context.Context, bucket, key string) (io.ReadCloser, ObjectInfo, error) {
	req, err := s.newRequest(ctx, http.MethodGet, s.url(bucket, key))
	if err != nil {
		return nil, ObjectInfo{}, fmt.Errorf("http get: creating request: %w", err)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, ObjectInfo{}, fmt.Errorf("http get: %w", err)
	}
	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, ObjectInfo{}, ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, ObjectInfo{}, fmt.Errorf("http get: status %d", resp.StatusCode)
	}
	info := ObjectInfo{
		Key:         key,
		Size:        resp.ContentLength,
		ETag:        resp.Header.Get("ETag"),
		ContentType: resp.Header.Get("Content-Type"),
	}
	if lm := resp.Header.Get("Last-Modified"); lm != "" {
		info.LastModified, _ = http.ParseTime(lm)
	}
	return resp.Body, info, nil
}

// Head retrieves object metadata via HTTP HEAD without downloading the body.
func (s *HTTPStore) Head(ctx context.Context, bucket, key string) (ObjectInfo, error) {
	req, err := s.newRequest(ctx, http.MethodHead, s.url(bucket, key))
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("http head: creating request: %w", err)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("http head: %w", err)
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return ObjectInfo{}, ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return ObjectInfo{}, fmt.Errorf("http head: status %d", resp.StatusCode)
	}
	info := ObjectInfo{
		Key:         key,
		Size:        resp.ContentLength,
		ETag:        resp.Header.Get("ETag"),
		ContentType: resp.Header.Get("Content-Type"),
	}
	if lm := resp.Header.Get("Last-Modified"); lm != "" {
		info.LastModified, _ = http.ParseTime(lm)
	}
	return info, nil
}

// List is not meaningfully supported over plain HTTP. It returns an empty result
// if the base URL is reachable, or an error otherwise.
func (s *HTTPStore) List(ctx context.Context, bucket string, opts ListOptions) ([]ObjectInfo, error) {
	// Plain HTTP servers don't expose directory listings in a standard way.
	// Return empty to satisfy the interface without failing.
	return nil, nil
}

// BucketExists checks whether the base URL is reachable with a HEAD request.
func (s *HTTPStore) BucketExists(ctx context.Context, bucket string) (bool, error) {
	req, err := s.newRequest(ctx, http.MethodHead, strings.TrimRight(bucket, "/")+"/")
	if err != nil {
		return false, fmt.Errorf("http bucket exists: creating request: %w", err)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return false, nil // unreachable is treated as non-existent
	}
	resp.Body.Close()
	return resp.StatusCode < 500, nil
}

var errReadOnly = errors.New("http store is read-only")

// Put is not supported on a read-only HTTP store.
func (s *HTTPStore) Put(ctx context.Context, bucket, key string, r io.Reader, size int64, contentType string) (string, error) {
	return "", errReadOnly
}

// PutIfMatch is not supported on a read-only HTTP store.
func (s *HTTPStore) PutIfMatch(ctx context.Context, bucket, key string, r io.Reader, size int64, contentType string, expectedETag string) (string, error) {
	return "", errReadOnly
}

// Delete is not supported on a read-only HTTP store.
func (s *HTTPStore) Delete(ctx context.Context, bucket, key string) error {
	return errReadOnly
}

// MakeBucket is not supported on a read-only HTTP store.
func (s *HTTPStore) MakeBucket(ctx context.Context, bucket string) error {
	return errReadOnly
}

// GetReaderAt returns a ReaderAtCloser that uses HTTP Range requests for
// random-access reads. This enables efficient Parquet column pruning over HTTP.
func (s *HTTPStore) GetReaderAt(ctx context.Context, bucket, key string) (ReaderAtCloser, int64, error) {
	// Issue a HEAD first to learn the file size and confirm the resource exists.
	info, err := s.Head(ctx, bucket, key)
	if err != nil {
		return nil, 0, err
	}
	if info.Size <= 0 {
		return nil, 0, fmt.Errorf("http get reader at: server did not report Content-Length")
	}
	return &httpReaderAt{
		store:  s,
		url:    s.url(bucket, key),
		ctx:    ctx,
		size:   info.Size,
	}, info.Size, nil
}

// httpReaderAt implements ReaderAtCloser using HTTP Range requests.
type httpReaderAt struct {
	store *HTTPStore
	url   string
	ctx   context.Context
	size  int64
}

// ReadAt reads len(p) bytes starting at byte offset off using an HTTP Range request.
func (r *httpReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= r.size {
		return 0, io.EOF
	}
	end := off + int64(len(p)) - 1
	if end >= r.size {
		end = r.size - 1
	}

	req, err := r.store.newRequest(r.ctx, http.MethodGet, r.url)
	if err != nil {
		return 0, fmt.Errorf("http range read: creating request: %w", err)
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, end))

	resp, err := r.store.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("http range read: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		return 0, io.EOF
	}
	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("http range read: status %d", resp.StatusCode)
	}

	// If the server ignores Range and returns the full body (200 OK), we need
	// to skip to the offset and read only len(p) bytes.
	if resp.StatusCode == http.StatusOK {
		if _, err := io.CopyN(io.Discard, resp.Body, off); err != nil {
			return 0, fmt.Errorf("http range read: seeking in full response: %w", err)
		}
	}

	n, err := io.ReadFull(resp.Body, p[:end-off+1])
	if err == io.ErrUnexpectedEOF {
		err = io.EOF
	}

	// ReadAt contract: if n < len(p), return a non-nil error.
	if n < len(p) && err == nil {
		err = io.EOF
	}

	// If we successfully read a partial buffer due to EOF at end of file,
	// we still need to report how many bytes were transferred via Content-Length
	// if available and correct.
	if err == nil && resp.StatusCode == http.StatusPartialContent {
		if cl := resp.Header.Get("Content-Length"); cl != "" {
			expected, parseErr := strconv.ParseInt(cl, 10, 64)
			if parseErr == nil && int64(n) < expected {
				err = io.ErrUnexpectedEOF
			}
		}
	}

	return n, err
}

// Close is a no-op because each ReadAt opens and closes its own HTTP request.
func (r *httpReaderAt) Close() error {
	return nil
}

// Compile-time interface assertions.
var (
	_ Store         = (*HTTPStore)(nil)
	_ ReaderAtStore = (*HTTPStore)(nil)
)
