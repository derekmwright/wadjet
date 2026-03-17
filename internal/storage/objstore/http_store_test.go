package objstore

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

func TestHTTPStore_ReadOnly(t *testing.T) {
	s := NewHTTPStore(HTTPConfig{})

	_, err := s.Put(context.Background(), "http://example.com", "key", nil, 0, "")
	if err != errReadOnly {
		t.Fatalf("Put: expected errReadOnly, got %v", err)
	}

	_, err = s.PutIfMatch(context.Background(), "http://example.com", "key", nil, 0, "", "")
	if err != errReadOnly {
		t.Fatalf("PutIfMatch: expected errReadOnly, got %v", err)
	}

	err = s.Delete(context.Background(), "http://example.com", "key")
	if err != errReadOnly {
		t.Fatalf("Delete: expected errReadOnly, got %v", err)
	}

	err = s.MakeBucket(context.Background(), "http://example.com")
	if err != errReadOnly {
		t.Fatalf("MakeBucket: expected errReadOnly, got %v", err)
	}
}

func TestHTTPStore_List(t *testing.T) {
	s := NewHTTPStore(HTTPConfig{})

	items, err := s.List(context.Background(), "http://example.com", ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if items != nil {
		t.Fatalf("expected nil result from HTTP List, got %v", items)
	}
}

func TestHTTPStore_URL(t *testing.T) {
	s := NewHTTPStore(HTTPConfig{})

	got := s.url("http://example.com/", "/path/to/file")
	if got != "http://example.com/path/to/file" {
		t.Fatalf("url = %q, want %q", got, "http://example.com/path/to/file")
	}

	got = s.url("http://example.com", "path/to/file")
	if got != "http://example.com/path/to/file" {
		t.Fatalf("url = %q, want %q", got, "http://example.com/path/to/file")
	}
}

func TestHTTPStore_Get(t *testing.T) {
	content := []byte("hello from http")
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/test/file.txt" {
			w.Header().Set("Content-Type", "text/plain")
			w.Header().Set("ETag", `"abc123"`)
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.Write(content)
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	s := NewHTTPStore(HTTPConfig{})
	ctx := context.Background()

	rc, info, err := s.Get(ctx, ts.URL, "test/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	if !bytes.Equal(data, content) {
		t.Fatalf("got %q, want %q", string(data), string(content))
	}
	if info.Key != "test/file.txt" {
		t.Fatalf("key = %q, want %q", info.Key, "test/file.txt")
	}
	if info.ContentType != "text/plain" {
		t.Fatalf("content type = %q, want text/plain", info.ContentType)
	}
}

func TestHTTPStore_Get_NotFound(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer ts.Close()

	s := NewHTTPStore(HTTPConfig{})
	_, _, err := s.Get(context.Background(), ts.URL, "missing")
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestHTTPStore_Get_ServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	s := NewHTTPStore(HTTPConfig{})
	_, _, err := s.Get(context.Background(), ts.URL, "key")
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
}

func TestHTTPStore_Head(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead && r.URL.Path == "/test/file.txt" {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Length", "42")
			w.Header().Set("ETag", `"def456"`)
			w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method == http.MethodHead {
			http.NotFound(w, r)
			return
		}
	}))
	defer ts.Close()

	s := NewHTTPStore(HTTPConfig{})
	ctx := context.Background()

	info, err := s.Head(ctx, ts.URL, "test/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if info.Key != "test/file.txt" {
		t.Fatalf("key = %q", info.Key)
	}
	if info.ETag != `"def456"` {
		t.Fatalf("etag = %q", info.ETag)
	}
}

func TestHTTPStore_Head_NotFound(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer ts.Close()

	s := NewHTTPStore(HTTPConfig{})
	_, err := s.Head(context.Background(), ts.URL, "missing")
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestHTTPStore_Head_ServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	s := NewHTTPStore(HTTPConfig{})
	_, err := s.Head(context.Background(), ts.URL, "key")
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
}

func TestHTTPStore_BucketExists(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	s := NewHTTPStore(HTTPConfig{})
	exists, err := s.BucketExists(context.Background(), ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("expected bucket to exist for reachable URL")
	}
}

func TestHTTPStore_BucketExists_ServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer ts.Close()

	s := NewHTTPStore(HTTPConfig{})
	exists, err := s.BucketExists(context.Background(), ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("503 should mean bucket does not exist")
	}
}

func TestHTTPStore_BucketExists_Unreachable(t *testing.T) {
	s := NewHTTPStore(HTTPConfig{Timeout: 100 * time.Millisecond})
	exists, err := s.BucketExists(context.Background(), "http://192.0.2.1:9999")
	if err != nil {
		t.Fatal(err) // should not error; just return false
	}
	if exists {
		t.Fatal("unreachable should return false")
	}
}

func TestHTTPStore_GetReaderAt(t *testing.T) {
	content := []byte("0123456789ABCDEF")
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", "16")
			w.WriteHeader(http.StatusOK)
			return
		}
		// Support range requests
		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "" {
			var start, end int
			n, _ := parseRange(rangeHeader, &start, &end)
			if n == 2 && start >= 0 && end < len(content) {
				w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
				w.WriteHeader(http.StatusPartialContent)
				w.Write(content[start : end+1])
				return
			}
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		w.Write(content)
	}))
	defer ts.Close()

	s := NewHTTPStore(HTTPConfig{})
	ctx := context.Background()

	rac, size, err := s.GetReaderAt(ctx, ts.URL, "/file")
	if err != nil {
		t.Fatal(err)
	}
	defer rac.Close()

	if size != 16 {
		t.Fatalf("size = %d, want 16", size)
	}

	// Read from beginning
	buf := make([]byte, 4)
	n, err := rac.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt(0): %v", err)
	}
	if n != 4 || string(buf) != "0123" {
		t.Fatalf("got %q, want %q", string(buf[:n]), "0123")
	}

	// Read from middle
	buf = make([]byte, 4)
	n, err = rac.ReadAt(buf, 10)
	if err != nil {
		t.Fatalf("ReadAt(10): %v", err)
	}
	if n != 4 || string(buf) != "ABCD" {
		t.Fatalf("got %q, want %q", string(buf[:n]), "ABCD")
	}

	// Read at end (partial)
	buf = make([]byte, 8)
	n, err = rac.ReadAt(buf, 12)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadAt(12): %v", err)
	}
	if n != 4 {
		t.Fatalf("expected 4 bytes at end, got %d", n)
	}
	if string(buf[:n]) != "CDEF" {
		t.Fatalf("got %q, want %q", string(buf[:n]), "CDEF")
	}
}

func TestHTTPStore_GetReaderAt_RangeNotSatisfiable(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", "10")
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
	}))
	defer ts.Close()

	s := NewHTTPStore(HTTPConfig{})
	ctx := context.Background()

	rac, _, err := s.GetReaderAt(ctx, ts.URL, "/file")
	if err != nil {
		t.Fatal(err)
	}
	defer rac.Close()

	buf := make([]byte, 5)
	_, err = rac.ReadAt(buf, 0)
	if err != io.EOF {
		t.Fatalf("expected io.EOF for 416 response, got %v", err)
	}
}

func TestHTTPStore_GetReaderAt_FallbackFullBody(t *testing.T) {
	// Server that ignores Range and returns full body (200 OK)
	content := []byte("0123456789")
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", "10")
			w.WriteHeader(http.StatusOK)
			return
		}
		// Ignore range header, return full body with 200
		w.WriteHeader(http.StatusOK)
		w.Write(content)
	}))
	defer ts.Close()

	s := NewHTTPStore(HTTPConfig{})
	ctx := context.Background()

	rac, _, err := s.GetReaderAt(ctx, ts.URL, "/file")
	if err != nil {
		t.Fatal(err)
	}
	defer rac.Close()

	// Read from offset 5 - should skip first 5 bytes of full response
	buf := make([]byte, 5)
	n, err := rac.ReadAt(buf, 5)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadAt(5): %v", err)
	}
	if n != 5 || string(buf) != "56789" {
		t.Fatalf("got n=%d %q, want 5 %q", n, string(buf[:n]), "56789")
	}
}

func TestHTTPStore_GetReaderAt_ServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", "10")
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	s := NewHTTPStore(HTTPConfig{})
	ctx := context.Background()

	rac, _, err := s.GetReaderAt(ctx, ts.URL, "/file")
	if err != nil {
		t.Fatal(err)
	}
	defer rac.Close()

	buf := make([]byte, 5)
	_, err = rac.ReadAt(buf, 0)
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
}

func TestHTTPStore_GetReaderAt_NotFound(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer ts.Close()

	s := NewHTTPStore(HTTPConfig{})
	_, _, err := s.GetReaderAt(context.Background(), ts.URL, "missing")
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestHTTPStore_GetReaderAt_NoContentLength(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return OK without Content-Length
		w.Header().Del("Content-Length")
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	s := NewHTTPStore(HTTPConfig{})
	_, _, err := s.GetReaderAt(context.Background(), ts.URL, "file")
	if err == nil {
		t.Fatal("expected error when server has no Content-Length")
	}
}

func TestHTTPStore_WithHeaders(t *testing.T) {
	var gotAuth string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Write([]byte("ok"))
	}))
	defer ts.Close()

	s := NewHTTPStore(HTTPConfig{
		Headers: map[string]string{
			"Authorization": "Bearer mytoken",
		},
	})

	rc, _, err := s.Get(context.Background(), ts.URL, "file")
	if err != nil {
		t.Fatal(err)
	}
	rc.Close()

	if gotAuth != "Bearer mytoken" {
		t.Fatalf("expected auth header, got %q", gotAuth)
	}
}

func TestHTTPReaderAtClose(t *testing.T) {
	r := &httpReaderAt{}
	if err := r.Close(); err != nil {
		t.Fatalf("Close() should return nil, got %v", err)
	}
}

func TestHTTPReaderAt_ReadAtBeyondSize(t *testing.T) {
	r := &httpReaderAt{size: 10}
	buf := make([]byte, 5)
	_, err := r.ReadAt(buf, 20) // offset beyond file size
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

func TestNewHTTPStore_DefaultTimeout(t *testing.T) {
	s := NewHTTPStore(HTTPConfig{})
	if s.client == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestNewHTTPStore_CustomTimeout(t *testing.T) {
	s := NewHTTPStore(HTTPConfig{Timeout: 5 * time.Second})
	if s.client.Timeout != 5*time.Second {
		t.Fatalf("expected 5s timeout, got %v", s.client.Timeout)
	}
}

// parseRange is a test helper to parse "bytes=start-end"
func parseRange(s string, start, end *int) (int, error) {
	n, err := parseRangeInner(s, start, end)
	return n, err
}

func parseRangeInner(s string, start, end *int) (int, error) {
	n := 0
	var st, en int
	for i := 0; i < len(s); i++ {
		if s[i] >= '0' && s[i] <= '9' {
			if n == 0 {
				for i < len(s) && s[i] >= '0' && s[i] <= '9' {
					st = st*10 + int(s[i]-'0')
					i++
				}
				*start = st
				n++
			} else if n == 1 {
				for i < len(s) && s[i] >= '0' && s[i] <= '9' {
					en = en*10 + int(s[i]-'0')
					i++
				}
				*end = en
				n++
				return n, nil
			}
		}
	}
	return n, nil
}
