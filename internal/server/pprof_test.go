package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPprofGoroutineEndpoint(t *testing.T) {
	s := New(Config{}, nil)
	ts := httptest.NewServer(s.mux)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/debug/pprof/goroutine?debug=2")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}
	buf := make([]byte, 512)
	n, _ := resp.Body.Read(buf)
	if !strings.Contains(string(buf[:n]), "goroutine") {
		t.Error("response does not contain goroutine dump")
	}
}
