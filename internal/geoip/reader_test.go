package geoip

import (
	"net"
	"sync"
	"testing"
)

func TestLookupCityNilReader(t *testing.T) {
	r := &Reader{}
	rec := r.LookupCity(net.ParseIP("8.8.8.8"))
	if rec != nil {
		t.Errorf("expected nil from unloaded reader, got %+v", rec)
	}
}

func TestLookupASNNilReader(t *testing.T) {
	r := &Reader{}
	rec := r.LookupASN(net.ParseIP("8.8.8.8"))
	if rec != nil {
		t.Errorf("expected nil from unloaded reader, got %+v", rec)
	}
}

func TestLookupCityNilIP(t *testing.T) {
	r := &Reader{}
	rec := r.LookupCity(nil)
	if rec != nil {
		t.Errorf("expected nil for nil IP, got %+v", rec)
	}
}

func TestLookupASNNilIP(t *testing.T) {
	r := &Reader{}
	rec := r.LookupASN(nil)
	if rec != nil {
		t.Errorf("expected nil for nil IP, got %+v", rec)
	}
}

func TestHasCityASNEmpty(t *testing.T) {
	r := &Reader{}
	if r.HasCity() {
		t.Error("HasCity should be false on empty reader")
	}
	if r.HasASN() {
		t.Error("HasASN should be false on empty reader")
	}
}

func TestLoadInvalidPath(t *testing.T) {
	// Save and restore DefaultReader state
	orig := DefaultReader
	DefaultReader = &Reader{}
	defer func() { DefaultReader = orig }()

	err := Load("/nonexistent/path.mmdb", "")
	if err == nil {
		t.Error("expected error loading nonexistent file")
	}

	err = Load("", "/nonexistent/path.mmdb")
	if err == nil {
		t.Error("expected error loading nonexistent file")
	}
}

func TestLoadEmptyPaths(t *testing.T) {
	orig := DefaultReader
	DefaultReader = &Reader{}
	defer func() { DefaultReader = orig }()

	err := Load("", "")
	if err != nil {
		t.Errorf("expected no error for empty paths, got %v", err)
	}
}

func TestCloseEmpty(t *testing.T) {
	r := &Reader{}
	// Temporarily set DefaultReader
	orig := DefaultReader
	DefaultReader = r
	defer func() { DefaultReader = orig }()

	if err := Close(); err != nil {
		t.Errorf("Close on empty reader should not error, got %v", err)
	}
}

func TestConcurrentLookups(t *testing.T) {
	r := &Reader{} // no DB loaded — should return nil safely under concurrency
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			r.LookupCity(net.ParseIP("8.8.8.8"))
		}()
		go func() {
			defer wg.Done()
			r.LookupASN(net.ParseIP("1.1.1.1"))
		}()
	}
	wg.Wait()
}
