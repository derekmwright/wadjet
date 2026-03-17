package geoip

import (
	"net"
	"sync"
	"testing"
)

func TestReaderNewInstanceHasNoDB(t *testing.T) {
	r := &Reader{}
	if r.HasCity() {
		t.Error("new reader should not have city DB")
	}
	if r.HasASN() {
		t.Error("new reader should not have ASN DB")
	}
}

func TestLookupCityInvalidIP(t *testing.T) {
	r := &Reader{}
	// Invalid IP (ParseIP returns nil for invalid strings)
	badIP := net.ParseIP("not-an-ip")
	rec := r.LookupCity(badIP)
	if rec != nil {
		t.Errorf("expected nil for invalid IP, got %+v", rec)
	}
}

func TestLookupASNInvalidIP(t *testing.T) {
	r := &Reader{}
	badIP := net.ParseIP("not-an-ip")
	rec := r.LookupASN(badIP)
	if rec != nil {
		t.Errorf("expected nil for invalid IP, got %+v", rec)
	}
}

func TestLookupCityIPv6NoDB(t *testing.T) {
	r := &Reader{}
	ipv6 := net.ParseIP("2001:4860:4860::8888")
	rec := r.LookupCity(ipv6)
	if rec != nil {
		t.Errorf("expected nil from unloaded reader for IPv6, got %+v", rec)
	}
}

func TestLookupASNIPv6NoDB(t *testing.T) {
	r := &Reader{}
	ipv6 := net.ParseIP("2001:4860:4860::8888")
	rec := r.LookupASN(ipv6)
	if rec != nil {
		t.Errorf("expected nil from unloaded reader for IPv6, got %+v", rec)
	}
}

func TestLoadBothInvalidPaths(t *testing.T) {
	orig := DefaultReader
	DefaultReader = &Reader{}
	defer func() { DefaultReader = orig }()

	// Both paths invalid - should fail on first (city)
	err := Load("/nonexistent/city.mmdb", "/nonexistent/asn.mmdb")
	if err == nil {
		t.Error("expected error for nonexistent city file")
	}
}

func TestLoadCityOnlyEmptyASN(t *testing.T) {
	orig := DefaultReader
	DefaultReader = &Reader{}
	defer func() { DefaultReader = orig }()

	// City invalid, ASN empty
	err := Load("/nonexistent/city.mmdb", "")
	if err == nil {
		t.Error("expected error for nonexistent city file")
	}
}

func TestCloseIdempotent(t *testing.T) {
	orig := DefaultReader
	DefaultReader = &Reader{}
	defer func() { DefaultReader = orig }()

	// Multiple closes should not error
	if err := Close(); err != nil {
		t.Errorf("first close: %v", err)
	}
	if err := Close(); err != nil {
		t.Errorf("second close: %v", err)
	}
}

func TestConcurrentHasChecks(t *testing.T) {
	r := &Reader{}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			r.HasCity()
		}()
		go func() {
			defer wg.Done()
			r.HasASN()
		}()
	}
	wg.Wait()
}

func TestConcurrentMixedLookups(t *testing.T) {
	r := &Reader{}
	var wg sync.WaitGroup
	ips := []net.IP{
		net.ParseIP("8.8.8.8"),
		net.ParseIP("1.1.1.1"),
		net.ParseIP("192.168.0.1"),
		net.ParseIP("10.0.0.1"),
		nil,
	}
	for i := 0; i < 50; i++ {
		for _, ip := range ips {
			ip := ip
			wg.Add(2)
			go func() {
				defer wg.Done()
				r.LookupCity(ip)
			}()
			go func() {
				defer wg.Done()
				r.LookupASN(ip)
			}()
		}
	}
	wg.Wait()
}

func TestCityRecordStructFields(t *testing.T) {
	// Test that CityRecord struct is properly initialized with zero values
	var rec CityRecord
	if rec.Country.ISOCode != "" {
		t.Error("expected empty ISOCode")
	}
	if rec.Location.Latitude != 0 {
		t.Error("expected zero latitude")
	}
	if rec.Location.Longitude != 0 {
		t.Error("expected zero longitude")
	}
}

func TestASNRecordStructFields(t *testing.T) {
	var rec ASNRecord
	if rec.Number != 0 {
		t.Error("expected zero ASN number")
	}
	if rec.Organization != "" {
		t.Error("expected empty organization")
	}
}

func TestLoadEmptyPathsPreservesState(t *testing.T) {
	orig := DefaultReader
	DefaultReader = &Reader{}
	defer func() { DefaultReader = orig }()

	// Loading empty paths should succeed and leave reader with no DB
	if err := Load("", ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if DefaultReader.HasCity() {
		t.Error("should not have city after loading empty paths")
	}
	if DefaultReader.HasASN() {
		t.Error("should not have ASN after loading empty paths")
	}
}

func TestLookupWithLoopbackIP(t *testing.T) {
	r := &Reader{}
	lo := net.ParseIP("127.0.0.1")
	if rec := r.LookupCity(lo); rec != nil {
		t.Error("expected nil from unloaded reader for loopback")
	}
	if rec := r.LookupASN(lo); rec != nil {
		t.Error("expected nil from unloaded reader for loopback")
	}
}

func TestLookupWithBroadcastIP(t *testing.T) {
	r := &Reader{}
	bcast := net.ParseIP("255.255.255.255")
	if rec := r.LookupCity(bcast); rec != nil {
		t.Error("expected nil from unloaded reader for broadcast")
	}
	if rec := r.LookupASN(bcast); rec != nil {
		t.Error("expected nil from unloaded reader for broadcast")
	}
}
