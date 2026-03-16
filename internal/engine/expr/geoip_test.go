package expr

import (
	"testing"
)

// TestGeoipFunctionsRegistered verifies all 11 GeoIP functions are in the registry.
func TestGeoipFunctionsRegistered(t *testing.T) {
	expected := []string{
		"geoip_country", "geoip_country_name", "geoip_city",
		"geoip_subdivision", "geoip_postal_code",
		"geoip_latitude", "geoip_longitude", "geoip_timezone",
		"geoip_continent", "geoip_asn", "geoip_org",
	}
	for _, name := range expected {
		if !DefaultRegistry.Has(name) {
			t.Errorf("function %q not registered in DefaultRegistry", name)
		}
	}
}

// TestGeoipNullPropagation verifies all GeoIP functions return nil for nil input.
func TestGeoipNullPropagation(t *testing.T) {
	funcs := []string{
		"geoip_country", "geoip_country_name", "geoip_city",
		"geoip_subdivision", "geoip_postal_code",
		"geoip_latitude", "geoip_longitude", "geoip_timezone",
		"geoip_continent", "geoip_asn", "geoip_org",
	}
	for _, name := range funcs {
		fn := DefaultRegistry.Lookup(name)
		if fn == nil {
			t.Errorf("function %q not found", name)
			continue
		}
		got := fn([]any{nil})
		if got != nil {
			t.Errorf("%s(nil) = %v, want nil", name, got)
		}
	}
}

// TestGeoipInvalidIP verifies all GeoIP functions return nil for unparseable IPs.
func TestGeoipInvalidIP(t *testing.T) {
	funcs := []string{
		"geoip_country", "geoip_country_name", "geoip_city",
		"geoip_subdivision", "geoip_postal_code",
		"geoip_latitude", "geoip_longitude", "geoip_timezone",
		"geoip_continent", "geoip_asn", "geoip_org",
	}
	for _, name := range funcs {
		fn := DefaultRegistry.Lookup(name)
		if fn == nil {
			t.Errorf("function %q not found", name)
			continue
		}
		got := fn([]any{"not-an-ip"})
		if got != nil {
			t.Errorf("%s('not-an-ip') = %v, want nil", name, got)
		}
	}
}

// TestGeoipNoDatabaseLoaded verifies functions return nil when no MMDB is loaded.
// (DefaultReader has no databases loaded in test environment.)
func TestGeoipNoDatabaseLoaded(t *testing.T) {
	funcs := []string{
		"geoip_country", "geoip_country_name", "geoip_city",
		"geoip_subdivision", "geoip_postal_code",
		"geoip_latitude", "geoip_longitude", "geoip_timezone",
		"geoip_continent", "geoip_asn", "geoip_org",
	}
	for _, name := range funcs {
		fn := DefaultRegistry.Lookup(name)
		if fn == nil {
			t.Errorf("function %q not found", name)
			continue
		}
		// Valid IP but no database loaded — should return nil gracefully
		got := fn([]any{"8.8.8.8"})
		if got != nil {
			t.Errorf("%s('8.8.8.8') with no DB = %v, want nil", name, got)
		}
	}
}

// TestGeoipEmptyArgs verifies functions handle empty argument lists.
func TestGeoipEmptyArgs(t *testing.T) {
	funcs := []string{
		"geoip_country", "geoip_country_name", "geoip_city",
		"geoip_subdivision", "geoip_postal_code",
		"geoip_latitude", "geoip_longitude", "geoip_timezone",
		"geoip_continent", "geoip_asn", "geoip_org",
	}
	for _, name := range funcs {
		fn := DefaultRegistry.Lookup(name)
		if fn == nil {
			t.Errorf("function %q not found", name)
			continue
		}
		got := fn([]any{})
		if got != nil {
			t.Errorf("%s() = %v, want nil", name, got)
		}
	}
}

// TestGeoipTotalFunctionCount verifies at least 254 functions are registered.
// The exact count may be higher if other tests register additional functions
// (e.g., TestRegisterFunc adds "double"), so we check >= 254.
func TestGeoipTotalFunctionCount(t *testing.T) {
	count := len(DefaultRegistry.Names())
	if count < 254 {
		t.Errorf("total registered functions = %d, want >= 254", count)
	}
}
