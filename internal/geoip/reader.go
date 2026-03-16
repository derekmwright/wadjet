// Package geoip provides MaxMind GeoIP2/GeoLite2 database lookups for IP geolocation and ASN enrichment.
package geoip

import (
	"fmt"
	"net"
	"sync"

	"github.com/oschwald/maxminddb-golang"
)

// CityRecord mirrors the MaxMind GeoIP2 City database schema.
type CityRecord struct {
	Country struct {
		ISOCode string            `maxminddb:"iso_code"`
		Names   map[string]string `maxminddb:"names"`
	} `maxminddb:"country"`
	City struct {
		Names map[string]string `maxminddb:"names"`
	} `maxminddb:"city"`
	Subdivisions []struct {
		ISOCode string            `maxminddb:"iso_code"`
		Names   map[string]string `maxminddb:"names"`
	} `maxminddb:"subdivisions"`
	Postal struct {
		Code string `maxminddb:"code"`
	} `maxminddb:"postal"`
	Location struct {
		Latitude  float64 `maxminddb:"latitude"`
		Longitude float64 `maxminddb:"longitude"`
		TimeZone  string  `maxminddb:"time_zone"`
	} `maxminddb:"location"`
	Continent struct {
		Code  string            `maxminddb:"code"`
		Names map[string]string `maxminddb:"names"`
	} `maxminddb:"continent"`
}

// ASNRecord mirrors the MaxMind GeoIP2 ASN database schema.
type ASNRecord struct {
	Number       uint   `maxminddb:"autonomous_system_number"`
	Organization string `maxminddb:"autonomous_system_organization"`
}

// Reader wraps MaxMind MMDB database handles with thread-safe access.
type Reader struct {
	mu   sync.RWMutex
	city *maxminddb.Reader
	asn  *maxminddb.Reader
}

// DefaultReader is the process-wide GeoIP reader. Scalar functions
// reference this instance; it starts empty and is populated by Load().
var DefaultReader = &Reader{}

// Load opens MaxMind MMDB database files. Either path may be empty to
// skip that database. Returns an error if a non-empty path fails to open.
func Load(cityPath, asnPath string) error {
	DefaultReader.mu.Lock()
	defer DefaultReader.mu.Unlock()

	// Close any previously loaded databases
	if DefaultReader.city != nil {
		DefaultReader.city.Close()
		DefaultReader.city = nil
	}
	if DefaultReader.asn != nil {
		DefaultReader.asn.Close()
		DefaultReader.asn = nil
	}

	if cityPath != "" {
		db, err := maxminddb.Open(cityPath)
		if err != nil {
			return fmt.Errorf("opening GeoIP city database %q: %w", cityPath, err)
		}
		DefaultReader.city = db
	}

	if asnPath != "" {
		db, err := maxminddb.Open(asnPath)
		if err != nil {
			return fmt.Errorf("opening GeoIP ASN database %q: %w", asnPath, err)
		}
		DefaultReader.asn = db
	}

	return nil
}

// Close releases all loaded MMDB databases.
func Close() error {
	DefaultReader.mu.Lock()
	defer DefaultReader.mu.Unlock()

	var errs []error
	if DefaultReader.city != nil {
		if err := DefaultReader.city.Close(); err != nil {
			errs = append(errs, err)
		}
		DefaultReader.city = nil
	}
	if DefaultReader.asn != nil {
		if err := DefaultReader.asn.Close(); err != nil {
			errs = append(errs, err)
		}
		DefaultReader.asn = nil
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// LookupCity queries the city database for the given IP. Returns nil if
// no city database is loaded or the IP is not found.
func (r *Reader) LookupCity(ip net.IP) *CityRecord {
	r.mu.RLock()
	db := r.city
	r.mu.RUnlock()

	if db == nil {
		return nil
	}

	var rec CityRecord
	if err := db.Lookup(ip, &rec); err != nil {
		return nil
	}
	return &rec
}

// LookupASN queries the ASN database for the given IP. Returns nil if
// no ASN database is loaded or the IP is not found.
func (r *Reader) LookupASN(ip net.IP) *ASNRecord {
	r.mu.RLock()
	db := r.asn
	r.mu.RUnlock()

	if db == nil {
		return nil
	}

	var rec ASNRecord
	if err := db.Lookup(ip, &rec); err != nil {
		return nil
	}
	return &rec
}

// HasCity reports whether a city database is loaded.
func (r *Reader) HasCity() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.city != nil
}

// HasASN reports whether an ASN database is loaded.
func (r *Reader) HasASN() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.asn != nil
}
