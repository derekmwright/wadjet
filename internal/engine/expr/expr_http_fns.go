// This file holds expr http fns; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"strconv"
	"strings"
)

// --- HTTP parsing functions ---
// These work on raw HTTP request/response payloads (text protocol).

// fnHTTPMethod extracts the HTTP method from a request payload.
// http_method(payload) → 'GET', 'POST', etc.
func fnHTTPMethod(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	sp := strings.IndexByte(s, ' ')
	if sp < 0 || sp > 7 {
		return nil
	}
	method := s[:sp]
	switch method {
	case "GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH", "TRACE", "CONNECT":
		return method
	default:
		return nil
	}
}

// fnHTTPPath extracts the request path from an HTTP request.
// http_path('GET /api/v1/users HTTP/1.1\r\n...') → '/api/v1/users'
func fnHTTPPath(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	sp1 := strings.IndexByte(s, ' ')
	if sp1 < 0 {
		return nil
	}
	rest := s[sp1+1:]
	sp2 := strings.IndexByte(rest, ' ')
	if sp2 < 0 {
		// try newline
		sp2 = strings.IndexByte(rest, '\r')
		if sp2 < 0 {
			sp2 = strings.IndexByte(rest, '\n')
		}
	}
	if sp2 < 0 {
		return rest
	}
	return rest[:sp2]
}

// fnHTTPHost extracts the Host header from an HTTP request.
func fnHTTPHost(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return extractHTTPHeader(toString(args[0]), "host")
}

// fnHTTPStatusCode extracts the status code from an HTTP response.
// http_status_code('HTTP/1.1 200 OK\r\n...') → 200
func fnHTTPStatusCode(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	if !strings.HasPrefix(s, "HTTP/") {
		return nil
	}
	sp1 := strings.IndexByte(s, ' ')
	if sp1 < 0 {
		return nil
	}
	rest := s[sp1+1:]
	sp2 := strings.IndexAny(rest, " \r\n")
	codeStr := rest
	if sp2 >= 0 {
		codeStr = rest[:sp2]
	}
	code, err := strconv.Atoi(codeStr)
	if err != nil {
		return nil
	}
	return int64(code)
}

// fnHTTPStatusClass classifies HTTP status: '1xx', '2xx', '3xx', '4xx', '5xx'.
func fnHTTPStatusClass(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	code := int(ToInt64(args[0]))
	switch {
	case code >= 100 && code < 200:
		return "1xx"
	case code >= 200 && code < 300:
		return "2xx"
	case code >= 300 && code < 400:
		return "3xx"
	case code >= 400 && code < 500:
		return "4xx"
	case code >= 500 && code < 600:
		return "5xx"
	default:
		return nil
	}
}

// fnHTTPContentType extracts Content-Type header value.
func fnHTTPContentType(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return extractHTTPHeader(toString(args[0]), "content-type")
}

// fnHTTPContentLength extracts Content-Length header as integer.
func fnHTTPContentLength(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	val := extractHTTPHeader(toString(args[0]), "content-length")
	if val == nil {
		return nil
	}
	n, err := strconv.ParseInt(val.(string), 10, 64)
	if err != nil {
		return nil
	}
	return n
}

// fnHTTPUserAgent extracts User-Agent header.
func fnHTTPUserAgent(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return extractHTTPHeader(toString(args[0]), "user-agent")
}

// fnHTTPHeader extracts any HTTP header by name.
// http_header(payload, 'X-Forwarded-For')
func fnHTTPHeader(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	return extractHTTPHeader(toString(args[0]), strings.ToLower(toString(args[1])))
}

// fnHTTPVersion extracts the HTTP version from a request or response.
// http_version('GET / HTTP/1.1\r\n...') → 'HTTP/1.1'
func fnHTTPVersion(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	// Response: starts with HTTP/
	if strings.HasPrefix(s, "HTTP/") {
		sp := strings.IndexAny(s, " \r\n")
		if sp < 0 {
			return s
		}
		return s[:sp]
	}
	// Request: HTTP version is after the second space on the first line
	line := s
	if nl := strings.IndexByte(s, '\r'); nl >= 0 {
		line = s[:nl]
	} else if nl := strings.IndexByte(s, '\n'); nl >= 0 {
		line = s[:nl]
	}
	sp := strings.LastIndex(line, " HTTP/")
	if sp < 0 {
		return nil
	}
	return line[sp+1:]
}

// fnIsHTTPRequest tests if payload looks like an HTTP request.
func fnIsHTTPRequest(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	for _, m := range []string{"GET ", "POST ", "PUT ", "DELETE ", "HEAD ", "OPTIONS ", "PATCH ", "TRACE ", "CONNECT "} {
		if strings.HasPrefix(s, m) {
			return true
		}
	}
	return false
}

// fnIsHTTPResponse tests if payload looks like an HTTP response.
func fnIsHTTPResponse(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return strings.HasPrefix(toString(args[0]), "HTTP/")
}
