package clid

import (
	"os"
	"strings"
	"testing"
)

// TestTelemetryReachesEveryModeThatHasAConsumer is P2's gate.
//
// InitTelemetry was called from runCoordinator and runWorker and NOT from
// runStandalone, so `telemetry:` and WADJET_OTEL_* reached nothing in the
// default run mode. The assertion is on the call sites, because standing an
// OTLP collector up in a unit test would gate a wiring fact behind a network
// service.
func TestTelemetryReachesEveryModeThatHasAConsumer(t *testing.T) {
	src, err := os.ReadFile("serve.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(src)
	for _, fn := range []string{"func runStandalone(", "func runCoordinator(", "func runWorker("} {
		start := strings.Index(text, fn)
		if start < 0 {
			t.Fatalf("%s not found", fn)
		}
		// The body runs to the next top-level func declaration.
		end := strings.Index(text[start+len(fn):], "\nfunc ")
		if end < 0 {
			end = len(text) - start - len(fn)
		}
		body := text[start : start+len(fn)+end]
		if !strings.Contains(body, "InitTelemetry(") {
			t.Errorf("%s never calls InitTelemetry: the telemetry: section and "+
				"WADJET_OTEL_* reach nothing in that mode", strings.TrimSuffix(fn, "("))
		}
	}
}
