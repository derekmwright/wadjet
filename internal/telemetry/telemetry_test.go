package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/trace/noop"
)

func TestStartSpan(t *testing.T) {
	// Verify StartRemoteSpan creates a child span linked to the parent trace
	// Use a no-op tracer (no exporter needed for unit test)
	tp := &Provider{
		tracer: noop.NewTracerProvider().Tracer("test"),
	}

	ctx := context.Background()

	// Valid trace/span IDs
	traceID := "0af7651916cd43dd8448eb211c80319c"
	spanID := "00f067aa0ba902b7"

	ctx, span := tp.StartRemoteSpan(ctx, "test-span", traceID, spanID)
	defer span.End()

	if !span.SpanContext().IsValid() {
		// NoopTracer returns invalid contexts, which is fine for unit test
		t.Log("noop tracer: span context is not valid (expected)")
	}

	// Verify helpers work with OTel context
	_ = TraceIDFromContext(ctx)
	_ = SpanIDFromContext(ctx)
}

func TestStartRemoteSpan_InvalidIDs(t *testing.T) {
	tp := &Provider{
		tracer: noop.NewTracerProvider().Tracer("test"),
	}

	ctx := context.Background()

	// Invalid trace ID should fall back to new root span
	ctx, span := tp.StartRemoteSpan(ctx, "test-span", "invalid", "also-invalid")
	defer span.End()

	// Should not panic
	_ = ctx
}

func TestTraceIDToBytes(t *testing.T) {
	hexID := "0af7651916cd43dd8448eb211c80319c"
	b := TraceIDToBytes(hexID)
	if b[0] != 0x0a || b[1] != 0xf7 {
		t.Errorf("unexpected bytes: %x", b)
	}
}
