// Package telemetry provides OpenTelemetry tracing for Wadjet.
//
// Wadjet already generates W3C-compatible TraceID/SpanID and propagates them
// through NATS tasks. This package bridges those IDs into the OTel SDK so they
// are exported to Jaeger, Tempo, or any OTLP-compatible backend.
package telemetry

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
)

// Config configures the OTLP trace exporter.
type Config struct {
	Endpoint    string  // OTLP gRPC endpoint (e.g., "localhost:4317")
	Insecure    bool    // use plaintext gRPC (no TLS)
	ServiceName string  // service name (default: "wadjet")
	SampleRate  float64 // sampling rate 0.0-1.0 (default: 1.0 = always)
}

// Provider wraps an OTel TracerProvider for clean shutdown.
type Provider struct {
	tp     *sdktrace.TracerProvider
	tracer trace.Tracer
}

// Init initializes the OTLP trace exporter and registers a global TracerProvider.
// Returns a Provider that must be shut down on exit.
func Init(ctx context.Context, cfg Config, logger *slog.Logger) (*Provider, error) {
	if cfg.ServiceName == "" {
		cfg.ServiceName = "wadjet"
	}
	if cfg.SampleRate <= 0 {
		cfg.SampleRate = 1.0
	}

	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(cfg.Endpoint),
	}
	if cfg.Insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}

	exporter, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating OTLP exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(cfg.ServiceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("creating OTel resource: %w", err)
	}

	var sampler sdktrace.Sampler
	if cfg.SampleRate >= 1.0 {
		sampler = sdktrace.AlwaysSample()
	} else {
		sampler = sdktrace.TraceIDRatioBased(cfg.SampleRate)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)

	otel.SetTracerProvider(tp)

	logger.Info("OpenTelemetry tracing initialized",
		"endpoint", cfg.Endpoint,
		"service", cfg.ServiceName,
		"sample_rate", cfg.SampleRate,
	)

	return &Provider{
		tp:     tp,
		tracer: tp.Tracer("wadjet"),
	}, nil
}

// Shutdown flushes pending spans and shuts down the exporter.
func (p *Provider) Shutdown(ctx context.Context) error {
	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return p.tp.Shutdown(shutdownCtx)
}

// Tracer returns the Wadjet tracer for creating spans.
func (p *Provider) Tracer() trace.Tracer {
	return p.tracer
}

// StartSpan begins a new span as a child of the current context.
func (p *Provider) StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return p.tracer.Start(ctx, name, trace.WithAttributes(attrs...))
}

// StartRemoteSpan begins a span linked to a remote parent identified by
// the Wadjet TraceID/SpanID strings (from distributed.TraceContext).
// This bridges Wadjet's custom propagation to OTel spans.
func (p *Provider) StartRemoteSpan(ctx context.Context, name, traceIDHex, parentSpanIDHex string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	traceID, err := trace.TraceIDFromHex(traceIDHex)
	if err != nil {
		// Fall back to a new root span
		return p.StartSpan(ctx, name, attrs...)
	}

	spanID, err := trace.SpanIDFromHex(parentSpanIDHex)
	if err != nil {
		return p.StartSpan(ctx, name, attrs...)
	}

	remoteCtx := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})

	ctx = trace.ContextWithRemoteSpanContext(ctx, remoteCtx)
	return p.tracer.Start(ctx, name, trace.WithAttributes(attrs...))
}

// SpanIDFromContext extracts the current span's ID as a hex string.
// Useful for propagating through Wadjet's task system.
func SpanIDFromContext(ctx context.Context) string {
	sc := trace.SpanFromContext(ctx).SpanContext()
	if !sc.IsValid() {
		return ""
	}
	return sc.SpanID().String()
}

// TraceIDFromContext extracts the current trace ID as a hex string.
func TraceIDFromContext(ctx context.Context) string {
	sc := trace.SpanFromContext(ctx).SpanContext()
	if !sc.IsValid() {
		return ""
	}
	return sc.TraceID().String()
}

// TraceIDToBytes converts a 32-char hex trace ID to [16]byte.
func TraceIDToBytes(hexID string) [16]byte {
	var b [16]byte
	decoded, _ := hex.DecodeString(hexID)
	copy(b[:], decoded)
	return b
}
