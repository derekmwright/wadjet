package distributed

import (
	"context"
	"crypto/rand"
	"encoding/hex"
)

type traceCtxKey struct{}

// TraceContext carries distributed trace identifiers across service boundaries.
type TraceContext struct {
	TraceID    string // 32-char hex
	SpanID     string // 16-char hex
	TraceFlags byte   // 0x01 = sampled
}

// NewTraceContext generates a new root trace with a random trace ID and span ID.
func NewTraceContext() TraceContext {
	return TraceContext{
		TraceID:    randomHex(16),
		SpanID:     randomHex(8),
		TraceFlags: 0x01,
	}
}

// NewSpanID generates a new random span ID within the same trace.
func (tc TraceContext) NewSpanID() TraceContext {
	return TraceContext{
		TraceID:    tc.TraceID,
		SpanID:     randomHex(8),
		TraceFlags: tc.TraceFlags,
	}
}

// ContextWithTrace stores a TraceContext in a Go context.
func ContextWithTrace(ctx context.Context, tc TraceContext) context.Context {
	return context.WithValue(ctx, traceCtxKey{}, tc)
}

// TraceFromContext extracts a TraceContext from a Go context. Returns zero
// value if none is set.
func TraceFromContext(ctx context.Context) TraceContext {
	tc, _ := ctx.Value(traceCtxKey{}).(TraceContext)
	return tc
}

func randomHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}
