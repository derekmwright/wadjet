package exec

import "context"

// ProgressReporter is a tiny interface the pipeline uses to report
// per-batch forward progress to the surrounding task. Implemented by
// the worker's per-task TaskProgress; the engine doesn't depend on
// any worker types because of import-cycle constraints.
//
// Callers MUST tolerate a nil receiver — it's used through
// ProgressReporterFromContext which returns nil for callers outside
// of a worker task (standalone queries, tests).
type ProgressReporter interface {
	AddRows(int64)
	AddBytes(int64)
}

type progressKey struct{}

// WithProgressReporter attaches a reporter to ctx. The worker's task
// dispatch path calls this; everyone else reads via
// ProgressReporterFromContext.
func WithProgressReporter(ctx context.Context, p ProgressReporter) context.Context {
	if p == nil {
		return ctx
	}
	return context.WithValue(ctx, progressKey{}, p)
}

// ProgressReporterFromContext returns the active progress reporter or
// nil if none. AddRows / AddBytes on the returned value are nil-safe
// only when the type embeds the nil-tolerance behaviour itself —
// callers should ALWAYS guard with `if p != nil` before invoking.
func ProgressReporterFromContext(ctx context.Context) ProgressReporter {
	if ctx == nil {
		return nil
	}
	v := ctx.Value(progressKey{})
	if v == nil {
		return nil
	}
	p, _ := v.(ProgressReporter)
	return p
}
