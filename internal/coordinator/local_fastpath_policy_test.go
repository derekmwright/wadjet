package coordinator

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// TestClassifyLocalFailure pins the #308 policy: a local fast-path execution
// failure is retried on the DAG only when it says nothing about what the
// query MEANS. Anything else is the query's outcome, because the alternative
// — running it again on a path that may disagree — turns a loud failure into
// a silently different answer.
func TestClassifyLocalFailure(t *testing.T) {
	deterministic := errors.New("unknown column \"l_shipdat\"")

	tests := []struct {
		name   string
		err    error
		ctxErr error
		strict bool
		want   localOutcome
	}{
		// Retriable: unrelated to the query's meaning.
		{"result budget bail-out", exec.ErrCollectBudget, nil, true, retryOnDAG},
		{"wrapped budget bail-out",
			fmt.Errorf("collect sink: %w", exec.ErrCollectBudget), nil, true, retryOnDAG},
		{"local memory budget", memory.ErrMemoryExceeded, nil, true, retryOnDAG},
		{"wrapped memory budget",
			fmt.Errorf("hash aggregate: %w", memory.ErrMemoryExceeded), nil, true, retryOnDAG},
		{"object store unreachable", objstore.ErrCircuitOpen, nil, true, retryOnDAG},

		// Deterministic: the DAG must not get a second opinion.
		{"deterministic query error", deterministic, nil, true, reportToClient},
		{"wrapped deterministic error",
			fmt.Errorf("projecting: %w", deterministic), nil, true, reportToClient},

		// Cancellation outranks everything, including the kill switch and
		// the otherwise-retriable classes: the caller asked for the query
		// to STOP, so re-running it is both waste and a misleading error.
		{"cancelled", context.Canceled, context.Canceled, true, reportToClient},
		{"cancelled, switch off", context.Canceled, context.Canceled, false, reportToClient},
		{"deadline exceeded", context.DeadlineExceeded, context.DeadlineExceeded, false, reportToClient},
		{"budget bail during cancellation",
			exec.ErrCollectBudget, context.Canceled, true, reportToClient},

		// Kill switch restores the unconditional fallback for the
		// deterministic class only.
		{"deterministic, switch off", deterministic, nil, false, retryOnDAG},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyLocalFailure(tt.err, tt.ctxErr, tt.strict)
			if got != tt.want {
				t.Errorf("classifyLocalFailure(%v, ctxErr=%v, strict=%v) = %v, want %v",
					tt.err, tt.ctxErr, tt.strict, got, tt.want)
			}
		})
	}
}

// TestFastPathStrictDefaultsOn guards the default: correctness-first unless
// an operator explicitly opts out.
func TestFastPathStrictDefaultsOn(t *testing.T) {
	if !FastPathStrict.On() {
		t.Error("fastpath-strict is off by default; a divergent DAG answer would be served silently")
	}
}
