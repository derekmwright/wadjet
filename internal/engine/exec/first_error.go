package exec

import "sync/atomic"

// FirstError records the first error any goroutine in a group reports.
//
// sync/atomic.Value panics when two stores into the same Value carry
// different CONCRETE types, and "whichever worker failed first" is exactly
// the slot where that happens: one worker stores what the panic boundary
// produced, another stores a *fmt.wrapError from its own return path, and
// the loser of that race takes the whole process down — every connection,
// not just the query that raced (#512, seen 17 times in one SQLancer soak).
//
// Boxing every error in this package's own struct makes the stored type
// uniform by construction, so no call site has to remember the rule and no
// future call site can break it by returning a differently-shaped error.
// Store the box, never the error.
type FirstError struct{ v atomic.Value }

// errBox is the one concrete type FirstError ever stores.
type errBox struct{ err error }

// Set records err if nothing has been recorded yet. A nil err is ignored, so
// callers can hand it an unconditional result.
func (f *FirstError) Set(err error) {
	if err != nil {
		f.v.CompareAndSwap(nil, errBox{err: err})
	}
}

// Err returns the first recorded error, or nil when none was recorded.
func (f *FirstError) Err() error {
	if b, ok := f.v.Load().(errBox); ok {
		return b.err
	}
	return nil
}
