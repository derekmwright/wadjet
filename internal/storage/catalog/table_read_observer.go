// SPDX-License-Identifier: MIT

package catalog

import "context"

// TableReadObserver is told of every table-record read (GetTable) made under
// a context that carries it, with the record read or the error, before the
// caller sees either. It runs on the reader's goroutine and must be safe for
// concurrent use. It cannot change what the read returns.
//
// One observer exists: a coordinator statement's table-incarnation check
// (ADR-0050 §3, physical.withIncarnationGuard), which pins the table's
// manifest at the statement's first read of its record and compares every
// record the statement reads with that pin. A context without an observer
// reads exactly as before.
type TableReadObserver func(ctx context.Context, name string, meta *TableMeta, err error)

type tableReadObserverKey struct{}

// WithTableReadObserver returns ctx carrying obs for every GetTable made
// under it.
func WithTableReadObserver(ctx context.Context, obs TableReadObserver) context.Context {
	return context.WithValue(ctx, tableReadObserverKey{}, obs)
}

// WithoutTableReadObserver returns ctx with no observer: a read that is not
// about a relation the statement reads (a system relation's listing of every
// table's columns) or the observer's own reads.
func WithoutTableReadObserver(ctx context.Context) context.Context {
	if tableReadObserverFrom(ctx) == nil {
		return ctx
	}
	return context.WithValue(ctx, tableReadObserverKey{}, TableReadObserver(nil))
}

func tableReadObserverFrom(ctx context.Context) TableReadObserver {
	if ctx == nil {
		return nil
	}
	obs, _ := ctx.Value(tableReadObserverKey{}).(TableReadObserver)
	return obs
}
