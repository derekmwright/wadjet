// Package sqlerr carries a PostgreSQL SQLSTATE alongside an error message.
//
// Wadjet speaks the PostgreSQL wire protocol, and a client branches on the
// SQLSTATE it is handed: 42P01 sends it to re-resolve a table name, 42703 to
// re-resolve a column, 22012 to report a data error. Before this package every
// failure crossed the wire as the blanket class 42000 (#366), because the
// planner and engine produced plain errors and the pgwire layer had nothing to
// extract a code from.
//
// An error created here travels through ordinary %w wrapping; the pgwire layer
// recovers the code with StateOf. Errors defined elsewhere can participate
// without importing this package by implementing the Coder interface.
package sqlerr

import (
	"errors"
	"fmt"
)

// Error is an error with a PostgreSQL SQLSTATE code.
type Error struct {
	Code    string // five-character SQLSTATE, e.g. "42P01"
	Message string
}

func (e *Error) Error() string { return e.Message }

// SQLState returns the code; Error participates in StateOf through the same
// Coder interface as foreign error types, so the OUTERMOST code in a wrap
// chain always wins regardless of which type carries it.
func (e *Error) SQLState() string { return e.Code }

// New builds an Error with the given SQLSTATE and formatted message.
// Quote renders a value's SOURCE TEXT the way PostgreSQL renders it inside an
// error message: between plain double quotes, BYTE for byte.
//
// Go's %q is not that. It escapes every non-ASCII byte, so a literal holding a
// NBSP came out as `"\u00a042"` where PostgreSQL emits the two characters
// themselves, and a client comparing the message — or a person reading it —
// sees text nobody typed (#638). PostgreSQL quotes with a bare
// `"%s"` and lets the bytes through, including an embedded quote character.
func Quote(s string) string { return `"` + s + `"` }

func New(code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// Coder is implemented by error types that know their own SQLSTATE but do not
// depend on this package (e.g. expr.UnknownFuncError).
type Coder interface {
	SQLState() string
}

// Wrap attaches a SQLSTATE to an existing error without disturbing its chain:
// the result unwraps to err, so errors.Is/As against the original still hold.
func Wrap(code string, err error) error {
	if err == nil {
		return nil
	}
	return &wrapped{code: code, err: err}
}

type wrapped struct {
	code string
	err  error
}

func (w *wrapped) Error() string    { return w.err.Error() }
func (w *wrapped) Unwrap() error    { return w.err }
func (w *wrapped) SQLState() string { return w.code }

// StateOf returns the SQLSTATE carried by err or anything it wraps, or ""
// when none is found. The outermost carrier in the wrap chain wins.
func StateOf(err error) string {
	var c Coder
	if errors.As(err, &c) {
		return c.SQLState()
	}
	return ""
}
