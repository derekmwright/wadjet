// SPDX-License-Identifier: MIT

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
	// Reason is the stable identity of an intentional capability refusal
	// (Refuse): a short kebab-case name a test or a tool reads instead of
	// matching words in Message, which may change. Empty for every other
	// error.
	Reason string
}

// RefusalReason is the refusal identity (ReasonOf).
func (e *Error) RefusalReason() string { return e.Reason }

// Refuse builds an intentional capability refusal: SQLSTATE 0A000 with the
// stable identity reason beside the sentence (ReasonOf). The sentence is
// what a client reads; reason is what a test reads (#1680 stage A: a
// refusal is credited by its identity, never by words in its sentence).
func Refuse(reason, format string, args ...any) *Error {
	return &Error{Code: "0A000", Message: fmt.Sprintf(format, args...), Reason: reason}
}

// Reasoner is implemented by a coded error that can carry a refusal
// identity across a boundary that rebuilds it (a worker's task failure).
type Reasoner interface {
	RefusalReason() string
}

// Attacher is implemented by a coded error rebuilt across a boundary that
// records whether its code was ATTACHED (Attached) where it was raised.
type Attacher interface {
	CodeAttached() bool
}

// raiser is the error in err's chain that raised its code: the DEEPEST
// carrier of a non-empty SQLSTATE (the refusal where it was raised; the
// layers above only frame or wrap it), nil when none carries one.
func raiser(err error) error {
	var deepest error
	for e := err; e != nil; e = errors.Unwrap(e) {
		if c, ok := e.(Coder); ok && c.SQLState() != "" {
			deepest = e
		}
	}
	return deepest
}

// ReasonOf is the refusal identity of the error that raised err's code
// (Refuse), "" when that error is no intentional refusal.
func ReasonOf(err error) string {
	if r, ok := raiser(err).(Reasoner); ok {
		return r.RefusalReason()
	}
	return ""
}

// Attached reports an error whose code was attached by Wrap to a chain
// that carried none: the code says what the wrapping layer guessed, not
// what the failing code raised, so the error's PROVENANCE is an uncoded
// failure. A test deciding whether an engine answer is an internal failure
// reads this (and StateOf), never words in the sentence.
func Attached(err error) bool {
	switch r := raiser(err).(type) {
	case *wrapped:
		return true
	case Attacher:
		return r.CodeAttached()
	}
	return false
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

// SentenceOf is the text a client is owed for a coded error: the message of
// the DEEPEST error in err's wrap chain that carries a SQLSTATE — the
// refusal where it was raised — without the stage and task labels the
// layers above it wrap around it on the way out. An uncoded error has no
// sentence ("").
//
// It is how a stage-DAG task failure reaches a client with PostgreSQL's
// sentence: `division by zero`, not `native DAG: stage scan-0 (scan): …:
// task 9f… failed after 3 attempts: fragment task 9f…: division by zero`,
// which is what made the embedded and the DAG servers send two messages for
// one refusal (#1145).
func SentenceOf(err error) string {
	sentence := ""
	for e := err; e != nil; e = errors.Unwrap(e) {
		if c, ok := e.(Coder); ok && c.SQLState() != "" {
			sentence = e.Error()
		}
	}
	return sentence
}

// Sentence is err as a client receives it at a door's boundary: a coded
// error carries the sentence of the refusal that raised it (SentenceOf),
// without the stage labels a layer above wrapped around it — `parsing
// derived table:`, `building plan for derived table:`, `executing query:` —
// wherever in the planner those labels were added (#1145). The original
// chain stays reachable through Unwrap, so errors.As/Is and StateOf see
// exactly what they saw. An uncoded error is an internal failure and is
// returned as it is, labels and all.
func Sentence(err error) error {
	if err == nil {
		return nil
	}
	code := StateOf(err)
	if code == "" {
		return err
	}
	msg := SentenceOf(err)
	if msg == "" || msg == err.Error() {
		return err
	}
	return &sentence{code: code, msg: msg, err: err}
}

type sentence struct {
	code, msg string
	err       error
}

func (s *sentence) Error() string    { return s.msg }
func (s *sentence) SQLState() string { return s.code }
func (s *sentence) Unwrap() error    { return s.err }
