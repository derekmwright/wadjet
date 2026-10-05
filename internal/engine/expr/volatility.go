// SPDX-License-Identifier: MIT

package expr

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// volatileBuiltins are the builtin functions this engine registers whose
// value is not fixed by their arguments: two calls with the same arguments
// may answer differently. PostgreSQL marks its own spellings VOLATILE
// (random()); rand() and uuid() are this engine's. The clock functions are
// NOT here: a statement reads its clock once (arc SC, IsClockFunc).
//
// TestEveryRegisteredVolatileFunctionIsMarked walks the registry and calls
// every function that takes no argument repeatedly, so a newly registered
// function whose answers differ cannot be left out of this set.
var volatileBuiltins = map[string]bool{
	"rand":   true,
	"random": true,
	"uuid":   true,
}

func init() {
	plansql.SetVolatileFunctionOracle(IsVolatileFunction)
}

// VolatileBuiltins lists the builtin functions marked volatile.
func VolatileBuiltins() []string {
	out := make([]string, 0, len(volatileBuiltins))
	for name := range volatileBuiltins {
		out = append(out, name)
	}
	return out
}

// IsVolatileBuiltin reports whether name is a builtin marked volatile.
func IsVolatileBuiltin(name string) bool {
	return volatileBuiltins[strings.ToLower(name)]
}

// IsVolatileFunction answers the one question the planner asks of a call:
// may two calls with the same arguments answer differently?
//
// A builtin answers by its mark. A function CREATE FUNCTION defined answers
// by its BODY: it is volatile when the body calls a volatile function,
// followed through every function the body calls (a cycle is cut where it
// closes; the store refuses one at CREATE time anyway). A function an
// embedding program registered from Go has no body to read and is volatile —
// PostgreSQL's default for a function not declared IMMUTABLE or STABLE.
func IsVolatileFunction(name string) bool {
	return isVolatileFunction(strings.ToLower(name), map[string]bool{})
}

func isVolatileFunction(name string, seen map[string]bool) bool {
	if volatileBuiltins[name] {
		return true
	}
	if !DefaultRegistry.IsUDF(name) || seen[name] {
		return false
	}
	seen[name] = true
	def, ok := DefaultUDFs.Get(name)
	if !ok {
		return true
	}
	return plansql.TextCallsAny(def.Body, func(fn string) bool {
		return isVolatileFunction(strings.ToLower(fn), seen)
	})
}
