// SPDX-License-Identifier: MIT

package expr

import (
	"fmt"
	"sort"
	"testing"
)

// THE VOLATILE SET IS THE REGISTRY'S, AND IT CANNOT FORGET ONE (#1531 round
// 3). Every registered function callable with no argument is called four
// times: one whose answers differ must be marked volatile, and every mark
// must name a registered function whose answers do differ. The clock
// functions are statement-stable (a statement reads its clock once) and are
// not volatile here.
func TestEveryRegisteredVolatileFunctionIsMarked(t *testing.T) {
	differs := func(name string) (d bool) {
		fn := DefaultRegistry.Lookup(name)
		if fn == nil {
			return false
		}
		defer func() {
			if recover() != nil {
				d = false
			}
		}()
		first := fmt.Sprint(fn(nil))
		for i := 0; i < 3; i++ {
			if fmt.Sprint(fn(nil)) != first {
				return true
			}
		}
		return false
	}
	var observed []string
	for _, name := range DefaultRegistry.Names() {
		if IsClockFunc(name) || DefaultRegistry.IsUDF(name) {
			continue
		}
		if sig, ok := SignatureOf(name); ok && !sig.Accepts(0) {
			continue
		}
		if differs(name) {
			observed = append(observed, name)
			if !IsVolatileBuiltin(name) {
				t.Errorf("%s() answers differently call to call but is not marked volatile", name)
			}
		}
	}
	for _, name := range VolatileBuiltins() {
		if !DefaultRegistry.Has(name) {
			t.Errorf("%s is marked volatile but is not a registered function", name)
		} else if !differs(name) {
			t.Errorf("%s is marked volatile but answers the same call to call", name)
		}
	}
	sort.Strings(observed)
	t.Logf("volatile builtins observed: %v", observed)
}

// A function CREATE FUNCTION defined is volatile when its BODY is, followed
// through the functions the body calls.
func TestUserFunctionVolatilityIsItsBody(t *testing.T) {
	defs := []UDFDef{
		{Name: "vol_t_r", Body: "random() * 2"},
		{Name: "vol_t_r2", Body: "vol_t_r() + 1"},
		{Name: "vol_t_d", Params: []string{"x"}, Body: "x * 2"},
		{Name: "vol_t_d2", Params: []string{"x"}, Body: "vol_t_d(x) + abs(x)"},
		{Name: "vol_t_lit", Body: "'random()'"},
	}
	for _, d := range defs {
		if err := DefaultUDFs.Register(d, true); err != nil {
			t.Fatalf("register %s: %v", d.Name, err)
		}
	}
	t.Cleanup(func() {
		for i := len(defs) - 1; i >= 0; i-- {
			_ = DefaultUDFs.Unregister(defs[i].Name, "", true)
		}
	})
	for name, want := range map[string]bool{
		"vol_t_r": true, "vol_t_r2": true, "VOL_T_R2": true, "vol_t_d": false, "vol_t_d2": false, "vol_t_lit": false,
		"random": true, "uuid": true, "abs": false, "now": false,
	} {
		if got := IsVolatileFunction(name); got != want {
			t.Errorf("IsVolatileFunction(%s) = %v, want %v", name, got, want)
		}
	}
}
