# Join runtime key type backstop

Source: internal/engine/exec/join_key_width.go — HashJoin.checkProbeKeyTypes, moved 2026-09-11 (#1026)

checkProbeKeyTypes is the RUNTIME backstop under the plan-time resolution:
the place where both sides' ACTUAL encodings are known at the same time.

The planner resolves a key pair from DECLARED types, and a side it cannot
type resolves to KeyTypeUnresolved — which is correct for every pair whose
two sides agree and silently wrong for one whose sides do not. Rather than
refuse at plan time on "cannot type" (which would refuse every join over a
table function, an unannotated scan or a shape the declared-type walk does
not cover, most of them perfectly well-typed at run time), the refusal
lives HERE, where the question is decidable and the answer cannot be a
false positive.

Two conditions, and only these two:

	R1 the integer fast path is on and a PROBE key column is not
	   integer-class. This is #615's panic: tryEnableIntKey saw only the
	   BUILD column, and inlineIntProbe / executeSemiAntiJoin / the bloom
	   then indexed the probe column's nil Int32Data / Int64Data. An error
	   here makes that index structurally unreachable.
	R2 both key columns are on the numeric ladder, their key ENCODINGS
	   differ, and the plan said no widening. This is #615's silent miss:
	   eight little-endian bytes on one side and a canonical decimal key on
	   the other, matching only where the byte strings coincide by accident.

A pair the ladder does not describe is left where it was only when NEITHER
fast path is engaged: a DATE against a TIMESTAMP still answers no matches,
as it always has. An INTEGER build against a STRING, BOOL, UUID or CIDR
probe does NOT — R1 catches it, because the integer fast path is on and the
probe has no integer storage. That shape used to PANIC on a nil typed
slice, so the change there is a query error where there was a recovered
crash, and PostgreSQL refuses the same pair outright (42883, no operator).
Turning the remaining ill-typed pairs into errors is a separate question
with a separate authority.
