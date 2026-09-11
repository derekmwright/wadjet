# Scan sanitize switch gate boundary

Source: internal/planner/logical/optimizer.go — ScanColSanitizeSwitch, moved 2026-09-11 (#1026)

ScanColSanitizeSwitch gates the DROPPING half of sanitizeScanNeeds — the
pollution A/B, and nothing else. It deliberately does NOT gate the
schema-spelling half; see the comment on that arm for why an optimization
switch must not decide which columns a scan reads.

It is REGISTERED, which is what puts it under the optimization-invariance
oracle: the oracle runs the corpus with each switch individually disabled
and requires identical results, and that is exactly the property this switch
lacked. Until #731's follow-up it changed 30 of the CamelCase battery's 63
cells when disabled — a switch load-bearing for correctness, which inverts
the doctrine registration exists to enforce. Registering it is how the
property stays true rather than being true today.

It is EXPORTED because the gate that can actually see it lives in another
package: the optimization-invariance oracle sweeps every registered switch
over TPC-H, whose columns are all lower case, and there the folded reference
and the schema spelling are the SAME STRING — disabling this switch on that
corpus cannot change a row by construction. The corpus that can see it is
the CamelCase invariance battery in internal/coordinator, and it drives both
states through this handle rather than reading the env var, so one run
covers both.
