# Server grpc query error class

Source: internal/server/grpc.go — func grpcQueryError(err error) error {, moved 2026-09-11 (#1026)
Superseded: SELECT table-access refusals now carry SQLSTATE42501 through the shared auth.TableAccess path and map to PermissionDenied here.

grpcQueryError maps an engine error onto this door's status code.

An authorization refusal is codes.PermissionDenied, not codes.Internal. Both
SQL RPCs wrapped EVERY engine error as Internal, so a reader whose DELETE was
refused with SQLSTATE 42501 before a single row was touched was told the
server had failed — a client cannot tell "you may not do that" from "we
broke", retries the second and gives up on the first, and an operator reading
the code alone sees an outage where there is a working control. The other
doors carry the class: HTTP 403, pgwire 42501 (SECURITY ADDENDUM 6).

It branches on the CLASS the error CARRIES — the SQLSTATE 42501 that
auth.EnforceDMLPolicies / auth.TableAccess attach, or auth.ErrUnauthorized
that auth.RequirePermission wraps — never on message text. A door that
pattern-matched sentences would silently reopen the day a message is
reworded, and would be a second copy of the decision besides.

One refusal does NOT reach this yet: a SELECT denied at the table level
returns a plain error from internal/auth/plan_enforce.go, with no class on
any door, so it still crosses as Internal here. Attaching 42501 there is an
internal/auth change (it moves pgwire's SQLSTATE too); when it lands this
mapping needs no edit.
