# Auth trusted request environment

Source: internal/auth/environment.go — func ContextWithEnvironment(ctx context.Context, env Environment) context.Context {, moved 2026-09-11 (#1026)

ContextWithEnvironment attaches the TRUSTED request environment — the one
the PROTOCOL BOUNDARY observed, not one any caller below it can assert.

It is attached where the connection is: the HTTP middleware, the pgwire
connection handshake and the gRPC authentication interceptor each know the
peer address and which protocol they are, and nothing further down does.
Anything the client can set (an `X-Forwarded-For` header, a startup
parameter, a gRPC metadata value) is NOT trusted here — a source-address
condition that a client could forge is not a control.

`Time` is deliberately NOT stamped at attach time. A pgwire connection lives
for hours and a policy conditioned on `env.hour` has to mean the hour the
STATEMENT ran, not the hour the socket opened; the decision stamps it (see
DecisionEnvironment).

A `SourceIP` carrying a PORT is reduced to its host here, once, so no door
has to remember: `net.Conn.RemoteAddr()` and `http.Request.RemoteAddr` are
`host:port`, while `env.source_ip` is documented and written as an IP. The
HTTP door passed `r.RemoteAddr` straight through, so a documented
`env.source_ip eq "127.0.0.1"` rule compared against `127.0.0.1:54321` and
never matched (#933).
