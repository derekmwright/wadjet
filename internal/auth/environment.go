package auth

import (
	"context"
	"net"
	"time"
)

type environmentKey struct{}

// ContextWithEnvironment attaches the TRUSTED request environment — the one
// the PROTOCOL BOUNDARY observed, not one any caller below it can assert.
//
// It is attached where the connection is: the HTTP middleware, the pgwire
// connection handshake and the gRPC authentication interceptor each know the
// peer address and which protocol they are, and nothing further down does.
// Anything the client can set (an `X-Forwarded-For` header, a startup
// parameter, a gRPC metadata value) is NOT trusted here — a source-address
// condition that a client could forge is not a control.
//
// `Time` is deliberately NOT stamped at attach time. A pgwire connection lives
// for hours and a policy conditioned on `env.hour` has to mean the hour the
// STATEMENT ran, not the hour the socket opened; the decision stamps it (see
// DecisionEnvironment).
//
// A `SourceIP` carrying a PORT is reduced to its host here, once, so no door
// has to remember: `net.Conn.RemoteAddr()` and `http.Request.RemoteAddr` are
// `host:port`, while `env.source_ip` is documented and written as an IP. The
// HTTP door passed `r.RemoteAddr` straight through, so a documented
// `env.source_ip eq "127.0.0.1"` rule compared against `127.0.0.1:54321` and
// never matched (#933).
func ContextWithEnvironment(ctx context.Context, env Environment) context.Context {
	env.SourceIP = hostOnly(env.SourceIP)
	return context.WithValue(ctx, environmentKey{}, env)
}

// DecisionEnvironment is the Environment a policy decision is evaluated
// against: what the protocol boundary attached, with `Time` stamped NOW and
// `protocol` as the fallback label when no boundary named one.
//
// Every shared enforcement path builds its environment through this and
// through nothing else. They used to build `Environment{Protocol: protocol}`
// by hand — no time, no address — so `env.time`, `env.hour` and
// `env.source_ip` were never published to the evaluator and every
// environment-conditioned rule matched nothing. Beside a broad allow, a deny
// that matches nothing is a grant (#933).
//
// The attached protocol WINS over the label when there is one: the label
// describes the execution path (`"embedded"` even for a statement that arrived
// over pgwire), and `env.protocol` means the door the client used.
func DecisionEnvironment(ctx context.Context, protocol string) Environment {
	env := EnvironmentFromContext(ctx)
	if env.Time.IsZero() {
		env.Time = time.Now()
	}
	if env.Protocol == "" {
		env.Protocol = protocol
	}
	return env
}

// hostOnly strips the port from a socket address, leaving an address that has
// none untouched (a bare IPv4, a bare IPv6, an empty string).
func hostOnly(addr string) string {
	if addr == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}
	return addr
}

// EnvironmentFromContext returns the environment attached by the protocol
// boundary, or the zero Environment when none was attached (an embedded
// caller, a background task, a test).
//
// The zero value is not a refusal: a policy that names no environment
// condition decides the same either way, and one that does simply does not
// match — which, beside a broad allow, is why the environment has to be
// attached on every door rather than left to whoever remembers (#933).
func EnvironmentFromContext(ctx context.Context) Environment {
	env, _ := ctx.Value(environmentKey{}).(Environment)
	return env
}
