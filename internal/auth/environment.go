package auth

import "context"

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
// TableAccess).
func ContextWithEnvironment(ctx context.Context, env Environment) context.Context {
	return context.WithValue(ctx, environmentKey{}, env)
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
