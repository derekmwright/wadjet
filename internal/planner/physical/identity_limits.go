package physical

import (
	"context"

	"github.com/derekmwright/wadjet/internal/config"
)

// The cost guard a POLICY sets, as opposed to the one the config file sets.
//
// `query_limits:` in the config file is per deployment and per role, and it
// reaches the planner through Planner.QueryLimits at each door's one
// planner-construction site. An ABAC `query_limit` obligation is per IDENTITY
// and per RELATION — it is decided while the policies are evaluated, which is
// after every planner has been built — so it travels on the context instead
// and is merged into the guard at the point the guard runs. That keeps ONE
// enforcement point for both (ADR-0033 decision 5: one enforcement path), and
// it means a door that gains a planner-construction site later carries the
// obligation without remembering to.
type identityLimitsKey struct{}

// WithIdentityQueryLimits attaches the limits an identity's policies impose.
// auth.EnforcePlanPolicies is its one caller.
func WithIdentityQueryLimits(ctx context.Context, lim *config.QueryLimits) context.Context {
	if lim == nil {
		return ctx
	}
	return context.WithValue(ctx, identityLimitsKey{}, lim)
}

// IdentityQueryLimitsFromContext returns those limits, or nil.
func IdentityQueryLimitsFromContext(ctx context.Context) *config.QueryLimits {
	if ctx == nil {
		return nil
	}
	lim, _ := ctx.Value(identityLimitsKey{}).(*config.QueryLimits)
	return lim
}

// tightestLimits combines the configured guard with the identity's, taking the
// SMALLER of each knob a limit is set on. A policy can only narrow: an
// obligation naming a bigger number than the deployment allows does not widen
// the deployment's guard, and a deployment with no guard at all still enforces
// the obligation.
func tightestLimits(configured, identity *config.QueryLimits) *config.QueryLimits {
	if identity == nil {
		return configured
	}
	if configured == nil {
		return identity
	}
	out := *configured
	tighten64 := func(dst *int64, src int64) {
		if src > 0 && (*dst == 0 || src < *dst) {
			*dst = src
		}
	}
	tighten64(&out.MaxScanBytes, identity.MaxScanBytes)
	tighten64(&out.MaxScanRows, identity.MaxScanRows)
	tighten64(&out.RequireFilterAboveBytes, identity.RequireFilterAboveBytes)
	tighten64(&out.RequireLimitAboveRows, identity.RequireLimitAboveRows)
	if identity.MaxScanFiles > 0 && (out.MaxScanFiles == 0 || identity.MaxScanFiles < out.MaxScanFiles) {
		out.MaxScanFiles = identity.MaxScanFiles
	}
	return &out
}
