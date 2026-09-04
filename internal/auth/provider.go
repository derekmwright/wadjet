package auth

import (
	"log/slog"
	"sync/atomic"
)

// authState holds an immutable snapshot of auth components.
type authState struct {
	authn     *Authenticator
	authz     *Authorizer
	policies  *PolicySet
	evaluator *PolicyEvaluator
	enabled   bool
}

// Provider wraps Authenticator, Authorizer, PolicySet, and PolicyEvaluator behind
// an atomic pointer so they can be swapped on config reload without locks.
type Provider struct {
	state  atomic.Pointer[authState]
	logger *slog.Logger
	// audit records what a policy DECIDED. It lives here, beside the
	// evaluator, so the one enforcement path carries the one audit point:
	// LogColumnPolicy used to be called from internal/server's HTTP handler
	// alone, over the result ROWS, so the embedded and pgwire doors — which
	// enforce through exactly the same call — recorded nothing, and a query
	// that returned no rows recorded nothing on any door (#859).
	audit *AuditLogger
}

// Audit returns the provider's audit logger. Never nil.
func (p *Provider) Audit() *AuditLogger {
	if p == nil {
		return nil
	}
	return p.audit
}

// NewProvider creates a Provider from initial auth components.
// Any parameter may be nil (auth disabled).
func NewProvider(authn *Authenticator, authz *Authorizer, policies *PolicySet, logger *slog.Logger) *Provider {
	if logger == nil {
		logger = slog.Default()
	}
	p := &Provider{logger: logger, audit: NewAuditLogger(logger)}
	enabled := authn != nil && authn.Enabled()
	p.state.Store(&authState{
		authn:    authn,
		authz:    authz,
		policies: policies,
		enabled:  enabled,
	})
	return p
}

// Authenticator returns the current Authenticator. Lock-free.
func (p *Provider) Authenticator() *Authenticator {
	return p.state.Load().authn
}

// Authorizer returns the current Authorizer. Lock-free.
func (p *Provider) Authorizer() *Authorizer {
	return p.state.Load().authz
}

// Policies returns the current PolicySet. Lock-free.
func (p *Provider) Policies() *PolicySet {
	return p.state.Load().policies
}

// Evaluator returns the current ABAC PolicyEvaluator. Lock-free.
func (p *Provider) Evaluator() *PolicyEvaluator {
	return p.state.Load().evaluator
}

// Enabled returns whether authentication is currently active. Lock-free.
func (p *Provider) Enabled() bool {
	return p.state.Load().enabled
}

// Update atomically replaces all auth components.
func (p *Provider) Update(authn *Authenticator, authz *Authorizer, policies *PolicySet) {
	enabled := authn != nil && authn.Enabled()
	p.state.Store(&authState{
		authn:    authn,
		authz:    authz,
		policies: policies,
		enabled:  enabled,
	})
	p.logger.Info("auth provider updated", "enabled", enabled)
}

// UpdateWithEvaluator atomically replaces all auth components including the ABAC evaluator.
func (p *Provider) UpdateWithEvaluator(authn *Authenticator, authz *Authorizer, policies *PolicySet, evaluator *PolicyEvaluator) {
	enabled := authn != nil && authn.Enabled()
	p.state.Store(&authState{
		authn:     authn,
		authz:     authz,
		policies:  policies,
		evaluator: evaluator,
		enabled:   enabled,
	})
	p.logger.Info("auth provider updated", "enabled", enabled, "abac", evaluator != nil)
}

// UpdateFromConfig rebuilds auth from a Config and atomically swaps.
// If abacPolicies is non-empty, builds an ABAC evaluator. Otherwise, if RBAC
// roles and cell policies are present, auto-migrates them to ABAC.
//
// A policy this cannot read returns an error and swaps NOTHING: the provider
// keeps the state it already had. That matters most on hot reload, where the
// alternative to refusing an unreadable `columns:` action is installing a
// weaker policy set than the operator asked for (#802).
func (p *Provider) UpdateFromConfig(cfg Config, policyCfgs []PolicyConfig, abacPolicies ...AccessControlPolicy) error {
	authn, authz := New(cfg)
	var legacyPolicies *PolicySet
	if len(policyCfgs) > 0 {
		var err error
		legacyPolicies, err = ParsePolicies(policyCfgs)
		if err != nil {
			return err
		}
	}

	var evaluator *PolicyEvaluator
	if len(abacPolicies) > 0 {
		// An obligation that cannot be enforced as written refuses here, the
		// way an unreadable `columns:` action already does (#802, #859).
		if err := ValidateABACPolicies(abacPolicies); err != nil {
			return err
		}
		evaluator = NewPolicyEvaluator(abacPolicies)
	} else if len(cfg.Roles) > 0 {
		// Auto-migrate RBAC to ABAC
		migrated, err := MigrateRBACToABAC(cfg.Roles, policyCfgs)
		if err != nil {
			return err
		}
		if err := ValidateABACPolicies(migrated); err != nil {
			return err
		}
		evaluator = NewPolicyEvaluator(migrated)
	}

	p.UpdateWithEvaluator(authn, authz, legacyPolicies, evaluator)
	return nil
}
