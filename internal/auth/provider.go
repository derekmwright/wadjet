package auth

import (
	"log/slog"
	"sync/atomic"
)

// authState holds an immutable snapshot of auth components.
type authState struct {
	authn    *Authenticator
	authz    *Authorizer
	policies *PolicySet
	enabled  bool
}

// Provider wraps Authenticator, Authorizer, and PolicySet behind an atomic
// pointer so they can be swapped on config reload without locks on the read path.
// Every HTTP request reads the current state via a single atomic load.
type Provider struct {
	state  atomic.Pointer[authState]
	logger *slog.Logger
}

// NewProvider creates a Provider from initial auth components.
// Any parameter may be nil (auth disabled).
func NewProvider(authn *Authenticator, authz *Authorizer, policies *PolicySet, logger *slog.Logger) *Provider {
	if logger == nil {
		logger = slog.Default()
	}
	p := &Provider{logger: logger}
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

// UpdateFromConfig rebuilds auth from a Config and atomically swaps.
func (p *Provider) UpdateFromConfig(cfg Config, policyCfgs []PolicyConfig) {
	authn, authz := New(cfg)
	var policies *PolicySet
	if len(policyCfgs) > 0 {
		policies = ParsePolicies(policyCfgs)
	}
	p.Update(authn, authz, policies)
}
