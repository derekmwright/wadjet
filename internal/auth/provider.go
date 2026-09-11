package auth

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
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
	// cat is the catalog a policy's NAMES are bound against, once, at load
	// (BindPoliciesToCatalog). It is set after the catalog exists — the
	// provider is built from the config file, which is read before anything
	// connects — so the first bind happens in BindToCatalog and every
	// subsequent one inside UpdateFromConfig, which is the hot-reload path.
	// nil means no catalog was ever attached, and then nothing is bound: the
	// fold-aware comparison in relationEq / policyKey is the floor either way.
	cat atomic.Pointer[catalog.Catalog]
	// bindErr is the last bind refusal, or nil. See BindError.
	bindErr atomic.Pointer[error]
	// bound is the authState that IS bound to cat, so a re-attach of a set
	// already bound to the same catalog writes NOTHING. That matters twice:
	// the HTTP DML door re-attaches on every statement (Server.dml), and a
	// re-attach that rewrites state is a lost update waiting for a hot reload
	// to land inside it.
	bound atomic.Pointer[authState]
}

// bindSwapAttempts bounds the CAS retry in BindToCatalog. A retry happens only
// when a policy set is installed WHILE a bind is in flight; exhausting this
// many is not a contended lock, it is a caller reinstalling in a loop, and the
// answer to that is a refusal rather than an unbounded spin on a security
// decision.
const bindSwapAttempts = 32

// bindSwapTestHook runs between the bind and the swap. Tests set it to widen
// the window a lost update would need (the knob pattern of
// exec.ForceAggDrainEvery: a defect whose trigger is a SCHEDULE cannot be
// gated by hoping the scheduler cooperates). Nil in every non-test build.
var bindSwapTestHook func()

// BindToCatalog attaches the catalog and binds a COPY of the current policy set.
// Already bound catalog/state is idempotent and writes nothing, including
// request-path reattachment. Failure swaps no policy set and records BindError
// so enforcement refuses even if the caller ignores the returned error.
// Install via CAS, never Store over a concurrently reloaded set; on CAS loss
// bind the newly installed set. Exhausted retries refuse rather than revert.
// See docs/internals/auth-policy-binding-cas.md for the design.
func (p *Provider) BindToCatalog(ctx context.Context, cat *catalog.Catalog) error {
	if p == nil || cat == nil {
		return nil
	}
	for attempt := 0; attempt < bindSwapAttempts; attempt++ {
		st := p.state.Load()
		if p.cat.Load() == cat && p.bound.Load() == st {
			return nil // already bound to this catalog: nothing to rewrite
		}
		var abac []AccessControlPolicy
		if st.evaluator != nil {
			abac = st.evaluator.policies
		}
		boundABAC, boundLegacy, err := BindPoliciesToCatalog(ctx, cat, abac, st.policies)
		if err != nil {
			// An unbindable set is not installed and not enforced. It is also
			// REMEMBERED: a caller that ignores this error must not end up
			// enforcing the unbound set, so every enforcement entry point asks
			// BindError() first and refuses. Fail closed, loudly, rather than
			// quietly on the floor.
			p.cat.Store(cat)
			p.bindErr.Store(&err)
			return err
		}
		var evaluator *PolicyEvaluator
		if st.evaluator != nil {
			evaluator = &PolicyEvaluator{policies: boundABAC}
		}
		next := &authState{
			authn:     st.authn,
			authz:     st.authz,
			policies:  boundLegacy,
			evaluator: evaluator,
			enabled:   st.enabled,
		}
		if bindSwapTestHook != nil {
			bindSwapTestHook()
		}
		// The set that was running keeps running until this line; nothing
		// observes a half-rewritten policy, and nothing observes an older set
		// after a newer install.
		if !p.state.CompareAndSwap(st, next) {
			continue
		}
		p.bound.Store(next)
		p.cat.Store(cat)
		p.bindErr.Store(nil)
		return nil
	}
	err := fmt.Errorf("binding the policy set to the catalog: the set was replaced %d times "+
		"while binding", bindSwapAttempts)
	p.cat.Store(cat)
	p.bindErr.Store(&err)
	return err
}

// AttachProvider binds p to cat from a constructor that has no error return.
//
// It is BindToCatalog for the three doors whose constructors predate the bind
// (`server.New`, `pgwire.NewServer`, `NewGRPCServer`): the refusal is logged
// and, more importantly, REMEMBERED, so `Provider.BindError` refuses every
// statement rather than letting an unbound set enforce nothing. Attaching a
// provider to a catalog goes through this or through BindToCatalog and through
// nothing else — `TestEveryProviderFieldIsAttachedThroughTheBindingFunction`
// fails when a site appears that does neither.
func AttachProvider(ctx context.Context, p *Provider, cat *catalog.Catalog, logger *slog.Logger) {
	if p == nil || cat == nil {
		return
	}
	if err := p.BindToCatalog(ctx, cat); err != nil && logger != nil {
		logger.Error("auth policy set REFUSED: it names a relation or column the catalog "+
			"does not hold; every query will be refused until it is corrected", "error", err)
	}
}

// BindError reports why the attached policy set could not be bound to the
// catalog, or nil.
//
// A non-nil value is a REFUSAL, not a warning: `EnforcePlanPolicies` and
// `EnforceDMLPolicies` both return it rather than run, so a set that names a
// relation the catalog does not hold cannot be attached and then silently
// enforce nothing. That is ADR-0033 rule 3 read the way an attach has to
// implement it — the alternative is a policy file that loads clean, matches
// nothing, and beside a broad allow is a grant (#882).
func (p *Provider) BindError() error {
	if p == nil {
		return nil
	}
	if e := p.bindErr.Load(); e != nil {
		return *e
	}
	return nil
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

// installState binds st to the attached catalog and makes it the running set.
//
// Every path that installs a policy set goes through here, which is what makes
// ADR-0033 rule 2 — a set that reaches a catalog is bound to it — hold by
// CONSTRUCTION rather than by each caller remembering. `Update` and
// `UpdateWithEvaluator` used to store a new set and touch neither the binding
// nor `bindErr`, so on an already-bound provider the new, UNBOUND set was
// enforced with `BindError()` still nil: a policy naming `HITS` against a
// catalog `Hits` matched nothing, and beside a broad allow a rule that matches
// nothing is a grant (#882).
//
// With no catalog attached there is nothing to bind against and the set is
// installed as it is. That is ADR-0033 rule 4's floor — the fold-aware
// comparison covers the catalog spelling and the folded one — and it is
// deliberately NOT a refusal: a provider built with an evaluator and never
// attached to a catalog is the documented embedded shape, and refusing there
// would take a working deployment down rather than close a hole.
func (p *Provider) installState(st *authState) error {
	cat := p.cat.Load()
	if cat == nil {
		p.state.Store(st)
		return nil
	}
	var abac []AccessControlPolicy
	if st.evaluator != nil {
		abac = st.evaluator.policies
	}
	boundABAC, boundLegacy, err := BindPoliciesToCatalog(context.Background(), cat, abac, st.policies)
	if err != nil {
		// #802's contract: a set that cannot be installed installs nothing and
		// the previous one keeps running — and the refusal is REMEMBERED, so a
		// caller with no error return does not enforce the unbound set.
		p.bindErr.Store(&err)
		return err
	}
	var evaluator *PolicyEvaluator
	if st.evaluator != nil {
		evaluator = &PolicyEvaluator{policies: boundABAC}
	}
	next := &authState{
		authn:     st.authn,
		authz:     st.authz,
		policies:  boundLegacy,
		evaluator: evaluator,
		enabled:   st.enabled,
	}
	p.state.Store(next)
	p.bound.Store(next)
	p.bindErr.Store(nil)
	return nil
}

// Update atomically replaces all auth components, binding them to the attached
// catalog (installState). A set that cannot be bound is NOT installed, the
// previous one keeps running, and the refusal is remembered: every statement is
// refused until a bindable set is installed.
func (p *Provider) Update(authn *Authenticator, authz *Authorizer, policies *PolicySet) {
	enabled := authn != nil && authn.Enabled()
	if err := p.installState(&authState{
		authn:    authn,
		authz:    authz,
		policies: policies,
		enabled:  enabled,
	}); err != nil {
		p.logger.Error("auth policy set REFUSED: it names a relation or column the catalog "+
			"does not hold; the previous set keeps running and every query is refused until "+
			"it is corrected", "error", err)
		return
	}
	p.logger.Info("auth provider updated", "enabled", enabled)
}

// UpdateWithEvaluator atomically replaces all auth components including the
// ABAC evaluator, binding them to the attached catalog (see Update).
func (p *Provider) UpdateWithEvaluator(authn *Authenticator, authz *Authorizer, policies *PolicySet, evaluator *PolicyEvaluator) {
	enabled := authn != nil && authn.Enabled()
	if err := p.installState(&authState{
		authn:     authn,
		authz:     authz,
		policies:  policies,
		evaluator: evaluator,
		enabled:   enabled,
	}); err != nil {
		p.logger.Error("auth policy set REFUSED: it names a relation or column the catalog "+
			"does not hold; the previous set keeps running and every query is refused until "+
			"it is corrected", "error", err)
		return
	}
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
	// A configuration that cannot be BUILT refuses the reload and swaps
	// nothing. `New` used to be called here and its error did not exist, so a
	// reload naming a JWT key the process cannot read replaced a working
	// authenticator with a disabled one and every door started serving
	// unauthenticated requests (#931).
	authn, authz, err := Build(cfg)
	if err != nil {
		return fmt.Errorf("authentication configuration: %w", err)
	}
	var legacyPolicies *PolicySet
	if len(policyCfgs) > 0 {
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

	// The names a policy uses bind ONCE, in installState, against the attached
	// catalog: every relation and every policed column is rewritten to the
	// catalog's own spelling, and one that does not resolve REFUSES the load.
	// Refusing returns before the swap, so a hot reload keeps the set already
	// running rather than installing one whose scoped rules would silently
	// never match (#882, #802's contract applied to names).
	enabled := authn != nil && authn.Enabled()
	if err := p.installState(&authState{
		authn:     authn,
		authz:     authz,
		policies:  legacyPolicies,
		evaluator: evaluator,
		enabled:   enabled,
	}); err != nil {
		return err
	}
	p.logger.Info("auth provider updated", "enabled", enabled, "abac", evaluator != nil)
	return nil
}
