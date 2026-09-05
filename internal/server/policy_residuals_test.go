package server

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/auth"
)

// The two residuals arc E7 left behind, gated: a row filter that names a
// column the SAME policy denies (E7 deferral 1 / E7-D2), and MERGE under a
// column policy (E7 deferral / E7-D3, still a refusal).

// pmFilterProvider denies `salary` and filters rows BY it — the shape E7's
// deferral 1 describes. A policy predicate is allowed to read what the policy
// takes away; that is what a policy predicate is for.
func pmFilterProvider(t *testing.T) *auth.Provider {
	t.Helper()
	evaluator := auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{
		Name: "f5-filter", Version: 1, Enabled: true,
		Rules: []auth.PolicyRule{
			{
				ID: "denies-and-filters-by-salary", EffectStr: "allow", Priority: 10,
				Subjects:  []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "clerk"}},
				Resources: []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: pmTable}},
				Actions:   []auth.Action{auth.ActionRead},
				Obligations: []auth.Obligation{
					{Type: "deny_column", Target: "salary"},
					{Type: "row_filter", Value: "salary > 700009"},
				},
			},
			{
				ID: "clerk-other", EffectStr: "allow", Priority: 10,
				Subjects:  []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "clerk"}},
				Resources: []auth.Condition{{Attribute: "resource.name", Op: "neq", Value: pmTable}},
				Actions:   []auth.Action{auth.ActionRead},
			},
			{
				ID: "admin-raw", EffectStr: "allow", Priority: 10,
				Subjects: []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "admin"}},
				Actions:  []auth.Action{auth.ActionRead},
			},
		},
	}})
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{
			{Key: "clerk-key", Name: "clerk", Role: "clerk"},
			{Key: "admin-key", Name: "admin", Role: "admin"},
		},
		Roles: []auth.RoleConfig{
			{Name: "clerk", Tables: []string{"*"}, Allow: []string{"read"}},
			{Name: "admin", Tables: []string{"*"}, Allow: []string{"admin"}},
		},
	})
	p := auth.NewProvider(authn, authz, nil, nil)
	p.UpdateWithEvaluator(authn, authz, nil, evaluator)
	return p
}

// TestARowFilterMayNameAColumnThePolicyDenies is E7-D2's gate.
//
// The filter sits BELOW the security projection and reads the row as stored
// (ADR-0033 decision 6), so it can compare a column the same policy removes
// from the identity's schema — and it MUST, or a policy of the form "you may
// see the rows above a threshold, but not the threshold column" cannot be
// written at all. What must not happen is the two halves disagreeing: the
// filter silently matching every row because the name did not bind, or the
// column becoming readable because the filter named it.
//
// E7 round 2's P3 gave the injected predicate its own binding pass against the
// table's UNFILTERED schema, which is the mechanism this asserts end to end.
func TestARowFilterMayNameAColumnThePolicyDenies(t *testing.T) {
	if testing.Short() {
		t.Skip("stands up embedded NATS and three workers")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUpWith(t, ctx, pmFilterProvider(t))

	// salary is 700000+id over ids 1..12, so `salary > 700009` selects 10,11,12.
	want := []string{"10", "11", "12"}

	for _, door := range rig.doors {
		t.Run(door.name, func(t *testing.T) {
			res, err := door.run(t, "clerk-key", `SELECT id FROM e7emp ORDER BY id`)
			if err != nil {
				t.Fatalf("a row filter over a DENIED column refused the query: %v", err)
			}
			got := make([]string, 0, len(res.rows))
			for _, r := range res.rows {
				got = append(got, r["id"])
			}
			sort.Strings(got)
			sort.Strings(want)
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Errorf("rows %v, want %v — the policy predicate must compare the STORED "+
					"salary; a filter that bound to nothing would return all twelve",
					got, want)
			}

			// And the column is still gone: naming it is 42703, exactly as it
			// is without the filter.
			if _, err := door.run(t, "clerk-key", `SELECT salary FROM e7emp`); err == nil {
				t.Error("SELECT salary answered; a policy that FILTERS by a denied column " +
					"must not make it readable")
			}

			// The control: an unpoliced identity sees all twelve.
			all, err := door.run(t, "admin-key", `SELECT id FROM e7emp`)
			if err != nil {
				t.Fatalf("admin: %v", err)
			}
			if len(all.rows) != pmRows {
				t.Errorf("the admin sees %d rows, want %d — the filter must bind to the "+
					"policed identity only", len(all.rows), pmRows)
			}
		})
	}
}

// TestARowFilterOverAMaskedColumnComparesTheStoredValue is the other half of
// E7-D2's boundary (round-1 P4).
//
// The filter sits BELOW the security projection, so it reads the row as
// STORED — ADR-0033 decision 6, PostgreSQL's RLS ordering. That is decidable
// only over a MASKED column, where the two readings give different row sets:
// `acct` is masked to 0 and stored as 900000+id, so `acct > 900006` selects
// ids 7..12 if the filter reads the stored value and NOTHING if it reads its
// own mask. A mask that could defeat the filter beneath it would be a policy
// that silently stops restricting.
func TestARowFilterOverAMaskedColumnComparesTheStoredValue(t *testing.T) {
	if testing.Short() {
		t.Skip("stands up embedded NATS and three workers")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	evaluator := auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{
		Name: "f5-masked-filter", Version: 1, Enabled: true,
		Rules: []auth.PolicyRule{
			{
				ID: "masks-and-filters-by-acct", EffectStr: "allow", Priority: 10,
				Subjects:  []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "teller"}},
				Resources: []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: pmTable}},
				Actions:   []auth.Action{auth.ActionRead},
				Obligations: []auth.Obligation{
					{Type: "mask_column", Target: "acct", Value: pmMaskAcct},
					{Type: "row_filter", Value: "acct > 900006"},
				},
			},
			{
				ID: "teller-other", EffectStr: "allow", Priority: 10,
				Subjects:  []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "teller"}},
				Resources: []auth.Condition{{Attribute: "resource.name", Op: "neq", Value: pmTable}},
				Actions:   []auth.Action{auth.ActionRead},
			},
		},
	}})
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{{Key: "teller-key", Name: "teller", Role: "teller"}},
		Roles:   []auth.RoleConfig{{Name: "teller", Tables: []string{"*"}, Allow: []string{"read"}}},
	})
	provider := auth.NewProvider(authn, authz, nil, nil)
	provider.UpdateWithEvaluator(authn, authz, nil, evaluator)

	rig := pmRigUpWith(t, ctx, provider)
	want := []string{"10", "11", "12", "7", "8", "9"} // sorted as text
	for _, door := range rig.doors {
		t.Run(door.name, func(t *testing.T) {
			res, err := door.run(t, "teller-key", `SELECT id, acct FROM e7emp ORDER BY id`)
			if err != nil {
				t.Fatalf("a row filter over a MASKED column refused the query: %v", err)
			}
			got := make([]string, 0, len(res.rows))
			for _, r := range res.rows {
				got = append(got, r["id"])
				if r["acct"] != pmMaskAcct {
					t.Errorf("acct = %q, want the mask %q — the filter reads the stored "+
						"value, the CLIENT reads the mask", r["acct"], pmMaskAcct)
				}
			}
			sort.Strings(got)
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Errorf("rows %v, want %v — a filter that compared the MASK would select "+
					"none of them, and one that did not bind would select all twelve",
					got, want)
			}
		})
	}
}

// TestARowFilterNamingAnotherTablesColumnRefuses is the third side of the same
// boundary: the injected predicate binds against the POLICED table's schema,
// so a column that exists elsewhere in the database is still a name this
// filter cannot resolve. `note` is e7other's column, not e7emp's.
func TestARowFilterNamingAnotherTablesColumnRefuses(t *testing.T) {
	if testing.Short() {
		t.Skip("stands up embedded NATS and three workers")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	evaluator := auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{
		Name: "f5-foreign-column", Version: 1, Enabled: true,
		Rules: []auth.PolicyRule{{
			ID: "foreign", EffectStr: "allow", Priority: 10,
			Subjects:  []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "foreign"}},
			Resources: []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: pmTable}},
			Actions:   []auth.Action{auth.ActionRead},
			// `note` is a column of e7other. It exists in the database and not
			// on the table this policy binds to.
			Obligations: []auth.Obligation{{Type: "row_filter", Value: "note = 'n1'"}},
		}},
	}})
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{{Key: "foreign-key", Name: "foreign", Role: "foreign"}},
		Roles:   []auth.RoleConfig{{Name: "foreign", Tables: []string{"*"}, Allow: []string{"read"}}},
	})
	provider := auth.NewProvider(authn, authz, nil, nil)
	provider.UpdateWithEvaluator(authn, authz, nil, evaluator)

	rig := pmRigUpWith(t, ctx, provider)
	for _, door := range rig.doors {
		t.Run(door.name, func(t *testing.T) {
			_, err := door.run(t, "foreign-key", `SELECT id FROM e7emp`)
			if err == nil {
				t.Fatal("a row_filter naming ANOTHER table's column answered; the predicate " +
					"binds against the policed table and would restrict no rows here")
			}
			if !strings.Contains(err.Error(), "note") {
				t.Errorf("refusal is %v; want one naming note", err)
			}
		})
	}
}

// TestARowFilterNamingAColumnThatDoesNotExistStillRefuses is the boundary from
// the other side: "the policy may read a denied column" must not become "the
// filter's names are never checked". A name the TABLE does not have is 42703.
func TestARowFilterNamingAColumnThatDoesNotExistStillRefuses(t *testing.T) {
	if testing.Short() {
		t.Skip("stands up embedded NATS and three workers")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	evaluator := auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{
		Name: "f5-typo", Version: 1, Enabled: true,
		Rules: []auth.PolicyRule{{
			ID: "typo", EffectStr: "allow", Priority: 10,
			Subjects: []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "typo"}},
			Actions:  []auth.Action{auth.ActionRead},
			Obligations: []auth.Obligation{
				{Type: "deny_column", Target: "salary"},
				{Type: "row_filter", Value: "salary_typo > 0"},
			},
		}},
	}})
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{{Key: "typo-key", Name: "typo", Role: "typo"}},
		Roles:   []auth.RoleConfig{{Name: "typo", Tables: []string{"*"}, Allow: []string{"read"}}},
	})
	provider := auth.NewProvider(authn, authz, nil, nil)
	provider.UpdateWithEvaluator(authn, authz, nil, evaluator)

	rig := pmRigUpWith(t, ctx, provider)
	for _, door := range rig.doors {
		t.Run(door.name, func(t *testing.T) {
			_, err := door.run(t, "typo-key", `SELECT id FROM e7emp`)
			if err == nil {
				t.Fatal("a row_filter naming a column the table does not have answered; " +
					"it would restrict no rows, which is a policy that restricts nothing")
			}
			if !strings.Contains(err.Error(), "salary_typo") {
				t.Errorf("refusal is %v; want one naming salary_typo", err)
			}
		})
	}
}

// TestMergeUnderAColumnPolicyIsRefusedOnEveryDoor is E7-D3's PIN.
//
// MERGE stays refused, and the reason is structural rather than an omission:
// `MergeWhenClause.SQL` is the raw text of a `SET a = …` or `(cols) VALUES (…)`
// clause and the only thing that decomposes it is wadjet/dml.go's
// applySetClauses / buildInsertRow, at EXECUTION time, with strings.Index and
// splitSetClauses. Rewriting the clause in internal/auth means a SECOND string
// splitter over the same text; the two would have to agree about every
// spelling forever, or the policy rewrite would cover a different set of
// expressions than the executor evaluates. That is ADR-0033 decision 5's
// second enforcement path, in the worst possible place. The fix is for the
// PARSER to decompose a WHEN clause into typed assignments the way it already
// decomposes `UPDATE ... SET`, so there is ONE decomposition — a parser change
// plus an executor rewrite, its own arc.
//
// This pin fails the day MERGE answers under a policy, which is when that arc
// lands and this test is replaced by a census.
func TestMergeUnderAColumnPolicyIsRefusedOnEveryDoor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	_, _, doors := dmlRig(t, ctx)

	const merge = `MERGE INTO e7emp t USING (SELECT 1 AS sid, 'zz' AS sdept) s ` +
		`ON t.id = s.sid WHEN MATCHED THEN UPDATE SET dept = s.sdept`

	for _, door := range doors {
		t.Run(door.name, func(t *testing.T) {
			before := door.read(t, 1, "dept")
			tag, err := door.exec(t, "writer-key", merge)
			if err == nil {
				t.Fatalf("MERGE ran under a column policy (%s). If that is now correct, "+
					"this pin is the thing to delete — and a DML census cell for MERGE is "+
					"what replaces it (ADR-0033)", tag)
			}
			if !strings.Contains(err.Error(), "MERGE is not available") {
				t.Errorf("MERGE refused with %v\n  want the policy refusal naming MERGE", err)
			}
			if got := door.read(t, 1, "dept"); got != before {
				t.Errorf("the refused MERGE still wrote: dept %q -> %q", before, got)
			}
		})
	}

	// A READ-ONLY identity's MERGE is refused by rule 1 — the write grant —
	// BEFORE the column policy's 0A000 gets a say. The order matters: 42501
	// says "you may not write this table", 0A000 says "the engine cannot run
	// this statement safely", and a client acts on them differently
	// (round-1 P5).
	for _, door := range doors {
		t.Run("read_only_identity/"+door.name, func(t *testing.T) {
			tag, err := door.exec(t, "reader-key", merge)
			if err == nil {
				t.Fatalf("a READ-only identity ran a MERGE: %s", tag)
			}
			if !strings.Contains(err.Error(), "permission denied for table") {
				t.Errorf("MERGE from a read-only identity refused with %v\n"+
					"  want 42501 permission denied, which is decided before the "+
					"column policy's refusal", err)
			}
		})
	}

	// The control: an identity with NO obligations merges normally. The
	// refusal is the policy's, not the statement's.
	for i, door := range doors {
		t.Run("unpoliced/"+door.name, func(t *testing.T) {
			row := 2 + i
			sql := fmt.Sprintf(
				`MERGE INTO e7emp t USING (SELECT %d AS sid, 'ok%d' AS sdept) s `+
					`ON t.id = s.sid WHEN MATCHED THEN UPDATE SET dept = s.sdept`, row, row)
			if _, err := door.exec(t, "admin-key", sql); err != nil {
				t.Fatalf("an unpoliced MERGE was refused: %v", err)
			}
			if got := door.read(t, row, "dept"); got != fmt.Sprintf("ok%d", row) {
				t.Errorf("row %d dept = %q after an unpoliced MERGE", row, got)
			}
		})
	}
}

// TestMergeIntoAnUnpolicedTargetMasksItsPolicedSource is the topology the pin
// does not cover: the refusal is decided by the TARGET's policy, so a MERGE
// whose target carries none RUNS — and its source read goes through the same
// db.Query the SELECT door uses, under the same identity, so a policed source
// arrives MASKED.
//
// That is a tightening the HTTP door gained with the provider it was missing
// (03cc9883): before it, this statement copied the STORED ssn into an
// unpoliced table on that door. It is also the reason the MERGE deferral is
// about the target's WHEN clauses and not about MERGE as a whole.
func TestMergeIntoAnUnpolicedTargetMasksItsPolicedSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	db, provider, doors := dmlRig(t, ctx)

	adminCtx := func() context.Context {
		id, err := provider.Authenticator().AuthenticateToken("admin-key")
		if err != nil {
			t.Fatal(err)
		}
		return auth.ContextWithIdentity(ctx, id)
	}

	for i, door := range doors {
		t.Run(door.name, func(t *testing.T) {
			row := 1 + i // e7other holds ids 1..3
			sql := fmt.Sprintf(
				`MERGE INTO e7other t USING (SELECT id AS sid, ssn AS sssn FROM e7emp WHERE id = %d) s `+
					`ON t.id = s.sid WHEN MATCHED THEN UPDATE SET note = s.sssn`, row)
			if _, err := door.exec(t, "writer-key", sql); err != nil {
				t.Fatalf("a MERGE into an UNPOLICED target was refused: %v\n"+
					"the refusal is the target's column policy, and this target has none", err)
			}
			res, err := db.Query(adminCtx(), fmt.Sprintf(
				"SELECT note AS v FROM e7other WHERE id = %d", row))
			if err != nil {
				t.Fatal(err)
			}
			if len(res.Rows) == 0 {
				t.Fatalf("e7other row %d is gone", row)
			}
			got := fmt.Sprint(res.Rows[0]["v"])
			if got == fmt.Sprintf("true-ssn-%02d", row) {
				t.Errorf("the MERGE stored the STORED ssn (%q) in an unpoliced table; the "+
					"source read must see the mask, as a SELECT does", got)
			}
			if got != pmMaskSSN {
				t.Errorf("e7other row %d note = %q, want the mask %q", row, got, pmMaskSSN)
			}
		})
	}
}
