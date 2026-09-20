// SPDX-License-Identifier: MIT

package physical

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// A QUALIFIED REFERENCE NAMES ONE RELATION IN SCOPE, OR IT IS REFUSED AS
// PostgreSQL REFUSES IT — arc RS's plan-time table (#1220, #1161, #1162).
//
// Every `want` below is live PostgreSQL 17.11 (postgres:17-alpine, --locale=C)
// over a fixture holding the same two relations as this repo's `lat_ord` /
// `lat_item`, measured before any code changed and re-measured at the tip.
// The columns asserted are the SQLSTATE and PostgreSQL's PRIMARY SENTENCE;
// PostgreSQL's DETAIL/HINT is carried in this engine's message as explanatory
// text after a colon and is deliberately not asserted byte for byte.
//
// The three cases the table separates, and why the split is not cosmetic:
//
//	missing FROM-clause entry   nothing at this level declares that name, and
//	                            a relation a LATER join introduces has not
//	                            been read yet — so a forward ON reference is
//	                            MISSING, which is the opposite of what #1220
//	                            expected and what PostgreSQL actually says;
//	invalid reference …         the statement HAS that entry, written earlier,
//	                            and this position cannot reach it;
//	table name … more than once one name would answer to two relations, which
//	                            PostgreSQL refuses at the FROM clause (42712).
//
// This gate is plan-time only and needs no storage, which is the point: the
// refusal it asserts is a property of the STATEMENT, so it must hold before a
// row is read and identically on every execution arm. The five-arm table is
// `coordinator.TestArcRSAQualifiedReferenceNamesOneRelationOnEveryArm`.
type rsCell struct {
	name, sql string
	// state is the SQLSTATE PostgreSQL 17.11 returns, "" when it ANSWERS.
	state string
	// sentence is PostgreSQL's primary message, which this engine's refusal
	// must contain. Empty where the statement is answered.
	sentence string
}

func rsCells() []rsCell {
	return []rsCell{
		// --- #1220: an ON clause is scoped to its own join ---------------
		{"on/namesNothing", "SELECT a.id FROM lat_ord a JOIN lat_item b ON c.id = a.id",
			"42P01", `missing FROM-clause entry for table "c"`},
		{"on/namesLaterJoin", "SELECT a.id FROM lat_ord a JOIN lat_item b ON j.id = a.id JOIN lat_item j ON j.id = b.id",
			"42P01", `missing FROM-clause entry for table "j"`},
		{"on/namesLaterJoinLeft", "SELECT a.id FROM lat_ord a LEFT JOIN lat_item b ON j.amount = b.amount JOIN lat_item j ON j.id = a.id",
			"42P01", `missing FROM-clause entry for table "j"`},
		{"on/namesLaterCommaItem", "SELECT a.id FROM lat_ord a JOIN lat_item b ON c.id = a.id, lat_item c",
			"42P01", `missing FROM-clause entry for table "c"`},
		{"on/namesOwnRightSideLater", "SELECT a.id FROM lat_ord a JOIN lat_item b ON b.id = c.id JOIN lat_item c ON c.id = a.id",
			"42P01", `missing FROM-clause entry for table "c"`},
		{"on/namesEarlierCommaItem", "SELECT a.id FROM lat_ord a, lat_item b JOIN lat_item c ON a.id = c.order_id",
			"42P01", `invalid reference to FROM-clause entry for table "a"`},
		{"on/namesEarlierCommaItemOuter", "SELECT a.id FROM lat_ord a, lat_item b LEFT JOIN lat_item c ON a.total > c.amount",
			"42P01", `invalid reference to FROM-clause entry for table "a"`},
		{"on/tableNameBehindAlias", "SELECT a.id FROM lat_ord a JOIN lat_item b ON lat_ord.id = b.order_id",
			"42P01", `invalid reference to FROM-clause entry for table "lat_ord"`},
		{"on/sqlancerThreeWay", "SELECT COUNT(*) AS n FROM lat_ord t0 JOIN lat_item t1 ON t2.amount = t1.amount JOIN lat_item t2 ON t2.id = t0.id",
			"42P01", `missing FROM-clause entry for table "t2"`},
		// The CONTROLS, which decide whether the rule is a rule or a ban.
		// Every one of these is a statement PostgreSQL ANSWERS, and each is
		// one edit away from a cell above.
		{"onOk/secondSeesFirst", "SELECT a.id FROM lat_ord a JOIN lat_item b ON a.id = b.order_id JOIN lat_item c ON a.id = c.order_id", "", ""},
		{"onOk/leftDeep", "SELECT a.id FROM lat_ord a JOIN lat_item b ON a.id = b.order_id JOIN lat_item c ON b.id = c.id", "", ""},
		{"onOk/thenComma", "SELECT a.id FROM lat_ord a JOIN lat_item b ON a.id = b.order_id, lat_item c", "", ""},
		{"onOk/commaBefore", "SELECT a.id FROM lat_item b JOIN lat_item c ON b.id = c.id, lat_ord a", "", ""},
		{"onOk/crossThenJoin", "SELECT a.id FROM lat_ord a CROSS JOIN lat_item b JOIN lat_item c ON a.id = c.order_id", "", ""},
		{"onOk/rightJoin", "SELECT a.id FROM lat_ord a JOIN lat_item b ON b.order_id = a.id RIGHT JOIN lat_item c ON a.id = c.order_id", "", ""},
		{"onOk/unaliased", "SELECT lat_ord.id FROM lat_ord JOIN lat_item ON lat_ord.id = lat_item.order_id", "", ""},
		{"onOk/correlatedOuterLevel", "SELECT o.id FROM lat_ord o WHERE EXISTS (SELECT 1 FROM lat_item b JOIN lat_item c ON b.id = c.id AND o.id = b.order_id)", "", ""},

		// --- #1161 / #1162: a window key is a reference too ---------------
		{"win/partitionNamesNothing", "SELECT o.id, COUNT(*) OVER (PARTITION BY zz.id) AS n FROM lat_ord o",
			"42P01", `missing FROM-clause entry for table "zz"`},
		{"win/orderNamesNothing", "SELECT o.id, COUNT(*) OVER (ORDER BY zz.id) AS n FROM lat_ord o",
			"42P01", `missing FROM-clause entry for table "zz"`},
		{"win/argNamesNothing", "SELECT o.id, SUM(zz.total) OVER () AS n FROM lat_ord o",
			"42P01", `missing FROM-clause entry for table "zz"`},
		{"win/orderExprNamesNothing", "SELECT o.id, SUM(o.total) OVER (ORDER BY zz.total + 1) AS n FROM lat_ord o",
			"42P01", `missing FROM-clause entry for table "zz"`},
		{"win/partitionOverJoin", "SELECT COUNT(*) AS n FROM lat_ord a JOIN lat_item b ON a.id = b.order_id",
			"", ""},
		// #1162's two shapes. A qualifier BOTH arms answer to names no one
		// relation, and the two spellings earn PostgreSQL's two sentences.
		{"win/ambiguousAcrossDerivedArms", "SELECT COUNT(*) OVER (PARTITION BY lat_item.id) AS n FROM (SELECT id FROM lat_item) x JOIN (SELECT id FROM lat_item) y ON x.id = y.id",
			"42P01", `missing FROM-clause entry for table "lat_item"`},
		{"win/ambiguousAcrossSelfJoin", "SELECT COUNT(*) OVER (PARTITION BY lat_item.order_id) AS n FROM lat_item a JOIN lat_item b ON a.id = b.id",
			"42P01", `invalid reference to FROM-clause entry for table "lat_item"`},
		{"win/partitionTableBehindAlias", "SELECT COUNT(*) OVER (PARTITION BY lat_ord.id) AS n FROM lat_ord o",
			"42P01", `invalid reference to FROM-clause entry for table "lat_ord"`},
		{"win/orderTableBehindAlias", "SELECT COUNT(*) OVER (ORDER BY lat_ord.id) AS n FROM lat_ord o",
			"42P01", `invalid reference to FROM-clause entry for table "lat_ord"`},
		{"win/argTableBehindAlias", "SELECT SUM(lat_ord.total) OVER () AS n FROM lat_ord o",
			"42P01", `invalid reference to FROM-clause entry for table "lat_ord"`},
		{"win/partitionDerivedInner", "SELECT x.id, SUM(x.total) OVER (PARTITION BY lat_ord.id) AS n FROM (SELECT id, total FROM lat_ord) x",
			"42P01", `missing FROM-clause entry for table "lat_ord"`},
		{"win/partitionOutputAliasQualified", "SELECT o.id AS g, COUNT(*) OVER (PARTITION BY g.id) AS n FROM lat_ord o",
			"42P01", `missing FROM-clause entry for table "g"`},
		// A window key sees the INPUT relation, never the SELECT list: PG
		// 42703 for the bare alias, which is the discriminator that says the
		// fix resolves against `resolve` and not against the output scope.
		{"win/partitionOutputAliasBare", "SELECT o.id AS g, COUNT(*) OVER (PARTITION BY g) AS n FROM lat_ord o",
			"42703", ""},
		{"win/partitionUnknownColumn", "SELECT o.id, COUNT(*) OVER (PARTITION BY o.nosuch) AS n FROM lat_ord o",
			"42703", ""},
		{"winOk/partition", "SELECT o.id, COUNT(*) OVER (PARTITION BY o.id) AS n FROM lat_ord o", "", ""},
		{"winOk/argOverJoin", "SELECT SUM(b.amount) OVER (PARTITION BY a.id) AS n FROM lat_ord a JOIN lat_item b ON a.id = b.order_id", "", ""},
		{"winOk/overDerived", "SELECT x.id, SUM(x.total) OVER (PARTITION BY x.id) AS n FROM (SELECT id, total FROM lat_ord) x", "", ""},
		{"winOk/grouped", "SELECT o.id AS g, SUM(o.total) AS s, ROW_NUMBER() OVER (PARTITION BY o.id ORDER BY SUM(o.total)) AS rn FROM lat_ord o GROUP BY o.id", "", ""},
		{"winOk/frameOffset", "SELECT o.id, SUM(o.total) OVER (ORDER BY o.id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS n FROM lat_ord o", "", ""},

		// --- the other positions, one cell per clause ---------------------
		{"pos/select", "SELECT zz.id FROM lat_ord o", "42P01", `missing FROM-clause entry for table "zz"`},
		{"pos/where", "SELECT o.id FROM lat_ord o WHERE zz.id = 1", "42P01", `missing FROM-clause entry for table "zz"`},
		{"pos/groupBy", "SELECT COUNT(*) AS n FROM lat_ord o GROUP BY zz.id", "42P01", `missing FROM-clause entry for table "zz"`},
		{"pos/having", "SELECT COUNT(*) AS n FROM lat_ord o GROUP BY o.id HAVING SUM(zz.total) > 0", "42P01", `missing FROM-clause entry for table "zz"`},
		{"pos/orderBy", "SELECT o.id FROM lat_ord o ORDER BY zz.id", "42P01", `missing FROM-clause entry for table "zz"`},
		{"pos/subqueryBody", "SELECT o.id, (SELECT MAX(zz.amount) FROM lat_item z WHERE z.order_id = o.id) AS v FROM lat_ord o",
			"42P01", `missing FROM-clause entry for table "zz"`},
		{"pos/usingJoin", "SELECT zz.id FROM lat_item a JOIN lat_item b USING (id)", "42P01", `missing FROM-clause entry for table "zz"`},
		{"pos/setOpArm", "SELECT o.id FROM lat_ord o UNION ALL SELECT i.id FROM lat_item i WHERE o.id = 1",
			"42P01", `missing FROM-clause entry for table "o"`},

		// --- a sibling FROM item is out of scope without LATERAL ----------
		{"sibling/withoutLateral", "SELECT s.m FROM lat_ord o, (SELECT o.id AS m) s", "42P01",
			`invalid reference to FROM-clause entry for table "o"`},
		{"sibling/writtenBefore", "SELECT s.m FROM (SELECT o.id AS m) s, lat_ord o", "42P01",
			`missing FROM-clause entry for table "o"`},
	}
}

func TestArcRSAQualifiedReferenceNamesOneRelationInScope(t *testing.T) {
	cat := &fakeCatalog{tables: map[string][]string{
		"lat_ord":  {"id", "customer", "total"},
		"lat_item": {"id", "order_id", "product", "amount"},
	}}
	answered := 0
	for _, tc := range rsCells() {
		t.Run(tc.name, func(t *testing.T) {
			info := mustExtract(t, tc.sql)
			err := validateColumns(context.Background(), cat, info)
			if tc.state == "" {
				if err != nil {
					t.Fatalf("PostgreSQL 17.11 ANSWERS this statement; the binder refused it\n  %s\n  %v",
						tc.sql, err)
				}
				answered++
				return
			}
			if err == nil {
				t.Fatalf("PostgreSQL 17.11 raises %s %s; the binder accepted the statement\n  %s",
					tc.state, tc.sentence, tc.sql)
			}
			if got := sqlerr.StateOf(err); got != tc.state {
				t.Errorf("SQLSTATE %s, want %s (PostgreSQL 17.11)\n  %s\n  %v",
					got, tc.state, tc.sql, err)
			}
			if tc.sentence != "" && !strings.Contains(err.Error(), tc.sentence) {
				t.Errorf("the refusal does not carry PostgreSQL's sentence\n  got  %v\n  want a message containing %q\n  %s",
					err, tc.sentence, tc.sql)
			}
		})
	}
	// A TABLE WHOSE EVERY CELL REFUSES PROVES ONLY THAT THE BINDER IS LOUD.
	// The controls are what say the rule is a rule: each is a statement
	// PostgreSQL answers and one edit from a refusing cell above.
	if answered < 13 {
		t.Fatalf("only %d control cells were answered — the table has stopped "+
			"discriminating between a reference in scope and one out of it", answered)
	}
}
