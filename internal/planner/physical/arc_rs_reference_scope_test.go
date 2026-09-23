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
// Every `want` is live PostgreSQL 17.11 (--locale=C) over the same two
// relations as `lat_ord` / `lat_item`: the SQLSTATE and PostgreSQL's PRIMARY
// sentence (its DETAIL/HINT rides after a colon and is not asserted). The
// three cases: `missing FROM-clause entry` — nothing at this level declares
// the name, including a relation a LATER join introduces; `invalid
// reference` — the entry was written earlier and this position cannot reach
// it; `specified more than once` (42712) — one name, two relations. Plan-time
// only, needing no storage; the five-arm table is
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
		// The DISCRIMINATOR between case 2 and case 3, which every cell above
		// missed because each aliased its relations to a DIFFERENT name: an
		// UNALIASED relation that is out of scope earns the ordinary case-2
		// sentence, not the alias one. The parser records an alias equal to
		// the table's own name for several spellings that wrote none, and an
		// alias equal to the name hides nothing. SQLancer found it: 178 of
		// 200 generated databases stopped on the alias sentence naming a
		// table as its own alias, where PostgreSQL's DETAIL — and SQLancer's
		// expected-error list — carry the case-2 wording.
		{"on/unaliasedRelationOutOfScope",
			"SELECT lat_ord.id FROM lat_ord, lat_item b JOIN lat_item c ON lat_ord.id = c.order_id",
			"42P01", `there is an entry for table "lat_ord", but it cannot be referenced from this part of the query`},
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
		// A `JOIN … USING` MERGES the joined column into one, so the bare name
		// is not ambiguous — PostgreSQL answers, and a sort or window key is
		// the one place this engine already bound it rather than refusing it
		// (ADR-0012 §5 #655). Resolving a window item's names must not undo
		// that; the contested cell below is the discriminator, where the two
		// `w` are NOT a merge and 42702 is PostgreSQL's own answer.
		{"winOk/usingMergedArgument",
			"SELECT b.product AS c, SUM(id) OVER (PARTITION BY b.product) AS s FROM lat_item a LEFT JOIN lat_item b USING (id)", "", ""},
		{"winOk/usingMergedPartitionKey",
			"SELECT COUNT(*) OVER (PARTITION BY id) AS s FROM lat_item a LEFT JOIN lat_item b USING (id)", "", ""},
		{"win/bareContestedAcrossArms",
			"SELECT x.w AS xw, y.w AS yw, SUM(y.w) OVER (PARTITION BY w) AS s FROM (SELECT id, total AS w FROM lat_ord) x JOIN (SELECT id, amount AS w FROM lat_item) y ON x.id = y.id",
			"42702", `column reference "w" is ambiguous`},

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

		// --- a qualified STAR is a qualified reference --------------------
		{"star/namesNothing", "SELECT zz.* FROM lat_ord o", "42P01", `missing FROM-clause entry for table "zz"`},
		{"star/tableBehindAlias", "SELECT lat_ord.* FROM lat_ord o", "42P01", `invalid reference to FROM-clause entry for table "lat_ord"`},
		{"starOk/ownAlias", "SELECT o.* FROM lat_ord o", "", ""},
		{"starOk/laterJoinIsVisibleInTheSelectList", "SELECT j.* FROM lat_ord a JOIN lat_item b ON a.id = b.order_id JOIN lat_item j ON j.id = a.id", "", ""},

		// --- one name, one relation (42712) -------------------------------
		{"dup/commaSelfJoin", "SELECT 1 AS k FROM lat_item, lat_item", "42712", `table name "lat_item" specified more than once`},
		{"dup/joinSelfJoin", "SELECT 1 AS k FROM lat_item JOIN lat_item ON true", "42712", `table name "lat_item" specified more than once`},
		{"dup/derivedSharedAlias", "SELECT 1 AS k FROM (SELECT id FROM lat_ord) x, (SELECT id FROM lat_item) x", "42712", `table name "x" specified more than once`},
		{"dupOk/oneAliasedOneNot", "SELECT lat_item.order_id FROM lat_item JOIN lat_item b ON lat_item.id = b.id", "", ""},
		{"dupOk/derivedAliasEqualsBaseName", "SELECT lat_item.id FROM (SELECT id FROM lat_ord) lat_item JOIN lat_item b ON lat_item.id = b.id", "", ""},
		// A DELIMITED alias keeps its bytes, so `t` and `"T"` are two names:
		// PostgreSQL answers this, and a duplicate verdict taken on the
		// FOLDED key would refuse it. The discriminator for #731's rule
		// reaching this verdict too.
		{"dupOk/delimitedAliasIsADifferentName", `SELECT t.id FROM lat_ord t, lat_item "T"`, "", ""},

		// --- a STAR OUTPUT does not open the FROM clause ------------------
		//
		// `SELECT *` mints output names the binder cannot enumerate, so an
		// ORDER BY or GROUP BY naming one of them must not be refused — but a
		// star never mints a QUALIFIER. One out-of-scope reference had two
		// dispositions decided by the enclosing SELECT list
		// (docs/design/window-key-ownership.md §(e) item 8).
		{"starScope/orderByUnderStar", "SELECT * FROM lat_ord o ORDER BY zz.id",
			"42P01", `missing FROM-clause entry for table "zz"`},
		{"starScope/groupByUnderStar", "SELECT * FROM lat_ord o GROUP BY zz.id",
			"42P01", `missing FROM-clause entry for table "zz"`},
		{"starScope/whereUnderStar", "SELECT * FROM lat_ord o WHERE zz.id = 1",
			"42P01", `missing FROM-clause entry for table "zz"`},
		{"starScope/windowKeyUnderStar", "SELECT *, COUNT(*) OVER (PARTITION BY zz.id) AS n FROM lat_ord o",
			"42P01", `missing FROM-clause entry for table "zz"`},
		// The control the openness exists for: a BARE name under a star is
		// still not refusable, because the star may have minted it.
		{"starScopeOk/bareOutputNameUnderStar", "SELECT * FROM lat_ord o ORDER BY id", "", ""},
		{"starScopeOk/qualifiedInScopeUnderStar", "SELECT * FROM lat_ord o ORDER BY o.id", "", ""},

		// --- a TABLE FUNCTION in FROM is a relation, and this rule reaches it
		//
		// Since arc TF (ADR-0039) a table function with a declared schema
		// leaves the scope CLOSED, so a qualifier naming no relation is
		// provably absent there too. These four are the interaction, measured
		// on PostgreSQL 17.11 over `generate_series`.
		{"tf/joinedLater",
			"SELECT o.id FROM lat_ord o JOIN generate_series(1, 3) g ON h.generate_series = o.id JOIN generate_series(1, 2) h ON true",
			"42P01", `missing FROM-clause entry for table "h"`},
		{"tf/windowKeyOutOfScope",
			"SELECT COUNT(*) OVER (PARTITION BY zz.v) AS n FROM generate_series(1, 3) AS g(v)",
			"42P01", `missing FROM-clause entry for table "zz"`},
		{"tf/starOutOfScope", "SELECT zz.* FROM generate_series(1, 3) g",
			"42P01", `missing FROM-clause entry for table "zz"`},
		{"tf/duplicateName", "SELECT 1 AS k FROM generate_series(1, 3) g, generate_series(1, 2) g",
			"42712", `table name "g" specified more than once`},
		{"tfOk/columnAliasList", "SELECT g.v FROM generate_series(1, 3) AS g(v)", "", ""},

		// --- a sibling FROM item is out of scope without LATERAL ----------
		{"sibling/withoutLateral", "SELECT s.m FROM lat_ord o, (SELECT o.id AS m) s", "42P01",
			`invalid reference to FROM-clause entry for table "o"`},
		{"sibling/writtenBefore", "SELECT s.m FROM (SELECT o.id AS m) s, lat_ord o", "42P01",
			`missing FROM-clause entry for table "o"`},
		// THE ORDER of the three cases is PostgreSQL's, and these two say so.
		// A derived table whose OWN FROM reads the named table under an alias
		// earns the ALIAS hint even when an outer FROM item of that same name
		// sits beside it; the sibling is consulted only where this block's own
		// FROM says nothing. Asked the other way round, one reference had two
		// hints decided by whether the ENCLOSING block aliased its own copy
		// (measured by the round-1 review, P1).
		{"sibling/bodyAliasWinsOverSibling",
			`SELECT s.m FROM lat_ord, (SELECT lat_ord.id AS m FROM lat_ord q) s`,
			"42P01", `perhaps you meant to reference the table alias "q"`},
		{"sibling/bodyWithoutThatRelationKeepsLateral",
			`SELECT s.m FROM lat_ord, (SELECT lat_ord.id AS m FROM lat_item q) s`,
			"42P01", `you must mark this subquery with LATERAL`},

		// --- a DELIMITED qualifier is byte-exact under a star too ---------
		//
		// resolveRef's CLOSED path has refused `"O".id` over `FROM lat_ord o`
		// since #731. The STAR-opened scope reaches the qualifier rule through
		// refuseUnknownRelationQualifier, which folded the name back — so the
		// delimited spelling kept exactly the two-dispositions split §(e) item
		// 8 records as closed (round-1 review, P3).
		{"delimStar/foldMatchesARelation", `SELECT * FROM lat_ord o ORDER BY "O".id`,
			"42P01", `missing FROM-clause entry for table "O"`},
		{"delimStar/groupBy", `SELECT * FROM lat_ord o GROUP BY "O".id`,
			"42P01", `missing FROM-clause entry for table "O"`},
		{"delimStar/where", `SELECT * FROM lat_ord o WHERE "O".id = 1`,
			"42P01", `missing FROM-clause entry for table "O"`},
		{"delimStar/windowKey", `SELECT *, COUNT(*) OVER (PARTITION BY "O".id) AS n FROM lat_ord o`,
			"42P01", `missing FROM-clause entry for table "O"`},
		{"delimStar/namedListMirror", `SELECT o.id AS v FROM lat_ord o ORDER BY "O".id`,
			"42P01", `missing FROM-clause entry for table "O"`},
		{"delimStarOk/declaredDelimited", `SELECT * FROM lat_ord "O" ORDER BY "O".id`, "", ""},
		{"delimStarOk/unquotedUnderStar", `SELECT * FROM lat_ord o ORDER BY o.id`, "", ""},
		{"delimStarOk/bareUnderStar", `SELECT * FROM lat_ord o ORDER BY id`, "", ""},
	}
}

func TestArcRSAQualifiedReferenceNamesOneRelationInScope(t *testing.T) {
	// TYPED like the coordinator's fixture. The name-only fakeCatalog types
	// every column it does not special-case as the zero TypeID — BOOLEAN —
	// and `SUM(o.total)` over a boolean is a 42883 refusal since arc BR.
	cat := brCatalog()
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
	if answered < 24 {
		t.Fatalf("only %d control cells were answered — the table has stopped "+
			"discriminating between a reference in scope and one out of it", answered)
	}
}
