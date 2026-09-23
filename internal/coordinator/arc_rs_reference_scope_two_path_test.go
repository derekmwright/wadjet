// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"
)

// A QUALIFIED REFERENCE NAMES ONE RELATION IN SCOPE ON EVERY ARM — arc RS's
// five-arm table (#1220, #1161, #1162).
//
//	{JOIN ON, window PARTITION BY / ORDER BY / argument, SELECT list, WHERE,
//	 GROUP BY, HAVING, ORDER BY, a qualified star, a subquery body, a USING
//	 join, a set-operation arm, a LATERAL body, a sibling FROM item}
//	x {names nothing, names a relation joined LATER, names another FROM item,
//	   names a base table an alias hides, names a relation BOTH arms scan,
//	   names a column the relation has not, in scope}
//	x {single, spilled512k, dag, dag-shuffled, dag-morsel4}
//
// Every `want` is live PostgreSQL 17.11 (--locale=C) over rows identical to
// `lat_ord` / `lat_item`. The refusal is at PLAN time, a property of the
// STATEMENT, so an arm that answers it never asked — as every arm did at the
// base for the `on/*` and `win*/*` cells. The `*Ok/*` controls are each one
// edit away from a refusing cell, so a rule that refuses everything fails.
type rsArmCell struct {
	name, sql string
	// refuse, when set, is PostgreSQL's primary sentence: every arm must
	// refuse and every refusal must carry it.
	refuse string
	// want is PostgreSQL's row set, for the cells it answers.
	want string
}

func rsArmCells() []rsArmCell {
	return []rsArmCell{
		// --- #1220 -------------------------------------------------------
		{name: "on/namesNothing",
			sql:    "SELECT a.id AS v FROM lat_ord a JOIN lat_item b ON c.id = a.id",
			refuse: `missing FROM-clause entry for table "c"`},
		{name: "on/namesLaterJoin",
			sql:    "SELECT a.id AS v FROM lat_ord a JOIN lat_item b ON j.id = a.id JOIN lat_item j ON j.id = b.id",
			refuse: `missing FROM-clause entry for table "j"`},
		{name: "on/namesLaterJoinOuter",
			sql:    "SELECT a.id AS v FROM lat_ord a LEFT JOIN lat_item b ON j.amount = b.amount JOIN lat_item j ON j.id = a.id",
			refuse: `missing FROM-clause entry for table "j"`},
		{name: "on/namesLaterCommaItem",
			sql:    "SELECT a.id AS v FROM lat_ord a JOIN lat_item b ON c.id = a.id, lat_item c",
			refuse: `missing FROM-clause entry for table "c"`},
		{name: "on/namesOwnRightSideLater",
			sql:    "SELECT a.id AS v FROM lat_ord a JOIN lat_item b ON b.id = c.id JOIN lat_item c ON c.id = a.id",
			refuse: `missing FROM-clause entry for table "c"`},
		{name: "on/sqlancerThreeWay",
			sql:    "SELECT COUNT(*) AS n FROM lat_ord t0 JOIN lat_item t1 ON t2.amount = t1.amount JOIN lat_item t2 ON t2.id = t0.id",
			refuse: `missing FROM-clause entry for table "t2"`},
		{name: "on/namesEarlierCommaItem",
			sql:    "SELECT a.id AS v FROM lat_ord a, lat_item b JOIN lat_item c ON a.id = c.order_id",
			refuse: `invalid reference to FROM-clause entry for table "a"`},
		{name: "on/namesEarlierCommaItemOuter",
			sql:    "SELECT a.id AS v FROM lat_ord a, lat_item b LEFT JOIN lat_item c ON a.total > c.amount",
			refuse: `invalid reference to FROM-clause entry for table "a"`},
		{name: "on/tableNameBehindAlias",
			sql:    "SELECT a.id AS v FROM lat_ord a JOIN lat_item b ON lat_ord.id = b.order_id",
			refuse: `invalid reference to FROM-clause entry for table "lat_ord"`},
		// The discriminator between PostgreSQL's two "invalid reference"
		// second lines: an UNALIASED relation out of scope earns the ordinary
		// one, and an alias equal to the table's own name hides nothing.
		// SQLancer found it (178 of 200 databases).
		{name: "on/unaliasedRelationOutOfScope",
			sql:    "SELECT lat_ord.id AS v FROM lat_ord, lat_item b JOIN lat_item c ON lat_ord.id = c.order_id",
			refuse: `there is an entry for table "lat_ord", but it cannot be referenced from this part of the query`},

		// --- #1161 / #1162 -----------------------------------------------
		{name: "win/partitionNamesNothing",
			sql:    "SELECT o.id AS v, COUNT(*) OVER (PARTITION BY zz.id) AS n FROM lat_ord o",
			refuse: `missing FROM-clause entry for table "zz"`},
		{name: "win/orderNamesNothing",
			sql:    "SELECT o.id AS v, COUNT(*) OVER (ORDER BY zz.id) AS n FROM lat_ord o",
			refuse: `missing FROM-clause entry for table "zz"`},
		{name: "win/argNamesNothing",
			sql:    "SELECT o.id AS v, SUM(zz.total) OVER () AS n FROM lat_ord o",
			refuse: `missing FROM-clause entry for table "zz"`},
		{name: "win/orderExprNamesNothing",
			sql:    "SELECT o.id AS v, SUM(o.total) OVER (ORDER BY zz.total + 1) AS n FROM lat_ord o",
			refuse: `missing FROM-clause entry for table "zz"`},
		{name: "win/ambiguousAcrossDerivedArms",
			sql: "SELECT COUNT(*) OVER (PARTITION BY lat_item.id) AS n FROM (SELECT id FROM lat_item) x " +
				"JOIN (SELECT id FROM lat_item) y ON x.id = y.id",
			refuse: `missing FROM-clause entry for table "lat_item"`},
		{name: "win/ambiguousAcrossSelfJoin",
			sql:    "SELECT COUNT(*) OVER (PARTITION BY lat_item.order_id) AS n FROM lat_item a JOIN lat_item b ON a.id = b.id",
			refuse: `invalid reference to FROM-clause entry for table "lat_item"`},
		{name: "win/partitionTableBehindAlias",
			sql:    "SELECT COUNT(*) OVER (PARTITION BY lat_ord.id) AS n FROM lat_ord o",
			refuse: `invalid reference to FROM-clause entry for table "lat_ord"`},
		{name: "win/orderTableBehindAlias",
			sql:    "SELECT COUNT(*) OVER (ORDER BY lat_ord.id) AS n FROM lat_ord o",
			refuse: `invalid reference to FROM-clause entry for table "lat_ord"`},
		{name: "win/argTableBehindAlias",
			sql:    "SELECT SUM(lat_ord.total) OVER () AS n FROM lat_ord o",
			refuse: `invalid reference to FROM-clause entry for table "lat_ord"`},
		{name: "win/partitionDerivedInner",
			sql:    "SELECT x.id AS v, SUM(x.total) OVER (PARTITION BY lat_ord.id) AS n FROM (SELECT id, total FROM lat_ord) x",
			refuse: `missing FROM-clause entry for table "lat_ord"`},
		{name: "win/partitionOutputAliasQualified",
			sql:    "SELECT o.id AS g, COUNT(*) OVER (PARTITION BY g.id) AS n FROM lat_ord o",
			refuse: `missing FROM-clause entry for table "g"`},
		// A window key sees the INPUT relation, never this block's SELECT
		// list: PostgreSQL's own 42703 for the bare alias, and it is the
		// discriminator that says the resolution is against the input scope.
		{name: "win/partitionOutputAliasBare",
			sql:    "SELECT o.id AS g, COUNT(*) OVER (PARTITION BY g) AS n FROM lat_ord o",
			refuse: `"g"`},
		{name: "win/partitionUnknownColumn",
			sql:    "SELECT o.id AS v, COUNT(*) OVER (PARTITION BY o.nosuch) AS n FROM lat_ord o",
			refuse: `"o.nosuch"`},

		// --- the other positions -----------------------------------------
		{name: "pos/select", sql: "SELECT zz.id AS v FROM lat_ord o",
			refuse: `missing FROM-clause entry for table "zz"`},
		{name: "pos/where", sql: "SELECT o.id AS v FROM lat_ord o WHERE zz.id = 1",
			refuse: `missing FROM-clause entry for table "zz"`},
		{name: "pos/groupBy", sql: "SELECT COUNT(*) AS n FROM lat_ord o GROUP BY zz.id",
			refuse: `missing FROM-clause entry for table "zz"`},
		{name: "pos/having", sql: "SELECT COUNT(*) AS n FROM lat_ord o GROUP BY o.id HAVING SUM(zz.total) > 0",
			refuse: `missing FROM-clause entry for table "zz"`},
		{name: "pos/orderBy", sql: "SELECT o.id AS v FROM lat_ord o ORDER BY zz.id",
			refuse: `missing FROM-clause entry for table "zz"`},
		{name: "pos/subqueryBody",
			sql:    "SELECT o.id AS v, (SELECT MAX(zz.amount) FROM lat_item z WHERE z.order_id = o.id) AS m FROM lat_ord o",
			refuse: `missing FROM-clause entry for table "zz"`},
		{name: "pos/usingJoin", sql: "SELECT zz.id AS v FROM lat_item a JOIN lat_item b USING (id)",
			refuse: `missing FROM-clause entry for table "zz"`},
		{name: "pos/setOpArm",
			sql:    "SELECT o.id AS v FROM lat_ord o UNION ALL SELECT i.id FROM lat_item i WHERE o.id = 1",
			refuse: `missing FROM-clause entry for table "o"`},
		{name: "pos/selectTableBehindAlias", sql: "SELECT lat_ord.id AS v FROM lat_ord o",
			refuse: `invalid reference to FROM-clause entry for table "lat_ord"`},
		{name: "pos/whereTableBehindAlias", sql: "SELECT o.id AS v FROM lat_ord o WHERE lat_ord.id = 1",
			refuse: `invalid reference to FROM-clause entry for table "lat_ord"`},
		{name: "pos/orderByTableBehindAlias", sql: "SELECT o.id AS v FROM lat_ord o ORDER BY lat_ord.id",
			refuse: `invalid reference to FROM-clause entry for table "lat_ord"`},
		{name: "sibling/withoutLateral", sql: "SELECT s.m AS v FROM lat_ord o, (SELECT o.id AS m) s",
			refuse: `invalid reference to FROM-clause entry for table "o"`},
		{name: "sibling/writtenBefore", sql: "SELECT s.m AS v FROM (SELECT o.id AS m) s, lat_ord o",
			refuse: `missing FROM-clause entry for table "o"`},
		// The ORDER of the three cases is PostgreSQL's: this block's own FROM
		// first, the sibling only where it says nothing (round-1 review, P1).
		{name: "sibling/bodyAliasWinsOverSibling",
			sql:    `SELECT s.m AS v FROM lat_ord, (SELECT lat_ord.id AS m FROM lat_ord q) s`,
			refuse: `perhaps you meant to reference the table alias "q"`},
		{name: "sibling/bodyWithoutThatRelationKeepsLateral",
			sql:    `SELECT s.m AS v FROM lat_ord, (SELECT lat_ord.id AS m FROM lat_item q) s`,
			refuse: `you must mark this subquery with LATERAL`},
		// A DELIMITED qualifier is byte-exact under a STAR too — the spelling
		// whose FOLD matches a relation in scope kept two dispositions decided
		// by the enclosing SELECT list until the round-1 review's P3.
		{name: "delimStar/foldMatchesARelation", sql: `SELECT * FROM lat_ord o ORDER BY "O".id`,
			refuse: `missing FROM-clause entry for table "O"`},
		{name: "delimStar/windowKey",
			sql:    `SELECT *, COUNT(*) OVER (PARTITION BY "O".id) AS n FROM lat_ord o`,
			refuse: `missing FROM-clause entry for table "O"`},
		{name: "delimStar/namedListMirror", sql: `SELECT o.id AS v FROM lat_ord o ORDER BY "O".id`,
			refuse: `missing FROM-clause entry for table "O"`},
		{name: "delimStarOk/declaredDelimited", sql: `SELECT * FROM lat_ord "O" ORDER BY "O".id`,
			want: "rows=3 1,Alice,150 | 2,Bob,200 | 3,Carol,0"},
		{name: "delimStarOk/unquotedUnderStar", sql: `SELECT * FROM lat_ord o ORDER BY o.id`,
			want: "rows=3 1,Alice,150 | 2,Bob,200 | 3,Carol,0"},

		// A STAR OUTPUT opens the scope for BARE names only: it mints output
		// names the binder cannot enumerate, and never a QUALIFIER. Before
		// this, one out-of-scope reference had two dispositions decided by the
		// enclosing SELECT list (window-key-ownership §(e) item 8).
		{name: "starScope/orderByUnderStar", sql: "SELECT * FROM lat_ord o ORDER BY zz.id",
			refuse: `missing FROM-clause entry for table "zz"`},
		{name: "starScope/groupByUnderStar", sql: "SELECT * FROM lat_ord o GROUP BY zz.id",
			refuse: `missing FROM-clause entry for table "zz"`},
		{name: "starScope/windowKeyUnderStar",
			sql:    "SELECT *, COUNT(*) OVER (PARTITION BY zz.id) AS n FROM lat_ord o",
			refuse: `missing FROM-clause entry for table "zz"`},
		{name: "starScopeOk/bareOutputNameUnderStar", sql: "SELECT * FROM lat_ord o ORDER BY id",
			want: "rows=3 1,Alice,150 | 2,Bob,200 | 3,Carol,0"},
		{name: "starScopeOk/qualifiedInScopeUnderStar", sql: "SELECT * FROM lat_ord o ORDER BY o.id",
			want: "rows=3 1,Alice,150 | 2,Bob,200 | 3,Carol,0"},

		// --- a qualified star, and one name per relation -------------------
		{name: "star/namesNothing", sql: "SELECT zz.* FROM lat_ord o",
			refuse: `missing FROM-clause entry for table "zz"`},
		{name: "star/tableBehindAlias", sql: "SELECT lat_ord.* FROM lat_ord o",
			refuse: `invalid reference to FROM-clause entry for table "lat_ord"`},
		{name: "dup/commaSelfJoin", sql: "SELECT 1 AS k FROM lat_item, lat_item",
			refuse: `table name "lat_item" specified more than once`},
		{name: "dup/joinSelfJoin", sql: "SELECT 1 AS k FROM lat_item JOIN lat_item ON true",
			refuse: `table name "lat_item" specified more than once`},
		{name: "dup/derivedSharedAlias",
			sql:    "SELECT 1 AS k FROM (SELECT id FROM lat_ord) x, (SELECT id FROM lat_item) x",
			refuse: `table name "x" specified more than once`},

		// --- the controls: PostgreSQL's rows, on all five arms -------------
		{name: "onOk/secondSeesFirst",
			sql:  "SELECT a.id AS v FROM lat_ord a JOIN lat_item b ON a.id = b.order_id JOIN lat_item c ON a.id = c.order_id",
			want: "rows=8 1 | 1 | 1 | 1 | 2 | 2 | 2 | 2"},
		{name: "onOk/leftDeep",
			sql:  "SELECT a.id AS v FROM lat_ord a JOIN lat_item b ON a.id = b.order_id JOIN lat_item c ON b.id = c.id",
			want: "rows=4 1 | 1 | 2 | 2"},
		{name: "onOk/thenComma",
			sql:  "SELECT a.id AS v FROM lat_ord a JOIN lat_item b ON a.id = b.order_id, lat_item c",
			want: "rows=16 1 | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2"},
		{name: "onOk/commaBefore",
			sql:  "SELECT a.id AS v FROM lat_item b JOIN lat_item c ON b.id = c.id, lat_ord a",
			want: "rows=12 1 | 1 | 1 | 1 | 2 | 2 | 2 | 2 | 3 | 3 | 3 | 3"},
		{name: "onOk/crossThenJoin",
			sql:  "SELECT a.id AS v FROM lat_ord a CROSS JOIN lat_item b JOIN lat_item c ON a.id = c.order_id",
			want: "rows=16 1 | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2"},
		{name: "onOk/rightJoin",
			sql:  "SELECT a.id AS v FROM lat_ord a JOIN lat_item b ON b.order_id = a.id RIGHT JOIN lat_item c ON a.id = c.order_id",
			want: "rows=8 1 | 1 | 1 | 1 | 2 | 2 | 2 | 2"},
		{name: "onOk/unaliased",
			sql:  "SELECT lat_ord.id AS v FROM lat_ord JOIN lat_item ON lat_ord.id = lat_item.order_id",
			want: "rows=4 1 | 1 | 2 | 2"},
		{name: "onOk/correlatedOuterLevel",
			sql: "SELECT o.id AS v FROM lat_ord o WHERE EXISTS (SELECT 1 FROM lat_item b JOIN lat_item c " +
				"ON b.id = c.id AND o.id = b.order_id)",
			want: "rows=2 1 | 2"},
		{name: "onOk/lateralRight",
			sql:  "SELECT a.id AS v FROM lat_ord a JOIN LATERAL (SELECT i.id AS m FROM lat_item i WHERE i.order_id = a.id) s ON s.m > 0",
			want: "rows=4 1 | 1 | 2 | 2"},
		{name: "winOk/partition",
			sql:  "SELECT o.id AS v, COUNT(*) OVER (PARTITION BY o.id) AS n FROM lat_ord o",
			want: "rows=3 1,1 | 2,1 | 3,1"},
		{name: "winOk/argOverJoin",
			sql:  "SELECT SUM(b.amount) OVER (PARTITION BY a.id) AS n FROM lat_ord a JOIN lat_item b ON a.id = b.order_id",
			want: "rows=4 150 | 150 | 200 | 200"},
		{name: "winOk/overDerived",
			sql:  "SELECT x.id AS v, SUM(x.total) OVER (PARTITION BY x.id) AS n FROM (SELECT id, total FROM lat_ord) x",
			want: "rows=3 1,150 | 2,200 | 3,0"},
		{name: "winOk/grouped",
			sql: "SELECT o.id AS g, SUM(o.total) AS s, ROW_NUMBER() OVER (PARTITION BY o.id ORDER BY SUM(o.total)) AS rn " +
				"FROM lat_ord o GROUP BY o.id",
			want: "rows=3 1,150,1 | 2,200,1 | 3,0,1"},
		{name: "winOk/frameOffset",
			sql:  "SELECT o.id AS v, SUM(o.total) OVER (ORDER BY o.id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS n FROM lat_ord o",
			want: "rows=3 1,150 | 2,350 | 3,200"},
		{name: "winOk/partitionSiblingJoin",
			sql:  "SELECT COUNT(*) OVER (PARTITION BY b.order_id) AS n FROM lat_ord a JOIN lat_item b ON a.id = b.order_id",
			want: "rows=4 2 | 2 | 2 | 2"},
		{name: "starOk/ownAlias", sql: "SELECT o.* FROM lat_ord o",
			want: "rows=3 1,Alice,150 | 2,Bob,200 | 3,Carol,0"},
		{name: "starOk/laterJoinVisibleInSelect",
			sql:  "SELECT j.* FROM lat_ord a JOIN lat_item b ON a.id = b.order_id JOIN lat_item j ON j.id = a.id",
			want: "rows=4 1,1,Widget,50 | 1,1,Widget,50 | 2,1,Gadget,100 | 2,1,Gadget,100"},
		{name: "dupOk/oneAliasedOneNot",
			sql:  "SELECT lat_item.order_id AS v FROM lat_item JOIN lat_item b ON lat_item.id = b.id",
			want: "rows=4 1 | 1 | 2 | 2"},
		{name: "dupOk/derivedAliasEqualsBaseName",
			sql:  "SELECT lat_item.id AS v FROM (SELECT id FROM lat_ord) lat_item JOIN lat_item b ON lat_item.id = b.id",
			want: "rows=3 1 | 2 | 3"},
		// A DELIMITED alias keeps its bytes, so `t` and `"T"` are two names
		// and PostgreSQL answers this: a duplicate verdict taken on the
		// FOLDED key would refuse it (#731).
		{name: "dupOk/delimitedAliasIsADifferentName",
			sql:  `SELECT t.id AS v FROM lat_ord t, lat_item "T"`,
			want: "rows=12 1 | 1 | 1 | 1 | 2 | 2 | 2 | 2 | 3 | 3 | 3 | 3"},
		{name: "posOk/usingMergedQualified", sql: "SELECT a.id AS v FROM lat_item a JOIN lat_item b USING (id)",
			want: "rows=4 1 | 2 | 3 | 4"},
		{name: "posOk/correlatedSubquery",
			sql:  "SELECT o.id AS v, (SELECT MAX(z.amount) FROM lat_item z WHERE z.order_id = o.id) AS m FROM lat_ord o",
			want: "rows=3 1,100 | 2,125 | 3,NULL"},
		{name: "posOk/lateralBodyOuter",
			sql:  "SELECT o.id AS v, s.m AS m FROM lat_ord o JOIN LATERAL (SELECT o.total AS m) s ON true",
			want: "rows=3 1,150 | 2,200 | 3,0"},
	}
}

func TestArcRSAQualifiedReferenceNamesOneRelationOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the reference-scope table")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	t.Cleanup(cancel)
	arms := r1Arms(t, ctx)
	answered := 0
	for _, tc := range rsArmCells() {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				got := arm.run(tc.sql)
				if tc.refuse != "" {
					if !strings.HasPrefix(got, "ERR ") {
						t.Errorf("%s\n  arm  %s\n  got  %s\n  every arm must refuse: PostgreSQL 17.11 raises %s",
							tc.sql, arm.name, got, tc.refuse)
						continue
					}
					if !strings.Contains(got, tc.refuse) {
						t.Errorf("%s\n  arm  %s\n  got  %s\n  the refusal must carry PostgreSQL's sentence %q",
							tc.sql, arm.name, got, tc.refuse)
					}
					continue
				}
				answered++
				if got != tc.want {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (PostgreSQL 17.11)",
						tc.sql, arm.name, got, tc.want)
				}
			}
		})
	}
	// A TABLE WHOSE EVERY CELL REFUSES PROVES ONLY THAT THE ENGINE IS LOUD.
	if want := 27 * len(arms); answered != want {
		t.Fatalf("%d (control, arm) pairs answered, want %d: the controls are what "+
			"say this rule is a rule and not a ban", answered, want)
	}
}
