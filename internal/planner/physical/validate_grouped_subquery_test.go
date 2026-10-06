// SPDX-License-Identifier: MIT

package physical

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

func TestArcCI1GroupedSubqueryReferences(t *testing.T) {
	cat := &fakeCatalog{tables: map[string][]string{"ss_t": {"id", "i"}, "ss_i": {"id", "v"}}}
	for _, tc := range []struct {
		name, sql string
		refused   bool
	}{
		{"expression", "SELECT i + 1, (SELECT max(x.v) FROM ss_i x WHERE x.v > i + 1) FROM ss_t t GROUP BY t.i + 1", true},
		{"grouped", "SELECT i, (SELECT max(x.v) FROM ss_i x WHERE x.v > i + 1) FROM ss_t t GROUP BY t.i", false},
		{"alias", "SELECT count(*) AS i FROM ss_t t GROUP BY t.i + 1 HAVING EXISTS (SELECT 1 FROM ss_i x WHERE x.v > t.i)", true},
		{"shadow", "SELECT count(*), (SELECT max(t.i) FROM ss_t t) FROM ss_t t GROUP BY t.i + 1", false},
		{"twoLevels", "SELECT count(*), (SELECT max(x.v) FROM ss_i x WHERE EXISTS (SELECT 1 FROM ss_i y WHERE y.v > t.i)) FROM ss_t t GROUP BY t.i + 1", true},
		{"nearerLevel", "SELECT count(*), (SELECT max(x.v) FROM ss_i x WHERE EXISTS (SELECT 1 FROM ss_i y WHERE y.v > x.v)) FROM ss_t t GROUP BY t.i + 1", false},
		{"union", "SELECT count(*), EXISTS (SELECT v FROM ss_i WHERE v > t.i UNION ALL SELECT v FROM ss_i) FROM ss_t t GROUP BY t.i + 1", true},
		{"windowGroupedSubquery", "SELECT rank() OVER (ORDER BY (SELECT max(v) FROM ss_i WHERE v > t.i)) FROM ss_t t GROUP BY (SELECT max(v) FROM ss_i WHERE v > t.i)", false},
		{"windowNestedAggregate", "SELECT sum(sum((SELECT max(v) FROM ss_i WHERE v > t.i))) OVER () FROM ss_t t GROUP BY t.i + 1", false},
		{"window", "SELECT count(*), rank() OVER (PARTITION BY (SELECT max(v) FROM ss_i WHERE v > t.i)) FROM ss_t t GROUP BY t.i + 1", true},
		{"outerAggregate", "SELECT (SELECT max(t.i) FROM ss_i x LIMIT 1) FROM ss_t t GROUP BY t.i + 1", false},
		{"mixedAggregate", "SELECT (SELECT max(t.i + x.v) FROM ss_i x) FROM ss_t t GROUP BY t.i + 1", true},
		{"aggregate", "SELECT max((SELECT max(v) FROM ss_i WHERE v > t.i)) FROM ss_t t GROUP BY t.i + 1", false},
		{"where", "SELECT count(*) FROM ss_t t WHERE EXISTS (SELECT 1 FROM ss_i WHERE v > t.i) GROUP BY t.i + 1", false},
		{"noKeys", "SELECT count(*), (SELECT max(v) FROM ss_i WHERE v > t.i) FROM ss_t t", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := bindColumns(context.Background(), cat, mustExtract(t, tc.sql), true)
			if !tc.refused {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || sqlerr.StateOf(err) != "42803" || !strings.Contains(err.Error(), `subquery uses ungrouped column "t.i" from outer query`) {
				t.Fatalf("want outer-column grouping refusal, got %v", err)
			}
		})
	}
}
