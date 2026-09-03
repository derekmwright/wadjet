package coordinator

import (
	"fmt"
	"strings"
	"testing"
)

// Every routing counter, read as ONE value.
//
// The arc's own position 9 is that a DAG-arm assertion carries the routing
// counter beside the rows, because "the DAG executed this" and "the DAG refused
// the plan and the coordinator-local pipeline answered" are different states
// that identical rows cannot tell apart (correctness protocol rule 11). Six of
// this arc's nine gates shipped without one, and five of their cells are
// TABLE-LESS — `SELECT .5`, `SELECT CAST(-0.5 AS BIGINT)` — which route on
// #806's refusal, so their two DAG arms were running the same coordinator-local
// engine the `single` arm had already run. Three arms, one engine, and nothing
// said so.
//
// All NINE counters are read rather than the one that happened to fire. A cell
// that starts routing on a DIFFERENT refusal is the same blindness wearing
// another name, and reading the whole set costs nine atomic loads per cell.
type a2Routes struct {
	TableLess         int64
	UnbuildableStage  int64
	UnreachableOutput int64
	ScalarProjection  int64
	InSubquery        int64
	Correlated        int64
	Distinct          int64
	GroupingSets      int64
	GroupKey          int64
}

func a2ReadRoutes(c *Coordinator) a2Routes {
	return a2Routes{
		TableLess:         c.TableLessLocalRoutes(),
		UnbuildableStage:  c.UnbuildableStageLocalRoutes(),
		UnreachableOutput: c.UnreachableOutputLocalRoutes(),
		ScalarProjection:  c.ScalarProjectionLocalRoutes(),
		InSubquery:        c.InSubqueryLocalRoutes(),
		Correlated:        c.CorrelatedLocalRoutes(),
		Distinct:          c.DistinctLocalRoutes(),
		GroupingSets:      c.GroupingSetsLocalRoutes(),
		GroupKey:          c.GroupKeyLocalRoutes(),
	}
}

func (a a2Routes) minus(b a2Routes) a2Routes {
	return a2Routes{
		TableLess:         a.TableLess - b.TableLess,
		UnbuildableStage:  a.UnbuildableStage - b.UnbuildableStage,
		UnreachableOutput: a.UnreachableOutput - b.UnreachableOutput,
		ScalarProjection:  a.ScalarProjection - b.ScalarProjection,
		InSubquery:        a.InSubquery - b.InSubquery,
		Correlated:        a.Correlated - b.Correlated,
		Distinct:          a.Distinct - b.Distinct,
		GroupingSets:      a.GroupingSets - b.GroupingSets,
		GroupKey:          a.GroupKey - b.GroupKey,
	}
}

// String names only the counters that moved, so a failure reads as
// "tableless+1" rather than as a nine-field struct dump. The zero value is
// "none", which is what a shape that reached the DAG shows.
func (a a2Routes) String() string {
	var parts []string
	for _, f := range []struct {
		name string
		n    int64
	}{
		{"tableless", a.TableLess}, {"unbuildable", a.UnbuildableStage},
		{"unreach", a.UnreachableOutput}, {"scalarproj", a.ScalarProjection},
		{"insubquery", a.InSubquery}, {"correlated", a.Correlated},
		{"distinct", a.Distinct}, {"groupingsets", a.GroupingSets},
		{"groupkey", a.GroupKey},
	} {
		if f.n != 0 {
			parts = append(parts, fmt.Sprintf("%s%+d", f.name, f.n))
		}
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, " ")
}

// a2CheckRoutes asserts the routing delta one DAG run produced.
//
// want is the zero value for a shape the DAG EXECUTES, which is most of them;
// a cell whose want is non-zero is stating that its two DAG arms are the
// coordinator-local pipeline, and its claim is therefore about that path.
func a2CheckRoutes(t *testing.T, arm string, before, after, want a2Routes, sql string) {
	t.Helper()
	got := after.minus(before)
	if got != want {
		t.Errorf("%s arm: routing counters moved %s, want %s\n"+
			"  (none = the DAG executed this shape; anything else = it refused the plan "+
			"and the coordinator-local pipeline answered, so this arm is not a second engine)"+
			"\n  SQL: %s", arm, got, want, sql)
	}
}
