// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// psqlListDatabases is the `\l` query psql 17 sends, by the server_version
// the server ADVERTISES (psql's describe.c listAllDbs): the locale column is
// datlocale from PostgreSQL 17, daticulocale for 15 and 16, and the ICU rules
// column exists from 16. A server that advertises one major and carries
// another's catalog answers the query psql chose with 42703.
var psqlListDatabases = map[int]string{
	15: psqlListDatabasesQuery("d.daticulocale", "NULL"),
	16: psqlListDatabasesQuery("d.daticulocale", "d.daticurules"),
	17: psqlListDatabasesQuery("d.datlocale", "d.daticurules"),
}

func psqlListDatabasesQuery(locale, rules string) string {
	return `SELECT
  d.datname as "Name",
  pg_catalog.pg_get_userbyid(d.datdba) as "Owner",
  pg_catalog.pg_encoding_to_char(d.encoding) as "Encoding",
  CASE d.datlocprovider WHEN 'b' THEN 'builtin' WHEN 'c' THEN 'libc' WHEN 'i' THEN 'icu' END AS "Locale Provider",
  d.datcollate as "Collate",
  d.datctype as "Ctype",
  ` + locale + ` as "Locale",
  ` + rules + ` as "ICU Rules",
  CASE WHEN pg_catalog.array_length(d.datacl, 1) = 0 THEN '(none)' ELSE pg_catalog.array_to_string(d.datacl, E'\n') END AS "Access privileges"
FROM pg_catalog.pg_database d
ORDER BY 1;`
}

// TestArcPCTheWireAdvertisesTheVersionTheCatalogModels is arc PC round 3's
// B1a gate. pg_catalog is PostgreSQL 17's (syscatalog's pg17_relations.tsv,
// ADR-0044), and a tool picks its catalog spellings by the version the
// server advertises: psql 17 `\l` against an advertised 15 asked for
// pg_database.daticulocale, which PostgreSQL 17 renamed, and raised 42703
// live while the corpus (recorded against 17) passed. Every place the
// version is reported — the startup ParameterStatus, SHOW, current_setting,
// version() — names ONE major, it is the catalog's, and the `\l` psql sends
// for that major answers.
func TestArcPCTheWireAdvertisesTheVersionTheCatalogModels(t *testing.T) {
	db := pcCorpusDB(t)
	ctx := context.Background()
	srv := startTestServer(t, db)
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://wadjet@%s/wadjet?sslmode=disable", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(ctx) })

	major := func(where, v string) int {
		t.Helper()
		m, err := strconv.Atoi(strings.SplitN(strings.TrimSpace(v), ".", 2)[0])
		if err != nil {
			t.Fatalf("%s = %q does not start with a major version", where, v)
		}
		return m
	}
	one := func(sql string) string {
		t.Helper()
		var v string
		if err := conn.QueryRow(ctx, sql, pgx.QueryExecModeSimpleProtocol).Scan(&v); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		return v
	}
	startup := major("ParameterStatus server_version", conn.PgConn().ParameterStatus("server_version"))
	num, err := strconv.Atoi(one("SHOW server_version_num"))
	if err != nil {
		t.Fatal(err)
	}
	reported := map[string]int{
		"ParameterStatus server_version":        startup,
		"SHOW server_version":                   major("SHOW server_version", one("SHOW server_version")),
		"SHOW server_version_num":               num / 10000,
		"current_setting('server_version_num')": mustAtoi(t, one("SELECT current_setting('server_version_num')")) / 10000,
		"version()":                             major("version()", strings.TrimPrefix(one("SELECT version()"), "PostgreSQL ")),
	}
	for where, m := range reported {
		if m != 17 {
			t.Errorf("%s reports PostgreSQL %d; the catalog is PostgreSQL 17's", where, m)
		}
	}
	q, ok := psqlListDatabases[startup]
	if !ok {
		t.Fatalf("the server advertises PostgreSQL %d, for which psql's \\l spelling is not recorded here", startup)
	}
	if o := pcRawText(ctx, conn, q); o.Err != "" || len(o.Rows) != 1 {
		t.Errorf("psql's \\l for the advertised PostgreSQL %d: %+v", startup, o)
	}
}

func mustAtoi(t *testing.T, s string) int {
	t.Helper()
	n, err := strconv.Atoi(s)
	if err != nil {
		t.Fatal(err)
	}
	return n
}
