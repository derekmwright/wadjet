package sqlancer.wadjet;

import sqlancer.common.DBMSCommon;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema.PostgresDataType;

/**
 * Emits {@code CREATE TABLE name (col TYPE, ...)} in wadjet's restricted
 * grammar (internal/planner/sql/parser.go: lexParseCreateTable) — a
 * single-token type name per column, no constraints. {@code
 * sqlancer.postgres.gen.PostgresTableGenerator} cannot be reused here: it
 * generates PRIMARY KEY/UNIQUE/CHECK/FOREIGN KEY/DEFAULT/COLLATE
 * constraints, TEMPORARY/UNLOGGED tables, LIKE/INHERITS/USING clauses, and
 * PARTITION BY RANGE/LIST/HASH with arbitrary expressions and opclasses —
 * all either a parse error or meaningless against wadjet's parser, and
 * none covered by ExpectedErrors (those are Postgres error strings).
 *
 * <p>
 * Columns are always nullable (no NOT NULL) for this first pass: whether
 * wadjet enforces NOT NULL on INSERT, and with what error text, is
 * untested territory that would otherwise need its own ExpectedErrors
 * entries. Widening the grammar (NOT NULL, PARTITION BY) is a follow-up
 * once the pilot's baseline signal is characterized — see
 * tools/sqlancer/README.md.
 *
 * <p>
 * The type pool is INT/BOOLEAN/TEXT (rendered as wadjet's BIGINT/BOOL/
 * TEXT) — the same restriction {@code PostgresDataType.getRandomType()}
 * already applies when {@code PostgresProvider.generateOnlyKnown} is set
 * (which {@link WadjetProvider} always does). This keeps the type
 * universe to wadjet-native scalar types and deliberately excludes
 * DECIMAL/FLOAT so the pilot's findings are not just re-reports of the
 * DECIMAL/float correctness work already in flight (wadjet#459, #462,
 * #463, #465, #474, #475, #476, #477) — see #289's pilot summary for the
 * full list. Widening to DECIMAL/FLOAT, and to wadjet's network types
 * (IPV4/IPV6/CIDR/MAC/PORT/PROTOCOL, which have no Postgres equivalent
 * and so need generator code of their own) is future work.
 */
public final class WadjetTableGenerator {

    private WadjetTableGenerator() {
    }

    public static SQLQueryAdapter generate(String tableName, PostgresGlobalState globalState) {
        // Single-digit column names only: PostgresSchema.getTableColumns()
        // reads columns back with "ORDER BY column_name", a lexical sort —
        // c10 would sort before c2. Capping at 8 columns (c0..c7) keeps
        // every name single-digit so lexical and ordinal order agree.
        int nrColumns = globalState.getRandomly().getInteger(1, 9);

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(tableName).append(" (");
        for (int i = 0; i < nrColumns; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(DBMSCommon.createColumnName(i)).append(" ")
                    .append(wadjetTypeName(PostgresDataType.getRandomType()));
        }
        sb.append(")");
        return new SQLQueryAdapter(sb.toString(), true);
    }

    private static String wadjetTypeName(PostgresDataType type) {
        switch (type) {
        case INT:
            return "BIGINT";
        case BOOLEAN:
            return "BOOL";
        case TEXT:
            return "TEXT";
        default:
            // PostgresDataType.getRandomType() only returns INT/BOOLEAN/TEXT
            // while WadjetProvider.generateOnlyKnown is true, which it always
            // is here (set unconditionally in WadjetProvider#createDatabase).
            throw new AssertionError(type);
        }
    }
}
