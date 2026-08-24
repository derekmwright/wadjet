package sqlancer.wadjet;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.PostgresVisitor;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.gen.PostgresExpressionGenerator;

/**
 * Minimal {@code INSERT INTO t(cols) VALUES (v1, v2), ...;} generator.
 *
 * <p>
 * Reuses {@code PostgresExpressionGenerator.generateConstant} for values
 * (the same literal-rendering machinery
 * {@code sqlancer.postgres.gen.PostgresInsertGenerator} uses, so the SQL
 * text it produces for a given constant is identical), but drops that
 * generator's {@code OVERRIDING SYSTEM|USER VALUE} clause — Postgres
 * identity-column syntax with no wadjet equivalent, a parse error not
 * covered by any ExpectedErrors entry — and its bulk-insert repeated-
 * tuple mode, which is untested against wadjet and off by default
 * anyway ({@code --bulk-insert} is not set by the harness).
 *
 * <p>
 * Picks from {@code getDatabaseTables()} directly rather than filtering
 * on {@code PostgresTable::isInsertable}: wadjet's
 * information_schema.tables leaves {@code is_insertable_into} NULL, which
 * the JDBC driver reads back as {@code false} for every table, so the
 * Postgres-style insertable filter empties the candidate list entirely.
 * {@link WadjetProvider} never creates views, so every generated table
 * is insertable in practice.
 */
public final class WadjetInsertGenerator {

    private WadjetInsertGenerator() {
    }

    public static SQLQueryAdapter insert(PostgresGlobalState globalState) {
        PostgresTable table = globalState.getSchema().getRandomTable();
        return insertRows(globalState, table);
    }

    private static SQLQueryAdapter insertRows(PostgresGlobalState globalState, PostgresTable table) {
        List<PostgresColumn> columns = table.getRandomNonEmptyColumnSubset();

        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(table.getName()).append("(");
        sb.append(columns.stream().map(PostgresColumn::getName).collect(Collectors.joining(", ")));
        sb.append(") VALUES ");

        int nrRows = Randomly.smallNumber() + 1;
        for (int i = 0; i < nrRows; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append("(");
            for (int j = 0; j < columns.size(); j++) {
                if (j != 0) {
                    sb.append(", ");
                }
                PostgresExpression constant = PostgresExpressionGenerator.generateConstant(globalState.getRandomly(),
                        columns.get(j).getType());
                sb.append(PostgresVisitor.asString(constant));
            }
            sb.append(")");
        }
        return new SQLQueryAdapter(sb.toString(), true);
    }
}
