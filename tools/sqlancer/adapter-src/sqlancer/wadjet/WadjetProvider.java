package sqlancer.wadjet;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.google.auto.service.AutoService;

import sqlancer.AbstractAction;
import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.StatementExecutor;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresOptions;
import sqlancer.postgres.PostgresProvider;

/**
 * Minimal SQLancer adapter for wadjet's PostgreSQL wire endpoint (wadjet
 * issue #289).
 *
 * <p>
 * Wadjet speaks enough of the pgwire protocol, and enough of
 * information_schema/pg_catalog (verified empirically: the exact
 * introspection queries {@code sqlancer.postgres.PostgresSchema} issues —
 * the information_schema/pg_class/pg_namespace table listing, per-table
 * information_schema.columns, pg_indexes, pg_statistic_ext, pg_collation,
 * pg_opclass, pg_operator, pg_am, pg_proc — all either return correct rows
 * or a valid empty result set, never a parse error) that SQLancer's
 * existing Postgres schema introspection, expression generation, and
 * TLP/NoREC/PQS oracle machinery (sqlancer.postgres.*) all work unmodified
 * against it. This class therefore extends {@link PostgresProvider} and
 * reuses its schema/expression/oracle layer as-is.
 *
 * <p>
 * What does <b>not</b> carry over is the DDL/action surface.
 * Empirically, against wadjet's actual parser
 * (internal/planner/sql/parser.go):
 * <ul>
 * <li>CREATE TABLE only understands {@code name TYPE [NOT NULL]} column
 * definitions (single-token type, optional precision/scale) plus a
 * trailing {@code PARTITION BY (cols...)} — no PRIMARY KEY / UNIQUE /
 * CHECK / FOREIGN KEY / DEFAULT / COLLATE / GENERATED, no
 * TEMPORARY/UNLOGGED, no LIKE/INHERITS/USING.
 * <li>CREATE DATABASE / DROP DATABASE parse-error outright — wadjet has
 * one catalog namespace, not Postgres's per-connection multi-database
 * model.
 * <li>CREATE INDEX and CREATE VIEW parse-error.
 * <li>VACUUM / CLUSTER / REINDEX / TRUNCATE / statistics objects /
 * sequences / NOTIFY-LISTEN / tablespaces / SET CONSTRAINTS / COMMENT ON
 * / transactions (BEGIN is accepted but is a pgwire no-op) are all
 * either unparseable or meaningless here.
 * </ul>
 * {@link PostgresProvider}'s {@code Action} enum and
 * {@code PostgresTableGenerator}/{@code PostgresInsertGenerator} assume
 * most of the above, so this provider replaces the action set and the
 * table/insert generators with wadjet-shaped minimal equivalents
 * ({@link WadjetTableGenerator}, {@link WadjetInsertGenerator}) and
 * replaces the per-round database bootstrap
 * ({@link PostgresProvider#createDatabase}, which issues
 * {@code DROP DATABASE}/{@code CREATE DATABASE}) with one that connects
 * once to the single database named in {@code --connection-url} and
 * drops any tables left over from a previous round.
 *
 * <p>
 * {@code UPDATE}/{@code DELETE} are deliberately excluded from the action
 * mix — see wadjet#483: on the pgwire/standalone path, once a table has
 * been read once, further writes to it (of any kind, not just
 * UPDATE/DELETE) are not visible to later reads. This provider's schema
 * generation phase runs pure INSERTs with no reads interleaved, so the
 * bug does not corrupt this harness's own results (each round's tables
 * are fully populated before the oracle phase issues its first SELECT),
 * but campaigns that want the DML surface exercised must wait on that
 * fix first — see tools/sqlancer/README.md.
 */
@AutoService(DatabaseProvider.class)
public class WadjetProvider extends PostgresProvider {

    protected String entryURL;
    protected String username;
    protected String password;
    protected String host;
    protected int port;
    protected String databaseName;

    public WadjetProvider() {
        super(PostgresGlobalState.class, PostgresOptions.class);
    }

    public enum Action implements AbstractAction<PostgresGlobalState> {

        INSERT(WadjetInsertGenerator::insert);

        private final SQLQueryProvider<PostgresGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<PostgresGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(PostgresGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    private static int mapActions(PostgresGlobalState globalState, Action a) {
        switch (a) {
        case INSERT:
            return globalState.getRandomly().getInteger(1, globalState.getOptions().getMaxNumberInserts() + 1);
        default:
            throw new AssertionError(a);
        }
    }

    @Override
    public String getDBMSName() {
        return "wadjet";
    }

    @Override
    public void generateDatabase(PostgresGlobalState globalState) throws Exception {
        readFunctions(globalState);
        createTables(globalState, Randomly.fromOptions(4, 5, 6));
        prepareTables(globalState);
    }

    @Override
    protected void createTables(PostgresGlobalState globalState, int numTables) throws Exception {
        while (globalState.getSchema().getDatabaseTables().size() < numTables) {
            try {
                String tableName = DBMSCommon.createTableName(globalState.getSchema().getDatabaseTables().size());
                SQLQueryAdapter createTable = WadjetTableGenerator.generate(tableName, globalState);
                globalState.executeStatement(createTable);
            } catch (IgnoreMeException e) {
                // retry with a freshly generated table shape
            }
        }
    }

    @Override
    protected void prepareTables(PostgresGlobalState globalState) throws Exception {
        StatementExecutor<PostgresGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                WadjetProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
    }

    /**
     * Wadjet has one catalog namespace (CREATE DATABASE/DROP DATABASE both
     * parse-error), so — instead of {@link PostgresProvider#createDatabase}'s
     * per-round {@code DROP DATABASE}/{@code CREATE DATABASE} — this connects
     * once to the database named in {@code --connection-url} and drops any
     * tables left over from a previous round, giving each round the same
     * "fresh database" starting point a real {@code CREATE DATABASE} would.
     */
    @Override
    public SQLConnection createDatabase(PostgresGlobalState globalState) throws SQLException {
        generateOnlyKnown = true;

        username = globalState.getOptions().getUserName();
        password = globalState.getOptions().getPassword();
        host = globalState.getOptions().getHost();
        port = globalState.getOptions().getPort();
        entryURL = globalState.getDbmsSpecificOptions().connectionURL;
        if (entryURL.startsWith("jdbc:")) {
            entryURL = entryURL.substring(5);
        }
        String entryPath = "/wadjet";
        databaseName = globalState.getDatabaseName();

        try {
            URI uri = new URI(entryURL);
            String pathURI = uri.getPath();
            if (pathURI != null && !pathURI.isEmpty()) {
                entryPath = pathURI;
            }
            if (host == null) {
                host = uri.getHost();
            }
            if (port == MainOptions.NO_SET_PORT) {
                port = uri.getPort();
            }
            entryURL = String.format("%s://%s:%d%s", uri.getScheme(), host, port, entryPath);
        } catch (URISyntaxException e) {
            throw new AssertionError(e);
        }

        globalState.getState().logStatement(String.format("\\c %s;", entryPath.substring(1)));
        Connection con = DriverManager.getConnection("jdbc:" + entryURL, username, password);

        dropLeftoverTables(globalState, con);

        return new SQLConnection(con);
    }

    private void dropLeftoverTables(PostgresGlobalState globalState, Connection con) throws SQLException {
        List<String> tables = new ArrayList<>();
        try (Statement s = con.createStatement();
                ResultSet rs = s.executeQuery(
                        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")) {
            while (rs.next()) {
                tables.add(rs.getString(1));
            }
        }
        for (String t : tables) {
            String dropSQL = "DROP TABLE " + t;
            globalState.getState().logStatement(dropSQL + ";");
            try (Statement s = con.createStatement()) {
                s.execute(dropSQL);
            }
        }
    }

}
