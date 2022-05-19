/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.singlestore;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.singlestore.SingleStoreQueryRunner.createSingleStoreQueryRunner;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestSingleStoreConnectorTest
        extends BaseJdbcConnectorTest
{
    protected TestingSingleStoreServer singleStoreServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        singleStoreServer = new TestingSingleStoreServer();
        return createSingleStoreQueryRunner(singleStoreServer, ImmutableMap.of(), ImmutableMap.of(), REQUIRED_TPCH_TABLES);
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        singleStoreServer.close();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY:
            case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY:
                return false;

            case SUPPORTS_AGGREGATION_PUSHDOWN:
                return false;

            case SUPPORTS_JOIN_PUSHDOWN:
                return true;

            case SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN:
            case SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM:
                return false;

            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
            case SUPPORTS_ADD_COLUMN_WITH_COMMENT:
                return false;

            case SUPPORTS_ARRAY:
            case SUPPORTS_ROW_TYPE:
                return false;

            case SUPPORTS_RENAME_SCHEMA:
            case SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                onRemoteDatabase(),
                "tpch.table",
                "(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT DEFAULT 43," +
                        "col_nonnull_default BIGINT NOT NULL DEFAULT 42," +
                        "col_required2 BIGINT NOT NULL)");
    }

    @Override
    protected TestTable createTableWithUnsupportedColumn()
    {
        return new TestTable(
                onRemoteDatabase(),
                "tpch.test_unsupported_column_present",
                "(one bigint, two decimal(50,0), three varchar(10))");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();

        if (typeName.equals("boolean")) {
            // SingleStore does not have built-in support for boolean type. SingleStore provides BOOLEAN as the synonym of TINYINT(1)
            // Querying the column with a boolean predicate subsequently fails with "Cannot apply operator: tinyint = boolean"
            return Optional.empty();
        }

        if (typeName.equals("time")) {
            // SingleStore supports only second precision
            // Skip 'time' that is alias of time(3) here and add test cases in TestSingleStoreTypeMapping.testTime instead
            return Optional.empty();
        }

        if (typeName.equals("timestamp(3) with time zone")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        if (typeName.equals("timestamp")) {
            // TODO this should either work or fail cleanly
            return Optional.empty();
        }

        if (typeName.equals("varchar")) {
            // TODO fails due to case insensitive UTF-8 comparisons
            return Optional.empty();
        }

        return Optional.of(dataMappingTestSetup);
    }

    @Override
    public void testInsertUnicode()
    {
        // SingleStore's utf8 encoding is 3 bytes and truncates strings upon encountering a 4 byte sequence
        throw new SkipException("SingleStore doesn't support utf8mb4");
    }

    @Override
    public void testInsertHighestUnicodeCharacter()
    {
        // SingleStore's utf8 encoding is 3 bytes and truncates strings upon encountering a 4 byte sequence
        throw new SkipException("SingleStore doesn't support utf8mb4");
    }

    @Override
    public void testDeleteWithLike()
    {
        assertThatThrownBy(super::testDeleteWithLike)
                .hasStackTraceContaining("TrinoException: Unsupported delete");
    }

    @Test
    public void testReadFromView()
    {
        onRemoteDatabase().execute("CREATE VIEW tpch.test_view AS SELECT * FROM tpch.orders");
        assertQuery("SELECT orderkey FROM test_view", "SELECT orderkey FROM orders");
        onRemoteDatabase().execute("DROP VIEW IF EXISTS tpch.test_view");
    }

    @Test
    public void testNameEscaping()
    {
        assertFalse(getQueryRunner().tableExists(getSession(), "test_table"));

        assertUpdate(getSession(), "CREATE TABLE test_table AS SELECT 123 x", 1);
        assertTrue(getQueryRunner().tableExists(getSession(), "test_table"));

        assertQuery(getSession(), "SELECT * FROM test_table", "SELECT 123");

        assertUpdate(getSession(), "DROP TABLE test_table");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_table"));
    }

    @Test
    public void testSingleStoreTinyint()
    {
        onRemoteDatabase().execute("CREATE TABLE tpch.mysql_test_tinyint1 (c_tinyint tinyint(1))");

        MaterializedResult actual = computeActual("SHOW COLUMNS FROM mysql_test_tinyint1");
        MaterializedResult expected = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("c_tinyint", "tinyint", "", "")
                .build();

        assertEquals(actual, expected);

        onRemoteDatabase().execute("INSERT INTO tpch.mysql_test_tinyint1 VALUES (127), (-128)");
        MaterializedResult materializedRows = computeActual("SELECT * FROM tpch.mysql_test_tinyint1 WHERE c_tinyint = 127");
        assertEquals(materializedRows.getRowCount(), 1);
        MaterializedRow row = getOnlyElement(materializedRows);

        assertEquals(row.getFields().size(), 1);
        assertEquals(row.getField(0), (byte) 127);

        assertUpdate("DROP TABLE mysql_test_tinyint1");
    }

    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return format(".* Field '%s' doesn't have a default value", columnName);
    }

    @Test
    public void testColumnComment()
    {
        // TODO add support for setting comments on existing column and replace the test with io.trino.testing.BaseConnectorTest#testCommentColumn

        onRemoteDatabase().execute("CREATE TABLE tpch.test_column_comment (col1 bigint COMMENT 'test comment', col2 bigint COMMENT '', col3 bigint)");

        assertQuery(
                "SELECT column_name, comment FROM information_schema.columns WHERE table_schema = 'tpch' AND table_name = 'test_column_comment'",
                "VALUES ('col1', 'test comment'), ('col2', null), ('col3', null)");

        assertUpdate("DROP TABLE test_column_comment");
    }

    @Test
    public void testPredicatePushdown()
    {
        // varchar equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'ROMANIA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isNotFullyPushedDown(FilterNode.class);

        // varchar range
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name BETWEEN 'POLAND' AND 'RPA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isNotFullyPushedDown(FilterNode.class);

        // varchar different case
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'romania'"))
                .returnsEmptyResult()
                .isNotFullyPushedDown(FilterNode.class);

        // bigint equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE nationkey = 19"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // bigint range, with decimal to bigint simplification
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE nationkey BETWEEN 18.5 AND 19.5"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // date equality
        assertThat(query("SELECT orderkey FROM orders WHERE orderdate = DATE '1992-09-29'"))
                .matches("VALUES BIGINT '1250', 34406, 38436, 57570")
                .isFullyPushedDown();

        // predicate over aggregation key (likely to be optimized before being pushed down into the connector)
        assertThat(query("SELECT * FROM (SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey) WHERE regionkey = 3"))
                .matches("VALUES (BIGINT '3', BIGINT '77')")
                .isNotFullyPushedDown(AggregationNode.class);

        // predicate over aggregation result
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey HAVING sum(nationkey) = 77"))
                .matches("VALUES (BIGINT '3', BIGINT '77')")
                .isNotFullyPushedDown(AggregationNode.class);
    }

    @Override
    public void testCreateTableAsSelectNegativeDate()
    {
        // TODO (https://github.com/trinodb/trino/issues/10320) SingleStore stores '0000-00-00' when inserted negative dates and it throws an exception during reading the row
        assertThatThrownBy(super::testCreateTableAsSelectNegativeDate)
                .hasStackTraceContaining("TrinoException: Driver returned null LocalDate for a non-null value");
    }

    @Test
    @Override
    public void testInsertNegativeDate()
    {
        // TODO (https://github.com/trinodb/trino/issues/10320) SingleStore stores '0000-00-00' when inserted negative dates and it throws an exception during reading the row
        assertThatThrownBy(super::testInsertNegativeDate)
                .hasStackTraceContaining("TrinoException: Driver returned null LocalDate for a non-null value");
    }

    /**
     * This test helps to tune TupleDomain simplification threshold.
     */
    @Test
    public void testNativeLargeIn()
    {
        onRemoteDatabase().execute("SELECT count(*) FROM tpch.orders WHERE " + getLongInClause(0, 300_000));
    }

    /**
     * This test helps to tune TupleDomain simplification threshold.
     */
    @Test
    public void testNativeMultipleInClauses()
    {
        String longInClauses = range(0, 30)
                .mapToObj(value -> getLongInClause(value * 10_000, 10_000))
                .collect(joining(" OR "));
        onRemoteDatabase().execute("SELECT count(*) FROM tpch.orders WHERE " + longInClauses);
    }

    private String getLongInClause(int start, int length)
    {
        String longValues = range(start, start + length)
                .mapToObj(Integer::toString)
                .collect(joining(", "));
        return "orderkey IN (" + longValues + ")";
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return singleStoreServer::execute;
    }
}
