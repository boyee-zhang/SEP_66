/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.prestoconnector;

import io.prestosql.Session;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.testing.AbstractTestIntegrationSmokeTest;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.sql.TestTable;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.createPrestoConnectorQueryRunner;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.createRemotePrestoQueryRunnerWithMemory;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.prestoConnectorConnectionUrl;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPrestoConnectorIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private DistributedQueryRunner remotePresto;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        remotePresto = closeAfterClass(createRemotePrestoQueryRunnerWithMemory(
                Map.of(),
                List.of()));
        DistributedQueryRunner queryRunner = createPrestoConnectorQueryRunner(
                false,
                Map.of(),
                Map.of("connection-url", prestoConnectorConnectionUrl(remotePresto, "tpch")));
        try {
            queryRunner.createCatalog("p2p_memory", "presto-connector", Map.of(
                    "connection-user", "p2p",
                    "connection-url", prestoConnectorConnectionUrl(remotePresto, "memory")));
        }
        catch (Exception e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }

        return queryRunner;
    }

    @Test
    public void testReadFromRemoteMemoryView()
    {
        Session memorySession = testSessionBuilder()
                .setCatalog("memory")
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        remotePresto.executeWithQueryId(memorySession, "CREATE OR REPLACE VIEW nation_count AS SELECT COUNT(*) cnt FROM tpch.tiny.nation");

        Session p2p = testSessionBuilder()
                .setCatalog("p2p_memory")
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        assertQuery(p2p, "SELECT cnt FROM nation_count", "SELECT 25");
    }

    @Test
    public void testAggregationPushdown()
    {
        // TODO support aggregation pushdown with GROUPING SETS
        // TODO support aggregation over expressions

        // SELECT DISTINCT
        assertThat(query("SELECT DISTINCT regionkey FROM nation")).isFullyPushedDown();

        // count()
        assertThat(query("SELECT count(*) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count(nationkey) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count(1) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count() FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count(DISTINCT regionkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);

        // GROUP BY
        assertThat(query("SELECT regionkey, min(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, max(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, avg(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();

        // GROUP BY and WHERE on bigint column
        // GROUP BY and WHERE on aggregation key
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation WHERE regionkey < 4 GROUP BY regionkey")).isFullyPushedDown();

        // GROUP BY and WHERE on varchar column
        // GROUP BY and WHERE on "other" (not aggregation key, not aggregation input)
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation WHERE regionkey < 4 AND name > 'AAA' GROUP BY regionkey")).isFullyPushedDown();

        // decimals
        try (TestTable testTable = new TestTable(
                remotePresto::execute,
                "memory.default.test_aggregation_pushdown",
                "(short_decimal decimal(9, 3), long_decimal decimal(30, 10))",
                List.of(
                        "(100.000, 100000000.000000000)",
                        "(123.321, 123456789.987654321)"))) {
            String tableName = "p2p_" + testTable.getName();
            assertThat(query("SELECT min(short_decimal), min(long_decimal) FROM " + tableName)).isFullyPushedDown();
            assertThat(query("SELECT max(short_decimal), max(long_decimal) FROM " + tableName)).isFullyPushedDown();
            assertThat(query("SELECT sum(short_decimal), sum(long_decimal) FROM " + tableName)).isFullyPushedDown();
            assertThat(query("SELECT avg(short_decimal), avg(long_decimal) FROM " + tableName)).isFullyPushedDown();
        }
    }

    @Test
    public void testAggregationPushdownWithPrestoSpecificFunctions()
    {
        assertThat(query("SELECT approx_distinct(regionkey) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT approx_distinct(name) FROM nation GROUP BY regionkey")).isFullyPushedDown();

        assertThat(query("SELECT max_by(name, nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();

        // checksum returns varbinary
        assertThat(query("SELECT checksum(nationkey) FROM nation")).isFullyPushedDown();

        // geometric_mean returns double
        assertThat(query("SELECT geometric_mean(nationkey) FROM nation")).isFullyPushedDown();
    }

    @Test
    public void testAggregationWithUnsupportedResultType()
    {
        // array_agg returns array, which is not supported
        assertThat(query("SELECT array_agg(nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);

        // histogram returns map, which is not supported
        assertThat(query("SELECT histogram(regionkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);

        // multimap_agg returns multimap, which is not supported
        assertThat(query("SELECT multimap_agg(regionkey, nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);

        // approx_set returns HyperLogLog, which is not supported
        assertThat(query("SELECT approx_set(nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);
    }

    @Test
    public void testLimitPushdown()
    {
        assertThat(query("SELECT name FROM nation LIMIT 30")).isFullyPushedDown(); // Use high limit for result determinism

        // with filter over numeric column
        assertThat(query("SELECT name FROM nation WHERE regionkey = 3 LIMIT 5")).isFullyPushedDown();

        // with filter over varchar column
        assertThat(query("SELECT name FROM nation WHERE name < 'EEE' LIMIT 5")).isFullyPushedDown();

        // with aggregation
        assertThat(query("SELECT max(regionkey) FROM nation LIMIT 5")).isFullyPushedDown(); // global aggregation, LIMIT removed
        assertThat(query("SELECT regionkey, max(name) FROM nation GROUP BY regionkey LIMIT 5")).isFullyPushedDown();
        assertThat(query("SELECT DISTINCT regionkey FROM nation LIMIT 5")).isFullyPushedDown();

        // with filter and aggregation
        assertThat(query("SELECT regionkey, count(*) FROM nation WHERE nationkey < 5 GROUP BY regionkey LIMIT 3")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, count(*) FROM nation WHERE name < 'EGYPT' GROUP BY regionkey LIMIT 3")).isFullyPushedDown();
    }

    @Test
    public void testCreateView()
    {
        // The test ensures that we do not inherit CREATE VIEW capabilities from presto-base-jdbc.
        // While generic VIEW support in base JDBC is feasible, Presto Connector would require special
        // considerations, see https://starburstdata.atlassian.net/browse/PRESTO-4795.
        assertThatThrownBy(() -> query("CREATE VIEW v AS SELECT 1 a"))
                .hasMessage("This connector does not support creating views");
    }
}
