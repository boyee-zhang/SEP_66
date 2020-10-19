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
package io.prestosql.plugin.kudu;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.execution.Lifespan;
import io.prestosql.execution.QueryStats;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.Split;
import io.prestosql.metadata.TableHandle;
import io.prestosql.operator.OperatorStats;
import io.prestosql.security.AllowAllAccessControl;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.DynamicFilter;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.split.SplitSource;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.ResultWithQueryId;
import io.prestosql.tpch.TpchTable;
import io.prestosql.transaction.TransactionId;
import io.prestosql.transaction.TransactionManager;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.prestosql.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.prestosql.plugin.kudu.KuduQueryRunnerFactory.createKuduQueryRunnerTpch;
import static io.prestosql.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING;
import static io.prestosql.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.prestosql.sql.analyzer.FeaturesConfig.JoinDistributionType.BROADCAST;
import static io.prestosql.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.NONE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestKuduIntegrationDynamicFilter
        extends AbstractTestQueryFramework
{
    private TestingKuduServer kuduServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        kuduServer = new TestingKuduServer();
        return createKuduQueryRunnerTpch(
                kuduServer,
                Optional.of(""),
                ImmutableMap.of("dynamic_filtering_wait_timeout", "1h"),
                ImmutableMap.of(
                        "dynamic-filtering.small-broadcast.max-distinct-values-per-driver", "100",
                        "dynamic-filtering.small-broadcast.range-row-limit-per-driver", "100"),
                TpchTable.getTables());
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        kuduServer.close();
    }

    @Test(timeOut = 30_000)
    public void testIncompleteDynamicFilterTimeout()
            throws Exception
    {
        QueryRunner runner = getQueryRunner();
        TransactionManager transactionManager = runner.getTransactionManager();
        TransactionId transactionId = transactionManager.beginTransaction(false);
        Session session = Session.builder(runner.getDefaultSession())
                .setCatalogSessionProperty("kudu", "dynamic_filtering_wait_timeout", "1s")
                .build()
                .beginTransactionId(transactionId, transactionManager, new AllowAllAccessControl());
        QualifiedObjectName tableName = new QualifiedObjectName("kudu", "tpch", "orders");
        Optional<TableHandle> tableHandle = runner.getMetadata().getTableHandle(session, tableName);
        assertTrue(tableHandle.isPresent());
        SplitSource splitSource = runner.getSplitManager()
                .getSplits(session, tableHandle.get(), UNGROUPED_SCHEDULING, new IncompleteDynamicFilter());
        List<Split> splits = new ArrayList<>();
        while (!splitSource.isFinished()) {
            splits.addAll(splitSource.getNextBatch(NOT_PARTITIONED, Lifespan.taskWide(), 1000).get().getSplits());
        }
        splitSource.close();
        assertFalse(splits.isEmpty());
    }

    private static class IncompleteDynamicFilter
            implements DynamicFilter
    {
        @Override
        public CompletableFuture<?> isBlocked()
        {
            return CompletableFuture.runAsync(() -> {
                try {
                    TimeUnit.HOURS.sleep(1);
                }
                catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            });
        }

        @Override
        public boolean isComplete()
        {
            return false;
        }

        @Override
        public boolean isAwaitable()
        {
            return true;
        }

        @Override
        public TupleDomain<ColumnHandle> getCurrentPredicate()
        {
            return TupleDomain.all();
        }
    }

    @Test
    public void testJoinDynamicFilteringSingleValue()
    {
        // Join lineitem with a single row of orders
        assertDynamicFiltering(
                "SELECT * FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.comment = 'nstructions sleep furiously among '",
                withBroadcastJoin(),
                6,
                6, 1);
    }

    @Test
    public void testJoinDynamicFilteringBlockProbeSide()
    {
        // Wait for both build sides to finish before starting the scan of 'lineitem' table (should be very selective given the dynamic filters).
        assertDynamicFiltering(
                "SELECT l.comment" +
                        " FROM  lineitem l, part p, orders o" +
                        " WHERE l.orderkey = o.orderkey AND o.comment = 'nstructions sleep furiously among '" +
                        " AND p.partkey = l.partkey AND p.comment = 'onic deposits'",
                withBroadcastJoinNonReordering(),
                1,
                1, 1, 1);
    }

    private void assertDynamicFiltering(@Language("SQL") String selectQuery, Session session, int expectedRowCount, int... expectedOperatorRowsRead)
    {
        DistributedQueryRunner runner = getDistributedQueryRunner();
        ResultWithQueryId<MaterializedResult> result = runner.executeWithQueryId(session, selectQuery);

        assertEquals(result.getResult().getRowCount(), expectedRowCount);
        assertEquals(getOperatorRowsRead(runner, result.getQueryId()).toArray(), expectedOperatorRowsRead);
    }

    private Session withBroadcastJoin()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                .build();
    }

    private Session withBroadcastJoinNonReordering()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, NONE.name())
                .build();
    }

    private static List<Integer> getOperatorRowsRead(DistributedQueryRunner runner, QueryId queryId)
    {
        QueryStats stats = runner.getCoordinator().getQueryManager().getFullQueryInfo(queryId).getQueryStats();
        return stats.getOperatorSummaries()
                .stream()
                .filter(summary -> summary.getOperatorType().equals("ScanFilterAndProjectOperator"))
                .map(OperatorStats::getInputPositions)
                .map(Math::toIntExact)
                .collect(toImmutableList());
    }
}
