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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.cost.CostProvider;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.StatsProvider;
import io.trino.cost.SymbolStatsEstimate;
import io.trino.cost.TaskCountEstimator;
import io.trino.execution.warnings.WarningCollector;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ValuesNode;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.plan.AggregationNode.singleAggregation;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDistinctAggregationController
{
    private static final int NODE_COUNT = 6;
    private static final TaskCountEstimator TASK_COUNT_ESTIMATOR = new TaskCountEstimator(() -> NODE_COUNT);

    @Test
    public void testSingleStepPreferredForHighCardinalitySingleGroupByKey()
    {
        DistinctAggregationController controller = new DistinctAggregationController(TASK_COUNT_ESTIMATOR);
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        Symbol groupingKey = symbolAllocator.newSymbol("groupingKey", BIGINT);

        AggregationNode aggregationNode = singleAggregation(
                new PlanNodeId("aggregation"),
                new ValuesNode(new PlanNodeId("source"), 1_000_000),
                ImmutableMap.of(),
                singleGroupingSet(ImmutableList.of(groupingKey)));
        Rule.Context context = context(
                ImmutableMap.of(aggregationNode, new PlanNodeStatsEstimate(1_000_000, ImmutableMap.of())),
                symbolAllocator);

        assertFalse(controller.shouldAddMarkDistinct(aggregationNode, context));
    }

    @Test
    public void testMarkDistinctPreferredForHighCardinalityMultipleGroupByKeys()
    {
        DistinctAggregationController controller = new DistinctAggregationController(TASK_COUNT_ESTIMATOR);
        SymbolAllocator symbolAllocator = new SymbolAllocator();

        Symbol lowCardinalityGroupingKey = symbolAllocator.newSymbol("lowCardinalityGroupingKey", BIGINT);
        Symbol highCardinalityGroupingKey = symbolAllocator.newSymbol("highCardinalityGroupingKey", BIGINT);

        AggregationNode aggregationNode = singleAggregation(
                new PlanNodeId("aggregation"),
                new ValuesNode(new PlanNodeId("source"), 1_000_000),
                ImmutableMap.of(),
                singleGroupingSet(ImmutableList.of(lowCardinalityGroupingKey, highCardinalityGroupingKey)));
        Rule.Context context = context(
                ImmutableMap.of(aggregationNode, new PlanNodeStatsEstimate(1_000_000, ImmutableMap.of(
                        lowCardinalityGroupingKey, SymbolStatsEstimate.builder().setDistinctValuesCount(10).build(),
                        highCardinalityGroupingKey, SymbolStatsEstimate.builder().setDistinctValuesCount(1_000_000).build()))),
                symbolAllocator);

        assertTrue(controller.shouldAddMarkDistinct(aggregationNode, context));
    }

    @Test
    public void testMarkDistinctPreferredForLowCardinality2GroupByKeys()
    {
        DistinctAggregationController controller = new DistinctAggregationController(TASK_COUNT_ESTIMATOR);
        SymbolAllocator symbolAllocator = new SymbolAllocator();

        List<Symbol> groupingKeys = ImmutableList.of(
                symbolAllocator.newSymbol("key1", BIGINT),
                symbolAllocator.newSymbol("key2", BIGINT));
        AggregationNode aggregationNode = singleAggregation(
                new PlanNodeId("aggregation"),
                new ValuesNode(new PlanNodeId("source"), 1_000_000),
                ImmutableMap.of(),
                singleGroupingSet(groupingKeys));
        Rule.Context context = context(
                ImmutableMap.of(aggregationNode, new PlanNodeStatsEstimate(
                        1_000_000,
                        groupingKeys.stream().collect(toImmutableMap(
                                Function.identity(),
                                key -> SymbolStatsEstimate.builder().setDistinctValuesCount(10).build())))),
                new SymbolAllocator());
        assertTrue(controller.shouldAddMarkDistinct(aggregationNode, context));
    }

    @Test
    public void testMarkDistinctPreferredForLowCardinality3GroupByKeys()
    {
        DistinctAggregationController controller = new DistinctAggregationController(TASK_COUNT_ESTIMATOR);
        SymbolAllocator symbolAllocator = new SymbolAllocator();

        List<Symbol> groupingKeys = ImmutableList.of(
                symbolAllocator.newSymbol("key1", BIGINT),
                symbolAllocator.newSymbol("key2", BIGINT),
                symbolAllocator.newSymbol("key3", BIGINT));
        AggregationNode aggregationNode = singleAggregation(
                new PlanNodeId("aggregation"),
                new ValuesNode(new PlanNodeId("source"), 1_000_000),
                ImmutableMap.of(),
                singleGroupingSet(groupingKeys));
        Rule.Context context = context(
                ImmutableMap.of(aggregationNode, new PlanNodeStatsEstimate(
                        1_000_000,
                        groupingKeys.stream().collect(toImmutableMap(
                                Function.identity(),
                                key -> SymbolStatsEstimate.builder().setDistinctValuesCount(10).build())))),
                new SymbolAllocator());
        assertTrue(controller.shouldAddMarkDistinct(aggregationNode, context));
    }

    private static Rule.Context context(Map<PlanNode, PlanNodeStatsEstimate> stats, final SymbolAllocator symbolAllocator)
    {
        PlanNodeIdAllocator planNodeIdAllocator = new PlanNodeIdAllocator();
        return new Rule.Context()
        {
            @Override
            public Lookup getLookup()
            {
                return Lookup.noLookup();
            }

            @Override
            public PlanNodeIdAllocator getIdAllocator()
            {
                return planNodeIdAllocator;
            }

            @Override
            public SymbolAllocator getSymbolAllocator()
            {
                return symbolAllocator;
            }

            @Override
            public Session getSession()
            {
                return TEST_SESSION;
            }

            @Override
            public StatsProvider getStatsProvider()
            {
                return stats::get;
            }

            @Override
            public CostProvider getCostProvider()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void checkTimeoutNotExhausted()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public WarningCollector getWarningCollector()
            {
                throw new UnsupportedOperationException();
            }
        };
    }
}
