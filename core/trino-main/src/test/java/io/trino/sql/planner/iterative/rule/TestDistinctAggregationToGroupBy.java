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
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.SymbolStatsEstimate;
import io.trino.cost.TaskCountEstimator;
import io.trino.spi.type.RowType;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.PlanNodeId;
import org.junit.jupiter.api.Test;

import static io.trino.SystemSessionProperties.DISTINCT_AGGREGATIONS_STRATEGY;
import static io.trino.SystemSessionProperties.TASK_CONCURRENCY;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.trino.sql.planner.assertions.PlanMatchPattern.globalAggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.groupId;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.symbol;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestDistinctAggregationToGroupBy
        extends BaseRuleTest
{
    private static final int NODES_COUNT = 4;
    private static final DistinctAggregationController DISTINCT_AGGREGATION_CONTROLLER = new DistinctAggregationController(
            new TaskCountEstimator(() -> NODES_COUNT),
            createTestMetadataManager());

    @Test
    public void testGlobalWithNonDistinct()
    {
        // 0 distinct aggregations
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "pre_aggregate")
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("non-distinct"), PlanBuilder.expression("sum(a)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("a")))))
                .doesNotFire();

        // 1 distinct aggregation
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "pre_aggregate")
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("non-distinct"), PlanBuilder.expression("sum(a)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct"), PlanBuilder.expression("sum(distinct b)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("a"), p.symbol("b")))))
                .matches(aggregation(
                        globalAggregation(),
                        ImmutableMap.of(
                                "non-distinct-final", functionCall("any_value", false, ImmutableList.of(symbol("non-distinct")), "gid-filter-0"),
                                "distinct-final", functionCall("sum", false, ImmutableList.of(symbol("b")), "gid-filter-1")),
                        project(
                                ImmutableMap.of(
                                        "gid-filter-0", expression("group_id = CAST (0 as BIGINT)"),
                                        "gid-filter-1", expression("group_id = CAST (1 as BIGINT)")),
                                aggregation(
                                        singleGroupingSet("b", "group_id"),
                                        ImmutableMap.of("non-distinct", functionCall("sum", ImmutableList.of("a"))),
                                        groupId(ImmutableList.of(
                                                        ImmutableList.of("a"),
                                                        ImmutableList.of("b")),
                                                "group_id",
                                                values("a", "b"))))));

        // 2 distinct aggregations
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "pre_aggregate")
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("non-distinct"), PlanBuilder.expression("sum(a)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct1"), PlanBuilder.expression("sum(distinct b)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct2"), PlanBuilder.expression("count(distinct c)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("a"), p.symbol("b"), p.symbol("c")))))
                .matches(aggregation(
                        globalAggregation(),
                        ImmutableMap.of(
                                "non-distinct-final", functionCall("any_value", false, ImmutableList.of(symbol("non-distinct")), "gid-filter-0"),
                                "distinct1-final", functionCall("sum", false, ImmutableList.of(symbol("b")), "gid-filter-1"),
                                "distinct2-final", functionCall("count", false, ImmutableList.of(symbol("c")), "gid-filter-2")),
                        project(
                                ImmutableMap.of(
                                        "gid-filter-0", expression("group_id = CAST (0 as BIGINT)"),
                                        "gid-filter-1", expression("group_id = CAST (1 as BIGINT)"),
                                        "gid-filter-2", expression("group_id = CAST (2 as BIGINT)")),
                                aggregation(
                                        singleGroupingSet("b", "c", "group_id"),
                                        ImmutableMap.of("non-distinct", functionCall("sum", ImmutableList.of("a"))),
                                        groupId(ImmutableList.of(
                                                        ImmutableList.of("a"),
                                                        ImmutableList.of("b"),
                                                        ImmutableList.of("c")),
                                                "group_id",
                                                values("a", "b", "c"))))));

        // 3 distinct aggregations, 2 on the same input
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "pre_aggregate")
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("non-distinct"), PlanBuilder.expression("sum(a)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct1"), PlanBuilder.expression("sum(distinct b)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct2"), PlanBuilder.expression("count(distinct c)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct3"), PlanBuilder.expression("sum(distinct c)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("a"), p.symbol("b"), p.symbol("c")))))
                .matches(aggregation(
                        globalAggregation(),
                        ImmutableMap.of(
                                "non-distinct-final", functionCall("any_value", false, ImmutableList.of(symbol("non-distinct")), "gid-filter-0"),
                                "distinct1-final", functionCall("sum", false, ImmutableList.of(symbol("b")), "gid-filter-1"),
                                "distinct2-final", functionCall("count", false, ImmutableList.of(symbol("c")), "gid-filter-2"),
                                "distinct3-final", functionCall("sum", false, ImmutableList.of(symbol("c")), "gid-filter-2")),
                        project(
                                ImmutableMap.of(
                                        "gid-filter-0", expression("group_id = CAST (0 as BIGINT)"),
                                        "gid-filter-1", expression("group_id = CAST (1 as BIGINT)"),
                                        "gid-filter-2", expression("group_id = CAST (2 as BIGINT)")),
                                aggregation(
                                        singleGroupingSet("b", "c", "group_id"),
                                        ImmutableMap.of("non-distinct", functionCall("sum", ImmutableList.of("a"))),
                                        groupId(ImmutableList.of(
                                                        ImmutableList.of("a"),
                                                        ImmutableList.of("b"),
                                                        ImmutableList.of("c")),
                                                "group_id",
                                                values("a", "b", "c"))))));

        // 2 distinct aggregations, 2 non-distinct
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "pre_aggregate")
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("non-distinct1"), PlanBuilder.expression("sum(a)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("non-distinct2"), PlanBuilder.expression("avg(d)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct1"), PlanBuilder.expression("sum(distinct b)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct2"), PlanBuilder.expression("count(distinct c)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("a"), p.symbol("b"), p.symbol("c"), p.symbol("d")))))
                .matches(aggregation(
                        globalAggregation(),
                        ImmutableMap.of(
                                "non-distinct1-final", functionCall("any_value", false, ImmutableList.of(symbol("non-distinct1")), "gid-filter-0"),
                                "non-distinct2-final", functionCall("any_value", false, ImmutableList.of(symbol("non-distinct2")), "gid-filter-0"),
                                "distinct1-final", functionCall("sum", false, ImmutableList.of(symbol("b")), "gid-filter-1"),
                                "distinct2-final", functionCall("count", false, ImmutableList.of(symbol("c")), "gid-filter-2")),
                        project(
                                ImmutableMap.of(
                                        "gid-filter-0", expression("group_id = CAST (0 as BIGINT)"),
                                        "gid-filter-1", expression("group_id = CAST (1 as BIGINT)"),
                                        "gid-filter-2", expression("group_id = CAST (2 as BIGINT)")),
                                aggregation(
                                        singleGroupingSet("b", "c", "group_id"),
                                        ImmutableMap.of(
                                                "non-distinct1", functionCall("sum", ImmutableList.of("a")),
                                                "non-distinct2", functionCall("avg", ImmutableList.of("d"))),
                                        groupId(ImmutableList.of(
                                                        ImmutableList.of("a", "d"),
                                                        ImmutableList.of("b"),
                                                        ImmutableList.of("c")),
                                                "group_id",
                                                values("a", "b", "c", "d"))))));

        // 2 distinct aggregations, 2 non-distinct on the same input
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "pre_aggregate")
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("non-distinct1"), PlanBuilder.expression("sum(a)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("non-distinct2"), PlanBuilder.expression("avg(a)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct1"), PlanBuilder.expression("sum(distinct b)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct2"), PlanBuilder.expression("count(distinct c)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("a"), p.symbol("b"), p.symbol("c")))))
                .matches(aggregation(
                        globalAggregation(),
                        ImmutableMap.of(
                                "non-distinct1-final", functionCall("any_value", false, ImmutableList.of(symbol("non-distinct1")), "gid-filter-0"),
                                "non-distinct2-final", functionCall("any_value", false, ImmutableList.of(symbol("non-distinct2")), "gid-filter-0"),
                                "distinct1-final", functionCall("sum", false, ImmutableList.of(symbol("b")), "gid-filter-1"),
                                "distinct2-final", functionCall("count", false, ImmutableList.of(symbol("c")), "gid-filter-2")),
                        project(
                                ImmutableMap.of(
                                        "gid-filter-0", expression("group_id = CAST (0 as BIGINT)"),
                                        "gid-filter-1", expression("group_id = CAST (1 as BIGINT)"),
                                        "gid-filter-2", expression("group_id = CAST (2 as BIGINT)")),
                                aggregation(
                                        singleGroupingSet("b", "c", "group_id"),
                                        ImmutableMap.of(
                                                "non-distinct1", functionCall("sum", ImmutableList.of("a")),
                                                "non-distinct2", functionCall("avg", ImmutableList.of("a"))),
                                        groupId(ImmutableList.of(
                                                        ImmutableList.of("a"),
                                                        ImmutableList.of("b"),
                                                        ImmutableList.of("c")),
                                                "group_id",
                                                values("a", "b", "c"))))));
    }

    @Test
    public void testDistinctAggregationsAndNonDistinctAggregationsOnTheSameInput()
    {
        //  distinct aggregations and non-distinct aggregations on the same input
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "pre_aggregate")
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("non-distinct1"), PlanBuilder.expression("sum(a)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("non-distinct2"), PlanBuilder.expression("avg(a)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct1"), PlanBuilder.expression("sum(distinct a)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct2"), PlanBuilder.expression("count(distinct a)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("a")))))
                .matches(aggregation(
                        globalAggregation(),
                        ImmutableMap.of(
                                "non-distinct1-final", functionCall("any_value", false, ImmutableList.of(symbol("non-distinct1")), "gid-filter-0"),
                                "non-distinct2-final", functionCall("any_value", false, ImmutableList.of(symbol("non-distinct2")), "gid-filter-0"),
                                "distinct1-final", functionCall("sum", false, ImmutableList.of(symbol("a")), "gid-filter-1"),
                                "distinct2-final", functionCall("count", false, ImmutableList.of(symbol("a")), "gid-filter-1")),
                        project(
                                ImmutableMap.of(
                                        "gid-filter-0", expression("group_id = CAST (0 as BIGINT)"),
                                        "gid-filter-1", expression("group_id = CAST (1 as BIGINT)")),
                                aggregation(
                                        singleGroupingSet("a", "group_id"),
                                        ImmutableMap.of(
                                                "non-distinct1", functionCall("sum", ImmutableList.of("a-non-distinct")),
                                                "non-distinct2", functionCall("avg", ImmutableList.of("a-non-distinct"))),
                                        groupId(
                                                ImmutableList.of(
                                                        ImmutableList.of("a-non-distinct"),
                                                        ImmutableList.of("a")),
                                                ImmutableMap.of("a-non-distinct", "a"),
                                                ImmutableList.of(),
                                                "group_id",
                                                values("a"))))));
    }

    @Test
    public void testNonDistinctWith0OnEmptyInput()
    {
        // global
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "pre_aggregate")
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("non-distinct"), PlanBuilder.expression("count(a)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct"), PlanBuilder.expression("sum(distinct b)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("a"), p.symbol("b")))))
                .matches(project(
                        ImmutableMap.of("non-distinct-final", expression("COALESCE(\"non-distinct-expression\", CAST(0 AS bigint))")),
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.of(
                                        "non-distinct-expression", functionCall("any_value", false, ImmutableList.of(symbol("non-distinct")), "gid-filter-0"),
                                        "distinct-final", functionCall("sum", false, ImmutableList.of(symbol("b")), "gid-filter-1")),
                                project(
                                        ImmutableMap.of(
                                                "gid-filter-0", expression("group_id = CAST (0 as BIGINT)"),
                                                "gid-filter-1", expression("group_id = CAST (1 as BIGINT)")),
                                        aggregation(
                                                singleGroupingSet("b", "group_id"),
                                                ImmutableMap.of("non-distinct", functionCall("count", ImmutableList.of("a"))),
                                                groupId(ImmutableList.of(
                                                                ImmutableList.of("a"),
                                                                ImmutableList.of("b")),
                                                        "group_id",
                                                        values("a", "b")))))));

        // group by
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "pre_aggregate")
                .on(p -> p.aggregation(builder -> builder
                        .singleGroupingSet(p.symbol("groupingKey"))
                        .addAggregation(p.symbol("non-distinct"), PlanBuilder.expression("count(a)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct"), PlanBuilder.expression("sum(distinct b)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("a"), p.symbol("b"), p.symbol("groupingKey")))))
                .matches(project(
                        ImmutableMap.of("non-distinct-final", expression("COALESCE(\"non-distinct-expression\", CAST(0 AS bigint))")),
                        aggregation(
                                singleGroupingSet("groupingKey"),
                                ImmutableMap.of(
                                        "non-distinct-expression", functionCall("any_value", false, ImmutableList.of(symbol("non-distinct")), "gid-filter-0"),
                                        "distinct-final", functionCall("sum", false, ImmutableList.of(symbol("b")), "gid-filter-1")),
                                project(
                                        ImmutableMap.of(
                                                "gid-filter-0", expression("group_id = CAST (0 as BIGINT)"),
                                                "gid-filter-1", expression("group_id = CAST (1 as BIGINT)")),
                                        aggregation(
                                                singleGroupingSet("groupingKey", "b", "group_id"),
                                                ImmutableMap.of("non-distinct", functionCall("count", ImmutableList.of("a"))),
                                                groupId(ImmutableList.of(
                                                                ImmutableList.of("a", "groupingKey"),
                                                                ImmutableList.of("b", "groupingKey")),
                                                        "group_id",
                                                        values("a", "b", "groupingKey")))))));
    }

    @Test
    public void testGlobalWithoutNonDistinct()
    {
        // 1 distinct aggregation
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "pre_aggregate")
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("distinct"), PlanBuilder.expression("sum(distinct b)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("b")))))
                .doesNotFire();

        // 2 distinct aggregations
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "pre_aggregate")
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("distinct1"), PlanBuilder.expression("sum(distinct b)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct2"), PlanBuilder.expression("count(distinct c)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("b"), p.symbol("c")))))
                .matches(aggregation(
                        globalAggregation(),
                        ImmutableMap.of(
                                "distinct1-final", functionCall("sum", false, ImmutableList.of(symbol("b")), "gid-filter-0"),
                                "distinct2-final", functionCall("count", false, ImmutableList.of(symbol("c")), "gid-filter-1")),
                        project(
                                ImmutableMap.of(
                                        "gid-filter-0", expression("group_id = CAST (0 as BIGINT)"),
                                        "gid-filter-1", expression("group_id = CAST (1 as BIGINT)")),
                                aggregation(
                                        singleGroupingSet("b", "c", "group_id"),
                                        ImmutableMap.of(),
                                        groupId(ImmutableList.of(
                                                        ImmutableList.of("b"),
                                                        ImmutableList.of("c")),
                                                "group_id",
                                                values("b", "c"))))));

        // 3 distinct aggregations, 2 on the same input
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "pre_aggregate")
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("distinct1"), PlanBuilder.expression("sum(distinct b)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct2"), PlanBuilder.expression("count(distinct c)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct3"), PlanBuilder.expression("sum(distinct c)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("b"), p.symbol("c")))))
                .matches(aggregation(
                        globalAggregation(),
                        ImmutableMap.of(
                                "distinct1-final", functionCall("sum", false, ImmutableList.of(symbol("b")), "gid-filter-0"),
                                "distinct2-final", functionCall("count", false, ImmutableList.of(symbol("c")), "gid-filter-1"),
                                "distinct3-final", functionCall("sum", false, ImmutableList.of(symbol("c")), "gid-filter-1")),
                        project(
                                ImmutableMap.of(
                                        "gid-filter-0", expression("group_id = CAST (0 as BIGINT)"),
                                        "gid-filter-1", expression("group_id = CAST (1 as BIGINT)")),
                                aggregation(
                                        singleGroupingSet("b", "c", "group_id"),
                                        ImmutableMap.of(),
                                        groupId(ImmutableList.of(
                                                        ImmutableList.of("b"),
                                                        ImmutableList.of("c")),
                                                "group_id",
                                                values("b", "c"))))));
    }

    @Test
    public void testDistinctOnNestedType()
    {
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "pre_aggregate")
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("non-distinct"), PlanBuilder.expression("sum(a)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct"), PlanBuilder.expression("count(distinct nested)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("a"), p.symbol("nested", RowType.anonymousRow(BIGINT, BIGINT))))))
                .matches(aggregation(
                        globalAggregation(),
                        ImmutableMap.of(
                                "non-distinct-final", functionCall("any_value", false, ImmutableList.of(symbol("non-distinct")), "gid-filter-0"),
                                "distinct-final", functionCall("count", false, ImmutableList.of(symbol("nested")), "gid-filter-1")),
                        project(
                                ImmutableMap.of(
                                        "gid-filter-0", expression("group_id = CAST (0 as BIGINT)"),
                                        "gid-filter-1", expression("group_id = CAST (1 as BIGINT)")),
                                aggregation(
                                        singleGroupingSet("nested", "group_id"),
                                        ImmutableMap.of("non-distinct", functionCall("sum", ImmutableList.of("a"))),
                                        groupId(ImmutableList.of(
                                                        ImmutableList.of("a"),
                                                        ImmutableList.of("nested")),
                                                "group_id",
                                                values("a", "nested"))))));
    }

    @Test
    public void testNonDistinctWithoutArgument()
    {
        // only count(*) + distinct
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "pre_aggregate")
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("non-distinct"), PlanBuilder.expression("count(*)"), ImmutableList.of())
                        .addAggregation(p.symbol("distinct"), PlanBuilder.expression("count(distinct b)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("b")))))
                .matches(project(
                        ImmutableMap.of("non-distinct-final", expression("COALESCE(\"non-distinct-expression\", CAST(0 AS bigint))")),
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.of(
                                        "non-distinct-expression", functionCall("any_value", false, ImmutableList.of(symbol("non-distinct")), "gid-filter-0"),
                                        "distinct-final", functionCall("count", false, ImmutableList.of(symbol("b")), "gid-filter-1")),
                                project(
                                        ImmutableMap.of(
                                                "gid-filter-0", expression("group_id = CAST (0 as BIGINT)"),
                                                "gid-filter-1", expression("group_id = CAST (1 as BIGINT)")),
                                        aggregation(
                                                singleGroupingSet("b", "group_id"),
                                                ImmutableMap.of("non-distinct", functionCall("count", ImmutableList.of())),
                                                groupId(ImmutableList.of(
                                                                ImmutableList.of(),
                                                                ImmutableList.of("b")),
                                                        "group_id",
                                                        values("b")))))));
        //  count(*) + other non-distinct + distinct
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "pre_aggregate")
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("count"), PlanBuilder.expression("count(*)"), ImmutableList.of())
                        .addAggregation(p.symbol("non-distinct"), PlanBuilder.expression("count(a)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct"), PlanBuilder.expression("count(distinct b)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("a"), p.symbol("b")))))
                .matches(project(
                        ImmutableMap.of(
                                "count-final", expression("COALESCE(\"count-expression\", CAST(0 AS bigint))"),
                                "non-distinct-final", expression("COALESCE(\"non-distinct-expression\", CAST(0 AS bigint))")),
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.of(
                                        "count-expression", functionCall("any_value", false, ImmutableList.of(symbol("count")), "gid-filter-0"),
                                        "non-distinct-expression", functionCall("any_value", false, ImmutableList.of(symbol("non-distinct")), "gid-filter-0"),
                                        "distinct-final", functionCall("count", false, ImmutableList.of(symbol("b")), "gid-filter-1")),
                                project(
                                        ImmutableMap.of(
                                                "gid-filter-0", expression("group_id = CAST (0 as BIGINT)"),
                                                "gid-filter-1", expression("group_id = CAST (1 as BIGINT)")),
                                        aggregation(
                                                singleGroupingSet("b", "group_id"),
                                                ImmutableMap.of(
                                                        "count", functionCall("count", ImmutableList.of()),
                                                        "non-distinct", functionCall("count", ImmutableList.of("a"))),
                                                groupId(ImmutableList.of(
                                                                ImmutableList.of("a"),
                                                                ImmutableList.of("b")),
                                                        "group_id",
                                                        values("a", "b")))))));
    }

    @Test
    public void testGroupByOneColumnWithNonDistinct()
    {
        // 0 distinct aggregations
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "pre_aggregate")
                .on(p -> p.aggregation(builder -> builder
                        .singleGroupingSet(p.symbol("groupingKey"))
                        .addAggregation(p.symbol("non-distinct"), PlanBuilder.expression("sum(a)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("a"), p.symbol("groupingKey")))))
                .doesNotFire();

        // 1 distinct aggregation
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "pre_aggregate")
                .on(p -> p.aggregation(builder -> builder
                        .singleGroupingSet(p.symbol("groupingKey"))
                        .addAggregation(p.symbol("non-distinct"), PlanBuilder.expression("sum(a)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct"), PlanBuilder.expression("sum(distinct b)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("a"), p.symbol("b"), p.symbol("groupingKey")))))
                .matches(aggregation(
                        singleGroupingSet("groupingKey"),
                        ImmutableMap.of(
                                "non-distinct-final", functionCall("any_value", false, ImmutableList.of(symbol("non-distinct")), "gid-filter-0"),
                                "distinct-final", functionCall("sum", false, ImmutableList.of(symbol("b")), "gid-filter-1")),
                        project(
                                ImmutableMap.of(
                                        "gid-filter-0", expression("group_id = CAST (0 as BIGINT)"),
                                        "gid-filter-1", expression("group_id = CAST (1 as BIGINT)")),
                                aggregation(
                                        singleGroupingSet("groupingKey", "b", "group_id"),
                                        ImmutableMap.of("non-distinct", functionCall("sum", ImmutableList.of("a"))),
                                        groupId(ImmutableList.of(
                                                        ImmutableList.of("a", "groupingKey"),
                                                        ImmutableList.of("b", "groupingKey")),
                                                "group_id",
                                                values("a", "b", "groupingKey"))))));

        // 2 distinct aggregations
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "pre_aggregate")
                .on(p -> p.aggregation(builder -> builder
                        .singleGroupingSet(p.symbol("groupingKey"))
                        .addAggregation(p.symbol("non-distinct"), PlanBuilder.expression("sum(a)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct1"), PlanBuilder.expression("sum(distinct b)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct2"), PlanBuilder.expression("count(distinct c)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("a"), p.symbol("b"), p.symbol("c"), p.symbol("groupingKey")))))
                .matches(aggregation(
                        singleGroupingSet("groupingKey"),
                        ImmutableMap.of(
                                "non-distinct-final", functionCall("any_value", false, ImmutableList.of(symbol("non-distinct")), "gid-filter-0"),
                                "distinct1-final", functionCall("sum", false, ImmutableList.of(symbol("b")), "gid-filter-1"),
                                "distinct2-final", functionCall("count", false, ImmutableList.of(symbol("c")), "gid-filter-2")),
                        project(
                                ImmutableMap.of(
                                        "gid-filter-0", expression("group_id = CAST (0 as BIGINT)"),
                                        "gid-filter-1", expression("group_id = CAST (1 as BIGINT)"),
                                        "gid-filter-2", expression("group_id = CAST (2 as BIGINT)")),
                                aggregation(
                                        singleGroupingSet("groupingKey", "b", "c", "group_id"),
                                        ImmutableMap.of("non-distinct", functionCall("sum", ImmutableList.of("a"))),
                                        groupId(ImmutableList.of(
                                                        ImmutableList.of("a", "groupingKey"),
                                                        ImmutableList.of("b", "groupingKey"),
                                                        ImmutableList.of("c", "groupingKey")),
                                                "group_id",
                                                values("a", "b", "c", "groupingKey"))))));

        // 3 distinct aggregations, 2 on the same input
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "pre_aggregate")
                .on(p -> p.aggregation(builder -> builder
                        .singleGroupingSet(p.symbol("groupingKey"))
                        .addAggregation(p.symbol("non-distinct"), PlanBuilder.expression("sum(a)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct1"), PlanBuilder.expression("sum(distinct b)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct2"), PlanBuilder.expression("count(distinct c)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct3"), PlanBuilder.expression("sum(distinct c)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("a"), p.symbol("b"), p.symbol("c"), p.symbol("groupingKey")))))
                .matches(aggregation(
                        singleGroupingSet("groupingKey"),
                        ImmutableMap.of(
                                "non-distinct-final", functionCall("any_value", false, ImmutableList.of(symbol("non-distinct")), "gid-filter-0"),
                                "distinct1-final", functionCall("sum", false, ImmutableList.of(symbol("b")), "gid-filter-1"),
                                "distinct2-final", functionCall("count", false, ImmutableList.of(symbol("c")), "gid-filter-2"),
                                "distinct3-final", functionCall("sum", false, ImmutableList.of(symbol("c")), "gid-filter-2")),
                        project(
                                ImmutableMap.of(
                                        "gid-filter-0", expression("group_id = CAST (0 as BIGINT)"),
                                        "gid-filter-1", expression("group_id = CAST (1 as BIGINT)"),
                                        "gid-filter-2", expression("group_id = CAST (2 as BIGINT)")),
                                aggregation(
                                        singleGroupingSet("groupingKey", "b", "c", "group_id"),
                                        ImmutableMap.of("non-distinct", functionCall("sum", ImmutableList.of("a"))),
                                        groupId(ImmutableList.of(
                                                        ImmutableList.of("a", "groupingKey"),
                                                        ImmutableList.of("b", "groupingKey"),
                                                        ImmutableList.of("c", "groupingKey")),
                                                "group_id",
                                                values("a", "b", "c", "groupingKey"))))));

        // 2 distinct aggregations, 2 non-distinct
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "pre_aggregate")
                .on(p -> p.aggregation(builder -> builder
                        .singleGroupingSet(p.symbol("groupingKey"))
                        .addAggregation(p.symbol("non-distinct1"), PlanBuilder.expression("sum(a)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("non-distinct2"), PlanBuilder.expression("avg(d)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct1"), PlanBuilder.expression("sum(distinct b)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct2"), PlanBuilder.expression("count(distinct c)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("a"), p.symbol("b"), p.symbol("c"), p.symbol("d"), p.symbol("groupingKey")))))
                .matches(aggregation(
                        singleGroupingSet("groupingKey"),
                        ImmutableMap.of(
                                "non-distinct1-final", functionCall("any_value", false, ImmutableList.of(symbol("non-distinct1")), "gid-filter-0"),
                                "non-distinct2-final", functionCall("any_value", false, ImmutableList.of(symbol("non-distinct2")), "gid-filter-0"),
                                "distinct1-final", functionCall("sum", false, ImmutableList.of(symbol("b")), "gid-filter-1"),
                                "distinct2-final", functionCall("count", false, ImmutableList.of(symbol("c")), "gid-filter-2")),
                        project(
                                ImmutableMap.of(
                                        "gid-filter-0", expression("group_id = CAST (0 as BIGINT)"),
                                        "gid-filter-1", expression("group_id = CAST (1 as BIGINT)"),
                                        "gid-filter-2", expression("group_id = CAST (2 as BIGINT)")),
                                aggregation(
                                        singleGroupingSet("groupingKey", "b", "c", "group_id"),
                                        ImmutableMap.of(
                                                "non-distinct1", functionCall("sum", ImmutableList.of("a")),
                                                "non-distinct2", functionCall("avg", ImmutableList.of("d"))),
                                        groupId(ImmutableList.of(
                                                        ImmutableList.of("a", "d", "groupingKey"),
                                                        ImmutableList.of("b", "groupingKey"),
                                                        ImmutableList.of("c", "groupingKey")),
                                                "group_id",
                                                values("a", "b", "c", "d", "groupingKey"))))));

        // 2 distinct aggregations, 2 non-distinct on the same input
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "pre_aggregate")
                .on(p -> p.aggregation(builder -> builder
                        .singleGroupingSet(p.symbol("groupingKey"))
                        .addAggregation(p.symbol("non-distinct1"), PlanBuilder.expression("sum(a)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("non-distinct2"), PlanBuilder.expression("avg(a)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct1"), PlanBuilder.expression("sum(distinct b)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct2"), PlanBuilder.expression("count(distinct c)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("a"), p.symbol("b"), p.symbol("c"), p.symbol("groupingKey")))))
                .matches(aggregation(
                        singleGroupingSet("groupingKey"),
                        ImmutableMap.of(
                                "non-distinct1-final", functionCall("any_value", false, ImmutableList.of(symbol("non-distinct1")), "gid-filter-0"),
                                "non-distinct2-final", functionCall("any_value", false, ImmutableList.of(symbol("non-distinct2")), "gid-filter-0"),
                                "distinct1-final", functionCall("sum", false, ImmutableList.of(symbol("b")), "gid-filter-1"),
                                "distinct2-final", functionCall("count", false, ImmutableList.of(symbol("c")), "gid-filter-2")),
                        project(
                                ImmutableMap.of(
                                        "gid-filter-0", expression("group_id = CAST (0 as BIGINT)"),
                                        "gid-filter-1", expression("group_id = CAST (1 as BIGINT)"),
                                        "gid-filter-2", expression("group_id = CAST (2 as BIGINT)")),
                                aggregation(
                                        singleGroupingSet("groupingKey", "b", "c", "group_id"),
                                        ImmutableMap.of(
                                                "non-distinct1", functionCall("sum", ImmutableList.of("a")),
                                                "non-distinct2", functionCall("avg", ImmutableList.of("a"))),
                                        groupId(ImmutableList.of(
                                                        ImmutableList.of("a", "groupingKey"),
                                                        ImmutableList.of("b", "groupingKey"),
                                                        ImmutableList.of("c", "groupingKey")),
                                                "group_id",
                                                values("a", "b", "c", "groupingKey"))))));
    }

    @Test
    public void testGroupByOneColumnWithoutNonDistinct()
    {
        // 1 distinct aggregation
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "pre_aggregate")
                .on(p -> p.aggregation(builder -> builder
                        .singleGroupingSet(p.symbol("groupingKey"))
                        .addAggregation(p.symbol("distinct"), PlanBuilder.expression("sum(distinct b)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("b"), p.symbol("groupingKey")))))
                .doesNotFire();

        // 2 distinct aggregations
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "pre_aggregate")
                .on(p -> p.aggregation(builder -> builder
                        .singleGroupingSet(p.symbol("groupingKey"))
                        .addAggregation(p.symbol("distinct1"), PlanBuilder.expression("sum(distinct b)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct2"), PlanBuilder.expression("count(distinct c)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("b"), p.symbol("c"), p.symbol("groupingKey")))))
                .matches(aggregation(
                        singleGroupingSet("groupingKey"),
                        ImmutableMap.of(
                                "distinct1-final", functionCall("sum", false, ImmutableList.of(symbol("b")), "gid-filter-0"),
                                "distinct2-final", functionCall("count", false, ImmutableList.of(symbol("c")), "gid-filter-1")),
                        project(
                                ImmutableMap.of(
                                        "gid-filter-0", expression("group_id = CAST (0 as BIGINT)"),
                                        "gid-filter-1", expression("group_id = CAST (1 as BIGINT)")),
                                aggregation(
                                        singleGroupingSet("groupingKey", "b", "c", "group_id"),
                                        ImmutableMap.of(),
                                        groupId(ImmutableList.of(
                                                        ImmutableList.of("b", "groupingKey"),
                                                        ImmutableList.of("c", "groupingKey")),
                                                "group_id",
                                                values("b", "c", "groupingKey"))))));

        // 3 distinct aggregations, 2 on the same input
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "pre_aggregate")
                .on(p -> p.aggregation(builder -> builder
                        .singleGroupingSet(p.symbol("groupingKey"))
                        .addAggregation(p.symbol("distinct1"), PlanBuilder.expression("sum(distinct b)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct2"), PlanBuilder.expression("count(distinct c)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct3"), PlanBuilder.expression("sum(distinct c)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("b"), p.symbol("c"), p.symbol("groupingKey")))))
                .matches(aggregation(
                        singleGroupingSet("groupingKey"),
                        ImmutableMap.of(
                                "distinct1-final", functionCall("sum", false, ImmutableList.of(symbol("b")), "gid-filter-0"),
                                "distinct2-final", functionCall("count", false, ImmutableList.of(symbol("c")), "gid-filter-1"),
                                "distinct3-final", functionCall("sum", false, ImmutableList.of(symbol("c")), "gid-filter-1")),
                        project(
                                ImmutableMap.of(
                                        "gid-filter-0", expression("group_id = CAST (0 as BIGINT)"),
                                        "gid-filter-1", expression("group_id = CAST (1 as BIGINT)")),
                                aggregation(
                                        singleGroupingSet("groupingKey", "b", "c", "group_id"),
                                        ImmutableMap.of(),
                                        groupId(ImmutableList.of(
                                                        ImmutableList.of("b", "groupingKey"),
                                                        ImmutableList.of("c", "groupingKey")),
                                                "group_id",
                                                values("b", "c", "groupingKey"))))));
    }

    @Test
    public void testGroupByMultipleColumnWithNonDistinct()
    {
        // 0 distinct aggregations
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "pre_aggregate")
                .on(p -> p.aggregation(builder -> builder
                        .singleGroupingSet(p.symbol("groupingKey1"), p.symbol("groupingKey2"))
                        .addAggregation(p.symbol("non-distinct"), PlanBuilder.expression("sum(a)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("a"), p.symbol("groupingKey1"), p.symbol("groupingKey2")))))
                .doesNotFire();

        // 1 distinct aggregation
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "pre_aggregate")
                .on(p -> p.aggregation(builder -> builder
                        .singleGroupingSet(p.symbol("groupingKey1"), p.symbol("groupingKey2"))
                        .addAggregation(p.symbol("non-distinct"), PlanBuilder.expression("sum(a)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct"), PlanBuilder.expression("sum(distinct b)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("a"), p.symbol("b"), p.symbol("groupingKey1"), p.symbol("groupingKey2")))))
                .matches(aggregation(
                        singleGroupingSet("groupingKey1", "groupingKey2"),
                        ImmutableMap.of(
                                "non-distinct-final", functionCall("any_value", false, ImmutableList.of(symbol("non-distinct")), "gid-filter-0"),
                                "distinct-final", functionCall("sum", false, ImmutableList.of(symbol("b")), "gid-filter-1")),
                        project(
                                ImmutableMap.of(
                                        "gid-filter-0", expression("group_id = CAST (0 as BIGINT)"),
                                        "gid-filter-1", expression("group_id = CAST (1 as BIGINT)")),
                                aggregation(
                                        singleGroupingSet("groupingKey1", "groupingKey2", "b", "group_id"),
                                        ImmutableMap.of("non-distinct", functionCall("sum", ImmutableList.of("a"))),
                                        groupId(ImmutableList.of(
                                                        ImmutableList.of("a", "groupingKey1", "groupingKey2"),
                                                        ImmutableList.of("b", "groupingKey1", "groupingKey2")),
                                                "group_id",
                                                values("a", "b", "groupingKey1", "groupingKey2"))))));

        // 2 distinct aggregations
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "pre_aggregate")
                .on(p -> p.aggregation(builder -> builder
                        .singleGroupingSet(p.symbol("groupingKey1"), p.symbol("groupingKey2"))
                        .addAggregation(p.symbol("non-distinct"), PlanBuilder.expression("sum(a)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct1"), PlanBuilder.expression("sum(distinct b)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct2"), PlanBuilder.expression("count(distinct c)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("a"), p.symbol("b"), p.symbol("c"), p.symbol("groupingKey1"), p.symbol("groupingKey2")))))
                .matches(aggregation(
                        singleGroupingSet("groupingKey1", "groupingKey2"),
                        ImmutableMap.of(
                                "non-distinct-final", functionCall("any_value", false, ImmutableList.of(symbol("non-distinct")), "gid-filter-0"),
                                "distinct1-final", functionCall("sum", false, ImmutableList.of(symbol("b")), "gid-filter-1"),
                                "distinct2-final", functionCall("count", false, ImmutableList.of(symbol("c")), "gid-filter-2")),
                        project(
                                ImmutableMap.of(
                                        "gid-filter-0", expression("group_id = CAST (0 as BIGINT)"),
                                        "gid-filter-1", expression("group_id = CAST (1 as BIGINT)"),
                                        "gid-filter-2", expression("group_id = CAST (2 as BIGINT)")),
                                aggregation(
                                        singleGroupingSet("groupingKey1", "groupingKey2", "b", "c", "group_id"),
                                        ImmutableMap.of("non-distinct", functionCall("sum", ImmutableList.of("a"))),
                                        groupId(ImmutableList.of(
                                                        ImmutableList.of("a", "groupingKey1", "groupingKey2"),
                                                        ImmutableList.of("b", "groupingKey1", "groupingKey2"),
                                                        ImmutableList.of("c", "groupingKey1", "groupingKey2")),
                                                "group_id",
                                                values("a", "b", "c", "groupingKey1", "groupingKey2"))))));

        // 3 distinct aggregations, 2 on the same input
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "pre_aggregate")
                .on(p -> p.aggregation(builder -> builder
                        .singleGroupingSet(p.symbol("groupingKey1"), p.symbol("groupingKey2"))
                        .addAggregation(p.symbol("non-distinct"), PlanBuilder.expression("sum(a)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct1"), PlanBuilder.expression("sum(distinct b)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct2"), PlanBuilder.expression("count(distinct c)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct3"), PlanBuilder.expression("sum(distinct c)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("a"), p.symbol("b"), p.symbol("c"), p.symbol("groupingKey1"), p.symbol("groupingKey2")))))
                .matches(aggregation(
                        singleGroupingSet("groupingKey1", "groupingKey2"),
                        ImmutableMap.of(
                                "non-distinct-final", functionCall("any_value", false, ImmutableList.of(symbol("non-distinct")), "gid-filter-0"),
                                "distinct1-final", functionCall("sum", false, ImmutableList.of(symbol("b")), "gid-filter-1"),
                                "distinct2-final", functionCall("count", false, ImmutableList.of(symbol("c")), "gid-filter-2"),
                                "distinct3-final", functionCall("sum", false, ImmutableList.of(symbol("c")), "gid-filter-2")),
                        project(
                                ImmutableMap.of(
                                        "gid-filter-0", expression("group_id = CAST (0 as BIGINT)"),
                                        "gid-filter-1", expression("group_id = CAST (1 as BIGINT)"),
                                        "gid-filter-2", expression("group_id = CAST (2 as BIGINT)")),
                                aggregation(
                                        singleGroupingSet("groupingKey1", "groupingKey2", "b", "c", "group_id"),
                                        ImmutableMap.of("non-distinct", functionCall("sum", ImmutableList.of("a"))),
                                        groupId(ImmutableList.of(
                                                        ImmutableList.of("a", "groupingKey1", "groupingKey2"),
                                                        ImmutableList.of("b", "groupingKey1", "groupingKey2"),
                                                        ImmutableList.of("c", "groupingKey1", "groupingKey2")),
                                                "group_id",
                                                values("a", "b", "c", "groupingKey1", "groupingKey2"))))));

        // 2 distinct aggregations, 2 non-distinct
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "pre_aggregate")
                .on(p -> p.aggregation(builder -> builder
                        .singleGroupingSet(p.symbol("groupingKey1"), p.symbol("groupingKey2"))
                        .addAggregation(p.symbol("non-distinct1"), PlanBuilder.expression("sum(a)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("non-distinct2"), PlanBuilder.expression("avg(d)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct1"), PlanBuilder.expression("sum(distinct b)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct2"), PlanBuilder.expression("count(distinct c)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("a"), p.symbol("b"), p.symbol("c"), p.symbol("d"), p.symbol("groupingKey1"), p.symbol("groupingKey2")))))
                .matches(aggregation(
                        singleGroupingSet("groupingKey1", "groupingKey2"),
                        ImmutableMap.of(
                                "non-distinct1-final", functionCall("any_value", false, ImmutableList.of(symbol("non-distinct1")), "gid-filter-0"),
                                "non-distinct2-final", functionCall("any_value", false, ImmutableList.of(symbol("non-distinct2")), "gid-filter-0"),
                                "distinct1-final", functionCall("sum", false, ImmutableList.of(symbol("b")), "gid-filter-1"),
                                "distinct2-final", functionCall("count", false, ImmutableList.of(symbol("c")), "gid-filter-2")),
                        project(
                                ImmutableMap.of(
                                        "gid-filter-0", expression("group_id = CAST (0 as BIGINT)"),
                                        "gid-filter-1", expression("group_id = CAST (1 as BIGINT)"),
                                        "gid-filter-2", expression("group_id = CAST (2 as BIGINT)")),
                                aggregation(
                                        singleGroupingSet("groupingKey1", "groupingKey2", "b", "c", "group_id"),
                                        ImmutableMap.of(
                                                "non-distinct1", functionCall("sum", ImmutableList.of("a")),
                                                "non-distinct2", functionCall("avg", ImmutableList.of("d"))),
                                        groupId(ImmutableList.of(
                                                        ImmutableList.of("a", "d", "groupingKey1", "groupingKey2"),
                                                        ImmutableList.of("b", "groupingKey1", "groupingKey2"),
                                                        ImmutableList.of("c", "groupingKey1", "groupingKey2")),
                                                "group_id",
                                                values("a", "b", "c", "d", "groupingKey1", "groupingKey2"))))));

        // 2 distinct aggregations, 2 non-distinct on the same input
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "pre_aggregate")
                .on(p -> p.aggregation(builder -> builder
                        .singleGroupingSet(p.symbol("groupingKey1"), p.symbol("groupingKey2"))
                        .addAggregation(p.symbol("non-distinct1"), PlanBuilder.expression("sum(a)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("non-distinct2"), PlanBuilder.expression("avg(a)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct1"), PlanBuilder.expression("sum(distinct b)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct2"), PlanBuilder.expression("count(distinct c)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("a"), p.symbol("b"), p.symbol("c"), p.symbol("groupingKey1"), p.symbol("groupingKey2")))))
                .matches(aggregation(
                        singleGroupingSet("groupingKey1", "groupingKey2"),
                        ImmutableMap.of(
                                "non-distinct1-final", functionCall("any_value", false, ImmutableList.of(symbol("non-distinct1")), "gid-filter-0"),
                                "non-distinct2-final", functionCall("any_value", false, ImmutableList.of(symbol("non-distinct2")), "gid-filter-0"),
                                "distinct1-final", functionCall("sum", false, ImmutableList.of(symbol("b")), "gid-filter-1"),
                                "distinct2-final", functionCall("count", false, ImmutableList.of(symbol("c")), "gid-filter-2")),
                        project(
                                ImmutableMap.of(
                                        "gid-filter-0", expression("group_id = CAST (0 as BIGINT)"),
                                        "gid-filter-1", expression("group_id = CAST (1 as BIGINT)"),
                                        "gid-filter-2", expression("group_id = CAST (2 as BIGINT)")),
                                aggregation(
                                        singleGroupingSet("groupingKey1", "groupingKey2", "b", "c", "group_id"),
                                        ImmutableMap.of(
                                                "non-distinct1", functionCall("sum", ImmutableList.of("a")),
                                                "non-distinct2", functionCall("avg", ImmutableList.of("a"))),
                                        groupId(ImmutableList.of(
                                                        ImmutableList.of("a", "groupingKey1", "groupingKey2"),
                                                        ImmutableList.of("b", "groupingKey1", "groupingKey2"),
                                                        ImmutableList.of("c", "groupingKey1", "groupingKey2")),
                                                "group_id",
                                                values("a", "b", "c", "groupingKey1", "groupingKey2"))))));
    }

    @Test
    public void testAutomaticDecisionForGlobal()
    {
        // global
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "automatic")
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("non-distinct"), PlanBuilder.expression("sum(a)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct"), PlanBuilder.expression("sum(distinct b)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("a"), p.symbol("b")))))
                .matches(aggregation(
                        globalAggregation(),
                        ImmutableMap.of(
                                "non-distinct-final", functionCall("any_value", false, ImmutableList.of(symbol("non-distinct")), "gid-filter-0"),
                                "distinct-final", functionCall("sum", false, ImmutableList.of(symbol("b")), "gid-filter-1")),
                        project(
                                ImmutableMap.of(
                                        "gid-filter-0", expression("group_id = CAST (0 as BIGINT)"),
                                        "gid-filter-1", expression("group_id = CAST (1 as BIGINT)")),
                                aggregation(
                                        singleGroupingSet("b", "group_id"),
                                        ImmutableMap.of("non-distinct", functionCall("sum", ImmutableList.of("a"))),
                                        groupId(ImmutableList.of(
                                                        ImmutableList.of("a"),
                                                        ImmutableList.of("b")),
                                                "group_id",
                                                values("a", "b"))))));
    }

    @Test
    public void testAutomaticDecisionForSingleGroupByKeyWithLowCardinality()
    {
        int clusterThreadCount = NODES_COUNT * tester().getSession().getSystemProperty(TASK_CONCURRENCY, Integer.class);
        PlanNodeId aggregationSourceId = new PlanNodeId("aggregationSourceId");
        // single group-by key, low cardinality
        Symbol groupingKey = new Symbol("groupingKey");
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "automatic")
                .overrideStats(aggregationSourceId.toString(), PlanNodeStatsEstimate.builder()
                        .addSymbolStatistics(groupingKey, SymbolStatsEstimate.builder().setDistinctValuesCount(2 * clusterThreadCount).build()).build())
                .on(p -> p.aggregation(builder -> builder
                        .singleGroupingSet(groupingKey)
                        .addAggregation(p.symbol("non-distinct"), PlanBuilder.expression("sum(a)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct"), PlanBuilder.expression("sum(distinct b)"), ImmutableList.of(BIGINT))
                        .source(p.values(aggregationSourceId, p.symbol("a"), p.symbol("b"), p.symbol("groupingKey")))))
                .matches(aggregation(
                        singleGroupingSet("groupingKey"),
                        ImmutableMap.of(
                                "non-distinct-final", functionCall("any_value", false, ImmutableList.of(symbol("non-distinct")), "gid-filter-0"),
                                "distinct-final", functionCall("sum", false, ImmutableList.of(symbol("b")), "gid-filter-1")),
                        project(
                                ImmutableMap.of(
                                        "gid-filter-0", expression("group_id = CAST (0 as BIGINT)"),
                                        "gid-filter-1", expression("group_id = CAST (1 as BIGINT)")),
                                aggregation(
                                        singleGroupingSet("groupingKey", "b", "group_id"),
                                        ImmutableMap.of("non-distinct", functionCall("sum", ImmutableList.of("a"))),
                                        groupId(ImmutableList.of(
                                                        ImmutableList.of("a", "groupingKey"),
                                                        ImmutableList.of("b", "groupingKey")),
                                                "group_id",
                                                values("a", "b", "groupingKey"))))));
    }

    @Test
    public void testAutomaticDecisionWithUnknownStats()
    {
        PlanNodeId aggregationSourceId = new PlanNodeId("aggregationSourceId");
        Symbol groupingKey = new Symbol("groupingKey");
        // single group-by key, unknown stats
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "automatic")
                .overrideStats(aggregationSourceId.toString(), PlanNodeStatsEstimate.builder()
                        .addSymbolStatistics(groupingKey, SymbolStatsEstimate.builder().setDistinctValuesCount(Double.NaN).build()).build())
                .on(p -> p.aggregation(builder -> builder
                        .singleGroupingSet(groupingKey)
                        .addAggregation(p.symbol("non-distinct"), PlanBuilder.expression("sum(a)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct"), PlanBuilder.expression("sum(distinct b)"), ImmutableList.of(BIGINT))
                        .source(p.values(aggregationSourceId, p.symbol("a"), p.symbol("b"), p.symbol("groupingKey")))))
                .matches(aggregation(
                        singleGroupingSet("groupingKey"),
                        ImmutableMap.of(
                                "non-distinct-final", functionCall("any_value", false, ImmutableList.of(symbol("non-distinct")), "gid-filter-0"),
                                "distinct-final", functionCall("sum", false, ImmutableList.of(symbol("b")), "gid-filter-1")),
                        project(
                                ImmutableMap.of(
                                        "gid-filter-0", expression("group_id = CAST (0 as BIGINT)"),
                                        "gid-filter-1", expression("group_id = CAST (1 as BIGINT)")),
                                aggregation(
                                        singleGroupingSet("groupingKey", "b", "group_id"),
                                        ImmutableMap.of("non-distinct", functionCall("sum", ImmutableList.of("a"))),
                                        groupId(ImmutableList.of(
                                                        ImmutableList.of("a", "groupingKey"),
                                                        ImmutableList.of("b", "groupingKey")),
                                                "group_id",
                                                values("a", "b", "groupingKey"))))));
    }

    @Test
    public void testAutomaticDecisionForSingleGroupByKeyWithHighCardinality()
    {
        int clusterThreadCount = NODES_COUNT * tester().getSession().getSystemProperty(TASK_CONCURRENCY, Integer.class);
        PlanNodeId aggregationSourceId = new PlanNodeId("aggregationSourceId");
        Symbol groupingKey = new Symbol("groupingKey");

        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "automatic")
                .overrideStats(aggregationSourceId.toString(), PlanNodeStatsEstimate.builder()
                        .addSymbolStatistics(groupingKey, SymbolStatsEstimate.builder().setDistinctValuesCount(1000 * clusterThreadCount).build()).build())
                .on(p -> p.aggregation(builder -> builder
                        .singleGroupingSet(groupingKey)
                        .addAggregation(p.symbol("non-distinct"), PlanBuilder.expression("sum(a)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct"), PlanBuilder.expression("sum(distinct b)"), ImmutableList.of(BIGINT))
                        .source(p.values(aggregationSourceId, p.symbol("a"), p.symbol("b"), p.symbol("groupingKey")))))
                .doesNotFire();
    }

    @Test
    public void testAutomaticDecisionForTwoGroupByKeyWithLowCardinality()
    {
        int clusterThreadCount = NODES_COUNT * tester().getSession().getSystemProperty(TASK_CONCURRENCY, Integer.class);
        PlanNodeId aggregationSourceId = new PlanNodeId("aggregationSourceId");
        Symbol groupingKey1 = new Symbol("groupingKey1");
        Symbol groupingKey2 = new Symbol("groupingKey2");
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "automatic")
                .overrideStats(aggregationSourceId.toString(), PlanNodeStatsEstimate.builder()
                        .addSymbolStatistics(groupingKey1, SymbolStatsEstimate.builder().setDistinctValuesCount(2 * clusterThreadCount).build())
                        .addSymbolStatistics(groupingKey2, SymbolStatsEstimate.builder().setDistinctValuesCount(2 * clusterThreadCount).build()).build())
                .on(p -> p.aggregation(builder -> builder
                        .singleGroupingSet(groupingKey1, groupingKey2)
                        .addAggregation(p.symbol("non-distinct"), PlanBuilder.expression("sum(a)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct"), PlanBuilder.expression("sum(distinct b)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("a"), p.symbol("b"), p.symbol("groupingKey1"), p.symbol("groupingKey2")))))
                .matches(aggregation(
                        singleGroupingSet("groupingKey1", "groupingKey2"),
                        ImmutableMap.of(
                                "non-distinct-final", functionCall("any_value", false, ImmutableList.of(symbol("non-distinct")), "gid-filter-0"),
                                "distinct-final", functionCall("sum", false, ImmutableList.of(symbol("b")), "gid-filter-1")),
                        project(
                                ImmutableMap.of(
                                        "gid-filter-0", expression("group_id = CAST (0 as BIGINT)"),
                                        "gid-filter-1", expression("group_id = CAST (1 as BIGINT)")),
                                aggregation(
                                        singleGroupingSet("groupingKey1", "groupingKey2", "b", "group_id"),
                                        ImmutableMap.of("non-distinct", functionCall("sum", ImmutableList.of("a"))),
                                        groupId(ImmutableList.of(
                                                        ImmutableList.of("a", "groupingKey1", "groupingKey2"),
                                                        ImmutableList.of("b", "groupingKey1", "groupingKey2")),
                                                "group_id",
                                                values("a", "b", "groupingKey1", "groupingKey2"))))));
    }

    @Test
    public void testAutomaticDecisionForThreeGroupByKeyWithLowCardinality()
    {
        int clusterThreadCount = NODES_COUNT * tester().getSession().getSystemProperty(TASK_CONCURRENCY, Integer.class);
        PlanNodeId aggregationSourceId = new PlanNodeId("aggregationSourceId");
        Symbol groupingKey1 = new Symbol("groupingKey1");
        Symbol groupingKey2 = new Symbol("groupingKey2");
        Symbol groupingKey3 = new Symbol("groupingKey3");
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "automatic")
                .overrideStats(aggregationSourceId.toString(), PlanNodeStatsEstimate.builder()
                        .addSymbolStatistics(groupingKey1, SymbolStatsEstimate.builder().setDistinctValuesCount(clusterThreadCount).build())
                        .addSymbolStatistics(groupingKey2, SymbolStatsEstimate.builder().setDistinctValuesCount(clusterThreadCount).build())
                        .addSymbolStatistics(groupingKey3, SymbolStatsEstimate.builder().setDistinctValuesCount(clusterThreadCount).build()).build())
                .on(p -> p.aggregation(builder -> builder
                        .singleGroupingSet(groupingKey1, groupingKey2, groupingKey3)
                        .addAggregation(p.symbol("non-distinct"), PlanBuilder.expression("sum(a)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct"), PlanBuilder.expression("sum(distinct b)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("a"), p.symbol("b"), p.symbol("groupingKey1"), p.symbol("groupingKey2"), p.symbol("groupingKey3")))))
                .doesNotFire();

        // three group-by keys, unknown stats - prefer mark-distinct
        tester().assertThat(new DistinctAggregationToGroupBy(tester().getPlannerContext(), DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "automatic")
                .overrideStats(aggregationSourceId.toString(), PlanNodeStatsEstimate.builder()
                        .addSymbolStatistics(groupingKey1, SymbolStatsEstimate.builder().setDistinctValuesCount(Double.NaN).build())
                        .addSymbolStatistics(groupingKey2, SymbolStatsEstimate.builder().setDistinctValuesCount(Double.NaN).build())
                        .addSymbolStatistics(groupingKey3, SymbolStatsEstimate.builder().setDistinctValuesCount(Double.NaN).build()).build())
                .on(p -> p.aggregation(builder -> builder
                        .singleGroupingSet(groupingKey1, groupingKey2, groupingKey3)
                        .addAggregation(p.symbol("non-distinct"), PlanBuilder.expression("sum(a)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("distinct"), PlanBuilder.expression("sum(distinct b)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("a"), p.symbol("b"), p.symbol("groupingKey1"), p.symbol("groupingKey2"), p.symbol("groupingKey3")))))
                .doesNotFire();
    }
}