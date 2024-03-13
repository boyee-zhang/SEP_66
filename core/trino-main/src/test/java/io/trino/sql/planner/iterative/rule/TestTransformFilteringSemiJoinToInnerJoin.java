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
import io.trino.sql.ir.ComparisonExpression;
import io.trino.sql.ir.LogicalExpression;
import io.trino.sql.ir.LongLiteral;
import io.trino.sql.ir.SymbolReference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.sql.ir.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.ir.LogicalExpression.Operator.AND;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.JoinType.INNER;

public class TestTransformFilteringSemiJoinToInnerJoin
        extends BaseRuleTest
{
    @Test
    public void testTransformSemiJoinToInnerJoin()
    {
        tester().assertThat(new TransformFilteringSemiJoinToInnerJoin())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol aInB = p.symbol("a_in_b");
                    return p.filter(
                            new LogicalExpression(AND, ImmutableList.of(new SymbolReference("a_in_b"), new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new LongLiteral("5")))),
                            p.semiJoin(
                                    p.values(a),
                                    p.values(b),
                                    a,
                                    b,
                                    aInB,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()));
                })
                .matches(project(
                        ImmutableMap.of("a", PlanMatchPattern.expression(new SymbolReference("a")), "a_in_b", PlanMatchPattern.expression(TRUE_LITERAL)),
                        join(INNER, builder -> builder
                                .equiCriteria("a", "b")
                                .filter(new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new LongLiteral("5")))
                                .left(values("a"))
                                .right(
                                        aggregation(
                                                singleGroupingSet("b"),
                                                ImmutableMap.of(),
                                                Optional.empty(),
                                                SINGLE,
                                                values("b"))))));
    }

    @Test
    public void testRemoveRedundantFilter()
    {
        tester().assertThat(new TransformFilteringSemiJoinToInnerJoin())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol aInB = p.symbol("a_in_b");
                    return p.filter(
                            new SymbolReference("a_in_b"),
                            p.semiJoin(
                                    p.values(a),
                                    p.values(b),
                                    a,
                                    b,
                                    aInB,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()));
                })
                .matches(project(
                        ImmutableMap.of("a", PlanMatchPattern.expression(new SymbolReference("a")), "a_in_b", PlanMatchPattern.expression(TRUE_LITERAL)),
                        join(INNER, builder -> builder
                                .equiCriteria("a", "b")
                                .left(values("a"))
                                .right(
                                        aggregation(
                                                singleGroupingSet("b"),
                                                ImmutableMap.of(),
                                                Optional.empty(),
                                                SINGLE,
                                                values("b"))))));
    }

    @Test
    public void testFilterNotMatching()
    {
        tester().assertThat(new TransformFilteringSemiJoinToInnerJoin())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol aInB = p.symbol("a_in_b");
                    return p.filter(
                            new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new LongLiteral("5")),
                            p.semiJoin(
                                    p.values(a),
                                    p.values(b),
                                    a,
                                    b,
                                    aInB,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()));
                })
                .doesNotFire();
    }

    @Test
    public void testDoNotRewriteInContextOfDelete()
    {
        tester().assertThat(new TransformFilteringSemiJoinToInnerJoin())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol aInB = p.symbol("a_in_b");
                    return p.filter(
                            new SymbolReference("a_in_b"),
                            p.semiJoin(
                                    p.tableScan(
                                            ImmutableList.of(a),
                                            true),
                                    p.values(b),
                                    a,
                                    b,
                                    aInB,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()));
                })
                .doesNotFire();

        tester().assertThat(new TransformFilteringSemiJoinToInnerJoin())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    Symbol aInB = p.symbol("a_in_b");
                    return p.filter(
                            new SymbolReference("a_in_b"),
                            p.semiJoin(
                                    p.project(
                                            Assignments.of(a, new SymbolReference("c")),
                                            p.tableScan(
                                                    ImmutableList.of(c),
                                                    true)),
                                    p.values(b),
                                    a,
                                    b,
                                    aInB,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()));
                })
                .doesNotFire();
    }
}
