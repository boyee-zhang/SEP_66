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
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import io.trino.sql.ir.ComparisonExpression;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionRewriter;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.InPredicate;
import io.trino.sql.ir.LogicalExpression;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.ir.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.ir.IrUtils.or;
import static io.trino.sql.ir.LogicalExpression.Operator.AND;

public final class NormalizeOrExpressionRewriter
{
    public static Expression normalizeOrExpression(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(), expression);
    }

    private NormalizeOrExpressionRewriter() {}

    private static class Visitor
            extends ExpressionRewriter<Void>
    {
        @Override
        public Expression rewriteLogicalExpression(LogicalExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            List<Expression> terms = node.terms().stream()
                    .map(expression -> treeRewriter.rewrite(expression, context))
                    .collect(toImmutableList());

            if (node.operator() == AND) {
                return and(terms);
            }

            ImmutableList.Builder<InPredicate> inPredicateBuilder = ImmutableList.builder();
            ImmutableSet.Builder<Expression> expressionToSkipBuilder = ImmutableSet.builder();
            ImmutableList.Builder<Expression> othersExpressionBuilder = ImmutableList.builder();
            groupComparisonAndInPredicate(terms).forEach((expression, values) -> {
                if (values.size() > 1) {
                    inPredicateBuilder.add(new InPredicate(expression, mergeToInListExpression(values)));
                    expressionToSkipBuilder.add(expression);
                }
            });

            Set<Expression> expressionToSkip = expressionToSkipBuilder.build();
            for (Expression expression : terms) {
                if (expression instanceof ComparisonExpression comparisonExpression && comparisonExpression.operator() == EQUAL) {
                    if (!expressionToSkip.contains(comparisonExpression.left())) {
                        othersExpressionBuilder.add(expression);
                    }
                }
                else if (expression instanceof InPredicate inPredicate) {
                    if (!expressionToSkip.contains(inPredicate.value())) {
                        othersExpressionBuilder.add(expression);
                    }
                }
                else {
                    othersExpressionBuilder.add(expression);
                }
            }

            return or(ImmutableList.<Expression>builder()
                    .addAll(othersExpressionBuilder.build())
                    .addAll(inPredicateBuilder.build())
                    .build());
        }

        private List<Expression> mergeToInListExpression(Collection<Expression> expressions)
        {
            LinkedHashSet<Expression> expressionValues = new LinkedHashSet<>();
            for (Expression expression : expressions) {
                if (expression instanceof ComparisonExpression comparisonExpression && comparisonExpression.operator() == EQUAL) {
                    expressionValues.add(comparisonExpression.right());
                }
                else if (expression instanceof InPredicate inPredicate) {
                    expressionValues.addAll(inPredicate.valueList());
                }
                else {
                    throw new IllegalStateException("Unexpected expression: " + expression);
                }
            }

            return ImmutableList.copyOf(expressionValues);
        }

        private Map<Expression, Collection<Expression>> groupComparisonAndInPredicate(List<Expression> terms)
        {
            ImmutableMultimap.Builder<Expression, Expression> expressionBuilder = ImmutableMultimap.builder();
            for (Expression expression : terms) {
                if (expression instanceof ComparisonExpression comparisonExpression && comparisonExpression.operator() == EQUAL) {
                    expressionBuilder.put(comparisonExpression.left(), comparisonExpression);
                }
                else if (expression instanceof InPredicate inPredicate) {
                    expressionBuilder.put(inPredicate.value(), inPredicate);
                }
            }

            return expressionBuilder.build().asMap();
        }
    }
}
