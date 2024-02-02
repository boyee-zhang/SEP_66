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
package io.trino.plugin.ignite;

import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.expression.Variable;

import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.basicAggregation;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.expressionType;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.functionName;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.singleArgument;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.variable;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ImplementBitwiseAndAggregate
        implements AggregateFunctionRule<JdbcExpression, ParameterizedExpression>
{
    private static final Capture<Variable> ARGUMENT = newCapture();

    private final JdbcTypeHandle bigintTypeHandle;

    public ImplementBitwiseAndAggregate(JdbcTypeHandle bigintTypeHandle)
    {
        this.bigintTypeHandle = requireNonNull(bigintTypeHandle, "bigintTypeHandle is null");
    }

    @Override
    public Pattern<AggregateFunction> getPattern()
    {
        return basicAggregation()
                .with(functionName().equalTo("bitwise_and_agg"))
                .with(singleArgument().matching(
                        variable()
                                .with(expressionType().matching(type -> type == BIGINT))
                                .capturedAs(ARGUMENT)));
    }

    @Override
    public Optional<JdbcExpression> rewrite(AggregateFunction aggregateFunction, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        Variable argument = captures.get(ARGUMENT);
        verify(aggregateFunction.getOutputType() == BIGINT);

        ParameterizedExpression rewrittenArgument = context.rewriteExpression(argument).orElseThrow();
        return Optional.of(new JdbcExpression(
                format("bit_and(%s)", rewrittenArgument.expression()),
                rewrittenArgument.parameters(),
                bigintTypeHandle));
    }
}
