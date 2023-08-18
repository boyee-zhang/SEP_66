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
package io.trino.plugin.mongodb.expression;

import io.trino.matching.Captures;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.expression.Constant;
import io.trino.spi.type.Type;

import java.util.Optional;

public abstract class RewriteConstant
        implements ConnectorExpressionRule<Constant, FilterExpression>
{
    @Override
    public Optional<FilterExpression> rewrite(Constant constant, Captures captures, RewriteContext<FilterExpression> context)
    {
        Type type = constant.getType();
        Object value = constant.getValue();
        if (value == null) {
            return Optional.of(new FilterExpression(null, FilterExpression.ExpressionType.LITERAL));
        }
        return handleNonNullValue(type, value);
    }

    protected abstract Optional<FilterExpression> handleNonNullValue(Type type, Object value);
}
