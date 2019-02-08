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
package io.prestosql.operator.aggregation;

import io.prestosql.metadata.FunctionHandle;
import io.prestosql.metadata.FunctionManager;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.tree.QualifiedName;

import java.util.concurrent.ThreadLocalRandom;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;

public class TestApproximateCountDistinctLong
        extends AbstractTestApproximateCountDistinct
{
    @Override
    public InternalAggregationFunction getAggregationFunction()
    {
        FunctionManager functionManager = metadata.getFunctionManager();
        FunctionHandle functionHandle = functionManager.lookupFunction(QualifiedName.of("approx_distinct"), fromTypes(BIGINT, DOUBLE));
        return functionManager.getAggregateFunctionImplementation(functionHandle);
    }

    @Override
    public Type getValueType()
    {
        return BIGINT;
    }

    @Override
    public Object randomValue()
    {
        return ThreadLocalRandom.current().nextLong();
    }
}
