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

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;
import io.prestosql.type.SqlIntervalYearMonth;

import java.util.List;

import static io.prestosql.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;

public class TestIntervalYearToMonthSumAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    protected Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder blockBuilder = INTERVAL_YEAR_MONTH.createBlockBuilder(null, length);
        for (int i = start; i < start + length; i++) {
            INTERVAL_YEAR_MONTH.writeLong(blockBuilder, i);
        }
        return new Block[] {blockBuilder.build()};
    }

    @Override
    protected SqlIntervalYearMonth getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }

        int sum = 0;
        for (int i = start; i < start + length; i++) {
            sum += i;
        }
        return new SqlIntervalYearMonth(sum);
    }

    @Override
    protected String getFunctionName()
    {
        return "sum";
    }

    @Override
    protected List<Type> getFunctionParameterTypes()
    {
        return ImmutableList.of(INTERVAL_YEAR_MONTH);
    }
}
