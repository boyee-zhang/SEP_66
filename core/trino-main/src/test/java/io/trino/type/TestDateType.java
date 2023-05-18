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
package io.trino.type;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.Type.Range;

import static io.trino.spi.type.DateType.DATE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDateType
        extends AbstractTestType
{
    public TestDateType()
    {
        super(DATE, SqlDate.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = DATE.createBlockBuilder(null, 15);
        DATE.writeLong(blockBuilder, 1111);
        DATE.writeLong(blockBuilder, 1111);
        DATE.writeLong(blockBuilder, 1111);
        DATE.writeLong(blockBuilder, 2222);
        DATE.writeLong(blockBuilder, 2222);
        DATE.writeLong(blockBuilder, 2222);
        DATE.writeLong(blockBuilder, 2222);
        DATE.writeLong(blockBuilder, 2222);
        DATE.writeLong(blockBuilder, 3333);
        DATE.writeLong(blockBuilder, 3333);
        DATE.writeLong(blockBuilder, 4444);
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        return ((Long) value) + 1;
    }

    @Override
    public void testRange()
    {
        Range range = type.getRange().orElseThrow();
        assertThat(range.getMin()).isEqualTo((long) Integer.MIN_VALUE);
        assertThat(range.getMax()).isEqualTo((long) Integer.MAX_VALUE);
    }

    @Override
    public void testPreviousValue()
    {
        long minValue = Integer.MIN_VALUE;
        long maxValue = Integer.MAX_VALUE;

        assertThat(type.getPreviousValue(minValue)).isEmpty();
        assertThat(type.getPreviousValue(minValue + 1)).hasValue(minValue);

        assertThat(type.getPreviousValue(getSampleValue())).hasValue(1110L);

        assertThat(type.getPreviousValue(maxValue - 1)).hasValue(maxValue - 2);
        assertThat(type.getPreviousValue(maxValue)).hasValue(maxValue - 1);
    }

    @Override
    public void testNextValue()
    {
        long minValue = Integer.MIN_VALUE;
        long maxValue = Integer.MAX_VALUE;

        assertThat(type.getNextValue(minValue)).hasValue(minValue + 1);
        assertThat(type.getNextValue(minValue + 1)).hasValue(minValue + 2);

        assertThat(type.getNextValue(getSampleValue())).hasValue(1112L);

        assertThat(type.getNextValue(maxValue - 1)).hasValue(maxValue);
        assertThat(type.getNextValue(maxValue)).isEmpty();
    }
}
