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
import io.trino.spi.type.Type.Range;

import static io.trino.spi.type.TinyintType.TINYINT;
import static org.testng.Assert.assertEquals;

public class TestTinyintType
        extends AbstractTestType
{
    public TestTinyintType()
    {
        super(TINYINT, Byte.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = TINYINT.createBlockBuilder(null, 15);
        TINYINT.writeLong(blockBuilder, 111);
        TINYINT.writeLong(blockBuilder, 111);
        TINYINT.writeLong(blockBuilder, 111);
        TINYINT.writeLong(blockBuilder, 22);
        TINYINT.writeLong(blockBuilder, 22);
        TINYINT.writeLong(blockBuilder, 22);
        TINYINT.writeLong(blockBuilder, 22);
        TINYINT.writeLong(blockBuilder, 22);
        TINYINT.writeLong(blockBuilder, 33);
        TINYINT.writeLong(blockBuilder, 33);
        TINYINT.writeLong(blockBuilder, 44);
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
        assertEquals(range.getMin(), (long) Byte.MIN_VALUE);
        assertEquals(range.getMax(), (long) Byte.MAX_VALUE);
    }
}
