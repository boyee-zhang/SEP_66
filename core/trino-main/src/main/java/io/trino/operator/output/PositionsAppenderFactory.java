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
package io.trino.operator.output;

import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.Int96ArrayBlock;
import io.trino.spi.type.FixedWidthType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VariableWidthType;

import javax.annotation.Nullable;

public class PositionsAppenderFactory
{
    public PositionsAppender create(Type type, @Nullable BlockBuilderStatus blockBuilderStatus, int expectedPositions)
    {
        return new BlockTypeDispatchingPositionsAppender(createDedicatedAppenderFor(type, blockBuilderStatus, expectedPositions));
    }

    private BlockTypeAwarePositionsAppender createDedicatedAppenderFor(Type type, @Nullable BlockBuilderStatus blockBuilderStatus, int expectedPositions)
    {
        if (type instanceof FixedWidthType) {
            switch (((FixedWidthType) type).getFixedSize()) {
                case Byte.BYTES:
                    return new io.trino.operator.output.BytePositionsAppender(blockBuilderStatus, expectedPositions);
                case Short.BYTES:
                    return new ShortPositionsAppender(blockBuilderStatus, expectedPositions);
                case Integer.BYTES:
                    return new io.trino.operator.output.IntPositionsAppender(blockBuilderStatus, expectedPositions);
                case Long.BYTES:
                    return new io.trino.operator.output.LongPositionsAppender(blockBuilderStatus, expectedPositions);
                case Int96ArrayBlock.INT96_BYTES:
                    return new io.trino.operator.output.Int96PositionsAppender(blockBuilderStatus, expectedPositions);
                case Int128ArrayBlock.INT128_BYTES:
                    return new io.trino.operator.output.Int128PositionsAppender(blockBuilderStatus, expectedPositions);
                default:
                    // size not supported directly, fallback to the generic appender
            }
        }
        else if (type instanceof VariableWidthType) {
            return new io.trino.operator.output.SlicePositionsAppender(blockBuilderStatus, expectedPositions);
        }

        return new TypedPositionsAppender(type, blockBuilderStatus, expectedPositions);
    }
}
