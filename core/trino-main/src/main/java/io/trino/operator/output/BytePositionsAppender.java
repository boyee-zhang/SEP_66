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

import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.operator.output.PositionsAppenderUtil.calculateBlockResetSize;
import static io.trino.operator.output.PositionsAppenderUtil.calculateNewArraySize;
import static java.lang.Math.max;

public class BytePositionsAppender
        implements PositionsAppender
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BytePositionsAppender.class).instanceSize();
    private static final Block NULL_VALUE_BLOCK = new ByteArrayBlock(1, Optional.of(new boolean[] {true}), new byte[1]);

    private boolean initialized;
    private int initialEntryCount;

    private int positionCount;
    private boolean hasNullValue;
    private boolean hasNonNullValue;

    // it is assumed that these arrays are the same length
    private boolean[] valueIsNull = new boolean[0];
    private byte[] values = new byte[0];

    private long retainedSizeInBytes;
    private long sizeInBytes;

    public BytePositionsAppender(int expectedEntries)
    {
        this.initialEntryCount = max(expectedEntries, 1);

        updateRetainedSize();
    }

    @Override
    public void append(IntArrayList positions, Block block)
    {
        if (positions.isEmpty()) {
            return;
        }
        // performance of this method depends on block being always the same, flat type
        checkArgument(block instanceof ByteArrayBlock);
        int[] positionArray = positions.elements();
        int positionsSize = positions.size();
        ensureCapacity(positionCount + positionsSize);

        if (block.mayHaveNull()) {
            for (int i = 0; i < positionsSize; i++) {
                int position = positionArray[i];
                boolean isNull = block.isNull(position);
                int positionIndex = positionCount + i;
                if (isNull) {
                    valueIsNull[positionIndex] = true;
                    hasNullValue = true;
                }
                else {
                    values[positionIndex] = block.getByte(position, 0);
                    hasNonNullValue = true;
                }
            }
            positionCount += positionsSize;
        }
        else {
            for (int i = 0; i < positionsSize; i++) {
                int position = positionArray[i];
                values[positionCount + i] = block.getByte(position, 0);
            }
            positionCount += positionsSize;
            hasNonNullValue = true;
        }

        updateSize(positionsSize);
    }

    @Override
    public void appendRle(RunLengthEncodedBlock block)
    {
        int rlePositionCount = block.getPositionCount();
        if (rlePositionCount == 0) {
            return;
        }
        int sourcePosition = 0;
        ensureCapacity(positionCount + rlePositionCount);
        if (block.isNull(sourcePosition)) {
            Arrays.fill(valueIsNull, positionCount, positionCount + rlePositionCount, true);
            hasNullValue = true;
        }
        else {
            byte value = block.getByte(sourcePosition, 0);
            Arrays.fill(values, positionCount, positionCount + rlePositionCount, value);
            hasNonNullValue = true;
        }
        positionCount += rlePositionCount;

        updateSize(rlePositionCount);
    }

    @Override
    public Block build()
    {
        if (!hasNonNullValue) {
            return new RunLengthEncodedBlock(NULL_VALUE_BLOCK, positionCount);
        }
        ByteArrayBlock result = new ByteArrayBlock(positionCount, hasNullValue ? Optional.of(valueIsNull) : Optional.empty(), values);
        reset();
        return result;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public long getSizeInBytes()
    {
        return sizeInBytes;
    }

    private void reset()
    {
        initialEntryCount = calculateBlockResetSize(positionCount);
        initialized = false;
        valueIsNull = new boolean[0];
        values = new byte[0];
        positionCount = 0;
        sizeInBytes = 0;
        hasNonNullValue = false;
        hasNullValue = false;
        updateRetainedSize();
    }

    private void ensureCapacity(int capacity)
    {
        if (values.length >= capacity) {
            return;
        }

        int newSize;
        if (initialized) {
            newSize = calculateNewArraySize(values.length);
        }
        else {
            newSize = initialEntryCount;
            initialized = true;
        }
        newSize = Math.max(newSize, capacity);

        valueIsNull = Arrays.copyOf(valueIsNull, newSize);
        values = Arrays.copyOf(values, newSize);
        updateRetainedSize();
    }

    private void updateSize(long positionsSize)
    {
        sizeInBytes += ByteArrayBlock.SIZE_IN_BYTES_PER_POSITION * positionsSize;
    }

    private void updateRetainedSize()
    {
        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(values);
    }
}
