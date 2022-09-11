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
package io.trino.spi.block;

import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.util.Arrays;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.ArrayBlock.createArrayBlockInternal;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

public class ArrayBlockBuilder
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = instanceSize(ArrayBlockBuilder.class);

    private int positionCount;

    @Nullable
    private final BlockBuilderStatus blockBuilderStatus;
    private boolean initialized;
    private final int initialEntryCount;

    private int[] offsets = new int[1];
    private boolean[] valueIsNull = new boolean[0];
    private boolean hasNullValue;
    private boolean hasNonNullRow;

    private final BlockBuilder values;
    private boolean currentEntryOpened;

    private long retainedSizeInBytes;

    /**
     * Caller of this constructor is responsible for making sure `valuesBlock` is constructed with the same `blockBuilderStatus` as the one in the argument
     */
    public ArrayBlockBuilder(BlockBuilder valuesBlock, BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        this(
                blockBuilderStatus,
                valuesBlock,
                expectedEntries);
    }

    public ArrayBlockBuilder(Type elementType, BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        this(
                blockBuilderStatus,
                elementType.createBlockBuilder(blockBuilderStatus, expectedEntries, expectedBytesPerEntry),
                expectedEntries);
    }

    public ArrayBlockBuilder(Type elementType, BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        this(
                blockBuilderStatus,
                elementType.createBlockBuilder(blockBuilderStatus, expectedEntries),
                expectedEntries);
    }

    /**
     * Caller of this private constructor is responsible for making sure `values` is constructed with the same `blockBuilderStatus` as the one in the argument
     */
    private ArrayBlockBuilder(@Nullable BlockBuilderStatus blockBuilderStatus, BlockBuilder values, int expectedEntries)
    {
        this.blockBuilderStatus = blockBuilderStatus;
        this.values = requireNonNull(values, "values is null");
        this.initialEntryCount = max(expectedEntries, 1);

        updateDataSize();
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        return values.getSizeInBytes() + ((Integer.BYTES + Byte.BYTES) * (long) positionCount);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes + values.getRetainedSizeInBytes();
    }

    public <E extends Throwable> void buildEntry(ArrayValueBuilder<E> builder)
            throws E
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Expected current entry to be closed but was opened");
        }

        currentEntryOpened = true;
        builder.build(values);
        entryAdded(false);
        currentEntryOpened = false;
    }

    @Override
    public BlockBuilder appendNull()
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Current entry must be closed before a null can be written");
        }

        entryAdded(true);
        return this;
    }

    private void entryAdded(boolean isNull)
    {
        if (valueIsNull.length <= positionCount) {
            growCapacity();
        }
        offsets[positionCount + 1] = values.getPositionCount();
        valueIsNull[positionCount] = isNull;
        hasNullValue |= isNull;
        hasNonNullRow |= !isNull;
        positionCount++;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Integer.BYTES + Byte.BYTES);
        }
    }

    private void growCapacity()
    {
        int newSize;
        if (initialized) {
            newSize = BlockUtil.calculateNewArraySize(valueIsNull.length);
        }
        else {
            newSize = initialEntryCount;
            initialized = true;
        }

        valueIsNull = Arrays.copyOf(valueIsNull, newSize);
        offsets = Arrays.copyOf(offsets, newSize + 1);
        updateDataSize();
    }

    private void updateDataSize()
    {
        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(offsets);
        if (blockBuilderStatus != null) {
            retainedSizeInBytes += BlockBuilderStatus.INSTANCE_SIZE;
        }
    }

    @Override
    public Block build()
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Current entry must be closed before the block can be built");
        }
        if (!hasNonNullRow) {
            return nullRle(positionCount);
        }
        return createArrayBlockInternal(0, positionCount, hasNullValue ? valueIsNull : null, offsets, values.build());
    }

    @Override
    public BlockBuilder newBlockBuilderLike(int expectedEntries, BlockBuilderStatus blockBuilderStatus)
    {
        return new ArrayBlockBuilder(blockBuilderStatus, values.newBlockBuilderLike(blockBuilderStatus), expectedEntries);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("ArrayBlockBuilder{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append('}');
        return sb.toString();
    }

    private Block nullRle(int positionCount)
    {
        ArrayBlock nullValueBlock = createArrayBlockInternal(0, 1, new boolean[] {true}, new int[] {0, 0}, values.newBlockBuilderLike(null).build());
        return RunLengthEncodedBlock.create(nullValueBlock, positionCount);
    }
}
