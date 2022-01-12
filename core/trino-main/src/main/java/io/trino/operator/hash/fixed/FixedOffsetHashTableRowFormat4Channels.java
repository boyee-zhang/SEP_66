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
package io.trino.operator.hash.fixed;

import io.trino.operator.hash.ColumnValueExtractor;
import io.trino.operator.hash.GroupByHashTableEntries;
import io.trino.operator.hash.HashTableRowFormat;
import io.trino.operator.hash.fastbb.FastByteBuffer;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.BigintType.BIGINT;

public class FixedOffsetHashTableRowFormat4Channels
        implements HashTableRowFormat
{
    private final int maxVarWidthBufferSize;
    private final ColumnValueExtractor columnValueExtractor1;
    private final ColumnValueExtractor columnValueExtractor0;
    private final ColumnValueExtractor columnValueExtractor2;
    private final ColumnValueExtractor columnValueExtractor3;
    private final int mainBufferOffset1;
    private final int mainBufferOffset2;
    private final int mainBufferOffset3;
    private final int mainBufferValuesLength;

    public FixedOffsetHashTableRowFormat4Channels(int maxVarWidthBufferSize, ColumnValueExtractor[] columnValueExtractors)
    {
        this.maxVarWidthBufferSize = maxVarWidthBufferSize;
        checkArgument(columnValueExtractors.length == 4);
        columnValueExtractor0 = columnValueExtractors[0];
        columnValueExtractor1 = columnValueExtractors[1];
        columnValueExtractor2 = columnValueExtractors[2];
        columnValueExtractor3 = columnValueExtractors[3];

        mainBufferOffset1 = calculateMainBufferSize(columnValueExtractor0);
        mainBufferOffset2 = mainBufferOffset1 + calculateMainBufferSize(columnValueExtractor1);
        mainBufferOffset3 = mainBufferOffset2 + calculateMainBufferSize(columnValueExtractor2);

        this.mainBufferValuesLength = mainBufferOffset3 + calculateMainBufferSize(columnValueExtractor3);
    }

    public int calculateMainBufferSize(ColumnValueExtractor columnValueExtractor)
    {
        return calculateMainBufferSize(columnValueExtractor, maxVarWidthBufferSize);
    }

    public static int calculateMainBufferSize(ColumnValueExtractor columnValueExtractor, int maxVarWidthBufferSize)
    {
        int bufferSize = columnValueExtractor.getSize();
        if (columnValueExtractor.isFixedSize()) {
            return bufferSize;
        }

        if (bufferSize > maxVarWidthBufferSize) {
            bufferSize = maxVarWidthBufferSize;
        }

        return bufferSize;
    }

    private void copyToMainBuffer(Page page, int position, FixedOffsetGroupByHashTableEntries row, int offset)
    {
        row.markNoOverflow(offset);
        int valuesOffset = row.getValuesOffset(offset);
        FastByteBuffer mainBuffer = row.getMainBuffer();

        Block block0 = page.getBlock(0);

        columnValueExtractor0.putValue(mainBuffer, valuesOffset, block0, position);

        Block block1 = page.getBlock(1);

        columnValueExtractor1.putValue(mainBuffer, valuesOffset + mainBufferOffset1, block1, position);

        Block block2 = page.getBlock(2);

        columnValueExtractor1.putValue(mainBuffer, valuesOffset + mainBufferOffset2, block2, position);

        Block block3 = page.getBlock(3);

        columnValueExtractor1.putValue(mainBuffer, valuesOffset + mainBufferOffset3, block3, position);
    }

    @Override
    public void copyToTable(Page page, int position, GroupByHashTableEntries table, int entriesPosition)
    {
        FixedOffsetGroupByHashTableEntries entries = (FixedOffsetGroupByHashTableEntries) table;
        putEntryValue(page, position, entries, entriesPosition);

        long hash = entries.calculateValuesHash(entriesPosition);
        entries.putHash(entriesPosition, hash);
    }

    @Override
    public void putEntry(GroupByHashTableEntries entries, int entriesPosition, int groupId, Page page, int position, long rawHash)
    {
        entries.putGroupId(entriesPosition, groupId);
        putEntryValue(page, position, (FixedOffsetGroupByHashTableEntries) entries, entriesPosition);
        entries.putHash(entriesPosition, rawHash);
    }

    private void putEntryValue(Page page, int position, FixedOffsetGroupByHashTableEntries entries, int entriesOffset)
    {
        entries.putIsNull(entriesOffset, 0, (byte) (page.getBlock(0).isNull(position) ? 1 : 0));
        entries.putIsNull(entriesOffset, 1, (byte) (page.getBlock(1).isNull(position) ? 1 : 0));
        entries.putIsNull(entriesOffset, 2, (byte) (page.getBlock(2).isNull(position) ? 1 : 0));
        entries.putIsNull(entriesOffset, 3, (byte) (page.getBlock(3).isNull(position) ? 1 : 0));
        copyToMainBuffer(page, position, entries, entriesOffset);
    }

    @Override
    public boolean keyEquals(GroupByHashTableEntries entries, int entriesPosition, Page page, int position, long rawHash)
    {
        if (rawHash != entries.getHash(entriesPosition)) {
            return false;
        }

        boolean overflow = entries.isOverflow(entriesPosition);
        FixedOffsetGroupByHashTableEntries table = (FixedOffsetGroupByHashTableEntries) entries;
        if (!overflow) {
            int valuesOffset = table.getValuesOffset(entriesPosition);
            FastByteBuffer mainBuffer = table.getMainBuffer();
            return valuesEquals(table, entriesPosition, page, position, mainBuffer, valuesOffset);
        }
        else {
            throw new UnsupportedOperationException();
        }
    }

    private boolean valuesEquals(FixedOffsetGroupByHashTableEntries table, int hashPosition,
            Page page, int position, FastByteBuffer buffer, int valuesOffset)
    {
        Block block0 = page.getBlock(0);

        boolean blockValue0Null = block0.isNull(position);
        byte tableValue0IsNull = table.isNull(hashPosition, 0);
        if (blockValue0Null) {
            return tableValue0IsNull == 1;
        }
        if (tableValue0IsNull == 1) {
            return false;
        }

        if (!columnValueExtractor0.valueEquals(buffer, valuesOffset, block0, position)) {
            return false;
        }

        Block block1 = page.getBlock(1);

        boolean blockValue1Null = block1.isNull(position);
        byte tableValue1IsNull = table.isNull(hashPosition, 1);
        if (blockValue1Null) {
            return tableValue1IsNull == 1;
        }
        if (tableValue1IsNull == 1) {
            return false;
        }

        if (!columnValueExtractor1.valueEquals(buffer, valuesOffset + mainBufferOffset1, block1, position)) {
            return false;
        }

        Block block2 = page.getBlock(2);

        boolean blockValue2Null = block2.isNull(position);
        byte tableValue2IsNull = table.isNull(hashPosition, 2);
        if (blockValue2Null) {
            return tableValue2IsNull == 1;
        }
        if (tableValue2IsNull == 1) {
            return false;
        }

        if (!columnValueExtractor2.valueEquals(buffer, valuesOffset + mainBufferOffset2, block2, position)) {
            return false;
        }

        Block block3 = page.getBlock(3);

        boolean blockValue3Null = block3.isNull(position);
        byte tableValue3IsNull = table.isNull(hashPosition, 3);
        if (blockValue3Null) {
            return tableValue3IsNull == 1;
        }
        if (tableValue3IsNull == 1) {
            return false;
        }

        if (!columnValueExtractor3.valueEquals(buffer, valuesOffset + mainBufferOffset3, block3, position)) {
            return false;
        }

        return true;
    }

    @Override
    public void appendValuesTo(GroupByHashTableEntries entries, int position, PageBuilder pageBuilder, int outputChannelOffset, boolean outputHash)
    {
        FixedOffsetGroupByHashTableEntries hashTable = (FixedOffsetGroupByHashTableEntries) entries;
        boolean overflow = hashTable.isOverflow(position);
        if (!overflow) {
            FastByteBuffer mainBuffer = hashTable.getMainBuffer();
            int valuesOffset = hashTable.getValuesOffset(position);

            BlockBuilder blockBuilder0 = pageBuilder.getBlockBuilder(outputChannelOffset);
            if (hashTable.isNull(position, 0) == 1) {
                blockBuilder0.appendNull();
            }
            else {
                columnValueExtractor0.appendValue(mainBuffer, valuesOffset, blockBuilder0);
            }
            BlockBuilder blockBuilder1 = pageBuilder.getBlockBuilder(outputChannelOffset + 1);
            if (hashTable.isNull(position, 1) == 1) {
                blockBuilder1.appendNull();
            }
            else {
                columnValueExtractor1.appendValue(mainBuffer, valuesOffset + mainBufferOffset1, blockBuilder1);
            }
            BlockBuilder blockBuilder2 = pageBuilder.getBlockBuilder(outputChannelOffset + 2);
            if (hashTable.isNull(position, 2) == 1) {
                blockBuilder2.appendNull();
            }
            else {
                columnValueExtractor2.appendValue(mainBuffer, valuesOffset + mainBufferOffset2, blockBuilder2);
            }
            BlockBuilder blockBuilder3 = pageBuilder.getBlockBuilder(outputChannelOffset + 3);
            if (hashTable.isNull(position, 3) == 1) {
                blockBuilder3.appendNull();
            }
            else {
                columnValueExtractor3.appendValue(mainBuffer, valuesOffset + mainBufferOffset3, blockBuilder3);
            }
        }
        else {
            throw new RuntimeException("not implemented! " + position);
        }

        if (outputHash) {
            BlockBuilder hashBlockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset + 4);
            BIGINT.writeLong(hashBlockBuilder, hashTable.getHash(position));
        }
    }

    @Override
    public GroupByHashTableEntries allocateRowBuffer(int hashChannelsCount)
    {
        return FixedOffsetGroupByHashTableEntries.allocate(1, FastByteBuffer.allocate(1024), hashChannelsCount, false, mainBufferValuesLength);
    }

    @Override
    public GroupByHashTableEntries allocateHashTableEntries(int hashChannelsCount, int hashCapacity, FastByteBuffer overflow)
    {
        return FixedOffsetGroupByHashTableEntries.allocate(hashCapacity, overflow, hashChannelsCount, true, mainBufferValuesLength);
    }
}
