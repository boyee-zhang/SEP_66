package io.trino.operator.hash.fixed;

import io.trino.operator.hash.ColumnValueExtractor;
import io.trino.operator.hash.FastByteBuffer;
import io.trino.operator.hash.GroupByHashTableEntries;
import io.trino.operator.hash.RowExtractor;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.BigintType.BIGINT;

public class FixedOffsetRowExtractor
        implements RowExtractor
{
    private static final int MAX_VAR_WIDTH_BUFFER_SIZE = 16;
    private final int[] hashChannels;
    private final ColumnValueExtractor[] columnValueExtractors;
    private final int[] mainBufferOffsets;
    private final int[] valueOffsets;
    private final byte[] isNull;
    private final int mainBufferValuesLength;

    public FixedOffsetRowExtractor(int[] hashChannels, ColumnValueExtractor[] columnValueExtractors)
    {
        checkArgument(hashChannels.length == columnValueExtractors.length);
        this.hashChannels = hashChannels;
        this.columnValueExtractors = columnValueExtractors;

        valueOffsets = new int[hashChannels.length];
        isNull = new byte[hashChannels.length];
        mainBufferOffsets = new int[hashChannels.length];
        int mainBufferOffset = 0;
        for (int i = 0; i < columnValueExtractors.length; i++) {
            mainBufferOffsets[i] = mainBufferOffset;
            mainBufferOffset += calculateMainBufferSize(columnValueExtractors[i]);
        }
        this.mainBufferValuesLength = mainBufferOffset;
    }

    public static int calculateMainBufferSize(ColumnValueExtractor columnValueExtractor)
    {
        int bufferSize = columnValueExtractor.getSize();
        if (columnValueExtractor.isFixedSize()) {
            return bufferSize;
        }

        if (bufferSize > MAX_VAR_WIDTH_BUFFER_SIZE) {
            bufferSize = MAX_VAR_WIDTH_BUFFER_SIZE;
        }

        return bufferSize;
    }

    private void copyToMainBuffer(Page page, int position, FixedOffsetGroupByHashTableEntries row, int offset)
    {
        row.markNoOverflow(offset);
        int valuesOffset = row.getValuesOffset(offset);
        FastByteBuffer mainBuffer = row.getMainBuffer();
        for (int i = 0; i < hashChannels.length; i++) {
            Block block = page.getBlock(hashChannels[i]);

            columnValueExtractors[i].putValue(mainBuffer, valuesOffset + mainBufferOffsets[i], block, position);
        }
    }

    @Override
    public void copyToEntriesTable(Page page, int position, GroupByHashTableEntries table, int entriesPosition)
    {
        FixedOffsetGroupByHashTableEntries entries = (FixedOffsetGroupByHashTableEntries) table;
        putEntryValue(page, position, entries, entriesPosition);

        long hash = entries.calculateValuesHash(entriesPosition);
        entries.putHash(entriesPosition, hash);
    }

    @Override
    public void putEntry(GroupByHashTableEntries entries, int hashPosition, int groupId, Page page, int position, long rawHash)
    {
        entries.putGroupId(hashPosition, groupId);
        putEntryValue(page, position, (FixedOffsetGroupByHashTableEntries) entries, hashPosition);
        entries.putHash(hashPosition, rawHash);
    }

    private void putEntryValue(Page page, int position, FixedOffsetGroupByHashTableEntries entries, int entriesOffset)
    {
        //        entries.clear();
        boolean overflow = false;
        int offset = 0;
        for (int i = 0; i < hashChannels.length; i++) {
            Block block = page.getBlock(hashChannels[i]);
            boolean valueIsNull = block.isNull(position);
            isNull[i] = (byte) (valueIsNull ? 1 : 0);

//            int valueLength = valueIsNull ? 0 : columnValueExtractors[i].getSerializedValueLength(block, position);
//            valueOffsets[i] = offset;
//            offset += valueLength;
//            if (valueLength > MAX_VAR_WIDTH_BUFFER_SIZE) {
//                overflow = true;
//            }
        }

        entries.putIsNull(entriesOffset, isNull);
        if (!overflow) {
            copyToMainBuffer(page, position, entries, entriesOffset);
        }
        else {
            /// put in overflow
//            entries.reserveOverflowLength(0, offset);
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public boolean keyEquals(GroupByHashTableEntries entries, int hashPosition, Page page, int position, long rawHash)
    {
        if (rawHash != entries.getHash(hashPosition)) {
            return false;
        }

        boolean overflow = entries.isOverflow(hashPosition);
        FixedOffsetGroupByHashTableEntries table = (FixedOffsetGroupByHashTableEntries) entries;
        if (!overflow) {
            int valuesOffset = table.getValuesOffset(hashPosition);
            FastByteBuffer mainBuffer = table.getMainBuffer();
            return valuesEquals(table, hashPosition, page, position, mainBuffer, valuesOffset);
        }
        else {
            throw new UnsupportedOperationException();
        }
    }

    private boolean valuesEquals(FixedOffsetGroupByHashTableEntries table, int hashPosition,
            Page page, int position, FastByteBuffer buffer, int valuesOffset)
    {
        for (int i = 0; i < hashChannels.length; i++) {
            Block block = page.getBlock(hashChannels[i]);

            boolean blockValueNull = block.isNull(position);
            byte tableValueIsNull = table.isNull(hashPosition, i);
            if (blockValueNull) {
                return tableValueIsNull == 1;
            }
            if (tableValueIsNull == 1) {
                return false;
            }

            if (!columnValueExtractors[i].valueEquals(buffer, valuesOffset + mainBufferOffsets[i], block, position)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void appendValuesTo(GroupByHashTableEntries entries, int hashPosition, PageBuilder pageBuilder, int outputChannelOffset, boolean outputHash)
    {
        FixedOffsetGroupByHashTableEntries hashTable = (FixedOffsetGroupByHashTableEntries) entries;
        boolean overflow = hashTable.isOverflow(hashPosition);
        if (!overflow) {
            FastByteBuffer mainBuffer = hashTable.getMainBuffer();
            int valuesOffset = hashTable.getValuesOffset(hashPosition);

            for (int i = 0; i < hashChannels.length; i++, outputChannelOffset++) {
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset);
                if (hashTable.isNull(hashPosition, i) == 1) {
                    blockBuilder.appendNull();
                }
                else {
                    columnValueExtractors[i].appendValue(mainBuffer, valuesOffset + mainBufferOffsets[i], blockBuilder);
                }
            }
        }
        else {
            throw new UnsupportedOperationException();
        }

        if (outputHash) {
            BlockBuilder hashBlockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset);
            BIGINT.writeLong(hashBlockBuilder, hashTable.getHash(hashPosition));
        }
    }

    @Override
    public GroupByHashTableEntries allocateRowBuffer(int hashChannelsCount, int dataValuesLength)
    {
        return new FixedOffsetGroupByHashTableEntries(1, FastByteBuffer.allocate(1024), hashChannelsCount, false, dataValuesLength);
    }

    @Override
    public GroupByHashTableEntries allocateHashTableEntries(int hashChannelsCount, int hashCapacity, FastByteBuffer overflow, int dataValuesLength)
    {
        return new FixedOffsetGroupByHashTableEntries(hashCapacity, overflow, hashChannelsCount, true, dataValuesLength);
    }

    @Override
    public int mainBufferValuesLength()
    {
        return mainBufferValuesLength;
    }
}
