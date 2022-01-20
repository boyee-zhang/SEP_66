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
package io.trino.operator.hash.fixedwidth.gen;

import io.trino.operator.hash.ColumnValueExtractor;
import io.trino.operator.hash.GroupByHashTableEntries;
import io.trino.operator.hash.HashTableRowHandler;
import io.trino.operator.hash.fastbb.FastByteBuffer;
import io.trino.operator.hash.fixedwidth.FixedWidthEntryStructure;
import io.trino.operator.hash.fixedwidth.FixedWidthGroupByHashTableEntries;
import io.trino.spi.Page;
import io.trino.spi.block.Block;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * This class has been generated by the FixedWidthHashTableRowHandlerGenerator class.
 * The generation is a one-time event and is not repeated during build
 */
public final class FixedWidthHashTableRowHandler6Channels
        implements HashTableRowHandler
{
    private final ColumnValueExtractor columnValueExtractor0;

    private final ColumnValueExtractor columnValueExtractor1;

    private final ColumnValueExtractor columnValueExtractor2;

    private final ColumnValueExtractor columnValueExtractor3;

    private final ColumnValueExtractor columnValueExtractor4;

    private final ColumnValueExtractor columnValueExtractor5;

    private final int value1Offset;

    private final int value2Offset;

    private final int value3Offset;

    private final int value4Offset;

    private final int value5Offset;

    public FixedWidthHashTableRowHandler6Channels(FixedWidthEntryStructure structure)
    {
        requireNonNull(structure, "structure is null");
        checkArgument(structure.getHashChannelsCount() == 6);
        columnValueExtractor0 = structure.getColumnValueExtractors()[0];
        columnValueExtractor1 = structure.getColumnValueExtractors()[1];
        columnValueExtractor2 = structure.getColumnValueExtractors()[2];
        columnValueExtractor3 = structure.getColumnValueExtractors()[3];
        columnValueExtractor4 = structure.getColumnValueExtractors()[4];
        columnValueExtractor5 = structure.getColumnValueExtractors()[5];
        value1Offset = structure.getValuesOffsets()[1];
        value2Offset = structure.getValuesOffsets()[2];
        value3Offset = structure.getValuesOffsets()[3];
        value4Offset = structure.getValuesOffsets()[4];
        value5Offset = structure.getValuesOffsets()[5];
    }

    @Override
    public void putEntry(GroupByHashTableEntries data, int entriesPosition, int groupId, Page page,
            int position, long rawHash)
    {
        FixedWidthGroupByHashTableEntries entries = (FixedWidthGroupByHashTableEntries) data;
        entries.putGroupId(entriesPosition, groupId);
        entries.putHash(entriesPosition, rawHash);
        int valuesOffset = entries.getValuesOffset(entriesPosition);
        FastByteBuffer buffer = entries.getBuffer();
        Block block0 = page.getBlock(0);
        byte value0IsNull = (byte) (block0.isNull(position) ? 1 : 0);
        entries.putIsNull(entriesPosition, 0, value0IsNull);
        columnValueExtractor0.putValue(buffer, valuesOffset, block0, position);
        Block block1 = page.getBlock(1);
        byte value1IsNull = (byte) (block1.isNull(position) ? 1 : 0);
        entries.putIsNull(entriesPosition, 1, value1IsNull);
        columnValueExtractor1.putValue(buffer, valuesOffset + value1Offset, block1, position);
        Block block2 = page.getBlock(2);
        byte value2IsNull = (byte) (block2.isNull(position) ? 1 : 0);
        entries.putIsNull(entriesPosition, 2, value2IsNull);
        columnValueExtractor2.putValue(buffer, valuesOffset + value2Offset, block2, position);
        Block block3 = page.getBlock(3);
        byte value3IsNull = (byte) (block3.isNull(position) ? 1 : 0);
        entries.putIsNull(entriesPosition, 3, value3IsNull);
        columnValueExtractor3.putValue(buffer, valuesOffset + value3Offset, block3, position);
        Block block4 = page.getBlock(4);
        byte value4IsNull = (byte) (block4.isNull(position) ? 1 : 0);
        entries.putIsNull(entriesPosition, 4, value4IsNull);
        columnValueExtractor4.putValue(buffer, valuesOffset + value4Offset, block4, position);
        Block block5 = page.getBlock(5);
        byte value5IsNull = (byte) (block5.isNull(position) ? 1 : 0);
        entries.putIsNull(entriesPosition, 5, value5IsNull);
        columnValueExtractor5.putValue(buffer, valuesOffset + value5Offset, block5, position);
    }

    @Override
    public boolean keyEquals(GroupByHashTableEntries data, int entriesPosition, Page page,
            int position, long rawHash)
    {
        FixedWidthGroupByHashTableEntries entries = (FixedWidthGroupByHashTableEntries) data;
        int valuesOffset = entries.getValuesOffset(entriesPosition);
        FastByteBuffer buffer = entries.getBuffer();
        Block block0 = page.getBlock(0);
        boolean block0ValueNull = block0.isNull(position);
        boolean entriesValue0IsNull = entries.isNull(entriesPosition, 0) == 1;
        if (block0ValueNull != entriesValue0IsNull) {
            return false;
        }
        if (!block0ValueNull && !columnValueExtractor0.valueEquals(buffer, valuesOffset, block0, position)) {
            return false;
        }
        Block block1 = page.getBlock(1);
        boolean block1ValueNull = block1.isNull(position);
        boolean entriesValue1IsNull = entries.isNull(entriesPosition, 1) == 1;
        if (block1ValueNull != entriesValue1IsNull) {
            return false;
        }
        if (!block1ValueNull && !columnValueExtractor1.valueEquals(buffer, valuesOffset + value1Offset, block1, position)) {
            return false;
        }
        Block block2 = page.getBlock(2);
        boolean block2ValueNull = block2.isNull(position);
        boolean entriesValue2IsNull = entries.isNull(entriesPosition, 2) == 1;
        if (block2ValueNull != entriesValue2IsNull) {
            return false;
        }
        if (!block2ValueNull && !columnValueExtractor2.valueEquals(buffer, valuesOffset + value2Offset, block2, position)) {
            return false;
        }
        Block block3 = page.getBlock(3);
        boolean block3ValueNull = block3.isNull(position);
        boolean entriesValue3IsNull = entries.isNull(entriesPosition, 3) == 1;
        if (block3ValueNull != entriesValue3IsNull) {
            return false;
        }
        if (!block3ValueNull && !columnValueExtractor3.valueEquals(buffer, valuesOffset + value3Offset, block3, position)) {
            return false;
        }
        Block block4 = page.getBlock(4);
        boolean block4ValueNull = block4.isNull(position);
        boolean entriesValue4IsNull = entries.isNull(entriesPosition, 4) == 1;
        if (block4ValueNull != entriesValue4IsNull) {
            return false;
        }
        if (!block4ValueNull && !columnValueExtractor4.valueEquals(buffer, valuesOffset + value4Offset, block4, position)) {
            return false;
        }
        Block block5 = page.getBlock(5);
        boolean block5ValueNull = block5.isNull(position);
        boolean entriesValue5IsNull = entries.isNull(entriesPosition, 5) == 1;
        if (block5ValueNull != entriesValue5IsNull) {
            return false;
        }
        return block5ValueNull || columnValueExtractor5.valueEquals(buffer, valuesOffset + value5Offset, block5, position);
    }
}
