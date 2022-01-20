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
public final class FixedWidthHashTableRowHandler13Channels
        implements HashTableRowHandler
{
    private final ColumnValueExtractor columnValueExtractor0;

    private final ColumnValueExtractor columnValueExtractor1;

    private final ColumnValueExtractor columnValueExtractor2;

    private final ColumnValueExtractor columnValueExtractor3;

    private final ColumnValueExtractor columnValueExtractor4;

    private final ColumnValueExtractor columnValueExtractor5;

    private final ColumnValueExtractor columnValueExtractor6;

    private final ColumnValueExtractor columnValueExtractor7;

    private final ColumnValueExtractor columnValueExtractor8;

    private final ColumnValueExtractor columnValueExtractor9;

    private final ColumnValueExtractor columnValueExtractor10;

    private final ColumnValueExtractor columnValueExtractor11;

    private final ColumnValueExtractor columnValueExtractor12;

    private final int value1Offset;

    private final int value2Offset;

    private final int value3Offset;

    private final int value4Offset;

    private final int value5Offset;

    private final int value6Offset;

    private final int value7Offset;

    private final int value8Offset;

    private final int value9Offset;

    private final int value10Offset;

    private final int value11Offset;

    private final int value12Offset;

    public FixedWidthHashTableRowHandler13Channels(FixedWidthEntryStructure structure)
    {
        requireNonNull(structure, "structure is null");
        checkArgument(structure.getHashChannelsCount() == 13);
        columnValueExtractor0 = structure.getColumnValueExtractors()[0];
        columnValueExtractor1 = structure.getColumnValueExtractors()[1];
        columnValueExtractor2 = structure.getColumnValueExtractors()[2];
        columnValueExtractor3 = structure.getColumnValueExtractors()[3];
        columnValueExtractor4 = structure.getColumnValueExtractors()[4];
        columnValueExtractor5 = structure.getColumnValueExtractors()[5];
        columnValueExtractor6 = structure.getColumnValueExtractors()[6];
        columnValueExtractor7 = structure.getColumnValueExtractors()[7];
        columnValueExtractor8 = structure.getColumnValueExtractors()[8];
        columnValueExtractor9 = structure.getColumnValueExtractors()[9];
        columnValueExtractor10 = structure.getColumnValueExtractors()[10];
        columnValueExtractor11 = structure.getColumnValueExtractors()[11];
        columnValueExtractor12 = structure.getColumnValueExtractors()[12];
        value1Offset = structure.getValuesOffsets()[1];
        value2Offset = structure.getValuesOffsets()[2];
        value3Offset = structure.getValuesOffsets()[3];
        value4Offset = structure.getValuesOffsets()[4];
        value5Offset = structure.getValuesOffsets()[5];
        value6Offset = structure.getValuesOffsets()[6];
        value7Offset = structure.getValuesOffsets()[7];
        value8Offset = structure.getValuesOffsets()[8];
        value9Offset = structure.getValuesOffsets()[9];
        value10Offset = structure.getValuesOffsets()[10];
        value11Offset = structure.getValuesOffsets()[11];
        value12Offset = structure.getValuesOffsets()[12];
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
        Block block6 = page.getBlock(6);
        byte value6IsNull = (byte) (block6.isNull(position) ? 1 : 0);
        entries.putIsNull(entriesPosition, 6, value6IsNull);
        columnValueExtractor6.putValue(buffer, valuesOffset + value6Offset, block6, position);
        Block block7 = page.getBlock(7);
        byte value7IsNull = (byte) (block7.isNull(position) ? 1 : 0);
        entries.putIsNull(entriesPosition, 7, value7IsNull);
        columnValueExtractor7.putValue(buffer, valuesOffset + value7Offset, block7, position);
        Block block8 = page.getBlock(8);
        byte value8IsNull = (byte) (block8.isNull(position) ? 1 : 0);
        entries.putIsNull(entriesPosition, 8, value8IsNull);
        columnValueExtractor8.putValue(buffer, valuesOffset + value8Offset, block8, position);
        Block block9 = page.getBlock(9);
        byte value9IsNull = (byte) (block9.isNull(position) ? 1 : 0);
        entries.putIsNull(entriesPosition, 9, value9IsNull);
        columnValueExtractor9.putValue(buffer, valuesOffset + value9Offset, block9, position);
        Block block10 = page.getBlock(10);
        byte value10IsNull = (byte) (block10.isNull(position) ? 1 : 0);
        entries.putIsNull(entriesPosition, 10, value10IsNull);
        columnValueExtractor10.putValue(buffer, valuesOffset + value10Offset, block10, position);
        Block block11 = page.getBlock(11);
        byte value11IsNull = (byte) (block11.isNull(position) ? 1 : 0);
        entries.putIsNull(entriesPosition, 11, value11IsNull);
        columnValueExtractor11.putValue(buffer, valuesOffset + value11Offset, block11, position);
        Block block12 = page.getBlock(12);
        byte value12IsNull = (byte) (block12.isNull(position) ? 1 : 0);
        entries.putIsNull(entriesPosition, 12, value12IsNull);
        columnValueExtractor12.putValue(buffer, valuesOffset + value12Offset, block12, position);
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
        if (!block5ValueNull && !columnValueExtractor5.valueEquals(buffer, valuesOffset + value5Offset, block5, position)) {
            return false;
        }
        Block block6 = page.getBlock(6);
        boolean block6ValueNull = block6.isNull(position);
        boolean entriesValue6IsNull = entries.isNull(entriesPosition, 6) == 1;
        if (block6ValueNull != entriesValue6IsNull) {
            return false;
        }
        if (!block6ValueNull && !columnValueExtractor6.valueEquals(buffer, valuesOffset + value6Offset, block6, position)) {
            return false;
        }
        Block block7 = page.getBlock(7);
        boolean block7ValueNull = block7.isNull(position);
        boolean entriesValue7IsNull = entries.isNull(entriesPosition, 7) == 1;
        if (block7ValueNull != entriesValue7IsNull) {
            return false;
        }
        if (!block7ValueNull && !columnValueExtractor7.valueEquals(buffer, valuesOffset + value7Offset, block7, position)) {
            return false;
        }
        Block block8 = page.getBlock(8);
        boolean block8ValueNull = block8.isNull(position);
        boolean entriesValue8IsNull = entries.isNull(entriesPosition, 8) == 1;
        if (block8ValueNull != entriesValue8IsNull) {
            return false;
        }
        if (!block8ValueNull && !columnValueExtractor8.valueEquals(buffer, valuesOffset + value8Offset, block8, position)) {
            return false;
        }
        Block block9 = page.getBlock(9);
        boolean block9ValueNull = block9.isNull(position);
        boolean entriesValue9IsNull = entries.isNull(entriesPosition, 9) == 1;
        if (block9ValueNull != entriesValue9IsNull) {
            return false;
        }
        if (!block9ValueNull && !columnValueExtractor9.valueEquals(buffer, valuesOffset + value9Offset, block9, position)) {
            return false;
        }
        Block block10 = page.getBlock(10);
        boolean block10ValueNull = block10.isNull(position);
        boolean entriesValue10IsNull = entries.isNull(entriesPosition, 10) == 1;
        if (block10ValueNull != entriesValue10IsNull) {
            return false;
        }
        if (!block10ValueNull && !columnValueExtractor10.valueEquals(buffer, valuesOffset + value10Offset, block10, position)) {
            return false;
        }
        Block block11 = page.getBlock(11);
        boolean block11ValueNull = block11.isNull(position);
        boolean entriesValue11IsNull = entries.isNull(entriesPosition, 11) == 1;
        if (block11ValueNull != entriesValue11IsNull) {
            return false;
        }
        if (!block11ValueNull && !columnValueExtractor11.valueEquals(buffer, valuesOffset + value11Offset, block11, position)) {
            return false;
        }
        Block block12 = page.getBlock(12);
        boolean block12ValueNull = block12.isNull(position);
        boolean entriesValue12IsNull = entries.isNull(entriesPosition, 12) == 1;
        if (block12ValueNull != entriesValue12IsNull) {
            return false;
        }
        return block12ValueNull || columnValueExtractor12.valueEquals(buffer, valuesOffset + value12Offset, block12, position);
    }
}
