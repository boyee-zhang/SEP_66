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

package io.prestosql.spi.block;

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSerde;

import java.util.Optional;

import static io.airlift.slice.Slices.wrappedIntArray;
import static io.prestosql.spi.block.AbstractMapBlock.HASH_MULTIPLIER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SingleMapBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "MAP_ELEMENT";

    private final TypeManager typeManager;

    public SingleMapBlockEncoding(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        SingleMapBlock singleMapBlock = (SingleMapBlock) block;
        TypeSerde.writeType(sliceOutput, singleMapBlock.getMapType());

        int offset = singleMapBlock.getOffset();
        int positionCount = singleMapBlock.getPositionCount();
        blockEncodingSerde.writeBlock(sliceOutput, singleMapBlock.getRawKeyBlock().getRegion(offset / 2, positionCount / 2));
        blockEncodingSerde.writeBlock(sliceOutput, singleMapBlock.getRawValueBlock().getRegion(offset / 2, positionCount / 2));
        int[] hashTable = singleMapBlock.getHashTable();

        if (hashTable != null) {
            int hashTableLength = positionCount / 2 * HASH_MULTIPLIER;
            sliceOutput.appendInt(hashTableLength);  // hashtable length
            sliceOutput.writeBytes(wrappedIntArray(hashTable, offset / 2 * HASH_MULTIPLIER, hashTableLength));
        }
        else {
            // if the hashTable is null, we write the length -1
            sliceOutput.appendInt(-1);
        }
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        MapType mapType = (MapType) TypeSerde.readType(typeManager, sliceInput);

        Block keyBlock = blockEncodingSerde.readBlock(sliceInput);
        Block valueBlock = blockEncodingSerde.readBlock(sliceInput);

        int hashTableLength = sliceInput.readInt();
        int[] hashTable = null;
        if (hashTableLength >= 0) {
            hashTable = new int[hashTableLength];
            sliceInput.readBytes(wrappedIntArray(hashTable));
        }

        if (keyBlock.getPositionCount() != valueBlock.getPositionCount()) {
            throw new IllegalArgumentException(
                    format("Deserialized SingleMapBlock violates invariants: key %d, value %d", keyBlock.getPositionCount(), valueBlock.getPositionCount()));
        }

        if (hashTable != null && keyBlock.getPositionCount() * HASH_MULTIPLIER != hashTable.length) {
            throw new IllegalArgumentException(
                    format("Deserialized SingleMapBlock violates invariants: expected hashtable size %d, actual hashtable size %d", keyBlock.getPositionCount() * HASH_MULTIPLIER, hashTable.length));
        }

        MapBlock mapBlock = MapBlock.createMapBlockInternal(
                mapType,
                0,
                1,
                Optional.empty(),
                new int[] {0, keyBlock.getPositionCount()},
                keyBlock,
                valueBlock,
                new AbstractMapBlock.HashTables(Optional.ofNullable(hashTable), 1, hashTableLength));

        return new SingleMapBlock(0, keyBlock.getPositionCount() * 2, mapBlock);
    }
}
