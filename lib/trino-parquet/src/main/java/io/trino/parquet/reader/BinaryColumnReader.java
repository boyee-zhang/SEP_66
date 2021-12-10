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
package io.trino.parquet.reader;

import io.airlift.slice.Slice;
import io.trino.parquet.RichColumnDescriptor;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.io.api.Binary;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static io.trino.spi.type.Varchars.truncateToLength;

public class BinaryColumnReader
        extends PrimitiveColumnReader
{
    public BinaryColumnReader(RichColumnDescriptor descriptor)
    {
        super(descriptor);
    }

    @Override
    protected void readValue(BlockBuilder blockBuilder, Type type)
    {
        Binary binary = valuesReader.readBytes();
        Slice value;
        if (binary.length() == 0) {
            value = EMPTY_SLICE;
        }
        else {
            value = wrappedBuffer(binary.getBytes());
        }
        if (type instanceof VarcharType) {
            value = truncateToLength(value, type);
        }
        if (type instanceof CharType) {
            value = truncateToLengthAndTrimSpaces(value, type);
        }
        type.writeSlice(blockBuilder, value);
    }

    @Override
    protected void skipValue()
    {
        valuesReader.readBytes();
    }
}
