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
package io.trino.plugin.hive.coercions;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.HexFormat;

import static io.trino.hive.formats.HiveClassNames.ORC_SERDE_CLASS;
import static io.trino.hive.formats.HiveClassNames.PARQUET_HIVE_SERDE_CLASS;
import static io.trino.plugin.hive.HiveStorageFormat.ORC;
import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.Varchars.truncateToLength;
import static java.nio.charset.StandardCharsets.UTF_8;

public class VarbinaryToVarcharCoercers
{
    private VarbinaryToVarcharCoercers() {}

    public static TypeCoercer<VarbinaryType, VarcharType> createVarbinaryToVarcharCoercer(VarcharType toType, HiveStorageFormat storageFormat)
    {
        if (storageFormat == ORC) {
            return new OrcVarbinaryToVarcharCoercer(toType);
        }
        if (storageFormat == PARQUET) {
            return new ParquetVarbinaryToVarcharCoercer(toType);
        }
        return new VarbinaryToVarcharCoercer(toType);
    }

    public static class VarbinaryToVarcharCoercer
            extends TypeCoercer<VarbinaryType, VarcharType>
    {
        public VarbinaryToVarcharCoercer(VarcharType toType)
        {
            super(VARBINARY, toType);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            try {
                Slice decodedValue = fromType.getSlice(block, position);
                if (toType.isUnbounded()) {
                    toType.writeSlice(blockBuilder, decodedValue);
                    return;
                }
                toType.writeSlice(blockBuilder, truncateToLength(decodedValue, toType.getBoundedLength()));
            }
            catch (RuntimeException e) {
                blockBuilder.appendNull();
            }
        }
    }

    public static class ParquetVarbinaryToVarcharCoercer
            extends TypeCoercer<VarbinaryType, VarcharType>
    {
        public ParquetVarbinaryToVarcharCoercer(VarcharType toType)
        {
            super(VARBINARY, toType);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            // Hive's coercion logic for Varbinary to Varchar
            // https://github.com/apache/hive/blob/8190d2be7b7165effa62bd21b7d60ef81fb0e4af/serde/src/java/org/apache/hadoop/hive/serde2/objectinspector/primitive/PrimitiveObjectInspectorUtils.java#L911
            // It uses Hadoop's Text#decode replaces malformed input with a substitution character i.e U+FFFD
            // https://github.com/apache/hadoop/blob/706d88266abcee09ed78fbaa0ad5f74d818ab0e9/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/Text.java#L414
            CharsetDecoder decoder = UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPLACE)
                    .onUnmappableCharacter(CodingErrorAction.REPLACE);

            try {
                Slice decodedValue = Slices.utf8Slice(decoder.decode(fromType.getSlice(block, position).toByteBuffer()).toString());
                if (toType.isUnbounded()) {
                    toType.writeSlice(blockBuilder, decodedValue);
                    return;
                }
                toType.writeSlice(blockBuilder, truncateToLength(decodedValue, toType.getBoundedLength()));
            }
            catch (CharacterCodingException e) {
                blockBuilder.appendNull();
            }
        }
    }

    public static class OrcVarbinaryToVarcharCoercer
            extends TypeCoercer<VarbinaryType, VarcharType>
    {
        private static final HexFormat HEX_FORMAT = HexFormat.of().withDelimiter(" ");

        public OrcVarbinaryToVarcharCoercer(VarcharType toType)
        {
            super(VARBINARY, toType);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            Slice value = fromType.getSlice(block, position);
            Slice hexValue = Slices.utf8Slice(HEX_FORMAT.formatHex(value.byteArray(), value.byteArrayOffset(), value.length()));
            if (toType.isUnbounded()) {
                toType.writeSlice(blockBuilder, hexValue);
                return;
            }
            toType.writeSlice(blockBuilder, truncateToLength(hexValue, toType.getBoundedLength()));
        }
    }
}
