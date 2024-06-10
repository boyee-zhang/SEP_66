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
import io.airlift.slice.SliceUtf8;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.util.HexFormat;

import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.Varchars.truncateToLength;

public class VarbinaryToVarcharCoercers
{
    private VarbinaryToVarcharCoercers() {}

    public static TypeCoercer<VarbinaryType, VarcharType> createVarbinaryToVarcharCoercer(VarcharType toType, boolean isOrcFile)
    {
        return isOrcFile ? new OrcVarbinaryToVarcharCoercer(toType) : new VarbinaryToVarcharCoercer(toType);
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
            Slice fixedSlice = SliceUtf8.fixInvalidUtf8(fromType.getSlice(block, position));
            if (toType.isUnbounded()) {
                toType.writeSlice(blockBuilder, fixedSlice);
                return;
            }
            toType.writeSlice(blockBuilder, truncateToLength(fixedSlice, toType.getBoundedLength()));
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
