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
package io.trino.plugin.hudi.coercer;

import io.airlift.slice.Slice;
import io.trino.plugin.hive.coercions.TypeCoercer;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DateType;
import io.trino.spi.type.VarcharType;

import java.time.LocalDate;

import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static java.lang.String.format;

public class DateToVarcharCoercer
        extends TypeCoercer<DateType, VarcharType>
{
    public DateToVarcharCoercer(DateType fromType, VarcharType toType)
    {
        super(fromType, toType);
    }

    @Override
    protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
    {
        long value = fromType.getLong(block, position);
        Slice converted = utf8Slice(LocalDate.ofEpochDay(value).toString());
        if (!toType.isUnbounded() && countCodePoints(converted) > toType.getBoundedLength()) {
            throw new TrinoException(INVALID_ARGUMENTS, format("Varchar representation of %s exceeds %s bounds", value, toType));
        }
        toType.writeSlice(blockBuilder, converted);
    }
}
