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
import io.trino.plugin.hive.coercions.CoercionUtils.CoercionContext;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.HiveTimestampPrecision.DEFAULT_PRECISION;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.hive.coercions.CoercionUtils.createCoercer;
import static io.trino.spi.predicate.Utils.blockToNativeValue;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestVarbinaryToVarcharCoercer
{
    private static final byte CONTINUATION_BYTE = (byte) 0b1011_1111;
    private static final byte X_CHAR = (byte) 'X';

    // Test cases are copied from https://github.com/airlift/slice/blob/master/src/test/java/io/airlift/slice/TestSliceUtf8.java

    @Test
    public void testVarbinaryToVarcharCoercion()
    {
        assertVarbinaryToVarcharCoercion(Slices.utf8Slice("abc"), VARBINARY, "abc", VARCHAR, false);
        assertVarbinaryToVarcharCoercion(Slices.utf8Slice("abc"), VARBINARY, "ab", createVarcharType(2), false);
        // Invalid UTF-8 encoding
        assertVarbinaryToVarcharCoercion(Slices.wrappedBuffer(X_CHAR, CONTINUATION_BYTE), VARBINARY, "X\uFFFD", VARCHAR, false);
        assertVarbinaryToVarcharCoercion(Slices.wrappedBuffer(X_CHAR, (byte) 0b11101101, (byte) 0xA0, (byte) 0x80), VARBINARY, "X\uFFFD", VARCHAR, false);
        assertVarbinaryToVarcharCoercion(Slices.wrappedBuffer(X_CHAR, (byte) 0b11101101, (byte) 0xBF, (byte) 0xBF), VARBINARY, "X\uFFFD", VARCHAR, false);
    }

    @Test
    public void testVarbinaryToVarcharCoercionForOrc()
    {
        assertVarbinaryToVarcharCoercion(Slices.utf8Slice("abc"), VARBINARY, "61 62 63", VARCHAR, true);
        assertVarbinaryToVarcharCoercion(Slices.utf8Slice("abc"), VARBINARY, "61", createVarcharType(2), true);
        // Invalid UTF-8 encoding
        assertVarbinaryToVarcharCoercion(Slices.wrappedBuffer(X_CHAR, CONTINUATION_BYTE), VARBINARY, "58 bf", VARCHAR, true);
        assertVarbinaryToVarcharCoercion(Slices.wrappedBuffer(X_CHAR, (byte) 0b11101101, (byte) 0xA0, (byte) 0x80), VARBINARY, "58 ed a0 80", VARCHAR, true);
        assertVarbinaryToVarcharCoercion(Slices.wrappedBuffer(X_CHAR, (byte) 0b11101101, (byte) 0xBF, (byte) 0xBF), VARBINARY, "58 ed bf bf", VARCHAR, true);
    }

    private static void assertVarbinaryToVarcharCoercion(Slice actualValue, Type fromType, String expectedValue, Type toType, boolean isOrcFile)
    {
        Block coercedBlock = createCoercer(TESTING_TYPE_MANAGER, toHiveType(fromType), toHiveType(toType), new CoercionContext(DEFAULT_PRECISION, isOrcFile)).orElseThrow()
                .apply(nativeValueToBlock(fromType, actualValue));
        assertThat(blockToNativeValue(toType, coercedBlock))
                .isEqualTo(utf8Slice(expectedValue));
    }
}
