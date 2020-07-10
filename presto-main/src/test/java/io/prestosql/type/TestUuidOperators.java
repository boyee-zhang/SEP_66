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
package io.prestosql.type;

import io.prestosql.operator.scalar.AbstractTestFunctions;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import org.testng.annotations.Test;

import java.util.UUID;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.function.OperatorType.HASH_CODE;
import static io.prestosql.spi.function.OperatorType.INDETERMINATE;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.SqlVarbinaryTestingUtil.sqlVarbinaryFromHex;
import static io.prestosql.type.UuidOperators.castFromVarcharToUuid;
import static io.prestosql.type.UuidType.UUID;

public class TestUuidOperators
        extends AbstractTestFunctions
{
    @Test
    public void testRandomUuid()
    {
        tryEvaluateWithAll("uuid()", UUID);
    }

    @Test
    public void testVarcharToUUIDCast()
    {
        assertFunction("CAST('00000000-0000-0000-0000-000000000000' AS UUID)", UUID, "00000000-0000-0000-0000-000000000000");
        assertFunction("CAST('12151fd2-7586-11e9-8f9e-2a86e4085a59' AS UUID)", UUID, "12151fd2-7586-11e9-8f9e-2a86e4085a59");
        assertFunction("CAST('300433ad-b0a1-3b53-a977-91cab582458e' AS UUID)", UUID, "300433ad-b0a1-3b53-a977-91cab582458e");
        assertFunction("CAST('d3074e99-de12-4b8c-a2a1-b7faf79faba6' AS UUID)", UUID, "d3074e99-de12-4b8c-a2a1-b7faf79faba6");
        assertFunction("CAST('dfa7eaf8-6a26-5749-8d36-336025df74e8' AS UUID)", UUID, "dfa7eaf8-6a26-5749-8d36-336025df74e8");
        assertFunction("CAST('12151FD2-7586-11E9-8F9E-2A86E4085A59' AS UUID)", UUID, "12151fd2-7586-11e9-8f9e-2a86e4085a59");
        assertInvalidCast("CAST('1-2-3-4-1' AS UUID)", "Invalid UUID string length: 9");
        assertInvalidCast("CAST('12151fd217586211e938f9e42a86e4085a59' AS UUID)", "Cannot cast value to UUID: 12151fd217586211e938f9e42a86e4085a59");
    }

    @Test
    public void testUUIDToVarcharCast()
    {
        assertFunction("CAST(UUID 'd3074e99-de12-4b8c-a2a1-b7faf79faba6' AS VARCHAR)", VARCHAR, "d3074e99-de12-4b8c-a2a1-b7faf79faba6");
        assertFunction("CAST(CAST('d3074e99-de12-4b8c-a2a1-b7faf79faba6' AS UUID) AS VARCHAR)", VARCHAR, "d3074e99-de12-4b8c-a2a1-b7faf79faba6");
    }

    @Test
    public void testVarbinaryToUUIDCast()
    {
        assertFunction("CAST(x'00000000000000000000000000000000' AS UUID)", UUID, "00000000-0000-0000-0000-000000000000");
        assertFunction("CAST(x'E9118675D21F1512595A08E4862A9E8F' AS UUID)", UUID, "12151fd2-7586-11e9-8f9e-2a86e4085a59");
        assertFunction("CAST(x'533BA1B0AD3304308E4582B5CA9177A9' AS UUID)", UUID, "300433ad-b0a1-3b53-a977-91cab582458e");
        assertFunction("CAST(x'8C4B12DE994E07D3A6AB9FF7FAB7A1A2' AS UUID)", UUID, "d3074e99-de12-4b8c-a2a1-b7faf79faba6");
        assertFunction("CAST(x'4957266AF8EAA7DFE874DF256033368D' AS UUID)", UUID, "dfa7eaf8-6a26-5749-8d36-336025df74e8");
        assertFunction("CAST(x'e9118675d21f1512595a08e4862a9e8f' AS UUID)", UUID, "12151fd2-7586-11e9-8f9e-2a86e4085a59");
        assertInvalidCast("CAST(x'f000001100' AS UUID)", "Invalid UUID binary length: 5");
    }

    @Test
    public void testUUIDToVarbinaryCast()
    {
        assertFunction("CAST(UUID '00000000-0000-0000-0000-000000000000' AS VARBINARY)", VARBINARY, sqlVarbinaryFromHex("00000000000000000000000000000000"));
        assertFunction("CAST(UUID '6b5f5b65-67e4-43b0-8ee3-586cd49f58a0' AS VARBINARY)", VARBINARY, sqlVarbinaryFromHex("B043E467655B5F6BA0589FD46C58E38E"));
    }

    @Test
    public void testEquals()
    {
        assertFunction("UUID '6b5f5b65-67e4-43b0-8ee3-586cd49f58a0' = UUID '6b5f5b65-67e4-43b0-8ee3-586cd49f58a0'", BOOLEAN, true);
        assertFunction("CAST('6b5f5b65-67e4-43b0-8ee3-586cd49f58a0' AS UUID) = CAST('6b5f5b65-67e4-43b0-8ee3-586cd49f58a0' AS UUID)", BOOLEAN, true);
    }

    @Test
    public void testDistinctFrom()
    {
        assertFunction("UUID '6b5f5b65-67e4-43b0-8ee3-586cd49f58a0' IS DISTINCT FROM UUID '6b5f5b65-67e4-43b0-8ee3-586cd49f58a0'", BOOLEAN, false);
        assertFunction("CAST(NULL AS UUID) IS DISTINCT FROM CAST(NULL AS UUID)", BOOLEAN, false);
        assertFunction("UUID '6b5f5b65-67e4-43b0-8ee3-586cd49f58a0' IS DISTINCT FROM UUID '6b5f5b65-67e4-43b0-8ee3-586cd49f58a1'", BOOLEAN, true);
        assertFunction("UUID '6b5f5b65-67e4-43b0-8ee3-586cd49f58a0' IS DISTINCT FROM CAST(NULL AS UUID)", BOOLEAN, true);
        assertFunction("CAST(NULL AS UUID) IS DISTINCT FROM UUID '6b5f5b65-67e4-43b0-8ee3-586cd49f58a0'", BOOLEAN, true);
    }

    @Test
    public void testNotEquals()
    {
        assertFunction("UUID '6b5f5b65-67e4-43b0-8ee3-586cd49f58a0' != UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'", BOOLEAN, true);
        assertFunction("CAST('12151fd2-7586-11e9-8f9e-2a86e4085a59' AS UUID) != UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'", BOOLEAN, false);
    }

    @Test
    public void testOrderOperators()
    {
        assertFunction("CAST('12151fd2-7586-11e9-8f9e-2a86e4085a58' AS UUID) < CAST('12151fd2-7586-11e9-8f9e-2a86e4085a59' AS UUID)", BOOLEAN, true);
        assertFunction("CAST('12151fd2-7586-11e9-8f9e-2a86e4085a59' AS UUID) < CAST('12151fd2-7586-11e9-8f9e-2a86e4085a58' AS UUID)", BOOLEAN, false);

        assertFunction("UUID '12151fd2-7586-11e9-8f9e-2a86e4085a52' BETWEEN UUID '12151fd2-7586-11e9-8f9e-2a86e4085a50' AND UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'", BOOLEAN, true);
        assertFunction("UUID '12151fd2-7586-11e9-8f9e-2a86e4085a52' BETWEEN UUID '12151fd2-7586-11e9-8f9e-2a86e4085a54' AND UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'", BOOLEAN, false);
    }

    @Test
    public void testIndeterminate()
    {
        assertOperator(INDETERMINATE, "CAST(null AS UUID)", BOOLEAN, true);
        assertOperator(INDETERMINATE, "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'", BOOLEAN, false);
    }

    @Test
    public void testHash()
    {
        assertOperator(HASH_CODE, "CAST(null AS UUID)", BIGINT, null);
        assertOperator(HASH_CODE, "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'", BIGINT, hashFromType("12151fd2-7586-11e9-8f9e-2a86e4085a59"));
    }

    private static long hashFromType(String uuidString)
    {
        BlockBuilder blockBuilder = UUID.createBlockBuilder(null, 1);
        UUID.writeSlice(blockBuilder, castFromVarcharToUuid(utf8Slice(uuidString)));
        Block block = blockBuilder.build();
        return UUID.hash(block, 0);
    }

    @Test
    public void testUuid_nil()
    {
        assertFunction("UUID_NIL()", UUID, "00000000-0000-0000-0000-000000000000");
    }

    @Test
    public void testUuid_ns_dns()
    {
        assertFunction("UUID_NS_DNS()", UUID, "6ba7b810-9dad-11d1-80b4-00c04fd430c8");
    }

    @Test
    public void testUuid_ns_url()
    {
        assertFunction("UUID_NS_URL()", UUID, "6ba7b811-9dad-11d1-80b4-00c04fd430c8");
    }

    @Test
    public void testUuid_ns_oid()
    {
        assertFunction("UUID_NS_OID()", UUID, "6ba7b812-9dad-11d1-80b4-00c04fd430c8");
    }

    @Test
    public void testUuid_ns_x500()
    {
        assertFunction("UUID_NS_X500()", UUID, "6ba7b814-9dad-11d1-80b4-00c04fd430c8");
    }

    @Test
    public void testUuid_v3()
    {
        assertFunction("UUID_V3(uuid_ns_dns(),'presto')", UUID, "8efeec8c-1d77-3a5a-b1bb-22e702970750");
        assertFunction("UUID_V3(uuid_ns_url(),'presto')", UUID, "f6c28578-3dcd-35b1-8f2a-8e0f96ed5f8e");
        assertFunction("UUID_V3(uuid_ns_oid(),'presto')", UUID, "eacef17c-d8de-3572-9f0d-2a1d690c5f91");
        assertFunction("UUID_V3(uuid_ns_x500(),'presto')", UUID, "4780824d-7b12-3bd6-81d3-7522dbdb5e64");
        assertFunction("UUID_V3(uuid_ns_dns(),'1')", UUID, "afd0b036-625a-3aa8-b639-9dc8c8fff0ff");
        assertFunction("UUID_V3(uuid_ns_url(),'A')", UUID, "960f8dd3-5e9d-3131-8863-342136e8ba2f");
        assertFunction("UUID_V3(uuid_ns_oid(),'')", UUID, "596b79dc-00dd-3991-a72f-d3696c38c64f");
    }

    @Test
    public void testUuid_v5()
    {
        assertFunction("UUID_V5(uuid_ns_dns(),'presto')", UUID, "85564ce8-4e26-5c3e-8e74-7513d079234b");
        assertFunction("UUID_V5(uuid_ns_url(),'presto')", UUID, "7086ed1d-3281-5a5b-9c45-af1d892d46bc");
        assertFunction("UUID_V5(uuid_ns_oid(),'presto')", UUID, "c5d36fda-4c92-58b9-be71-af90ac45b631");
        assertFunction("UUID_V5(uuid_ns_x500(),'presto')", UUID, "ddbffa49-375f-5810-aa87-5bdcb1d0e370");
        assertFunction("UUID_V5(uuid_ns_dns(),'1')", UUID, "b04965e6-a9bb-591f-8f8a-1adcb2c8dc39");
        assertFunction("UUID_V5(uuid_ns_url(),'A')", UUID, "e4a949f3-70ea-501d-afab-a776e00be584");
        assertFunction("UUID_V5(uuid_ns_oid(),'')", UUID, "0a68eb57-c88a-5f34-9e9d-27f85e68af4f");
    }
}
