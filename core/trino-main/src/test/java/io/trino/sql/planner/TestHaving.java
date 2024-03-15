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
package io.trino.sql.planner;

import io.airlift.slice.Slices;
import io.trino.sql.ir.GenericLiteral;
import io.trino.sql.planner.assertions.BasePlanTest;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestHaving
        extends BasePlanTest
{
    @Test
    public void testImplicitGroupBy()
    {
        assertPlan(
                "SELECT 'a' FROM (VALUES 1, 1, 2) t(a) HAVING true",
                output(values(List.of("a_symbol"), List.of(List.of(GenericLiteral.constant(createVarcharType(1), Slices.utf8Slice("a")))))));
    }
}
