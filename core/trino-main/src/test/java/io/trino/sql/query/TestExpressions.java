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
package io.trino.sql.query;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestExpressions
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testBooleanExpressionInCase()
    {
        assertThat(assertions.query("VALUES CASE 1 IS NULL WHEN true THEN 10 ELSE 20 END")).matches("VALUES 20");
        assertThat(assertions.query("VALUES CASE 1 IS NOT NULL WHEN true THEN 10 ELSE 20 END")).matches("VALUES 10");
        assertThat(assertions.query("VALUES CASE 1 BETWEEN 0 AND 2 WHEN true THEN 10 ELSE 20 END")).matches("VALUES 10");
        assertThat(assertions.query("VALUES CASE 1 NOT BETWEEN 0 AND 2 WHEN true THEN 10 ELSE 20 END")).matches("VALUES 20");
        assertThat(assertions.query("VALUES CASE 1 IN (1, 2) WHEN true THEN 10 ELSE 20 END")).matches("VALUES 10");
        assertThat(assertions.query("VALUES CASE 1 NOT IN (1, 2) WHEN true THEN 10 ELSE 20 END")).matches("VALUES 20");
        assertThat(assertions.query("VALUES CASE 1 = 1 WHEN true THEN 10 ELSE 20 END")).matches("VALUES 10");
        assertThat(assertions.query("VALUES CASE 1 = 2 WHEN true THEN 10 ELSE 20 END")).matches("VALUES 20");
        assertThat(assertions.query("VALUES CASE 1 < 2 WHEN true THEN 10 ELSE 20 END")).matches("VALUES 10");
        assertThat(assertions.query("VALUES CASE 1 > 2 WHEN true THEN 10 ELSE 20 END")).matches("VALUES 20");
    }

    @Test
    public void testInShortCircuit()
    {
        assertThat(assertions.query("SELECT IF(3 IN (2, 4, 3, 5 / 0), 1e0, x + x) FROM (VALUES rand()) t(x)"))
                .failure().hasMessage("Division by zero");

        // the in-predicate is inlined into Values and evaluated by the ExpressionInterpreter: eager evaluation, failure.
        assertThat(assertions.query("SELECT 3 IN (2, 4, 3, 5 / 0)"))
                .failure().hasMessage("Division by zero");
    }

    @Test
    public void testInlineNullBind()
    {
        // https://github.com/trinodb/trino/issues/3411
        assertThat(assertions.query("SELECT try(k) FROM (SELECT null) t(k)")).matches("VALUES null");
    }
}
