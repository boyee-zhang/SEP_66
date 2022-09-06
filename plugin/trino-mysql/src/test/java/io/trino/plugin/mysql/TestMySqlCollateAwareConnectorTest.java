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
package io.trino.plugin.mysql;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;

import static io.trino.plugin.mysql.MySqlQueryRunner.createMySqlQueryRunner;

public class TestMySqlCollateAwareConnectorTest
        extends TestMySqlConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        mySqlServer = closeAfterClass(new TestingMySqlServer(false));
        return createMySqlQueryRunner(
                mySqlServer,
                ImmutableMap.of(),
                ImmutableMap.of("mysql.experimental.enable-string-pushdown-with-collate", "true"),
                REQUIRED_TPCH_TABLES);
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY, SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY -> true;
            case SUPPORTS_TOPN_PUSHDOWN_WITH_VARCHAR,
                    SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY,
                    SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_INEQUALITY,
                    SUPPORTS_AGGREGATION_PUSHDOWN_WITH_VARCHAR -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }
}
