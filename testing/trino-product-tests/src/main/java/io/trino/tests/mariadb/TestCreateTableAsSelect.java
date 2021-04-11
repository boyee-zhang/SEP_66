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
package io.trino.tests.mariadb;

import io.trino.tempto.AfterTestWithContext;
import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.query.QueryResult;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.query.QueryExecutor.query;
import static io.trino.tests.TestGroups.MARIADB;
import static io.trino.tests.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.utils.QueryExecutors.onMariaDb;
import static java.lang.String.format;

public class TestCreateTableAsSelect
        extends ProductTest
{
    private static final String TABLE_NAME = "test.nation_tmp";

    @BeforeTestWithContext
    @AfterTestWithContext
    public void dropTestTable()
    {
        onMariaDb().executeQuery(format("DROP TABLE IF EXISTS %s", TABLE_NAME));
    }

    @Test(groups = {MARIADB, PROFILE_SPECIFIC_TESTS})
    public void testCreateTableAsSelect()
    {
        QueryResult queryResult = query(format("CREATE TABLE mariadb.%s AS SELECT * FROM tpch.tiny.nation", TABLE_NAME));
        assertThat(queryResult).containsOnly(row(25));
    }
}
