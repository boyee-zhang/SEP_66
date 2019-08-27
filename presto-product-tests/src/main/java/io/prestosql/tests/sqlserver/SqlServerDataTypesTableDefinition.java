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
package io.prestosql.tests.sqlserver;

import com.google.common.collect.ImmutableList;
import io.prestosql.tempto.fulfillment.table.jdbc.RelationalDataSource;
import io.prestosql.tempto.fulfillment.table.jdbc.RelationalTableDefinition;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;

import static io.prestosql.tests.sqlserver.TestConstants.CONNECTOR_NAME;
import static java.util.Collections.emptyIterator;
import static java.util.Collections.nCopies;

public final class SqlServerDataTypesTableDefinition
{
    private SqlServerDataTypesTableDefinition() {}

    public static final RelationalTableDefinition SQLSERVER_ALL_TYPES;

    public static final RelationalTableDefinition SQLSERVER_INSERT;

    private static final String ALL_TYPES_TABLE_NAME = "all_types";

    private static final String INSERT_TABLE_NAME = "insert_table";

    private static final String ALL_TYPES_DDL =
            "CREATE TABLE %NAME% (" +
                    "bi bigint, " +
                    "si smallint, " +
                    "i int, " +
                    "ti tinyint, " +
                    "f float, " +
                    "r real," +
                    "c char(4), " +
                    "vc varchar(6), " +
                    "te text, " +
                    "nc nchar(5), " +
                    "nvc nvarchar(7), " +
                    "nt text," +
                    "d date, " +
                    "dt datetime, " +
                    "dt2 datetime2, " +
                    "dt3 datetime2, " +
                    "sdt smalldatetime, " +
                    "pf30 float(30), " +
                    "pf22 float(22))";

    private static final String INSERT_DDL =
            "CREATE TABLE %NAME% (bi bigint, si smallint, i int, f float,c char(4), vc varchar(6), pf30 float(30), d date) ";

    static {
        RelationalDataSource dataSource = () -> ImmutableList.<List<Object>>of(
                ImmutableList.of(
                        Long.MIN_VALUE,
                        Short.MIN_VALUE,
                        Integer.MIN_VALUE,
                        Byte.MIN_VALUE,
                        Double.MIN_VALUE,
                        -3.40E+38f,
                        "\0",
                        "\0",
                        "\0",
                        "\0",
                        "\0",
                        "\0",
                        Date.valueOf("1953-01-02"),
                        Timestamp.valueOf("1953-01-01 00:00:00.000"),
                        Timestamp.valueOf("2001-01-01 00:00:00.123"),
                        Timestamp.valueOf("1970-01-01 00:00:00"),
                        Timestamp.valueOf("1960-01-01 00:00:00"),
                        Double.MIN_VALUE,
                        -3.40E+38f),
                ImmutableList.of(
                        Long.MAX_VALUE,
                        Short.MAX_VALUE,
                        Integer.MAX_VALUE,
                        Byte.MAX_VALUE,
                        Double.MAX_VALUE,
                        Float.MAX_VALUE,
                        "abcd",
                        "abcdef",
                        "abcd",
                        "abcde",
                        "abcdefg",
                        "abcd",
                        Date.valueOf("9999-12-31"),
                        Timestamp.valueOf("9999-12-31 23:59:59.997"),
                        Timestamp.valueOf("9999-12-31 23:59:59.999"),
                        Timestamp.valueOf("9999-12-31 23:59:59.999"),
                        Timestamp.valueOf("2079-06-05 23:59:59"),
                        12345678912.3456756,
                        12345678.6557f),
                nCopies(19, null))
                .iterator();

        SQLSERVER_ALL_TYPES = RelationalTableDefinition.builder(ALL_TYPES_TABLE_NAME)
                .withDatabase(CONNECTOR_NAME)
                .setCreateTableDDLTemplate(ALL_TYPES_DDL)
                .setDataSource(dataSource)
                .build();

        SQLSERVER_INSERT = RelationalTableDefinition.builder(INSERT_TABLE_NAME)
                .withDatabase(CONNECTOR_NAME)
                .setCreateTableDDLTemplate(INSERT_DDL)
                .setDataSource(Collections::emptyIterator)
                .build();
    }
}
