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
package io.trino.tests.product.hive;

import com.google.common.collect.ImmutableMap;
import io.trino.tempto.Requirement;
import io.trino.tempto.RequirementsProvider;
import io.trino.tempto.Requires;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.fulfillment.table.MutableTableRequirement;
import io.trino.tempto.fulfillment.table.hive.HiveTableDefinition;
import org.testng.annotations.Test;

import java.util.Map;

import static io.trino.tempto.Requirements.compose;
import static io.trino.tempto.fulfillment.table.MutableTableRequirement.State.CREATED;
import static io.trino.tests.product.TestGroups.JDBC;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class TestHiveCoercionOnUnpartitionedTable
        extends BaseTestHiveCoercion
{
    public static final HiveTableDefinition HIVE_COERCION_ORC = tableDefinitionBuilder("ORC")
            .setNoData()
            .build();

    public static final HiveTableDefinition HIVE_TIMESTAMP_COERCION_ORC = tableDefinitionForTimestampCoercionBuilder("ORC")
            .setNoData()
            .build();

    private static HiveTableDefinition.HiveTableDefinitionBuilder tableDefinitionBuilder(String fileFormat)
    {
        String tableName = format("%s_hive_coercion_unpartitioned", fileFormat.toLowerCase(ENGLISH));
        return HiveTableDefinition.builder(tableName)
                // all nested primitive coercions and adding/removing trailing nested fields are covered across row_to_row, list_to_list, and map_to_map
                .setCreateTableDDLTemplate("""
                        CREATE TABLE %NAME%(
                            row_to_row                         STRUCT<keep: STRING, ti2si: TINYINT, si2int: SMALLINT, int2bi: INT, bi2vc: BIGINT, lower2uppercase: BIGINT>,
                            list_to_list                       ARRAY<STRUCT<ti2int: TINYINT, si2bi: SMALLINT, bi2vc: BIGINT, remove: STRING>>,
                            map_to_map                         MAP<TINYINT, STRUCT<ti2bi: TINYINT, int2bi: INT, float2double: FLOAT>>,
                            boolean_to_varchar                 BOOLEAN,
                            tinyint_to_smallint                TINYINT,
                            tinyint_to_int                     TINYINT,
                            tinyint_to_bigint                  TINYINT,
                            tinyint_to_double                  TINYINT,
                            tinyint_to_shortdecimal            TINYINT,
                            tinyint_to_longdecimal             TINYINT,
                            smallint_to_int                    SMALLINT,
                            smallint_to_bigint                 SMALLINT,
                            smallint_to_double                 SMALLINT,
                            smallint_to_shortdecimal           SMALLINT,
                            smallint_to_longdecimal            SMALLINT,
                            int_to_bigint                      INT,
                            int_to_double                      INT,
                            int_to_shortdecimal                INT,
                            int_to_longdecimal                 INT,
                            bigint_to_double                   BIGINT,
                            bigint_to_varchar                  BIGINT,
                            bigint_to_shortdecimal             BIGINT,
                            bigint_to_longdecimal              BIGINT,
                            float_to_double                    FLOAT,
                            double_to_float                    DOUBLE,
                            double_to_string                   DOUBLE,
                            double_to_bounded_varchar          DOUBLE,
                            double_infinity_to_string          DOUBLE,
                            shortdecimal_to_shortdecimal       DECIMAL(10,2),
                            shortdecimal_to_longdecimal        DECIMAL(10,2),
                            longdecimal_to_shortdecimal        DECIMAL(20,12),
                            longdecimal_to_longdecimal         DECIMAL(20,12),
                            longdecimal_to_tinyint             DECIMAL(20,12),
                            shortdecimal_to_tinyint            DECIMAL(10,2),
                            longdecimal_to_smallint            DECIMAL(20,12),
                            shortdecimal_to_smallint           DECIMAL(10,2),
                            too_big_shortdecimal_to_smallint   DECIMAL(10,2),
                            longdecimal_to_int                 DECIMAL(20,12),
                            shortdecimal_to_int                DECIMAL(10,2),
                            shortdecimal_with_0_scale_to_int   DECIMAL(10,0),
                            longdecimal_to_bigint              DECIMAL(20,4),
                            shortdecimal_to_bigint             DECIMAL(10,2),
                            float_to_decimal                   FLOAT,
                            double_to_decimal                  DOUBLE,
                            decimal_to_float                   DECIMAL(10,5),
                            decimal_to_double                  DECIMAL(10,5),
                            short_decimal_to_varchar           DECIMAL(10,5),
                            long_decimal_to_varchar            DECIMAL(20,12),
                            short_decimal_to_bounded_varchar   DECIMAL(10,5),
                            long_decimal_to_bounded_varchar    DECIMAL(20,12),
                            varchar_to_bigger_varchar          VARCHAR(3),
                            varchar_to_smaller_varchar         VARCHAR(3),
                            varchar_to_date                    VARCHAR(10),
                            varchar_to_distant_date            VARCHAR(12),
                            varchar_to_float                   VARCHAR(40),
                            string_to_float                    STRING,
                            varchar_to_float_infinity          VARCHAR(40),
                            varchar_to_special_float           VARCHAR(40),
                            varchar_to_double                  VARCHAR(40),
                            string_to_double                   STRING,
                            varchar_to_double_infinity         VARCHAR(40),
                            varchar_to_special_double          VARCHAR(40),
                            date_to_string                     DATE,
                            date_to_bounded_varchar            DATE,
                            char_to_bigger_char                CHAR(3),
                            char_to_smaller_char               CHAR(3),
                            timestamp_millis_to_date           TIMESTAMP,
                            timestamp_micros_to_date           TIMESTAMP,
                            timestamp_nanos_to_date            TIMESTAMP,
                            timestamp_to_string                TIMESTAMP,
                            timestamp_to_bounded_varchar       TIMESTAMP,
                            timestamp_to_smaller_varchar       TIMESTAMP,
                            smaller_varchar_to_timestamp       VARCHAR(4),
                            varchar_to_timestamp               STRING,
                            id                                 BIGINT)
                       STORED AS\s""" + fileFormat);
    }

    private static HiveTableDefinition.HiveTableDefinitionBuilder tableDefinitionForTimestampCoercionBuilder(String fileFormat)
    {
        String tableName = format("%s_hive_timestamp_coercion_unpartitioned", fileFormat.toLowerCase(ENGLISH));
        return HiveTableDefinition.builder(tableName)
                .setCreateTableDDLTemplate("""
                         CREATE TABLE %NAME%(
                             timestamp_row_to_row       STRUCT<keep: TIMESTAMP, si2i: SMALLINT, timestamp2string: TIMESTAMP, string2timestamp: STRING, timestamp2date: TIMESTAMP>,
                             timestamp_list_to_list     ARRAY<STRUCT<keep: TIMESTAMP, si2i: SMALLINT, timestamp2string: TIMESTAMP, string2timestamp: STRING, timestamp2date: TIMESTAMP>>,
                             timestamp_map_to_map       MAP<SMALLINT, STRUCT<keep: TIMESTAMP, si2i: SMALLINT, timestamp2string: TIMESTAMP, string2timestamp: STRING, timestamp2date: TIMESTAMP>>,
                             timestamp_to_string        TIMESTAMP,
                             string_to_timestamp        STRING,
                             timestamp_to_date          TIMESTAMP,
                             id                         BIGINT)
                        STORED AS\s""" + fileFormat);
    }

    public static final class OrcRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return compose(
                    MutableTableRequirement.builder(HIVE_COERCION_ORC).withState(CREATED).build(),
                    MutableTableRequirement.builder(HIVE_TIMESTAMP_COERCION_ORC).withState(CREATED).build());
        }
    }

    @Requires(OrcRequirements.class)
    @Test(groups = JDBC)
    public void testHiveCoercionOrc()
    {
        doTestHiveCoercion(HIVE_COERCION_ORC);
    }

    @Requires(OrcRequirements.class)
    @Test(groups = JDBC)
    public void testHiveCoercionWithDifferentTimestampPrecision()
    {
        doTestHiveCoercionWithDifferentTimestampPrecision(HIVE_TIMESTAMP_COERCION_ORC);
    }

    @Override
    protected Map<ColumnContext, String> expectedExceptionsWithTrinoContext()
    {
        // TODO: These expected failures should be fixed.
        return ImmutableMap.<ColumnContext, String>builder()
                // ORC
                .put(columnContext("orc", "row_to_row"), "Cannot read SQL type 'smallint' from ORC stream '.row_to_row.ti2si' of type BYTE")
                .put(columnContext("orc", "list_to_list"), "Cannot read SQL type 'integer' from ORC stream '.list_to_list.item.ti2int' of type BYTE")
                .put(columnContext("orc", "map_to_map"), "Cannot read SQL type 'integer' from ORC stream '.map_to_map.key' of type BYTE")
                .put(columnContext("orc", "tinyint_to_smallint"), "Cannot read SQL type 'smallint' from ORC stream '.tinyint_to_smallint' of type BYTE")
                .put(columnContext("orc", "tinyint_to_int"), "Cannot read SQL type 'integer' from ORC stream '.tinyint_to_int' of type BYTE")
                .put(columnContext("orc", "tinyint_to_bigint"), "Cannot read SQL type 'bigint' from ORC stream '.tinyint_to_bigint' of type BYTE")
                .put(columnContext("orc", "bigint_to_varchar"), "Cannot read SQL type 'varchar' from ORC stream '.bigint_to_varchar' of type LONG")
                .put(columnContext("orc", "double_to_float"), "Cannot read SQL type 'real' from ORC stream '.double_to_float' of type DOUBLE")
                .put(columnContext("orc", "longdecimal_to_shortdecimal"), "Decimal does not fit long (invalid table schema?)")
                .put(columnContext("orc", "float_to_decimal"), "Cannot read SQL type 'decimal(10,5)' from ORC stream '.float_to_decimal' of type FLOAT")
                .put(columnContext("orc", "double_to_decimal"), "Cannot read SQL type 'decimal(10,5)' from ORC stream '.double_to_decimal' of type DOUBLE")
                .put(columnContext("orc", "decimal_to_float"), "Cannot read SQL type 'real' from ORC stream '.decimal_to_float' of type DECIMAL")
                .put(columnContext("orc", "decimal_to_double"), "Cannot read SQL type 'double' from ORC stream '.decimal_to_double' of type DECIMAL")
                .put(columnContext("orc", "short_decimal_to_varchar"), "Cannot read SQL type 'varchar' from ORC stream '.short_decimal_to_varchar' of type DECIMAL")
                .put(columnContext("orc", "long_decimal_to_varchar"), "Cannot read SQL type 'varchar' from ORC stream '.long_decimal_to_varchar' of type DECIMAL")
                .put(columnContext("orc", "longdecimal_to_tinyint"), "Cannot read SQL type 'tinyint' from ORC stream '.longdecimal_to_tinyint' of type DECIMAL")
                .put(columnContext("orc", "shortdecimal_to_tinyint"), "Cannot read SQL type 'tinyint' from ORC stream '.shortdecimal_to_tinyint' of type DECIMAL")
                .put(columnContext("orc", "longdecimal_to_smallint"), "Cannot read SQL type 'smallint' from ORC stream '.longdecimal_to_smallint' of type DECIMAL")
                .put(columnContext("orc", "shortdecimal_to_smallint"), "Cannot read SQL type 'smallint' from ORC stream '.shortdecimal_to_smallint' of type DECIMAL")
                .put(columnContext("orc", "too_big_shortdecimal_to_smallint"), "Cannot read SQL type 'smallint' from ORC stream '.too_big_shortdecimal_to_smallint' of type DECIMAL")
                .put(columnContext("orc", "longdecimal_to_int"), "Cannot read SQL type 'integer' from ORC stream '.longdecimal_to_int' of type DECIMAL")
                .put(columnContext("orc", "shortdecimal_to_int"), "Cannot read SQL type 'integer' from ORC stream '.shortdecimal_to_int' of type DECIMAL")
                .put(columnContext("orc", "shortdecimal_with_0_scale_to_int"), "Cannot read SQL type 'integer' from ORC stream '.shortdecimal_with_0_scale_to_int' of type DECIMAL")
                .put(columnContext("orc", "longdecimal_to_bigint"), "Cannot read SQL type 'bigint' from ORC stream '.longdecimal_to_bigint' of type DECIMAL")
                .put(columnContext("orc", "shortdecimal_to_bigint"), "Cannot read SQL type 'bigint' from ORC stream '.shortdecimal_to_bigint' of type DECIMAL")
                .put(columnContext("orc", "short_decimal_to_bounded_varchar"), "Cannot read SQL type 'varchar(30)' from ORC stream '.short_decimal_to_bounded_varchar' of type DECIMAL")
                .put(columnContext("orc", "long_decimal_to_bounded_varchar"), "Cannot read SQL type 'varchar(30)' from ORC stream '.long_decimal_to_bounded_varchar' of type DECIMAL")
                .put(columnContext("orc", "timestamp_row_to_row"), "Cannot read SQL type 'varchar' from ORC stream '.timestamp_row_to_row.timestamp2string' of type TIMESTAMP with attributes {}")
                .put(columnContext("orc", "timestamp_list_to_list"), "Cannot read SQL type 'varchar' from ORC stream '.timestamp_row_to_row.timestamp2string' of type TIMESTAMP with attributes {}")
                .put(columnContext("orc", "timestamp_map_to_map"), "Cannot read SQL type 'varchar' from ORC stream '.timestamp_row_to_row.timestamp2string' of type TIMESTAMP with attributes {}")
                .buildOrThrow();
    }
}
