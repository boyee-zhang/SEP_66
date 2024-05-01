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
package io.trino.plugin.iceberg;

import io.trino.Session;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.Table;
import io.trino.sql.tree.ExplainType;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergMaterializedView
        extends BaseIcebergMaterializedViewTest
{
    private Session secondIceberg;
    private HiveMetastore metastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .build();
        try {
            metastore = ((IcebergConnector) queryRunner.getCoordinator().getConnector(ICEBERG_CATALOG)).getInjector()
                    .getInstance(HiveMetastoreFactory.class)
                    .createMetastore(Optional.empty());

            queryRunner.createCatalog("iceberg2", "iceberg", Map.of(
                    "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                    "hive.metastore.catalog.dir", queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg2-catalog").toString(),
                    "iceberg.hive-catalog-name", "hive"));

            secondIceberg = Session.builder(queryRunner.getDefaultSession())
                    .setCatalog("iceberg2")
                    .build();

            queryRunner.createCatalog("iceberg_legacy_mv", "iceberg", Map.of(
                    "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                    "hive.metastore.catalog.dir", queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data").toString(),
                    "iceberg.hive-catalog-name", "hive",
                    "iceberg.materialized-views.hide-storage-table", "false"));

            queryRunner.execute(secondIceberg, "CREATE SCHEMA " + secondIceberg.getSchema().orElseThrow());

            queryRunner.installPlugin(createMockConnectorPlugin());
            queryRunner.createCatalog("mock", "mock");
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
        return queryRunner;
    }

    @Override
    protected String getSchemaDirectory()
    {
        return "local:///tpch";
    }

    @Override
    protected String getStorageMetadataLocation(String materializedViewName)
    {
        Table table = metastore.getTable("tpch", materializedViewName).orElseThrow();
        return table.getParameters().get(METADATA_LOCATION_PROP);
    }

    @Test
    public void testIncrementalRefresh()
    {
        Session defaultSession = getSession();
        Session incrRefreshEnabled = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", "incremental_refresh_enabled", "true")
                .build();

        String matViewDef = "SELECT a, b FROM source_table WHERE a < 3 OR a > 5";

        // create source table and two identical MVs
        assertUpdate("CREATE TABLE source_table (a int, b varchar)");
        assertUpdate("INSERT INTO source_table VALUES (1, 'abc'), (2, 'def')", 2);
        assertUpdate("CREATE MATERIALIZED VIEW mat_view_test_1 AS %s".formatted(matViewDef));
        assertUpdate("CREATE MATERIALIZED VIEW mat_view_test_2 AS %s".formatted(matViewDef));

        // check MV props
        Map<String, String> matViewProps = getStorageTableMetadata("mat_view_test_1").properties();
        assertThat(matViewProps).containsOnlyKeys("write.format.default", "write.parquet.compression-codec");

        // execute first refresh: afterwards both MVs will contain: (1, 'abc'), (2, 'def')
        assertUpdate("REFRESH MATERIALIZED VIEW mat_view_test_1", 2);
        assertUpdate("REFRESH MATERIALIZED VIEW mat_view_test_2", 2);

        // add some new rows to source
        assertUpdate("INSERT INTO source_table VALUES (3, 'ghi'), (4, 'jkl'), (5, 'mno'), (6, 'pqr')", 4);

        // will do incremental refresh, and only add: (6, 'pqr')
        assertUpdate(incrRefreshEnabled, "REFRESH MATERIALIZED VIEW mat_view_test_1", 1);
        // will do full refresh, and (re)add: (1, 'abc'), (2, 'def'), (6, 'pqr')
        assertUpdate(defaultSession, "REFRESH MATERIALIZED VIEW mat_view_test_2", 3);

        // cleanup
        assertUpdate("DROP MATERIALIZED VIEW mat_view_test_1");
        assertUpdate("DROP MATERIALIZED VIEW mat_view_test_2");
        assertUpdate("DROP TABLE source_table");
    }

    @Test
    public void testIncrementalRefreshDisabledForAggregation()
    {
        Session incrRefreshEnabled = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", "incremental_refresh_enabled", "true")
                .build();

        String matViewDef = "SELECT b, sum(a) AS total FROM source_table GROUP BY b";

        // create source table and MV
        assertUpdate("CREATE TABLE source_table (a int, b varchar)");
        assertUpdate("INSERT INTO source_table VALUES (1, 'abc'), (2, 'def')", 2);
        assertUpdate("CREATE MATERIALIZED VIEW mat_view_test_1 AS %s".formatted(matViewDef));

        // check MV props
        Map<String, String> matViewProps = getStorageTableMetadata("mat_view_test_1").properties();
        assertThat(matViewProps).containsExactlyInAnyOrderEntriesOf(Map.of(
                "write.format.default", "PARQUET",
                "write.parquet.compression-codec", "zstd",
                "materialized-view.incremental-refresh.contains-group-by", "true",
                "materialized-view.incremental-refresh.contains-select-function", "true"));

        // execute first refresh: afterwards MV will contain: ('abc', 1), ('def', 2)
        assertUpdate(incrRefreshEnabled, "REFRESH MATERIALIZED VIEW mat_view_test_1", 2);

        // add some new rows to source
        assertUpdate("INSERT INTO source_table VALUES (3, 'ghi'), (4, 'jkl'), (5, 'abc'), (6, 'ghi')", 4);

        // will perform full refresh: ('abc', 6), ('def', 2), ('ghi', 9), ('jkl', 4)
        assertUpdate(incrRefreshEnabled, "REFRESH MATERIALIZED VIEW mat_view_test_1", 4);

        // cleanup
        assertUpdate("DROP MATERIALIZED VIEW mat_view_test_1");
        assertUpdate("DROP TABLE source_table");
    }

    @Test
    public void testIncrementalRefreshDisabledForJoin()
    {
        Session incrRefreshEnabled = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", "incremental_refresh_enabled", "true")
                .build();

        String matViewDef = "SELECT c.custkey, o.price FROM orders o JOIN customer c ON o.custkey = c.custkey";

        // create source table and MV
        assertUpdate("CREATE TABLE orders (orderkey int, custkey int, price int, itemname varchar)");
        assertUpdate("CREATE TABLE customer (custkey int, name varchar)");
        assertUpdate("INSERT INTO orders VALUES (1, 1, 1500, 'Microwave'), (2, 1, 3000, 'Desk lamp'), (3, 2, 4000, 'Flight ticket')", 3);
        assertUpdate("INSERT INTO customer VALUES (1, 'Mike'), (2, 'Lakshmi')", 2);
        assertUpdate("CREATE MATERIALIZED VIEW mat_view_test_1 AS %s".formatted(matViewDef));

        // check MV props
        Map<String, String> matViewProps = getStorageTableMetadata("mat_view_test_1").properties();
        assertThat(matViewProps).containsExactlyInAnyOrderEntriesOf(Map.of(
                "write.format.default", "PARQUET",
                "write.parquet.compression-codec", "zstd",
                "materialized-view.incremental-refresh.contains-join", "true"));

        // execute first refresh: afterwards MV will contain: (1, 1500), (1, 3000), (2, 4000)
        assertUpdate(incrRefreshEnabled, "REFRESH MATERIALIZED VIEW mat_view_test_1", 3);

        // add some new rows to source
        assertUpdate("INSERT INTO orders VALUES (4, 1, 2000, 'Napkins'), (5, 2, 5500, 'Sunscreen')", 2);

        // will perform full refresh: (1, 1500), (1, 3000), (2, 4000), (1, 2000), (2, 5500)
        assertUpdate(incrRefreshEnabled, "REFRESH MATERIALIZED VIEW mat_view_test_1", 5);

        // cleanup
        assertUpdate("DROP MATERIALIZED VIEW mat_view_test_1");
        assertUpdate("DROP TABLE orders");
        assertUpdate("DROP TABLE customer");
    }

    @Test
    public void testTwoIcebergCatalogs()
    {
        Session defaultIceberg = getSession();

        // Base table for staleness check
        String createTable = "CREATE TABLE common_base_table AS SELECT 10 value";
        assertUpdate(secondIceberg, createTable, 1); // this one will be used by MV
        assertUpdate(defaultIceberg, createTable, 1); // this one exists so that it can be mistakenly treated as the base table

        assertUpdate(defaultIceberg, """
                            CREATE MATERIALIZED VIEW iceberg.tpch.mv_on_iceberg2
                            AS SELECT sum(value) AS s FROM iceberg2.tpch.common_base_table
                """);

        // The MV is initially stale
        assertThat(getExplainPlan("TABLE mv_on_iceberg2", ExplainType.Type.IO))
                .contains("\"table\" : \"common_base_table\"");
        assertThat(query("TABLE mv_on_iceberg2"))
                .matches("VALUES BIGINT '10'");

        // After REFRESH, the MV is fresh
        assertUpdate(defaultIceberg, "REFRESH MATERIALIZED VIEW mv_on_iceberg2", 1);
        assertThat(getExplainPlan("TABLE mv_on_iceberg2", ExplainType.Type.IO))
                .contains("\"table\" : \"mv_on_iceberg2$materialized_view_storage")
                .doesNotContain("common_base_table");
        assertThat(query("TABLE mv_on_iceberg2"))
                .matches("VALUES BIGINT '10'");

        // After INSERT to the base table, the MV is still fresh, because it currently does not detect changes to tables in other catalog.
        assertUpdate(secondIceberg, "INSERT INTO common_base_table VALUES 7", 1);
        assertThat(getExplainPlan("TABLE mv_on_iceberg2", ExplainType.Type.IO))
                .contains("\"table\" : \"mv_on_iceberg2$materialized_view_storage")
                .doesNotContain("common_base_table");
        assertThat(query("TABLE mv_on_iceberg2"))
                .matches("VALUES BIGINT '10'");

        // After REFRESH, the MV is fresh again
        assertUpdate(defaultIceberg, "REFRESH MATERIALIZED VIEW mv_on_iceberg2", 1);
        assertThat(getExplainPlan("TABLE mv_on_iceberg2", ExplainType.Type.IO))
                .contains("\"table\" : \"mv_on_iceberg2$materialized_view_storage")
                .doesNotContain("common_base_table");
        assertThat(query("TABLE mv_on_iceberg2"))
                .matches("VALUES BIGINT '17'");

        assertUpdate(secondIceberg, "DROP TABLE common_base_table");
        assertUpdate(defaultIceberg, "DROP TABLE common_base_table");
        assertUpdate("DROP MATERIALIZED VIEW mv_on_iceberg2");
    }
}
