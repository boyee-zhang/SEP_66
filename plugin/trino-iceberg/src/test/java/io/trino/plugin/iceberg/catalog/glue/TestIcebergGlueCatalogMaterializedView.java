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
package io.trino.plugin.iceberg.catalog.glue;

import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.AWSGlueAsyncClientBuilder;
import com.amazonaws.services.glue.model.BatchDeleteTableRequest;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.metastore.glue.AwsApiCallStats;
import io.trino.plugin.iceberg.BaseIcebergMaterializedViewTest;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hive.metastore.glue.AwsSdkUtil.getPaginatedResults;
import static io.trino.plugin.hive.metastore.glue.converter.GlueToTrinoConverter.getTableParameters;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestIcebergGlueCatalogMaterializedView
        extends BaseIcebergMaterializedViewTest
{
    private final String schemaName = "test_iceberg_materialized_view_" + randomNameSuffix();

    private File schemaDirectory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.schemaDirectory = Files.createTempDirectory("test_iceberg").toFile();
        schemaDirectory.deleteOnExit();

        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.of(
                                "iceberg.catalog.type", "glue",
                                "hive.metastore.glue.default-warehouse-dir", schemaDirectory.getAbsolutePath()))
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withClonedTpchTables(ImmutableList.of())
                                .withSchemaName(schemaName)
                                .build())
                .build();

        queryRunner.createCatalog("iceberg_legacy_mv", "iceberg", Map.of(
                "iceberg.catalog.type", "glue",
                "hive.metastore.glue.default-warehouse-dir", schemaDirectory.getAbsolutePath(),
                "iceberg.materialized-views.hide-storage-table", "false"));

        return queryRunner;
    }

    @Override
    protected String getSchemaDirectory()
    {
        return new File(schemaDirectory, schemaName + ".db").getPath();
    }

    @Override
    protected String getStorageMetadataLocation(String materializedViewName)
    {
        AWSGlueAsync glueClient = AWSGlueAsyncClientBuilder.defaultClient();
        Table table = glueClient.getTable(new GetTableRequest()
                .withDatabaseName(schemaName)
                .withName(materializedViewName))
                .getTable();
        return getTableParameters(table).get(METADATA_LOCATION_PROP);
    }

    @AfterAll
    public void cleanup()
    {
        cleanUpSchema(schemaName);
    }

    private static void cleanUpSchema(String schema)
    {
        AWSGlueAsync glueClient = AWSGlueAsyncClientBuilder.defaultClient();
        Set<String> tableNames = getPaginatedResults(
                glueClient::getTables,
                new GetTablesRequest().withDatabaseName(schema),
                GetTablesRequest::setNextToken,
                GetTablesResult::getNextToken,
                new AwsApiCallStats())
                .map(GetTablesResult::getTableList)
                .flatMap(Collection::stream)
                .map(Table::getName)
                .collect(toImmutableSet());
        glueClient.batchDeleteTable(new BatchDeleteTableRequest()
                .withDatabaseName(schema)
                .withTablesToDelete(tableNames));
        glueClient.deleteDatabase(new DeleteDatabaseRequest()
                .withName(schema));
    }
}
