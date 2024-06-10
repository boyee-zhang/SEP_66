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
package io.trino.plugin.deltalake.metastore.file;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableOperations;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;

import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.deltalake.metastore.DeltaLakeTableMetadataScheduler.deltaParameters;
import static io.trino.plugin.hive.metastore.MetastoreUtil.buildInitialPrivilegeSet;
import static java.util.Objects.requireNonNull;

public class DeltaLakeFileMetastoreTableOperations
        extends DeltaLakeTableOperations
{
    private final HiveMetastore metastore;

    public DeltaLakeFileMetastoreTableOperations(ConnectorSession session, HiveMetastore metastore, SchemaTableName schemaTableName)
    {
        super(session, schemaTableName);
        this.metastore = requireNonNull(metastore, "metastore is null");
    }

    @Override
    public void commitToExistingTable(long version, String schemaString, Optional<String> tableComment)
    {
        Table currentTable = metastore.getTable(schemaTableName.getSchemaName(), schemaTableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(schemaTableName));
        Map<String, String> parameters = ImmutableMap.<String, String>builder()
                .putAll(currentTable.getParameters())
                .putAll(deltaParameters(version, schemaString, tableComment))
                .buildKeepingLast();
        Table updatedTable = currentTable.withParameters(parameters);
        metastore.replaceTable(currentTable.getDatabaseName(), currentTable.getTableName(), updatedTable, buildInitialPrivilegeSet(currentTable.getOwner().orElseThrow()));
    }
}
