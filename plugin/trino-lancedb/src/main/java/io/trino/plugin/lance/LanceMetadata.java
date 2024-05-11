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
package io.trino.plugin.lance;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.plugin.lance.internal.LanceReader;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.expression.ConnectorExpression;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.function.UnaryOperator.identity;

public class LanceMetadata
        implements ConnectorMetadata
{
    public static final String SCHEMA_NAME = "default";
    private final LanceReader lanceReader;
    private final LanceConfig lanceConfig;

    @Inject
    public LanceMetadata(LanceReader lanceReader, LanceConfig lanceConfig)
    {
        this.lanceReader = requireNonNull(lanceReader, "lanceClient is null");
        Function<String, String> identifierQuote = identity();
        this.lanceConfig = lanceConfig;
    }

    public static List<ColumnMetadata> getColumnsMetadata(LanceReader lanceReader, String tableName)
    {
        Map<String, ColumnHandle> columnHandlers = lanceReader.getColumnHandle(tableName);
        return columnHandlers.values().stream().map(c -> ((LanceColumnHandle) c).getColumnMetadata())
                .collect(toImmutableList());
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(SCHEMA_NAME);
    }

    @Override
    public LanceTableHandle getTableHandle(ConnectorSession session, SchemaTableName name,
            Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        return new LanceTableHandle(name.getSchemaName(), name.getTableName(), lanceReader.getTablePath(name).toUri().toString());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        LanceTableHandle lanceTableHandle = (LanceTableHandle) table;
        try {
            List<ColumnMetadata> columnsMetadata = getColumnsMetadata(lanceReader, ((LanceTableHandle) table).getTableName());
            SchemaTableName schemaTableName =
                    new SchemaTableName(lanceTableHandle.getSchemaName(), lanceTableHandle.getTableName());
            return new ConnectorTableMetadata(schemaTableName, columnsMetadata);
        }
        catch (Exception e) {
            return null;
        }
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaNameOrNull)
    {
        return lanceReader.listTables(session, schemaNameOrNull.isPresent() ? schemaNameOrNull.get() : SCHEMA_NAME);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        LanceTableHandle lanceTableHandle = (LanceTableHandle) tableHandle;
        try {
            return lanceReader.getColumnHandle(lanceTableHandle.getTableName());
        }
        catch (Exception e) {
            throw new TableNotFoundException(new SchemaTableName(lanceTableHandle.getSchemaName(), lanceTableHandle.getTableName()));
        }
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session,
            SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : lanceReader.listTables(session, prefix.toString())) {
            ConnectorTableMetadata tableMetadata =
                    new ConnectorTableMetadata(tableName, getColumnsMetadata(lanceReader, tableName.getTableName()));
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle)
    {
        return ((LanceColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(ConnectorSession session,
            ConnectorTableHandle handle, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        throw new UnsupportedOperationException("unsupported");
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session,
            ConnectorTableHandle table, long limit)
    {
        // TODO: support limit
        throw new UnsupportedOperationException("unsupported");
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session,
            ConnectorTableHandle table, Constraint constraint)
    {
        // TODO: support limit
        throw new UnsupportedOperationException("unsupported");
    }

    @VisibleForTesting
    public LanceConfig getLanceConfig()
    {
        return lanceConfig;
    }

    @VisibleForTesting
    public LanceReader getLanceReader()
    {
        return lanceReader;
    }
}
