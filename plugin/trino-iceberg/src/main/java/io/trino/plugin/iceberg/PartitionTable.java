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

import com.google.common.collect.ImmutableList;
import io.trino.spi.block.Block;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.StructLikeWrapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.iceberg.IcebergUtil.addPartitionColumn;
import static io.trino.plugin.iceberg.IcebergUtil.getIdentityPartitions;
import static io.trino.plugin.iceberg.IcebergUtil.getPartitionColumnType;
import static io.trino.plugin.iceberg.IcebergUtil.primitiveFieldTypes;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Collectors.toUnmodifiableSet;

public class PartitionTable
        implements SystemTable
{
    private final TypeManager typeManager;
    private final Table icebergTable;
    private final Optional<Long> snapshotId;
    private final Map<Integer, Type.PrimitiveType> idToTypeMapping;
    private final List<NestedField> nonPartitionPrimitiveColumns;
    private final Optional<IcebergPartitionColumn> partitionColumnType;
    private final List<PartitionField> partitionFields;
    private final Optional<RowType> dataColumnType;
    private final List<RowType> columnMetricTypes;
    private final List<io.trino.spi.type.Type> resultTypes;
    private final ConnectorTableMetadata connectorTableMetadata;

    public PartitionTable(SchemaTableName tableName, TypeManager typeManager, Table icebergTable, Optional<Long> snapshotId)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");
        this.idToTypeMapping = primitiveFieldTypes(icebergTable.schema());

        List<NestedField> columns = icebergTable.schema().columns();
        this.partitionFields = getAllPartitionFields(icebergTable);

        ImmutableList.Builder<ColumnMetadata> columnMetadataBuilder = ImmutableList.builder();

        this.partitionColumnType = getPartitionColumnType(partitionFields, icebergTable.schema(), typeManager);
        partitionColumnType.ifPresent(icebergPartitionColumn ->
                columnMetadataBuilder.add(new ColumnMetadata("partition", icebergPartitionColumn.rowType())));

        Stream.of("record_count", "file_count", "total_size")
                .forEach(metric -> columnMetadataBuilder.add(new ColumnMetadata(metric, BIGINT)));

        Set<Integer> identityPartitionIds = getIdentityPartitions(icebergTable.spec()).keySet().stream()
                .map(PartitionField::sourceId)
                .collect(toSet());

        this.nonPartitionPrimitiveColumns = columns.stream()
                .filter(column -> !identityPartitionIds.contains(column.fieldId()) && column.type().isPrimitiveType())
                .collect(toImmutableList());

        this.dataColumnType = getMetricsColumnType(this.nonPartitionPrimitiveColumns);
        if (dataColumnType.isPresent()) {
            columnMetadataBuilder.add(new ColumnMetadata("data", dataColumnType.get()));
            this.columnMetricTypes = dataColumnType.get().getFields().stream()
                    .map(RowType.Field::getType)
                    .map(RowType.class::cast)
                    .collect(toImmutableList());
        }
        else {
            this.columnMetricTypes = ImmutableList.of();
        }

        ImmutableList<ColumnMetadata> columnMetadata = columnMetadataBuilder.build();
        this.resultTypes = columnMetadata.stream()
                .map(ColumnMetadata::getType)
                .collect(toImmutableList());
        this.connectorTableMetadata = new ConnectorTableMetadata(tableName, columnMetadata);
    }

    @Override
    public Distribution getDistribution()
    {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return connectorTableMetadata;
    }

    static List<PartitionField> getAllPartitionFields(Table icebergTable)
    {
        Set<Integer> existingColumnsIds = icebergTable.schema()
                .columns().stream()
                .map(NestedField::fieldId)
                .collect(toUnmodifiableSet());

        List<PartitionField> visiblePartitionFields = icebergTable.specs()
                .values().stream()
                .flatMap(partitionSpec -> partitionSpec.fields().stream())
                // skip columns that were dropped
                .filter(partitionField -> existingColumnsIds.contains(partitionField.sourceId()))
                .collect(toImmutableList());

        return filterOutDuplicates(visiblePartitionFields);
    }

    private static List<PartitionField> filterOutDuplicates(List<PartitionField> visiblePartitionFields)
    {
        Set<Integer> alreadyExistingFieldIds = new HashSet<>();
        List<PartitionField> result = new ArrayList<>();
        for (PartitionField partitionField : visiblePartitionFields) {
            if (!alreadyExistingFieldIds.contains(partitionField.fieldId())) {
                alreadyExistingFieldIds.add(partitionField.fieldId());
                result.add(partitionField);
            }
        }
        return result;
    }

    private Optional<RowType> getMetricsColumnType(List<NestedField> columns)
    {
        List<RowType.Field> metricColumns = columns.stream()
                .map(column -> RowType.field(
                        column.name(),
                        RowType.from(ImmutableList.of(
                                new RowType.Field(Optional.of("min"), toTrinoType(column.type(), typeManager)),
                                new RowType.Field(Optional.of("max"), toTrinoType(column.type(), typeManager)),
                                new RowType.Field(Optional.of("null_count"), BIGINT),
                                new RowType.Field(Optional.of("nan_count"), BIGINT)))))
                .collect(toImmutableList());
        if (metricColumns.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(RowType.from(metricColumns));
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        if (snapshotId.isEmpty()) {
            return new InMemoryRecordSet(resultTypes, ImmutableList.of()).cursor();
        }
        TableScan tableScan = icebergTable.newScan()
                .useSnapshot(snapshotId.get())
                .includeColumnStats();
        // TODO make the cursor lazy
        return buildRecordCursor(getStatisticsByPartition(tableScan));
    }

    private Map<StructLikeWrapperWithFieldIdToIndex, IcebergStatistics> getStatisticsByPartition(TableScan tableScan)
    {
        try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
            Map<StructLikeWrapperWithFieldIdToIndex, IcebergStatistics.Builder> partitions = new HashMap<>();
            for (FileScanTask fileScanTask : fileScanTasks) {
                DataFile dataFile = fileScanTask.file();
                Types.StructType structType = fileScanTask.spec().partitionType();
                StructLike partitionStruct = dataFile.partition();
                StructLikeWrapper partitionWrapper = StructLikeWrapper.forType(structType).set(partitionStruct);
                StructLikeWrapperWithFieldIdToIndex structLikeWrapperWithFieldIdToIndex = new StructLikeWrapperWithFieldIdToIndex(partitionWrapper, structType);

                partitions.computeIfAbsent(
                                structLikeWrapperWithFieldIdToIndex,
                                ignored -> new IcebergStatistics.Builder(icebergTable.schema().columns(), typeManager))
                        .acceptDataFile(dataFile, fileScanTask.spec());
            }

            return partitions.entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().build()));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private RecordCursor buildRecordCursor(Map<StructLikeWrapperWithFieldIdToIndex, IcebergStatistics> partitionStatistics)
    {
        List<Type> partitionTypes = partitionTypes();
        List<? extends Class<?>> partitionColumnClass = partitionTypes.stream()
                .map(type -> type.typeId().javaClass())
                .collect(toImmutableList());

        ImmutableList.Builder<List<Object>> records = ImmutableList.builder();

        for (Map.Entry<StructLikeWrapperWithFieldIdToIndex, IcebergStatistics> partitionEntry : partitionStatistics.entrySet()) {
            StructLikeWrapperWithFieldIdToIndex partitionStruct = partitionEntry.getKey();
            IcebergStatistics icebergStatistics = partitionEntry.getValue();
            List<Object> row = new ArrayList<>();

            // add data for partition columns
            addPartitionColumn(partitionColumnType, partitionTypes, partitionColumnClass, partitionStruct, row);

            // add the top level metrics.
            row.add(icebergStatistics.getRecordCount());
            row.add(icebergStatistics.getFileCount());
            row.add(icebergStatistics.getSize());

            // add column level metrics
            dataColumnType.ifPresent(dataColumnType -> {
                try {
                    row.add(buildRowValue(dataColumnType, fields -> {
                        for (int i = 0; i < columnMetricTypes.size(); i++) {
                            Integer fieldId = nonPartitionPrimitiveColumns.get(i).fieldId();
                            Object min = icebergStatistics.getMinValues().get(fieldId);
                            Object max = icebergStatistics.getMaxValues().get(fieldId);
                            Long nullCount = icebergStatistics.getNullCounts().get(fieldId);
                            Long nanCount = icebergStatistics.getNanCounts().get(fieldId);
                            if (min == null && max == null && nullCount == null) {
                                throw new MissingColumnMetricsException();
                            }

                            RowType columnMetricType = columnMetricTypes.get(i);
                            columnMetricType.writeObject(fields.get(i), getColumnMetricBlock(columnMetricType, min, max, nullCount, nanCount));
                        }
                    }));
                }
                catch (MissingColumnMetricsException ignored) {
                    row.add(null);
                }
            });

            records.add(row);
        }

        return new InMemoryRecordSet(resultTypes, records.build()).cursor();
    }

    private static class MissingColumnMetricsException
            extends Exception
    {}

    private List<Type> partitionTypes()
    {
        ImmutableList.Builder<Type> partitionTypeBuilder = ImmutableList.builder();
        for (PartitionField partitionField : partitionFields) {
            Type.PrimitiveType sourceType = idToTypeMapping.get(partitionField.sourceId());
            Type type = partitionField.transform().getResultType(sourceType);
            partitionTypeBuilder.add(type);
        }
        return partitionTypeBuilder.build();
    }

    private static SqlRow getColumnMetricBlock(RowType columnMetricType, Object min, Object max, Long nullCount, Long nanCount)
    {
        return buildRowValue(columnMetricType, fieldBuilders -> {
            List<RowType.Field> fields = columnMetricType.getFields();
            writeNativeValue(fields.get(0).getType(), fieldBuilders.get(0), min);
            writeNativeValue(fields.get(1).getType(), fieldBuilders.get(1), max);
            writeNativeValue(fields.get(2).getType(), fieldBuilders.get(2), nullCount);
            writeNativeValue(fields.get(3).getType(), fieldBuilders.get(3), nanCount);
        });
    }
}
