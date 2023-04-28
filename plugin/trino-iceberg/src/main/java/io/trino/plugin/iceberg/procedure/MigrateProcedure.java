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
package io.trino.plugin.iceberg.procedure;

import com.google.common.base.Enums;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.DeltaLakeColumnMetadata;
import io.trino.plugin.deltalake.DeltaLakeColumnType;
import io.trino.plugin.deltalake.DeltaLakeSplitManager;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.RawHiveMetastoreFactory;
import io.trino.plugin.hive.metastore.Storage;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergSecurityConfig;
import io.trino.plugin.iceberg.PartitionData;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.plugin.iceberg.fileio.ForwardingInputFile;
import io.trino.spi.TrinoException;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.orc.OrcMetrics;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

import javax.inject.Inject;
import javax.inject.Provider;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Streams.concat;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_FILESYSTEM_ERROR;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.PATH_PROPERTY;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.deserializePartitionValue;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.hive.HiveMetadata.TRANSACTIONAL;
import static io.trino.plugin.hive.HiveMetadata.extractHiveStorageFormat;
import static io.trino.plugin.hive.metastore.MetastoreUtil.buildInitialPrivilegeSet;
import static io.trino.plugin.hive.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.hive.util.HiveUtil.isDeltaLakeTable;
import static io.trino.plugin.hive.util.HiveUtil.isHudiTable;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_COMMIT_ERROR;
import static io.trino.plugin.iceberg.IcebergPageSink.getIcebergValue;
import static io.trino.plugin.iceberg.IcebergSecurityConfig.IcebergSecurity.SYSTEM;
import static io.trino.plugin.iceberg.IcebergUtil.isIcebergTable;
import static io.trino.plugin.iceberg.PartitionFields.parsePartitionFields;
import static io.trino.plugin.iceberg.TypeConverter.toIcebergTypeForNewColumn;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Boolean.parseBoolean;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.apache.iceberg.SortOrder.unsorted;
import static org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import static org.apache.iceberg.mapping.NameMappingParser.toJson;

public class MigrateProcedure
        implements Provider<Procedure>
{
    private static final Logger log = Logger.get(MigrateProcedure.class);

    public static final String PROVIDER_PROPERTY_KEY = "provider";
    public static final String PROVIDER_PROPERTY_VALUE = "iceberg";
    private static final MetricsConfig METRICS_CONFIG = MetricsConfig.getDefault();

    private final TrinoCatalogFactory catalogFactory;
    private final HiveMetastoreFactory metastoreFactory;
    private final TransactionLogAccess transactionLogAccess;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final TypeManager typeManager;
    private final int formatVersion;
    private final boolean isUsingSystemSecurity;

    private enum RecursiveDirectory
    {
        TRUE,
        FALSE,
        FAIL,
        /**/
    }

    private static final MethodHandle MIGRATE;

    static {
        try {
            MIGRATE = lookup().unreflect(MigrateProcedure.class.getMethod("migrate", ConnectorSession.class, String.class, String.class, String.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    @Inject
    public MigrateProcedure(
            TrinoCatalogFactory catalogFactory,
            @RawHiveMetastoreFactory HiveMetastoreFactory metastoreFactory,
            TransactionLogAccess transactionLogAccess,
            TrinoFileSystemFactory fileSystemFactory,
            TypeManager typeManager,
            IcebergConfig icebergConfig,
            IcebergSecurityConfig securityConfig)
    {
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
        this.metastoreFactory = requireNonNull(metastoreFactory, "metastoreFactory is null");
        this.transactionLogAccess = requireNonNull(transactionLogAccess, "transactionLogAccess is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.formatVersion = icebergConfig.getFormatVersion();
        this.isUsingSystemSecurity = securityConfig.getSecuritySystem() == SYSTEM;
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "migrate",
                ImmutableList.of(
                        new Procedure.Argument("SCHEMA_NAME", VARCHAR),
                        new Procedure.Argument("TABLE_NAME", VARCHAR),
                        new Procedure.Argument("RECURSIVE_DIRECTORY", VARCHAR, false, utf8Slice("fail"))),
                MIGRATE.bindTo(this));
    }

    public void migrate(ConnectorSession session, String schemaName, String tableName, String recursiveDirectory)
    {
        // this line guarantees that classLoader that we stored in the field will be used inside try/catch
        // as we captured reference to PluginClassLoader during initialization of this class
        // we can use it now to correctly execute the procedure
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doMigrate(session, schemaName, tableName, recursiveDirectory);
        }
    }

    public void doMigrate(ConnectorSession session, String schemaName, String tableName, String recursiveDirectory)
    {
        SchemaTableName sourceTableName = new SchemaTableName(schemaName, tableName);
        HiveMetastore metastore = metastoreFactory.createMetastore(Optional.of(session.getIdentity()));

        io.trino.plugin.hive.metastore.Table hiveTable = metastore.getTable(schemaName, tableName).orElseThrow(() -> new TableNotFoundException(sourceTableName));
        String transactionalProperty = hiveTable.getParameters().get(TRANSACTIONAL);
        if (parseBoolean(transactionalProperty)) {
            throw new TrinoException(NOT_SUPPORTED, "Migrating transactional tables is unsupported");
        }
        else if (!"MANAGED_TABLE".equalsIgnoreCase(hiveTable.getTableType()) && !"EXTERNAL_TABLE".equalsIgnoreCase(hiveTable.getTableType())) {
            throw new TrinoException(NOT_SUPPORTED, "The procedure doesn't support migrating %s table type".formatted(hiveTable.getTableType()));
        }
        else if (isDeltaLakeTable(hiveTable)) {
            migrateDeltaLakeTable(session, metastore, sourceTableName, hiveTable);
        }
        else if (isHudiTable(hiveTable)) {
            throw new TrinoException(NOT_SUPPORTED, "The procedure doesn't support migrating Hudi tables");
        }
        else if (isIcebergTable(hiveTable)) {
            throw new TrinoException(NOT_SUPPORTED, "The table is already an Iceberg table");
        }
        else {
            RecursiveDirectory recursive = Enums.getIfPresent(RecursiveDirectory.class, recursiveDirectory.toUpperCase(ENGLISH)).toJavaUtil()
                    .orElseThrow(() -> new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Invalid recursive_directory: " + recursiveDirectory));
            migrateHiveTable(session, recursive, metastore, sourceTableName, hiveTable);
        }
    }

    private void migrateDeltaLakeTable(
            ConnectorSession session,
            HiveMetastore metastore,
            SchemaTableName schemaTableName,
            io.trino.plugin.hive.metastore.Table hiveTable)
    {
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        TrinoCatalog catalog = catalogFactory.create(session.getIdentity());

        Map<String, String> serdeParameters = hiveTable.getStorage().getSerdeParameters();
        String location = serdeParameters.get(PATH_PROPERTY);
        if (location == null) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, format("No %s property defined for table: %s", PATH_PROPERTY, hiveTable));
        }

        TableSnapshot tableSnapshot;
        try {
            tableSnapshot = transactionLogAccess.loadSnapshot(schemaTableName, location, session);
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_FILESYSTEM_ERROR, "Unable to load Delta Lake table for migration", e);
        }

        ProtocolEntry protocolEntry = transactionLogAccess.getProtocolEntries(tableSnapshot, session)
                .reduce((first, second) -> second)
                .orElseThrow(() -> new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Protocol entry not found in transaction log for table " + tableSnapshot.getTable()));
        MetadataEntry metadataEntry = transactionLogAccess.getMetadataEntry(tableSnapshot, session);

        if (protocolEntry.getMinReaderVersion() > 1) {
            throw new TrinoException(NOT_SUPPORTED, "Migration of Delta Lake tables using reader versions above 1 is not supported");
        }
        if (protocolEntry.getMinWriterVersion() > 2) {
            throw new TrinoException(NOT_SUPPORTED, "Migration of Delta Lake tables using writer versions above 2 is not supported");
        }

        List<DeltaLakeColumnMetadata> deltaLakeColumns = DeltaLakeSchemaSupport.extractSchema(metadataEntry, typeManager);
        AtomicInteger nextFieldId = new AtomicInteger(1);
        List<Types.NestedField> icebergColumns = new ArrayList<>();
        for (DeltaLakeColumnMetadata column : deltaLakeColumns) {
            int index = nextFieldId.getAndIncrement();
            Type deltaType = typeManager.getType(column.getType().getTypeSignature());
            org.apache.iceberg.types.Type icebergType;
            if (deltaType instanceof TimestampWithTimeZoneType) {
                throw new TrinoException(NOT_SUPPORTED, "Migrations of tables with Timestamp with Time Zone columns are not supported");
            }
            else {
                icebergType = toIcebergTypeForNewColumn(deltaType, nextFieldId);
            }
            Types.NestedField field = Types.NestedField.of(index, column.getColumnMetadata().isNullable(), column.getName(), icebergType, column.getColumnMetadata().getComment());
            icebergColumns.add(field);
        }
        org.apache.iceberg.types.Type icebergSchema = Types.StructType.of(icebergColumns);
        AtomicInteger freshFieldIds = new AtomicInteger(1);
        icebergSchema = TypeUtil.assignFreshIds(icebergSchema, freshFieldIds::getAndIncrement);
        Schema schema = new Schema(icebergSchema.asStructType().fields());

        NameMapping nameMapping = MappingUtil.create(schema);
        Map<String, String> properties = icebergTableProperties(location, hiveTable.getParameters(), nameMapping, Optional.ofNullable(metadataEntry.getDescription()));
        PartitionSpec partitionSpec = partitionSpecFromDeltaLake(schema, metadataEntry);

        Map<String, DeltaLakeColumnHandle> columnHandles = deltaLakeColumns.stream().collect(toImmutableMap(
                DeltaLakeColumnMetadata::getName,
                columnMetadata -> new DeltaLakeColumnHandle(
                        columnMetadata.getName(),
                        columnMetadata.getType(),
                        OptionalInt.empty(),
                        columnMetadata.getPhysicalName(),
                        columnMetadata.getPhysicalColumnType(),
                        DeltaLakeColumnType.REGULAR)));

        ImmutableList.Builder<DataFile> dataFilesBuilder = ImmutableList.builder();
        List<AddFileEntry> deltaLakeAddFileEntries = transactionLogAccess.getActiveFiles(tableSnapshot, session);
        int fileCount = 0;
        for (AddFileEntry addFileEntry : deltaLakeAddFileEntries) {
            // TODO: Rewrite Delta Lake metrics rather than re-reading the file
            String filePath = DeltaLakeSplitManager.buildSplitPath(location, addFileEntry);
            log.debug("Building data files from '%s' from Delta Lake file %d of %d", filePath, fileCount++, deltaLakeAddFileEntries.size());
            InputFile inputFile = new ForwardingInputFile(fileSystem.newInputFile(filePath));
            Metrics metrics = ParquetUtil.fileMetrics(inputFile, METRICS_CONFIG, nameMapping);
            dataFilesBuilder.add(DataFiles.builder(partitionSpec)
                    .withPath(filePath)
                    .withFormat(FileFormat.PARQUET)
                    .withFileSizeInBytes(addFileEntry.getSize())
                    .withMetrics(metrics)
                    .withPartition(partitionDataFromDeltaLake(schema, partitionSpec, columnHandles, addFileEntry.getCanonicalPartitionValues()))
                    .build());
        }

        commitNewTable(
                session,
                catalog,
                metastore,
                hiveTable,
                schemaTableName,
                schema,
                partitionSpec,
                location,
                properties,
                dataFilesBuilder.build());
    }

    private void migrateHiveTable(
            ConnectorSession session,
            RecursiveDirectory recursive,
            HiveMetastore metastore,
            SchemaTableName sourceTableName,
            io.trino.plugin.hive.metastore.Table hiveTable)
    {
        hiveTable.getStorage()
                .getBucketProperty()
                .ifPresent(bucket -> {
                    throw new TrinoException(NOT_SUPPORTED, "Cannot migrate bucketed table: " + bucket.getBucketedBy());
                });

        TrinoCatalog catalog = catalogFactory.create(session.getIdentity());
        Schema schema = toIcebergSchema(concat(hiveTable.getDataColumns().stream(), hiveTable.getPartitionColumns().stream()).toList());
        NameMapping nameMapping = MappingUtil.create(schema);
        HiveStorageFormat storageFormat = extractHiveStorageFormat(hiveTable.getStorage().getStorageFormat());
        String location = hiveTable.getStorage().getLocation();

        Map<String, String> properties = icebergTableProperties(location, hiveTable.getParameters(), nameMapping, Optional.ofNullable(hiveTable.getParameters().get(TABLE_COMMENT)));
        List<String> partitionColumnNames = hiveTable.getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());
        PartitionSpec partitionSpec = parsePartitionFields(schema, partitionColumnNames);
        try {
            ImmutableList.Builder<DataFile> dataFilesBuilder = ImmutableList.builder();
            if (hiveTable.getPartitionColumns().isEmpty()) {
                log.debug("Building data files from %s", location);
                dataFilesBuilder.addAll(buildDataFiles(session, recursive, storageFormat, location, partitionSpec, new PartitionData(new Object[]{}), nameMapping));
            }
            else {
                Map<String, Optional<Partition>> partitions = listAllPartitions(metastore, hiveTable);
                int fileCount = 1;
                for (Map.Entry<String, Optional<Partition>> partition : partitions.entrySet()) {
                    Storage storage = partition.getValue().orElseThrow().getStorage();
                    log.debug("Building data files from '%s' for partition %d of %d", storage.getLocation(), fileCount++, partitions.size());
                    HiveStorageFormat partitionStorageFormat = extractHiveStorageFormat(storage.getStorageFormat());
                    StructLike partitionData = DataFiles.data(partitionSpec, partition.getKey());
                    dataFilesBuilder.addAll(buildDataFiles(session, recursive, partitionStorageFormat, storage.getLocation(), partitionSpec, partitionData, nameMapping));
                }
            }

            commitNewTable(
                    session,
                    catalog,
                    metastore,
                    hiveTable,
                    sourceTableName,
                    schema,
                    partitionSpec,
                    location,
                    properties,
                    dataFilesBuilder.build());
        }
        catch (Exception e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to migrate table", e);
        }
    }

    private void commitNewTable(
            ConnectorSession session,
            TrinoCatalog catalog,
            HiveMetastore metastore,
            io.trino.plugin.hive.metastore.Table hiveTable,
            SchemaTableName sourceTableName,
            Schema schema,
            PartitionSpec partitionSpec,
            String location,
            Map<String, String> properties,
            List<DataFile> dataFiles)
    {
        log.debug("Start new transaction");
        Transaction transaction = catalog.newCreateTableTransaction(
                session,
                sourceTableName,
                schema,
                partitionSpec,
                unsorted(),
                location,
                properties);

        log.debug("Append data %d data files", dataFiles.size());
        Table table = transaction.table();
        AppendFiles append = table.newAppend();
        dataFiles.forEach(append::appendFile);
        append.commit();

        log.debug("Set preparatory table properties in a metastore for migrations");
        PrincipalPrivileges principalPrivileges = isUsingSystemSecurity ? NO_PRIVILEGES : buildInitialPrivilegeSet(session.getUser());
        io.trino.plugin.hive.metastore.Table newTable = io.trino.plugin.hive.metastore.Table.builder(hiveTable)
                .setParameter(METADATA_LOCATION_PROP, location)
                .setParameter(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(ENGLISH))
                .build();
        metastore.replaceTable(sourceTableName.getSchemaName(), sourceTableName.getTableName(), newTable, principalPrivileges);

        transaction.commitTransaction();
        log.debug("Successfully migrated %s table to Iceberg format", sourceTableName);
    }

    private Map<String, String> icebergTableProperties(String location, Map<String, String> hiveTableProperties, NameMapping nameMapping, Optional<String> tableComment)
    {
        Map<String, String> icebergTableProperties = new HashMap<>();

        // Copy all Hive table properties so that we can undo the migration easily. This is same as Spark implementation.
        icebergTableProperties.putAll(hiveTableProperties);
        icebergTableProperties.remove("path");
        icebergTableProperties.remove("transient_lastDdlTime");
        icebergTableProperties.remove("serialization.format");

        icebergTableProperties.put("migrated", "true");
        icebergTableProperties.putIfAbsent("location", location);
        icebergTableProperties.put(PROVIDER_PROPERTY_KEY, PROVIDER_PROPERTY_VALUE);
        icebergTableProperties.put(METADATA_LOCATION_PROP, location);
        icebergTableProperties.put(DEFAULT_NAME_MAPPING, toJson(nameMapping));
        icebergTableProperties.put(FORMAT_VERSION, String.valueOf(formatVersion));

        tableComment.ifPresent(comment -> icebergTableProperties.put(TABLE_COMMENT, comment));

        return ImmutableMap.copyOf(icebergTableProperties);
    }

    private Schema toIcebergSchema(List<Column> columns)
    {
        AtomicInteger nextFieldId = new AtomicInteger(1);
        List<Types.NestedField> icebergColumns = new ArrayList<>();
        for (Column column : columns) {
            int index = icebergColumns.size();
            org.apache.iceberg.types.Type type = toIcebergTypeForNewColumn(typeManager.getType(column.getType().getTypeSignature()), nextFieldId);
            Types.NestedField field = Types.NestedField.of(index, false, column.getName(), type, column.getComment().orElse(null));
            icebergColumns.add(field);
        }
        org.apache.iceberg.types.Type icebergSchema = Types.StructType.of(icebergColumns);
        icebergSchema = TypeUtil.assignFreshIds(icebergSchema, nextFieldId::getAndIncrement);
        return new Schema(icebergSchema.asStructType().fields());
    }

    public Map<String, Optional<Partition>> listAllPartitions(HiveMetastore metastore, io.trino.plugin.hive.metastore.Table table)
    {
        List<String> partitionNames = table.getPartitionColumns().stream().map(Column::getName).collect(toImmutableList());
        Optional<List<String>> partitions = metastore.getPartitionNamesByFilter(table.getDatabaseName(), table.getTableName(), partitionNames, TupleDomain.all());
        if (partitions.isEmpty()) {
            return ImmutableMap.of();
        }
        return metastore.getPartitionsByNames(table, partitions.get());
    }

    private List<DataFile> buildDataFiles(ConnectorSession session, RecursiveDirectory recursive, HiveStorageFormat format, String location, PartitionSpec partitionSpec, StructLike partition, NameMapping nameMapping)
            throws IOException
    {
        // TODO: Introduce parallelism
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        FileIterator files = fileSystem.listFiles(location);
        ImmutableList.Builder<DataFile> dataFilesBuilder = ImmutableList.builder();
        while (files.hasNext()) {
            FileEntry file = files.next();
            String relativePath = file.location().substring(location.length());
            if (relativePath.contains("/_") || relativePath.contains("/.")) {
                continue;
            }
            if (recursive == RecursiveDirectory.FALSE && isRecursive(location, file.location())) {
                continue;
            }
            else if (recursive == RecursiveDirectory.FAIL && isRecursive(location, file.location())) {
                throw new TrinoException(NOT_SUPPORTED, "Recursive directory must not exist when recursive_directory argument is 'fail': " + file.location());
            }

            Metrics metrics = loadMetrics(fileSystem, format, file.location(), nameMapping);
            DataFile dataFile = buildDataFile(file, partition, partitionSpec, format.name(), metrics);
            dataFilesBuilder.add(dataFile);
        }
        List<DataFile> dataFiles = dataFilesBuilder.build();
        log.debug("Found %d files in '%s'", dataFiles.size(), location);
        return dataFiles;
    }

    private static boolean isRecursive(String baseLocation, String location)
    {
        verify(location.startsWith(baseLocation), "%s should start with %s", location, baseLocation);
        String suffix = location.substring(baseLocation.length() + 1).replaceFirst("^/+", "");
        return suffix.contains("/");
    }

    private Metrics loadMetrics(TrinoFileSystem fileSystem, HiveStorageFormat storageFormat, String path, NameMapping nameMapping)
    {
        InputFile inputFile = new ForwardingInputFile(fileSystem.newInputFile(path));
        return switch (storageFormat) {
            case ORC -> OrcMetrics.fromInputFile(inputFile, METRICS_CONFIG, nameMapping);
            case PARQUET -> ParquetUtil.fileMetrics(inputFile, METRICS_CONFIG, nameMapping);
            case AVRO -> new Metrics(Avro.rowCount(inputFile), null, null, null, null);
            default -> throw new TrinoException(NOT_SUPPORTED, "Unsupported storage format: " + storageFormat);
        };
    }

    private static DataFile buildDataFile(FileEntry file, StructLike partition, PartitionSpec spec, String format, Metrics metrics)
    {
        return DataFiles.builder(spec)
                .withPath(file.location())
                .withFormat(format)
                .withFileSizeInBytes(file.length())
                .withMetrics(metrics)
                .withPartition(partition)
                .build();
    }

    private static PartitionSpec partitionSpecFromDeltaLake(Schema icebergSchema, MetadataEntry metadataEntry)
    {
        if (metadataEntry.getCanonicalPartitionColumns().isEmpty()) {
            return PartitionSpec.unpartitioned();
        }

        PartitionSpec.Builder partitionSpecBuilder = PartitionSpec.builderFor(icebergSchema);
        metadataEntry.getCanonicalPartitionColumns().forEach(partitionSpecBuilder::identity);
        return partitionSpecBuilder.build();
    }

    private static PartitionData partitionDataFromDeltaLake(Schema icebergSchema, PartitionSpec partitionSpec, Map<String, DeltaLakeColumnHandle> columnHandles, Map<String, Optional<String>> deltaPartitioning)
    {
        Object[] partition = partitionSpec.fields().stream()
                .map(partitionField -> {
                    String columnName = icebergSchema.findField(partitionField.sourceId()).name();
                    Optional<String> deltaPartitionValue = deltaPartitioning.get(columnName);
                    DeltaLakeColumnHandle columnHandle = columnHandles.get(columnName);
                    if (columnHandle == null) {
                        throw new IllegalStateException("Missing DeltaLakeColumnHandle for partition field " + partitionField);
                    }
                    Object trinoValue = deserializePartitionValue(columnHandle, deltaPartitionValue);
                    return getIcebergValue(RunLengthEncodedBlock.create(columnHandle.getType(), trinoValue, 1), 0, columnHandle.getType());
                })
                .toArray();
        return new PartitionData(partition);
    }
}
