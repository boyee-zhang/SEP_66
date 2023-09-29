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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.cache.CachingHostAddressProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitSource;
import io.trino.plugin.deltalake.functions.tablechanges.TableChangesSplitSource;
import io.trino.plugin.deltalake.functions.tablechanges.TableChangesTableFunctionHandle;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeFileStatistics;
import io.trino.spi.SplitWeight;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;

import java.net.URI;
import java.net.URLDecoder;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.deltalake.DeltaLakeAnalyzeProperties.AnalyzeMode.FULL_REFRESH;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.pathColumnHandle;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.createStatisticsPredicate;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getDynamicFilteringWaitTimeout;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getMaxInitialSplitSize;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getMaxSplitSize;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.extractSchema;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.deserializePartitionValue;
import static io.trino.spi.connector.FixedSplitSource.emptySplitSource;
import static java.lang.Math.clamp;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class DeltaLakeSplitManager
        implements ConnectorSplitManager
{
    private final TypeManager typeManager;
    private final TransactionLogAccess transactionLogAccess;
    private final ExecutorService executor;
    private final int maxInitialSplits;
    private final int maxSplitsPerSecond;
    private final int maxOutstandingSplits;
    private final double minimumAssignedSplitWeight;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final JsonCodec<DeltaLakeCacheSplitId> splitIdCodec;
    private final DeltaLakeTransactionManager deltaLakeTransactionManager;
    private final CachingHostAddressProvider cachingHostAddressProvider;

    @Inject
    public DeltaLakeSplitManager(
            TypeManager typeManager,
            TransactionLogAccess transactionLogAccess,
            ExecutorService executor,
            DeltaLakeConfig config,
            TrinoFileSystemFactory fileSystemFactory,
            JsonCodec<DeltaLakeCacheSplitId> splitIdCodec,
            DeltaLakeTransactionManager deltaLakeTransactionManager,
            CachingHostAddressProvider cachingHostAddressProvider)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.transactionLogAccess = requireNonNull(transactionLogAccess, "transactionLogAccess is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.maxInitialSplits = config.getMaxInitialSplits();
        this.maxSplitsPerSecond = config.getMaxSplitsPerSecond();
        this.maxOutstandingSplits = config.getMaxOutstandingSplits();
        this.minimumAssignedSplitWeight = config.getMinimumAssignedSplitWeight();
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.splitIdCodec = requireNonNull(splitIdCodec, "splitIdCodec is null");
        this.deltaLakeTransactionManager = requireNonNull(deltaLakeTransactionManager, "deltaLakeTransactionManager is null");
        this.cachingHostAddressProvider = requireNonNull(cachingHostAddressProvider, "cacheHostAddressProvider is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle handle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        DeltaLakeTableHandle deltaLakeTableHandle = (DeltaLakeTableHandle) handle;
        if (deltaLakeTableHandle.getEnforcedPartitionConstraint().isNone() || deltaLakeTableHandle.getNonPartitionConstraint().isNone()) {
            if (deltaLakeTableHandle.isRecordScannedFiles()) {
                return new FixedSplitSource(ImmutableList.of(), ImmutableList.of());
            }
            return emptySplitSource();
        }

        DeltaLakeSplitSource splitSource = new DeltaLakeSplitSource(
                deltaLakeTableHandle.getSchemaTableName(),
                getSplits(transaction, deltaLakeTableHandle, session, deltaLakeTableHandle.getMaxScannedFileSize(), dynamicFilter.getColumnsCovered(), constraint),
                executor,
                maxSplitsPerSecond,
                maxOutstandingSplits,
                dynamicFilter,
                getDynamicFilteringWaitTimeout(session),
                deltaLakeTableHandle.isRecordScannedFiles());

        return new ClassLoaderSafeConnectorSplitSource(splitSource, DeltaLakeSplitManager.class.getClassLoader());
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableFunctionHandle function)
    {
        if (function instanceof TableChangesTableFunctionHandle tableFunctionHandle) {
            return new TableChangesSplitSource(session, fileSystemFactory, tableFunctionHandle);
        }
        throw new UnsupportedOperationException("Unrecognized function: " + function);
    }

    @Override
    public Optional<CacheSplitId> getCacheSplitId(ConnectorSplit split)
    {
        DeltaLakeSplit deltaLakeSplit = (DeltaLakeSplit) split;

        // ensure cache id generation is revisited whenever split classes change
        deltaLakeSplit = new DeltaLakeSplit(
                deltaLakeSplit.getPath(),
                deltaLakeSplit.getStart(),
                deltaLakeSplit.getLength(),
                deltaLakeSplit.getFileSize(),
                deltaLakeSplit.getFileRowCount(),
                deltaLakeSplit.getFileModifiedTime(),
                deltaLakeSplit.getDeletionVector(),
                // weight does not impact split rows
                SplitWeight.standard(),
                deltaLakeSplit.getStatisticsPredicate(),
                deltaLakeSplit.getPartitionKeys());

        return Optional.of(new CacheSplitId(splitIdCodec.toJson(new DeltaLakeCacheSplitId(
                deltaLakeSplit.getPath(),
                deltaLakeSplit.getStart(),
                deltaLakeSplit.getLength(),
                deltaLakeSplit.getFileSize(),
                deltaLakeSplit.getFileRowCount(),
                deltaLakeSplit.getFileModifiedTime(),
                deltaLakeSplit.getPartitionKeys(),
                deltaLakeSplit.getDeletionVector()))));
    }

    private Stream<DeltaLakeSplit> getSplits(
            ConnectorTransactionHandle transaction,
            DeltaLakeTableHandle tableHandle,
            ConnectorSession session,
            Optional<DataSize> maxScannedFileSize,
            Set<ColumnHandle> columnsCoveredByDynamicFilter,
            Constraint constraint)
    {
        TableSnapshot tableSnapshot = deltaLakeTransactionManager.get(transaction, session.getIdentity())
                .getSnapshot(session, tableHandle.getSchemaTableName(), tableHandle.getLocation(), tableHandle.getReadVersion());
        Stream<AddFileEntry> validDataFiles = transactionLogAccess.getActiveFiles(
                tableSnapshot,
                tableHandle.getMetadataEntry(),
                tableHandle.getProtocolEntry(),
                tableHandle.getEnforcedPartitionConstraint(),
                tableHandle.getProjectedColumns(),
                session);
        TupleDomain<DeltaLakeColumnHandle> enforcedPartitionConstraint = tableHandle.getEnforcedPartitionConstraint();
        TupleDomain<DeltaLakeColumnHandle> nonPartitionConstraint = tableHandle.getNonPartitionConstraint();
        Domain pathDomain = getPathDomain(nonPartitionConstraint);

        boolean splittable =
                // Delta Lake handles updates and deletes by copying entire data files, minus updates/deletes. Because of this we can only have one Split/UpdatablePageSource
                // per file. TODO (https://github.com/trinodb/trino/issues/17063) use deletion vectors instead of copy-on-write and remove DeltaLakeTableHandle.writeType
                tableHandle.getWriteType().isEmpty() &&
                        // When only partitioning columns projected, there is no point splitting the files
                        mayAnyDataColumnProjected(tableHandle);
        AtomicInteger remainingInitialSplits = new AtomicInteger(maxInitialSplits);
        Optional<Instant> filesModifiedAfter = tableHandle.getAnalyzeHandle().flatMap(AnalyzeHandle::getFilesModifiedAfter);
        Optional<Long> maxScannedFileSizeInBytes = maxScannedFileSize.map(DataSize::toBytes);

        MetadataEntry metadataEntry = tableHandle.getMetadataEntry();
        boolean isOptimize = tableHandle.isOptimize();
        if (isOptimize) {
            checkArgument(maxScannedFileSizeInBytes.isPresent(), "maxScannedFileSizeInBytes must be provided when performing OPTIMIZE");
            validDataFiles = filterValidDataFilesForOptimize(validDataFiles, maxScannedFileSizeInBytes.get());
        }

        Set<String> predicatedColumnNames = Stream.concat(
                        nonPartitionConstraint.getDomains().orElseThrow().keySet().stream(),
                        columnsCoveredByDynamicFilter.stream()
                                .map(DeltaLakeColumnHandle.class::cast))
                .map(DeltaLakeColumnHandle::getBaseColumnName)
                .collect(toImmutableSet());
        List<DeltaLakeColumnMetadata> schema = extractSchema(metadataEntry, tableHandle.getProtocolEntry(), typeManager);
        List<DeltaLakeColumnMetadata> predicatedColumns = schema.stream()
                .filter(column -> predicatedColumnNames.contains(column.getName()))
                .collect(toImmutableList());
        return validDataFiles
                .flatMap(addAction -> {
                    if (tableHandle.getAnalyzeHandle().isPresent() &&
                            !(tableHandle.getAnalyzeHandle().get().getAnalyzeMode() == FULL_REFRESH) && !addAction.isDataChange()) {
                        // skip files which do not introduce data change on non FULL REFRESH
                        return Stream.empty();
                    }

                    String splitPath = buildSplitPath(Location.of(tableHandle.getLocation()), addAction).toString();
                    if (!pathMatchesPredicate(pathDomain, splitPath)) {
                        return Stream.empty();
                    }

                    if (filesModifiedAfter.isPresent() && addAction.getModificationTime() <= filesModifiedAfter.get().toEpochMilli()) {
                        return Stream.empty();
                    }

                    if (addAction.getDeletionVector().isEmpty() && maxScannedFileSizeInBytes.isPresent() && addAction.getSize() > maxScannedFileSizeInBytes.get()) {
                        return Stream.empty();
                    }

                    Map<DeltaLakeColumnHandle, Domain> enforcedDomains = enforcedPartitionConstraint.getDomains().orElseThrow();
                    if (!partitionMatchesPredicate(addAction.getCanonicalPartitionValues(), enforcedDomains)) {
                        return Stream.empty();
                    }

                    TupleDomain<DeltaLakeColumnHandle> statisticsPredicate = createStatisticsPredicate(
                            addAction,
                            predicatedColumns,
                            metadataEntry.getLowercasePartitionColumns());
                    if (!nonPartitionConstraint.overlaps(statisticsPredicate)) {
                        return Stream.empty();
                    }

                    if (constraint.predicate().isPresent()) {
                        Map<String, Optional<String>> partitionValues = addAction.getCanonicalPartitionValues();
                        Map<ColumnHandle, NullableValue> deserializedValues = constraint.getPredicateColumns().orElseThrow().stream()
                                .map(DeltaLakeColumnHandle.class::cast)
                                .filter(column -> column.isBaseColumn() && partitionValues.containsKey(column.getBaseColumnName()))
                                .collect(toImmutableMap(identity(), column -> new NullableValue(
                                        column.getBaseType(),
                                        deserializePartitionValue(column, partitionValues.get(column.getBaseColumnName())))));
                        if (!constraint.predicate().get().test(deserializedValues)) {
                            return Stream.empty();
                        }
                    }

                    return splitsForFile(
                            session,
                            addAction,
                            splitPath,
                            addAction.getCanonicalPartitionValues(),
                            statisticsPredicate,
                            splittable,
                            remainingInitialSplits)
                            .stream();
                });
    }

    private static Stream<AddFileEntry> filterValidDataFilesForOptimize(Stream<AddFileEntry> validDataFiles, long maxScannedFileSizeInBytes)
    {
        // Value being present is a pending file (potentially the only one) for a given partition.
        // Value being empty is a tombstone, indicates that there were in the stream previously at least 2 files selected for processing for a given partition.
        Map<Map<String, Optional<String>>, Optional<AddFileEntry>> pendingAddFileEntriesMap = new HashMap<>();
        return validDataFiles
                .filter(addFileEntry -> addFileEntry.getSize() < maxScannedFileSizeInBytes)
                .flatMap(addFileEntry -> {
                    Map<String, Optional<String>> canonicalPartitionValues = addFileEntry.getCanonicalPartitionValues();
                    if (pendingAddFileEntriesMap.containsKey(canonicalPartitionValues)) {
                        Optional<AddFileEntry> alreadyQueuedAddFileEntry = pendingAddFileEntriesMap.get(canonicalPartitionValues);
                        if (alreadyQueuedAddFileEntry.isEmpty()) {
                            return Stream.of(addFileEntry);
                        }
                        pendingAddFileEntriesMap.put(canonicalPartitionValues, Optional.empty());
                        return Stream.of(alreadyQueuedAddFileEntry.get(), addFileEntry);
                    }

                    pendingAddFileEntriesMap.put(canonicalPartitionValues, Optional.of(addFileEntry));
                    return Stream.empty();
                });
    }

    private static boolean mayAnyDataColumnProjected(DeltaLakeTableHandle tableHandle)
    {
        if (tableHandle.getProjectedColumns().isEmpty()) {
            return true;
        }
        return tableHandle.getProjectedColumns().get().stream()
                .map(DeltaLakeColumnHandle::getColumnType)
                .anyMatch(DeltaLakeColumnType.REGULAR::equals);
    }

    public static boolean partitionMatchesPredicate(Map<String, Optional<String>> partitionKeys, Map<DeltaLakeColumnHandle, Domain> domains)
    {
        for (Map.Entry<DeltaLakeColumnHandle, Domain> enforcedDomainsEntry : domains.entrySet()) {
            DeltaLakeColumnHandle partitionColumn = enforcedDomainsEntry.getKey();
            Domain partitionDomain = enforcedDomainsEntry.getValue();
            if (!partitionDomain.includesNullableValue(deserializePartitionValue(partitionColumn, partitionKeys.get(partitionColumn.getBasePhysicalColumnName())))) {
                return false;
            }
        }
        return true;
    }

    private static Domain getPathDomain(TupleDomain<DeltaLakeColumnHandle> effectivePredicate)
    {
        return effectivePredicate.getDomains()
                .flatMap(domains -> Optional.ofNullable(domains.get(pathColumnHandle())))
                .orElseGet(() -> Domain.all(pathColumnHandle().getBaseType()));
    }

    private static boolean pathMatchesPredicate(Domain pathDomain, String path)
    {
        return pathDomain.includesNullableValue(utf8Slice(path));
    }

    private List<DeltaLakeSplit> splitsForFile(
            ConnectorSession session,
            AddFileEntry addFileEntry,
            String splitPath,
            Map<String, Optional<String>> partitionKeys,
            TupleDomain<DeltaLakeColumnHandle> statisticsPredicate,
            boolean splittable,
            AtomicInteger remainingInitialSplits)
    {
        long fileSize = addFileEntry.getSize();

        if (!splittable) {
            // remainingInitialSplits is not used when !splittable
            return ImmutableList.of(new DeltaLakeSplit(
                    splitPath,
                    0,
                    fileSize,
                    fileSize,
                    addFileEntry.getStats().flatMap(DeltaLakeFileStatistics::getNumRecords),
                    addFileEntry.getModificationTime(),
                    addFileEntry.getDeletionVector(),
                    cachingHostAddressProvider.getHosts(splitPath),
                    SplitWeight.standard(),
                    statisticsPredicate,
                    partitionKeys));
        }

        ImmutableList.Builder<DeltaLakeSplit> splits = ImmutableList.builder();
        long currentOffset = 0;
        while (currentOffset < fileSize) {
            long maxSplitSize;
            if (remainingInitialSplits.get() > 0 && remainingInitialSplits.getAndDecrement() > 0) {
                maxSplitSize = getMaxInitialSplitSize(session).toBytes();
            }
            else {
                maxSplitSize = getMaxSplitSize(session).toBytes();
            }
            long splitSize = Math.min(maxSplitSize, fileSize - currentOffset);

            splits.add(new DeltaLakeSplit(
                    splitPath,
                    currentOffset,
                    splitSize,
                    fileSize,
                    Optional.empty(),
                    addFileEntry.getModificationTime(),
                    addFileEntry.getDeletionVector(),
                    cachingHostAddressProvider.getHosts(splitPath),
                    SplitWeight.fromProportion(clamp((double) splitSize / maxSplitSize, minimumAssignedSplitWeight, 1.0)),
                    statisticsPredicate,
                    partitionKeys));

            currentOffset += splitSize;
        }

        return splits.build();
    }

    public static Location buildSplitPath(Location tableLocation, AddFileEntry addAction)
    {
        // paths are relative to the table location and are RFC 2396 URIs
        // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-file-and-remove-file
        URI uri = URI.create(addAction.getPath());
        String path = uri.getPath();

        // org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem encodes the path as URL when opening files
        // https://issues.apache.org/jira/browse/HADOOP-18580
        Optional<String> scheme = tableLocation.scheme();
        if (scheme.isPresent() && (scheme.get().equals("abfs") || scheme.get().equals("abfss"))) {
            // Replace '+' with '%2B' beforehand. Otherwise, the character becomes a space ' ' by URL decode.
            path = URLDecoder.decode(path.replace("+", "%2B"), UTF_8);
        }
        return tableLocation.appendPath(path);
    }
}
