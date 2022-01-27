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

package io.trino.plugin.hudi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartition;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.TimelineUtils;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.plugin.hudi.HudiUtil.mergePredicates;
import static java.util.Objects.requireNonNull;

public class HudiTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final String basePath;
    private final HoodieTableType tableType;
    private final TupleDomain<HiveColumnHandle> partitionPredicates;
    private final TupleDomain<HiveColumnHandle> regularPredicates;
    private final Optional<List<HivePartition>> partitions;
    private final Optional<HoodieTableMetaClient> metaClient;

    @JsonCreator
    public HudiTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("basePath") String basePath,
            @JsonProperty("tableType") HoodieTableType tableType,
            @JsonProperty("partitionPredicates") TupleDomain<HiveColumnHandle> partitionPredicates,
            @JsonProperty("regularPredicates") TupleDomain<HiveColumnHandle> regularPredicates)
    {
        this(schemaName, tableName, basePath, tableType, partitionPredicates,
                regularPredicates, Optional.empty(), Optional.empty());
    }

    public HudiTableHandle(
            String schemaName,
            String tableName,
            String basePath,
            HoodieTableType tableType,
            TupleDomain<HiveColumnHandle> partitionPredicates,
            TupleDomain<HiveColumnHandle> regularPredicates,
            Optional<HoodieTableMetaClient> metaClient)
    {
        this(schemaName, tableName, basePath, tableType, partitionPredicates,
                regularPredicates, Optional.empty(), metaClient);
    }

    public HudiTableHandle(
            String schemaName,
            String tableName,
            String basePath,
            HoodieTableType tableType,
            TupleDomain<HiveColumnHandle> partitionPredicates,
            TupleDomain<HiveColumnHandle> regularPredicates,
            Optional<List<HivePartition>> partitions,
            Optional<HoodieTableMetaClient> metaClient)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.basePath = requireNonNull(basePath, "basePath is null");
        this.tableType = requireNonNull(tableType, "tableType is null");
        this.partitionPredicates = requireNonNull(partitionPredicates, "partitionPredicates is null");
        this.regularPredicates = requireNonNull(regularPredicates, "regularPredicates is null");
        this.partitions = requireNonNull(partitions, "partitions is null").map(ImmutableList::copyOf);
        this.metaClient = requireNonNull(metaClient, "metaClient is null");
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public String getBasePath()
    {
        return basePath;
    }

    @JsonProperty
    public HoodieTableType getTableType()
    {
        return tableType;
    }

    @JsonProperty
    public TupleDomain<HiveColumnHandle> getPartitionPredicates()
    {
        return partitionPredicates;
    }

    @JsonProperty
    public TupleDomain<HiveColumnHandle> getRegularPredicates()
    {
        return regularPredicates;
    }

    @JsonIgnore
    public Optional<List<HivePartition>> getPartitions()
    {
        if (partitions.isEmpty()) {
            List<String> partitionIds = TimelineUtils.getPartitionsWritten(metaClient.get().getActiveTimeline());
            List<HivePartition> hivePartitions = partitionIds.stream()
                    .map(p -> new HivePartition(getSchemaTableName(), p, ImmutableMap.of()))
                    .collect(Collectors.toList());
            return Optional.of(hivePartitions);
        }

        return partitions;
    }

    @JsonIgnore
    public Optional<HoodieTableMetaClient> getMetaClient()
    {
        return metaClient;
    }

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    HudiTableHandle withPredicates(HudiPredicates predicates)
    {
        return new HudiTableHandle(
                schemaName,
                tableName,
                basePath,
                tableType,
                mergePredicates(partitionPredicates,
                        predicates.getPartitionColumnPredicates().transformKeys(HiveColumnHandle.class::cast)),
                mergePredicates(regularPredicates,
                        predicates.getRegularColumnPredicates().transformKeys(HiveColumnHandle.class::cast)),
                partitions,
                metaClient);
    }

    @Override
    public String toString()
    {
        return getSchemaTableName().toString();
    }
}
