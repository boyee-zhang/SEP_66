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
package io.trino.plugin.raptor.legacy;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.raptor.legacy.backup.BackupService;
import io.trino.plugin.raptor.legacy.metadata.BucketShards;
import io.trino.plugin.raptor.legacy.metadata.ShardManager;
import io.trino.plugin.raptor.legacy.metadata.ShardNodes;
import io.trino.plugin.raptor.legacy.util.SynchronizedResultIterator;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPartitionHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;
import org.jdbi.v3.core.result.ResultIterator;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.plugin.raptor.legacy.RaptorErrorCode.RAPTOR_NO_HOST_FOR_SHARD;
import static io.trino.plugin.raptor.legacy.RaptorSessionProperties.getOneSplitPerBucketThreshold;
import static io.trino.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.stream.Collectors.toSet;

public class RaptorSplitManager
        implements ConnectorSplitManager
{
    private final NodeSupplier nodeSupplier;
    private final ShardManager shardManager;
    private final boolean backupAvailable;
    private final ExecutorService executor;

    @Inject
    public RaptorSplitManager(CatalogName catalogName, NodeSupplier nodeSupplier, ShardManager shardManager, BackupService backupService)
    {
        this(catalogName, nodeSupplier, shardManager, requireNonNull(backupService, "backupService is null").isBackupAvailable());
    }

    public RaptorSplitManager(CatalogName catalogName, NodeSupplier nodeSupplier, ShardManager shardManager, boolean backupAvailable)
    {
        this.nodeSupplier = requireNonNull(nodeSupplier, "nodeSupplier is null");
        this.shardManager = requireNonNull(shardManager, "shardManager is null");
        this.backupAvailable = backupAvailable;
        this.executor = newCachedThreadPool(daemonThreadsNamed("raptor-split-" + catalogName + "-%s"));
    }

    @PreDestroy
    public void destroy()
    {
        executor.shutdownNow();
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle handle,
            SplitSchedulingStrategy splitSchedulingStrategy,
            DynamicFilter dynamicFilter)
    {
        RaptorTableHandle table = (RaptorTableHandle) handle;
        long tableId = table.getTableId();
        boolean bucketed = table.getBucketCount().isPresent();
        boolean merged = bucketed && !table.isDelete() && (table.getBucketCount().getAsInt() >= getOneSplitPerBucketThreshold(session));
        OptionalLong transactionId = table.getTransactionId();
        Optional<List<String>> bucketToNode = table.getBucketAssignments();
        verify(bucketed == bucketToNode.isPresent(), "mismatched bucketCount and bucketToNode presence");
        return new RaptorSplitSource(tableId, merged, table.getConstraint(), transactionId, bucketToNode);
    }

    private static List<HostAddress> getAddressesForNodes(Map<String, Node> nodeMap, Iterable<String> nodeIdentifiers)
    {
        ImmutableList.Builder<HostAddress> nodes = ImmutableList.builder();
        for (String id : nodeIdentifiers) {
            Node node = nodeMap.get(id);
            if (node != null) {
                nodes.add(node.getHostAndPort());
            }
        }
        return nodes.build();
    }

    private static <T> T selectRandom(Iterable<T> elements)
    {
        List<T> list = ImmutableList.copyOf(elements);
        return list.get(ThreadLocalRandom.current().nextInt(list.size()));
    }

    private class RaptorSplitSource
            implements ConnectorSplitSource
    {
        private final Map<String, Node> nodesById = uniqueIndex(nodeSupplier.getWorkerNodes(), Node::getNodeIdentifier);
        private final long tableId;
        private final OptionalLong transactionId;
        private final Optional<List<String>> bucketToNode;
        private final ResultIterator<BucketShards> iterator;

        @GuardedBy("this")
        private CompletableFuture<ConnectorSplitBatch> future;

        public RaptorSplitSource(
                long tableId,
                boolean merged,
                TupleDomain<RaptorColumnHandle> effectivePredicate,
                OptionalLong transactionId,
                Optional<List<String>> bucketToNode)
        {
            this.tableId = tableId;
            this.transactionId = requireNonNull(transactionId, "transactionId is null");
            this.bucketToNode = requireNonNull(bucketToNode, "bucketToNode is null");

            ResultIterator<BucketShards> iterator;
            if (bucketToNode.isPresent()) {
                iterator = shardManager.getShardNodesBucketed(tableId, merged, bucketToNode.get(), effectivePredicate);
            }
            else {
                iterator = shardManager.getShardNodes(tableId, effectivePredicate);
            }
            this.iterator = new SynchronizedResultIterator<>(iterator);
        }

        @Override
        public synchronized CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
        {
            checkState((future == null) || future.isDone(), "previous batch not completed");
            future = supplyAsync(batchSupplier(maxSize), executor);
            return future;
        }

        @Override
        public synchronized void close()
        {
            if (future != null) {
                future.cancel(true);
                future = null;
            }
            executor.execute(iterator::close);
        }

        @Override
        public boolean isFinished()
        {
            return !iterator.hasNext();
        }

        private Supplier<ConnectorSplitBatch> batchSupplier(int maxSize)
        {
            return () -> {
                ImmutableList.Builder<ConnectorSplit> list = ImmutableList.builder();
                for (int i = 0; i < maxSize; i++) {
                    if (Thread.currentThread().isInterrupted()) {
                        throw new RuntimeException("Split batch fetch was interrupted");
                    }
                    if (!iterator.hasNext()) {
                        break;
                    }
                    list.add(createSplit(iterator.next()));
                }
                return new ConnectorSplitBatch(list.build(), isFinished());
            };
        }

        private ConnectorSplit createSplit(BucketShards bucketShards)
        {
            if (bucketShards.getBucketNumber().isPresent()) {
                return createBucketSplit(bucketShards.getBucketNumber().getAsInt(), bucketShards.getShards());
            }

            verify(bucketShards.getShards().size() == 1, "wrong shard count for non-bucketed table");
            ShardNodes shard = getOnlyElement(bucketShards.getShards());
            UUID shardId = shard.getShardUuid();
            Set<String> nodeIds = shard.getNodeIdentifiers();

            List<HostAddress> addresses = getAddressesForNodes(nodesById, nodeIds);
            if (addresses.isEmpty()) {
                if (!backupAvailable) {
                    throw new TrinoException(RAPTOR_NO_HOST_FOR_SHARD, format("No host for shard %s found: %s", shardId, nodeIds));
                }

                // Pick a random node and optimistically assign the shard to it.
                // That node will restore the shard from the backup location.
                Set<Node> availableNodes = nodeSupplier.getWorkerNodes();
                if (availableNodes.isEmpty()) {
                    throw new TrinoException(NO_NODES_AVAILABLE, "No nodes available to run query");
                }
                Node node = selectRandom(availableNodes);
                shardManager.replaceShardAssignment(tableId, shardId, node.getNodeIdentifier(), true);
                addresses = ImmutableList.of(node.getHostAndPort());
            }

            return new RaptorSplit(shardId, addresses, transactionId);
        }

        private ConnectorSplit createBucketSplit(int bucketNumber, Set<ShardNodes> shards)
        {
            // Bucket splits contain all the shards for the bucket
            // and run on the node assigned to the bucket.

            String nodeId = bucketToNode.get().get(bucketNumber);
            Node node = nodesById.get(nodeId);
            if (node == null) {
                throw new TrinoException(NO_NODES_AVAILABLE, "Node for bucket is offline: " + nodeId);
            }

            Set<UUID> shardUuids = shards.stream()
                    .map(ShardNodes::getShardUuid)
                    .collect(toSet());
            HostAddress address = node.getHostAndPort();

            return new RaptorSplit(shardUuids, bucketNumber, address, transactionId);
        }
    }
}
