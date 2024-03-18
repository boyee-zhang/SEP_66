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
package io.trino.plugin.varada.dispatcher;

import io.trino.plugin.varada.CoordinatorNodeManager;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;
import java.util.function.ToIntFunction;

public class DispatcherNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private final ConnectorNodePartitioningProvider nodePartitionProvider;
    private final CoordinatorNodeManager coordinatorNodeManager;
    private final DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;

    public DispatcherNodePartitioningProvider(ConnectorNodePartitioningProvider nodePartitionProvider,
                                              CoordinatorNodeManager coordinatorNodeManager,
                                              DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer)
    {
        this.nodePartitionProvider = nodePartitionProvider;
        this.coordinatorNodeManager = coordinatorNodeManager;
        this.dispatcherProxiedConnectorTransformer = dispatcherProxiedConnectorTransformer;
    }

    @Override
    public Optional<ConnectorBucketNodeMap> getBucketNodeMapping(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        return dispatcherProxiedConnectorTransformer.getBucketNodeMapping(transactionHandle, session, partitioningHandle, nodePartitionProvider, coordinatorNodeManager.getWorkerNodes());
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        ToIntFunction<ConnectorSplit> splitBucketFunction = nodePartitionProvider.getSplitBucketFunction(transactionHandle, session, partitioningHandle);
        return value -> splitBucketFunction.applyAsInt(((DispatcherSplit) value).getProxyConnectorSplit());
    }

    @Override
    public BucketFunction getBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle, List<Type> partitionChannelTypes, int bucketCount)
    {
        return nodePartitionProvider.getBucketFunction(transactionHandle, session, partitioningHandle, partitionChannelTypes, bucketCount);
    }
}
