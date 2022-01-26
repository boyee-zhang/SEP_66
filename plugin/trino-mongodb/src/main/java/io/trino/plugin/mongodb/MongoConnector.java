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
package io.trino.plugin.mongodb;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
import static io.trino.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class MongoConnector
        implements Connector
{
    private final MongoSession mongoSession;
    private final MongoSplitManager splitManager;
    private final MongoPageSourceProvider pageSourceProvider;
    private final MongoPageSinkProvider pageSinkProvider;

    private final ConcurrentMap<ConnectorTransactionHandle, MongoMetadata> transactions = new ConcurrentHashMap<>();

    @Inject
    public MongoConnector(
            MongoSession mongoSession,
            MongoSplitManager splitManager,
            MongoPageSourceProvider pageSourceProvider,
            MongoPageSinkProvider pageSinkProvider)
    {
        this.mongoSession = mongoSession;
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
    }

    @Override
    public Set<Class<?>> getHandleClasses()
    {
        return ImmutableSet.<Class<?>>builder()
                .add(MongoTableHandle.class)
                .add(MongoColumnHandle.class)
                .add(MongoSplit.class)
                .add(MongoOutputTableHandle.class)
                .add(MongoInsertTableHandle.class)
                .add(MongoTransactionHandle.class)
                .build();
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        checkConnectorSupports(READ_UNCOMMITTED, isolationLevel);
        MongoTransactionHandle transaction = new MongoTransactionHandle();
        transactions.put(transaction, new MongoMetadata(mongoSession));
        return transaction;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
    {
        MongoMetadata metadata = transactions.get(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        return metadata;
    }

    @Override
    public void commit(ConnectorTransactionHandle transaction)
    {
        checkArgument(transactions.remove(transaction) != null, "no such transaction: %s", transaction);
    }

    @Override
    public void rollback(ConnectorTransactionHandle transaction)
    {
        MongoMetadata metadata = transactions.remove(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        metadata.rollback();
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public void shutdown()
    {
        mongoSession.shutdown();
    }
}
