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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.bootstrap.LifeCycleManager;
import io.prestosql.plugin.base.classloader.ClassLoaderSafeConnectorMetadata;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.ConnectorHandleResolver;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorNodePartitioningProvider;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.procedure.Procedure;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.tracer.ConnectorTracerFactory;
import io.prestosql.spi.transaction.IsolationLevel;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
import static io.prestosql.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class HiveConnector
        implements Connector
{
    private final LifeCycleManager lifeCycleManager;
    private final Supplier<TransactionalMetadata> metadataFactory;
    private final ConnectorSplitManager splitManager;
    private final ConnectorPageSourceProvider pageSourceProvider;
    private final ConnectorPageSinkProvider pageSinkProvider;
    private final ConnectorNodePartitioningProvider nodePartitioningProvider;
    private final ConnectorTracerFactory tracerFactory;
    private final Set<SystemTable> systemTables;
    private final Set<Procedure> procedures;
    private final List<PropertyMetadata<?>> sessionProperties;
    private final List<PropertyMetadata<?>> schemaProperties;
    private final List<PropertyMetadata<?>> tableProperties;
    private final List<PropertyMetadata<?>> analyzeProperties;

    private final ConnectorAccessControl accessControl;
    private final ClassLoader classLoader;

    private final HiveTransactionManager transactionManager;

    public HiveConnector(
            LifeCycleManager lifeCycleManager,
            Supplier<TransactionalMetadata> metadataFactory,
            HiveTransactionManager transactionManager,
            ConnectorSplitManager splitManager,
            ConnectorPageSourceProvider pageSourceProvider,
            ConnectorPageSinkProvider pageSinkProvider,
            ConnectorNodePartitioningProvider nodePartitioningProvider,
            ConnectorTracerFactory tracerFactory,
            Set<SystemTable> systemTables,
            Set<Procedure> procedures,
            List<PropertyMetadata<?>> sessionProperties,
            List<PropertyMetadata<?>> schemaProperties,
            List<PropertyMetadata<?>> tableProperties,
            List<PropertyMetadata<?>> analyzeProperties,
            ConnectorAccessControl accessControl,
            ClassLoader classLoader)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadataFactory = requireNonNull(metadataFactory, "metadata is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
        this.nodePartitioningProvider = requireNonNull(nodePartitioningProvider, "nodePartitioningProvider is null");
        this.tracerFactory = requireNonNull(tracerFactory, "connectorTracerFactory is null");
        this.systemTables = ImmutableSet.copyOf(requireNonNull(systemTables, "systemTables is null"));
        this.procedures = ImmutableSet.copyOf(requireNonNull(procedures, "procedures is null"));
        this.sessionProperties = ImmutableList.copyOf(requireNonNull(sessionProperties, "sessionProperties is null"));
        this.schemaProperties = ImmutableList.copyOf(requireNonNull(schemaProperties, "schemaProperties is null"));
        this.tableProperties = ImmutableList.copyOf(requireNonNull(tableProperties, "tableProperties is null"));
        this.analyzeProperties = ImmutableList.copyOf(requireNonNull(analyzeProperties, "analyzeProperties is null"));
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public Optional<ConnectorHandleResolver> getHandleResolver()
    {
        return Optional.of(new HiveHandleResolver());
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
    {
        ConnectorMetadata metadata = transactionManager.get(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        return new ClassLoaderSafeConnectorMetadata(metadata, classLoader);
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorTracerFactory getTracerFactory()
    {
        return tracerFactory;
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
    public ConnectorNodePartitioningProvider getNodePartitioningProvider()
    {
        return nodePartitioningProvider;
    }

    @Override
    public Set<SystemTable> getSystemTables()
    {
        return systemTables;
    }

    @Override
    public Set<Procedure> getProcedures()
    {
        return procedures;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getSchemaProperties()
    {
        return schemaProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getAnalyzeProperties()
    {
        return analyzeProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    @Override
    public ConnectorAccessControl getAccessControl()
    {
        return accessControl;
    }

    @Override
    public boolean isSingleStatementWritesOnly()
    {
        return false;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        checkConnectorSupports(READ_UNCOMMITTED, isolationLevel);
        ConnectorTransactionHandle transaction = new HiveTransactionHandle();
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            transactionManager.put(transaction, metadataFactory.get());
        }
        return transaction;
    }

    @Override
    public void commit(ConnectorTransactionHandle transaction)
    {
        TransactionalMetadata metadata = transactionManager.remove(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            metadata.commit();
        }
    }

    @Override
    public void rollback(ConnectorTransactionHandle transaction)
    {
        TransactionalMetadata metadata = transactionManager.remove(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            metadata.rollback();
        }
    }

    @Override
    public final void shutdown()
    {
        lifeCycleManager.stop();
    }
}
