/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake.distributed;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.starburstdata.presto.plugin.snowflake.jdbc.SnowflakeJdbcClientModule;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.prestosql.plugin.base.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import io.prestosql.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.s3.PrestoS3FileSystem;
import io.prestosql.plugin.hive.s3.PrestoS3FileSystemStats;
import io.prestosql.plugin.jdbc.JdbcModule;
import io.prestosql.plugin.jdbc.JdbcPageSinkProvider;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.procedure.Procedure;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.prestosql.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

class SnowflakeDistributedModule
        extends AbstractConfigurationAwareModule
{
    private final String catalogName;

    SnowflakeDistributedModule(String catalogName)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        newOptionalBinder(binder, ConnectorAccessControl.class);
        newSetBinder(binder, Procedure.class);
        bindSessionPropertiesProvider(binder, SnowflakeDistributedSessionProperties.class);
        binder.bind(SnowflakeMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(SnowflakeConnectionManager.class).in(Scopes.SINGLETON);
        binder.bind(SnowflakeSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(SnowflakePageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(JdbcPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(Connector.class).to(SnowflakeDistributedConnector.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(SnowflakeDistributedConfig.class);

        binder.bind(SnowflakeExportStats.class).toInstance(new SnowflakeExportStats());
        newExporter(binder).export(SnowflakeExportStats.class).as(generator -> generator.generatedNameOf(SnowflakeExportStats.class, catalogName));

        binder.bind(PrestoS3FileSystemStats.class).toInstance(PrestoS3FileSystem.getFileSystemStats());
        newExporter(binder).export(PrestoS3FileSystemStats.class)
                .as(generator -> generator.generatedNameOf(PrestoS3FileSystem.class));
        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FileFormatDataSourceStats.class).withGeneratedName();

        install(new JdbcModule(catalogName));
        install(new SnowflakeJdbcClientModule(catalogName, true));

        // cleanup
        binder.bind(ExecutorCleanup.class).in(Scopes.SINGLETON);
    }

    @Inject
    @Provides
    @Singleton
    public ConnectorSplitManager createSplitManager(SnowflakeSplitManager snowflakeSplitManager)
    {
        return new ClassLoaderSafeConnectorSplitManager(snowflakeSplitManager, getClass().getClassLoader());
    }

    @Inject
    @Provides
    @Singleton
    public ConnectorPageSourceProvider createConnectorPageSourceProvider(SnowflakePageSourceProvider snowflakePageSourceProvider)
    {
        return new ClassLoaderSafeConnectorPageSourceProvider(snowflakePageSourceProvider, getClass().getClassLoader());
    }

    @Provides
    @Singleton
    public static ListeningExecutorService createListeningExecutorService()
    {
        return listeningDecorator(newCachedThreadPool(daemonThreadsNamed("snowflake-%s")));
    }
}
