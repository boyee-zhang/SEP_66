/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.sqlserver;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import com.starburstdata.presto.plugin.jdbc.JdbcJoinPushdownSupportModule;
import com.starburstdata.presto.plugin.jdbc.StarburstJdbcMetadataFactory;
import com.starburstdata.presto.plugin.jdbc.dynamicfiltering.ForDynamicFiltering;
import com.starburstdata.presto.plugin.jdbc.redirection.JdbcTableScanRedirectionModule;
import com.starburstdata.presto.plugin.jdbc.stats.JdbcStatisticsConfig;
import com.starburstdata.presto.plugin.sqlserver.CatalogOverridingModule.ForCatalogOverriding;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcMetadataFactory;
import io.trino.plugin.jdbc.JdbcRecordSetProvider;
import io.trino.plugin.jdbc.JdbcSplitManager;
import io.trino.plugin.jdbc.MaxDomainCompactionThreshold;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.sqlserver.SqlServerTableProperties;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSplitManager;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;
import static io.trino.plugin.jdbc.JdbcModule.bindTablePropertiesProvider;
import static io.trino.plugin.sqlserver.SqlServerClient.SQL_SERVER_MAX_LIST_EXPRESSIONS;

public class StarburstSqlServerClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(io.trino.plugin.sqlserver.SqlServerConfig.class);
        configBinder(binder).bindConfig(StarburstCommonSqlServerConfig.class);

        newOptionalBinder(binder, JdbcMetadataFactory.class).setBinding().to(StarburstJdbcMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(StarburstSqlServerClient.class).in(SINGLETON);
        newOptionalBinder(binder, Key.get(int.class, MaxDomainCompactionThreshold.class)).setBinding().toInstance(SQL_SERVER_MAX_LIST_EXPRESSIONS);

        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);

        bindSessionPropertiesProvider(binder, StarburstCommonSqlServerSessionProperties.class);
        bindSessionPropertiesProvider(binder, StarburstSqlServerSessionProperties.class);

        bindTablePropertiesProvider(binder, SqlServerTableProperties.class);

        binder.bind(ConnectorSplitManager.class).annotatedWith(ForDynamicFiltering.class).to(JdbcSplitManager.class).in(SINGLETON);
        binder.bind(ConnectorRecordSetProvider.class).annotatedWith(ForDynamicFiltering.class).to(JdbcRecordSetProvider.class).in(SINGLETON);

        install(new SqlServerAuthenticationModule());
        install(new CatalogOverridingModule());
        install(new JdbcJoinPushdownSupportModule());
        install(new JdbcTableScanRedirectionModule());
    }

    @Provides
    @Singleton
    @ForCatalogOverriding
    public ConnectionFactory getConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider)
    {
        return new DriverConnectionFactory(new SQLServerDriver(), config, credentialProvider);
    }
}
