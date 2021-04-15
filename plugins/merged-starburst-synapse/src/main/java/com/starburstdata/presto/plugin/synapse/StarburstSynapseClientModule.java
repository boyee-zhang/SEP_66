/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.synapse;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import com.starburstdata.presto.plugin.jdbc.JdbcJoinPushdownSupportModule;
import com.starburstdata.presto.plugin.jdbc.auth.ForAuthentication;
import com.starburstdata.presto.plugin.jdbc.auth.NoImpersonationModule;
import com.starburstdata.presto.plugin.jdbc.authtolocal.AuthToLocalModule;
import com.starburstdata.presto.plugin.jdbc.dynamicfiltering.ForDynamicFiltering;
import com.starburstdata.presto.plugin.jdbc.redirection.JdbcTableScanRedirectionModule;
import com.starburstdata.presto.plugin.jdbc.stats.JdbcStatisticsConfig;
import com.starburstdata.presto.plugin.sqlserver.SqlServerImpersonatingConnectionFactory;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcRecordSetProvider;
import io.trino.plugin.jdbc.JdbcSplitManager;
import io.trino.plugin.jdbc.MaxDomainCompactionThreshold;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.CredentialProviderModule;
import io.trino.plugin.sqlserver.SqlServerConfig;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSplitManager;

import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static com.starburstdata.presto.plugin.synapse.SynapseConfig.SynapseAuthenticationType.ACTIVE_DIRECTORY_PASSWORD;
import static com.starburstdata.presto.plugin.synapse.SynapseConfig.SynapseAuthenticationType.PASSWORD;
import static io.airlift.configuration.ConditionalModule.installModuleIf;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.sqlserver.SqlServerClient.SQL_SERVER_MAX_LIST_EXPRESSIONS;

public class StarburstSynapseClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(SqlServerConfig.class);
        // The SNAPSHOT ISOLATION seems not supported by Synapse, but the docs (
        // https://docs.microsoft.com/en-us/sql/t-sql/statements/set-transaction-isolation-level-transact-sql?view=sql-server-ver15) don't explain
        // whether this is the expected behavior.
        configBinder(binder).bindConfigDefaults(SqlServerConfig.class, config -> config.setSnapshotIsolationDisabled(true));

        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(StarburstSynapseClient.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, Key.get(int.class, MaxDomainCompactionThreshold.class)).setBinding().toInstance(SQL_SERVER_MAX_LIST_EXPRESSIONS);

        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        // TODO(https://starburstdata.atlassian.net/browse/SEP-5073) implement additional table properties support for Synapse
//        bindTablePropertiesProvider(binder, SqlServerTableProperties.class);

        binder.bind(ConnectorSplitManager.class).annotatedWith(ForDynamicFiltering.class).to(JdbcSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorRecordSetProvider.class).annotatedWith(ForDynamicFiltering.class).to(JdbcRecordSetProvider.class).in(Scopes.SINGLETON);

        install(installModuleIf(
                SynapseConfig.class,
                SynapseConfig::isImpersonationEnabled,
                new ImpersonationModule(),
                new NoImpersonationModule()));

        install(installModuleIf(
                SynapseConfig.class,
                config -> config.getAuthenticationType() == ACTIVE_DIRECTORY_PASSWORD,
                new ActiveDirectoryPasswordModule()));

        install(installModuleIf(
                SynapseConfig.class,
                config -> config.getAuthenticationType() == PASSWORD,
                new PasswordModule()));

        install(new JdbcJoinPushdownSupportModule());
        install(new JdbcTableScanRedirectionModule());
    }

    private static class ImpersonationModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.install(new AuthToLocalModule());
            binder.bind(ConnectionFactory.class).annotatedWith(ForBaseJdbc.class).to(SqlServerImpersonatingConnectionFactory.class).in(Scopes.SINGLETON);
        }
    }

    private class ActiveDirectoryPasswordModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            install(new CredentialProviderModule());
        }

        @Provides
        @Singleton
        @ForAuthentication
        public ConnectionFactory getConnectionFactory(
                BaseJdbcConfig baseJdbcConfig,
                CredentialProvider credentialProvider)
        {
            checkState(
                    !baseJdbcConfig.getConnectionUrl().contains("authentication="),
                    "Cannot specify 'authentication' parameter in JDBC URL when using Active Directory password authentication: %s",
                    baseJdbcConfig.getConnectionUrl());
            Properties properties = new Properties();
            properties.setProperty("authentication", "ActiveDirectoryPassword");

            return new DriverConnectionFactory(
                    new SQLServerDriver(),
                    baseJdbcConfig.getConnectionUrl(),
                    properties,
                    credentialProvider);
        }
    }

    private class PasswordModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            install(new CredentialProviderModule());
        }

        @Provides
        @Singleton
        @ForAuthentication
        public ConnectionFactory getConnectionFactory(BaseJdbcConfig baseJdbcConfig, CredentialProvider credentialProvider)
        {
            return new DriverConnectionFactory(new SQLServerDriver(), baseJdbcConfig, credentialProvider);
        }
    }
}
