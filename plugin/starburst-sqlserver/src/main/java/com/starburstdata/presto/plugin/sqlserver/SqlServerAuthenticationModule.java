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
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import com.starburstdata.presto.kerberos.ConnectorKerberosManagerModule;
import com.starburstdata.presto.kerberos.KerberosManager;
import com.starburstdata.presto.license.LicenseManager;
import com.starburstdata.presto.plugin.jdbc.auth.AuthenticationBasedIdentityCacheMappingModule;
import com.starburstdata.presto.plugin.jdbc.auth.ForImpersonation;
import com.starburstdata.presto.plugin.jdbc.auth.PasswordPassThroughModule;
import com.starburstdata.presto.plugin.jdbc.kerberos.KerberosConfig;
import com.starburstdata.presto.plugin.jdbc.kerberos.KerberosConnectionFactory;
import com.starburstdata.presto.plugin.jdbc.kerberos.PassThroughKerberosConnectionFactory;
import com.starburstdata.presto.plugin.sqlserver.CatalogOverridingModule.ForCatalogOverriding;
import com.starburstdata.presto.plugin.toolkit.authtolocal.AuthToLocalModule;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.CredentialProviderModule;
import io.trino.plugin.jdbc.credential.EmptyCredentialProvider;

import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static com.google.inject.Scopes.SINGLETON;
import static com.starburstdata.presto.plugin.jdbc.auth.NoImpersonationModule.noImpersonationModuleWithCredentialProvider;
import static com.starburstdata.presto.plugin.jdbc.auth.NoImpersonationModule.noImpersonationModuleWithSingletonIdentity;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerConfig.SqlServerAuthenticationType.KERBEROS;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerConfig.SqlServerAuthenticationType.KERBEROS_PASS_THROUGH;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerConfig.SqlServerAuthenticationType.PASSWORD;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerConfig.SqlServerAuthenticationType.PASSWORD_PASS_THROUGH;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class SqlServerAuthenticationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(conditionalModule(
                StarburstSqlServerConfig.class,
                config -> config.getAuthenticationType() == PASSWORD,
                new PasswordModule()));

        install(conditionalModule(
                StarburstSqlServerConfig.class,
                config -> config.getAuthenticationType() == PASSWORD_PASS_THROUGH,
                new SqlServerPasswordPassThroughModule()));

        install(conditionalModule(
                StarburstSqlServerConfig.class,
                config -> config.getAuthenticationType() == KERBEROS,
                new KerberosModule()));

        install(conditionalModule(
                StarburstSqlServerConfig.class,
                config -> config.getAuthenticationType() == KERBEROS_PASS_THROUGH,
                new KerberosPassThroughModule()));
    }

    private static class PasswordModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            install(new CredentialProviderModule());
            install(conditionalModule(
                    StarburstSqlServerConfig.class,
                    StarburstSqlServerConfig::isImpersonationEnabled,
                    new ImpersonationModule(),
                    noImpersonationModuleWithCredentialProvider()));
        }

        @Provides
        @Singleton
        @ForCatalogOverriding
        public ConnectionFactory getConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider)
        {
            return createBasicConnectionFactory(config, credentialProvider);
        }
    }

    private static class SqlServerPasswordPassThroughModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            install(new PasswordPassThroughModule<>(StarburstSqlServerConfig.class, StarburstSqlServerConfig::isImpersonationEnabled));
            binder.bind(ConnectionFactory.class)
                    .annotatedWith(ForBaseJdbc.class)
                    .to(Key.get(ConnectionFactory.class, ForImpersonation.class))
                    .in(SINGLETON);
        }

        @Provides
        @Singleton
        @ForCatalogOverriding
        public ConnectionFactory getConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider)
        {
            return createBasicConnectionFactory(config, credentialProvider);
        }
    }

    private static class KerberosModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        public void setup(Binder binder)
        {
            configBinder(binder).bindConfig(KerberosConfig.class);
            install(conditionalModule(
                    StarburstSqlServerConfig.class,
                    StarburstSqlServerConfig::isImpersonationEnabled,
                    new ImpersonationModule(),
                    noImpersonationModuleWithSingletonIdentity()));
        }

        @Inject
        @Provides
        @Singleton
        @ForCatalogOverriding
        public static ConnectionFactory getConnectionFactory(LicenseManager licenseManager, BaseJdbcConfig baseJdbcConfig, KerberosConfig kerberosConfig)
        {
            checkState(
                    !baseJdbcConfig.getConnectionUrl().contains("user="),
                    "Cannot specify 'user' parameter in JDBC URL when using Kerberos authentication: %s",
                    baseJdbcConfig.getConnectionUrl());
            Properties properties = new Properties();
            properties.setProperty("integratedSecurity", "true");
            properties.setProperty("authenticationScheme", "JavaKerberos");
            return new KerberosConnectionFactory(licenseManager, new SQLServerDriver(), baseJdbcConfig, kerberosConfig, properties);
        }
    }

    private static class KerberosPassThroughModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        public void setup(Binder binder)
        {
            install(new ConnectorKerberosManagerModule());
            checkState(
                    !buildConfigObject(StarburstSqlServerConfig.class).isImpersonationEnabled(),
                    "Impersonation is not allowed when using credentials pass-through");
            install(new AuthenticationBasedIdentityCacheMappingModule());
            binder.bind(ConnectionFactory.class)
                    .annotatedWith(ForBaseJdbc.class)
                    .to(Key.get(ConnectionFactory.class, ForImpersonation.class))
                    .in(SINGLETON);
        }

        @Provides
        @Singleton
        @ForCatalogOverriding
        public static ConnectionFactory getConnectionFactory(BaseJdbcConfig baseJdbcConfig, KerberosManager kerberosManager)
        {
            checkState(
                    !baseJdbcConfig.getConnectionUrl().contains("user="),
                    "Cannot specify 'user' parameter in JDBC URL when using Kerberos authentication: %s",
                    baseJdbcConfig.getConnectionUrl());
            Properties properties = new Properties();
            properties.setProperty("integratedSecurity", "true");
            properties.setProperty("authenticationScheme", "JavaKerberos");

            ConnectionFactory connectionFactory = new DriverConnectionFactory(
                    new SQLServerDriver(),
                    baseJdbcConfig.getConnectionUrl(),
                    properties,
                    new EmptyCredentialProvider());

            return new PassThroughKerberosConnectionFactory(kerberosManager, connectionFactory);
        }
    }

    private static class ImpersonationModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.install(new AuthToLocalModule());
            binder.install(new AuthenticationBasedIdentityCacheMappingModule());
            binder.bind(ConnectionFactory.class).annotatedWith(ForBaseJdbc.class).to(SqlServerImpersonatingConnectionFactory.class).in(SINGLETON);
        }
    }

    private static ConnectionFactory createBasicConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider)
    {
        return new DriverConnectionFactory(new SQLServerDriver(), config, credentialProvider);
    }
}
