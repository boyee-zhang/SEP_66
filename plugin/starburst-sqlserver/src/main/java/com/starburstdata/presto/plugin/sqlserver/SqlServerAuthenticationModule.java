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
import com.google.inject.Module;
import com.starburstdata.presto.plugin.jdbc.auth.ForImpersonation;
import com.starburstdata.presto.plugin.jdbc.auth.PasswordPassThroughModule;
import com.starburstdata.presto.plugin.jdbc.authtolocal.AuthToLocalModule;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.credential.CredentialProviderModule;

import static com.google.inject.Scopes.SINGLETON;
import static com.starburstdata.presto.plugin.jdbc.auth.NoImpersonationModule.noImpersonationModuleWithCredentialProvider;
import static com.starburstdata.presto.plugin.sqlserver.SqlServerConfig.SqlServerAuthenticationType.PASSWORD;
import static com.starburstdata.presto.plugin.sqlserver.SqlServerConfig.SqlServerAuthenticationType.PASSWORD_PASS_THROUGH;
import static io.airlift.configuration.ConditionalModule.installModuleIf;

public class SqlServerAuthenticationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(installModuleIf(
                SqlServerConfig.class,
                config -> config.getAuthenticationType() == PASSWORD,
                new PasswordModule()));

        install(installModuleIf(
                SqlServerConfig.class,
                config -> config.getAuthenticationType() == PASSWORD_PASS_THROUGH,
                moduleBinder -> {
                    moduleBinder.bind(ConnectionFactory.class)
                            .annotatedWith(ForBaseJdbc.class)
                            .to(Key.get(ConnectionFactory.class, ForImpersonation.class))
                            .in(SINGLETON);
                    install(new PasswordPassThroughModule<>(SqlServerConfig.class, SqlServerConfig::isImpersonationEnabled));
                }));
    }

    private static class PasswordModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            install(new CredentialProviderModule());
            install(installModuleIf(
                    SqlServerConfig.class,
                    SqlServerConfig::isImpersonationEnabled,
                    new ImpersonationModule(),
                    noImpersonationModuleWithCredentialProvider()));
        }
    }

    private static class ImpersonationModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.install(new AuthToLocalModule());
            binder.bind(ConnectionFactory.class).annotatedWith(ForBaseJdbc.class).to(SqlServerImpersonatingConnectionFactory.class).in(SINGLETON);
        }
    }
}
