/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.prestoconnector;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.starburstdata.presto.license.LicenceCheckingConnectorFactory;
import com.starburstdata.presto.license.LicenseManager;
import com.starburstdata.presto.license.LicenseManagerProvider;
import com.starburstdata.presto.plugin.jdbc.dynamicfiltering.jdbc.DynamicFilteringJdbcConnectorFactory;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;

import static com.starburstdata.presto.license.StarburstPrestoFeature.PRESTO_CONNECTOR;
import static io.airlift.configuration.ConfigurationAwareModule.combine;
import static java.util.Objects.requireNonNull;

public class PrestoConnectorPlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new LicenceCheckingConnectorFactory(PRESTO_CONNECTOR, getConnectorFactory(new LicenseManagerProvider().get())));
    }

    @VisibleForTesting
    ConnectorFactory getConnectorFactory(LicenseManager licenseManager)
    {
        requireNonNull(licenseManager, "licenseManager is null");
        return new DynamicFilteringJdbcConnectorFactory(
                // "presto-connector" is reserved for the parallel variant
                // TODO maybe we want to choose variants with a config toggle instead?
                "presto-connector-jdbc",
                (String catalogName) -> combine(
                        binder -> binder.bind(LicenseManager.class).toInstance(licenseManager),
                        new PrestoConnectorModule()),
                licenseManager);
    }
}
