/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.saphana;

import com.google.common.collect.ImmutableList;
import com.starburstdata.presto.license.LicenceCheckingConnectorFactory;
import io.prestosql.plugin.jdbc.JdbcConnectorFactory;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;

public class SapHanaPlugin
        implements Plugin
{
    public static final String CONNECTOR_NAME = "sap-hana";
    public static final String FEATURE_NAME = CONNECTOR_NAME;

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new LicenceCheckingConnectorFactory(new JdbcConnectorFactory(CONNECTOR_NAME, SapHanaClientModule::new), FEATURE_NAME));
    }
}
