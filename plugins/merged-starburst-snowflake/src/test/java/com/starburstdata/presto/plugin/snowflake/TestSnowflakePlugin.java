/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.testing.TestingConnectorContext;
import org.testng.annotations.Test;

import java.util.List;

import static com.starburstdata.presto.license.LicenseTesting.unlicensed;
import static org.testng.Assert.assertEquals;

public class TestSnowflakePlugin
{
    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new SnowflakePlugin();
        List<ConnectorFactory> connectorFactories = ImmutableList.copyOf(plugin.getConnectorFactories());
        assertEquals(connectorFactories.size(), 2);

        unlicensed(connectorFactories.get(0)).create(
                "test",
                ImmutableMap.of(
                        "connection-url", "test",
                        "snowflake.role", "test",
                        "snowflake.database", "test",
                        "snowflake.warehouse", "test"),
                new TestingConnectorContext())
                .shutdown();
        unlicensed(connectorFactories.get(1)).create(
                "test",
                ImmutableMap.of(
                        "connection-url", "test",
                        "snowflake.impersonation-type", "ROLE",
                        "snowflake.database", "test",
                        "snowflake.stage-schema", "test",
                        "snowflake.warehouse", "test"),
                new TestingConnectorContext())
                .shutdown();
    }
}
