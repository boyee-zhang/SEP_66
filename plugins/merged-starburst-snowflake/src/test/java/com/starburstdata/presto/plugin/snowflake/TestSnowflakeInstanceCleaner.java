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
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.tpch.TpchTable;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;

import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.ROLE;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.TEST_DATABASE;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.TEST_WAREHOUSE;
import static com.starburstdata.presto.plugin.snowflake.jdbc.SnowflakeClient.IDENTIFIER_QUOTE;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toUnmodifiableSet;

public class TestSnowflakeInstanceCleaner
{
    public static final Logger LOG = Logger.get(TestSnowflakeInstanceCleaner.class);

    /**
     * List of table names that will not be dropped.
     */
    private static final Collection<String> tablesToKeep = ImmutableSet.<String>builder()
            .addAll(TpchTable.getTables().stream()
                    .map(TpchTable::getTableName)
                    .map(s -> s.toLowerCase(Locale.ENGLISH))
                    .collect(toUnmodifiableSet()))
            // additional tables/views used in tests that must not be dropped
            // TODO Migrate all "required" objects to a schema other than SnowflakeServer.TEST_SCHEMA
            .add("sf10_lineitem")
            .add("current_warehouse")
            .build();

    public static final Collection<String> tableTypesToDrop = ImmutableList.of("BASE TABLE", "VIEW");

    private SnowflakeServer snowflakeServer;

    @BeforeClass
    public void setUp()
    {
        snowflakeServer = new SnowflakeServer();
    }

    @Test(dataProvider = "cleanUpSchemasDataProvider")
    public void cleanUpTables(String schemaName)
    {
        logObjectsCount(schemaName);
        if (!tablesToKeep.isEmpty()) {
            LOG.info("Will not drop these tables: %s", join(", ", tablesToKeep));
        }

        LOG.info("Identifying tables to drop...");
        // Drop all tables created more than 24 hours ago
        List<SnowflakeObject> objectsToDrop;
        try (Handle handle = Jdbi.create(snowflakeServer.getConnection()).open()) {
            handle.execute("USE ROLE " + ROLE);
            handle.execute("USE WAREHOUSE " + TEST_WAREHOUSE);
            handle.execute("USE DATABASE " + TEST_DATABASE);
            objectsToDrop = handle.createQuery("" +
                            "SELECT table_schema, table_name, table_type " +
                            "FROM INFORMATION_SCHEMA.TABLES " +
                            "WHERE datediff(hour, created, current_timestamp) > 24 " +
                            "AND table_catalog = :table_catalog AND lower(table_schema) = :table_schema " +
                            // the table_name is lowercased before comparision to ensure it matches case from tablesToKeep
                            "AND lower(table_name) NOT IN (<tables_to_keep>) " +
                            "AND table_type IN (<table_types_to_drop>)")
                    .bind("table_catalog", TEST_DATABASE)
                    .bind("table_schema", schemaName)
                    .bindList("tables_to_keep", tablesToKeep)
                    .bindList("table_types_to_drop", tableTypesToDrop)
                    .map((rs, ctx) -> new SnowflakeObject(rs.getString("TABLE_SCHEMA"), rs.getString("TABLE_NAME"), rs.getString("TABLE_TYPE")))
                    .list();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }

        if (objectsToDrop.isEmpty()) {
            LOG.info("Did not find any objects to drop.");
            return;
        }

        LOG.info("Dropping %s objects.", objectsToDrop.size());
        LOG.info("Dropping: %s", objectsToDrop.stream().map(snowflakeObject -> snowflakeObject.schemaName + "." + snowflakeObject.tableName).collect(joining(", ")));
        try (Handle handle = Jdbi.create(snowflakeServer.getConnection()).open()) {
            handle.execute("USE ROLE " + ROLE);
            handle.execute("USE WAREHOUSE " + TEST_WAREHOUSE);
            handle.execute("USE DATABASE " + TEST_DATABASE);
            objectsToDrop.forEach(snowflakeObject -> {
                String dropStatement = getDropStatement(snowflakeObject.schemaName, snowflakeObject.tableName, snowflakeObject.tableType);
                LOG.info("Executing: %s", dropStatement);
                handle.execute(dropStatement);
            });
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }

        logObjectsCount(schemaName);
    }

    @DataProvider
    public static Object[][] cleanUpSchemasDataProvider()
    {
        return new Object[][] {
                {TEST_SCHEMA},
        };
    }

    private void logObjectsCount(String schemaName)
    {
        try (Handle handle = Jdbi.create(snowflakeServer.getConnection()).open()) {
            handle.execute("USE ROLE " + ROLE);
            handle.execute("USE WAREHOUSE " + TEST_WAREHOUSE);
            handle.execute("USE DATABASE " + TEST_DATABASE);
            handle.createQuery("" +
                    "SELECT table_type, count(*) AS c " +
                    "FROM INFORMATION_SCHEMA.TABLES " +
                    "WHERE table_catalog = :table_catalog AND lower(table_schema) = :table_schema " +
                    "GROUP BY table_type")
                    .bind("table_catalog", TEST_DATABASE)
                    .bind("table_schema", schemaName)
                    .map((rs, ctx) -> (Entry<String, Long>) new SimpleImmutableEntry<>(rs.getString("TABLE_TYPE"), rs.getLong("C")))
                    .list()
                    .forEach(entry -> LOG.info("Schema '%s' contains %s objects of type '%s'", schemaName, entry.getValue(), entry.getKey()));
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getDropStatement(String schemaName, String objectName, String objectType)
    {
        switch (objectType) {
            case "BASE TABLE":
                return format("DROP TABLE IF EXISTS %s.%s", quoted(schemaName), quoted(objectName));
            case "VIEW":
                return format("DROP VIEW IF EXISTS %s.%s", quoted(schemaName), quoted(objectName));
            default:
                throw new IllegalArgumentException("Unexpected object type " + objectType);
        }
    }

    private static String quoted(String identifier)
    {
        return IDENTIFIER_QUOTE + identifier.replace(IDENTIFIER_QUOTE, IDENTIFIER_QUOTE + IDENTIFIER_QUOTE) + IDENTIFIER_QUOTE;
    }

    private static class SnowflakeObject
    {
        private final String schemaName;
        private final String tableName;
        private final String tableType;

        private SnowflakeObject(String schemaName, String tableName, String tableType)
        {
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.tableType = tableType;
        }
    }
}
