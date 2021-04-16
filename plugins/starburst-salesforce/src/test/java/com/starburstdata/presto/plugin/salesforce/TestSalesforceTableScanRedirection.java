/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.salesforce;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.starburstdata.presto.redirection.AbstractTableScanRedirectionTest;
import com.starburstdata.presto.redirection.RedirectedTable;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static org.testng.Assert.assertEquals;

public class TestSalesforceTableScanRedirection
        extends AbstractTableScanRedirectionTest
{
    @Override
    protected QueryRunner createQueryRunner(Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return SalesforceQueryRunner.builder()
                .setTables(tables)
                .addConnectorProperties(getRedirectionProperties("salesforce", "salesforce"))
                .build();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = createQueryRunner(tables);
        queryRunner.installPlugin(new MemoryPlugin());
        queryRunner.createCatalog("memory", "memory", ImmutableMap.of());
        queryRunner.execute("CREATE SCHEMA memory.target_schema");
        queryRunner.execute(
                redirectionDisabled(queryRunner.getDefaultSession()),
                "CREATE TABLE memory.target_schema.nation__c AS " +
                        "SELECT nationkey__c, name__c, regionkey__c FROM nation__c WHERE regionkey__c = 0 LIMIT 1");
        return queryRunner;
    }

    @Override
    protected Map<String, String> getRedirectionProperties(String sourceCatalogName, String sourceSchema)
            throws IOException
    {
        RedirectedTable redirectedTable = new RedirectedTable(
                "memory",
                "target_schema",
                "nation__c",
                Optional.of(ImmutableSet.of("nationkey__c", "name__c", "regionkey__c")));
        return getRedirectionProperties(sourceCatalogName, sourceSchema, ImmutableMap.of("nation__c", redirectedTable));
    }

    @Test
    @Override
    public void testRedirection()
    {
        assertEquals(computeActual("SELECT nationkey__c, name__c, regionkey__c FROM nation__c").getRowCount(), 1);
        assertEquals(computeActual("SELECT nationkey__c FROM nation__c LIMIT 200").getRowCount(), 1);
        assertEquals(computeActual("SELECT COUNT(*) FROM nation__c").getOnlyValue(), 1L);
        assertEquals(computeActual("SELECT nationkey__c FROM nation__c WHERE regionkey__c >= 0").getRowCount(), 1);
        assertEquals(computeActual("SELECT nationkey__c, name__c, sum(regionkey__c) FROM nation__c GROUP BY 1, 2").getRowCount(), 1);
    }

    @Test
    @Override
    public void testNoRedirection()
    {
        assertEquals(computeActual(
                redirectionDisabled(getSession()),
                "SELECT nationkey__c, name__c, regionkey__c FROM nation__c").getRowCount(), 25);
        // no redirection mapping for region table
        assertEquals(computeActual("SELECT * from region__c").getRowCount(), 5);
        // target table does not contain all source table columns
        assertEquals(computeActual("SELECT * FROM nation__c").getRowCount(), 25);
        // Projected column comment not present in target table
        assertEquals(computeActual("SELECT nationkey__c, name__c, comment__c FROM nation__c").getRowCount(), 25);
        // Predicate column comment not present in target table
        assertEquals(computeActual("SELECT nationkey__c FROM nation__c WHERE comment__c IS NOT NULL").getRowCount(), 25);
        // Aggregation column comment not present in target table
        assertEquals(computeActual("SELECT nationkey__c, count(comment__c) FROM nation__c GROUP BY 1").getRowCount(), 25);
    }
}
