/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.oracle;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.SharedResource.Lease;
import org.junit.jupiter.api.Test;

import static io.trino.tpch.TpchTable.NATION;

public class TestOracleLicenseProtection
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Lease<TestingStarburstOracleServer> oracleServer = closeAfterClass(TestingStarburstOracleServer.getInstance());
        return OracleQueryRunner.builder(oracleServer)
                .withTables(ImmutableList.of(NATION))
                .withUnlockEnterpriseFeatures(false)
                .build();
    }

    @Test
    public void testLicenseProtectionOfParallelismViaSessionProperty()
    {
        Session noParallelismSession = Session.builder(getSession())
                .setCatalogSessionProperty("oracle", "parallelism_type", "no_parallelism")
                .build();

        Session partitionsParallelismSession = Session.builder(getSession())
                .setCatalogSessionProperty("oracle", "parallelism_type", "partitions")
                .build();

        assertQuery(noParallelismSession, "SELECT * FROM nation");
        assertQuery(partitionsParallelismSession, "SELECT * FROM nation");
    }
}