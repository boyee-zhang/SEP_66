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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.sqlserver.TestingSqlServer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerQueryRunner.ALICE_USER;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerQueryRunner.BOB_USER;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerQueryRunner.CHARLIE_USER;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerQueryRunner.UNKNOWN_USER;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerQueryRunner.createSession;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerQueryRunner.createStarburstSqlServerQueryRunner;

public class TestSqlServerImpersonation
        extends AbstractTestQueryFramework
{
    private TestingSqlServer sqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        sqlServer = new TestingSqlServer();
        sqlServer.start();
        return createStarburstSqlServerQueryRunner(
                sqlServer,
                true,
                session -> createSession(ALICE_USER),
                ImmutableMap.of("sqlserver.impersonation.enabled", "true"),
                ImmutableList.of());
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        sqlServer.close();
    }

    @Test
    public void testUserImpersonation()
    {
        assertQuery(createSession(ALICE_USER), "SELECT * FROM user_context", "SELECT 'alice_login', 'SA', 'alice_login', 'alice', 'alice'");
        assertQuery(createSession(BOB_USER), "SELECT * FROM user_context", "SELECT 'bob_login', 'SA', 'bob_login', 'bob', 'bob'");
        assertQueryFails(createSession(CHARLIE_USER), "SELECT * FROM user_context", "line 1:15: Table 'sqlserver.dbo.user_context' does not exist");
        assertQueryFails(
                createSession(UNKNOWN_USER),
                "SELECT count(*) FROM orders",
                "Cannot execute as the database principal because the principal \"non_existing_user\" does not exist, " +
                        "this type of principal cannot be impersonated, or you do not have permission.");
    }
}
