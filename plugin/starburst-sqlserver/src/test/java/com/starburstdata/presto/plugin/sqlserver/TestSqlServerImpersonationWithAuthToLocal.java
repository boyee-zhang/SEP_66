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
import io.prestosql.plugin.sqlserver.TestingSqlServer;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static com.google.common.io.Resources.getResource;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerQueryRunner.ALICE_USER;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerQueryRunner.BOB_USER;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerQueryRunner.CHARLIE_USER;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerQueryRunner.UNKNOWN_USER;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerQueryRunner.createSession;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerQueryRunner.createStarburstSqlServerQueryRunner;

@Test
public class TestSqlServerImpersonationWithAuthToLocal
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
                session -> createSession(ALICE_USER + "/admin@company.com"),
                ImmutableMap.of(
                        "sqlserver.impersonation.enabled", "true",
                        "auth-to-local.config-file", getResource("auth-to-local.json").getPath()),
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
        assertQuery(
                createSession(ALICE_USER + "/admin@company.com"),
                "SELECT * FROM user_context",
                "SELECT 'alice_login', 'SA', 'alice_login', 'alice', 'alice'");
        assertQuery(
                createSession(BOB_USER + "/user@company.com"),
                "SELECT * FROM user_context",
                "SELECT 'bob_login', 'SA', 'bob_login', 'bob', 'bob'");
        assertQueryFails(
                createSession(CHARLIE_USER + "/hr@company.com"),
                "SELECT * FROM user_context",
                "line 1:15: Table sqlserver.dbo.user_context does not exist");
        assertQueryFails(
                createSession(UNKNOWN_USER + "/marketing@company.com"),
                "SELECT * FROM user_context",
                ".*database principal .* does not exist.*");
        assertQueryFails(
                createSession(UNKNOWN_USER + "/marketing@other.com"),
                "SELECT * FROM user_context",
                "No auth-to-local rule was found for user \\[non_existing_user/marketing@other.com\\] and principal is missing");
        assertQueryFails(
                createSession(BOB_USER),
                "SELECT * FROM user_context",
                "No auth-to-local rule was found for user \\[bob\\] and principal is missing");
    }
}
