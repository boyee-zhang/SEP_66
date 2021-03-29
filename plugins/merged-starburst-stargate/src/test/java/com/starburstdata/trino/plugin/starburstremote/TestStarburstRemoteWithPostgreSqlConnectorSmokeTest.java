/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */

package com.starburstdata.trino.plugin.starburstremote;

import io.trino.plugin.jdbc.BaseJdbcConnectorSmokeTest;
import io.trino.plugin.postgresql.TestingPostgreSqlServer;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;

import java.util.Map;
import java.util.Optional;

import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteQueryRunner.createStarburstRemoteQueryRunner;
import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteQueryRunner.createStarburstRemoteQueryRunnerWithPostgreSql;
import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteQueryRunner.starburstRemoteConnectionUrl;

public class TestStarburstRemoteWithPostgreSqlConnectorSmokeTest
        extends BaseJdbcConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingPostgreSqlServer postgreSqlServer = closeAfterClass(new TestingPostgreSqlServer());

        DistributedQueryRunner remoteStarburst = closeAfterClass(createStarburstRemoteQueryRunnerWithPostgreSql(
                postgreSqlServer,
                Map.of(),
                Map.of(
                        "connection-url", postgreSqlServer.getJdbcUrl(),
                        "connection-user", postgreSqlServer.getUser(),
                        "connection-password", postgreSqlServer.getPassword()),
                REQUIRED_TPCH_TABLES,
                Optional.empty()));

        return createStarburstRemoteQueryRunner(
                false,
                Map.of(),
                Map.of(
                        "connection-url", starburstRemoteConnectionUrl(remoteStarburst, "postgresql"),
                        "allow-drop-table", "true"));
    }

    @Override
    @SuppressWarnings("DuplicateBranchesInSwitch")
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_TOPN_PUSHDOWN:
                return true;

            case SUPPORTS_CREATE_TABLE:
            case SUPPORTS_RENAME_TABLE:
                // Writes are not enabled
                return false;

            case SUPPORTS_CREATE_VIEW:
                // TODO Add support in Remote connector (https://starburstdata.atlassian.net/browse/PRESTO-4795)
                return false;

            case SUPPORTS_INSERT:
            case SUPPORTS_DELETE:
                // Writes are not enabled
                return false;

            case SUPPORTS_ARRAY:
                // TODO Add support in Remote connector (https://starburstdata.atlassian.net/browse/PRESTO-4798)
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }
}
