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

import com.starburstdata.presto.plugin.jdbc.dynamicfiltering.AbstractDynamicFilteringTest;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteQueryRunner.createStarburstRemoteQueryRunner;
import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteQueryRunner.createStarburstRemoteQueryRunnerWithMemory;
import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteQueryRunner.starburstRemoteConnectionUrl;
import static io.trino.tpch.TpchTable.ORDERS;

public class TestStarburstRemoteDynamicFilteringWritesEnabled
        extends AbstractDynamicFilteringTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner remoteStarburst = closeAfterClass(createStarburstRemoteQueryRunnerWithMemory(
                Map.of(),
                List.of(ORDERS),
                Optional.empty()));
        return createStarburstRemoteQueryRunner(
                true,
                Map.of(),
                Map.of(
                        "connection-url", starburstRemoteConnectionUrl(remoteStarburst, "memory"),
                        "allow-drop-table", "true"));
    }

    @Override
    protected boolean supportsSplitDynamicFiltering()
    {
        // JDBC connectors always generate single split
        // TODO https://starburstdata.atlassian.net/browse/SEP-4769 revisit in parallel Starburst Remote connector
        return false;
    }
}
