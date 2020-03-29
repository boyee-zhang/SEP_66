/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.oracle;

import io.prestosql.plugin.jdbc.JdbcHandleResolver;
import io.prestosql.spi.connector.ConnectorSplit;

public class OracleHandleResolver
        extends JdbcHandleResolver
{
    @Override
    public Class<? extends ConnectorSplit> getSplitClass()
    {
        return OracleSplit.class;
    }
}
