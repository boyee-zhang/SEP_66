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

import com.google.common.collect.ImmutableMap;
import com.starburstdata.presto.testing.DataProviders;
import com.starburstdata.presto.testing.SessionMutator;
import io.trino.Session;
import io.trino.plugin.sqlserver.TestSqlServerConnectorTest;
import io.trino.plugin.sqlserver.TestingSqlServer;
import io.trino.testing.QueryRunner;
import io.trino.testng.services.Flaky;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static com.starburstdata.presto.plugin.sqlserver.StarburstCommonSqlServerSessionProperties.BULK_COPY_FOR_WRITE;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerQueryRunner.CATALOG;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerQueryRunner.createStarburstSqlServerQueryRunner;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerSessionProperties.BULK_COPY_FOR_WRITE_LOCK_DESTINATION_TABLE;
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.NON_TRANSACTIONAL_INSERT;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestStarburstSqlServerConnectorTest
        extends TestSqlServerConnectorTest
{
    private final SessionMutator sessionMutator = new SessionMutator(super::getSession);

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        sqlServer = closeAfterClass(new TestingSqlServer());
        return createStarburstSqlServerQueryRunner(sqlServer, false, ImmutableMap.of(), REQUIRED_TPCH_TABLES);
    }

    @Override
    protected Session getSession()
    {
        return sessionMutator.getSession();
    }

    @Override
    protected Session joinPushdownEnabled(Session session)
    {
        return Session.builder(super.joinPushdownEnabled(session))
                // strategy is AUTOMATIC by default and would not work for certain test cases (even if statistics are collected)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "join_pushdown_strategy", "EAGER")
                .build();
    }

    @Flaky(issue = "fn_dblog() returns information only about the active portion of the transaction log, therefore it is flaky", match = ".*")
    @Test(dataProviderClass = DataProviders.class, dataProvider = "doubleTrueFalse")
    public void testCreateTableAsSelectWriteBulkiness(boolean bulkCopyForWrite, boolean bulkCopyForWriteLockDestinationTable)
            throws SQLException
    {
        String table = "bulk_copy_ctas_" + randomTableSuffix();
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(CATALOG, BULK_COPY_FOR_WRITE, Boolean.toString(bulkCopyForWrite))
                .setCatalogSessionProperty(CATALOG, BULK_COPY_FOR_WRITE_LOCK_DESTINATION_TABLE, Boolean.toString(bulkCopyForWriteLockDestinationTable))
                .build();

        // there should be enough rows in source table to minimal logging be enabled. `nation` table is too small.
        assertQuerySucceeds(session, format("CREATE TABLE %s as SELECT * FROM tpch.tiny.customer", table));

        // check whether minimal logging was applied.
        // Unlike fully logged operations, which use the transaction log to keep track of every row change,
        // minimally logged operations keep track of extent allocations and meta-data changes only.
        assertThat(getTableOperationsCount("LOP_INSERT_ROWS", table))
                .isEqualTo(bulkCopyForWrite && bulkCopyForWriteLockDestinationTable ? 0 : 1500);

        // check that there are no locks remained on the target table after bulk copy
        assertQuery("SELECT count(*) FROM " + table, "SELECT count(*) FROM customer");
        assertUpdate(format("INSERT INTO %s SELECT * FROM tpch.tiny.customer LIMIT 1", table), 1);
        assertQuery("SELECT count(*) FROM " + table, "SELECT count(*) + 1 FROM customer");

        assertUpdate("DROP TABLE " + table);
    }

    @Flaky(issue = "fn_dblog() returns information only about the active portion of the transaction log, therefore it is flaky", match = ".*")
    @Test(dataProviderClass = DataProviders.class, dataProvider = "tripleTrueFalse")
    public void testInsertWriteBulkiness(boolean nonTransactionalInsert, boolean bulkCopyForWrite, boolean bulkCopyForWriteLockDestinationTable)
            throws SQLException
    {
        String table = "bulk_copy_insert_" + randomTableSuffix();
        assertQuerySucceeds(format("CREATE TABLE %s as SELECT * FROM tpch.tiny.customer WHERE 0 = 1", table));
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(CATALOG, NON_TRANSACTIONAL_INSERT, Boolean.toString(nonTransactionalInsert))
                .setCatalogSessionProperty(CATALOG, BULK_COPY_FOR_WRITE, Boolean.toString(bulkCopyForWrite))
                .setCatalogSessionProperty(CATALOG, BULK_COPY_FOR_WRITE_LOCK_DESTINATION_TABLE, Boolean.toString(bulkCopyForWriteLockDestinationTable))
                .build();

        // there should be enough rows in source table to minimal logging be enabled. `nation` table is too small.
        assertQuerySucceeds(session, format("INSERT INTO %s SELECT * FROM tpch.tiny.customer", table));

        // check whether minimal logging was applied.
        // Unlike fully logged operations, which use the transaction log to keep track of every row change,
        // minimally logged operations keep track of extent allocations and meta-data changes only.
        assertThat(getTableOperationsCount("LOP_INSERT_ROWS", table))
                .isEqualTo(bulkCopyForWrite && bulkCopyForWriteLockDestinationTable ? 0 : 1500);

        // check that there are no locks remained on the target table after bulk copy
        assertQuery("SELECT count(*) FROM " + table, "SELECT count(*) FROM customer");
        assertUpdate(format("INSERT INTO %s SELECT * FROM tpch.tiny.customer LIMIT 1", table), 1);
        assertQuery("SELECT count(*) FROM " + table, "SELECT count(*) + 1 FROM customer");

        assertUpdate("DROP TABLE " + table);
    }

    private int getTableOperationsCount(String operation, String table)
            throws SQLException
    {
        try (Connection connection = sqlServer.createConnection();
                Handle handle = Jdbi.open(connection)) {
            // fn_dblog() function only returns information about the active portion of the transaction log such as open transactions or the last activity
            // therefore tests which use this function are flaky, but it's almost not possible to reproduce this flakiness.
            // There was no better option found to test if minimal logging was enabled than to query `LOP_INSERT_ROWS` from fn_dblog(NULL,NULL)
            return handle.createQuery("" +
                    "SELECT COUNT(*) as cnt " +
                    "FROM fn_dblog(NULL,NULL) " +
                    "WHERE Operation = :operation " +
                    "AND AllocUnitName = CONCAT('dbo.', :table_name)")
                    .bind("operation", operation)
                    .bind("table_name", table)
                    .mapTo(Integer.class)
                    .one();
        }
    }
}
