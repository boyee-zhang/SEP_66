/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.tests.product;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.Requires;
import io.trino.tempto.fulfillment.table.hive.tpch.ImmutableTpchTablesRequirements;
import io.trino.tempto.hadoop.hdfs.HdfsClient;
import io.trino.tempto.query.QueryExecutor;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URISyntaxException;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.HDFS_IMPERSONATION;
import static io.trino.tests.product.TestGroups.HDFS_NO_IMPERSONATION;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.connectToTrino;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

@Requires(ImmutableTpchTablesRequirements.ImmutableNationTable.class)
public class TestImpersonation
        extends ProductTest
{
    private QueryExecutor aliceExecutor;

    @Inject
    private HdfsClient hdfsClient;

    @Inject
    @Named("databases.alice@presto.jdbc_user")
    private String aliceJdbcUser;

    // The value for configuredHdfsUser is profile dependent
    // For non-Kerberos environments this variable will be equal to -DHADOOP_USER_NAME as set in jvm.config
    // For Kerberized environments this variable will be equal to the hive.hdfs.trino.principal property as set in hive.properties
    @Inject
    @Named("databases.presto.configured_hdfs_user")
    private String configuredHdfsUser;

    @BeforeTestWithContext
    public void setup()
    {
        aliceExecutor = connectToTrino("alice@presto");
    }

    @Test(groups = {HDFS_NO_IMPERSONATION, PROFILE_SPECIFIC_TESTS})
    public void testHdfsImpersonationDisabled()
    {
        String tableName = "check_hdfs_impersonation_disabled";
        checkTableOwner(tableName, configuredHdfsUser, aliceExecutor);
    }

    @Test(groups = {HDFS_IMPERSONATION, PROFILE_SPECIFIC_TESTS})
    public void testHdfsImpersonationEnabled()
    {
        String tableName = "check_hdfs_impersonation_enabled";
        checkTableOwner(tableName, aliceJdbcUser, aliceExecutor);
        checkTableGroup(tableName, aliceExecutor);
    }

    @Test(groups = {HDFS_IMPERSONATION, PROFILE_SPECIFIC_TESTS})
    public void testViewsWithSecurityDefiner()
    {
        aliceExecutor.executeQuery("DROP VIEW IF EXISTS hive_test_view_impersonation_security_definer");
        aliceExecutor.executeQuery("CREATE VIEW hive_test_view_impersonation_security_definer SECURITY DEFINER AS SELECT * FROM nation");
        assertThat(onTrino().executeQuery("SELECT * FROM hive_test_view_impersonation_security_definer")).hasRowsCount(25);
        assertThat(onTrino().executeQuery("SELECT n_nationkey, n_name, n_regionkey, n_comment FROM hive_test_view_impersonation_security_definer WHERE n_nationkey < 3"))
                .containsOnly(
                        row(0, "ALGERIA", 0, " haggle. carefully final deposits detect slyly agai"),
                        row(1, "ARGENTINA", 1, "al foxes promise slyly according to the regular accounts. bold requests alon"),
                        row(2, "BRAZIL", 1, "y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special "));
    }

    @Test(groups = {HDFS_IMPERSONATION, PROFILE_SPECIFIC_TESTS})
    public void testViewsWithSecurityInvoker()
    {
        aliceExecutor.executeQuery("DROP VIEW IF EXISTS hive_test_view_impersonation_security_invoker");
        aliceExecutor.executeQuery("CREATE VIEW hive_test_view_impersonation_security_invoker SECURITY DEFINER AS SELECT * FROM nation");

        assertThatThrownBy(() -> onTrino().executeQuery("SELECT * FROM hive_test_view_impersonation_security_invoker"))
                .hasMessageContaining("Failed to list directory: *");
    }

    private static String getTableLocation(QueryExecutor executor, String tableName)
    {
        String location = getOnlyElement(executor.executeQuery(format("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*$', '') FROM %s", tableName)).column(1));
        if (location.startsWith("hdfs://")) {
            try {
                URI uri = new URI(location);
                return uri.getPath();
            }
            catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }
        return location;
    }

    private void checkTableOwner(String tableName, String expectedOwner, QueryExecutor executor)
    {
        executor.executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        executor.executeQuery(format("CREATE TABLE %s AS SELECT 'abc' c", tableName));
        String tableLocation = getTableLocation(executor, tableName);
        String owner = hdfsClient.getOwner(tableLocation);
        assertEquals(owner, expectedOwner);
        executor.executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
    }

    private void checkTableGroup(String tableName, QueryExecutor executor)
    {
        executor.executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        executor.executeQuery(format("CREATE TABLE %s AS SELECT 'abc' c", tableName));
        String tableLocation = getTableLocation(executor, tableName);
        String warehouseLocation = tableLocation.substring(0, tableLocation.lastIndexOf("/"));

        // user group info of warehouseLocation(/user/hive/warehouse) is alice:supergroup
        // tableLocation is /user/hive/warehouse/check_hdfs_impersonation_enabled. When create table,
        // user alice doesn't have permission to setOwner, so the user group info should be alice:supergroup still
        String warehouseLocationGroup = hdfsClient.getGroup(warehouseLocation);
        String tableLocationGroup = hdfsClient.getGroup(warehouseLocation);
        assertEquals(tableLocationGroup, warehouseLocationGroup);
    }
}
