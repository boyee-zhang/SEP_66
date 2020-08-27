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
package io.prestosql.plugin.hive;

import io.prestosql.plugin.hive.azure.HiveAzureConfig;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;

public class TestHiveFileSystemAbfsOAuth
        extends AbstractTestHiveFileSystemAbfs
{
    private String clientEndpoint;
    private String clientId;
    private String clientSecret;

    @Parameters({
            "hive.hadoop2.metastoreHost",
            "hive.hadoop2.metastorePort",
            "hive.hadoop2.databaseName",
            "test.hive.azure.abfs.container",
            "test.hive.azure.abfs.storage-account",
            "test.hive.azure.abfs.test-directory",
            "hive.azure.abfs.oauth-client-endpoint",
            "hive.azure.abfs.oauth-client-id",
            "hive.azure.abfs.oauth-client-secret",
    })
    @BeforeClass
    public void setup(
            String host,
            int port,
            String databaseName,
            String container,
            String account,
            String testDirectory,
            String clientEndpoint,
            String clientId,
            String clientSecret)
    {
        this.clientEndpoint = checkParameter(clientEndpoint, "client endpoint");
        this.clientId = checkParameter(clientId, "client ID");
        this.clientSecret = checkParameter(clientSecret, "client secret");
        super.setup(host, port, databaseName, container, account, testDirectory);
    }

    @Override
    protected HiveAzureConfig getConfig()
    {
        return new HiveAzureConfig()
                .setAbfsOAuthClientEndpoint(clientEndpoint)
                .setAbfsOAuthClientId(clientId)
                .setAbfsOAuthClientSecret(clientSecret);
    }
}
