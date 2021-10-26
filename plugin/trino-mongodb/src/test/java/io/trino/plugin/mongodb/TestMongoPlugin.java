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
package io.trino.plugin.mongodb;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.type.Type;
import io.trino.testing.TestingConnectorContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.mongodb.ObjectIdType.OBJECT_ID;
import static org.testng.Assert.assertEquals;

public class TestMongoPlugin
{
    private MongoServer server;
    private String connectionString;

    @BeforeClass
    public void start()
    {
        server = new MongoServer();
        connectionString = server.getConnectionString().toString();
    }

    @Test
    public void testCreateConnector()
    {
        MongoPlugin plugin = new MongoPlugin();

        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        Connector connector = factory.create("test", ImmutableMap.of("mongodb.connection-string", connectionString), new TestingConnectorContext());

        Type type = getOnlyElement(plugin.getTypes());
        assertEquals(type, OBJECT_ID);

        connector.shutdown();
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        server.close();
    }
}
