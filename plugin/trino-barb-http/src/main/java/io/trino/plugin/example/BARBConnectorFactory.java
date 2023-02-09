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
package io.trino.plugin.example;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.trino.plugin.base.TypeDeserializerModule;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.HashMap;
import java.util.Map;

import static io.trino.plugin.base.Versions.checkSpiVersion;
import static java.util.Objects.requireNonNull;

public class BARBConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "example-http";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");
        checkSpiVersion(context, this);

        // A plugin is not required to use Guice; it is just very convenient
        Bootstrap app = new Bootstrap(
                new JsonModule(),
                new TypeDeserializerModule(context.getTypeManager()),
                new ExampleModule());
        requiredConfig = new HashMap<String, String>() {{
                put("Authorization", "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNjc1OTc1OTg4LCJpYXQiOjE2NzU5MzI3ODgsImp0aSI6IjFjMGY0NzMyNzUwMDQ4OTdhZTc3Zjg5ZWJlNjJmZDYyIiwidXNlcl9pZCI6IjljMTAzNmI2LTM1NTAtNDhhYS05YjkzLTBjNjU1NGVmMjcwZCJ9.jRN7ugUbopVfkqZA2yAOZnk1n07a4OR6HZcnSQHDYWA");
            }};
        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(requiredConfig)
                .initialize();

        return injector.getInstance(BARBConnector.class);
    }
}
