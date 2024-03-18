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
package io.trino.plugin.varada.dispatcher;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.expression.rewrite.ExpressionService;
import io.trino.spi.connector.ConnectorMetadata;

import static java.util.Objects.requireNonNull;

@Singleton
public class DispatcherMetadataFactory
{
    private final ExpressionService expressionService;
    private final DispatcherTableHandleBuilderProvider dispatcherTableHandleBuilderProvider;
    private final GlobalConfiguration globalConfiguration;

    @Inject
    public DispatcherMetadataFactory(
            ExpressionService expressionService,
            DispatcherTableHandleBuilderProvider dispatcherTableHandleBuilderProvider,
            GlobalConfiguration globalConfiguration)
    {
        this.expressionService = requireNonNull(expressionService);
        this.dispatcherTableHandleBuilderProvider = requireNonNull(dispatcherTableHandleBuilderProvider);
        this.globalConfiguration = requireNonNull(globalConfiguration);
    }

    public DispatcherMetadata createMetadata(ConnectorMetadata connectorMetadata)
    {
        return new DispatcherMetadata(
                connectorMetadata,
                expressionService,
                dispatcherTableHandleBuilderProvider,
                globalConfiguration);
    }
}
