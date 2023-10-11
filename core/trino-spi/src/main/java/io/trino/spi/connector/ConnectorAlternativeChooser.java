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
package io.trino.spi.connector;

import java.util.List;

import static java.util.Objects.requireNonNull;

public interface ConnectorAlternativeChooser
{
    /**
     * Chooses a page source from provided alternatives for a given split.
     * Preferable the chosen alternative is the best performant one.
     */
    Choice chooseAlternative(
            ConnectorSession session,
            ConnectorSplit split,
            List<ConnectorTableHandle> alternatives);

    /**
     * Returns whether the engine should perform dynamic row filtering on top of the returned page source.
     * While dynamic row filtering can be extended to any connector, it is currently restricted to data lake connectors.
     */
    default boolean shouldPerformDynamicRowFiltering()
    {
        return false;
    }

    record Choice(int chosenTableHandleIndex, ConnectorAlternativePageSourceProvider pageSourceProvider)
    {
        public Choice
        {
            requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        }
    }
}
