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
package io.trino.operator.dynamicfiltering;

import com.google.inject.Inject;
import io.trino.Session;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.RecordPageSource;

import java.util.List;

import static io.trino.SystemSessionProperties.getDynamicRowFilterSelectivityThreshold;
import static io.trino.SystemSessionProperties.getDynamicRowFilteringWaitTimeout;
import static io.trino.SystemSessionProperties.isDynamicRowFilteringEnabled;
import static java.util.Objects.requireNonNull;

public class DynamicRowFilteringPageSourceProvider
{
    private final DynamicPageFilterCache dynamicPageFilterCache;

    @Inject
    public DynamicRowFilteringPageSourceProvider(DynamicPageFilterCache dynamicPageFilterCache)
    {
        this.dynamicPageFilterCache = requireNonNull(dynamicPageFilterCache, "dynamicPageFilterCollector is null");
    }

    public ConnectorPageSource createPageSource(ConnectorPageSource delegatePageSource, Session session, List<ColumnHandle> columns, DynamicFilter dynamicFilter)
    {
        if (dynamicFilter.isComplete() && dynamicFilter.getCurrentPredicate().isAll()) {
            return delegatePageSource;
        }
        if (delegatePageSource instanceof RecordPageSource || !isDynamicRowFilteringEnabled(session)) {
            return delegatePageSource;
        }

        return new DynamicRowFilteringPageSource(
                delegatePageSource,
                getDynamicRowFilterSelectivityThreshold(session),
                getDynamicRowFilteringWaitTimeout(session),
                columns,
                dynamicPageFilterCache.getDynamicPageFilter(dynamicFilter, columns));
    }
}
