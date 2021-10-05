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
package io.trino.plugin.elasticsearch;

import io.trino.plugin.elasticsearch.client.ElasticsearchClient;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;

import static io.trino.plugin.elasticsearch.ElasticsearchQueryBuilder.buildSearchQuery;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

class CountQueryPageSource
        implements ConnectorPageSource
{
    // This implementation of the page source is used whenever a query doesn't reference any columns
    // from the ES table. We need to limit the number of rows per page in case there are projections
    // in the query that can cause page sizes to explode. For example: SELECT rand() FROM some_table
    private static final int BATCH_SIZE = 10000;

    private final long readTimeNanos;
    private long remaining;

    public CountQueryPageSource(ElasticsearchClient client, ElasticsearchTableHandle table, ElasticsearchSplit split)
    {
        requireNonNull(client, "client is null");
        requireNonNull(table, "table is null");
        requireNonNull(split, "split is null");

        long start = System.nanoTime();
        long count = client.count(
                split.getIndex(),
                split.getShard(),
                buildSearchQuery(table.getConstraint().transformKeys(ElasticsearchColumnHandle.class::cast), table.getQuery(), table.getRegexes()));
        readTimeNanos = System.nanoTime() - start;

        if (table.getLimit().isPresent()) {
            count = Math.min(table.getLimit().getAsLong(), count);
        }

        remaining = count;
    }

    @Override
    public boolean isFinished()
    {
        return remaining == 0;
    }

    @Override
    public Page getNextPage()
    {
        int batch = toIntExact(Math.min(BATCH_SIZE, remaining));
        remaining -= batch;

        return new Page(batch);
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
    }
}
