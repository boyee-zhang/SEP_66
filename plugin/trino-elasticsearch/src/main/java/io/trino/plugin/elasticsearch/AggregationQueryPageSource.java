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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.elasticsearch.client.ElasticsearchClient;
import io.trino.plugin.elasticsearch.decoders.Decoder;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.elasticsearch.ElasticsearchQueryBuilder.buildAggregationQuery;
import static io.trino.plugin.elasticsearch.ElasticsearchQueryBuilder.buildSearchQuery;
import static java.util.Objects.requireNonNull;

public class AggregationQueryPageSource
        implements ConnectorPageSource
{
    private long readTimeNanos;
    private final List<Decoder> decoders;
    private final BlockBuilder[] columnBuilders;
    private final List<ElasticsearchColumnHandle> columns;
    private final ElasticsearchClient client;
    private final ElasticsearchTableHandle table;
    private final ElasticsearchSplit split;
    private final int pageSize;
    // the after parameter that composite aggregation used to fetch by page
    private Optional<Map<String, Object>> after;
    private boolean fetched;
    private static final SearchHit noUsed = new SearchHit(0);

    public AggregationQueryPageSource(
            ElasticsearchClient client,
            ElasticsearchTableHandle table,
            ElasticsearchSplit split,
            List<ElasticsearchColumnHandle> columns,
            int pageSize)
    {
        requireNonNull(client, "client is null");
        requireNonNull(table, "table is null");
        requireNonNull(split, "split is null");
        requireNonNull(columns, "columns is null");
        this.client = client;
        this.table = table;
        this.split = split;
        this.pageSize = pageSize <= 0 ? 1000 : pageSize;
        this.columns = columns;
        this.after = Optional.empty();
        columnBuilders = columns.stream()
                .map(ElasticsearchColumnHandle::getType)
                .map(type -> type.createBlockBuilder(null, 1))
                .toArray(BlockBuilder[]::new);
        decoders = createDecoders(columns);
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return fetched && after.isEmpty();
    }

    @Override
    public Page getNextPage()
    {
        long start = System.nanoTime();
        SearchResponse searchResponse = client.beginSearch(
                split.getIndex(),
                split.getShard(),
                buildSearchQuery(table.getConstraint().transform(ElasticsearchColumnHandle.class::cast), table.getQuery()),
                Optional.ofNullable(
                        buildAggregationQuery(table.getTermAggregations(), table.getMetricAggregations(), Optional.of(pageSize), after)),
                Optional.empty(),
                Collections.emptyList(),
                Optional.empty(),
                table.getLimit());
        readTimeNanos += System.nanoTime() - start;
        fetched = true;
        List<Map<String, Object>> flatResult = getResult(searchResponse);
        after = extractAfter(searchResponse);
        for (Map<String, Object> result : flatResult) {
            for (int i = 0; i < columns.size(); i++) {
                String key = columns.get(i).getName();
                decoders.get(i).decode(noUsed, () -> result.get(key), columnBuilders[i]);
            }
        }
        Block[] blocks = new Block[columnBuilders.length];
        for (int i = 0; i < columnBuilders.length; i++) {
            blocks[i] = columnBuilders[i].build();
            columnBuilders[i] = columnBuilders[i].newBlockBuilderLike(null);
        }
        return new Page(blocks);
    }

    // TODO consider support 6.1/6.2
    //  6.1/6.2: no after_key, just use last bucket as the next retrieve parameter
    //  >= 6.3: the result contains after_key, use after_key as the next retrieve
    private Optional<Map<String, Object>> extractAfter(SearchResponse searchResponse)
    {
        Aggregations aggregations = searchResponse.getAggregations();
        if (aggregations == null) {
            return Optional.empty();
        }
        for (Aggregation agg : aggregations) {
            if (agg instanceof CompositeAggregation) {
                CompositeAggregation compositeAggregation = (CompositeAggregation) agg;
                return Optional.ofNullable(compositeAggregation.afterKey());
            }
        }
        return Optional.empty();
    }

    private List<Map<String, Object>> getResult(SearchResponse searchResponse)
    {
        Aggregations aggregations = searchResponse.getAggregations();
        if (aggregations == null) {
            return Collections.emptyList();
        }
        Map<String, Object> singleValueMap = new LinkedHashMap<>();
        ImmutableList.Builder<Map<String, Object>> result = ImmutableList.builder();
        boolean hasBucketsAggregation = false;
        boolean hasMetricAggregation = false;
        for (Aggregation agg : aggregations) {
            if (agg instanceof CompositeAggregation) {
                hasBucketsAggregation = true;
                if (hasMetricAggregation) {
                    throw new UnsupportedOperationException("Impossible merge different bucket and metric aggregation, this must not be appear.");
                }
                CompositeAggregation compositeAggregation = (CompositeAggregation) agg;
                for (CompositeAggregation.Bucket bucket : compositeAggregation.getBuckets()) {
                    Map<String, Object> line = new HashMap<>();
                    line.putAll(bucket.getKey());
                    for (Aggregation metricAgg : bucket.getAggregations()) {
                        if (metricAgg instanceof NumericMetricsAggregation.SingleValue) {
                            NumericMetricsAggregation.SingleValue sv = (NumericMetricsAggregation.SingleValue) metricAgg;
                            line.put(metricAgg.getName(), sv.value());
                        }
                    }
                    result.add(line);
                }
            }
            else if (agg instanceof NumericMetricsAggregation.SingleValue) {
                hasMetricAggregation = true;
                if (hasBucketsAggregation) {
                    throw new UnsupportedOperationException("Impossible merge different bucket and metric aggregation, this must not be appear.");
                }
                NumericMetricsAggregation.SingleValue sv = (NumericMetricsAggregation.SingleValue) agg;
                singleValueMap.put(agg.getName(), sv.value());
            }
        }
        if (hasBucketsAggregation) {
            return result.build();
        }
        else {
            return ImmutableList.of(singleValueMap);
        }
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
            throws IOException
    {
    }

    private List<Decoder> createDecoders(List<ElasticsearchColumnHandle> columns)
    {
        return columns.stream()
                .map(column -> ScanQueryPageSource.createDecoder(column.getName(), column.getType()))
                .collect(toImmutableList());
    }
}
