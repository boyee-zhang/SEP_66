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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.elasticsearch.aggregation.MetricAggregation;
import io.trino.plugin.elasticsearch.aggregation.TermAggregation;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public final class ElasticsearchTableHandle
        implements ConnectorTableHandle
{
    public enum Type
    {
        SCAN, QUERY, AGG
    }

    private final Type type;
    private final String schema;
    private final String index;
    private final TupleDomain<ColumnHandle> constraint;
    private final Optional<String> query;
    private final OptionalLong limit;
    // for group by fields
    private final List<TermAggregation> termAggregations;
    // for aggregation methods and fields
    private final List<MetricAggregation> metricAggregations;

    public ElasticsearchTableHandle(Type type, String schema, String index, Optional<String> query)
    {
        this.type = requireNonNull(type, "type is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.index = requireNonNull(index, "index is null");
        this.query = requireNonNull(query, "query is null");

        constraint = TupleDomain.all();
        limit = OptionalLong.empty();
        termAggregations = Collections.emptyList();
        metricAggregations = Collections.emptyList();
    }

    @JsonCreator
    public ElasticsearchTableHandle(
            @JsonProperty("type") Type type,
            @JsonProperty("schema") String schema,
            @JsonProperty("index") String index,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("query") Optional<String> query,
            @JsonProperty("limit") OptionalLong limit,
            @JsonProperty("aggTerms") List<TermAggregation> termAggregations,
            @JsonProperty("aggregates") List<MetricAggregation> metricAggregations)
    {
        this.type = requireNonNull(type, "type is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.index = requireNonNull(index, "index is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.query = requireNonNull(query, "query is null");
        this.limit = requireNonNull(limit, "limit is null");
        this.termAggregations = requireNonNull(termAggregations, "aggTerms is null");
        this.metricAggregations = requireNonNull(metricAggregations, "aggregates is null");
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getIndex()
    {
        return index;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
    }

    @JsonProperty
    public Optional<String> getQuery()
    {
        return query;
    }

    @JsonProperty
    public List<TermAggregation> getTermAggregations()
    {
        return termAggregations;
    }

    @JsonProperty
    public List<MetricAggregation> getMetricAggregations()
    {
        return metricAggregations;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ElasticsearchTableHandle that = (ElasticsearchTableHandle) o;
        return type == that.type &&
                schema.equals(that.schema) &&
                index.equals(that.index) &&
                constraint.equals(that.constraint) &&
                query.equals(that.query) &&
                limit.equals(that.limit);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, schema, index, constraint, query, limit);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(type + ":" + index);

        StringBuilder attributes = new StringBuilder();
        limit.ifPresent(value -> attributes.append("limit=" + value));
        query.ifPresent(value -> attributes.append("query" + value));

        if (attributes.length() > 0) {
            builder.append("(");
            builder.append(attributes);
            builder.append(")");
        }

        return builder.toString();
    }
}
