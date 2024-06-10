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
package io.trino.plugin.bigquery;

import com.google.cloud.bigquery.JobInfo.CreateDisposition;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;

public final class BigQuerySessionProperties
        implements SessionPropertiesProvider
{
    private static final String SKIP_VIEW_MATERIALIZATION = "skip_view_materialization";
    private static final String VIEW_MATERIALIZATION_WITH_FILTER = "view_materialization_with_filter";
    private static final String QUERY_RESULTS_CACHE_ENABLED = "query_results_cache_enabled";
    private static final String CREATE_DISPOSITION_TYPE = "create_disposition_type";
    private static final String READ_PARALLELISM = "read_parallelism";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public BigQuerySessionProperties(BigQueryConfig config)
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(booleanProperty(
                        SKIP_VIEW_MATERIALIZATION,
                        "Skip materializing views",
                        config.isSkipViewMaterialization(),
                        false))
                .add(booleanProperty(
                        VIEW_MATERIALIZATION_WITH_FILTER,
                        "Materialize views with filters",
                        config.isViewMaterializationWithFilter(),
                        false))
                .add(booleanProperty(
                        QUERY_RESULTS_CACHE_ENABLED,
                        "Enable query results cache",
                        config.isQueryResultsCacheEnabled(),
                        false))
                .add(enumProperty(
                        CREATE_DISPOSITION_TYPE,
                        "Create disposition type",
                        CreateDisposition.class,
                        CreateDisposition.CREATE_IF_NEEDED, // https://cloud.google.com/bigquery/docs/cached-results
                        true))
                .add(integerProperty(
                        READ_PARALLELISM,
                        "How many parallel streams are used to read data from BigQuery",
                        config.getParallelism().orElse(null),
                        false))
                .build();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isSkipViewMaterialization(ConnectorSession session)
    {
        return session.getProperty(SKIP_VIEW_MATERIALIZATION, Boolean.class);
    }

    public static boolean isViewMaterializationWithFilter(ConnectorSession session)
    {
        return session.getProperty(VIEW_MATERIALIZATION_WITH_FILTER, Boolean.class);
    }

    public static boolean isQueryResultsCacheEnabled(ConnectorSession session)
    {
        return session.getProperty(QUERY_RESULTS_CACHE_ENABLED, Boolean.class);
    }

    public static OptionalInt readParallelism(ConnectorSession session)
    {
        return Optional.ofNullable(session.getProperty(READ_PARALLELISM, Integer.class))
                .map(OptionalInt::of)
                .orElse(OptionalInt.empty());
    }

    public static CreateDisposition createDisposition(ConnectorSession session)
    {
        return session.getProperty(CREATE_DISPOSITION_TYPE, CreateDisposition.class);
    }
}
