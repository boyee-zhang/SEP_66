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

import com.google.api.gax.rpc.HeaderProvider;
import com.google.auth.Credentials;
import com.google.common.cache.CacheBuilder;
import io.trino.collect.cache.NonEvictableCache;
import io.trino.spi.connector.ConnectorSession;

import javax.inject.Inject;

import java.util.Optional;

import static io.trino.collect.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.collect.cache.SafeCaches.buildNonEvictableCache;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class BigQueryClientFactory
{
    private final IdentityCacheMapping identityCacheMapping;
    private final BigQueryCredentialsSupplier credentialsSupplier;
    private final ViewMaterializationCache materializationCache;
    private final HeaderProvider headerProvider;
    private final NonEvictableCache<IdentityCacheMapping.IdentityCacheKey, BigQueryClient> clientCache;
    private final BigQueryConfig bigQueryConfig;

    @Inject
    public BigQueryClientFactory(
            IdentityCacheMapping identityCacheMapping,
            BigQueryCredentialsSupplier credentialsSupplier,
            BigQueryConfig bigQueryConfig,
            ViewMaterializationCache materializationCache,
            HeaderProvider headerProvider)
    {
        this.identityCacheMapping = requireNonNull(identityCacheMapping, "identityCacheMapping is null");
        this.credentialsSupplier = requireNonNull(credentialsSupplier, "credentialsSupplier is null");
        this.bigQueryConfig = requireNonNull(bigQueryConfig, "bigQueryConfig is null");
        this.materializationCache = requireNonNull(materializationCache, "materializationCache is null");
        this.headerProvider = requireNonNull(headerProvider, "headerProvider is null");

        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder()
                .expireAfterWrite(bigQueryConfig.getServiceCacheTtl().toMillis(), MILLISECONDS);

        clientCache = buildNonEvictableCache(cacheBuilder);
    }

    public BigQueryClient create(ConnectorSession session)
    {
        IdentityCacheMapping.IdentityCacheKey cacheKey = identityCacheMapping.getRemoteUserCacheKey(session);

        return uncheckedCacheGet(clientCache, cacheKey, () -> createBigQueryClient(session));
    }

    protected BigQueryClient createBigQueryClient(ConnectorSession session)
    {
        Optional<Credentials> credentials = credentialsSupplier.getCredentials(session);
        return new BigQueryClient(bigQueryConfig, headerProvider, credentials, materializationCache);
    }
}
