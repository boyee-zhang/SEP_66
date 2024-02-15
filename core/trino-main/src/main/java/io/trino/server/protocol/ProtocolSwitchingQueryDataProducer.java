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
package io.trino.server.protocol;

import com.google.inject.Inject;
import io.trino.Session;
import io.trino.client.QueryDataEncodings;
import io.trino.spi.QueryId;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ProtocolSwitchingQueryDataProducer
        implements QueryDataProducerFactory
{
    private final LegacyQueryDataProducer legacyQueryDataProducer;
    private final EncodedQueryDataProducer encodedQueryDataProducer;

    @Inject
    public ProtocolSwitchingQueryDataProducer(LegacyQueryDataProducer legacyQueryDataProducer, EncodedQueryDataProducer encodedQueryDataProducer)
    {
        this.legacyQueryDataProducer = requireNonNull(legacyQueryDataProducer, "legacyQueryDataProducer is null");
        this.encodedQueryDataProducer = requireNonNull(encodedQueryDataProducer, "encodedQueryDataProducer is null");
    }

    @Override
    public QueryDataProducer create(Session session, QueryId queryId)
    {
        Optional<QueryDataEncodings> requestedFormats = session.getQueryDataEncoding()
                .map(QueryDataEncodings::parseEncodings);

        if (requestedFormats.isEmpty()) {
            return legacyQueryDataProducer;
        }

        return encodedQueryDataProducer;
    }
}