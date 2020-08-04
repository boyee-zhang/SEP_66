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
package io.prestosql.decoder.json;

import com.fasterxml.jackson.databind.JsonNode;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.Type;

import java.util.concurrent.TimeUnit;

import static io.prestosql.decoder.DecoderErrorCode.DECODER_CONVERSION_NOT_SUPPORTED;
import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static java.lang.String.format;

public abstract class AbstractDateTimeJsonValueProvider
        extends FieldValueProvider
{
    protected final ConnectorSession session;
    protected final JsonNode value;
    protected final DecoderColumnHandle columnHandle;

    protected AbstractDateTimeJsonValueProvider(ConnectorSession session, JsonNode value, DecoderColumnHandle columnHandle)
    {
        this.session = session;
        this.value = value;
        this.columnHandle = columnHandle;
    }

    @Override
    public final boolean isNull()
    {
        return value.isMissingNode() || value.isNull();
    }

    @Override
    public final long getLong()
    {
        long millisUtc = getMillisUtc();

        Type type = columnHandle.getType();

        if (type.equals(TIME) || type.equals(TIME_WITH_TIME_ZONE)) {
            if (millisUtc < 0 || millisUtc >= TimeUnit.DAYS.toMillis(1)) {
                throw new PrestoException(
                        DECODER_CONVERSION_NOT_SUPPORTED,
                        format("could not parse value '%s' as '%s' for column '%s'", value.asText(), columnHandle.getType(), columnHandle.getName()));
            }
        }

        if (type.equals(DATE)) {
            return TimeUnit.MILLISECONDS.toDays(millisUtc);
        }
        if (type.equals(TIME)) {
            return millisUtc;
        }
        if (type instanceof TimestampType) {
            return packDateTimeWithZone(millisUtc, session.getTimeZoneKey());
        }
        if (type instanceof TimestampWithTimeZoneType || type.equals(TIME_WITH_TIME_ZONE)) {
            return packDateTimeWithZone(millisUtc, getTimeZone());
        }

        return millisUtc;
    }

    /**
     * @return epoch milliseconds in UTC
     */
    protected abstract long getMillisUtc();

    /**
     * @return TimeZoneKey for value
     */
    protected abstract TimeZoneKey getTimeZone();
}
