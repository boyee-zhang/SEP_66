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
package io.prestosql.operator.scalar.timestamp;

import io.airlift.slice.Slice;
import io.prestosql.spi.function.LiteralParameter;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestamp;
import io.prestosql.type.DateTimes;

import java.time.format.DateTimeFormatter;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.type.DateTimes.scaleEpochMillisToMicros;
import static java.time.ZoneOffset.UTC;

@ScalarOperator(CAST)
public final class TimestampToVarcharCast
{
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");

    private TimestampToVarcharCast() {}

    @LiteralParameters({"x", "p"})
    @SqlType("varchar(x)")
    public static Slice cast(@LiteralParameter("p") long precision, @SqlType("timestamp(p)") long timestamp)
    {
        long epochMicros = timestamp;
        if (precision <= 3) {
            epochMicros = scaleEpochMillisToMicros(timestamp);
        }

        return utf8Slice(DateTimes.formatTimestamp((int) precision, epochMicros, 0, UTC, TIMESTAMP_FORMATTER));
    }

    @LiteralParameters({"x", "p"})
    @SqlType("varchar(x)")
    public static Slice cast(@LiteralParameter("p") long precision, @SqlType("timestamp(p)") LongTimestamp timestamp)
    {
        return utf8Slice(DateTimes.formatTimestamp((int) precision, timestamp.getEpochMicros(), timestamp.getPicosOfMicro(), UTC, TIMESTAMP_FORMATTER));
    }
}
