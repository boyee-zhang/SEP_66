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

import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;
import static io.prestosql.spi.type.TimestampType.createTimestampType;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static java.util.Arrays.asList;

public class TestRFC2822JsonFieldDecoder
{
    private final JsonFieldDecoderTester tester = new JsonFieldDecoderTester("rfc2822");

    @Test
    public void testDecode()
    {
        tester.assertDecodedAs("\"Mon Feb 12 13:15:16 Z 2018\"", DATE, 17574); // TODO should it be supported really?
        tester.assertDecodedAs("\"Thu Jan 01 13:15:19 Z 1970\"", TIME, 47719000); // TODO should it be supported really?
        tester.assertDecodedAs("\"Thu Jan 01 13:15:19 Z 1970\"", TIME_WITH_TIME_ZONE, packDateTimeWithZone(47719000, UTC_KEY)); // TODO should it be supported really?
        tester.assertDecodedAs("\"Fri Feb 09 13:15:19 Z 2018\"", createTimestampType(0), packDateTimeWithZone(1518182119000L, SESSION.getTimeZoneKey()));
        tester.assertDecodedAs("\"Fri Feb 09 13:15:19 Z 2018\"", createTimestampWithTimeZoneType(0), packDateTimeWithZone(1518182119000L, UTC_KEY));
        tester.assertDecodedAs("\"Fri Feb 09 15:15:19 +02:00 2018\"", createTimestampType(0), packDateTimeWithZone(1518182119000L, SESSION.getTimeZoneKey()));
        tester.assertDecodedAs("\"Fri Feb 09 15:15:19 +02:00 2018\"", createTimestampWithTimeZoneType(0), packDateTimeWithZone(1518182119000L, TimeZoneKey.getTimeZoneKeyForOffset(120)));
    }

    @Test
    public void testDecodeNulls()
    {
        for (Type type : asList(DATE, TIME, TIME_WITH_TIME_ZONE, createTimestampType(0), createTimestampWithTimeZoneType(0))) {
            tester.assertDecodedAsNull("null", type);
            tester.assertMissingDecodedAsNull(type);
        }
    }

    @Test
    public void testDecodeInvalid()
    {
        tester.assertInvalidInput("{}", createTimestampType(0), "\\Qcould not parse non-value node as 'timestamp(0)' for column 'some_column'\\E");
        tester.assertInvalidInput("\"a\"", createTimestampType(0), "\\Qcould not parse value 'a' as 'timestamp(0)' for column 'some_column'\\E");
        tester.assertInvalidInput("2018", createTimestampType(0), "\\Qcould not parse value '2018' as 'timestamp(0)' for column 'some_column'\\E");
        tester.assertInvalidInput("\"Mon Feb 12 13:15:16 Z\"", createTimestampType(0), "could not parse value '.*' as 'timestamp\\(0\\)' for column 'some_column'");
        tester.assertInvalidInput("\"Mon Feb 12 13:15:16 2018\"", createTimestampType(0), "could not parse value '.*' as 'timestamp\\(0\\)' for column 'some_column'");
        tester.assertInvalidInput("\"Mon Feb 12 Z 2018\"", createTimestampType(0), "could not parse value '.*' as 'timestamp\\(0\\)' for column 'some_column'");
        tester.assertInvalidInput("\"Mon Feb 13:15:16 Z 2018\"", createTimestampType(0), "could not parse value '.*' as 'timestamp\\(0\\)' for column 'some_column'");
        tester.assertInvalidInput("\"Mon 12 13:15:16 Z 2018\"", createTimestampType(0), "could not parse value '.*' as 'timestamp\\(0\\)' for column 'some_column'");
        tester.assertInvalidInput("\"Feb 12 13:15:16 Z 2018\"", createTimestampType(0), "could not parse value '.*' as 'timestamp\\(0\\)' for column 'some_column'");
        tester.assertInvalidInput("\"Fri Feb 09 13:15:19 Europe/Warsaw 2018\"", createTimestampType(0), "could not parse value '.*' as 'timestamp\\(0\\)' for column 'some_column'");
        tester.assertInvalidInput("\"Fri Feb 09 13:15:19 EST 2018\"", createTimestampType(0), "could not parse value '.*' as 'timestamp\\(0\\)' for column 'some_column'");
    }
}
