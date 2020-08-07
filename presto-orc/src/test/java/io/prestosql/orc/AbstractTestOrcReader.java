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
package io.prestosql.orc;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.SqlDate;
import io.prestosql.spi.type.SqlDecimal;
import io.prestosql.spi.type.SqlTimestamp;
import io.prestosql.spi.type.SqlVarbinary;
import org.joda.time.DateTimeZone;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Lists.newArrayList;
import static io.prestosql.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.CharType.createCharType;
import static io.prestosql.spi.type.Chars.padSpaces;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.DateTimeTestingUtils.sqlTimestampOf;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

public abstract class AbstractTestOrcReader
{
    private static final int CHAR_LENGTH = 10;

    private static final DecimalType DECIMAL_TYPE_PRECISION_2 = DecimalType.createDecimalType(2, 1);
    private static final DecimalType DECIMAL_TYPE_PRECISION_4 = DecimalType.createDecimalType(4, 2);
    private static final DecimalType DECIMAL_TYPE_PRECISION_8 = DecimalType.createDecimalType(8, 4);
    private static final DecimalType DECIMAL_TYPE_PRECISION_17 = DecimalType.createDecimalType(17, 8);
    private static final DecimalType DECIMAL_TYPE_PRECISION_18 = DecimalType.createDecimalType(18, 8);
    private static final DecimalType DECIMAL_TYPE_PRECISION_38 = DecimalType.createDecimalType(38, 16);
    private static final CharType CHAR = createCharType(CHAR_LENGTH);

    private final OrcTester tester;

    public AbstractTestOrcReader(OrcTester tester)
    {
        this.tester = tester;
    }

    @BeforeClass
    public void setUp()
    {
        assertEquals(DateTimeZone.getDefault(), HIVE_STORAGE_TIME_ZONE);
    }

    @Test
    public void testBooleanSequence()
            throws Exception
    {
        tester.testRoundTrip(BOOLEAN, newArrayList(limit(cycle(ImmutableList.of(true, false, false)), 30_000)));
    }

    @Test
    public void testLongSequence()
            throws Exception
    {
        testRoundTripNumeric(intsBetween(0, 31_234));
    }

    @Test
    public void testNegativeLongSequence()
            throws Exception
    {
        // A flaw in ORC encoding makes it impossible to represent timestamp
        // between 1969-12-31 23:59:59.000, exclusive, and 1970-01-01 00:00:00.000, exclusive.
        // Therefore, such data won't round trip and are skipped from test.
        testRoundTripNumeric(intsBetween(-31_234, -999));
    }

    @Test
    public void testLongSequenceWithHoles()
            throws Exception
    {
        testRoundTripNumeric(skipEvery(5, intsBetween(0, 31_234)));
    }

    @Test
    public void testLongDirect()
            throws Exception
    {
        testRoundTripNumeric(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), 30_000));
    }

    @Test
    public void testLongDirect2()
            throws Exception
    {
        List<Integer> values = new ArrayList<>(31_234);
        for (int i = 0; i < 31_234; i++) {
            values.add(i);
        }
        Collections.shuffle(values, new Random(0));
        testRoundTripNumeric(values);
    }

    @Test
    public void testLongShortRepeat()
            throws Exception
    {
        testRoundTripNumeric(limit(repeatEach(4, cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17))), 30_000));
    }

    @Test
    public void testLongPatchedBase()
            throws Exception
    {
        testRoundTripNumeric(limit(cycle(concat(intsBetween(0, 18), intsBetween(0, 18), ImmutableList.of(30_000, 20_000, 400_000, 30_000, 20_000))), 30_000));
    }

    @Test
    public void testLongStrideDictionary()
            throws Exception
    {
        testRoundTripNumeric(concat(ImmutableList.of(1), nCopies(9999, 123), ImmutableList.of(2), nCopies(9999, 123)));
    }

    private void testRoundTripNumeric(Iterable<? extends Number> values)
            throws Exception
    {
        List<Long> writeValues = ImmutableList.copyOf(values).stream()
                .map(Number::longValue)
                .collect(toList());
        tester.testRoundTrip(
                TINYINT,
                writeValues.stream()
                        .map(Long::byteValue) // truncate values to byte range
                        .collect(toList()));

        tester.testRoundTrip(
                SMALLINT,
                writeValues.stream()
                        .map(Long::shortValue) // truncate values to short range
                        .collect(toList()));

        tester.testRoundTrip(
                INTEGER,
                writeValues.stream()
                        .map(Long::intValue) // truncate values to int range
                        .collect(toList()));

        tester.testRoundTrip(BIGINT, writeValues);

        tester.testRoundTrip(
                DATE,
                writeValues.stream()
                        .map(Long::intValue)
                        .map(SqlDate::new)
                        .collect(toList()));

        tester.testRoundTrip(
                TIMESTAMP,
                writeValues.stream()
                        .map(timestamp -> sqlTimestampOf(timestamp))
                        .collect(toList()));
    }

    @Test
    public void testFloatSequence()
            throws Exception
    {
        tester.testRoundTrip(REAL, floatSequence(0.0f, 0.1f, 30_000));
    }

    @Test
    public void testFloatNaNInfinity()
            throws Exception
    {
        tester.testRoundTrip(REAL, ImmutableList.of(1000.0f, -1.23f, Float.POSITIVE_INFINITY));
        tester.testRoundTrip(REAL, ImmutableList.of(-1000.0f, Float.NEGATIVE_INFINITY, 1.23f));
        tester.testRoundTrip(REAL, ImmutableList.of(0.0f, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY));

        tester.testRoundTrip(REAL, ImmutableList.of(Float.NaN, -0.0f, 1.0f));
        tester.testRoundTrip(REAL, ImmutableList.of(Float.NaN, -1.0f, Float.POSITIVE_INFINITY));
        tester.testRoundTrip(REAL, ImmutableList.of(Float.NaN, Float.NEGATIVE_INFINITY, 1.0f));
        tester.testRoundTrip(REAL, ImmutableList.of(Float.NaN, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY));
    }

    @Test
    public void testDoubleSequence()
            throws Exception
    {
        tester.testRoundTrip(DOUBLE, doubleSequence(0, 0.1, 30_000));
    }

    @Test
    public void testDecimalSequence()
            throws Exception
    {
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_2, decimalSequence("-30", "1", 60, 2, 1));
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_4, decimalSequence("-3000", "1", 60_00, 4, 2));
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_8, decimalSequence("-3000000", "100", 60_000, 8, 4));
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_17, decimalSequence("-30000000000", "1000000", 60_000, 17, 8));
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_18, decimalSequence("-30000000000", "1000000", 60_000, 18, 8));
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_38, decimalSequence("-3000000000000000000", "100000000000000", 60_000, 38, 16));

        Random random = new Random(0);
        List<SqlDecimal> values = new ArrayList<>();
        values.add(new SqlDecimal(new BigInteger("9".repeat(18)), DECIMAL_TYPE_PRECISION_18.getPrecision(), DECIMAL_TYPE_PRECISION_18.getScale()));
        values.add(new SqlDecimal(new BigInteger("-" + "9".repeat(18)), DECIMAL_TYPE_PRECISION_18.getPrecision(), DECIMAL_TYPE_PRECISION_18.getScale()));
        BigInteger nextValue = BigInteger.ONE;
        for (int i = 0; i < 59; i++) {
            values.add(new SqlDecimal(nextValue, DECIMAL_TYPE_PRECISION_18.getPrecision(), DECIMAL_TYPE_PRECISION_18.getScale()));
            values.add(new SqlDecimal(nextValue.negate(), DECIMAL_TYPE_PRECISION_18.getPrecision(), DECIMAL_TYPE_PRECISION_18.getScale()));
            nextValue = nextValue.multiply(BigInteger.valueOf(2));
        }
        for (int i = 0; i < 100_000; ++i) {
            BigInteger value = new BigInteger(59, random);
            if (random.nextBoolean()) {
                value = value.negate();
            }
            values.add(new SqlDecimal(value, DECIMAL_TYPE_PRECISION_18.getPrecision(), DECIMAL_TYPE_PRECISION_18.getScale()));
        }
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_18, values);

        random = new Random(0);
        values = new ArrayList<>();
        values.add(new SqlDecimal(new BigInteger("9".repeat(38)), DECIMAL_TYPE_PRECISION_38.getPrecision(), DECIMAL_TYPE_PRECISION_38.getScale()));
        values.add(new SqlDecimal(new BigInteger("-" + "9".repeat(38)), DECIMAL_TYPE_PRECISION_38.getPrecision(), DECIMAL_TYPE_PRECISION_38.getScale()));
        nextValue = BigInteger.ONE;
        for (int i = 0; i < 127; i++) {
            values.add(new SqlDecimal(nextValue, 38, 16));
            values.add(new SqlDecimal(nextValue.negate(), 38, 16));
            nextValue = nextValue.multiply(BigInteger.valueOf(2));
        }
        for (int i = 0; i < 100_000; ++i) {
            BigInteger value = new BigInteger(126, random);
            if (random.nextBoolean()) {
                value = value.negate();
            }
            values.add(new SqlDecimal(value, DECIMAL_TYPE_PRECISION_38.getPrecision(), DECIMAL_TYPE_PRECISION_38.getScale()));
        }
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_38, values);
    }

    @Test
    public void testDoubleNaNInfinity()
            throws Exception
    {
        tester.testRoundTrip(DOUBLE, ImmutableList.of(1000.0, -1.0, Double.POSITIVE_INFINITY));
        tester.testRoundTrip(DOUBLE, ImmutableList.of(-1000.0, Double.NEGATIVE_INFINITY, 1.0));
        tester.testRoundTrip(DOUBLE, ImmutableList.of(0.0, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY));

        tester.testRoundTrip(DOUBLE, ImmutableList.of(Double.NaN, -1.0, 1.0));
        tester.testRoundTrip(DOUBLE, ImmutableList.of(Double.NaN, -1.0, Double.POSITIVE_INFINITY));
        tester.testRoundTrip(DOUBLE, ImmutableList.of(Double.NaN, Double.NEGATIVE_INFINITY, 1.0));
        tester.testRoundTrip(DOUBLE, ImmutableList.of(Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY));
    }

    @Test
    public void testTimestamp()
            throws Exception
    {
        @SuppressWarnings("deprecation")
        List<SqlTimestamp> values = ImmutableList.of(
                SqlTimestamp.fromMillis(3, 0),
                SqlTimestamp.fromMillis(3, 10),
                SqlTimestamp.fromMillis(3, 1123456789L), // 1970-01-14T00:04:16.789Z
                SqlTimestamp.fromMillis(3, 1000123456789L), // 2001-09-10T12:04:16.789Z
                SqlTimestamp.fromMillis(3, 1575553299564L)); // 2019-12-05T13:41:39.564Z
        tester.testRoundTrip(TIMESTAMP, newArrayList(limit(cycle(values), 30_000)));
    }

    @Test
    public void testStringUnicode()
            throws Exception
    {
        tester.testRoundTrip(VARCHAR, newArrayList(limit(cycle(ImmutableList.of("apple", "apple pie", "apple\uD835\uDC03", "apple\uFFFD")), 30_000)));
    }

    @Test
    public void testStringDirectSequence()
            throws Exception
    {
        tester.testRoundTrip(
                VARCHAR,
                intsBetween(0, 30_000).stream()
                        .map(Object::toString)
                        .collect(toList()));
    }

    @Test
    public void testStringDictionarySequence()
            throws Exception
    {
        tester.testRoundTrip(
                VARCHAR,
                newArrayList(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), 30_000)).stream()
                        .map(Object::toString)
                        .collect(toList()));
    }

    @Test
    public void testStringStrideDictionary()
            throws Exception
    {
        tester.testRoundTrip(VARCHAR, newArrayList(concat(ImmutableList.of("a"), nCopies(9999, "123"), ImmutableList.of("b"), nCopies(9999, "123"))));
    }

    @Test
    public void testEmptyStringSequence()
            throws Exception
    {
        tester.testRoundTrip(VARCHAR, newArrayList(limit(cycle(""), 30_000)));
    }

    @Test
    public void testCharDirectSequence()
            throws Exception
    {
        tester.testRoundTrip(
                CHAR,
                intsBetween(0, 30_000).stream()
                        .map(this::toCharValue)
                        .collect(toList()));
    }

    @Test
    public void testCharDictionarySequence()
            throws Exception
    {
        tester.testRoundTrip(
                CHAR,
                newArrayList(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), 30_000)).stream()
                        .map(this::toCharValue)
                        .collect(toList()));
    }

    @Test
    public void testEmptyCharSequence()
            throws Exception
    {
        tester.testRoundTrip(CHAR, newArrayList(limit(cycle("          "), 30_000)));
    }

    private String toCharValue(Object value)
    {
        return padSpaces(value.toString(), CHAR);
    }

    @Test
    public void testBinaryDirectSequence()
            throws Exception
    {
        tester.testRoundTrip(
                VARBINARY,
                intsBetween(0, 30_000).stream()
                        .map(Object::toString)
                        .map(string -> string.getBytes(UTF_8))
                        .map(SqlVarbinary::new)
                        .collect(toList()));
    }

    @Test
    public void testBinaryDictionarySequence()
            throws Exception
    {
        tester.testRoundTrip(
                VARBINARY, ImmutableList.copyOf(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), 30_000)).stream()
                        .map(Object::toString)
                        .map(string -> string.getBytes(UTF_8))
                        .map(SqlVarbinary::new)
                        .collect(toList()));
    }

    @Test
    public void testEmptyBinarySequence()
            throws Exception
    {
        tester.testRoundTrip(VARBINARY, nCopies(30_000, new SqlVarbinary(new byte[0])));
    }

    private static <T> Iterable<T> skipEvery(int n, Iterable<T> iterable)
    {
        return () -> new AbstractIterator<>()
        {
            private final Iterator<T> delegate = iterable.iterator();
            private int position;

            @Override
            protected T computeNext()
            {
                while (true) {
                    if (!delegate.hasNext()) {
                        return endOfData();
                    }

                    T next = delegate.next();
                    position++;
                    if (position <= n) {
                        return next;
                    }
                    position = 0;
                }
            }
        };
    }

    private static <T> Iterable<T> repeatEach(int n, Iterable<T> iterable)
    {
        return () -> new AbstractIterator<>()
        {
            private final Iterator<T> delegate = iterable.iterator();
            private int position;
            private T value;

            @Override
            protected T computeNext()
            {
                if (position == 0) {
                    if (!delegate.hasNext()) {
                        return endOfData();
                    }
                    value = delegate.next();
                }

                position++;
                if (position >= n) {
                    position = 0;
                }
                return value;
            }
        };
    }

    protected static List<Double> doubleSequence(double start, double step, int items)
    {
        List<Double> values = new ArrayList<>();
        double nextValue = start;
        for (int i = 0; i < items; i++) {
            values.add(nextValue);
            nextValue += step;
        }
        return values;
    }

    private static List<Float> floatSequence(float start, float step, int items)
    {
        ImmutableList.Builder<Float> values = ImmutableList.builder();
        float nextValue = start;
        for (int i = 0; i < items; i++) {
            values.add(nextValue);
            nextValue += step;
        }
        return values.build();
    }

    private static List<SqlDecimal> decimalSequence(String start, String step, int items, int precision, int scale)
    {
        BigInteger decimalStep = new BigInteger(step);

        List<SqlDecimal> values = new ArrayList<>();
        BigInteger nextValue = new BigInteger(start);
        for (int i = 0; i < items; i++) {
            values.add(new SqlDecimal(nextValue, precision, scale));
            nextValue = nextValue.add(decimalStep);
        }
        return values;
    }

    private static ContiguousSet<Integer> intsBetween(int lowerInclusive, int upperExclusive)
    {
        return ContiguousSet.create(Range.closedOpen(lowerInclusive, upperExclusive), DiscreteDomain.integers());
    }
}
