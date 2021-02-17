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
package io.trino.plugin.jdbc;

import com.google.common.base.CharMatcher;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.io.BaseEncoding.base16;
import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.decodeUnscaledValue;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.spi.type.Timestamps.roundDiv;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.math.RoundingMode.UNNECESSARY;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public final class StandardColumnMappings
{
    private static final int MAX_LOCAL_DATE_TIME_PRECISION = 9;

    private StandardColumnMappings() {}

    public static ColumnMapping booleanColumnMapping()
    {
        return ColumnMapping.booleanMapping(BOOLEAN, ResultSet::getBoolean, booleanWriteFunction());
    }

    public static BooleanWriteFunction booleanWriteFunction()
    {
        return PreparedStatement::setBoolean;
    }

    public static ColumnMapping tinyintColumnMapping()
    {
        return ColumnMapping.longMapping(TINYINT, ResultSet::getByte, tinyintWriteFunction());
    }

    public static LongWriteFunction tinyintWriteFunction()
    {
        return (statement, index, value) -> statement.setByte(index, SignedBytes.checkedCast(value));
    }

    public static ColumnMapping smallintColumnMapping()
    {
        return ColumnMapping.longMapping(SMALLINT, ResultSet::getShort, smallintWriteFunction());
    }

    public static LongWriteFunction smallintWriteFunction()
    {
        return (statement, index, value) -> statement.setShort(index, Shorts.checkedCast(value));
    }

    public static ColumnMapping integerColumnMapping()
    {
        return ColumnMapping.longMapping(INTEGER, ResultSet::getInt, integerWriteFunction());
    }

    public static LongWriteFunction integerWriteFunction()
    {
        return (statement, index, value) -> statement.setInt(index, toIntExact(value));
    }

    public static ColumnMapping bigintColumnMapping()
    {
        return ColumnMapping.longMapping(BIGINT, ResultSet::getLong, bigintWriteFunction());
    }

    public static LongWriteFunction bigintWriteFunction()
    {
        return PreparedStatement::setLong;
    }

    public static ColumnMapping realColumnMapping()
    {
        return ColumnMapping.longMapping(REAL, (resultSet, columnIndex) -> floatToRawIntBits(resultSet.getFloat(columnIndex)), realWriteFunction());
    }

    public static LongWriteFunction realWriteFunction()
    {
        return (statement, index, value) -> statement.setFloat(index, intBitsToFloat(toIntExact(value)));
    }

    public static ColumnMapping doubleColumnMapping()
    {
        return ColumnMapping.doubleMapping(DOUBLE, ResultSet::getDouble, doubleWriteFunction());
    }

    public static DoubleWriteFunction doubleWriteFunction()
    {
        return PreparedStatement::setDouble;
    }

    public static ColumnMapping decimalColumnMapping(DecimalType decimalType)
    {
        return decimalColumnMapping(decimalType, UNNECESSARY);
    }

    public static ColumnMapping decimalColumnMapping(DecimalType decimalType, RoundingMode roundingMode)
    {
        if (decimalType.isShort()) {
            checkArgument(roundingMode == UNNECESSARY, "Round mode is not supported for short decimal, map the type to long decimal instead");
            return ColumnMapping.longMapping(
                    decimalType,
                    shortDecimalReadFunction(decimalType),
                    shortDecimalWriteFunction(decimalType));
        }
        return ColumnMapping.sliceMapping(
                decimalType,
                longDecimalReadFunction(decimalType, roundingMode),
                longDecimalWriteFunction(decimalType));
    }

    public static LongReadFunction shortDecimalReadFunction(DecimalType decimalType)
    {
        return shortDecimalReadFunction(decimalType, UNNECESSARY);
    }

    public static LongReadFunction shortDecimalReadFunction(DecimalType decimalType, RoundingMode roundingMode)
    {
        // JDBC driver can return BigDecimal with lower scale than column's scale when there are trailing zeroes
        int scale = requireNonNull(decimalType, "decimalType is null").getScale();
        requireNonNull(roundingMode, "roundingMode is null");
        return (resultSet, columnIndex) -> encodeShortScaledValue(resultSet.getBigDecimal(columnIndex), scale, roundingMode);
    }

    public static LongWriteFunction shortDecimalWriteFunction(DecimalType decimalType)
    {
        requireNonNull(decimalType, "decimalType is null");
        checkArgument(decimalType.isShort());
        return (statement, index, value) -> {
            BigInteger unscaledValue = BigInteger.valueOf(value);
            BigDecimal bigDecimal = new BigDecimal(unscaledValue, decimalType.getScale(), new MathContext(decimalType.getPrecision()));
            statement.setBigDecimal(index, bigDecimal);
        };
    }

    public static SliceReadFunction longDecimalReadFunction(DecimalType decimalType)
    {
        return longDecimalReadFunction(decimalType, UNNECESSARY);
    }

    public static SliceReadFunction longDecimalReadFunction(DecimalType decimalType, RoundingMode roundingMode)
    {
        // JDBC driver can return BigDecimal with lower scale than column's scale when there are trailing zeroes
        int scale = requireNonNull(decimalType, "decimalType is null").getScale();
        requireNonNull(roundingMode, "roundingMode is null");
        return (resultSet, columnIndex) -> encodeScaledValue(resultSet.getBigDecimal(columnIndex).setScale(scale, roundingMode));
    }

    public static SliceWriteFunction longDecimalWriteFunction(DecimalType decimalType)
    {
        requireNonNull(decimalType, "decimalType is null");
        checkArgument(!decimalType.isShort());
        return (statement, index, value) -> {
            BigInteger unscaledValue = decodeUnscaledValue(value);
            BigDecimal bigDecimal = new BigDecimal(unscaledValue, decimalType.getScale(), new MathContext(decimalType.getPrecision()));
            statement.setBigDecimal(index, bigDecimal);
        };
    }

    public static ColumnMapping defaultCharColumnMapping(int columnSize)
    {
        if (columnSize > CharType.MAX_LENGTH) {
            return defaultVarcharColumnMapping(columnSize);
        }
        return charColumnMapping(createCharType(columnSize));
    }

    public static ColumnMapping charColumnMapping(CharType charType)
    {
        requireNonNull(charType, "charType is null");
        return ColumnMapping.sliceMapping(charType, charReadFunction(charType), charWriteFunction());
    }

    public static SliceReadFunction charReadFunction(CharType charType)
    {
        requireNonNull(charType, "charType is null");
        return (resultSet, columnIndex) -> {
            Slice slice = utf8Slice(CharMatcher.is(' ').trimTrailingFrom(resultSet.getString(columnIndex)));
            checkLengthInCodePoints(slice, charType, charType.getLength());
            return slice;
        };
    }

    public static SliceWriteFunction charWriteFunction()
    {
        return (statement, index, value) -> {
            statement.setString(index, value.toStringUtf8());
        };
    }

    public static ColumnMapping defaultVarcharColumnMapping(int columnSize)
    {
        if (columnSize > VarcharType.MAX_LENGTH) {
            return varcharColumnMapping(createUnboundedVarcharType());
        }
        return varcharColumnMapping(createVarcharType(columnSize));
    }

    public static ColumnMapping varcharColumnMapping(VarcharType varcharType)
    {
        return ColumnMapping.sliceMapping(varcharType, varcharReadFunction(varcharType), varcharWriteFunction());
    }

    public static SliceReadFunction varcharReadFunction(VarcharType varcharType)
    {
        requireNonNull(varcharType, "varcharType is null");
        if (varcharType.isUnbounded()) {
            return (resultSet, columnIndex) -> utf8Slice(resultSet.getString(columnIndex));
        }
        return (resultSet, columnIndex) -> {
            Slice slice = utf8Slice(resultSet.getString(columnIndex));
            checkLengthInCodePoints(slice, varcharType, varcharType.getBoundedLength());
            return slice;
        };
    }

    private static void checkLengthInCodePoints(Slice value, Type characterDataType, int lengthLimit)
    {
        // Quick check in bytes
        if (value.length() <= lengthLimit) {
            return;
        }
        // Actual check
        if (countCodePoints(value) <= lengthLimit) {
            return;
        }
        throw new IllegalStateException(format(
                "Illegal value for type %s: '%s' [%s]",
                characterDataType,
                value.toStringUtf8(),
                base16().encode(value.getBytes())));
    }

    public static SliceWriteFunction varcharWriteFunction()
    {
        return (statement, index, value) -> statement.setString(index, value.toStringUtf8());
    }

    public static ColumnMapping varbinaryColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                VARBINARY,
                varbinaryReadFunction(),
                varbinaryWriteFunction(),
                DISABLE_PUSHDOWN);
    }

    public static SliceReadFunction varbinaryReadFunction()
    {
        return (resultSet, columnIndex) -> wrappedBuffer(resultSet.getBytes(columnIndex));
    }

    public static SliceWriteFunction varbinaryWriteFunction()
    {
        return (statement, index, value) -> statement.setBytes(index, value.getBytes());
    }

    public static ColumnMapping dateColumnMapping()
    {
        return ColumnMapping.longMapping(
                DATE,
                (resultSet, columnIndex) -> {
                    /*
                     * JDBC returns a date using a timestamp at midnight in the JVM timezone, or earliest time after that if there was no midnight.
                     * This works correctly for all dates and zones except when the missing local times 'gap' is 24h. I.e. this fails when JVM time
                     * zone is Pacific/Apia and date to be returned is 2011-12-30.
                     *
                     * `return resultSet.getObject(columnIndex, LocalDate.class).toEpochDay()` avoids these problems but
                     * is currently known not to work with Redshift (old Postgres connector) and SQL Server.
                     */
                    long localMillis = resultSet.getDate(columnIndex).getTime();
                    // Convert it to a ~midnight in UTC.
                    long utcMillis = ISOChronology.getInstance().getZone().getMillisKeepLocal(DateTimeZone.UTC, localMillis);
                    // convert to days
                    return MILLISECONDS.toDays(utcMillis);
                },
                dateWriteFunction());
    }

    public static LongWriteFunction dateWriteFunction()
    {
        return (statement, index, value) -> {
            // convert to midnight in default time zone
            long millis = DAYS.toMillis(value);
            statement.setDate(index, new Date(DateTimeZone.UTC.getMillisKeepLocal(DateTimeZone.getDefault(), millis)));
        };
    }

    /**
     * @deprecated This method uses {@link java.sql.Time} and the class cannot represent time value when JVM zone had
     * forward offset change (a 'gap') at given time on 1970-01-01. If driver only supports {@link LocalTime}, use
     * {@link #timeColumnMapping} instead.
     */
    @Deprecated
    public static ColumnMapping timeColumnMappingUsingSqlTime()
    {
        return ColumnMapping.longMapping(
                TIME,
                (resultSet, columnIndex) -> {
                    Time time = resultSet.getTime(columnIndex);
                    return (toLocalTime(time).toNanoOfDay() * PICOSECONDS_PER_NANOSECOND) % PICOSECONDS_PER_DAY;
                },
                timeWriteFunctionUsingSqlTime());
    }

    private static LocalTime toLocalTime(Time sqlTime)
    {
        // Time.toLocalTime() does not preserve second fraction
        return sqlTime.toLocalTime()
                // TODO is the conversion correct if sqlTime.getTime() < 0?
                .withNano(toIntExact(MILLISECONDS.toNanos(sqlTime.getTime() % 1000)));
    }

    /**
     * @deprecated This method uses {@link java.sql.Time} and the class cannot represent time value when JVM zone had
     * forward offset change (a 'gap') at given time on 1970-01-01. If driver only supports {@link LocalTime}, use
     * {@link #timeWriteFunction} instead.
     */
    @Deprecated
    public static LongWriteFunction timeWriteFunctionUsingSqlTime()
    {
        return (statement, index, value) -> statement.setTime(index, toSqlTime(fromPrestoTime(value)));
    }

    private static Time toSqlTime(LocalTime localTime)
    {
        // Time.valueOf does not preserve second fraction
        return new Time(Time.valueOf(localTime).getTime() + NANOSECONDS.toMillis(localTime.getNano()));
    }

    public static ColumnMapping timeColumnMapping(TimeType timeType)
    {
        checkArgument(timeType.getPrecision() <= 9, "Unsupported type precision: %s", timeType);
        return ColumnMapping.longMapping(
                timeType,
                (resultSet, columnIndex) -> {
                    LocalTime time = resultSet.getObject(columnIndex, LocalTime.class);
                    long nanosOfDay = time.toNanoOfDay();
                    verify(nanosOfDay < NANOSECONDS_PER_DAY, "Invalid value of nanosOfDay: %s", nanosOfDay);
                    long picosOfDay = nanosOfDay * PICOSECONDS_PER_NANOSECOND;
                    return round(picosOfDay, 12 - timeType.getPrecision());
                },
                timeWriteFunction(timeType.getPrecision()));
    }

    public static LongWriteFunction timeWriteFunction(int precision)
    {
        checkArgument(precision <= 9, "Unsupported precision: %s", precision);
        return (statement, index, picosOfDay) -> {
            picosOfDay = round(picosOfDay, 12 - precision);
            if (picosOfDay == PICOSECONDS_PER_DAY) {
                picosOfDay = 0;
            }
            statement.setObject(index, fromPrestoTime(picosOfDay));
        };
    }

    /**
     * @deprecated This method uses {@link java.sql.Timestamp} and the class cannot represent date-time value when JVM zone had
     * forward offset change (a 'gap'). This includes regular DST changes (e.g. Europe/Warsaw) and one-time policy changes
     * (Asia/Kathmandu's shift by 15 minutes on January 1, 1986, 00:00:00). If driver only supports {@link LocalDateTime}, use
     * {@link #timestampColumnMapping} instead.
     */
    @Deprecated
    public static ColumnMapping timestampColumnMappingUsingSqlTimestamp(TimestampType timestampType)
    {
        // TODO support higher precision
        checkArgument(timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION, "Precision is out of range: %s", timestampType.getPrecision());
        return ColumnMapping.longMapping(
                timestampType,
                (resultSet, columnIndex) -> {
                    Timestamp timestamp = resultSet.getTimestamp(columnIndex);
                    return toPrestoTimestamp(timestampType, timestamp.toLocalDateTime());
                },
                timestampWriteFunctionUsingSqlTimestamp(timestampType));
    }

    public static ColumnMapping timestampColumnMapping(TimestampType timestampType)
    {
        if (timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION) {
            return ColumnMapping.longMapping(
                    timestampType,
                    timestampReadFunction(timestampType),
                    timestampWriteFunction(timestampType));
        }
        checkArgument(timestampType.getPrecision() <= MAX_LOCAL_DATE_TIME_PRECISION, "Precision is out of range: %s", timestampType.getPrecision());
        return ColumnMapping.objectMapping(
                timestampType,
                longTimestampReadFunction(timestampType),
                longTimestampWriteFunction(timestampType));
    }

    public static LongReadFunction timestampReadFunction(TimestampType timestampType)
    {
        checkArgument(timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION, "Precision is out of range: %s", timestampType.getPrecision());
        return (resultSet, columnIndex) -> toPrestoTimestamp(timestampType, resultSet.getObject(columnIndex, LocalDateTime.class));
    }

    private static ObjectReadFunction longTimestampReadFunction(TimestampType timestampType)
    {
        checkArgument(timestampType.getPrecision() > TimestampType.MAX_SHORT_PRECISION && timestampType.getPrecision() < MAX_LOCAL_DATE_TIME_PRECISION,
                "Precision is out of range: %s", timestampType.getPrecision());
        return ObjectReadFunction.of(
                LongTimestamp.class,
                (resultSet, columnIndex) -> toLongTimestamp(timestampType, resultSet.getObject(columnIndex, LocalDateTime.class)));
    }

    /**
     * @deprecated This method uses {@link java.sql.Timestamp} and the class cannot represent date-time value when JVM zone had
     * forward offset change (a 'gap'). This includes regular DST changes (e.g. Europe/Warsaw) and one-time policy changes
     * (Asia/Kathmandu's shift by 15 minutes on January 1, 1986, 00:00:00). If driver only supports {@link LocalDateTime}, use
     * {@link #timestampWriteFunction} instead.
     */
    @Deprecated
    public static LongWriteFunction timestampWriteFunctionUsingSqlTimestamp(TimestampType timestampType)
    {
        checkArgument(timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION, "Precision is out of range: %s", timestampType.getPrecision());
        return (statement, index, value) -> statement.setTimestamp(index, Timestamp.valueOf(fromPrestoTimestamp(value)));
    }

    public static LongWriteFunction timestampWriteFunction(TimestampType timestampType)
    {
        checkArgument(timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION, "Precision is out of range: %s", timestampType.getPrecision());
        return (statement, index, value) -> statement.setObject(index, fromPrestoTimestamp(value));
    }

    public static ObjectWriteFunction longTimestampWriteFunction(TimestampType timestampType)
    {
        checkArgument(timestampType.getPrecision() > TimestampType.MAX_SHORT_PRECISION, "Precision is out of range: %s", timestampType.getPrecision());
        return ObjectWriteFunction.of(
                LongTimestamp.class,
                (statement, index, value) -> statement.setObject(index, fromLongTimestamp(value, timestampType.getPrecision())));
    }

    public static long toPrestoTimestamp(TimestampType timestampType, LocalDateTime localDateTime)
    {
        long precision = timestampType.getPrecision();
        checkArgument(precision <= TimestampType.MAX_SHORT_PRECISION, "Precision is out of range: %s", precision);
        Instant instant = localDateTime.atZone(UTC).toInstant();
        long epochMicros = instant.getEpochSecond() * MICROSECONDS_PER_SECOND + roundDiv(instant.getNano(), NANOSECONDS_PER_MICROSECOND);
        verify(
                epochMicros == round(epochMicros, TimestampType.MAX_SHORT_PRECISION - timestampType.getPrecision()),
                "Invalid value of epochMicros for precision %s: %s",
                timestampType.getPrecision(),
                epochMicros);
        return epochMicros;
    }

    public static LongTimestamp toLongTimestamp(TimestampType timestampType, LocalDateTime localDateTime)
    {
        long precision = timestampType.getPrecision();
        checkArgument(precision > TimestampType.MAX_SHORT_PRECISION, "Precision is out of range: %s", precision);
        Instant instant = localDateTime.atZone(UTC).toInstant();
        long epochMicros = instant.getEpochSecond() * MICROSECONDS_PER_SECOND + floorDiv(instant.getNano(), NANOSECONDS_PER_MICROSECOND);
        int picosOfMicro = (instant.getNano() % NANOSECONDS_PER_MICROSECOND) * PICOSECONDS_PER_NANOSECOND;
        verify(
                picosOfMicro == round(picosOfMicro, TimestampType.MAX_PRECISION - timestampType.getPrecision()),
                "Invalid value of picosOfMicro for precision %s: %s",
                timestampType.getPrecision(),
                picosOfMicro);
        return new LongTimestamp(epochMicros, picosOfMicro);
    }

    public static LocalDateTime fromPrestoTimestamp(long epochMicros)
    {
        long epochSecond = floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
        int nanoFraction = floorMod(epochMicros, MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND;
        Instant instant = Instant.ofEpochSecond(epochSecond, nanoFraction);
        return LocalDateTime.ofInstant(instant, UTC);
    }

    public static LocalDateTime fromLongTimestamp(LongTimestamp timestamp, int precision)
    {
        long epochSeconds = floorDiv(timestamp.getEpochMicros(), MICROSECONDS_PER_SECOND);
        int microsOfSecond = floorMod(timestamp.getEpochMicros(), MICROSECONDS_PER_SECOND);
        long picosOfMicro = round(timestamp.getPicosOfMicro(), TimestampType.MAX_PRECISION - precision);
        int nanosOfSecond = (microsOfSecond * NANOSECONDS_PER_MICROSECOND) + toIntExact(picosOfMicro / PICOSECONDS_PER_NANOSECOND);

        Instant instant = Instant.ofEpochSecond(epochSeconds, nanosOfSecond);
        return LocalDateTime.ofInstant(instant, UTC);
    }

    public static LocalTime fromPrestoTime(long value)
    {
        // value can round up to NANOSECONDS_PER_DAY, so we need to do % to keep it in the desired range
        return LocalTime.ofNanoOfDay(roundDiv(value, PICOSECONDS_PER_NANOSECOND) % NANOSECONDS_PER_DAY);
    }

    /**
     * @deprecated Each connector should provide its own explicit type mapping, along with respective tests.
     */
    @Deprecated
    public static Optional<ColumnMapping> jdbcTypeToPrestoType(JdbcTypeHandle typeHandle)
    {
        switch (typeHandle.getJdbcType()) {
            case Types.BIT:
            case Types.BOOLEAN:
                return Optional.of(booleanColumnMapping());

            case Types.TINYINT:
                return Optional.of(tinyintColumnMapping());

            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());

            case Types.INTEGER:
                return Optional.of(integerColumnMapping());

            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());

            case Types.REAL:
                return Optional.of(realColumnMapping());

            case Types.FLOAT:
            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.NUMERIC:
            case Types.DECIMAL:
                int decimalDigits = typeHandle.getRequiredDecimalDigits();
                int precision = typeHandle.getRequiredColumnSize() + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                if (precision > Decimals.MAX_PRECISION) {
                    return Optional.empty();
                }
                return Optional.of(decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0))));

            case Types.CHAR:
            case Types.NCHAR:
                return Optional.of(defaultCharColumnMapping(typeHandle.getRequiredColumnSize()));

            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                return Optional.of(defaultVarcharColumnMapping(typeHandle.getRequiredColumnSize()));

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Optional.of(varbinaryColumnMapping());

            case Types.DATE:
                return Optional.of(dateColumnMapping());

            case Types.TIME:
                // TODO default to `timeColumnMapping`
                return Optional.of(timeColumnMappingUsingSqlTime());

            case Types.TIMESTAMP:
                // TODO default to `timestampColumnMapping`
                return Optional.of(timestampColumnMappingUsingSqlTimestamp(TIMESTAMP_MILLIS));
        }
        return Optional.empty();
    }
}
