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
package io.prestosql.jdbc;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logging;
import io.prestosql.server.testing.TestingPrestoServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestJdbcResultSet
{
    private TestingPrestoServer server;

    private Connection connection;
    private Statement statement;

    @BeforeClass
    public void setupServer()
    {
        Logging.initialize();
        server = TestingPrestoServer.create();
    }

    @AfterClass(alwaysRun = true)
    public void tearDownServer()
            throws Exception
    {
        server.close();
    }

    @SuppressWarnings("JDBCResourceOpenedButNotSafelyClosed")
    @BeforeMethod
    public void setup()
            throws Exception
    {
        connection = createConnection();
        statement = connection.createStatement();
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
            throws Exception
    {
        statement.close();
        connection.close();
    }

    @Test
    public void testDuplicateColumnLabels()
            throws Exception
    {
        try (ResultSet rs = statement.executeQuery("SELECT 123 x, 456 x")) {
            ResultSetMetaData metadata = rs.getMetaData();
            assertEquals(metadata.getColumnCount(), 2);
            assertEquals(metadata.getColumnName(1), "x");
            assertEquals(metadata.getColumnName(2), "x");

            assertTrue(rs.next());
            assertEquals(rs.getLong(1), 123L);
            assertEquals(rs.getLong(2), 456L);
            assertEquals(rs.getLong("x"), 123L);
        }
    }

    @Test
    public void testPrimitiveTypes()
            throws Exception
    {
        checkRepresentation("123", Types.INTEGER, 123);
        checkRepresentation("12300000000", Types.BIGINT, 12300000000L);
        checkRepresentation("REAL '123.45'", Types.REAL, 123.45f);
        checkRepresentation("1e-1", Types.DOUBLE, 0.1);
        checkRepresentation("1.0E0 / 0.0E0", Types.DOUBLE, Double.POSITIVE_INFINITY);
        checkRepresentation("0.0E0 / 0.0E0", Types.DOUBLE, Double.NaN);
        checkRepresentation("true", Types.BOOLEAN, true);
        checkRepresentation("'hello'", Types.VARCHAR, "hello");
        checkRepresentation("cast('foo' as char(5))", Types.CHAR, "foo  ");
    }

    @Test
    public void testDecimal()
            throws Exception
    {
        checkRepresentation("0.1", Types.DECIMAL, new BigDecimal("0.1"));
        checkRepresentation("DECIMAL '0.1'", Types.DECIMAL, new BigDecimal("0.1"));
    }

    @Test
    public void testDate()
            throws Exception
    {
        checkRepresentation("DATE '2018-02-13'", Types.DATE, (rs, column) -> {
            assertEquals(rs.getObject(column), Date.valueOf(LocalDate.of(2018, 2, 13)));
            assertEquals(rs.getObject(column, Date.class), Date.valueOf(LocalDate.of(2018, 2, 13)));
            assertEquals(rs.getDate(column), Date.valueOf(LocalDate.of(2018, 2, 13)));
            assertThrows(IllegalArgumentException.class, () -> rs.getTime(column));
            assertThrows(IllegalArgumentException.class, () -> rs.getTimestamp(column));
        });
    }

    @Test
    public void testTime()
            throws Exception
    {
        checkRepresentation("TIME '09:39:05'", Types.TIME, (rs, column) -> {
            assertEquals(rs.getObject(column), Time.valueOf(LocalTime.of(9, 39, 5)));
            assertEquals(rs.getObject(column, Time.class), Time.valueOf(LocalTime.of(9, 39, 5)));
            assertThrows(() -> rs.getDate(column));
            assertEquals(rs.getTime(column), Time.valueOf(LocalTime.of(9, 39, 5)));
            assertThrows(() -> rs.getTimestamp(column));
        });

        // TODO https://github.com/prestosql/presto/issues/37
        // TODO line 1:8: '00:39:05' is not a valid time literal
//        checkRepresentation("TIME '00:39:05'", Types.TIME, (rs, column) -> {
//            ...
//        });

        checkRepresentation("TIME '09:39:07 +01:00'", Types.TIME /* TODO TIME_WITH_TIMEZONE */, (rs, column) -> {
            assertEquals(rs.getObject(column), Time.valueOf(LocalTime.of(1, 39, 7))); // TODO this should represent TIME '09:39:07 +01:00'
            assertThrows(() -> rs.getDate(column));
            assertEquals(rs.getTime(column), Time.valueOf(LocalTime.of(1, 39, 7))); // TODO this should fail, or represent TIME '09:39:07'
            assertThrows(() -> rs.getTimestamp(column));
        });

        checkRepresentation("TIME '01:39:07 +01:00'", Types.TIME /* TODO TIME_WITH_TIMEZONE */, (rs, column) -> {
            Time someBogusValue = new Time(
                    Time.valueOf(
                            LocalTime.of(16, 39, 7)).getTime() /* 16:39:07 = 01:39:07 - +01:00 shift + Bahia_Banderas's shift (-8) (modulo 24h which we "un-modulo" below) */
                            - DAYS.toMillis(1) /* because we use currently 'shifted' representation, not possible to create just using LocalTime */
                            + HOURS.toMillis(1) /* because there was offset shift on 1970-01-01 in America/Bahia_Banderas */);
            assertEquals(rs.getObject(column), someBogusValue); // TODO this should represent TIME '01:39:07 +01:00'
            assertThrows(() -> rs.getDate(column));
            assertEquals(rs.getTime(column), someBogusValue); // TODO this should fail, or represent TIME '01:39:07'
            assertThrows(() -> rs.getTimestamp(column));
        });

        checkRepresentation("TIME '00:39:07 +01:00'", Types.TIME /* TODO TIME_WITH_TIMEZONE */, (rs, column) -> {
            Time someBogusValue = new Time(
                    Time.valueOf(
                            LocalTime.of(15, 39, 7)).getTime() /* 15:39:07 = 00:39:07 - +01:00 shift + Bahia_Banderas's shift (-8) (modulo 24h which we "un-modulo" below) */
                            - DAYS.toMillis(1) /* because we use currently 'shifted' representation, not possible to create just using LocalTime */
                            + HOURS.toMillis(1) /* because there was offset shift on 1970-01-01 in America/Bahia_Banderas */);
            assertEquals(rs.getObject(column), someBogusValue); // TODO this should represent TIME '00:39:07 +01:00'
            assertThrows(() -> rs.getDate(column));
            assertEquals(rs.getTime(column), someBogusValue); // TODO this should fail, as there no java.sql.Time representation for TIME '00:39:07' in America/Bahia_Banderas
            assertThrows(() -> rs.getTimestamp(column));
        });
    }

    @Test
    public void testTimestamp()
            throws Exception
    {
        checkRepresentation("TIMESTAMP '2018-02-13 13:14:15.123'", Types.TIMESTAMP, (rs, column) -> {
            assertEquals(rs.getObject(column), Timestamp.valueOf(LocalDateTime.of(2018, 2, 13, 13, 14, 15, 123_000_000)));
            assertEquals(rs.getObject(column, Timestamp.class), Timestamp.valueOf(LocalDateTime.of(2018, 2, 13, 13, 14, 15, 123_000_000)));
            assertThrows(() -> rs.getDate(column));
            assertThrows(() -> rs.getTime(column));
            assertEquals(rs.getTimestamp(column), Timestamp.valueOf(LocalDateTime.of(2018, 2, 13, 13, 14, 15, 123_000_000)));
        });

        checkRepresentation("TIMESTAMP '2018-02-13 13:14:15.111111111111'", Types.TIMESTAMP, (rs, column) -> {
            assertEquals(rs.getObject(column), Timestamp.valueOf(LocalDateTime.of(2018, 2, 13, 13, 14, 15, 111_111_111)));
            assertThrows(() -> rs.getDate(column));
            assertThrows(() -> rs.getTime(column));
            assertEquals(rs.getTimestamp(column), Timestamp.valueOf(LocalDateTime.of(2018, 2, 13, 13, 14, 15, 111_111_111)));
        });

        checkRepresentation("TIMESTAMP '2018-02-13 13:14:15.555555555555'", Types.TIMESTAMP, (rs, column) -> {
            assertEquals(rs.getObject(column), Timestamp.valueOf(LocalDateTime.of(2018, 2, 13, 13, 14, 15, 555_555_556)));
            assertThrows(() -> rs.getDate(column));
            assertThrows(() -> rs.getTime(column));
            assertEquals(rs.getTimestamp(column), Timestamp.valueOf(LocalDateTime.of(2018, 2, 13, 13, 14, 15, 555_555_556)));
        });

        // TODO https://github.com/prestosql/presto/issues/37
        // TODO line 1:8: '1970-01-01 00:14:15.123' is not a valid timestamp literal; the expected values will pro
//        checkRepresentation("TIMESTAMP '1970-01-01 00:14:15.123'", Types.TIMESTAMP, (rs, column) -> {
//            ...
//        });

        // TODO https://github.com/prestosql/presto/issues/4363
//        checkRepresentation("TIMESTAMP '-123456-01-23 01:23:45.123456789'", Types.TIMESTAMP, (rs, column) -> {
//            ...
//        });

        checkRepresentation("TIMESTAMP '123456-01-23 01:23:45.123456789'", Types.TIMESTAMP, (rs, column) -> {
            assertEquals(rs.getObject(column), Timestamp.valueOf(LocalDateTime.of(123456, 1, 23, 1, 23, 45, 123_456_789)));
            assertThrows(() -> rs.getDate(column));
            assertThrows(() -> rs.getTime(column));
            assertEquals(rs.getTimestamp(column), Timestamp.valueOf(LocalDateTime.of(123456, 1, 23, 1, 23, 45, 123_456_789)));
        });

        checkRepresentation("TIMESTAMP '2018-02-13 13:14:15.227 Europe/Warsaw'", Types.TIMESTAMP /* TODO TIMESTAMP_WITH_TIMEZONE */, (rs, column) -> {
            assertEquals(rs.getObject(column), Timestamp.valueOf(LocalDateTime.of(2018, 2, 13, 6, 14, 15, 227_000_000))); // TODO this should represent TIMESTAMP '2018-02-13 13:14:15.227 Europe/Warsaw'
            assertThrows(() -> rs.getDate(column));
            assertThrows(() -> rs.getTime(column));
            assertEquals(rs.getTimestamp(column), Timestamp.valueOf(LocalDateTime.of(2018, 2, 13, 6, 14, 15, 227_000_000))); // TODO this should fail, or represent TIMESTAMP '2018-02-13 13:14:15.227'
        });

        checkRepresentation("TIMESTAMP '1970-01-01 09:14:15.227 Europe/Warsaw'", Types.TIMESTAMP /* TODO TIMESTAMP_WITH_TIMEZONE */, (rs, column) -> {
            assertEquals(rs.getObject(column), Timestamp.valueOf(LocalDateTime.of(1970, 1, 1, 1, 14, 15, 227_000_000))); // TODO this should represent TIMESTAMP '1970-01-01 09:14:15.227 Europe/Warsaw'
            assertThrows(() -> rs.getDate(column));
            assertThrows(() -> rs.getTime(column));
            assertEquals(rs.getTimestamp(column), Timestamp.valueOf(LocalDateTime.of(1970, 1, 1, 1, 14, 15, 227_000_000))); // TODO this should fail, or represent TIMESTAMP '1970-01-01 09:14:15.227'
        });

        checkRepresentation("TIMESTAMP '1970-01-01 00:14:15.227 Europe/Warsaw'", Types.TIMESTAMP /* TODO TIMESTAMP_WITH_TIMEZONE */, (rs, column) -> {
            assertEquals(rs.getObject(column), Timestamp.valueOf(LocalDateTime.of(1969, 12, 31, 15, 14, 15, 227_000_000))); // TODO this should represent TIMESTAMP '1970-01-01 00:14:15.227 Europe/Warsaw'
            assertThrows(() -> rs.getDate(column));
            assertThrows(() -> rs.getTime(column));
            // TODO this should fail, as there no java.sql.Timestamp representation for TIMESTAMP '1970-01-01 00:14:15.227ó' in America/Bahia_Banderas
            assertEquals(rs.getTimestamp(column), Timestamp.valueOf(LocalDateTime.of(1969, 12, 31, 15, 14, 15, 227_000_000)));
        });

        // TODO https://github.com/prestosql/presto/issues/4363
//        checkRepresentation("TIMESTAMP '-12345-01-23 01:23:45.123456789 Europe/Warsaw'", Types.TIMESTAMP /* TODO TIMESTAMP_WITH_TIMEZONE */, (rs, column) -> {
//            ...
//        });

        checkRepresentation("TIMESTAMP '12345-01-23 01:23:45.123456789 Europe/Warsaw'", Types.TIMESTAMP /* TODO TIMESTAMP_WITH_TIMEZONE */, (rs, column) -> {
            assertEquals(rs.getObject(column), Timestamp.valueOf(LocalDateTime.of(12345, 1, 22, 18, 23, 45, 123_456_789))); // TODO this should contain the zone
            assertThrows(() -> rs.getDate(column));
            assertThrows(() -> rs.getTime(column));
            assertEquals(rs.getTimestamp(column), Timestamp.valueOf(LocalDateTime.of(12345, 1, 22, 18, 23, 45, 123_456_789)));
        });
    }

    @Test
    public void testIpAddress()
            throws Exception
    {
        checkRepresentation("IPADDRESS '1.2.3.4'", Types.JAVA_OBJECT, "1.2.3.4");
    }

    @Test
    public void testUuid()
            throws Exception
    {
        checkRepresentation("UUID '0397e63b-2b78-4b7b-9c87-e085fa225dd8'", Types.JAVA_OBJECT, "0397e63b-2b78-4b7b-9c87-e085fa225dd8");
    }

    @Test
    public void testArray()
            throws Exception
    {
        checkRepresentation("ARRAY[1, 2]", Types.ARRAY, (rs, column) -> assertEquals(rs.getArray(column).getArray(), new int[] {1, 2}));
    }

    @Test
    public void testMap()
            throws Exception
    {
        checkRepresentation("map(ARRAY['k1', 'k2'], ARRAY[BIGINT '42', -117])", Types.JAVA_OBJECT, (rs, column) -> {
            assertEquals(rs.getObject(column), ImmutableMap.of("k1", 42L, "k2", -117L));
            assertEquals(rs.getObject(column, Map.class), ImmutableMap.of("k1", 42L, "k2", -117L));
        });

        // NULL value
        checkRepresentation("map(ARRAY['k1', 'k2'], ARRAY[42, NULL])", Types.JAVA_OBJECT, (rs, column) -> {
            Map<String, Integer> expected = new HashMap<>();
            expected.put("k1", 42);
            expected.put("k2", null);
            assertEquals(rs.getObject(column), expected);
            assertEquals(rs.getObject(column, Map.class), expected);
        });
    }

    @Test
    public void testRow()
            throws Exception
    {
        // named row
        checkRepresentation("CAST(ROW(42, 'Presto') AS ROW(a_bigint bigint, a_varchar varchar(17)))", Types.JAVA_OBJECT, (rs, column) -> {
            assertEquals(rs.getObject(column), ImmutableMap.of("a_bigint", 42L, "a_varchar", "Presto"));
            assertEquals(rs.getObject(column, Map.class), ImmutableMap.of("a_bigint", 42L, "a_varchar", "Presto"));
        });

        // partially named row
        checkRepresentation("CAST(ROW(42, 'Presto') AS ROW(a_bigint bigint, varchar(17)))", Types.JAVA_OBJECT, (rs, column) -> {
            assertEquals(rs.getObject(column), ImmutableMap.of("a_bigint", 42L, "field1", "Presto"));
            assertEquals(rs.getObject(column, Map.class), ImmutableMap.of("a_bigint", 42L, "field1", "Presto"));
        });

        // anonymous row
        checkRepresentation("ROW(42, 'Presto')", Types.JAVA_OBJECT, (rs, column) -> {
            assertEquals(rs.getObject(column), ImmutableMap.of("field0", 42, "field1", "Presto"));
            assertEquals(rs.getObject(column, Map.class), ImmutableMap.of("field0", 42, "field1", "Presto"));
        });

        // name collision
        checkRepresentation("CAST(ROW(42, 'Presto') AS ROW(field1 integer, varchar(17)))", Types.JAVA_OBJECT, (rs, column) -> {
            // TODO (https://github.com/prestosql/presto/issues/4594) both fields should be visible or exception thrown
            assertEquals(rs.getObject(column), ImmutableMap.of("field1", "Presto"));
            assertEquals(rs.getObject(column, Map.class), ImmutableMap.of("field1", "Presto"));
        });
    }

    private void checkRepresentation(String expression, int expectedSqlType, Object expectedRepresentation)
            throws Exception
    {
        checkRepresentation(expression, expectedSqlType, (rs, column) -> {
            assertEquals(rs.getObject(column), expectedRepresentation);
            assertEquals(rs.getObject(column, expectedRepresentation.getClass()), expectedRepresentation);
        });
    }

    private void checkRepresentation(String expression, int expectedSqlType, ResultAssertion assertion)
            throws Exception
    {
        try (ResultSet rs = statement.executeQuery("SELECT " + expression)) {
            ResultSetMetaData metadata = rs.getMetaData();
            assertEquals(metadata.getColumnCount(), 1);
            assertEquals(metadata.getColumnType(1), expectedSqlType);
            assertTrue(rs.next());
            assertion.accept(rs, 1);
            assertFalse(rs.next());
        }
    }

    @Test
    public void testStatsExtraction()
            throws Exception
    {
        try (PrestoResultSet rs = (PrestoResultSet) statement.executeQuery("SELECT 123 x, 456 x")) {
            assertNotNull(rs.getStats());
            assertTrue(rs.next());
            assertNotNull(rs.getStats());
            assertFalse(rs.next());
            assertNotNull(rs.getStats());
        }
    }

    @Test
    public void testMaxRowsUnset()
            throws SQLException
    {
        assertMaxRowsLimit(0);
        assertMaxRowsResult(7);
    }

    @Test
    public void testMaxRowsUnlimited()
            throws SQLException
    {
        assertMaxRowsLimit(0);
        statement.setMaxRows(0);
        assertMaxRowsLimit(0);
        assertMaxRowsResult(7);
    }

    @Test
    public void testMaxRowsLimited()
            throws SQLException
    {
        assertMaxRowsLimit(0);
        statement.setMaxRows(4);
        assertMaxRowsLimit(4);
        assertMaxRowsResult(4);
    }

    @Test
    public void testMaxRowsLimitLargerThanResult()
            throws SQLException
    {
        assertMaxRowsLimit(0);
        statement.setMaxRows(10);
        assertMaxRowsLimit(10);
        assertMaxRowsResult(7);
    }

    @Test
    public void testLargeMaxRowsUnlimited()
            throws SQLException
    {
        assertMaxRowsLimit(0);
        statement.setLargeMaxRows(0);
        assertMaxRowsLimit(0);
        assertMaxRowsResult(7);
    }

    @Test
    public void testLargeMaxRowsLimited()
            throws SQLException
    {
        assertMaxRowsLimit(0);
        statement.setLargeMaxRows(4);
        assertMaxRowsLimit(4);
        assertMaxRowsResult(4);
    }

    @Test
    public void testLargeMaxRowsLimitLargerThanResult()
            throws SQLException
    {
        long limit = Integer.MAX_VALUE * 10L;
        statement.setLargeMaxRows(limit);
        assertEquals(statement.getLargeMaxRows(), limit);
        assertMaxRowsResult(7);
    }

    private void assertMaxRowsLimit(int expectedLimit)
            throws SQLException
    {
        assertEquals(statement.getMaxRows(), expectedLimit);
        assertEquals(statement.getLargeMaxRows(), expectedLimit);
    }

    private void assertMaxRowsResult(long expectedCount)
            throws SQLException
    {
        try (ResultSet rs = statement.executeQuery("SELECT * FROM (VALUES (1), (2), (3), (4), (5), (6), (7)) AS x (a)")) {
            assertEquals(countRows(rs), expectedCount);
        }
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Max rows exceeds limit of 2147483647")
    public void testMaxRowsExceedsLimit()
            throws SQLException
    {
        statement.setLargeMaxRows(Integer.MAX_VALUE * 10L);
        statement.getMaxRows();
    }

    private Connection createConnection()
            throws SQLException
    {
        String url = format("jdbc:presto://%s", server.getAddress());
        return DriverManager.getConnection(url, "test", null);
    }

    private static long countRows(ResultSet rs)
            throws SQLException
    {
        long count = 0;
        while (rs.next()) {
            count++;
        }
        return count;
    }

    @FunctionalInterface
    private interface ResultAssertion
    {
        void accept(ResultSet rs, int column)
                throws Exception;
    }
}
