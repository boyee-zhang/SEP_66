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
package io.trino.operator.scalar;

import com.google.common.base.MoreObjects;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import io.trino.sql.parser.ParsingException;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.Statement;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.sql.SqlFormatter.formatSql;

public final class SqlFunctions
{
    private SqlFunctions() {}

    private static final SqlParser SQL_PARSER = new SqlParser();

    @ScalarFunction
    @Description("Format Trino SQL statement")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice formatTrinoQuery(@SqlType(StandardTypes.VARCHAR) Slice query)
            throws TrinoException
    {
        String queryString = query.toStringUtf8();
        Statement statement;
        try {
            statement = SQL_PARSER.createStatement(queryString, new NodeLocation(1, 1));
        }
        catch (ParsingException e) {
            throw new TrinoException(
                    INVALID_FUNCTION_ARGUMENT,
                    "Input cannot be parsed as a statement: [%s]: %s".formatted(queryString, MoreObjects.firstNonNull(e.getMessage(), e)),
                    e);
        }
        String formatted = formatSql(statement).trim();
        return utf8Slice(formatted);
    }
}
