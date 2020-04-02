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
package io.prestosql.plugin.jdbc;

import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.type.Type;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class JdbcRecordSet
        implements RecordSet
{
    private final JdbcClient jdbcClient;
    private final JdbcTableHandle table;
    private final JdbcSplit split;
    private final ConnectorSession session;

    public JdbcRecordSet(JdbcClient jdbcClient, ConnectorSession session, JdbcSplit split, JdbcTableHandle table)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        this.split = requireNonNull(split, "split is null");
        this.table = requireNonNull(table, "table is null");
        this.session = requireNonNull(session, "session is null");
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return table.getColumns()
                .stream()
                .map(JdbcColumnHandle::getColumnType)
                .collect(toImmutableList());
    }

    @Override
    public RecordCursor cursor()
    {
        return new JdbcRecordCursor(jdbcClient, session, split, table);
    }
}
