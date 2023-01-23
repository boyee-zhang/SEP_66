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

import com.google.common.base.Joiner;
import io.trino.plugin.jdbc.JdbcProcedureHandle.ProcedureQuery;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.jdbc.ptf.Procedure;
import io.trino.spi.connector.ConnectorSession;

import javax.inject.Inject;

import java.sql.Connection;

public class TestingH2QueryBuilder
        extends DefaultQueryBuilder
{
    @Inject
    public TestingH2QueryBuilder(RemoteQueryModifier queryModifier)
    {
        super(queryModifier);
    }

    @Override
    public ProcedureQuery createProcedureQuery(JdbcClient client, ConnectorSession session, Connection connection, Procedure.ProcedureInformation procedureInformation)
    {
        return new JdbcProcedureHandle.ProcedureQuery(
                "CALL %s.%s (%s)"
                        .formatted(
                                procedureInformation.schemaName(),
                                procedureInformation.procedureName(),
                                Joiner.on(", ").join(procedureInformation.inputArguments())));
    }
}
