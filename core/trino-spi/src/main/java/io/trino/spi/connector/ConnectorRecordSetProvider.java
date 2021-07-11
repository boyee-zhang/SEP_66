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
package io.trino.spi.connector;

import java.util.List;

public interface ConnectorRecordSetProvider
{
    default RecordSet getRecordSet(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<? extends ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        return getRecordSet(transaction, session, split, columns, dynamicFilter);
    }

    @Deprecated
    default RecordSet getRecordSet(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            List<? extends ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        throw new UnsupportedOperationException("getRecordSet() must be implemented");
    }
}
