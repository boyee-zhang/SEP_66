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
package io.prestosql.plugin.bigquery;

import io.prestosql.spi.connector.ConnectorTransactionHandle;

public enum BigQueryTransactionHandle
        implements ConnectorTransactionHandle
{
    INSTANCE;
}
//public class BigQueryTransactionHandle
//        implements ConnectorTransactionHandle
//{
//    private final UUID uuid;
//
//    public BigQueryTransactionHandle()
//    {
//        this(UUID.randomUUID());
//    }
//
//    @JsonCreator
//    public BigQueryTransactionHandle(@JsonProperty("uuid") UUID uuid)
//    {
//        this.uuid = requireNonNull(uuid, "uuid is null");
//    }
//
//    @JsonProperty
//    public UUID getUuid()
//    {
//        return uuid;
//    }
//
//    @Override
//    public boolean equals(Object obj)
//    {
//        if (this == obj) {
//            return true;
//        }
//        if ((obj == null) || (getClass() != obj.getClass())) {
//            return false;
//        }
//        BigQueryTransactionHandle other = (BigQueryTransactionHandle) obj;
//        return Objects.equals(uuid, other.uuid);
//    }
//
//    @Override
//    public int hashCode()
//    {
//        return Objects.hash(uuid);
//    }
//
//    @Override
//    public String toString()
//    {
//        return toStringHelper(this)
//                .add("uuid", uuid)
//                .toString();
//    }
//}
