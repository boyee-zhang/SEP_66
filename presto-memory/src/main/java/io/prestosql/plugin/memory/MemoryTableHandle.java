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
package io.prestosql.plugin.memory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.Longs;
import io.prestosql.spi.connector.ConnectorTableHandle;

import java.util.Objects;

public final class MemoryTableHandle
        implements ConnectorTableHandle
{
    private final long id;

    @JsonCreator
    public MemoryTableHandle(@JsonProperty("id") long id)
    {
        this.id = id;
    }

    @JsonProperty
    public long getId()
    {
        return id;
    }

    @Override
    public int hashCode()
    {
        return Longs.hashCode(id);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MemoryTableHandle other = (MemoryTableHandle) obj;
        return Objects.equals(this.getId(), other.getId());
    }

    @Override
    public String toString()
    {
        return Long.toString(id);
    }
}
