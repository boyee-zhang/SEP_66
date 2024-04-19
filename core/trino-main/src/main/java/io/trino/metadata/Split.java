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
package io.trino.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.lang.System.identityHashCode;
import static java.util.Objects.requireNonNull;

public record Split(CatalogHandle catalogHandle, ConnectorSplit connectorSplit)
{
    private static final int INSTANCE_SIZE = instanceSize(Split.class);

    public Split
    {
        requireNonNull(catalogHandle, "catalogHandle is null");
        requireNonNull(connectorSplit, "connectorSplit is null");
    }

    @JsonIgnore // TODO remove after https://github.com/airlift/airlift/pull/1141
    public Map<String, String> getInfo()
    {
        return firstNonNull(connectorSplit.getSplitInfo(), ImmutableMap.of());
    }

    @JsonIgnore // TODO remove after https://github.com/airlift/airlift/pull/1141
    public List<HostAddress> getAddresses()
    {
        return connectorSplit.getAddresses();
    }

    @JsonIgnore // TODO remove after https://github.com/airlift/airlift/pull/1141
    public boolean isRemotelyAccessible()
    {
        return connectorSplit.isRemotelyAccessible();
    }

    @JsonIgnore // TODO remove after https://github.com/airlift/airlift/pull/1141
    public SplitWeight getSplitWeight()
    {
        return connectorSplit.getSplitWeight();
    }

    @JsonIgnore // TODO remove after https://github.com/airlift/airlift/pull/1141
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + catalogHandle.getRetainedSizeInBytes()
                + connectorSplit.getRetainedSizeInBytes();
    }

    @Override
    public boolean equals(Object o)
    {
        // There is no equality contract for ConnectorSplit and there is no requirement for
        // ConnectorSplits returned from ConnectorSplitSource to be different. For example,
        // a ConnectorSplitSource could return 10 times same "produce a row" split instance,
        // so that the table has effectively 10 rows. The blackhole connector does that.
        return this == o;
    }

    @Override
    public int hashCode()
    {
        return identityHashCode(this);
    }
}
