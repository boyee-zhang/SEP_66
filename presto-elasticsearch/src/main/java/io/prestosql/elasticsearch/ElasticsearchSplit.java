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
package io.prestosql.elasticsearch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ElasticsearchSplit
        implements ConnectorSplit
{
    private final int shard;
    private final Optional<String> address;

    @JsonCreator
    public ElasticsearchSplit(
            @JsonProperty("shard") int shard,
            @JsonProperty("address") Optional<String> address)
    {
        this.shard = shard;
        this.address = requireNonNull(address, "address is null");
    }

    @JsonProperty
    public int getShard()
    {
        return shard;
    }

    @JsonProperty
    public Optional<String> getAddress()
    {
        return address;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return address.map(host -> ImmutableList.of(HostAddress.fromString(host)))
                .orElseGet(ImmutableList::of);
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(shard)
                .toString();
    }
}
