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
package io.trino.plugin.clickhouse;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

public class ClickHouseConfig
{
    // TODO (https://github.com/trinodb/trino/issues/7102) reconsider default behavior
    private boolean mapStringAsVarchar;
    private int maxSplitsPerShard = 20;
    private String connectionUser;
    private String connectionPassword;

    public boolean isMapStringAsVarchar()
    {
        return mapStringAsVarchar;
    }

    @Config("clickhouse.map-string-as-varchar")
    @ConfigDescription("Map ClickHouse String and FixedString as varchar instead of varbinary")
    public ClickHouseConfig setMapStringAsVarchar(boolean mapStringAsVarchar)
    {
        this.mapStringAsVarchar = mapStringAsVarchar;
        return this;
    }

    public int getMaxSplitsPerShard()
    {
        return maxSplitsPerShard;
    }

    @Config("clickhouse.max-splits-per-shard")
    @ConfigDescription("max trino splits per clickhouse shard")
    public ClickHouseConfig setMaxSplitsPerShard(int maxSplitsPerShard)
    {
        this.maxSplitsPerShard = maxSplitsPerShard;
        return this;
    }

    public String getConnectionUser()
    {
        return connectionUser;
    }

    @Config("connection-user")
    @ConfigDescription("connection-user")
    public ClickHouseConfig setConnectionUser(String connectionUser)
    {
        this.connectionUser = connectionUser;
        return this;
    }

    public String getConnectionPassword()
    {
        return connectionPassword;
    }

    @Config("connection-password")
    @ConfigDescription("connection-password")
    public ClickHouseConfig setConnectionPassword(String connectionPassword)
    {
        this.connectionPassword = connectionPassword;
        return this;
    }
}
