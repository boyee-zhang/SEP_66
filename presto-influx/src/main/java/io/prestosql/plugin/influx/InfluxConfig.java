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

package io.prestosql.plugin.influx;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class InfluxConfig
{

    private long cacheMetaDataMillis = 10000;
    private String host = "localhost";
    private int port = 8086;
    private String database;
    private String userName;
    private String password;

    @NotNull
    public long getCacheMetaDataMillis()
    {
        return cacheMetaDataMillis;
    }

    @Config("cache-meta-data-millis")
    public InfluxConfig setCacheMetaDataMillis(long cacheMetaDataMillis)
    {
        this.cacheMetaDataMillis = cacheMetaDataMillis;
        return this;
    }

    @NotNull
    public String getHost()
    {
        return host;
    }

    @Config("host")
    public InfluxConfig setHost(String host)
    {
        this.host = host;
        return this;
    }

    public int getPort()
    {
        return port;
    }

    @Config("port")
    public InfluxConfig setPort(int port)
    {
        this.port = port;
        return this;
    }

    @NotNull
    public String getDatabase()
    {
        return database;
    }

    @Config("database")
    public InfluxConfig setDatabase(String database)
    {
        this.database = database;
        return this;
    }

    @NotNull
    public String getUserName()
    {
        return userName;
    }

    @Config("user")
    public InfluxConfig setUserName(String userName)
    {
        this.userName = userName;
        return this;
    }

    @NotNull
    public String getPassword()
    {
        return password;
    }

    @Config("password")
    public InfluxConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }
}
