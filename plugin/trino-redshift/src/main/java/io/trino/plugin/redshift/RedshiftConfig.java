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
package io.trino.plugin.redshift;

import io.airlift.configuration.Config;

public class RedshiftConfig
{
    private boolean legacyTypeMapping;
    private int jdbcFetchSize = 1000;

    public boolean isLegacyTypeMapping()
    {
        return legacyTypeMapping;
    }

    @Config("redshift.use-legacy-type-mapping")
    public RedshiftConfig setLegacyTypeMapping(boolean legacyTypeMapping)
    {
        this.legacyTypeMapping = legacyTypeMapping;
        return this;
    }

    public int getJdbcFetchSize()
    {
        return jdbcFetchSize;
    }

    @Config("redshift.jdbc-fetch-size")
    public RedshiftConfig setJdbcFetchSize(int jdbcFetchSize)
    {
        this.jdbcFetchSize = jdbcFetchSize;
        return this;
    }
}
