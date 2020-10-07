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
package io.prestosql.plugin.hive.metastore;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

public class MetastoreConfig
{
    private boolean hideDeltaLakeTables;

    public boolean isHideDeltaLakeTables()
    {
        return hideDeltaLakeTables;
    }

    @Config("hive.hide-delta-lake-tables")
    @ConfigDescription("Hide Delta Lake tables in table listings")
    public MetastoreConfig setHideDeltaLakeTables(boolean hideDeltaLakeTables)
    {
        this.hideDeltaLakeTables = hideDeltaLakeTables;
        return this;
    }
}
