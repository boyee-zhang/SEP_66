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
package io.trino.plugin.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class HiveInputInfo
{
    private final List<String> partitionIds;

    @JsonCreator
    public HiveInputInfo(@JsonProperty("partitionIds") List<String> partitionIds)
    {
        this.partitionIds = partitionIds;
    }

    @JsonProperty
    public List<String> getPartitionIds()
    {
        return partitionIds;
    }
}
