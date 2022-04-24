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
package io.trino.testing;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public final class FaultTolerantExecutionConnectorTestHelper
{
    private FaultTolerantExecutionConnectorTestHelper() {}

    public static Map<String, String> getExtraProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("retry-policy", "TASK")
                .put("query.hash-partition-count", "5")
                .put("fault-tolerant-execution-target-task-input-size", "10MB")
                .put("fault-tolerant-execution-target-task-split-count", "4")
                // to trigger spilling
                .put("exchange.deduplication-buffer-size", "1kB")
                .put("fault-tolerant-execution-task-memory", "1GB")
                .buildOrThrow();
    }
}
