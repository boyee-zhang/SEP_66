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
package io.prestosql.server.protocol;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class ExecutingStatementConfig
{
    private Duration maxWaitDefault = new Duration(1, TimeUnit.SECONDS);
    private Duration maxWaitLimit = new Duration(1, TimeUnit.SECONDS);

    @NotNull
    public Duration getMaxWaitDefault()
    {
        return maxWaitDefault;
    }

    @Config("query.max-wait.default")
    @ConfigDescription("Specify maxWait default for executing query endpoint")
    public ExecutingStatementConfig setMaxWaitDefault(Duration maxWaitDefault)
    {
        this.maxWaitDefault = maxWaitDefault;
        return this;
    }

    @NotNull
    public Duration getMaxWaitLimit()
    {
        return maxWaitLimit;
    }

    @Config("query.max-wait.max")
    @ConfigDescription("Specify maxWait limit for executing query endpoint")
    public ExecutingStatementConfig setMaxWaitLimit(Duration maxWaitLimit)
    {
        this.maxWaitLimit = maxWaitLimit;
        return this;
    }
}
