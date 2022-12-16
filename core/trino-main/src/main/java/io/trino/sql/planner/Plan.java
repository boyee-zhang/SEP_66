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
package io.trino.sql.planner;

import io.trino.cost.StatsAndCosts;
import io.trino.sql.planner.plan.PlanNode;

import static java.util.Objects.requireNonNull;

public class Plan
{
    private final PlanNode root;
    private final TypeProvider types;
    private final StatsAndCosts statsAndCosts;
    private final int hashPartitionCount;

    public Plan(PlanNode root, TypeProvider types, StatsAndCosts statsAndCosts, int hashPartitionCount)
    {
        this.root = requireNonNull(root, "root is null");
        this.types = requireNonNull(types, "types is null");
        this.statsAndCosts = requireNonNull(statsAndCosts, "statsAndCosts is null");
        this.hashPartitionCount = hashPartitionCount;
    }

    public PlanNode getRoot()
    {
        return root;
    }

    public TypeProvider getTypes()
    {
        return types;
    }

    public StatsAndCosts getStatsAndCosts()
    {
        return statsAndCosts;
    }

    public int getHashPartitionCount()
    {
        return hashPartitionCount;
    }
}
