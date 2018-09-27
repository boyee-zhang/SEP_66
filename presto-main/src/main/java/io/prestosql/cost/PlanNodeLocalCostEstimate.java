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
package io.prestosql.cost;

import io.prestosql.sql.planner.plan.PlanNode;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.Double.NaN;

public class PlanNodeLocalCostEstimate
{
    private final double cpuCost;
    private final double maxMemory;
    private final double networkCost;

    public static PlanNodeLocalCostEstimate unknown()
    {
        return of(NaN, NaN, NaN);
    }

    public static PlanNodeLocalCostEstimate zero()
    {
        return of(0, 0, 0);
    }

    public static PlanNodeLocalCostEstimate ofCpu(double cpuCost)
    {
        return of(cpuCost, 0, 0);
    }

    public static PlanNodeLocalCostEstimate ofNetwork(double networkCost)
    {
        return of(0, 0, networkCost);
    }

    public static PlanNodeLocalCostEstimate of(double cpuCost, double maxMemory, double networkCost)
    {
        return new PlanNodeLocalCostEstimate(cpuCost, maxMemory, networkCost);
    }

    private PlanNodeLocalCostEstimate(double cpuCost, double maxMemory, double networkCost)
    {
        this.cpuCost = cpuCost;
        this.maxMemory = maxMemory;
        this.networkCost = networkCost;
    }

    public double getCpuCost()
    {
        return cpuCost;
    }

    public double getMaxMemory()
    {
        return maxMemory;
    }

    public double getNetworkCost()
    {
        return networkCost;
    }

    public PlanNodeLocalCostEstimate add(PlanNodeLocalCostEstimate other)
    {
        return new PlanNodeLocalCostEstimate(
                other.cpuCost + this.cpuCost,
                other.maxMemory + this.maxMemory,
                other.networkCost + this.networkCost);
    }

    public PlanCostEstimate add(PlanCostEstimate costEstimate)
    {
        return new PlanCostEstimate(
                costEstimate.getCpuCost() + this.cpuCost,
                costEstimate.getMaxMemory() + this.maxMemory,
                costEstimate.getMaxMemoryWhenOutputting() + this.maxMemory,
                costEstimate.getNetworkCost() + this.networkCost);
    }

    /**
     * @deprecated This class represents individual cost of a part of a plan (usually of a single {@link PlanNode}). Use {@link CostProvider} instead.
     */
    @Deprecated
    public PlanCostEstimate toPlanCost()
    {
        return new PlanCostEstimate(cpuCost, maxMemory, maxMemory, networkCost);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("cpuCost", cpuCost)
                .add("maxMemory", maxMemory)
                .add("networkCost", networkCost)
                .toString();
    }
}
