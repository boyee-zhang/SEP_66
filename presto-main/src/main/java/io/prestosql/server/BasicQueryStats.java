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
package io.prestosql.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.execution.QueryStats;
import io.prestosql.operator.BlockedReason;
import org.joda.time.DateTime;

import javax.annotation.concurrent.Immutable;

import java.util.OptionalDouble;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Lightweight version of QueryStats. Parts of the web UI depend on the fields
 * being named consistently across these classes.
 */
@Immutable
public class BasicQueryStats
{
    private final DateTime createTime;
    private final DateTime endTime;

    private final Duration queuedTime;
    private final Duration elapsedTime;
    private final Duration executionTime;

    private final int totalDrivers;
    private final int queuedDrivers;
    private final int runningDrivers;
    private final int completedDrivers;

    private final DataSize physicalInputDataSize;
    private final long physicalInputPositions;

    private final DataSize internalNetworkInputDataSize;
    private final long internalNetworkInputPositions;

    private final double cumulativeUserMemory;
    private final DataSize userMemoryReservation;
    private final DataSize totalMemoryReservation;
    private final DataSize peakUserMemoryReservation;
    private final DataSize peakTotalMemoryReservation;
    private final Duration totalCpuTime;
    private final Duration totalScheduledTime;

    private final boolean fullyBlocked;
    private final Set<BlockedReason> blockedReasons;

    private final OptionalDouble progressPercentage;

    @JsonCreator
    public BasicQueryStats(
            @JsonProperty("createTime") DateTime createTime,
            @JsonProperty("endTime") DateTime endTime,
            @JsonProperty("queuedTime") Duration queuedTime,
            @JsonProperty("elapsedTime") Duration elapsedTime,
            @JsonProperty("executionTime") Duration executionTime,
            @JsonProperty("totalDrivers") int totalDrivers,
            @JsonProperty("queuedDrivers") int queuedDrivers,
            @JsonProperty("runningDrivers") int runningDrivers,
            @JsonProperty("completedDrivers") int completedDrivers,
            @JsonProperty("physicalInputDataSize") DataSize physicalInputDataSize,
            @JsonProperty("physicalInputPositions") long physicalInputPositions,
            @JsonProperty("internalNetworkInputDataSize") DataSize internalNetworkInputDataSize,
            @JsonProperty("internalNetworkInputPositions") long internalNetworkInputPositions,
            @JsonProperty("cumulativeUserMemory") double cumulativeUserMemory,
            @JsonProperty("userMemoryReservation") DataSize userMemoryReservation,
            @JsonProperty("totalMemoryReservation") DataSize totalMemoryReservation,
            @JsonProperty("peakUserMemoryReservation") DataSize peakUserMemoryReservation,
            @JsonProperty("peakTotalMemoryReservation") DataSize peakTotalMemoryReservation,
            @JsonProperty("totalCpuTime") Duration totalCpuTime,
            @JsonProperty("totalScheduledTime") Duration totalScheduledTime,
            @JsonProperty("fullyBlocked") boolean fullyBlocked,
            @JsonProperty("blockedReasons") Set<BlockedReason> blockedReasons,
            @JsonProperty("progressPercentage") OptionalDouble progressPercentage)
    {
        this.createTime = createTime;
        this.endTime = endTime;

        this.queuedTime = requireNonNull(queuedTime, "queuedTime is null");
        this.elapsedTime = requireNonNull(elapsedTime, "elapsedTime is null");
        this.executionTime = requireNonNull(executionTime, "executionTime is null");

        checkArgument(totalDrivers >= 0, "totalDrivers is negative");
        this.totalDrivers = totalDrivers;
        checkArgument(queuedDrivers >= 0, "queuedDrivers is negative");
        this.queuedDrivers = queuedDrivers;
        checkArgument(runningDrivers >= 0, "runningDrivers is negative");
        this.runningDrivers = runningDrivers;
        checkArgument(completedDrivers >= 0, "completedDrivers is negative");
        this.completedDrivers = completedDrivers;

        this.physicalInputDataSize = physicalInputDataSize;
        this.physicalInputPositions = physicalInputPositions;

        this.internalNetworkInputDataSize = internalNetworkInputDataSize;
        this.internalNetworkInputPositions = internalNetworkInputPositions;

        this.cumulativeUserMemory = cumulativeUserMemory;
        this.userMemoryReservation = userMemoryReservation;
        this.totalMemoryReservation = totalMemoryReservation;
        this.peakUserMemoryReservation = peakUserMemoryReservation;
        this.peakTotalMemoryReservation = peakTotalMemoryReservation;
        this.totalCpuTime = totalCpuTime;
        this.totalScheduledTime = totalScheduledTime;

        this.fullyBlocked = fullyBlocked;
        this.blockedReasons = ImmutableSet.copyOf(requireNonNull(blockedReasons, "blockedReasons is null"));

        this.progressPercentage = requireNonNull(progressPercentage, "progressPercentage is null");
    }

    public BasicQueryStats(QueryStats queryStats)
    {
        this(queryStats.getCreateTime(),
                queryStats.getEndTime(),
                queryStats.getQueuedTime(),
                queryStats.getElapsedTime(),
                queryStats.getExecutionTime(),
                queryStats.getTotalDrivers(),
                queryStats.getQueuedDrivers(),
                queryStats.getRunningDrivers(),
                queryStats.getCompletedDrivers(),
                queryStats.getPhysicalInputDataSize(),
                queryStats.getPhysicalInputPositions(),
                queryStats.getInternalNetworkInputDataSize(),
                queryStats.getInternalNetworkInputPositions(),
                queryStats.getCumulativeUserMemory(),
                queryStats.getUserMemoryReservation(),
                queryStats.getTotalMemoryReservation(),
                queryStats.getPeakUserMemoryReservation(),
                queryStats.getPeakTotalMemoryReservation(),
                queryStats.getTotalCpuTime(),
                queryStats.getTotalScheduledTime(),
                queryStats.isFullyBlocked(),
                queryStats.getBlockedReasons(),
                queryStats.getProgressPercentage());
    }

    public static BasicQueryStats immediateFailureQueryStats()
    {
        DateTime now = DateTime.now();
        return new BasicQueryStats(
                now,
                now,
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                0,
                0,
                0,
                0,
                DataSize.ofBytes(0),
                0,
                DataSize.ofBytes(0),
                0,
                0,
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                false,
                ImmutableSet.of(),
                OptionalDouble.empty());
    }

    @JsonProperty
    public DateTime getCreateTime()
    {
        return createTime;
    }

    @JsonProperty
    public DateTime getEndTime()
    {
        return endTime;
    }

    @JsonProperty
    public Duration getQueuedTime()
    {
        return queuedTime;
    }

    @JsonProperty
    public Duration getElapsedTime()
    {
        return elapsedTime;
    }

    @JsonProperty
    public Duration getExecutionTime()
    {
        return executionTime;
    }

    @JsonProperty
    public int getTotalDrivers()
    {
        return totalDrivers;
    }

    @JsonProperty
    public int getQueuedDrivers()
    {
        return queuedDrivers;
    }

    @JsonProperty
    public int getRunningDrivers()
    {
        return runningDrivers;
    }

    @JsonProperty
    public int getCompletedDrivers()
    {
        return completedDrivers;
    }

    @JsonProperty
    public long getPhysicalInputPositions()
    {
        return physicalInputPositions;
    }

    @JsonProperty
    public DataSize getPhysicalInputDataSize()
    {
        return physicalInputDataSize;
    }

    @JsonProperty
    public DataSize getInternalNetworkInputDataSize()
    {
        return internalNetworkInputDataSize;
    }

    @JsonProperty
    public long getInternalNetworkInputPositions()
    {
        return internalNetworkInputPositions;
    }

    @JsonProperty
    public double getCumulativeUserMemory()
    {
        return cumulativeUserMemory;
    }

    @JsonProperty
    public DataSize getUserMemoryReservation()
    {
        return userMemoryReservation;
    }

    @JsonProperty
    public DataSize getTotalMemoryReservation()
    {
        return totalMemoryReservation;
    }

    @JsonProperty
    public DataSize getPeakUserMemoryReservation()
    {
        return peakUserMemoryReservation;
    }

    @JsonProperty
    public DataSize getPeakTotalMemoryReservation()
    {
        return peakTotalMemoryReservation;
    }

    @JsonProperty
    public Duration getTotalCpuTime()
    {
        return totalCpuTime;
    }

    @JsonProperty
    public Duration getTotalScheduledTime()
    {
        return totalScheduledTime;
    }

    @JsonProperty
    public boolean isFullyBlocked()
    {
        return fullyBlocked;
    }

    @JsonProperty
    public Set<BlockedReason> getBlockedReasons()
    {
        return blockedReasons;
    }

    @JsonProperty
    public OptionalDouble getProgressPercentage()
    {
        return progressPercentage;
    }
}
