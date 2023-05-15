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
package io.trino.event;

import io.airlift.json.JsonCodec;
import io.trino.eventlistener.EventListenerManager;
import io.trino.execution.TaskId;
import io.trino.operator.DriverStats;
import io.trino.operator.OperatorStats;
import io.trino.operator.SplitOperatorInfo;
import io.trino.spi.eventlistener.SplitCompletedEvent;
import io.trino.spi.eventlistener.SplitFailureInfo;
import io.trino.spi.eventlistener.SplitStatistics;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.time.Duration;
import java.util.Optional;

import static java.lang.Math.toIntExact;
import static java.time.Duration.ofMillis;
import static java.util.Objects.requireNonNull;

public class SplitMonitor
{
    private final JsonCodec<DriverStats> driverStatsJsonCodec;
    private final EventListenerManager eventListenerManager;
    private final int maxJsonLimit;

    @Inject
    public SplitMonitor(EventListenerManager eventListenerManager, JsonCodec<DriverStats> driverStatsJsonCodec,
            SplitMonitorConfig splitMonitorConfig)
    {
        this.eventListenerManager = requireNonNull(eventListenerManager, "eventListenerManager is null");
        this.driverStatsJsonCodec = requireNonNull(driverStatsJsonCodec, "driverStatsJsonCodec is null");
        this.maxJsonLimit = toIntExact(
                requireNonNull(splitMonitorConfig, "splitMonitorConfig is null").getMaxSplitOutputStageJsonSize()
                        .toBytes());
    }

    public void splitCompletedEvent(TaskId taskId, DriverStats driverStats)
    {
        splitCompletedEvent(taskId, driverStats, null, null);
    }

    public void splitFailedEvent(TaskId taskId, DriverStats driverStats, Throwable cause)
    {
        splitCompletedEvent(taskId, driverStats, cause.getClass().getName(), cause.getMessage());
    }

    private void splitCompletedEvent(TaskId taskId, DriverStats driverStats, @Nullable String failureType, @Nullable String failureMessage)
    {
        Duration queuedTime = ofMillis(driverStats.getQueuedTime().toMillis());
        Optional<Duration> queuedTimeIfSplitRan = Optional.empty();
        if (driverStats.getStartTime() != null) {
            queuedTimeIfSplitRan = Optional.of(queuedTime);
        }

        Duration elapsedTime = ofMillis(driverStats.getElapsedTime().toMillis());
        Optional<Duration> elapsedTimeIfSplitRan = Optional.empty();
        if (driverStats.getEndTime() != null) {
            elapsedTimeIfSplitRan = Optional.of(elapsedTime);
        }

        Optional<SplitFailureInfo> splitFailureMetadata = Optional.empty();
        if (failureType != null) {
            splitFailureMetadata = Optional.of(new SplitFailureInfo(failureType, failureMessage != null ? failureMessage : ""));
        }

        Optional<String> splitCatalog = driverStats.getOperatorStats().stream()
                .map(OperatorStats::getInfo)
                .filter(SplitOperatorInfo.class::isInstance)
                .map(SplitOperatorInfo.class::cast)
                .map(info -> info.getCatalogHandle().getCatalogName())
                .findFirst();

        eventListenerManager.splitCompleted(
                new SplitCompletedEvent(
                        taskId.getQueryId().toString(),
                        taskId.getStageId().toString(),
                        taskId.toString(),
                        splitCatalog,
                        driverStats.getCreateTime().toDate().toInstant(),
                        Optional.ofNullable(driverStats.getStartTime()).map(startTime -> startTime.toDate().toInstant()),
                        Optional.ofNullable(driverStats.getEndTime()).map(endTime -> endTime.toDate().toInstant()),
                        new SplitStatistics(
                                ofMillis(driverStats.getTotalCpuTime().toMillis()),
                                elapsedTime,
                                queuedTime,
                                ofMillis(driverStats.getRawInputReadTime().toMillis()),
                                driverStats.getRawInputPositions(),
                                driverStats.getRawInputDataSize().toBytes(),
                                queuedTimeIfSplitRan,
                                elapsedTimeIfSplitRan),
                        splitFailureMetadata,
                        driverStatsJsonCodec.toJsonWithLengthLimit(driverStats, maxJsonLimit)));
    }
}
