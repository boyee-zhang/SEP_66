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
package io.trino.operator.join.unspilled;

import com.google.common.io.Closer;
import io.trino.operator.InterpretedHashGenerator;
import io.trino.operator.exchange.LocalPartitionGenerator;
import io.trino.operator.join.LookupSource;
import io.trino.operator.join.OuterPositionIterator;
import io.trino.operator.join.TrackingLookupSourceSupplier;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Integer.numberOfTrailingZeros;
import static java.lang.Math.toIntExact;

@NotThreadSafe
public class PartitionedLookupSource
        implements LookupSource
{
    public static TrackingLookupSourceSupplier createPartitionedLookupSourceSupplier(List<Supplier<LookupSource>> partitions, List<Type> hashChannelTypes, boolean outer, BlockTypeOperators blockTypeOperators)
    {
        if (outer) {
            OuterPositionTracker.Factory outerPositionTrackerFactory = new OuterPositionTracker.Factory(partitions);

            return new TrackingLookupSourceSupplier()
            {
                @Override
                public LookupSource getLookupSource()
                {
                    return new PartitionedLookupSource(
                            partitions.stream()
                                    .map(Supplier::get)
                                    .collect(toImmutableList()),
                            hashChannelTypes,
                            Optional.of(outerPositionTrackerFactory.create()),
                            blockTypeOperators);
                }

                @Override
                public OuterPositionIterator getOuterPositionIterator()
                {
                    return outerPositionTrackerFactory.getOuterPositionIterator();
                }
            };
        }
        return TrackingLookupSourceSupplier.nonTracking(
                () -> new PartitionedLookupSource(
                        partitions.stream()
                                .map(Supplier::get)
                                .collect(toImmutableList()),
                        hashChannelTypes,
                        Optional.empty(),
                        blockTypeOperators));
    }

    private final LookupSource[] lookupSources;
    private final LocalPartitionGenerator partitionGenerator;
    private final int partitionMask;
    private final int shiftSize;
    @Nullable
    private final OuterPositionTracker outerPositionTracker;
    private final boolean uniqueMapping;

    private boolean closed;

    private PartitionedLookupSource(List<? extends LookupSource> lookupSources, List<Type> hashChannelTypes, Optional<OuterPositionTracker> outerPositionTracker, BlockTypeOperators blockTypeOperators)
    {
        this.lookupSources = lookupSources.toArray(new LookupSource[lookupSources.size()]);

        // this generator is only used for getJoinPosition without a rawHash and in this case
        // the hash channels are always packed in a page without extra columns
        this.partitionGenerator = new LocalPartitionGenerator(InterpretedHashGenerator.createPositionalWithTypes(hashChannelTypes, blockTypeOperators), lookupSources.size());

        this.partitionMask = lookupSources.size() - 1;
        this.shiftSize = numberOfTrailingZeros(lookupSources.size()) + 1;
        this.outerPositionTracker = outerPositionTracker.orElse(null);

        uniqueMapping = lookupSources.stream()
                .allMatch(lookupSource -> lookupSource.isMappingUnique());
    }

    @Override
    public boolean isEmpty()
    {
        return Arrays.stream(lookupSources).allMatch(LookupSource::isEmpty);
    }

    @Override
    public long getJoinPositionCount()
    {
        return Arrays.stream(lookupSources)
                .mapToLong(LookupSource::getJoinPositionCount)
                .sum();
    }

    @Override
    public long getInMemorySizeInBytes()
    {
        return Arrays.stream(lookupSources).mapToLong(LookupSource::getInMemorySizeInBytes).sum();
    }

    @Override
    public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage)
    {
        return getJoinPosition(position, hashChannelsPage, allChannelsPage, partitionGenerator.getRawHash(hashChannelsPage, position));
    }

    @Override
    public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage, long rawHash)
    {
        int partition = partitionGenerator.getPartition(rawHash);
        LookupSource lookupSource = lookupSources[partition];
        long joinPosition = lookupSource.getJoinPosition(position, hashChannelsPage, allChannelsPage, rawHash);
        if (joinPosition < 0) {
            return joinPosition;
        }
        return encodePartitionedJoinPosition(partition, toIntExact(joinPosition));
    }

    @Override
    public void getJoinPosition(int[] positions, Page hashChannelsPage, Page allChannelsPage, long[] rawHashes, long[] result)
    {
        int positionCount = positions.length;
        int partitionCount = partitionGenerator.getPartitionCount();

        int[] partitions = new int[positionCount];
        int[] partitionPositionsCount = new int[partitionCount];

        // Get the partitions for every position and calculate the size of every partition
        for (int i = 0; i < positionCount; i++) {
            int partition = partitionGenerator.getPartition(rawHashes[positions[i]]);
            partitions[i] = partition;
            partitionPositionsCount[partition]++;
        }

        int[][] positionsPerPartition = new int[partitionCount][];
        for (int partition = 0; partition < partitionCount; partition++) {
            positionsPerPartition[partition] = new int[partitionPositionsCount[partition]];
        }

        // Split input positions into partitions
        int[] positionsPerPartitionCount = new int[partitionCount];
        for (int i = 0; i < positionCount; i++) {
            int partition = partitions[i];
            positionsPerPartition[partition][positionsPerPartitionCount[partition]] = positions[i];
            positionsPerPartitionCount[partition]++;
        }

        // Delegate partitioned positions to designated lookup sources
        for (int partition = 0; partition < partitionCount; partition++) {
            lookupSources[partition].getJoinPosition(positionsPerPartition[partition], hashChannelsPage, allChannelsPage, rawHashes, result);
        }

        for (int i = 0; i < positionCount; i++) {
            int partition = partitions[i];
            result[positions[i]] = encodePartitionedJoinPosition(partition, (int) result[positions[i]]);
        }
    }

    @Override
    public void getJoinPosition(int[] positions, Page hashChannelsPage, Page allChannelsPage, long[] result)
    {
        int positionCount = positions.length;
        long[] rawHashes = new long[result.length];
        for (int i = 0; i < positionCount; i++) {
            rawHashes[positions[i]] = partitionGenerator.getRawHash(hashChannelsPage, positions[i]);
        }

        getJoinPosition(positions, hashChannelsPage, allChannelsPage, rawHashes, result);
    }

    @Override
    public long getNextJoinPosition(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
    {
        int partition = decodePartition(currentJoinPosition);
        long joinPosition = decodeJoinPosition(currentJoinPosition);
        LookupSource lookupSource = lookupSources[partition];
        long nextJoinPosition = lookupSource.getNextJoinPosition(joinPosition, probePosition, allProbeChannelsPage);
        if (nextJoinPosition < 0) {
            return nextJoinPosition;
        }
        return encodePartitionedJoinPosition(partition, toIntExact(nextJoinPosition));
    }

    @Override
    public boolean isJoinPositionEligible(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
    {
        int partition = decodePartition(currentJoinPosition);
        long joinPosition = decodeJoinPosition(currentJoinPosition);
        LookupSource lookupSource = lookupSources[partition];
        return lookupSource.isJoinPositionEligible(joinPosition, probePosition, allProbeChannelsPage);
    }

    @Override
    public void appendTo(long partitionedJoinPosition, PageBuilder pageBuilder, int outputChannelOffset)
    {
        int partition = decodePartition(partitionedJoinPosition);
        int joinPosition = decodeJoinPosition(partitionedJoinPosition);
        lookupSources[partition].appendTo(joinPosition, pageBuilder, outputChannelOffset);
        if (outerPositionTracker != null) {
            outerPositionTracker.positionVisited(partition, joinPosition);
        }
    }

    @Override
    public long joinPositionWithinPartition(long joinPosition)
    {
        return decodeJoinPosition(joinPosition);
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }

        try (Closer closer = Closer.create()) {
            if (outerPositionTracker != null) {
                closer.register(outerPositionTracker::commit);
            }
            Arrays.stream(lookupSources).forEach(closer::register);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        closed = true;
    }

    private int decodePartition(long partitionedJoinPosition)
    {
        //noinspection NumericCastThatLosesPrecision - partitionMask is an int
        return (int) (partitionedJoinPosition & partitionMask);
    }

    private int decodeJoinPosition(long partitionedJoinPosition)
    {
        return toIntExact(partitionedJoinPosition >>> shiftSize);
    }

    private long encodePartitionedJoinPosition(int partition, int joinPosition)
    {
        return (((long) joinPosition) << shiftSize) | (partition);
    }

    private static class PartitionedLookupOuterPositionIterator
            implements OuterPositionIterator
    {
        private final LookupSource[] lookupSources;
        private final boolean[][] visitedPositions;

        @GuardedBy("this")
        private int currentSource;

        @GuardedBy("this")
        private int currentPosition;

        public PartitionedLookupOuterPositionIterator(LookupSource[] lookupSources, boolean[][] visitedPositions)
        {
            this.lookupSources = lookupSources;
            this.visitedPositions = visitedPositions;
        }

        @Override
        public synchronized boolean appendToNext(PageBuilder pageBuilder, int outputChannelOffset)
        {
            while (currentSource < lookupSources.length) {
                while (currentPosition < visitedPositions[currentSource].length) {
                    if (!visitedPositions[currentSource][currentPosition]) {
                        lookupSources[currentSource].appendTo(currentPosition, pageBuilder, outputChannelOffset);
                        currentPosition++;
                        return true;
                    }
                    currentPosition++;
                }
                currentPosition = 0;
                currentSource++;
            }
            return false;
        }
    }

    /**
     * Each LookupSource has it's own copy of OuterPositionTracker instance.
     * Each of those OuterPositionTracker must be committed after last write
     * and before first read.
     * <p>
     * All instances share visitedPositions array, but it is safe because each thread
     * starts with visitedPositions filled with false values and marks only some positions
     * to true. Since we don't care what will be the order of those writes to
     * visitedPositions, writes can be without synchronization.
     * <p>
     * Memory visibility between last writes in commit() and first read in
     * getVisitedPositions() is guaranteed by accessing AtomicLong referenceCount
     * variables in those two methods.
     */
    private static class OuterPositionTracker
    {
        public static class Factory
        {
            private final LookupSource[] lookupSources;
            private final boolean[][] visitedPositions;
            private final AtomicBoolean finished = new AtomicBoolean();
            private final AtomicLong referenceCount = new AtomicLong();

            public Factory(List<Supplier<LookupSource>> partitions)
            {
                this.lookupSources = partitions.stream()
                        .map(Supplier::get)
                        .toArray(LookupSource[]::new);

                visitedPositions = Arrays.stream(this.lookupSources)
                        .map(LookupSource::getJoinPositionCount)
                        .map(Math::toIntExact)
                        .map(boolean[]::new)
                        .toArray(boolean[][]::new);
            }

            public OuterPositionTracker create()
            {
                return new OuterPositionTracker(visitedPositions, finished, referenceCount);
            }

            public OuterPositionIterator getOuterPositionIterator()
            {
                // touching atomic values ensures memory visibility between commit and getVisitedPositions
                verify(referenceCount.get() == 0);
                finished.set(true);
                return new PartitionedLookupOuterPositionIterator(lookupSources, visitedPositions);
            }
        }

        private final boolean[][] visitedPositions; // shared across multiple operators/drivers
        private final AtomicBoolean finished; // shared across multiple operators/drivers
        private final AtomicLong referenceCount; // shared across multiple operators/drivers
        private boolean written; // unique per each operator/driver

        private OuterPositionTracker(boolean[][] visitedPositions, AtomicBoolean finished, AtomicLong referenceCount)
        {
            this.visitedPositions = visitedPositions;
            this.finished = finished;
            this.referenceCount = referenceCount;
        }

        /**
         * No synchronization here, because it would be very expensive. Check comment above.
         */
        public void positionVisited(int partition, int position)
        {
            if (!written) {
                written = true;
                verify(!finished.get());
                referenceCount.incrementAndGet();
            }
            visitedPositions[partition][position] = true;
        }

        public void commit()
        {
            if (written) {
                // touching atomic values ensures memory visibility between commit and getVisitedPositions
                referenceCount.decrementAndGet();
            }
        }
    }

    @Override
    public boolean isMappingUnique()
    {
        return uniqueMapping;
    }
}
