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
package io.trino.operator.exchange;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.slice.XxHash64;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.operator.BucketPartitionFunction;
import io.trino.operator.HashGenerator;
import io.trino.operator.InterpretedHashGenerator;
import io.trino.operator.PartitionFunction;
import io.trino.operator.PrecomputedHashGenerator;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.sql.planner.MergePartitioningHandle;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.SystemPartitioningHandle;
import io.trino.type.BlockTypeOperators;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.operator.exchange.LocalExchangeSink.finishedLocalExchangeSink;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

@ThreadSafe
public class LocalExchange
{
    private static final int SCALE_WRITERS_MAX_PARTITIONS_PER_WRITER = 128;

    private final Supplier<LocalExchanger> exchangerSupplier;

    private final List<LocalExchangeSource> sources;

    // Physical written bytes for each writer in the same order as source buffers
    private final List<Supplier<Long>> physicalWrittenBytesSuppliers = new CopyOnWriteArrayList<>();

    @GuardedBy("this")
    private boolean allSourcesFinished;

    @GuardedBy("this")
    private boolean noMoreSinkFactories;

    @GuardedBy("this")
    private final Set<LocalExchangeSinkFactory> openSinkFactories = new HashSet<>();

    @GuardedBy("this")
    private final Set<LocalExchangeSink> sinks = new HashSet<>();

    @GuardedBy("this")
    private int nextSourceIndex;

    public LocalExchange(
            NodePartitioningManager nodePartitioningManager,
            Session session,
            int defaultConcurrency,
            PartitioningHandle partitioning,
            boolean scaleWriters,
            boolean passThrough,
            List<Integer> partitionChannels,
            List<Type> partitionChannelTypes,
            Optional<Integer> partitionHashChannel,
            DataSize maxBufferedBytes,
            BlockTypeOperators blockTypeOperators,
            DataSize writerMinSize)
    {
        int bufferCount = computeBufferCount(partitioning, passThrough, defaultConcurrency, partitionChannels);

        if (scaleWriters) {
            if (partitioning.equals(FIXED_ARBITRARY_DISTRIBUTION)) {
                LocalExchangeMemoryManager memoryManager = new LocalExchangeMemoryManager(maxBufferedBytes.toBytes());
                sources = IntStream.range(0, bufferCount)
                        .mapToObj(i -> new LocalExchangeSource(memoryManager, source -> checkAllSourcesFinished()))
                        .collect(toImmutableList());
                exchangerSupplier = () -> new ScaleWriterExchanger(
                        asPageConsumers(sources),
                        memoryManager,
                        maxBufferedBytes.toBytes(),
                        () -> {
                            // Avoid using stream api for performance reasons
                            long physicalWrittenBytes = 0;
                            for (Supplier<Long> physicalWrittenBytesSupplier : physicalWrittenBytesSuppliers) {
                                physicalWrittenBytes += physicalWrittenBytesSupplier.get();
                            }
                            return physicalWrittenBytes;
                        },
                        writerMinSize);
            }
            else if (partitioning.equals(FIXED_HASH_DISTRIBUTION) || partitioning.getCatalogHandle().isPresent()) {
                int partitionCount = bufferCount * SCALE_WRITERS_MAX_PARTITIONS_PER_WRITER;
                List<Supplier<Long2LongMap>> writerPartitionRowCountsSuppliers = new CopyOnWriteArrayList<>();
                UniformPartitionRebalancer uniformPartitionRebalancer = new UniformPartitionRebalancer(
                        physicalWrittenBytesSuppliers,
                        () -> computeAggregatedPartitionRowCounts(writerPartitionRowCountsSuppliers),
                        partitionCount,
                        bufferCount,
                        writerMinSize.toBytes());

                LocalExchangeMemoryManager memoryManager = new LocalExchangeMemoryManager(maxBufferedBytes.toBytes());
                sources = IntStream.range(0, bufferCount)
                        .mapToObj(i -> new LocalExchangeSource(memoryManager, source -> checkAllSourcesFinished()))
                        .collect(toImmutableList());

                exchangerSupplier = () -> {
                    PartitionFunction partitionFunction = createPartitionFunction(
                            nodePartitioningManager,
                            session,
                            blockTypeOperators,
                            partitioning,
                            partitionCount,
                            partitionChannels,
                            partitionChannelTypes,
                            partitionHashChannel);
                    ScaleWriterPartitioningExchanger exchanger = new ScaleWriterPartitioningExchanger(
                            asPageConsumers(sources),
                            memoryManager,
                            maxBufferedBytes.toBytes(),
                            createPartitionPagePreparer(partitioning, partitionChannels),
                            partitionFunction,
                            partitionCount,
                            uniformPartitionRebalancer);
                    writerPartitionRowCountsSuppliers.add(exchanger::getAndResetPartitionRowCounts);
                    return exchanger;
                };
            }
            else {
                throw new IllegalArgumentException("Unexpected partitioning handle: " + partitioning);
            }
        }
        else if (passThrough) {
            List<LocalExchangeMemoryManager> memoryManagers = IntStream.range(0, bufferCount)
                    .mapToObj(i -> new LocalExchangeMemoryManager(maxBufferedBytes.toBytes() / bufferCount))
                    .collect(toImmutableList());
            sources = memoryManagers.stream()
                    .map(memoryManager -> new LocalExchangeSource(memoryManager, source -> checkAllSourcesFinished()))
                    .collect(toImmutableList());
            AtomicInteger nextSource = new AtomicInteger();
            exchangerSupplier = () -> {
                int currentSource = nextSource.getAndIncrement();
                checkState(currentSource < sources.size(), "no more sources");
                return new PassthroughExchanger(sources.get(currentSource), memoryManagers.get(currentSource));
            };
        }
        else if (partitioning.equals(SINGLE_DISTRIBUTION) || partitioning.equals(FIXED_ARBITRARY_DISTRIBUTION)) {
            LocalExchangeMemoryManager memoryManager = new LocalExchangeMemoryManager(maxBufferedBytes.toBytes());
            sources = IntStream.range(0, bufferCount)
                    .mapToObj(i -> new LocalExchangeSource(memoryManager, source -> checkAllSourcesFinished()))
                    .collect(toImmutableList());
            exchangerSupplier = () -> new RandomExchanger(asPageConsumers(sources), memoryManager);
        }
        else if (partitioning.equals(FIXED_HASH_DISTRIBUTION) || partitioning.getCatalogHandle().isPresent() ||
                (partitioning.getConnectorHandle() instanceof MergePartitioningHandle)) {
            LocalExchangeMemoryManager memoryManager = new LocalExchangeMemoryManager(maxBufferedBytes.toBytes());
            sources = IntStream.range(0, bufferCount)
                    .mapToObj(i -> new LocalExchangeSource(memoryManager, source -> checkAllSourcesFinished()))
                    .collect(toImmutableList());
            exchangerSupplier = () -> {
                PartitionFunction partitionFunction = createPartitionFunction(
                        nodePartitioningManager,
                        session,
                        blockTypeOperators,
                        partitioning,
                        bufferCount,
                        partitionChannels,
                        partitionChannelTypes,
                        partitionHashChannel);
                return new PartitioningExchanger(
                        asPageConsumers(sources),
                        memoryManager,
                        createPartitionPagePreparer(partitioning, partitionChannels),
                        partitionFunction);
            };
        }
        else {
            throw new IllegalArgumentException("Unsupported local exchange partitioning " + partitioning);
        }
    }

    public int getBufferCount()
    {
        return sources.size();
    }

    public synchronized LocalExchangeSinkFactory createSinkFactory()
    {
        checkState(!noMoreSinkFactories, "No more sink factories already set");
        LocalExchangeSinkFactory newFactory = new LocalExchangeSinkFactory(this);
        openSinkFactories.add(newFactory);
        return newFactory;
    }

    public synchronized LocalExchangeSource getNextSource(Supplier<Long> physicalWrittenBytesSupplier)
    {
        checkState(nextSourceIndex < sources.size(), "All operators already created");
        LocalExchangeSource result = sources.get(nextSourceIndex);
        physicalWrittenBytesSuppliers.add(physicalWrittenBytesSupplier);
        nextSourceIndex++;
        return result;
    }

    private Long2LongMap computeAggregatedPartitionRowCounts(List<Supplier<Long2LongMap>> writerPartitionRowCountsSuppliers)
    {
        Long2LongMap aggregatedPartitionRowCounts = new Long2LongOpenHashMap();
        List<Long2LongMap> writerPartitionRowCounts = writerPartitionRowCountsSuppliers.stream()
                .map(Supplier::get)
                .collect(toImmutableList());

        writerPartitionRowCounts.forEach(partitionRowCounts ->
                partitionRowCounts.forEach((writerPartitionId, rowCount) ->
                        aggregatedPartitionRowCounts.merge(writerPartitionId.longValue(), rowCount.longValue(), Long::sum)));

        return aggregatedPartitionRowCounts;
    }

    private static Function<Page, Page> createPartitionPagePreparer(PartitioningHandle partitioning, List<Integer> partitionChannels)
    {
        Function<Page, Page> partitionPagePreparer;
        if (partitioning.getConnectorHandle() instanceof SystemPartitioningHandle) {
            partitionPagePreparer = identity();
        }
        else {
            int[] partitionChannelsArray = Ints.toArray(partitionChannels);
            partitionPagePreparer = page -> page.getColumns(partitionChannelsArray);
        }
        return partitionPagePreparer;
    }

    private static PartitionFunction createPartitionFunction(
            NodePartitioningManager nodePartitioningManager,
            Session session,
            BlockTypeOperators blockTypeOperators,
            PartitioningHandle partitioning,
            int partitionCount,
            List<Integer> partitionChannels,
            List<Type> partitionChannelTypes,
            Optional<Integer> partitionHashChannel)
    {
        checkArgument(Integer.bitCount(partitionCount) == 1, "partitionCount must be a power of 2");

        if (isSystemPartitioning(partitioning)) {
            HashGenerator hashGenerator;
            if (partitionHashChannel.isPresent()) {
                hashGenerator = new PrecomputedHashGenerator(partitionHashChannel.get());
            }
            else {
                hashGenerator = new InterpretedHashGenerator(partitionChannelTypes, Ints.toArray(partitionChannels), blockTypeOperators);
            }
            return new LocalPartitionGenerator(hashGenerator, partitionCount);
        }

        // Distribute buckets assigned to this node among threads.
        // The same bucket function (with the same bucket count) as for node
        // partitioning must be used. This way rows within a single bucket
        // will be being processed by single thread.
        int bucketCount = getBucketCount(session, nodePartitioningManager, partitioning);
        int[] bucketToPartition = new int[bucketCount];

        for (int bucket = 0; bucket < bucketCount; bucket++) {
            // mix the bucket bits so we don't use the same bucket number used to distribute between stages
            int hashedBucket = (int) XxHash64.hash(Long.reverse(bucket));
            bucketToPartition[bucket] = hashedBucket & (partitionCount - 1);
        }

        if (partitioning.getConnectorHandle() instanceof MergePartitioningHandle handle) {
            return handle.getPartitionFunction(
                    (scheme, types) -> nodePartitioningManager.getPartitionFunction(session, scheme, types, bucketToPartition),
                    partitionChannelTypes,
                    bucketToPartition);
        }

        return new BucketPartitionFunction(
                nodePartitioningManager.getBucketFunction(session, partitioning, partitionChannelTypes, bucketCount),
                bucketToPartition);
    }

    public static int getBucketCount(Session session, NodePartitioningManager nodePartitioningManager, PartitioningHandle partitioning)
    {
        if (partitioning.getConnectorHandle() instanceof MergePartitioningHandle) {
            // TODO: can we always use this code path?
            return nodePartitioningManager.getNodePartitioningMap(session, partitioning).getBucketToPartition().length;
        }
        return nodePartitioningManager.getBucketNodeMap(session, partitioning).getBucketCount();
    }

    private static boolean isSystemPartitioning(PartitioningHandle partitioning)
    {
        return partitioning.getConnectorHandle() instanceof SystemPartitioningHandle;
    }

    private void checkAllSourcesFinished()
    {
        checkNotHoldsLock(this);

        if (!sources.stream().allMatch(LocalExchangeSource::isFinished)) {
            return;
        }

        // all sources are finished, so finish the sinks
        ImmutableList<LocalExchangeSink> openSinks;
        synchronized (this) {
            allSourcesFinished = true;

            openSinks = ImmutableList.copyOf(sinks);
            sinks.clear();
        }

        // since all sources are finished there is no reason to allow new pages to be added
        // this can happen with a limit query
        openSinks.forEach(LocalExchangeSink::finish);
        checkAllSinksComplete();
    }

    private LocalExchangeSink createSink(LocalExchangeSinkFactory factory)
    {
        checkNotHoldsLock(this);

        synchronized (this) {
            checkState(openSinkFactories.contains(factory), "Factory is already closed");

            if (allSourcesFinished) {
                // all sources have completed so return a sink that is already finished
                return finishedLocalExchangeSink();
            }

            // Note: exchanger can be stateful so create a new one for each sink
            LocalExchanger exchanger = exchangerSupplier.get();
            LocalExchangeSink sink = new LocalExchangeSink(exchanger, this::sinkFinished);
            sinks.add(sink);
            return sink;
        }
    }

    private void sinkFinished(LocalExchangeSink sink)
    {
        checkNotHoldsLock(this);

        synchronized (this) {
            sinks.remove(sink);
        }
        checkAllSinksComplete();
    }

    private void noMoreSinkFactories()
    {
        checkNotHoldsLock(this);

        synchronized (this) {
            noMoreSinkFactories = true;
        }
        checkAllSinksComplete();
    }

    private void sinkFactoryClosed(LocalExchangeSinkFactory sinkFactory)
    {
        checkNotHoldsLock(this);

        synchronized (this) {
            openSinkFactories.remove(sinkFactory);
        }
        checkAllSinksComplete();
    }

    private void checkAllSinksComplete()
    {
        checkNotHoldsLock(this);

        synchronized (this) {
            if (!noMoreSinkFactories || !openSinkFactories.isEmpty() || !sinks.isEmpty()) {
                return;
            }
        }

        sources.forEach(LocalExchangeSource::finish);
    }

    @VisibleForTesting
    LocalExchangeSource getSource(int partitionIndex)
    {
        return sources.get(partitionIndex);
    }

    private static void checkNotHoldsLock(Object lock)
    {
        checkState(!Thread.holdsLock(lock), "Cannot execute this method while holding a lock");
    }

    private static int computeBufferCount(PartitioningHandle partitioning, boolean passThrough, int defaultConcurrency, List<Integer> partitionChannels)
    {
        int bufferCount;
        if (passThrough) {
            bufferCount = defaultConcurrency;
            checkArgument(partitionChannels.isEmpty(), "Passthrough exchange must not have partition channels");
        }
        else if (partitioning.equals(SINGLE_DISTRIBUTION)) {
            bufferCount = 1;
            checkArgument(partitionChannels.isEmpty(), "Gather exchange must not have partition channels");
        }
        else if (partitioning.equals(FIXED_ARBITRARY_DISTRIBUTION)) {
            bufferCount = defaultConcurrency;
            checkArgument(partitionChannels.isEmpty(), "Arbitrary exchange must not have partition channels");
        }
        else if (partitioning.equals(FIXED_HASH_DISTRIBUTION) || partitioning.getCatalogHandle().isPresent() ||
                (partitioning.getConnectorHandle() instanceof MergePartitioningHandle)) {
            // partitioned exchange
            bufferCount = defaultConcurrency;
        }
        else {
            throw new IllegalArgumentException("Unsupported local exchange partitioning " + partitioning);
        }
        return bufferCount;
    }

    private static List<Consumer<Page>> asPageConsumers(List<LocalExchangeSource> sources)
    {
        return sources.stream()
                .map(buffer -> (Consumer<Page>) buffer::addPage)
                .collect(toImmutableList());
    }

    // Sink factory is entirely a pass thought to LocalExchange.
    // This class only exists as a separate entity to deal with the complex lifecycle caused
    // by operator factories (e.g., duplicate and noMoreSinkFactories).
    @ThreadSafe
    public static class LocalExchangeSinkFactory
            implements Closeable
    {
        private final LocalExchange exchange;

        private LocalExchangeSinkFactory(LocalExchange exchange)
        {
            this.exchange = requireNonNull(exchange, "exchange is null");
        }

        public LocalExchangeSink createSink()
        {
            return exchange.createSink(this);
        }

        public LocalExchangeSinkFactory duplicate()
        {
            return exchange.createSinkFactory();
        }

        @Override
        public void close()
        {
            exchange.sinkFactoryClosed(this);
        }

        public void noMoreSinkFactories()
        {
            exchange.noMoreSinkFactories();
        }
    }
}
