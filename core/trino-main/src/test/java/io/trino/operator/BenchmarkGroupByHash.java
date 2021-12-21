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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
import io.trino.array.LongBigArray;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.AbstractLongType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinCompiler;
import io.trino.type.BlockTypeOperators;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.AsyncProfiler;
import org.openjdk.jmh.runner.RunnerException;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.operator.UpdateMemory.NOOP;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkGroupByHash
{
    private static final int POSITIONS = 10_000_000;
    private static final String GROUP_COUNT_STRING = "3000000";
    private static final int GROUP_COUNT = Integer.parseInt(GROUP_COUNT_STRING);
    private static final int EXPECTED_SIZE = 10000;
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final BlockTypeOperators TYPE_OPERATOR_FACTORY = new BlockTypeOperators(TYPE_OPERATORS);

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public void groupByHashPreCompute(BenchmarkData data, Blackhole blackhole)
    {
        GroupByHash groupByHash = new MultiChannelGroupByHash(data.getTypes(), data.getChannels(), data.getHashChannel(), data.getExpectedSize(), false, data.getJoinCompiler(), TYPE_OPERATOR_FACTORY, NOOP);
        addInputPagesToHash(groupByHash, data.getPages());

        buildPages(groupByHash, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public void groupByHashPreComputeBatch(BatchedBenchmarkData data, Blackhole blackhole)
    {
        GroupByHash groupByHash = new MultiChannelGroupByHashBatch(data.getTypes(), data.getChannels(), data.getHashChannel(), data.getExpectedSize(), false, data.getJoinCompiler(), TYPE_OPERATOR_FACTORY, NOOP, data.batchSize);
        addInputPagesToHash(groupByHash, data.getPages());

        buildPages(groupByHash, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public void groupByHashPreComputeInline(BenchmarkData data, Blackhole blackhole)
    {
        GroupByHash groupByHash = new MultiChannelGroupByHashInline(data.getTypes(), data.getChannels(), data.getHashChannel(), data.getExpectedSize(), false, data.getJoinCompiler(), TYPE_OPERATOR_FACTORY, NOOP);
        addInputPagesToHash(groupByHash, data.getPages());

        buildPages(groupByHash, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public void groupByHashPreComputeInlineMultiChannelBigInt(BenchmarkData data, Blackhole blackhole)
    {
        GroupByHash groupByHash = new MultiChannelBigintGroupByHashInline(data.getChannels(), data.getHashChannel(), data.getExpectedSize(), NOOP);
        addInputPagesToHash(groupByHash, data.getPages());

        buildPages(groupByHash, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public void groupByHashPreComputeInlineMultiChannelBigIntBatch(BatchedBenchmarkData data, Blackhole blackhole)
    {
        GroupByHash groupByHash = new MultiChannelBigintGroupByHashInlineBatch(data.getChannels(), data.getHashChannel(),
                data.getExpectedSize(), NOOP, data.batchSize);
        addInputPagesToHash(groupByHash, data.getPages());

        buildPages(groupByHash, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public void groupByHashPreComputeInlineMultiChannelBigIntBB(BenchmarkData data, Blackhole blackhole)
    {
        GroupByHash groupByHash = new MultiChannelBigintGroupByHashInlineBB(data.getChannels(), data.getHashChannel(), data.getExpectedSize(), NOOP);
        addInputPagesToHash(groupByHash, data.getPages());

        buildPages(groupByHash, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public void groupByHashPreComputeInlineMultiChannelBigIntFastBB(FastByteBufferBenchmarkData data, Blackhole blackhole)
    {
        GroupByHash groupByHash = new MultiChannelBigintGroupByHashInlineFastBB(data.getChannels(), data.getHashChannel(), data.getExpectedSize(), NOOP);
        addInputPagesToHash(groupByHash, data.getPages());

        buildPages(groupByHash, blackhole);
    }

    private void buildPages(GroupByHash groupByHash, Blackhole blackhole)
    {
        PageBuilder pageBuilder = new PageBuilder(groupByHash.getTypes());
        GroupByHash.GroupCursor groups = groupByHash.consecutiveGroups();
        while (groups.hasNext()) {
            groups.next();
            pageBuilder.declarePosition();
            groups.appendValuesTo(pageBuilder, 0);
            if (pageBuilder.isFull()) {
                blackhole.consume(pageBuilder.build());
                pageBuilder.reset();
            }
        }
        blackhole.consume(pageBuilder.build());
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public List<Page> benchmarkHashPosition(BenchmarkData data)
    {
        InterpretedHashGenerator hashGenerator = new InterpretedHashGenerator(data.getTypes(), data.getChannels(), TYPE_OPERATOR_FACTORY);
        ImmutableList.Builder<Page> results = ImmutableList.builderWithExpectedSize(data.getPages().size());
        for (Page page : data.getPages()) {
            long[] hashes = new long[page.getPositionCount()];
            for (int position = 0; position < page.getPositionCount(); position++) {
                hashes[position] = hashGenerator.hashPosition(position, page);
            }
            results.add(page.appendColumn(new LongArrayBlock(page.getPositionCount(), Optional.empty(), hashes)));
        }
        return results.build();
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public void addPagePreCompute(BenchmarkData data, Blackhole blackhole)
    {
        GroupByHash groupByHash = new MultiChannelGroupByHash(data.getTypes(), data.getChannels(), data.getHashChannel(), EXPECTED_SIZE, false, data.getJoinCompiler(), TYPE_OPERATOR_FACTORY, NOOP);
        addInputPagesToHash(groupByHash, data.getPages());

        buildPages(groupByHash, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public void bigintGroupByHash(SingleChannelBenchmarkData data, Blackhole blackhole)
    {
        GroupByHash groupByHash = new BigintGroupByHash(0, data.getHashEnabled(), data.expectedSize, NOOP);
        benchmarkGroupByHash(data, groupByHash, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public void bigintGroupByHashGID(SingleChannelBenchmarkData data, Blackhole blackhole)
    {
        GroupByHash groupByHash = new BigintGroupByHashInlineGID(0, data.getHashEnabled(), data.expectedSize, NOOP);
        benchmarkGroupByHash(data, groupByHash, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public void bigintGroupByHashGIDBigArray(SingleChannelBenchmarkData data, Blackhole blackhole)
    {
        GroupByHash groupByHash = new BigintGroupByHashInlineGIDBigArray(0, data.getHashEnabled(), data.expectedSize, NOOP);
        benchmarkGroupByHash(data, groupByHash, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public void bigintGroupByHashBatch(SingleChannelBenchmarkData data, Blackhole blackhole)
    {
        GroupByHash groupByHash = new BigintGroupByHashBatchNoRehash(0, data.getHashEnabled(), data.expectedSize, NOOP);
        benchmarkGroupByHash(data, groupByHash, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public void bigintGroupByHashMultiChannel(SingleChannelBenchmarkData data, Blackhole blackhole)
    {
        GroupByHash groupByHash = new MultiChannelBigintGroupByHashInline(new int[] {0},
                data.getHashEnabled() ? Optional.of(1) : Optional.empty(), data.expectedSize, NOOP);
        benchmarkGroupByHash(data, groupByHash, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public void bigintGroupByHashBatchGID(SingleChannelBenchmarkData data, Blackhole blackhole)
    {
        GroupByHash groupByHash = new BigintGroupByHashBatchInlineGID(0, data.getHashEnabled(), data.expectedSize, NOOP);
        benchmarkGroupByHash(data, groupByHash, blackhole);
    }

    private void benchmarkGroupByHash(SingleChannelBenchmarkData data, GroupByHash groupByHash, Blackhole blackhole)
    {
        addInputPagesToHash(groupByHash, data.getPages());
        buildPages(groupByHash, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public long baseline(BaselinePagesData data)
    {
        int hashSize = arraySize(GROUP_COUNT, 0.9f);
        int mask = hashSize - 1;
        long[] table = new long[hashSize];
        Arrays.fill(table, -1);

        long groupIds = 0;
        for (Page page : data.getPages()) {
            Block block = page.getBlock(0);
            int positionCount = block.getPositionCount();
            for (int position = 0; position < positionCount; position++) {
                long value = block.getLong(position, 0);

                int tablePosition = (int) (value & mask);
                while (table[tablePosition] != -1 && table[tablePosition] != value) {
                    tablePosition++;
                }
                if (table[tablePosition] == -1) {
                    table[tablePosition] = value;
                    groupIds++;
                }
            }
        }
        return groupIds;
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public long baselineBigArray(BaselinePagesData data)
    {
        int hashSize = arraySize(GROUP_COUNT, 0.9f);
        int mask = hashSize - 1;
        LongBigArray table = new LongBigArray(-1);
        table.ensureCapacity(hashSize);

        long groupIds = 0;
        for (Page page : data.getPages()) {
            Block block = page.getBlock(0);
            int positionCount = block.getPositionCount();
            for (int position = 0; position < positionCount; position++) {
                long value = BIGINT.getLong(block, position);

                int tablePosition = (int) XxHash64.hash(value) & mask;
                while (table.get(tablePosition) != -1 && table.get(tablePosition) != value) {
                    tablePosition++;
                }
                if (table.get(tablePosition) == -1) {
                    table.set(tablePosition, value);
                    groupIds++;
                }
            }
        }
        return groupIds;
    }

    private static void addInputPagesToHash(GroupByHash groupByHash, List<Page> pages)
    {
        for (Page page : pages) {
            Work<?> work = groupByHash.addPage(page);
            boolean finished;
            do {
                finished = work.process();
            }
            while (!finished);
        }
    }

    private static List<Page> createBigintPages(int positionCount, int groupCount, int channelCount, boolean hashEnabled, boolean useMixedBlockTypes)
    {
        List<Type> types = Collections.nCopies(channelCount, BIGINT);
        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        if (hashEnabled) {
            types = ImmutableList.copyOf(Iterables.concat(types, ImmutableList.of(BIGINT)));
        }

        PageBuilder pageBuilder = new PageBuilder(types);
        int pageCount = 0;
        for (int position = 0; position < positionCount; position++) {
            int rand = ThreadLocalRandom.current().nextInt(groupCount);
            pageBuilder.declarePosition();
            for (int numChannel = 0; numChannel < channelCount; numChannel++) {
                BIGINT.writeLong(pageBuilder.getBlockBuilder(numChannel), rand);
            }
            if (hashEnabled) {
                BIGINT.writeLong(pageBuilder.getBlockBuilder(channelCount), AbstractLongType.hash(rand));
            }
            if (pageBuilder.isFull()) {
                Page page = pageBuilder.build();
                pageBuilder.reset();
                if (useMixedBlockTypes) {
                    if (pageCount % 3 == 0) {
                        pages.add(page);
                    }
                    else if (pageCount % 3 == 1) {
                        // rle page
                        Block[] blocks = new Block[page.getChannelCount()];
                        for (int channel = 0; channel < blocks.length; ++channel) {
                            blocks[channel] = new RunLengthEncodedBlock(page.getBlock(channel).getSingleValueBlock(0), page.getPositionCount());
                        }
                        pages.add(new Page(blocks));
                    }
                    else {
                        // dictionary page
                        int[] positions = IntStream.range(0, page.getPositionCount()).toArray();
                        Block[] blocks = new Block[page.getChannelCount()];
                        for (int channel = 0; channel < page.getChannelCount(); ++channel) {
                            blocks[channel] = new DictionaryBlock(page.getBlock(channel), positions);
                        }
                        pages.add(new Page(blocks));
                    }
                }
                else {
                    pages.add(page);
                }
                pageCount++;
            }
        }
        pages.add(pageBuilder.build());
        return pages.build();
    }

    private static List<Page> createVarcharPages(int positionCount, int groupCount, int channelCount, boolean hashEnabled)
    {
        List<Type> types = Collections.nCopies(channelCount, VARCHAR);
        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        if (hashEnabled) {
            types = ImmutableList.copyOf(Iterables.concat(types, ImmutableList.of(BIGINT)));
        }

        PageBuilder pageBuilder = new PageBuilder(types);
        for (int position = 0; position < positionCount; position++) {
            int rand = ThreadLocalRandom.current().nextInt(groupCount);
            Slice value = Slices.wrappedBuffer(ByteBuffer.allocate(4).putInt(rand).flip());
            pageBuilder.declarePosition();
            for (int channel = 0; channel < channelCount; channel++) {
                VARCHAR.writeSlice(pageBuilder.getBlockBuilder(channel), value);
            }
            if (hashEnabled) {
                BIGINT.writeLong(pageBuilder.getBlockBuilder(channelCount), XxHash64.hash(value));
            }
            if (pageBuilder.isFull()) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
        }
        pages.add(pageBuilder.build());
        return pages.build();
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BaselinePagesData
    {
        @Param("1")
        private int channelCount = 1;

        @Param("false")
        private boolean hashEnabled;

        @Param(GROUP_COUNT_STRING)
        private int groupCount;

        private List<Page> pages;

        @Setup
        public void setup()
        {
            pages = createBigintPages(POSITIONS, groupCount, channelCount, hashEnabled, false);
        }

        public List<Page> getPages()
        {
            return pages;
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class SingleChannelBenchmarkData
    {
        @Param({"1024", "100000", "3000000", "6000000"})
        public int expectedSize = 1024;

        @Param({"10000000", "3000000", "1000000", "100000", "10000", "1000", "100", "10", "4"})
        private int groupCount = GROUP_COUNT;

        @Param("1")
        private int channelCount = 1;

        @Param({"true", "false"})
        private boolean hashEnabled = true;

        private List<Page> pages;
        private List<Type> types;
        private int[] channels;

        @Setup
        public void setup()
        {
            setup(false);
        }

        public void setup(boolean useMixedBlockTypes)
        {
            pages = createBigintPages(POSITIONS, groupCount, channelCount, hashEnabled, useMixedBlockTypes);
            types = Collections.nCopies(1, BIGINT);
            channels = new int[1];
            for (int i = 0; i < 1; i++) {
                channels[i] = i;
            }
        }

        public List<Page> getPages()
        {
            return pages;
        }

        public List<Type> getTypes()
        {
            return types;
        }

        public boolean getHashEnabled()
        {
            return hashEnabled;
        }
    }

    @State(Scope.Thread)
    public static class FastByteBufferBenchmarkData
            extends BenchmarkData
    {
        @Param({"true", "false"})
        private boolean useOffHeap;

        @Override
        public void setup()
        {
            OffHeapByteBuffer.USE_OFF_HEAP = useOffHeap;
            super.setup();
        }
    }

    @State(Scope.Thread)
    public static class BatchedBenchmarkData
            extends BenchmarkData
    {

        @Param({"8", "16", "32", "64", "128"})
        private int batchSize = 16;
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"1", "5", "10", "15", "20"})
        private int channelCount = 1;

        // todo add more group counts when JMH support programmatic ability to set OperationsPerInvocation
        @Param(GROUP_COUNT_STRING)
        private int groupCount = GROUP_COUNT;

        @Param({"true", "false"})
        private boolean hashEnabled;

        @Param({"VARCHAR", "BIGINT"})
        private String dataType = "VARCHAR";

        @Param({"true", "false"})
        private boolean rehash;

        private List<Page> pages;
        private Optional<Integer> hashChannel;
        private List<Type> types;
        private int[] channels;
        private JoinCompiler joinCompiler;

        @Setup
        public void setup()
        {
            switch (dataType) {
                case "VARCHAR":
                    types = Collections.nCopies(channelCount, VARCHAR);
                    pages = createVarcharPages(POSITIONS, groupCount, channelCount, hashEnabled);
                    break;
                case "BIGINT":
                    types = Collections.nCopies(channelCount, BIGINT);
                    pages = createBigintPages(POSITIONS, groupCount, channelCount, hashEnabled, false);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported dataType");
            }
            hashChannel = hashEnabled ? Optional.of(channelCount) : Optional.empty();
            channels = new int[channelCount];
            for (int i = 0; i < channelCount; i++) {
                channels[i] = i;
            }

            this.joinCompiler = new JoinCompiler(TYPE_OPERATORS);
        }

        public List<Page> getPages()
        {
            return pages;
        }

        public Optional<Integer> getHashChannel()
        {
            return hashChannel;
        }

        public List<Type> getTypes()
        {
            return types;
        }

        public int[] getChannels()
        {
            return channels;
        }

        public JoinCompiler getJoinCompiler()
        {
            return joinCompiler;
        }

        public int getExpectedSize()
        {
            return rehash ? 10_000 : groupCount;
        }
    }

//    static {
//        // pollute BigintGroupByHash profile by different block types
//        SingleChannelBenchmarkData singleChannelBenchmarkData = new SingleChannelBenchmarkData();
//        singleChannelBenchmarkData.setup(true);
//        BenchmarkGroupByHash hash = new BenchmarkGroupByHash();
//        for (int i = 0; i < 5; ++i) {
//            hash.bigintGroupByHash(singleChannelBenchmarkData, fakeBlackhole());
//        }
//    }

    private static Blackhole fakeBlackhole()
    {
        return new Blackhole("Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.");
    }

    public static void main(String[] args)
            throws RunnerException, IOException
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.dataType = "BIGINT";
        data.groupCount = 8;
        data.channelCount = 2;
        data.setup();
//        new BenchmarkGroupByHash().groupByHashPreCompute(data, fakeBlackhole());
//        new BenchmarkGroupByHash().addPagePreCompute(data, fakeBlackhole());

//        SingleChannelBenchmarkData singleChannelBenchmarkData = new SingleChannelBenchmarkData();
//        singleChannelBenchmarkData.setup();
//        new BenchmarkGroupByHash().bigintGroupByHashBatch(singleChannelBenchmarkData);

        String profilerOutputDir = profilerOutputDir();
        new BenchmarkGroupByHash().groupByHashPreComputeInlineMultiChannelBigInt(data, fakeBlackhole());
        benchmark(BenchmarkGroupByHash.class)
//                .includeMethod("bigintGroupByHash")
//                .includeMethod("bigintGroupByHashGID")
//                .includeMethod("bigintGroupByHashMultiChannel")
//                .includeMethod("bigintGroupByHashBatchGID")
//                .includeMethod("bigintGroupByHashGIDBigArray")
                .includeMethod("groupByHashPreCompute")
//                .includeMethod("groupByHashPreComputeInline")
//                .includeMethod("groupByHashPreComputeInlineMultiChannelBigInt")
//                .includeMethod("groupByHashPreComputeInlineMultiChannelBigIntBB")
//                .includeMethod("groupByHashPreComputeInlineMultiChannelBigIntFastBB")
//                .includeMethod("groupByHashPreComputeBatch")
                .includeMethod("groupByHashPreComputeInlineMultiChannelBigIntBatch")
//                .includeMethod("groupByHashPreCompute.*")
//                .includeMethod("baseline")
//                .includeMethod("baselineBigArray")
                .withOptions(optionsBuilder -> optionsBuilder
//                        .addProfiler(GCProfiler.class)
                        .addProfiler(AsyncProfiler.class, String.format("dir=%s;output=text;output=flamegraph", profilerOutputDir))
//                        .addProfiler(DTraceAsmProfiler.class, "event=branch-misses")
//                        .addProfiler(DTraceAsmProfiler.class, String.format("hotThreshold=0.1;tooBigThreshold=3000;saveLog=true;saveLogTo=%s", profilerOutputDir, profilerOutputDir))
                        .jvmArgs("-Xmx32g")
//                        .param("hashEnabled", "true")
                        .param("hashEnabled", "false")
//                        .param("expectedSize", "10000")
                        .param("groupCount", "3000000")
//                        .param("groupCount", "8")
//                        .param("groupCount", "8")
//                        .param("channelCount", "2", "5", "10")
                        .param("channelCount", "5", "10")
                        .param("dataType", "BIGINT")
                        .param("useOffHeap", "false")
                        .param("batchSize", "16")
//                        .param("rehash", "true")
                        .forks(1)
//                        .jvmArgsAppend("-XX:+UnlockDiagnosticVMOptions", "-XX:+TraceClassLoading", "-XX:+LogCompilation", "-XX:+DebugNonSafepoints", "-XX:+PrintAssembly", "-XX:+PrintInlining")
//                        .jvmArgsAppend("-XX:MaxInlineSize=300", "-XX:InlineSmallCode=3000")
                        .warmupIterations(40)
                        .measurementIterations(10))
                .run();
        File dir = new File(profilerOutputDir);
        if (dir.list().length == 0) {
            FileUtils.deleteDirectory(dir);
        }
    }

    private static String profilerOutputDir()
    {
        try {
            String jmhDir = "jmh";
            new File(jmhDir).mkdirs();
            String outDir = jmhDir + "/" + String.valueOf(Files.list(Paths.get(jmhDir))
                    .map(path -> Integer.parseInt(path.getFileName().toString()) + 1)
                    .sorted(Comparator.reverseOrder())
                    .findFirst().orElse(0));
            new File(outDir).mkdirs();
            return outDir;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
