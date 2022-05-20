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
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.trino.ExceededMemoryLimitException;
import io.trino.RowPagesBuilder;
import io.trino.operator.SetBuilderOperator.SetBuilderOperatorFactory;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.MaterializedResult;
import io.trino.type.BlockTypeOperators;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.collect.Iterables.concat;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.GroupByHashYieldAssertion.createPagesWithDistinctHashKeys;
import static io.trino.operator.GroupByHashYieldAssertion.finishOperatorWithYieldingGroupByHash;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestHashSemiJoinOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private TaskContext taskContext;
    private TypeOperators typeOperators;
    private BlockTypeOperators blockTypeOperators;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
        taskContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION);
        typeOperators = new TypeOperators();
        blockTypeOperators = new BlockTypeOperators(typeOperators);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @DataProvider(name = "hashEnabledValues")
    public static Object[][] hashEnabledValuesProvider()
    {
        return new Object[][] {{true}, {false}};
    }

    @DataProvider
    public Object[][] dataType()
    {
        return new Object[][] {{VARCHAR}, {BIGINT}};
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testSemiJoin(boolean hashEnabled)
    {
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();

        // build
        OperatorContext operatorContext = driverContext.addOperatorContext(0, new PlanNodeId("test"), ValuesOperator.class.getSimpleName());
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, Ints.asList(0), BIGINT);
        Operator buildOperator = new ValuesOperator(operatorContext, rowPagesBuilder
                .row(10L)
                .row(30L)
                .row(30L)
                .row(35L)
                .row(36L)
                .row(37L)
                .row(50L)
                .build());
        SetBuilderOperatorFactory setBuilderOperatorFactory = new SetBuilderOperatorFactory(
                1,
                new PlanNodeId("test"),
                rowPagesBuilder.getTypes().get(0),
                0,
                rowPagesBuilder.getHashChannel(),
                10,
                new JoinCompiler(typeOperators),
                blockTypeOperators);
        Operator setBuilderOperator = setBuilderOperatorFactory.createOperator(driverContext);

        Driver driver = Driver.createDriver(driverContext, buildOperator, setBuilderOperator);
        while (!driver.isFinished()) {
            driver.processUntilBlocked();
        }

        // probe
        List<Type> probeTypes = ImmutableList.of(BIGINT, BIGINT);
        RowPagesBuilder rowPagesBuilderProbe = rowPagesBuilder(hashEnabled, Ints.asList(0), BIGINT, BIGINT);
        List<Page> probeInput = rowPagesBuilderProbe
                .addSequencePage(10, 30, 0)
                .build();
        Optional<Integer> probeHashChannel = hashEnabled ? Optional.of(probeTypes.size()) : Optional.empty();
        OperatorFactory joinOperatorFactory = HashSemiJoinOperator.createOperatorFactory(
                2,
                new PlanNodeId("test"),
                setBuilderOperatorFactory.getSetProvider(),
                rowPagesBuilderProbe.getTypes(),
                0,
                probeHashChannel);

        // expected
        MaterializedResult expected = resultBuilder(driverContext.getSession(), concat(probeTypes, ImmutableList.of(BOOLEAN)))
                .row(30L, 0L, true)
                .row(31L, 1L, false)
                .row(32L, 2L, false)
                .row(33L, 3L, false)
                .row(34L, 4L, false)
                .row(35L, 5L, true)
                .row(36L, 6L, true)
                .row(37L, 7L, true)
                .row(38L, 8L, false)
                .row(39L, 9L, false)
                .build();

        OperatorAssertion.assertOperatorEquals(joinOperatorFactory, driverContext, probeInput, expected, hashEnabled, ImmutableList.of(probeTypes.size()));
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testSemiJoinOnVarcharType(boolean hashEnabled)
    {
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();

        // build
        OperatorContext operatorContext = driverContext.addOperatorContext(0, new PlanNodeId("test"), ValuesOperator.class.getSimpleName());
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, Ints.asList(0), VARCHAR);
        Operator buildOperator = new ValuesOperator(operatorContext, rowPagesBuilder
                .row("10")
                .row("30")
                .row("30")
                .row("35")
                .row("36")
                .row("37")
                .row("50")
                .build());
        SetBuilderOperatorFactory setBuilderOperatorFactory = new SetBuilderOperatorFactory(
                1,
                new PlanNodeId("test"),
                rowPagesBuilder.getTypes().get(0),
                0,
                rowPagesBuilder.getHashChannel(),
                10,
                new JoinCompiler(typeOperators),
                blockTypeOperators);
        Operator setBuilderOperator = setBuilderOperatorFactory.createOperator(driverContext);

        Driver driver = Driver.createDriver(driverContext, buildOperator, setBuilderOperator);
        while (!driver.isFinished()) {
            driver.processUntilBlocked();
        }

        // probe
        List<Type> probeTypes = ImmutableList.of(VARCHAR, BIGINT);
        RowPagesBuilder rowPagesBuilderProbe = rowPagesBuilder(hashEnabled, Ints.asList(0), VARCHAR, BIGINT);
        List<Page> probeInput = rowPagesBuilderProbe
                .addSequencePage(10, 30, 0)
                .build();
        Optional<Integer> probeHashChannel = hashEnabled ? Optional.of(probeTypes.size()) : Optional.empty();
        OperatorFactory joinOperatorFactory = HashSemiJoinOperator.createOperatorFactory(
                2,
                new PlanNodeId("test"),
                setBuilderOperatorFactory.getSetProvider(),
                rowPagesBuilderProbe.getTypes(),
                0,
                probeHashChannel);

        // expected
        MaterializedResult expected = resultBuilder(driverContext.getSession(), concat(probeTypes, ImmutableList.of(BOOLEAN)))
                .row("30", 0L, true)
                .row("31", 1L, false)
                .row("32", 2L, false)
                .row("33", 3L, false)
                .row("34", 4L, false)
                .row("35", 5L, true)
                .row("36", 6L, true)
                .row("37", 7L, true)
                .row("38", 8L, false)
                .row("39", 9L, false)
                .build();

        OperatorAssertion.assertOperatorEquals(joinOperatorFactory, driverContext, probeInput, expected, hashEnabled, ImmutableList.of(probeTypes.size()));
    }

    @Test(dataProvider = "dataType")
    public void testSemiJoinMemoryReservationYield(Type type)
    {
        // We only need the first column so we are creating the pages with hashEnabled false
        List<Page> input = createPagesWithDistinctHashKeys(type, 5_000, 500);

        // create the operator
        SetBuilderOperatorFactory setBuilderOperatorFactory = new SetBuilderOperatorFactory(
                1,
                new PlanNodeId("test"),
                type,
                0,
                Optional.of(1),
                10,
                new JoinCompiler(typeOperators),
                blockTypeOperators);

        // run test
        GroupByHashYieldAssertion.GroupByHashYieldResult result = finishOperatorWithYieldingGroupByHash(
                input,
                type,
                setBuilderOperatorFactory,
                operator -> ((SetBuilderOperator) operator).getCapacity(),
                1_400_000);

        assertGreaterThanOrEqual(result.getYieldCount(), 5);
        assertGreaterThan(result.getMaxReservedBytes(), 20L << 20);
        assertEquals(result.getOutput().stream().mapToInt(Page::getPositionCount).sum(), 0);
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testBuildSideNulls(boolean hashEnabled)
    {
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();

        // build
        OperatorContext operatorContext = driverContext.addOperatorContext(0, new PlanNodeId("test"), ValuesOperator.class.getSimpleName());
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, Ints.asList(0), buildTypes);
        Operator buildOperator = new ValuesOperator(operatorContext, rowPagesBuilder
                .row(0L)
                .row(1L)
                .row(2L)
                .row(2L)
                .row(3L)
                .row((Object) null)
                .build());
        SetBuilderOperatorFactory setBuilderOperatorFactory = new SetBuilderOperatorFactory(
                1,
                new PlanNodeId("test"),
                buildTypes.get(0),
                0,
                rowPagesBuilder.getHashChannel(),
                10,
                new JoinCompiler(typeOperators),
                blockTypeOperators);
        Operator setBuilderOperator = setBuilderOperatorFactory.createOperator(driverContext);

        Driver driver = Driver.createDriver(driverContext, buildOperator, setBuilderOperator);
        while (!driver.isFinished()) {
            driver.processUntilBlocked();
        }

        // probe
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder rowPagesBuilderProbe = rowPagesBuilder(hashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = rowPagesBuilderProbe
                .addSequencePage(4, 1)
                .build();
        Optional<Integer> probeHashChannel = hashEnabled ? Optional.of(probeTypes.size()) : Optional.empty();
        OperatorFactory joinOperatorFactory = HashSemiJoinOperator.createOperatorFactory(
                2,
                new PlanNodeId("test"),
                setBuilderOperatorFactory.getSetProvider(),
                rowPagesBuilderProbe.getTypes(),
                0,
                probeHashChannel);

        // expected
        MaterializedResult expected = resultBuilder(driverContext.getSession(), concat(probeTypes, ImmutableList.of(BOOLEAN)))
                .row(1L, true)
                .row(2L, true)
                .row(3L, true)
                .row(4L, null)
                .build();

        OperatorAssertion.assertOperatorEquals(joinOperatorFactory, driverContext, probeInput, expected, hashEnabled, ImmutableList.of(probeTypes.size()));
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testProbeSideNulls(boolean hashEnabled)
    {
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();

        // build
        OperatorContext operatorContext = driverContext.addOperatorContext(0, new PlanNodeId("test"), ValuesOperator.class.getSimpleName());
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, Ints.asList(0), buildTypes);
        Operator buildOperator = new ValuesOperator(operatorContext, rowPagesBuilder
                .row(0L)
                .row(1L)
                .row(3L)
                .build());
        SetBuilderOperatorFactory setBuilderOperatorFactory = new SetBuilderOperatorFactory(
                1,
                new PlanNodeId("test"),
                buildTypes.get(0),
                0,
                rowPagesBuilder.getHashChannel(),
                10,
                new JoinCompiler(typeOperators),
                blockTypeOperators);
        Operator setBuilderOperator = setBuilderOperatorFactory.createOperator(driverContext);

        Driver driver = Driver.createDriver(driverContext, buildOperator, setBuilderOperator);
        while (!driver.isFinished()) {
            driver.processUntilBlocked();
        }

        // probe
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder rowPagesBuilderProbe = rowPagesBuilder(hashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = rowPagesBuilderProbe
                .row(0L)
                .row((Object) null)
                .row(1L)
                .row(2L)
                .build();
        Optional<Integer> probeHashChannel = hashEnabled ? Optional.of(probeTypes.size()) : Optional.empty();
        OperatorFactory joinOperatorFactory = HashSemiJoinOperator.createOperatorFactory(
                2,
                new PlanNodeId("test"),
                setBuilderOperatorFactory.getSetProvider(),
                rowPagesBuilderProbe.getTypes(),
                0,
                probeHashChannel);

        // expected
        MaterializedResult expected = resultBuilder(driverContext.getSession(), concat(probeTypes, ImmutableList.of(BOOLEAN)))
                .row(0L, true)
                .row(null, null)
                .row(1L, true)
                .row(2L, false)
                .build();

        OperatorAssertion.assertOperatorEquals(joinOperatorFactory, driverContext, probeInput, expected, hashEnabled, ImmutableList.of(probeTypes.size()));
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testProbeAndBuildNulls(boolean hashEnabled)
    {
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();

        // build
        OperatorContext operatorContext = driverContext.addOperatorContext(0, new PlanNodeId("test"), ValuesOperator.class.getSimpleName());
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, Ints.asList(0), buildTypes);
        Operator buildOperator = new ValuesOperator(operatorContext, rowPagesBuilder
                .row(0L)
                .row(1L)
                .row((Object) null)
                .row(3L)
                .build());
        SetBuilderOperatorFactory setBuilderOperatorFactory = new SetBuilderOperatorFactory(
                1,
                new PlanNodeId("test"),
                buildTypes.get(0),
                0,
                rowPagesBuilder.getHashChannel(),
                10,
                new JoinCompiler(typeOperators),
                blockTypeOperators);
        Operator setBuilderOperator = setBuilderOperatorFactory.createOperator(driverContext);

        Driver driver = Driver.createDriver(driverContext, buildOperator, setBuilderOperator);
        while (!driver.isFinished()) {
            driver.processUntilBlocked();
        }

        // probe
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder rowPagesBuilderProbe = rowPagesBuilder(hashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = rowPagesBuilderProbe
                .row(0L)
                .row((Object) null)
                .row(1L)
                .row(2L)
                .build();
        Optional<Integer> probeHashChannel = hashEnabled ? Optional.of(probeTypes.size()) : Optional.empty();
        OperatorFactory joinOperatorFactory = HashSemiJoinOperator.createOperatorFactory(
                2,
                new PlanNodeId("test"),
                setBuilderOperatorFactory.getSetProvider(),
                rowPagesBuilderProbe.getTypes(),
                0,
                probeHashChannel);

        // expected
        MaterializedResult expected = resultBuilder(driverContext.getSession(), concat(probeTypes, ImmutableList.of(BOOLEAN)))
                .row(0L, true)
                .row(null, null)
                .row(1L, true)
                .row(2L, null)
                .build();

        OperatorAssertion.assertOperatorEquals(joinOperatorFactory, driverContext, probeInput, expected, hashEnabled, ImmutableList.of(probeTypes.size()));
    }

    @Test(dataProvider = "hashEnabledValues", expectedExceptions = ExceededMemoryLimitException.class, expectedExceptionsMessageRegExp = "Query exceeded per-node memory limit of.*")
    public void testMemoryLimit(boolean hashEnabled)
    {
        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION, DataSize.ofBytes(100))
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        OperatorContext operatorContext = driverContext.addOperatorContext(0, new PlanNodeId("test"), ValuesOperator.class.getSimpleName());
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, Ints.asList(0), buildTypes);
        Operator buildOperator = new ValuesOperator(operatorContext, rowPagesBuilder
                .addSequencePage(10000, 20)
                .build());
        SetBuilderOperatorFactory setBuilderOperatorFactory = new SetBuilderOperatorFactory(
                1,
                new PlanNodeId("test"),
                buildTypes.get(0),
                0,
                rowPagesBuilder.getHashChannel(),
                10,
                new JoinCompiler(typeOperators),
                blockTypeOperators);
        Operator setBuilderOperator = setBuilderOperatorFactory.createOperator(driverContext);

        Driver driver = Driver.createDriver(driverContext, buildOperator, setBuilderOperator);
        while (!driver.isFinished()) {
            driver.processUntilBlocked();
        }
    }
}
