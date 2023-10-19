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
package io.trino.spi.predicate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import io.airlift.json.ObjectMapperProvider;
import io.trino.spi.block.Block;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.block.TestingBlockJsonSerde;
import io.trino.spi.type.TestingTypeDeserializer;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import org.assertj.core.api.AssertProvider;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSortedRangeSet
{
    @Test
    public void testEmptySet()
    {
        SortedRangeSet rangeSet = SortedRangeSet.none(BIGINT);
        assertThat(rangeSet.getType()).isEqualTo(BIGINT);
        assertThat(rangeSet.isNone()).isTrue();
        assertThat(rangeSet.isAll()).isFalse();
        assertThat(rangeSet.isSingleValue()).isFalse();
        assertThat(rangeSet.getOrderedRanges().isEmpty()).isTrue();
        assertThat(rangeSet.getRangeCount()).isEqualTo(0);
        assertThat(rangeSet.complement()).isEqualTo(SortedRangeSet.all(BIGINT));
        assertThat(rangeSet.containsValue(0L)).isFalse();
        assertThat(rangeSet.toString()).isEqualTo("SortedRangeSet[type=bigint, ranges=0, {}]");
    }

    @Test
    public void testEntireSet()
    {
        SortedRangeSet rangeSet = SortedRangeSet.all(BIGINT);
        assertThat(rangeSet.getType()).isEqualTo(BIGINT);
        assertThat(rangeSet.isNone()).isFalse();
        assertThat(rangeSet.isAll()).isTrue();
        assertThat(rangeSet.isSingleValue()).isFalse();
        assertThat(rangeSet.getRangeCount()).isEqualTo(1);
        assertThat(rangeSet.complement()).isEqualTo(SortedRangeSet.none(BIGINT));
        assertThat(rangeSet.containsValue(0L)).isTrue();
        assertThat(rangeSet.toString()).isEqualTo("SortedRangeSet[type=bigint, ranges=1, {(<min>,<max>)}]");
    }

    @Test
    public void testSingleValue()
    {
        SortedRangeSet rangeSet = SortedRangeSet.of(BIGINT, 10L);

        SortedRangeSet complement = SortedRangeSet.of(Range.greaterThan(BIGINT, 10L), Range.lessThan(BIGINT, 10L));

        assertThat(rangeSet.getType()).isEqualTo(BIGINT);
        assertThat(rangeSet.isNone()).isFalse();
        assertThat(rangeSet.isAll()).isFalse();
        assertThat(rangeSet.isSingleValue()).isTrue();
        assertThat(rangeSet.getOrderedRanges()).isEqualTo(ImmutableList.of(Range.equal(BIGINT, 10L)));
        assertThat(rangeSet.getRangeCount()).isEqualTo(1);
        assertThat(rangeSet.complement()).isEqualTo(complement);
        assertThat(rangeSet.containsValue(10L)).isTrue();
        assertThat(rangeSet.containsValue(9L)).isFalse();
        assertThat(rangeSet.toString()).isEqualTo("SortedRangeSet[type=bigint, ranges=1, {[10]}]");

        assertThat(SortedRangeSet.of(Range.equal(VARCHAR, utf8Slice("LARGE PLATED NICKEL"))).toString()).isEqualTo("SortedRangeSet[type=varchar, ranges=1, {[LARGE PLATED NICKEL]}]");
    }

    @Test
    public void testBoundedSet()
    {
        SortedRangeSet rangeSet = SortedRangeSet.of(
                Range.equal(BIGINT, 10L),
                Range.equal(BIGINT, 0L),
                Range.range(BIGINT, 9L, true, 11L, false),
                Range.equal(BIGINT, 0L),
                Range.range(BIGINT, 2L, true, 4L, true),
                Range.range(BIGINT, 4L, false, 5L, true));

        ImmutableList<Range> normalizedResult = ImmutableList.of(
                Range.equal(BIGINT, 0L),
                Range.range(BIGINT, 2L, true, 5L, true),
                Range.range(BIGINT, 9L, true, 11L, false));

        SortedRangeSet complement = SortedRangeSet.of(
                Range.lessThan(BIGINT, 0L),
                Range.range(BIGINT, 0L, false, 2L, false),
                Range.range(BIGINT, 5L, false, 9L, false),
                Range.greaterThanOrEqual(BIGINT, 11L));

        assertThat(rangeSet.getType()).isEqualTo(BIGINT);
        assertThat(rangeSet.isNone()).isFalse();
        assertThat(rangeSet.isAll()).isFalse();
        assertThat(rangeSet.isSingleValue()).isFalse();
        assertThat(rangeSet.getOrderedRanges()).isEqualTo(normalizedResult);
        assertThat(rangeSet).isEqualTo(SortedRangeSet.copyOf(BIGINT, normalizedResult));
        assertThat(rangeSet.getRangeCount()).isEqualTo(3);
        assertThat(rangeSet.complement()).isEqualTo(complement);
        assertThat(rangeSet.containsValue(0L)).isTrue();
        assertThat(rangeSet.containsValue(1L)).isFalse();
        assertThat(rangeSet.containsValue(7L)).isFalse();
        assertThat(rangeSet.containsValue(9L)).isTrue();
        assertThat(rangeSet.toString()).isEqualTo("SortedRangeSet[type=bigint, ranges=3, {[0], [2,5], [9,11)}]");
        assertThat(rangeSet.toString(ToStringSession.INSTANCE, 2)).isEqualTo("SortedRangeSet[type=bigint, ranges=3, {[0], ..., [9,11)}]");
        assertThat(rangeSet.toString(ToStringSession.INSTANCE, 1)).isEqualTo("SortedRangeSet[type=bigint, ranges=3, {[0], ...}]");
    }

    @Test
    public void testUnboundedSet()
    {
        SortedRangeSet rangeSet = SortedRangeSet.of(
                Range.greaterThan(BIGINT, 10L),
                Range.lessThanOrEqual(BIGINT, 0L),
                Range.range(BIGINT, 2L, true, 4L, false),
                Range.range(BIGINT, 4L, true, 6L, false),
                Range.range(BIGINT, 1L, false, 2L, false),
                Range.range(BIGINT, 9L, false, 11L, false));

        ImmutableList<Range> normalizedResult = ImmutableList.of(
                Range.lessThanOrEqual(BIGINT, 0L),
                Range.range(BIGINT, 1L, false, 6L, false),
                Range.greaterThan(BIGINT, 9L));

        SortedRangeSet complement = SortedRangeSet.of(
                Range.range(BIGINT, 0L, false, 1L, true),
                Range.range(BIGINT, 6L, true, 9L, true));

        assertThat(rangeSet.getType()).isEqualTo(BIGINT);
        assertThat(rangeSet.isNone()).isFalse();
        assertThat(rangeSet.isAll()).isFalse();
        assertThat(rangeSet.isSingleValue()).isFalse();
        assertThat(rangeSet.getOrderedRanges()).isEqualTo(normalizedResult);
        assertThat(rangeSet).isEqualTo(SortedRangeSet.copyOf(BIGINT, normalizedResult));
        assertThat(rangeSet.getRangeCount()).isEqualTo(3);
        assertThat(rangeSet.complement()).isEqualTo(complement);
        assertThat(rangeSet.containsValue(0L)).isTrue();
        assertThat(rangeSet.containsValue(4L)).isTrue();
        assertThat(rangeSet.containsValue(7L)).isFalse();
        assertThat(rangeSet.toString()).isEqualTo("SortedRangeSet[type=bigint, ranges=3, {(<min>,0], (1,6), (9,<max>)}]");
    }

    @Test
    public void testCreateWithRanges()
    {
        // two low-unbounded, first shorter
        assertThat(SortedRangeSet.of(Range.lessThan(BIGINT, 5L), Range.lessThan(BIGINT, 10L)).getOrderedRanges())
                .containsExactly(Range.lessThan(BIGINT, 10L));
        assertThat(SortedRangeSet.of(Range.lessThan(BIGINT, 10L), Range.lessThanOrEqual(BIGINT, 10L)).getOrderedRanges())
                .containsExactly(Range.lessThanOrEqual(BIGINT, 10L));

        // two low-unbounded, second shorter
        assertThat(SortedRangeSet.of(Range.lessThan(BIGINT, 10L), Range.lessThan(BIGINT, 5L)).getOrderedRanges())
                .containsExactly(Range.lessThan(BIGINT, 10L));
        assertThat(SortedRangeSet.of(Range.lessThanOrEqual(BIGINT, 10L), Range.lessThan(BIGINT, 10L)).getOrderedRanges())
                .containsExactly(Range.lessThanOrEqual(BIGINT, 10L));

        // two high-unbounded, first shorter
        assertThat(SortedRangeSet.of(Range.greaterThan(BIGINT, 10L), Range.greaterThan(BIGINT, 5L)).getOrderedRanges())
                .containsExactly(Range.greaterThan(BIGINT, 5L));
        assertThat(SortedRangeSet.of(Range.greaterThan(BIGINT, 10L), Range.greaterThanOrEqual(BIGINT, 10L)).getOrderedRanges())
                .containsExactly(Range.greaterThanOrEqual(BIGINT, 10L));

        // two high-unbounded, second shorter
        assertThat(SortedRangeSet.of(Range.greaterThan(BIGINT, 5L), Range.greaterThan(BIGINT, 10L)).getOrderedRanges())
                .containsExactly(Range.greaterThan(BIGINT, 5L));
        assertThat(SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 10L), Range.greaterThan(BIGINT, 10L)).getOrderedRanges())
                .containsExactly(Range.greaterThanOrEqual(BIGINT, 10L));
    }

    @Test
    public void testGetSingleValue()
    {
        assertThat(SortedRangeSet.of(BIGINT, 0L).getSingleValue()).isEqualTo(0L);
        assertThatThrownBy(() -> SortedRangeSet.all(BIGINT).getSingleValue())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("SortedRangeSet does not have just a single value");
    }

    @Test
    public void testSpan()
    {
        assertThatThrownBy(() -> SortedRangeSet.none(BIGINT).getSpan())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Cannot get span if no ranges exist");

        assertThat(SortedRangeSet.all(BIGINT).getSpan()).isEqualTo(Range.all(BIGINT));
        assertThat(SortedRangeSet.of(BIGINT, 0L).getSpan()).isEqualTo(Range.equal(BIGINT, 0L));
        assertThat(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).getSpan()).isEqualTo(Range.range(BIGINT, 0L, true, 1L, true));
        assertThat(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.greaterThan(BIGINT, 1L)).getSpan()).isEqualTo(Range.greaterThanOrEqual(BIGINT, 0L));
        assertThat(SortedRangeSet.of(Range.lessThan(BIGINT, 0L), Range.greaterThan(BIGINT, 1L)).getSpan()).isEqualTo(Range.all(BIGINT));
    }

    @Test
    public void testOverlaps()
    {
        assertThat(SortedRangeSet.all(BIGINT).overlaps(SortedRangeSet.all(BIGINT))).isTrue();
        assertThat(SortedRangeSet.all(BIGINT).overlaps(SortedRangeSet.none(BIGINT))).isFalse();
        assertThat(SortedRangeSet.all(BIGINT).overlaps(SortedRangeSet.of(BIGINT, 0L))).isTrue();
        assertThat(SortedRangeSet.all(BIGINT).overlaps(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)))).isTrue();
        assertThat(SortedRangeSet.all(BIGINT).overlaps(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)))).isTrue();
        assertThat(SortedRangeSet.all(BIGINT).overlaps(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L), Range.lessThan(BIGINT, 0L)))).isTrue();

        assertThat(SortedRangeSet.none(BIGINT).overlaps(SortedRangeSet.all(BIGINT))).isFalse();
        assertThat(SortedRangeSet.none(BIGINT).overlaps(SortedRangeSet.none(BIGINT))).isFalse();
        assertThat(SortedRangeSet.none(BIGINT).overlaps(SortedRangeSet.of(BIGINT, 0L))).isFalse();
        assertThat(SortedRangeSet.none(BIGINT).overlaps(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)))).isFalse();
        assertThat(SortedRangeSet.none(BIGINT).overlaps(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)))).isFalse();
        assertThat(SortedRangeSet.none(BIGINT).overlaps(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L), Range.lessThan(BIGINT, 0L)))).isFalse();

        assertThat(SortedRangeSet.of(BIGINT, 0L).overlaps(SortedRangeSet.all(BIGINT))).isTrue();
        assertThat(SortedRangeSet.of(BIGINT, 0L).overlaps(SortedRangeSet.none(BIGINT))).isFalse();
        assertThat(SortedRangeSet.of(BIGINT, 0L).overlaps(SortedRangeSet.of(BIGINT, 0L))).isTrue();
        assertThat(SortedRangeSet.of(BIGINT, 0L).overlaps(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)))).isTrue();
        assertThat(SortedRangeSet.of(BIGINT, 0L).overlaps(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)))).isFalse();
        assertThat(SortedRangeSet.of(BIGINT, 0L).overlaps(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L), Range.lessThan(BIGINT, 0L)))).isFalse();

        assertThat(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).overlaps(SortedRangeSet.of(Range.equal(BIGINT, 1L)))).isTrue();
        assertThat(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).overlaps(SortedRangeSet.of(Range.equal(BIGINT, 2L)))).isFalse();
        assertThat(SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 0L)).overlaps(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)))).isTrue();
        assertThat(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)).overlaps(SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 0L)))).isTrue();
        assertThat(SortedRangeSet.of(Range.lessThan(BIGINT, 0L)).overlaps(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)))).isFalse();
    }

    @Test
    public void testContains()
    {
        assertThat(SortedRangeSet.all(BIGINT).contains(SortedRangeSet.all(BIGINT))).isTrue();
        assertThat(SortedRangeSet.all(BIGINT).contains(SortedRangeSet.none(BIGINT))).isTrue();
        assertThat(SortedRangeSet.all(BIGINT).contains(SortedRangeSet.of(BIGINT, 0L))).isTrue();
        assertThat(SortedRangeSet.all(BIGINT).contains(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)))).isTrue();
        assertThat(SortedRangeSet.all(BIGINT).contains(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)))).isTrue();
        assertThat(SortedRangeSet.all(BIGINT).contains(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L), Range.lessThan(BIGINT, 0L)))).isTrue();

        assertThat(SortedRangeSet.none(BIGINT).contains(SortedRangeSet.all(BIGINT))).isFalse();
        assertThat(SortedRangeSet.none(BIGINT).contains(SortedRangeSet.none(BIGINT))).isTrue();
        assertThat(SortedRangeSet.none(BIGINT).contains(SortedRangeSet.of(BIGINT, 0L))).isFalse();
        assertThat(SortedRangeSet.none(BIGINT).contains(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)))).isFalse();
        assertThat(SortedRangeSet.none(BIGINT).contains(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)))).isFalse();
        assertThat(SortedRangeSet.none(BIGINT).contains(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L), Range.lessThan(BIGINT, 0L)))).isFalse();

        ValueSet valueSet = SortedRangeSet.of(BIGINT, 0L);
        assertThat(valueSet.contains(valueSet)).isTrue();

        assertThat(SortedRangeSet.of(BIGINT, 0L).contains(SortedRangeSet.all(BIGINT))).isFalse();
        assertThat(SortedRangeSet.of(BIGINT, 0L).contains(SortedRangeSet.none(BIGINT))).isTrue();
        assertThat(SortedRangeSet.of(BIGINT, 0L).contains(SortedRangeSet.of(BIGINT, 0L))).isTrue();
        assertThat(SortedRangeSet.of(BIGINT, 0L).contains(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)))).isFalse();
        assertThat(SortedRangeSet.of(BIGINT, 0L).contains(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)))).isFalse();
        assertThat(SortedRangeSet.of(BIGINT, 0L).contains(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L), Range.lessThan(BIGINT, 0L)))).isFalse();

        assertThat(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).contains(SortedRangeSet.of(Range.equal(BIGINT, 1L)))).isTrue();
        assertThat(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).contains(SortedRangeSet.of(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)))).isFalse();
        assertThat(SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 0L)).contains(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)))).isTrue();
        assertThat(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)).contains(SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 0L)))).isFalse();
        assertThat(SortedRangeSet.of(Range.lessThan(BIGINT, 0L)).contains(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)))).isFalse();

        Range rangeA = Range.range(BIGINT, 0L, true, 2L, true);
        Range rangeB = Range.range(BIGINT, 4L, true, 6L, true);
        Range rangeC = Range.range(BIGINT, 8L, true, 10L, true);
        assertThat(SortedRangeSet.of(rangeA, rangeB).contains(SortedRangeSet.of(rangeC))).isFalse();
        assertThat(SortedRangeSet.of(rangeB, rangeC).contains(SortedRangeSet.of(rangeA))).isFalse();
        assertThat(SortedRangeSet.of(rangeA, rangeC).contains(SortedRangeSet.of(rangeB))).isFalse();
        assertThat(SortedRangeSet.of(rangeA, rangeB).contains(SortedRangeSet.of(rangeB, rangeC))).isFalse();
        assertThat(SortedRangeSet.of(rangeA, rangeB, rangeC).contains(SortedRangeSet.of(rangeA))).isTrue();
        assertThat(SortedRangeSet.of(rangeA, rangeB, rangeC).contains(SortedRangeSet.of(rangeB))).isTrue();
        assertThat(SortedRangeSet.of(rangeA, rangeB, rangeC).contains(SortedRangeSet.of(rangeC))).isTrue();
        assertThat(SortedRangeSet.of(rangeA, rangeB, rangeC).contains(
                SortedRangeSet.of(Range.equal(BIGINT, 4L), Range.equal(BIGINT, 6L), Range.equal(BIGINT, 9L)))).isTrue();
        assertThat(SortedRangeSet.of(rangeA, rangeB, rangeC).contains(
                SortedRangeSet.of(Range.equal(BIGINT, 1L), Range.range(BIGINT, 6L, true, 10L, true)))).isFalse();
    }

    @Test
    public void testContainsValue()
    {
        // BIGINT all
        assertSortedRangeSet(SortedRangeSet.all(BIGINT))
                .containsValue(Long.MIN_VALUE)
                .containsValue(0L)
                .containsValue(42L)
                .containsValue(Long.MAX_VALUE);

        // BIGINT range
        assertSortedRangeSet(SortedRangeSet.of(Range.range(BIGINT, 10L, true, 41L, true)))
                .doesNotContainValue(9L)
                .containsValue(10L)
                .containsValue(11L)
                .containsValue(30L)
                .containsValue(41L)
                .doesNotContainValue(42L);

        assertSortedRangeSet(SortedRangeSet.of(Range.range(BIGINT, 10L, false, 41L, false)))
                .doesNotContainValue(10L)
                .containsValue(11L)
                .containsValue(40L)
                .doesNotContainValue(41L);

        // REAL all
        assertSortedRangeSet(SortedRangeSet.all(REAL))
                .containsValue((long) floatToRawIntBits(42.0f))
                .containsValue((long) floatToRawIntBits(Float.NaN));

        // REAL range
        assertSortedRangeSet(SortedRangeSet.of(Range.range(REAL, (long) floatToRawIntBits(10.0f), true, (long) floatToRawIntBits(41.0f), true)))
                .doesNotContainValue((long) floatToRawIntBits(9.999999f))
                .containsValue((long) floatToRawIntBits(10.0f))
                .containsValue((long) floatToRawIntBits(41.0f))
                .doesNotContainValue((long) floatToRawIntBits(41.00001f))
                .doesNotContainValue((long) floatToRawIntBits(Float.NaN));

        assertSortedRangeSet(SortedRangeSet.of(Range.range(REAL, (long) floatToRawIntBits(10.0f), false, (long) floatToRawIntBits(41.0f), false)))
                .doesNotContainValue((long) floatToRawIntBits(10.0f))
                .containsValue((long) floatToRawIntBits(10.00001f))
                .containsValue((long) floatToRawIntBits(40.99999f))
                .doesNotContainValue((long) floatToRawIntBits(41.0f))
                .doesNotContainValue((long) floatToRawIntBits(Float.NaN));

        // DOUBLE all
        assertSortedRangeSet(SortedRangeSet.all(DOUBLE))
                .containsValue(42.0)
                .containsValue(Double.NaN);

        // DOUBLE range
        assertSortedRangeSet(SortedRangeSet.of(Range.range(DOUBLE, 10.0, true, 41.0, true)))
                .doesNotContainValue(9.999999999999999)
                .containsValue(10.0)
                .containsValue(41.0)
                .doesNotContainValue(41.00000000000001)
                .doesNotContainValue(Double.NaN);

        assertSortedRangeSet(SortedRangeSet.of(Range.range(DOUBLE, 10.0, false, 41.0, false)))
                .doesNotContainValue(10.0)
                .containsValue(10.00000000000001)
                .containsValue(40.99999999999999)
                .doesNotContainValue(41.0)
                .doesNotContainValue(Double.NaN);
    }

    @Test
    public void testContainsValueRejectNull()
    {
        SortedRangeSet all = SortedRangeSet.all(BIGINT);
        SortedRangeSet none = SortedRangeSet.none(BIGINT);
        SortedRangeSet someRange = SortedRangeSet.of(Range.range(BIGINT, 10L, false, 41L, false));

        assertThatThrownBy(() -> all.containsValue(null))
                .hasMessage("value is null");
        assertThatThrownBy(() -> none.containsValue(null))
                .hasMessage("value is null");
        assertThatThrownBy(() -> someRange.containsValue(null))
                .hasMessage("value is null");
    }

    @Test
    public void testIntersect()
    {
        assertThat(SortedRangeSet.none(BIGINT).intersect(
                SortedRangeSet.none(BIGINT))).isEqualTo(SortedRangeSet.none(BIGINT));

        assertThat(SortedRangeSet.all(BIGINT).intersect(
                SortedRangeSet.all(BIGINT))).isEqualTo(SortedRangeSet.all(BIGINT));

        assertThat(SortedRangeSet.none(BIGINT).intersect(
                SortedRangeSet.all(BIGINT))).isEqualTo(SortedRangeSet.none(BIGINT));

        assertThat(SortedRangeSet.of(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L)).intersect(
                SortedRangeSet.of(Range.equal(BIGINT, 2L), Range.equal(BIGINT, 4L)))).isEqualTo(SortedRangeSet.of(Range.equal(BIGINT, 2L)));

        assertThat(SortedRangeSet.all(BIGINT).intersect(
                SortedRangeSet.of(Range.equal(BIGINT, 2L), Range.equal(BIGINT, 4L)))).isEqualTo(SortedRangeSet.of(Range.equal(BIGINT, 2L), Range.equal(BIGINT, 4L)));

        assertThat(SortedRangeSet.of(Range.range(BIGINT, 0L, true, 4L, false)).intersect(
                SortedRangeSet.of(Range.equal(BIGINT, 2L), Range.greaterThan(BIGINT, 3L)))).isEqualTo(SortedRangeSet.of(Range.equal(BIGINT, 2L), Range.range(BIGINT, 3L, false, 4L, false)));

        assertThat(SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 0L)).intersect(
                SortedRangeSet.of(Range.lessThanOrEqual(BIGINT, 0L)))).isEqualTo(SortedRangeSet.of(Range.equal(BIGINT, 0L)));

        assertThat(SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, -1L)).intersect(
                SortedRangeSet.of(Range.lessThanOrEqual(BIGINT, 1L)))).isEqualTo(SortedRangeSet.of(Range.range(BIGINT, -1L, true, 1L, true)));
    }

    @Test
    public void testUnion()
    {
        assertUnion(SortedRangeSet.none(BIGINT), SortedRangeSet.none(BIGINT), SortedRangeSet.none(BIGINT));
        assertUnion(SortedRangeSet.all(BIGINT), SortedRangeSet.all(BIGINT), SortedRangeSet.all(BIGINT));
        assertUnion(SortedRangeSet.none(BIGINT), SortedRangeSet.all(BIGINT), SortedRangeSet.all(BIGINT));

        assertUnion(
                SortedRangeSet.of(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)),
                SortedRangeSet.of(Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L)),
                SortedRangeSet.of(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L)));

        assertUnion(SortedRangeSet.all(BIGINT), SortedRangeSet.of(Range.equal(BIGINT, 0L)), SortedRangeSet.all(BIGINT));

        assertUnion(
                SortedRangeSet.of(Range.range(BIGINT, 0L, true, 4L, false)),
                SortedRangeSet.of(Range.greaterThan(BIGINT, 3L)),
                SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 0L)));

        assertUnion(
                SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 0L)),
                SortedRangeSet.of(Range.lessThanOrEqual(BIGINT, 0L)),
                SortedRangeSet.of(Range.all(BIGINT)));

        assertUnion(
                SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)),
                SortedRangeSet.of(Range.lessThan(BIGINT, 0L)),
                SortedRangeSet.of(BIGINT, 0L).complement());

        assertUnion(
                SortedRangeSet.of(Range.range(BIGINT, 0L, true, 10L, false)),
                SortedRangeSet.of(Range.equal(BIGINT, 9L)),
                SortedRangeSet.of(Range.range(BIGINT, 0L, true, 10L, false)));

        // two low-unbounded, first shorter
        assertUnion(
                SortedRangeSet.of(Range.lessThan(BIGINT, 5L)),
                SortedRangeSet.of(Range.lessThan(BIGINT, 10L)),
                SortedRangeSet.of(Range.lessThan(BIGINT, 10L)));
        assertUnion(
                SortedRangeSet.of(Range.lessThan(BIGINT, 10L)),
                SortedRangeSet.of(Range.lessThanOrEqual(BIGINT, 10L)),
                SortedRangeSet.of(Range.lessThanOrEqual(BIGINT, 10L)));

        // two low-unbounded, second shorter
        assertUnion(
                SortedRangeSet.of(Range.lessThan(BIGINT, 10L)),
                SortedRangeSet.of(Range.lessThan(BIGINT, 5L)),
                SortedRangeSet.of(Range.lessThan(BIGINT, 10L)));
        assertUnion(
                SortedRangeSet.of(Range.lessThanOrEqual(BIGINT, 10L)),
                SortedRangeSet.of(Range.lessThan(BIGINT, 10L)),
                SortedRangeSet.of(Range.lessThanOrEqual(BIGINT, 10L)));

        // two high-unbounded, first shorter
        assertUnion(
                SortedRangeSet.of(Range.greaterThan(BIGINT, 10L)),
                SortedRangeSet.of(Range.greaterThan(BIGINT, 5L)),
                SortedRangeSet.of(Range.greaterThan(BIGINT, 5L)));
        assertUnion(
                SortedRangeSet.of(Range.greaterThan(BIGINT, 10L)),
                SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 10L)),
                SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 10L)));

        // two high-unbounded, second shorter
        assertUnion(
                SortedRangeSet.of(Range.greaterThan(BIGINT, 5L)),
                SortedRangeSet.of(Range.greaterThan(BIGINT, 10L)),
                SortedRangeSet.of(Range.greaterThan(BIGINT, 5L)));
        assertUnion(
                SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 10L)),
                SortedRangeSet.of(Range.greaterThan(BIGINT, 10L)),
                SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 10L)));

        assertUnion(
                SortedRangeSet.of(Range.range(createVarcharType(25), utf8Slice("LARGE PLATED "), true, utf8Slice("LARGE PLATED!"), false)),
                SortedRangeSet.of(Range.equal(createVarcharType(25), utf8Slice("LARGE PLATED NICKEL"))),
                SortedRangeSet.of(Range.range(createVarcharType(25), utf8Slice("LARGE PLATED "), true, utf8Slice("LARGE PLATED!"), false)));
    }

    @Test
    public void testSubtract()
    {
        assertThat(SortedRangeSet.all(BIGINT).subtract(SortedRangeSet.all(BIGINT))).isEqualTo(SortedRangeSet.none(BIGINT));
        assertThat(SortedRangeSet.all(BIGINT).subtract(SortedRangeSet.none(BIGINT))).isEqualTo(SortedRangeSet.all(BIGINT));
        assertThat(SortedRangeSet.all(BIGINT).subtract(SortedRangeSet.of(BIGINT, 0L))).isEqualTo(SortedRangeSet.of(BIGINT, 0L).complement());
        assertThat(SortedRangeSet.all(BIGINT).subtract(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)))).isEqualTo(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).complement());
        assertThat(SortedRangeSet.all(BIGINT).subtract(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)))).isEqualTo(SortedRangeSet.of(Range.lessThanOrEqual(BIGINT, 0L)));

        assertThat(SortedRangeSet.none(BIGINT).subtract(SortedRangeSet.all(BIGINT))).isEqualTo(SortedRangeSet.none(BIGINT));
        assertThat(SortedRangeSet.none(BIGINT).subtract(SortedRangeSet.none(BIGINT))).isEqualTo(SortedRangeSet.none(BIGINT));
        assertThat(SortedRangeSet.none(BIGINT).subtract(SortedRangeSet.of(BIGINT, 0L))).isEqualTo(SortedRangeSet.none(BIGINT));
        assertThat(SortedRangeSet.none(BIGINT).subtract(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)))).isEqualTo(SortedRangeSet.none(BIGINT));
        assertThat(SortedRangeSet.none(BIGINT).subtract(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)))).isEqualTo(SortedRangeSet.none(BIGINT));

        assertThat(SortedRangeSet.of(BIGINT, 0L).subtract(SortedRangeSet.all(BIGINT))).isEqualTo(SortedRangeSet.none(BIGINT));
        assertThat(SortedRangeSet.of(BIGINT, 0L).subtract(SortedRangeSet.none(BIGINT))).isEqualTo(SortedRangeSet.of(BIGINT, 0L));
        assertThat(SortedRangeSet.of(BIGINT, 0L).subtract(SortedRangeSet.of(BIGINT, 0L))).isEqualTo(SortedRangeSet.none(BIGINT));
        assertThat(SortedRangeSet.of(BIGINT, 0L).subtract(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)))).isEqualTo(SortedRangeSet.none(BIGINT));
        assertThat(SortedRangeSet.of(BIGINT, 0L).subtract(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)))).isEqualTo(SortedRangeSet.of(BIGINT, 0L));

        assertThat(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).subtract(SortedRangeSet.all(BIGINT))).isEqualTo(SortedRangeSet.none(BIGINT));
        assertThat(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).subtract(SortedRangeSet.none(BIGINT))).isEqualTo(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)));
        assertThat(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).subtract(SortedRangeSet.of(BIGINT, 0L))).isEqualTo(SortedRangeSet.of(BIGINT, 1L));
        assertThat(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).subtract(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)))).isEqualTo(SortedRangeSet.none(BIGINT));
        assertThat(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).subtract(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)))).isEqualTo(SortedRangeSet.of(Range.equal(BIGINT, 0L)));

        assertThat(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)).subtract(SortedRangeSet.all(BIGINT))).isEqualTo(SortedRangeSet.none(BIGINT));
        assertThat(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)).subtract(SortedRangeSet.none(BIGINT))).isEqualTo(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)));
        assertThat(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)).subtract(SortedRangeSet.of(BIGINT, 0L))).isEqualTo(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)));
        assertThat(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)).subtract(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)))).isEqualTo(SortedRangeSet.of(Range.range(BIGINT, 0L, false, 1L, false), Range.greaterThan(BIGINT, 1L)));
        assertThat(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)).subtract(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)))).isEqualTo(SortedRangeSet.none(BIGINT));
    }

    @Test
    public void testJsonSerialization()
            throws Exception
    {
        TestingTypeManager typeManager = new TestingTypeManager();
        TestingBlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();

        ObjectMapper mapper = new ObjectMapperProvider().get()
                .registerModule(new SimpleModule()
                        .addDeserializer(Type.class, new TestingTypeDeserializer(typeManager))
                        .addSerializer(Block.class, new TestingBlockJsonSerde.Serializer(blockEncodingSerde))
                        .addDeserializer(Block.class, new TestingBlockJsonSerde.Deserializer(blockEncodingSerde)));

        SortedRangeSet set = SortedRangeSet.all(BIGINT);
        assertThat(set).isEqualTo(mapper.readValue(mapper.writeValueAsString(set), SortedRangeSet.class));

        set = SortedRangeSet.none(DOUBLE);
        assertThat(set).isEqualTo(mapper.readValue(mapper.writeValueAsString(set), SortedRangeSet.class));

        set = SortedRangeSet.of(VARCHAR, utf8Slice("abc"));
        assertThat(set).isEqualTo(mapper.readValue(mapper.writeValueAsString(set), SortedRangeSet.class));

        set = SortedRangeSet.of(Range.equal(BOOLEAN, true), Range.equal(BOOLEAN, false));
        assertThat(set).isEqualTo(mapper.readValue(mapper.writeValueAsString(set), SortedRangeSet.class));
    }

    @Test
    public void testExpandRangesForDenseType()
    {
        for (Type type : Arrays.asList(BIGINT, INTEGER, SMALLINT, TINYINT, createDecimalType(2))) {
            assertThat(ValueSet.ofRanges(Range.equal(type, 1L))
                    .tryExpandRanges(0))
                    .isEqualTo(Optional.empty());

            assertThat(ValueSet.none(type)
                    .tryExpandRanges(0))
                    .isEqualTo(Optional.of(List.of()));

            assertThat(ValueSet.ofRanges(Range.range(type, 1L, true, 5L, true))
                    .tryExpandRanges(10))
                    .isEqualTo(Optional.of(List.of(1L, 2L, 3L, 4L, 5L)));

            assertThat(ValueSet.of(type, 1L, 2L, 3L, 4L, 5L)
                    .tryExpandRanges(10))
                    .isEqualTo(Optional.of(List.of(1L, 2L, 3L, 4L, 5L)));

            type.getRange().ifPresent(range -> {
                long min = (long) range.getMin();

                assertThat(ValueSet.ofRanges(Range.range(type, min, true, min + 3, true))
                        .tryExpandRanges(10))
                        .isEqualTo(Optional.of(List.of(min, min + 1, min + 2, min + 3)));
                assertThat(ValueSet.ofRanges(Range.lessThan(type, min + 4))
                        .tryExpandRanges(10))
                        .isEqualTo(Optional.of(List.of(min, min + 1, min + 2, min + 3)));
                assertThat(ValueSet.ofRanges(Range.lessThanOrEqual(type, min + 3))
                        .tryExpandRanges(10))
                        .isEqualTo(Optional.of(List.of(min, min + 1, min + 2, min + 3)));

                long max = (long) range.getMax();

                assertThat(ValueSet.ofRanges(Range.range(type, max - 3, true, max, true))
                        .tryExpandRanges(10))
                        .isEqualTo(Optional.of(List.of(max - 3, max - 2, max - 1, max)));
                assertThat(ValueSet.ofRanges(Range.greaterThan(type, max - 4))
                        .tryExpandRanges(10))
                        .isEqualTo(Optional.of(List.of(max - 3, max - 2, max - 1, max)));
                assertThat(ValueSet.ofRanges(Range.greaterThanOrEqual(type, max - 3))
                        .tryExpandRanges(10))
                        .isEqualTo(Optional.of(List.of(max - 3, max - 2, max - 1, max)));
            });

            assertThat(ValueSet.ofRanges(Range.range(type, 1L, true, 5L, true))
                    .tryExpandRanges(10))
                    .isEqualTo(Optional.of(List.of(1L, 2L, 3L, 4L, 5L)));

            assertThat(ValueSet.ofRanges(Range.range(type, 1L, true, 5L, true))
                    .tryExpandRanges(5))
                    .isEqualTo(Optional.of(List.of(1L, 2L, 3L, 4L, 5L)));

            assertThat(ValueSet.ofRanges(Range.range(type, 1L, true, 5L, false))
                    .tryExpandRanges(5))
                    .isEqualTo(Optional.of(List.of(1L, 2L, 3L, 4L)));

            assertThat(ValueSet.ofRanges(Range.range(type, 1L, true, 6L, true))
                    .tryExpandRanges(5))
                    .isEqualTo(Optional.empty());

            assertThat(ValueSet.ofRanges(Range.range(type, 1L, true, 6L, false))
                    .tryExpandRanges(5))
                    .isEqualTo(Optional.of(List.of(1L, 2L, 3L, 4L, 5L)));

            assertThat(ValueSet.ofRanges(Range.range(type, 1L, false, 5L, true))
                    .tryExpandRanges(5))
                    .isEqualTo(Optional.of(List.of(2L, 3L, 4L, 5L)));

            assertThat(ValueSet.ofRanges(Range.range(type, 1L, true, 5L, false))
                    .tryExpandRanges(5))
                    .isEqualTo(Optional.of(List.of(1L, 2L, 3L, 4L)));

            assertThat(ValueSet.ofRanges(Range.range(type, 1L, false, 5L, false))
                    .tryExpandRanges(5))
                    .isEqualTo(Optional.of(List.of(2L, 3L, 4L)));

            assertThat(ValueSet.ofRanges(Range.range(type, 1L, false, 2L, false))
                    .tryExpandRanges(5))
                    .isEqualTo(Optional.of(List.of()));

            assertThat(ValueSet.ofRanges(Range.range(type, 1L, false, 3L, false))
                    .tryExpandRanges(5))
                    .isEqualTo(Optional.of(List.of(2L)));

            assertThat(ValueSet.ofRanges(Range.range(type, 1L, true, 5L, true))
                    .tryExpandRanges(3))
                    .isEqualTo(Optional.empty());

            assertThat(ValueSet.of(type, 1L, 2L, 3L, 4L, 5L)
                    .tryExpandRanges(3))
                    .isEqualTo(Optional.empty());

            assertThat(ValueSet.ofRanges(Range.greaterThan(type, 1L))
                    .tryExpandRanges(3))
                    .isEqualTo(Optional.empty());

            assertThat(ValueSet.ofRanges(Range.greaterThanOrEqual(type, 1L))
                    .tryExpandRanges(3))
                    .isEqualTo(Optional.empty());

            assertThat(ValueSet.ofRanges(Range.lessThan(type, 1L))
                    .tryExpandRanges(3))
                    .isEqualTo(Optional.empty());

            assertThat(ValueSet.ofRanges(Range.lessThanOrEqual(type, 1L))
                    .tryExpandRanges(3))
                    .isEqualTo(Optional.empty());
        }
    }

    private void assertUnion(SortedRangeSet first, SortedRangeSet second, SortedRangeSet expected)
    {
        assertThat(first.union(second)).isEqualTo(expected);
        assertThat(first.union(ImmutableList.of(first, second))).isEqualTo(expected);
    }

    private static SortedRangeSetAssert assertSortedRangeSet(SortedRangeSet sortedRangeSet)
    {
        return assertThat((AssertProvider<SortedRangeSetAssert>) () -> new SortedRangeSetAssert(sortedRangeSet));
    }

    private static class SortedRangeSetAssert
    {
        private final SortedRangeSet sortedRangeSet;

        public SortedRangeSetAssert(SortedRangeSet sortedRangeSet)
        {
            this.sortedRangeSet = requireNonNull(sortedRangeSet, "sortedRangeSet is null");
        }

        public SortedRangeSetAssert containsValue(Object value)
        {
            if (!sortedRangeSet.containsValue(value)) {
                throw new AssertionError(format("Expected %s to contain %s", sortedRangeSet, value));
            }
            return this;
        }

        public SortedRangeSetAssert doesNotContainValue(Object value)
        {
            if (sortedRangeSet.containsValue(value)) {
                throw new AssertionError(format("Expected %s not to contain %s", sortedRangeSet, value));
            }
            return this;
        }
    }
}
