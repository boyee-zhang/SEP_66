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
package io.prestosql.operator.scalar.date;

import io.prestosql.operator.scalar.AbstractTestExtract;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestExtract
        extends AbstractTestExtract
{
    @Override
    protected List<String> types()
    {
        return List.of("date");
    }

    @Override
    public void testYear()
    {
        assertThat(assertions.expression("EXTRACT(YEAR FROM DATE '2020-05-10')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM DATE '1960-05-10')")).matches("BIGINT '1960'");
    }

    @Override
    public void testMonth()
    {
        assertThat(assertions.expression("EXTRACT(MONTH FROM DATE '2020-05-10')")).matches("BIGINT '5'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM DATE '1960-05-10')")).matches("BIGINT '5'");
    }

    @Override
    public void testDay()
    {
        assertThat(assertions.expression("EXTRACT(DAY FROM DATE '2020-05-10')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM DATE '1960-05-10')")).matches("BIGINT '10'");
    }

    @Override
    public void testDayOfWeek()
    {
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM DATE '2020-05-10')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM DATE '1960-05-10')")).matches("BIGINT '2'");
    }

    @Override
    public void testDayOfYear()
    {
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM DATE '2020-05-10')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM DATE '1960-05-10')")).matches("BIGINT '131'");
    }

    @Override
    public void testQuarter()
    {
        assertThat(assertions.expression("EXTRACT(QUARTER FROM DATE '2020-05-10')")).matches("BIGINT '2'");
        assertThat(assertions.expression("EXTRACT(QUARTER FROM DATE '1960-05-10')")).matches("BIGINT '2'");
    }

    @Override
    public void testYearOfWeek()
    {
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM DATE '2020-05-10')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM DATE '1960-05-10')")).matches("BIGINT '1960'");
    }
}
