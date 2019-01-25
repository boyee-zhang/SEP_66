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
package io.prestosql.sql.query;

import org.junit.jupiter.api.extension.ExtendWith;

import static io.prestosql.SystemSessionProperties.USE_MARK_DISTINCT;
import static io.prestosql.testing.TestingSession.testSessionBuilder;

@ExtendWith(TestDistinctAggregationsNoMarkDistinct.WithDisabledMarkDistinctQueryAssertionExtension.class)
public class TestDistinctAggregationsNoMarkDistinct
        implements BaseDistinctAggregationsTest
{
    public static class WithDisabledMarkDistinctQueryAssertionExtension
            extends QueryAssertionsExtension
    {
        public WithDisabledMarkDistinctQueryAssertionExtension()
        {
            super(() -> new QueryAssertions(testSessionBuilder()
                    .setSystemProperty(USE_MARK_DISTINCT, "false")
                    .build()));
        }
    }
}
