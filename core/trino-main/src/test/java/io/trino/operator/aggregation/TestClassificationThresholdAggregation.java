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
package io.trino.operator.aggregation;

import java.util.ArrayList;
import java.util.Iterator;

public class TestClassificationThresholdAggregation
        extends TestPrecisionRecallAggregation
{
    public TestClassificationThresholdAggregation()
    {
        super("classification_thresholds");
    }

    @Override
    protected ArrayList<Double> getExpectedValue(int start, int length)
    {
        final Iterator<BucketResult> iterator =
                TestPrecisionRecallAggregation.getResultsIterator(start, length);
        final ArrayList<Double> expected = new ArrayList<>();
        while (iterator.hasNext()) {
            final BucketResult result = iterator.next();
            expected.add((result.left + result.right) / 2);
        }
        return expected;
    }
}
