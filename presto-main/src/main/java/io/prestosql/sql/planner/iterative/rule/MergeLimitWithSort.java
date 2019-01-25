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
package io.prestosql.sql.planner.iterative.rule;

import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.iterative.TraitSet;
import io.prestosql.sql.planner.plan.LimitNode;
import io.prestosql.sql.planner.plan.SortNode;
import io.prestosql.sql.planner.plan.TopNNode;

import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.plan.Patterns.limit;
import static io.prestosql.sql.planner.plan.Patterns.sort;
import static io.prestosql.sql.planner.plan.Patterns.source;

public class MergeLimitWithSort
        implements Rule<LimitNode>
{
    private static final Capture<SortNode> CHILD = newCapture();

    private static final Pattern<LimitNode> PATTERN = limit()
            .with(source().matching(sort().capturedAs(CHILD)));

    @Override
    public Pattern<LimitNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(LimitNode parent, Captures captures, TraitSet traitSet, Context context)
    {
        SortNode child = captures.get(CHILD);

        return Result.ofPlanNode(
                new TopNNode(
                        parent.getId(),
                        child.getSource(),
                        parent.getCount(),
                        child.getOrderingScheme(),
                        parent.isPartial() ? TopNNode.Step.PARTIAL : TopNNode.Step.SINGLE));
    }
}
