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

import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.iterative.TraitSet;
import io.prestosql.sql.planner.plan.PlanNode;

import static java.util.Objects.requireNonNull;

public final class SimpleRule<T extends PlanNode>
        implements Rule<T>
{
    public static <T extends PlanNode> Rule<T> rule(Pattern<T> pattern, ApplyFunction<T> applyFunction)
    {
        return new SimpleRule<>(pattern, applyFunction);
    }

    private final Pattern<T> pattern;
    private final ApplyFunction<T> applyFunction;

    private SimpleRule(Pattern<T> pattern, ApplyFunction<T> applyFunction)
    {
        this.pattern = requireNonNull(pattern, "pattern is null");
        this.applyFunction = requireNonNull(applyFunction, "applyFunction is null");
    }

    @Override
    public final Pattern<T> getPattern()
    {
        return pattern;
    }

    @Override
    public final Result apply(T node, Captures captures, TraitSet traitSet, Context context)
    {
        return applyFunction.apply(node, captures, traitSet, context);
    }

    @FunctionalInterface
    public interface ApplyFunction<T extends PlanNode>
    {
        Result apply(T node, Captures captures, TraitSet traitSet, Context context);
    }
}
