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
package io.trino.sql.ir.optimizer.rule;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;

/**
 * Simplifies logical expression containing terms and negations of those terms.
 * E.g.,
 * <ul>
 *     <li>{@code And(x, $not(x), ...) -> And(x, $is_null(x), ...)}
 *     <li>{@code Or(x, $not(x), ...) -> Or(x, $not($is_null(x), ...)}
 * </ul>
 */
public class EvaluateComplementaryLogicalTerms
        implements IrOptimizerRule
{
    private final Metadata metadata;

    public EvaluateComplementaryLogicalTerms(PlannerContext context)
    {
        this.metadata = context.getMetadata();
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Logical logical)) {
            return Optional.empty();
        }

        Set<Expression> positives = new HashSet<>();
        Set<Call> negatives = new HashSet<>();

        List<Expression> newTerms = new ArrayList<>();

        for (Expression term : logical.terms()) {
            if (!isDeterministic(term)) {
                newTerms.add(term);
            }
            else if (term instanceof Call not && not.function().name().equals(builtinFunctionName("$not"))) {
                negatives.add(not);
            }
            else {
                positives.add(term);
            }
        }

        if (newTerms.size() == logical.terms().size()) {
            return Optional.empty();
        }

        boolean changed = false;
        for (Call negative : negatives) {
            Expression positive = negative.arguments().getFirst();
            if (positives.contains(positive)) {
                changed = true;
                newTerms.add(switch (logical.operator()) {
                    case AND -> new Logical(AND, ImmutableList.of(positive, new IsNull(positive)));
                    case OR -> new Logical(OR, ImmutableList.of(positive, not(metadata, new IsNull(positive))));
                });
                positives.remove(positive);
            }
            else {
                newTerms.add(negative);
            }
        }

        if (!changed) {
            return Optional.empty();
        }

        // add back all unmatched terms
        newTerms.addAll(positives);

        if (newTerms.size() == 1) {
            return Optional.of(newTerms.getFirst());
        }

        return Optional.of(new Logical(logical.operator(), newTerms));
    }
}
