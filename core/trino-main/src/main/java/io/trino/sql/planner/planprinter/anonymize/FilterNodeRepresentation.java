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

package io.trino.sql.planner.planprinter.anonymize;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.planprinter.TypedSymbol;

import java.util.List;
import java.util.Objects;

import static io.trino.sql.ExpressionUtils.unResolveFunctions;
import static io.trino.sql.planner.planprinter.anonymize.AnonymizationUtils.anonymize;
import static java.util.Objects.requireNonNull;

public class FilterNodeRepresentation
        extends AnonymizedNodeRepresentation
{
    private final String predicate;

    @JsonCreator
    public FilterNodeRepresentation(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("outputLayout") List<TypedSymbol> outputLayout,
            @JsonProperty("sources") List<AnonymizedNodeRepresentation> sources,
            @JsonProperty("predicate") String predicate)
    {
        super(id, outputLayout, sources);
        this.predicate = requireNonNull(predicate, "predicate is null");
    }

    @JsonProperty
    public String getPredicate()
    {
        return predicate;
    }

    public static FilterNodeRepresentation fromPlanNode(FilterNode node, TypeProvider typeProvider, List<AnonymizedNodeRepresentation> sources)
    {
        return new FilterNodeRepresentation(
                node.getId(),
                anonymize(node.getOutputSymbols(), typeProvider),
                sources,
                anonymize(unResolveFunctions(node.getPredicate())).toString());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FilterNodeRepresentation)) {
            return false;
        }
        FilterNodeRepresentation that = (FilterNodeRepresentation) o;
        return getId().equals(that.getId())
                && getOutputLayout().equals(that.getOutputLayout())
                && getSources().equals(that.getSources())
                && predicate.equals(that.predicate);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getId(), getOutputLayout(), getSources(), predicate);
    }
}
