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
package io.trino.cache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.connector.ColumnHandle;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ChooseAlternativeNode.FilteredTableScan;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.tree.Expression;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * This class provides a common subplan (shared between different subplans in a query) and a way
 * to adapt it to original plan.
 */
public class CommonPlanAdaptation
{
    /**
     * Common subplan (shared between different subplans in a query)
     */
    private final PlanNode commonSubplan;
    /**
     * Signature of common subplan.
     */
    private final PlanSignature commonSubplanSignature;
    /**
     * Common subplan {@link FilteredTableScan}.
     */
    private final FilteredTableScan commonSubplanFilteredTableScan;
    /**
     * Dynamic filter disjuncts from all common subplans.
     */
    private final Expression commonDynamicFilterDisjuncts;
    /**
     * Mapping from {@link CacheColumnId} to relevant (for plan signature) dynamic filtering columns.
     */
    private final Map<CacheColumnId, ColumnHandle> dynamicFilterColumnMapping;
    /**
     * Optional predicate that needs to be applied in order to adapt common subplan to
     * original plan.
     */
    private final Optional<Expression> adaptationPredicate;
    /**
     * Optional projections that need to applied in order to adapt common subplan
     * to original plan.
     */
    private final Optional<Assignments> adaptationAssignments;
    /**
     * Mapping between {@link CacheColumnId} and symbols.
     */
    private final Map<CacheColumnId, Symbol> columnIdMapping;
    /**
     * Adaptation conjuncts with symbol names canonicalized as {@link CacheColumnId}.
     */
    private final List<Expression> canonicalAdaptationConjuncts;

    public CommonPlanAdaptation(
            PlanNode commonSubplan,
            PlanSignature commonSubplanSignature,
            CommonPlanAdaptation childAdaptation,
            Optional<Expression> adaptationPredicate,
            Optional<Assignments> adaptationAssignments,
            Map<CacheColumnId, Symbol> columnIdMapping,
            List<Expression> canonicalAdaptationConjuncts)
    {
        this(
                commonSubplan,
                commonSubplanSignature,
                childAdaptation.getCommonSubplanFilteredTableScan(),
                childAdaptation.getCommonDynamicFilterDisjuncts(),
                childAdaptation.getDynamicFilterColumnMapping(),
                adaptationPredicate,
                adaptationAssignments,
                columnIdMapping,
                canonicalAdaptationConjuncts);
    }

    public CommonPlanAdaptation(
            PlanNode commonSubplan,
            PlanSignature commonSubplanSignature,
            FilteredTableScan commonSubplanFilteredTableScan,
            Expression commonDynamicFilterDisjuncts,
            Map<CacheColumnId, ColumnHandle> dynamicFilterColumnMapping,
            Optional<Expression> adaptationPredicate,
            Optional<Assignments> adaptationAssignments,
            Map<CacheColumnId, Symbol> columnIdMapping,
            List<Expression> canonicalAdaptationConjuncts)
    {
        this.commonSubplan = requireNonNull(commonSubplan, "commonSubplan is null");
        this.commonSubplanSignature = requireNonNull(commonSubplanSignature, "commonSubplanSignature is null");
        this.commonSubplanFilteredTableScan = requireNonNull(commonSubplanFilteredTableScan, "commonSubplanFilteredTableScan is null");
        this.commonDynamicFilterDisjuncts = requireNonNull(commonDynamicFilterDisjuncts, "commonDynamicFilterDisjuncts is null");
        this.dynamicFilterColumnMapping = requireNonNull(dynamicFilterColumnMapping, "dynamicFilterColumnMapping is null");
        this.adaptationPredicate = requireNonNull(adaptationPredicate, "adaptationPredicate is null");
        this.adaptationAssignments = requireNonNull(adaptationAssignments, "adaptationAssignments is null");
        this.columnIdMapping = ImmutableMap.copyOf(requireNonNull(columnIdMapping, "columnIdMapping is null"));
        this.canonicalAdaptationConjuncts = ImmutableList.copyOf(requireNonNull(canonicalAdaptationConjuncts, "canonicalAdaptationConjuncts is null"));
    }

    public PlanNode adaptCommonSubplan(PlanNode commonSubplan, PlanNodeIdAllocator idAllocator)
    {
        checkArgument(this.commonSubplan.getOutputSymbols().equals(commonSubplan.getOutputSymbols()));
        PlanNode adaptedPlan = commonSubplan;
        if (adaptationPredicate.isPresent()) {
            adaptedPlan = new FilterNode(
                    idAllocator.getNextId(),
                    adaptedPlan,
                    adaptationPredicate.get());
        }
        if (adaptationAssignments.isPresent()) {
            adaptedPlan = new ProjectNode(
                    idAllocator.getNextId(),
                    adaptedPlan,
                    adaptationAssignments.get());
        }
        return adaptedPlan;
    }

    public PlanNode getCommonSubplan()
    {
        return commonSubplan;
    }

    public PlanSignature getCommonSubplanSignature()
    {
        return commonSubplanSignature;
    }

    public FilteredTableScan getCommonSubplanFilteredTableScan()
    {
        return commonSubplanFilteredTableScan;
    }

    public Expression getCommonDynamicFilterDisjuncts()
    {
        return commonDynamicFilterDisjuncts;
    }

    public Map<CacheColumnId, ColumnHandle> getDynamicFilterColumnMapping()
    {
        return dynamicFilterColumnMapping;
    }

    public Map<CacheColumnId, Symbol> getColumnIdMapping()
    {
        return columnIdMapping;
    }

    public List<Expression> getCanonicalAdaptationConjuncts()
    {
        return canonicalAdaptationConjuncts;
    }
}
