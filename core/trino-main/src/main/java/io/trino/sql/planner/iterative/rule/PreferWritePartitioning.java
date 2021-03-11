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
package io.trino.sql.planner.iterative.rule;

import io.trino.Session;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.TableWriterNode;

import java.util.Optional;

import static io.trino.SystemSessionProperties.getPreferredWritePartitioningMinNumberOfPartitions;
import static io.trino.SystemSessionProperties.isUsePreferredWritePartitioning;
import static io.trino.cost.AggregationStatsRule.getRowsCount;
import static io.trino.sql.planner.plan.Patterns.tableWriterNode;
import static java.lang.Double.isNaN;

public class PreferWritePartitioning
        implements Rule<TableWriterNode>
{
    @Override
    public Pattern<TableWriterNode> getPattern()
    {
        return tableWriterNode();
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isUsePreferredWritePartitioning(session);
    }

    @Override
    public Result apply(TableWriterNode node, Captures captures, Context context)
    {
        if (node.getPreferredPartitioningScheme().isEmpty()) {
            return Result.empty();
        }

        int minimumNumberOfPartitions = getPreferredWritePartitioningMinNumberOfPartitions(context.getSession());
        if (minimumNumberOfPartitions <= 1) {
            // Force 'preferred write partitioning' even if stats are missing or broken
            return enable(node);
        }

        double expectedNumberOfPartitions = getRowsCount(
                context.getStatsProvider().getStats(node.getSource()),
                node.getPreferredPartitioningScheme().get().getPartitioning().getColumns());

        if (isNaN(expectedNumberOfPartitions) || expectedNumberOfPartitions < minimumNumberOfPartitions) {
            return Result.empty();
        }

        return enable(node);
    }

    private Result enable(TableWriterNode node)
    {
        return Result.ofPlanNode(new TableWriterNode(
                node.getId(),
                node.getSource(),
                node.getTarget(),
                node.getRowCountSymbol(),
                node.getFragmentSymbol(),
                node.getColumns(),
                node.getColumnNames(),
                node.getNotNullColumnSymbols(),
                node.getPreferredPartitioningScheme(),
                Optional.empty(),
                node.getStatisticsAggregation(),
                node.getStatisticsAggregationDescriptor()));
    }
}
