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
package io.trino.sql.rewrite;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.cost.CachingStatsProvider;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.StatsCalculator;
import io.trino.cost.SymbolStatsEstimate;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.security.AccessControl;
import io.trino.spi.security.GroupProvider;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.sql.QueryUtil;
import io.trino.sql.analyzer.QueryExplainer;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SelectItem;
import io.trino.sql.tree.ShowStats;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.TableSubquery;
import io.trino.sql.tree.Values;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.PREFER_PARTIAL_AGGREGATION;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.QueryUtil.aliased;
import static io.trino.sql.QueryUtil.query;
import static io.trino.sql.QueryUtil.selectAll;
import static io.trino.sql.QueryUtil.selectList;
import static io.trino.sql.QueryUtil.simpleQuery;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static java.lang.Double.isFinite;
import static java.lang.Math.round;
import static java.util.Objects.requireNonNull;

public class ShowStatsRewrite
        implements StatementRewrite.Rewrite
{
    private static final Expression NULL_DOUBLE = new Cast(new NullLiteral(), toSqlType(DOUBLE));
    private static final Expression NULL_VARCHAR = new Cast(new NullLiteral(), toSqlType(VARCHAR));

    @Override
    public Statement rewrite(
            Session session,
            Metadata metadata,
            SqlParser parser,
            Optional<QueryExplainer> queryExplainer,
            Statement node,
            List<Expression> parameters,
            Map<NodeRef<Parameter>, Expression> parameterLookup,
            GroupProvider groupProvider,
            AccessControl accessControl,
            WarningCollector warningCollector,
            StatsCalculator statsCalculator)
    {
        return (Statement) new Visitor(session, parameters, queryExplainer, warningCollector, statsCalculator).process(node, null);
    }

    private static class Visitor
            extends AstVisitor<Node, Void>
    {
        private final Session session;
        private final List<Expression> parameters;
        private final Optional<QueryExplainer> queryExplainer;
        private final WarningCollector warningCollector;
        private final StatsCalculator statsCalculator;

        private Visitor(Session session, List<Expression> parameters, Optional<QueryExplainer> queryExplainer, WarningCollector warningCollector, StatsCalculator statsCalculator)
        {
            this.session = requireNonNull(session, "session is null");
            this.parameters = requireNonNull(parameters, "parameters is null");
            this.queryExplainer = requireNonNull(queryExplainer, "queryExplainer is null");
            this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
            this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        }

        @Override
        protected Node visitShowStats(ShowStats node, Void context)
        {
            checkState(queryExplainer.isPresent(), "Query explainer must be provided for SHOW STATS SELECT");

            Query query = getRelation(node);
            QuerySpecification specification = (QuerySpecification) query.getQueryBody();
            Plan plan = queryExplainer.get().getLogicalPlan(disablePartialAggregation(session), query(specification), parameters, warningCollector);
            CachingStatsProvider cachingStatsProvider = new CachingStatsProvider(statsCalculator, session, plan.getTypes());
            PlanNodeStatsEstimate stats = cachingStatsProvider.getStats(plan.getRoot());
            return rewriteShowStats(plan, stats);
        }

        private Query getRelation(ShowStats node)
        {
            if (node.getRelation() instanceof Table) {
                return simpleQuery(selectList(new AllColumns()), node.getRelation());
            }
            if (node.getRelation() instanceof TableSubquery) {
                return ((TableSubquery) node.getRelation()).getQuery();
            }
            throw new IllegalArgumentException("Expected either TableSubquery or Table as relation");
        }

        private Node rewriteShowStats(Plan plan, PlanNodeStatsEstimate planNodeStatsEstimate)
        {
            List<String> statsColumnNames = buildColumnsNames();
            List<SelectItem> selectItems = buildSelectItems(statsColumnNames);
            ImmutableList.Builder<Expression> rowsBuilder = ImmutableList.builder();
            verify(plan.getRoot() instanceof OutputNode, "Expected plan root be OutputNode, but was: %s", plan.getRoot().getClass().getName());
            OutputNode root = (OutputNode) plan.getRoot();
            for (int columnIndex = 0; columnIndex < root.getOutputSymbols().size(); columnIndex++) {
                Symbol outputSymbol = root.getOutputSymbols().get(columnIndex);
                String columnName = root.getColumnNames().get(columnIndex);
                Type columnType = plan.getTypes().get(outputSymbol);
                SymbolStatsEstimate symbolStatistics = planNodeStatsEstimate.getSymbolStatistics(outputSymbol);
                ImmutableList.Builder<Expression> rowValues = ImmutableList.builder();
                rowValues.add(new StringLiteral(columnName));
                rowValues.add(toDoubleLiteral(symbolStatistics.getAverageRowSize() * planNodeStatsEstimate.getOutputRowCount() * (1 - symbolStatistics.getNullsFraction())));
                rowValues.add(toDoubleLiteral(symbolStatistics.getDistinctValuesCount()));
                rowValues.add(toDoubleLiteral(symbolStatistics.getNullsFraction()));
                rowValues.add(NULL_DOUBLE);
                rowValues.add(toStringLiteral(columnType, symbolStatistics.getLowValue()));
                rowValues.add(toStringLiteral(columnType, symbolStatistics.getHighValue()));
                rowsBuilder.add(new Row(rowValues.build()));
            }
            // Stats for whole table
            ImmutableList.Builder<Expression> rowValues = ImmutableList.builder();
            rowValues.add(NULL_VARCHAR);
            rowValues.add(NULL_DOUBLE);
            rowValues.add(NULL_DOUBLE);
            rowValues.add(NULL_DOUBLE);
            rowValues.add(toDoubleLiteral(planNodeStatsEstimate.getOutputRowCount()));
            rowValues.add(NULL_VARCHAR);
            rowValues.add(NULL_VARCHAR);
            rowsBuilder.add(new Row(rowValues.build()));
            List<Expression> resultRows = rowsBuilder.build();

            return simpleQuery(selectAll(selectItems), aliased(new Values(resultRows), "table_stats", statsColumnNames));
        }

        @Override
        protected Node visitNode(Node node, Void context)
        {
            return node;
        }

        private static Session disablePartialAggregation(Session session)
        {
            // Disabling prefer_partial_aggregation allows estimates
            // to be propagated through the aggregate node.
            return Session.builderFromTransaction(session)
                    .setSystemProperty(PREFER_PARTIAL_AGGREGATION, "false")
                    .build();
        }

        private static List<String> buildColumnsNames()
        {
            return ImmutableList.<String>builder()
                    .add("column_name")
                    .add("data_size")
                    .add("distinct_values_count")
                    .add("nulls_fraction")
                    .add("row_count")
                    .add("low_value")
                    .add("high_value")
                    .build();
        }

        private static List<SelectItem> buildSelectItems(List<String> columnNames)
        {
            return columnNames.stream()
                    .map(QueryUtil::unaliasedName)
                    .collect(toImmutableList());
        }

        private static Expression toStringLiteral(Type type, double value)
        {
            if (!isFinite(value)) {
                return NULL_VARCHAR;
            }
            if (type.equals(BigintType.BIGINT) || type.equals(IntegerType.INTEGER) || type.equals(SmallintType.SMALLINT) || type.equals(TinyintType.TINYINT)) {
                return new StringLiteral(Long.toString(round(value)));
            }
            if (type.equals(DOUBLE) || type instanceof DecimalType) {
                return new StringLiteral(Double.toString(value));
            }
            if (type.equals(RealType.REAL)) {
                return new StringLiteral(Float.toString((float) value));
            }
            if (type.equals(DATE)) {
                return new StringLiteral(LocalDate.ofEpochDay(round(value)).toString());
            }
            throw new IllegalArgumentException("Unexpected type: " + type);
        }
    }

    private static Expression toDoubleLiteral(double value)
    {
        if (!isFinite(value)) {
            return NULL_DOUBLE;
        }
        return new DoubleLiteral(Double.toString(value));
    }
}
