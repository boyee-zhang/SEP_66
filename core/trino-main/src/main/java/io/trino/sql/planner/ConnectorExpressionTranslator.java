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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.metadata.LiteralFunction;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorCast;
import io.trino.spi.expression.ConnectorComparison;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.ConnectorLogicalExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarcharType;
import io.trino.sql.ExpressionUtils;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.TypeSignatureProvider;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CharLiteral;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SymbolReference;
import io.trino.type.JoniRegexp;
import io.trino.type.Re2JRegexp;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.trino.SystemSessionProperties.isComplexExpressionPushdown;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.type.LikeFunctions.LIKE_PATTERN_FUNCTION_NAME;
import static java.util.Objects.requireNonNull;

public final class ConnectorExpressionTranslator
{
    private ConnectorExpressionTranslator() {}

    public static Expression translate(Session session, ConnectorExpression expression, PlannerContext plannerContext, Map<String, Symbol> variableMappings, LiteralEncoder literalEncoder)
    {
        return new ConnectorToSqlExpressionTranslator(session, plannerContext, literalEncoder, variableMappings)
                .translate(session, expression)
                .orElseThrow(() -> new UnsupportedOperationException("Expression is not supported: " + expression.toString()));
    }

    public static Optional<ConnectorExpression> translate(Session session, Expression expression, TypeAnalyzer types, TypeProvider inputTypes, PlannerContext plannerContext)
    {
        return new SqlToConnectorExpressionTranslator(session, types.getTypes(session, inputTypes, expression), plannerContext)
                .process(expression);
    }

    private static class ConnectorToSqlExpressionTranslator
    {
        private final Session session;
        private final PlannerContext plannerContext;
        private final LiteralEncoder literalEncoder;
        private final Map<String, Symbol> variableMappings;

        public ConnectorToSqlExpressionTranslator(Session session, PlannerContext plannerContext, LiteralEncoder literalEncoder, Map<String, Symbol> variableMappings)
        {
            this.session = requireNonNull(session, "session is null");
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.literalEncoder = requireNonNull(literalEncoder, "literalEncoder is null");
            this.variableMappings = requireNonNull(variableMappings, "variableMappings is null");
        }

        public Optional<Expression> translate(Session session, ConnectorExpression expression)
        {
            if (expression instanceof Variable) {
                String name = ((Variable) expression).getName();
                return Optional.of(variableMappings.get(name).toSymbolReference());
            }

            if (expression instanceof Constant) {
                return Optional.of(literalEncoder.toExpression(session, ((Constant) expression).getValue(), expression.getType()));
            }

            if (expression instanceof ConnectorCast) {
                return translateCast((ConnectorCast) expression);
            }

            if (expression instanceof ConnectorComparison) {
                return translateComparison((ConnectorComparison) expression);
            }

            if (expression instanceof ConnectorLogicalExpression) {
                return translateLogicalExpression((ConnectorLogicalExpression) expression);
            }

            if (expression instanceof FieldDereference) {
                FieldDereference dereference = (FieldDereference) expression;
                return translate(session, dereference.getTarget())
                        .map(base -> new SubscriptExpression(base, new LongLiteral(Long.toString(dereference.getField() + 1))));
            }

            if (expression instanceof Call) {
                return translateCall((Call) expression);
            }

            return Optional.empty();
        }

        private Optional<Expression> translateLogicalExpression(ConnectorLogicalExpression expression)
        {
            ImmutableList.Builder<Expression> translatedTerms = ImmutableList.builder();

            for (ConnectorExpression term : expression.getTerms()) {
                Optional<Expression> translatedTerm = translate(session, term);
                if (translatedTerm.isEmpty()) {
                    return Optional.empty();
                }

                translatedTerms.add(translatedTerm.get());
            }

            return Optional.of(new LogicalExpression(LogicalExpression.Operator.valueOf(expression.getOperator()), translatedTerms.build()));
        }

        private Optional<Expression> translateComparison(ConnectorComparison expression)
        {
            Optional<Expression> left = translate(session, expression.getLeft());
            Optional<Expression> right = expression.getRight().flatMap(value -> translate(session, value));
            if (left.isPresent() && right.isPresent()) {
                return Optional.of(new ComparisonExpression(ComparisonExpression.Operator.forSymbol(expression.getOperatorSymbol()), left.get(), right.get()));
            }
            return Optional.empty();
        }

        private Optional<Expression> translateCast(ConnectorCast expression)
        {
            Optional<Expression> translatedExpression = translate(session, expression.getExpression());
            return translatedExpression.map(value -> new Cast(value, toSqlType(expression.getType())));
        }

        protected Optional<Expression> translateCall(Call call)
        {
            if (call.getFunctionName().getCatalogSchemaName().isPresent()) {
                return Optional.empty();
            }
            QualifiedName name = QualifiedName.of(call.getFunctionName().getFunctionName());
            List<TypeSignature> argumentTypes = call.getArguments().stream()
                    .map(argument -> argument.getType().getTypeSignature())
                    .collect(toImmutableList());
            ResolvedFunction resolved = plannerContext.getMetadata().resolveFunction(session, name, TypeSignatureProvider.fromTypeSignatures(argumentTypes));

            if (LIKE_PATTERN_FUNCTION_NAME.equals(resolved.getSignature().getName()) && call.getArguments().size() == 2) {
                return translateLike(call.getArguments().get(0), call.getArguments().get(1));
            }

            if (LIKE_PATTERN_FUNCTION_NAME.equals(resolved.getSignature().getName()) && call.getArguments().size() == 3) {
                return translateLikeWithEscape(call.getArguments().get(0), call.getArguments().get(1), call.getArguments().get(2));
            }

            FunctionCallBuilder builder = FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                    .setName(name);
            for (int i = 0; i < call.getArguments().size(); i++) {
                Type type = resolved.getSignature().getArgumentTypes().get(i);
                Expression expression = ConnectorExpressionTranslator.translate(session, call.getArguments().get(i), plannerContext, variableMappings, literalEncoder);
                builder.addArgument(type, expression);
            }
            return Optional.of(builder.build());
        }

        private Optional<Expression> translateLikeWithEscape(ConnectorExpression value, ConnectorExpression pattern, ConnectorExpression escape)
        {
            Optional<Expression> translatedValue = translate(session, value);
            Optional<Expression> translatedPattern = translate(session, pattern);
            Optional<Expression> translatedEscape = translate(session, escape);

            if (translatedValue.isPresent() && translatedPattern.isPresent() && translatedEscape.isPresent()) {
                return Optional.of(new LikePredicate(translatedValue.get(), translatedPattern.get(), translatedEscape.get()));
            }

            return Optional.empty();
        }

        protected Optional<Expression> translateLike(ConnectorExpression value, ConnectorExpression pattern)
        {
            Optional<Expression> translatedValue = translate(session, value);
            Optional<Expression> translatedPattern = translate(session, pattern);
            if (translatedValue.isPresent() && translatedPattern.isPresent()) {
                return Optional.of(new LikePredicate(translatedValue.get(), translatedPattern.get(), Optional.empty()));
            }
            return Optional.empty();
        }
    }

    public static class SqlToConnectorExpressionTranslator
            extends AstVisitor<Optional<ConnectorExpression>, Void>
    {
        private final Session session;
        private final Map<NodeRef<Expression>, Type> types;
        private final PlannerContext plannerContext;

        public SqlToConnectorExpressionTranslator(Session session, Map<NodeRef<Expression>, Type> types, PlannerContext plannerContext)
        {
            this.session = requireNonNull(session, "session is null");
            this.types = requireNonNull(types, "types is null");
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        }

        @Override
        protected Optional<ConnectorExpression> visitSymbolReference(SymbolReference node, Void context)
        {
            return Optional.of(new Variable(node.getName(), typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            return Optional.of(new Constant(node.getValue(), typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitStringLiteral(StringLiteral node, Void context)
        {
            return Optional.of(new Constant(node.getSlice(), typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitDoubleLiteral(DoubleLiteral node, Void context)
        {
            return Optional.of(new Constant(node.getValue(), typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitDecimalLiteral(DecimalLiteral node, Void context)
        {
            return Optional.of(new Constant(Decimals.parse(node.getValue()).getObject(), typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitCharLiteral(CharLiteral node, Void context)
        {
            return Optional.of(new Constant(node.getSlice(), typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitBinaryLiteral(BinaryLiteral node, Void context)
        {
            return Optional.of(new Constant(node.getValue(), typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitLongLiteral(LongLiteral node, Void context)
        {
            return Optional.of(new Constant(node.getValue(), typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitNullLiteral(NullLiteral node, Void context)
        {
            return Optional.of(new Constant(null, typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitFunctionCall(FunctionCall node, Void context)
        {
            if (!isComplexExpressionPushdown(session)) {
                return Optional.empty();
            }

            // Currently, we treat node as a builtin function.
            // TODO Distinguish between builtin, plugin-provided and runtime-added functions.
            if (node.getFilter().isPresent() || node.getOrderBy().isPresent() || node.getWindow().isPresent() || node.getNullTreatment().isPresent() || node.isDistinct()) {
                return Optional.empty();
            }

            String functionName = ResolvedFunction.extractFunctionName(node.getName());

            if (LiteralFunction.LITERAL_FUNCTION_NAME.equalsIgnoreCase(functionName)) {
                Object value = ExpressionUtils.evaluateConstantExpression(plannerContext, session, node);
                verify(!(value instanceof Expression), "Literal function did not evaluate to a constant: %s", node);
                if (value instanceof JoniRegexp) {
                    Slice pattern = ((JoniRegexp) value).pattern();
                    return Optional.of(new Constant(pattern, VarcharType.createVarcharType(countCodePoints(pattern))));
                }
                if (value instanceof Re2JRegexp) {
                    Slice pattern = Slices.utf8Slice(((Re2JRegexp) value).pattern());
                    return Optional.of(new Constant(pattern, VarcharType.createVarcharType(countCodePoints(pattern))));
                }
                return Optional.of(new Constant(value, types.get(NodeRef.of(node))));
            }

            ImmutableList.Builder<ConnectorExpression> arguments = ImmutableList.builder();
            for (Expression argumentExpression : node.getArguments()) {
                Optional<ConnectorExpression> argument = process(argumentExpression);
                if (argument.isEmpty()) {
                    return Optional.empty();
                }
                arguments.add(argument.get());
            }

            return Optional.of(new Call(typeOf(node), new FunctionName(functionName), arguments.build()));
        }

        @Override
        protected Optional<ConnectorExpression> visitLikePredicate(LikePredicate node, Void context)
        {
            Optional<ConnectorExpression> value = process(node.getValue());
            Optional<ConnectorExpression> pattern = process(node.getPattern());
            Optional<ConnectorExpression> escape = node.getEscape().flatMap(this::process);
            if (value.isPresent() && pattern.isPresent()) {
                return Optional.of(new Call(typeOf(node), new FunctionName(LIKE_PATTERN_FUNCTION_NAME), List.of(value.get(), pattern.get())));
            }

            if (value.isPresent() && pattern.isPresent() && escape.isPresent()) {
                return Optional.of(new Call(typeOf(node), new FunctionName(LIKE_PATTERN_FUNCTION_NAME), List.of(value.get(), pattern.get(), escape.get())));
            }

            return Optional.empty();
        }

        @Override
        protected Optional<ConnectorExpression> visitSubscriptExpression(SubscriptExpression node, Void context)
        {
            if (!(typeOf(node.getBase()) instanceof RowType)) {
                return Optional.empty();
            }

            Optional<ConnectorExpression> translatedBase = process(node.getBase());
            if (translatedBase.isEmpty()) {
                return Optional.empty();
            }

            return Optional.of(new FieldDereference(typeOf(node), translatedBase.get(), (int) (((LongLiteral) node.getIndex()).getValue() - 1)));
        }

        @Override
        protected Optional<ConnectorExpression> visitComparisonExpression(ComparisonExpression node, Void context)
        {
            Optional<ConnectorExpression> leftExpression = process(node.getLeft());
            if (leftExpression.isEmpty()) {
                return Optional.empty();
            }

            return Optional.of(new ConnectorComparison(typeOf(node), node.getOperator().getValue(), leftExpression.get(), process(node.getRight())));
        }

        @Override
        protected Optional<ConnectorExpression> visitLogicalExpression(LogicalExpression node, Void context)
        {
            ImmutableList.Builder<ConnectorExpression> terms = ImmutableList.builder();

            for (Expression term : node.getTerms()) {
                Optional<ConnectorExpression> termExpression = process(term);
                if (termExpression.isEmpty()) {
                    return Optional.empty();
                }

                terms.add(termExpression.get());
            }

            return Optional.of(new ConnectorLogicalExpression(node.getOperator().name(), terms.build()));
        }

        @Override
        protected Optional<ConnectorExpression> visitInPredicate(InPredicate node, Void context)
        {
            Optional<ConnectorExpression> valueExpression = process(node.getValue());
            if (valueExpression.isEmpty()) {
                return Optional.empty();
            }

            Optional<ConnectorExpression> valueList = process(node.getValueList());
            if (valueList.isEmpty()) {
                return Optional.empty();
            }

            return Optional.of(new Call(typeOf(node), new FunctionName("in"), List.of(valueExpression.get(), valueList.get())));
        }

        @Override
        protected Optional<ConnectorExpression> visitCast(Cast node, Void context)
        {
            Optional<ConnectorExpression> expression = process(node.getExpression());
            if (expression.isEmpty()) {
                return Optional.empty();
            }

            return Optional.of(new ConnectorCast(typeOf(node), expression.get()));
        }

        @Override
        protected Optional<ConnectorExpression> visitExpression(Expression node, Void context)
        {
            return Optional.empty();
        }

        private Type typeOf(Expression node)
        {
            return types.get(NodeRef.of(node));
        }
    }
}
