/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake.jdbc;

import com.google.common.collect.ImmutableList;
import com.starburstdata.trino.plugin.snowflake.SnowflakeSessionProperties;
import io.airlift.slice.Slices;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.type.Type;

import java.util.Optional;

import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.constant;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class RewriteJsonPath
        implements ConnectorExpressionRule<Constant, ParameterizedExpression>
{
    private final Pattern<Constant> pattern;

    public RewriteJsonPath(Type jsonPathType)
    {
        this.pattern = constant().with(type().matching(jsonPathType.getClass()::isInstance));
    }

    @Override
    public Pattern<Constant> getPattern()
    {
        return pattern;
    }

    @Override
    public Optional<ParameterizedExpression> rewrite(Constant constant, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        if (!SnowflakeSessionProperties.getExperimentalPushdownEnabled(context.getSession())) {
            return Optional.empty();
        }

        if (constant.getValue() == null) {
            return Optional.empty();
        }
        String snowflakeJsonPath = constant.getValue().toString();
        if (snowflakeJsonPath == null) {
            return Optional.empty();
        }
        if (snowflakeJsonPath.startsWith("$.")) {
            snowflakeJsonPath = snowflakeJsonPath.substring("$.".length());
        }
        return Optional.of(new ParameterizedExpression("?", ImmutableList.of(new QueryParameter(VARCHAR, Optional.of(Slices.utf8Slice(snowflakeJsonPath))))));
    }
}