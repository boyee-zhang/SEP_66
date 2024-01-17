/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake.parallel;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.trino.plugin.base.mapping.MappingConfig;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.plugin.base.session.PropertyMetadataUtil.dataSizeProperty;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static java.util.Objects.requireNonNull;

public class SnowflakeParallelSessionProperties
        implements SessionPropertiesProvider
{
    public static final String CLIENT_RESULT_CHUNK_SIZE = "client_result_chunk_size";
    public static final String QUOTED_IDENTIFIERS_IGNORE_CASE = "quoted_identifiers_ignore_case";
    private static final String TARGET_SPLIT_SIZE = "target_split_size";
    private static final DataSize DEFAULT_TARGET_SPLIT_SIZE = DataSize.of(256, MEGABYTE);

    private final MappingConfig mappingConfig;

    @Inject
    public SnowflakeParallelSessionProperties(MappingConfig mappingConfig)
    {
        this.mappingConfig = requireNonNull(mappingConfig, "mappingConfig is null");
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return ImmutableList.of(
                dataSizeProperty(
                        CLIENT_RESULT_CHUNK_SIZE,
                        "Max result chunk size (MB)",
                        // 160 is Snowflake default: https://docs.snowflake.com/en/sql-reference/parameters#client-result-chunk-size
                        DataSize.of(160, DataSize.Unit.MEGABYTE),
                        true),
                booleanProperty(
                        QUOTED_IDENTIFIERS_IGNORE_CASE,
                        "Propagate QUOTED_IDENTIFIERS_IGNORE_CASE to Snowflake, changes how it resolves quoted identifiers",
                        false,
                        value -> checkArgument(
                                !(value && mappingConfig.isCaseInsensitiveNameMatching()),
                                "Enabling quoted_identifiers_ignore_case not supported for Snowflake when case-insensitive-name-matching is enabled"),
                        true),
                dataSizeProperty(
                        TARGET_SPLIT_SIZE,
                        "Target size of a single split",
                        DEFAULT_TARGET_SPLIT_SIZE,
                        true));
    }

    public static Optional<DataSize> getResultChunkSize(ConnectorSession session)
    {
        return Optional.ofNullable(session.getProperty(CLIENT_RESULT_CHUNK_SIZE, DataSize.class));
    }

    public static boolean getQuotedIdentifiersIgnoreCase(ConnectorSession session)
    {
        return session.getProperty(QUOTED_IDENTIFIERS_IGNORE_CASE, Boolean.class);
    }

    public static DataSize getTargetSplitSize(ConnectorSession session)
    {
        return session.getProperty(TARGET_SPLIT_SIZE, DataSize.class);
    }
}