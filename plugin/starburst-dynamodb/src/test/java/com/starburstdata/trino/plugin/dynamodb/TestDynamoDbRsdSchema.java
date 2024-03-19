/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.dynamodb;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.TrinoException;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static com.starburstdata.trino.plugin.dynamodb.DynamoDbJdbcClient.getRsdSupportedOperators;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestDynamoDbRsdSchema
        extends AbstractTestQueryFramework
{
    private static final String EXPECTED_PROGRAMMATICALLY_GENERATED_SCHEMAS_DIRECTORY = "src/test/resources/programmatically-generated-schemas";
    private static final String EXPECTED_AUTO_GENERATED_SCHEMAS_DIRECTORY = "src/test/resources/auto-generated-schemas";
    private static final List<TpchTable<?>> TPCH_TABLES = TpchTable.getTables();

    private String actualSchemasDirectory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingDynamoDbServer server = closeAfterClass(new TestingDynamoDbServer());
        this.actualSchemasDirectory = server.getSchemaDirectory().getAbsolutePath();
        return DynamoDbQueryRunner.builder(server.getEndpointUrl(), server.getSchemaDirectory())
                .setFirstColumnAsPrimaryKeyEnabled(true)
                .setTables(TPCH_TABLES) // Explicitly defined the tables to make it reusable in the `testVerifyAutoGeneratedSchemas` test.
                .addConnectorProperties(
                        Map.of("dynamodb.generate-schema-files", "ON_START")) // Set to the same value as that of set in Galaxy configs.
                .build();
    }

    @Test
    @Disabled("Temporarily disabled to troubleshoot test flakiness. To be enabled through https://github.com/starburstdata/cork/issues/510")
    public void testVerifyAutoGeneratedSchemas()
    {
        requireNonNull(actualSchemasDirectory);
        assertTrue(Files.exists(Path.of(actualSchemasDirectory)));
        assertTrue(Files.exists(Path.of(EXPECTED_PROGRAMMATICALLY_GENERATED_SCHEMAS_DIRECTORY)));

        for (TpchTable<?> table : TPCH_TABLES) {
            String tableName = table.getTableName();

            // Verify the programmatically generated file is as expected
            File actualProgrammaticallyGeneratedRsdFile = new File(actualSchemasDirectory, tableName + ".rsd");
            File expectedProgrammaticallyGeneratedRsdFile = new File(EXPECTED_PROGRAMMATICALLY_GENERATED_SCHEMAS_DIRECTORY, tableName + ".rsd");
            assertThat(actualProgrammaticallyGeneratedRsdFile).exists()
                    .hasSameTextualContentAs(expectedProgrammaticallyGeneratedRsdFile);

            // Delete programmatically generated rsd file
            assertTrue(actualProgrammaticallyGeneratedRsdFile.delete());

            assertQuerySucceeds("SELECT * FROM " + tableName); // rsd is auto generated as table is queried, if absent.

            // Verify automatically generated file content is as expected
            File actualAutoGeneratedRsdFile = new File(actualProgrammaticallyGeneratedRsdFile.getAbsolutePath()); // Auto generated rsd file's absolute path is same as programmatically generated rsd file
            File expectedAutoGeneratedRsdFile = new File(EXPECTED_AUTO_GENERATED_SCHEMAS_DIRECTORY, tableName + ".rsd");
            assertThat(actualAutoGeneratedRsdFile).exists()
                    .hasSameTextualContentAs(expectedAutoGeneratedRsdFile);
        }
    }

    @Test
    public void testSqlToRsdSupportedOperatorsMapping()
    {
        String equals = "=";
        String notEquals = "!=";
        String gt = "&amp;gt;";
        String lt = "&amp;lt;";
        String gte = "&amp;gt;=";
        String lte = "&amp;lt;=";
        String is = "IS";
        String isNot = "IS_NOT";

        List<String> allOperators = List.of(equals, notEquals, gt, lt, gte, lte, is, isNot);
        List<String> isIsNotOperators = List.of(is, isNot);

        ImmutableSet.of("boolean", "varbinary", "tinyint", "smallint", "bigint", "integer", "real", "double")
                .forEach(type -> assertThat(getRsdSupportedOperators(type)).isEqualTo(allOperators));

        ImmutableSet.of("varchar(25)", "string").forEach(type -> assertThat(getRsdSupportedOperators(type)).isEqualTo(allOperators));

        assertThat(getRsdSupportedOperators("char(2)")).isEqualTo(allOperators);
        assertThatThrownBy(() -> getRsdSupportedOperators("char"))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Unsupported column type: char");

        ImmutableSet.of("date", "time(3)", "time(3) with time zone", "timestamp(3) with time zone")
                .forEach(type -> assertThat(getRsdSupportedOperators(type)).isEqualTo(isIsNotOperators));

        ImmutableSet.of("json").forEach(type -> assertThat(getRsdSupportedOperators(type)).isEqualTo(allOperators));

        ImmutableSet.of("decimal(12,5)").forEach(type -> assertThat(getRsdSupportedOperators(type)).isEqualTo(allOperators));
        assertThatThrownBy(() -> getRsdSupportedOperators("decimal"))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Unsupported column type: decimal");

        assertThatThrownBy(() -> getRsdSupportedOperators("non_existing"))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Unsupported column type: non_existing");
    }
}
