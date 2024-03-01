/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery;

import com.google.common.collect.ImmutableMap;
import io.starburst.schema.discovery.models.DiscoveredTable;
import io.starburst.schema.discovery.models.TableFormat;
import io.starburst.schema.discovery.options.GeneralOptions;
import io.starburst.schema.discovery.options.OptionsMap;
import io.starburst.schema.discovery.processor.Processor;
import io.trino.filesystem.Location;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Comparator;
import java.util.concurrent.Executors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.starburst.schema.discovery.Util.schemaDiscoveryInstances;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergSchemaDiscovery
{
    private static final OptionsMap OPTIONS = new OptionsMap(ImmutableMap.of(GeneralOptions.SAMPLE_FILES_PER_TABLE_MODULO, "1"));

    @Test
    public void testIceberg()
    {
        Location directory = Util.testFilePath("iceberg/table1");
        Processor processor = new Processor(schemaDiscoveryInstances, Util.fileSystem(), directory, OPTIONS, Executors.newCachedThreadPool());
        processor.startRootProcessing();
        assertThat(processor)
                .succeedsWithin(Duration.ofSeconds(1))
                .matches(discovered -> discovered.rootPath().path().endsWith("iceberg/table1/")
                                       && discovered.errors().isEmpty()
                                       && discovered.tables().size() == 1)
                .extracting(discovered -> getOnlyElement(discovered.tables()))
                .matches(table -> table.valid() && table.path().path().equals("s3://starburst-benchmarks-data/iceberg-tpcds-sf1-orc-part/catalog_page") && table.format() == TableFormat.ICEBERG);
    }

    @Test
    public void testIcebergTableWithoutLocationInMetadata()
    {
        Location directory = Util.testFilePath("iceberg/table_invalid");
        Processor processor = new Processor(schemaDiscoveryInstances, Util.fileSystem(), directory, OPTIONS, Executors.newCachedThreadPool());
        processor.startRootProcessing();
        assertThat(processor)
                .succeedsWithin(Duration.ofSeconds(1))
                .matches(discovered -> discovered.rootPath().path().endsWith("iceberg/table_invalid/")
                                       && discovered.errors().isEmpty()
                                       && discovered.tables().size() == 1)
                .extracting(discovered -> getOnlyElement(discovered.tables()))
                .matches(table -> !table.valid(), "Table must be invalid")
                .matches(table -> !table.errors().isEmpty() && table.errors().getFirst().equals("Failed to read location from iceberg table's latest metadata file - [Cannot parse missing string: location]"), "Table must have invalid metadata error")
                .matches(table -> table.format() == TableFormat.ERROR, "Table must have error format");
    }

    @Test
    public void testIcebergTablesParent()
    {
        Location directory = Util.testFilePath("iceberg");
        Processor processor = new Processor(schemaDiscoveryInstances, Util.fileSystem(), directory, OPTIONS, Executors.newCachedThreadPool());
        processor.startRootProcessing();
        assertThat(processor)
                .succeedsWithin(Duration.ofSeconds(1))
                .matches(discovered -> discovered.rootPath().path().endsWith("iceberg/")
                                       && discovered.tables().size() == 3)
                .extracting(discoveredSchema -> discoveredSchema.tables().stream()
                        .sorted(Comparator.comparing(DiscoveredTable::path))
                        .collect(toImmutableList()))
                .matches(tables ->
                        !tables.get(0).valid() && !tables.get(0).errors().isEmpty() && tables.get(0).format() == TableFormat.ERROR &&
                        tables.get(1).valid() && tables.get(1).path().path().equals("s3://starburst-benchmarks-data/iceberg-tpcds-sf1-orc-part/catalog_page") && tables.get(1).format() == TableFormat.ICEBERG &&
                        tables.get(2).valid() && tables.get(2).path().path().equals("s3://starburst-benchmarks-data/iceberg-tpcds-sf10-orc-part/catalog_page") && tables.get(2).format() == TableFormat.ICEBERG);
    }

    @Test
    public void testModuloRecursiveIceberg()
    {
        OptionsMap optionsMap = new OptionsMap(ImmutableMap.of(GeneralOptions.SAMPLE_FILES_PER_TABLE_MODULO, "8", GeneralOptions.MAX_SAMPLE_FILES_PER_TABLE, "1", GeneralOptions.DISCOVERY_MODE, "recursive_directories"));
        Location directory = Util.testFilePath("iceberg");
        Processor processor = new Processor(schemaDiscoveryInstances, Util.fileSystem(), directory, optionsMap, Executors.newCachedThreadPool());
        processor.startRootProcessing();
        assertThat(processor)
                .succeedsWithin(Duration.ofSeconds(1))
                .matches(discovered -> discovered.rootPath().path().endsWith("iceberg/")
                                       && discovered.tables().size() == 3)
                .extracting(discoveredSchema -> discoveredSchema.tables().stream()
                        .sorted(Comparator.comparing(DiscoveredTable::path))
                        .collect(toImmutableList()))
                .matches(tables ->
                        !tables.get(0).valid() && !tables.get(0).errors().isEmpty() && tables.get(0).format() == TableFormat.ERROR &&
                        tables.get(1).valid() && tables.get(1).path().path().equals("s3://starburst-benchmarks-data/iceberg-tpcds-sf1-orc-part/catalog_page") && tables.get(1).format() == TableFormat.ICEBERG &&
                        tables.get(2).valid() && tables.get(2).path().path().equals("s3://starburst-benchmarks-data/iceberg-tpcds-sf10-orc-part/catalog_page") && tables.get(2).format() == TableFormat.ICEBERG);
    }
}
