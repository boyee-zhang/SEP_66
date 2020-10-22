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
package io.prestosql.plugin.hive.parquet;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.apache.parquet.hadoop.ParquetWriter;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class TestParquetWriterConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ParquetWriterConfig.class)
                .setParquetOptimizedWriterEnabled(false)
                .setBlockSize(DataSize.ofBytes(ParquetWriter.DEFAULT_BLOCK_SIZE))
                .setPageSize(DataSize.ofBytes(ParquetWriter.DEFAULT_PAGE_SIZE)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.parquet.experimental-optimized-writer.enabled", "true")
                .put("hive.parquet.writer.block-size", "234MB")
                .put("hive.parquet.writer.page-size", "11MB")
                .build();

        ParquetWriterConfig expected = new ParquetWriterConfig()
                .setParquetOptimizedWriterEnabled(true)
                .setBlockSize(DataSize.of(234, MEGABYTE))
                .setPageSize(DataSize.of(11, MEGABYTE));

        assertFullMapping(properties, expected);
    }
}
