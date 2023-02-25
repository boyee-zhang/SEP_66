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
package io.trino.plugin.redshift;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestRedshiftConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(RedshiftConfig.class)
                .setLegacyTypeMapping(false)
                .setJdbcFetchSize(1000));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("redshift.use-legacy-type-mapping", "true")
                .put("redshift.jdbc-fetch-size", "2000")
                .buildOrThrow();

        RedshiftConfig expected = new RedshiftConfig()
                .setLegacyTypeMapping(true)
                .setJdbcFetchSize(2000);

        assertFullMapping(properties, expected);
    }
}
