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
package io.prestosql.plugin.kafka.encoder.csv;

import io.prestosql.plugin.kafka.encoder.EncoderColumnHandle;
import io.prestosql.plugin.kafka.encoder.RowEncoder;
import io.prestosql.plugin.kafka.encoder.RowEncoderFactory;

import java.util.Map;
import java.util.Set;

public class CsvRowEncoderFactory
        implements RowEncoderFactory
{
    @Override
    public RowEncoder create(Map<String, String> encoderParams, Set<EncoderColumnHandle> columnHandles)
    {
        return new CsvRowEncoder(columnHandles);
    }
}
