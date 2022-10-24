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
package io.trino.plugin.hive.parquet;

import io.airlift.slice.Slice;
import io.trino.filesystem.TrinoInput;
import io.trino.parquet.AbstractParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.spi.TrinoException;

import java.io.IOException;

import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static java.lang.String.format;

public class HdfsParquetDataSource
        extends AbstractParquetDataSource
{
    private final TrinoInput input;
    private final FileFormatDataSourceStats stats;

    public HdfsParquetDataSource(
            ParquetDataSourceId id,
            long estimatedSize,
            TrinoInput input,
            FileFormatDataSourceStats stats,
            ParquetReaderOptions options)
    {
        super(id, estimatedSize, options);
        this.input = input;
        this.stats = stats;
    }

    @Override
    public void close()
            throws IOException
    {
        input.close();
    }

    @Override
    protected Slice readTailInternal(int length)
    {
        try {
            //  Handle potentially imprecise file lengths by reading the footer
            long readStart = System.nanoTime();
            Slice tailSlice = input.readTail(length);
            stats.readDataBytesPerSecond(tailSlice.length(), System.nanoTime() - readStart);
            return tailSlice;
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_FILESYSTEM_ERROR, format("Error reading tail from %s with length %s", getId(), length), e);
        }
    }

    @Override
    protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
    {
        try {
            long readStart = System.nanoTime();
            input.readFully(position, buffer, bufferOffset, bufferLength);
            stats.readDataBytesPerSecond(bufferLength, System.nanoTime() - readStart);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_FILESYSTEM_ERROR, format("Error reading from %s at position %s", getId(), position), e);
        }
    }
}
