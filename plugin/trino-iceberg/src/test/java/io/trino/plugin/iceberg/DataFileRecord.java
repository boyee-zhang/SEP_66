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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.MaterializedRow;

import java.util.Map;

import static org.testng.Assert.assertEquals;

public class DataFileRecord
{
    private final int content;
    private final String filePath;
    private final String fileFormat;
    private final long recordCount;
    private final long fileSizeInBytes;
    private final Map<String, Long> columnSizes;
    private final Map<String, Long> valueCounts;
    private final Map<String, Long> nullValueCounts;
    private final Map<String, Long> nanValueCounts;
    private final Map<String, String> lowerBounds;
    private final Map<String, String> upperBounds;

    public static DataFileRecord toDataFileRecord(MaterializedRow row)
    {
        assertEquals(row.getFieldCount(), 14);
        return new DataFileRecord(
                (int) row.getField(0),
                (String) row.getField(1),
                (String) row.getField(2),
                (long) row.getField(3),
                (long) row.getField(4),
                row.getField(5) != null ? ImmutableMap.copyOf((Map<String, Long>) row.getField(5)) : null,
                row.getField(6) != null ? ImmutableMap.copyOf((Map<String, Long>) row.getField(6)) : null,
                row.getField(7) != null ? ImmutableMap.copyOf((Map<String, Long>) row.getField(7)) : null,
                row.getField(8) != null ? ImmutableMap.copyOf((Map<String, Long>) row.getField(8)) : null,
                row.getField(9) != null ? ImmutableMap.copyOf((Map<String, String>) row.getField(9)) : null,
                row.getField(10) != null ? ImmutableMap.copyOf((Map<String, String>) row.getField(10)) : null);
    }

    private DataFileRecord(
            int content,
            String filePath,
            String fileFormat,
            long recordCount,
            long fileSizeInBytes,
            Map<String, Long> columnSizes,
            Map<String, Long> valueCounts,
            Map<String, Long> nullValueCounts,
            Map<String, Long> nanValueCounts,
            Map<String, String> lowerBounds,
            Map<String, String> upperBounds)
    {
        this.content = content;
        this.filePath = filePath;
        this.fileFormat = fileFormat;
        this.recordCount = recordCount;
        this.fileSizeInBytes = fileSizeInBytes;
        this.columnSizes = columnSizes;
        this.valueCounts = valueCounts;
        this.nullValueCounts = nullValueCounts;
        this.nanValueCounts = nanValueCounts;
        this.lowerBounds = lowerBounds;
        this.upperBounds = upperBounds;
    }

    public int getContent()
    {
        return content;
    }

    public String getFilePath()
    {
        return filePath;
    }

    public String getFileFormat()
    {
        return fileFormat;
    }

    public long getRecordCount()
    {
        return recordCount;
    }

    public long getFileSizeInBytes()
    {
        return fileSizeInBytes;
    }

    public Map<String, Long> getColumnSizes()
    {
        return columnSizes;
    }

    public Map<String, Long> getValueCounts()
    {
        return valueCounts;
    }

    public Map<String, Long> getNullValueCounts()
    {
        return nullValueCounts;
    }

    public Map<String, Long> getNanValueCounts()
    {
        return nanValueCounts;
    }

    public Map<String, String> getLowerBounds()
    {
        return lowerBounds;
    }

    public Map<String, String> getUpperBounds()
    {
        return upperBounds;
    }
}
