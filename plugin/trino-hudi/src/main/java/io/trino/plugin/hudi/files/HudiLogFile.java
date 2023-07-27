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
package io.trino.plugin.hudi.files;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.Location;

import java.util.Comparator;
import java.util.Objects;

import static io.trino.plugin.hudi.files.FSUtils.getBaseCommitTimeFromLogPath;
import static io.trino.plugin.hudi.files.FSUtils.getFileIdFromLogPath;
import static io.trino.plugin.hudi.files.FSUtils.getFileVersionFromLog;
import static io.trino.plugin.hudi.files.FSUtils.getWriteTokenFromLogPath;

public class HudiLogFile
        implements HudiFile
{
    private static final Comparator<HudiLogFile> LOG_FILE_COMPARATOR_REVERSED = new HudiLogFile.LogFileComparator().reversed();

    private final String pathStr;
    private final long fileLen;
    private final long modificationTime;

    public HudiLogFile(FileEntry fileEntry)
    {
        this(fileEntry.location().toString(), fileEntry.length(), fileEntry.lastModified().toEpochMilli());
    }

    @JsonCreator
    public HudiLogFile(@JsonProperty("pathStr") String pathStr,
                       @JsonProperty("fileLen") long fileLen,
                       @JsonProperty("modificationTime") long modificationTime)
    {
        this.pathStr = pathStr;
        this.fileLen = fileLen;
        this.modificationTime = modificationTime;
    }

    public String getFileId()
    {
        return getFileIdFromLogPath(getLocation());
    }

    public String getBaseCommitTime()
    {
        return getBaseCommitTimeFromLogPath(getLocation());
    }

    public int getLogVersion()
    {
        return getFileVersionFromLog(getLocation());
    }

    public String getLogWriteToken()
    {
        return getWriteTokenFromLogPath(getLocation());
    }

    @JsonProperty
    public String getPathStr()
    {
        return pathStr;
    }

    @Override
    public Location getLocation()
    {
        return Location.of(pathStr);
    }

    @Override
    public long getOffset()
    {
        return 0L;
    }

    @JsonProperty
    @Override
    public long getFileLen()
    {
        return fileLen;
    }

    @JsonProperty
    @Override
    public long getFileModifiedTime()
    {
        return modificationTime;
    }

    public static Comparator<HudiLogFile> getReverseLogFileComparator()
    {
        return LOG_FILE_COMPARATOR_REVERSED;
    }

    public static class LogFileComparator
            implements Comparator<HudiLogFile>
    {
        private transient Comparator<String> writeTokenComparator;

        private Comparator<String> getWriteTokenComparator()
        {
            if (null == writeTokenComparator) {
                // writeTokenComparator is not serializable. Hence, lazy loading
                writeTokenComparator = Comparator.nullsFirst(Comparator.naturalOrder());
            }
            return writeTokenComparator;
        }

        @Override
        public int compare(HudiLogFile o1, HudiLogFile o2)
        {
            String baseInstantTime1 = o1.getBaseCommitTime();
            String baseInstantTime2 = o2.getBaseCommitTime();

            if (baseInstantTime1.equals(baseInstantTime2)) {
                if (o1.getLogVersion() == o2.getLogVersion()) {
                    // Compare by write token when base-commit and log-version is same
                    return getWriteTokenComparator().compare(o1.getLogWriteToken(), o2.getLogWriteToken());
                }

                // compare by log-version when base-commit is same
                return Integer.compare(o1.getLogVersion(), o2.getLogVersion());
            }

            // compare by base-commits
            return baseInstantTime1.compareTo(baseInstantTime2);
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HudiLogFile that = (HudiLogFile) o;
        return Objects.equals(pathStr, that.pathStr);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(pathStr);
    }

    @Override
    public String toString()
    {
        return "HoodieLogFile{pathStr='" + pathStr + '\'' + ", fileLen=" + fileLen + '}';
    }
}
