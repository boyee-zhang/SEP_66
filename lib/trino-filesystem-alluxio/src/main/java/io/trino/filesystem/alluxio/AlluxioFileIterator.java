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
package io.trino.filesystem.alluxio;

import alluxio.client.file.URIStatus;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static io.trino.filesystem.alluxio.AlluxioUtils.convertToLocation;
import static java.util.Objects.requireNonNull;

public class AlluxioFileIterator
        implements FileIterator
{
    private final List<URIStatus> files;
    private final String mountRoot;
    private int index;

    public AlluxioFileIterator(List<URIStatus> files, String mountRoot)
    {
        this.files = requireNonNull(files, "files is null");
        this.mountRoot = requireNonNull(mountRoot, "mountRoot is null");
    }

    @Override
    public boolean hasNext()
            throws IOException
    {
        if (files.isEmpty() || index >= files.size()) {
            return false;
        }
        if (files.get(index) != null) {
            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public FileEntry next()
            throws IOException
    {
        if (!hasNext()) {
            return null;
        }
        URIStatus fileStatus = files.get(index++);
        Location location = convertToLocation(fileStatus, mountRoot);
        return new FileEntry(
                location,
                fileStatus.getLength(),
                Instant.ofEpochMilli(fileStatus.getLastModificationTimeMs()),
                Optional.empty());
    }
}
