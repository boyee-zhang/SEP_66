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
package io.trino.operator.hash;

import io.trino.spi.Page;
import io.trino.spi.PageBuilder;

public interface NoRehashHashTable
{
    int putIfAbsent(int position, Page page);

    boolean contains(int position, Page page);

    boolean contains(int position, Page page, long rawHash);

    int getSize();

    int getCapacity();

    long getEstimatedSize();

    void copyFrom(NoRehashHashTable other);

    void appendValuesTo(int hashPosition, PageBuilder pageBuilder, int outputChannelOffset, boolean outputHash);

    long getRawHash(int groupId);

    long getHashCollisions();

    int isNull(int hashPosition, int index);

    boolean needRehash();

    int maxFill();

    NoRehashHashTable resize(int newCapacity);
}
