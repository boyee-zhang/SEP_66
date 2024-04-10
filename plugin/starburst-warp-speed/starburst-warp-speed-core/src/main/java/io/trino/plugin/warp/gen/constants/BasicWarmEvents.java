
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

package io.trino.plugin.warp.gen.constants;

public enum BasicWarmEvents
{
    BASIC_WARM_EVENTS_COMPRESSION_LZ,
    BASIC_WARM_EVENTS_DIRECT_ROOT,
    BASIC_WARM_EVENTS_ENTRY_RAW,
    BASIC_WARM_EVENTS_ENTRY_BM,
    BASIC_WARM_EVENTS_ENTRY_RANGE,
    BASIC_WARM_EVENTS_ENTRY_DELTA,
    BASIC_WARM_EVENTS_ENTRY_DELTA_BIT_PACKING,
    BASIC_WARM_EVENTS_ENTRY_SINGLES,
    BASIC_WARM_EVENTS_SINGLE_CHUNK,
    BASIC_WARM_EVENTS_NUM_OF;

    BasicWarmEvents()
    {
    }
}