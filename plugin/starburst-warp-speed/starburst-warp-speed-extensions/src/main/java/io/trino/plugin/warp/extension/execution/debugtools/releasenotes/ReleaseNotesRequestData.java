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
package io.trino.plugin.warp.extension.execution.debugtools.releasenotes;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public record ReleaseNotesRequestData(io.trino.plugin.warp.extension.execution.debugtools.releasenotes.ReleaseNotesRequestData.ReleaseNoteContentType type)
{
    @JsonCreator
    public ReleaseNotesRequestData(@JsonProperty("type") ReleaseNoteContentType type)
    {
        this.type = type;
    }

    public enum ReleaseNoteContentType
    {
        LATEST,
        RELEASE,
        ALL
    }
}
