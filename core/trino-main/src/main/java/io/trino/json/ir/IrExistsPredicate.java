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
package io.trino.json.ir;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class IrExistsPredicate
        extends IrPredicate
{
    private final IrPathNode path;

    @JsonCreator
    public IrExistsPredicate(@JsonProperty("path") IrPathNode path)
    {
        super();
        this.path = requireNonNull(path, "path is null");
    }

    @Override
    protected <R, C> R accept(IrJsonPathVisitor<R, C> visitor, C context)
    {
        return visitor.visitIrExistsPredicate(this, context);
    }

    @JsonProperty
    public IrPathNode getPath()
    {
        return path;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        IrExistsPredicate other = (IrExistsPredicate) obj;
        return Objects.equals(this.path, other.path);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(path);
    }
}
