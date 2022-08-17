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
package io.trino.sql.ir;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

@Immutable
public class FieldReference
        extends Expression
{
    private final int fieldIndex;

    @JsonCreator
    public FieldReference(
            @JsonProperty("fieldIndex") int fieldIndex)
    {
        checkArgument(fieldIndex >= 0, "fieldIndex must be >= 0");

        this.fieldIndex = fieldIndex;
    }

    @JsonProperty
    public int getFieldIndex()
    {
        return fieldIndex;
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitFieldReference(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
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

        FieldReference that = (FieldReference) o;

        return fieldIndex == that.fieldIndex;
    }

    @Override
    public int hashCode()
    {
        return fieldIndex;
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        return fieldIndex == ((FieldReference) other).fieldIndex;
    }
}
