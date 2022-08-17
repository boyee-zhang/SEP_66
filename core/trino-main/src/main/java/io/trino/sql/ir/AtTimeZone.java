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
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class AtTimeZone
        extends Expression
{
    private final Expression value;
    private final Expression timeZone;

    @JsonCreator
    public AtTimeZone(
            @JsonProperty("value") Expression value,
            @JsonProperty("timeZone") Expression timeZone)
    {
        checkArgument(timeZone instanceof IntervalLiteral || timeZone instanceof StringLiteral, "timeZone must be IntervalLiteral or StringLiteral");
        this.value = requireNonNull(value, "value is null");
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
    }

    @JsonProperty
    public Expression getValue()
    {
        return value;
    }

    @JsonProperty
    public Expression getTimeZone()
    {
        return timeZone;
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitAtTimeZone(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(value, timeZone);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value, timeZone);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        AtTimeZone atTimeZone = (AtTimeZone) obj;
        return Objects.equals(value, atTimeZone.value) && Objects.equals(timeZone, atTimeZone.timeZone);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other);
    }
}
